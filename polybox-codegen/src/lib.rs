//! Macros for the `polybox` crate.
//!
//! See [GitHub](https://github.com/jvdwrf/polybox) for more information.

extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Expr, Fields, Lit, Type};

/// Derives the `Interface` trait for an enum, allowing it to be used as a message
/// interface in the Polybox framework.
///
/// Under the hood, this macro generates implementations for the `Interface`, `Message`, and `AsSet` traits,
/// as well as `FromPayload` and `TryIntoPayload` for each variant of the enum.
///
/// The macro expects the enum variants to be of the form `Variant(Payload<T>)`,
/// where `T` is a type that implements the `Message` trait.
#[proc_macro_derive(Interface, attributes(polybox))]
pub fn derive_interface(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;

    // 1. Determine the base path (default: ::polybox)
    let mut base_path: syn::Path = syn::parse_str("::polybox").unwrap();

    for attr in &input.attrs {
        if attr.path().is_ident("polybox") {
            // Correct way to parse nested meta (e.g. #[polybox(crate = "...")] ) in syn 2.0
            let _ = attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("crate") {
                    let value = meta.value()?;
                    let expr: Expr = value.parse()?;
                    if let Expr::Lit(syn::ExprLit {
                        lit: Lit::Str(lit_str),
                        ..
                    }) = expr
                    {
                        if let Ok(parsed_path) = syn::parse_str::<syn::Path>(&lit_str.value()) {
                            base_path = parsed_path;
                        }
                    }
                }
                Ok(())
            });
        }
    }

    // Ensure we are working with an enum
    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => panic!("Interface derive can only be used on enums"),
    };

    let mut inner_types = Vec::new();
    let mut try_from_matches = Vec::new();
    let mut try_into_matches = Vec::new();
    let mut into_matches = Vec::new();
    let mut from_impls = Vec::new();

    for variant in variants {
        let variant_name = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed[0].ty;

                let inner_type = extract_inner_type(field_type)
                    .expect("Interface variants must be of type Payload<T>");

                inner_types.push(inner_type);

                try_from_matches.push(quote! {
                    let payload = match payload.downcast::<#inner_type>() {
                        Ok(payload) => return Ok(Self::#variant_name(payload)),
                        Err(payload) => payload,
                    };
                });

                try_into_matches.push(quote! {
                    if id == std::any::TypeId::of::<#inner_type>() {
                        if let Self::#variant_name(payload) = self {
                            // SAFETY: Verified type matches dynamic I parameter.
                            let converted = unsafe {
                                std::mem::transmute_copy::<#base_path::Payload<#inner_type>, #base_path::Payload<I>>(&payload)
                            };
                            std::mem::forget(payload);
                            return Ok(converted);
                        }
                    }
                });

                into_matches.push(quote! {
                    Self::#variant_name(payload) => #base_path::BoxedPayload::new::<#inner_type>(payload),
                });

                from_impls.push(quote! {
                    impl #base_path::FromPayload<#inner_type> for #enum_name {
                        fn from_payload(payload: #base_path::Payload<#inner_type>) -> Self {
                            Self::#variant_name(payload)
                        }
                    }

                    impl #base_path::TryIntoPayload<#inner_type> for #enum_name {
                        fn try_into_payload(self) -> Result<#base_path::Payload<#inner_type>, Self> {
                            if let #enum_name::#variant_name(payload) = self {
                                Ok(payload)
                            } else {
                                Err(self)
                            }
                        }
                    }
                });
            }
            _ => panic!("Interface derive only supports variants with a single unnamed field, e.g., A(Payload<T>)"),
        }
    }

    let expanded = quote! {
        impl #base_path::Interface for #enum_name {
            fn try_from_boxed_payload(payload: #base_path::BoxedPayload) -> Result<Self, #base_path::BoxedPayload> {
                #(#try_from_matches)*
                Err(payload)
            }

            // Could be added to improve performance, but would require unsafe transmute to avoid double downcasting.
            // fn try_into_payload<I: #base_path::Message>(self) -> Result<#base_path::Payload<I>, Self> {
            //     let id = std::any::TypeId::of::<I>();
            //     #(#try_into_matches)*
            //     Err(self)
            // }

            fn into_boxed_payload(self) -> #base_path::BoxedPayload {
                match self {
                    #(#into_matches)*
                }
            }
        }

        impl #base_path::Message for #enum_name {
            type Kind = #base_path::FireAndForget;
        }

        impl #base_path::type_sets::AsSet for #enum_name {
            type Set = #base_path::type_sets::Set![#(#inner_types),*];
        }

        impl #base_path::TryIntoPayload<#enum_name> for #enum_name {
            fn try_into_payload(self) -> Result<#base_path::Payload<#enum_name>, Self> {
                Ok(self)
            }
        }

        impl #base_path::FromPayload<#enum_name> for #enum_name {
            fn from_payload(payload: #base_path::Payload<#enum_name>) -> Self {
                payload
            }
        }

        #(#from_impls)*
    };

    TokenStream::from(expanded)
}

fn extract_inner_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        let segment = type_path.path.segments.last()?;
        if segment.ident == "Payload" {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                    return Some(inner_ty);
                }
            }
        }
    }
    None
}

/// Derives the `Message` trait for a struct, allowing it to be used as a message
/// in the Polybox framework.
///
/// This macro accepts an optional `reply` attribute to specify the reply type for the message.
///
/// # Example
/// ```ignore
/// #[derive(Message)]
/// struct SimpleMessage;
///
/// #[derive(Message)]
/// #[msg(reply = u32)]
/// struct MessageWithReply;
/// ```
#[proc_macro_derive(Message, attributes(polybox, msg))]
pub fn derive_invocation(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // // 1. Determine the base path (default: ::polybox)
    // let mut base_path: syn::Path = syn::parse_str("::polybox").unwrap();

    // // 2. Determine the default Kind (default: ::polybox::FireAndForget)
    // let mut kind_type = quote!(::polybox::FireAndForget);

    let polybox_attr = &input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("polybox"));

    let base_path = if let Some(attr) = polybox_attr {
        let mut base_path: syn::Path = syn::parse_str("::polybox").unwrap();
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("crate") {
                let value = meta.value()?;
                let expr: Expr = value.parse()?;
                if let Expr::Lit(syn::ExprLit {
                    lit: Lit::Str(lit_str),
                    ..
                }) = expr
                {
                    if let Ok(parsed_path) = syn::parse_str::<syn::Path>(&lit_str.value()) {
                        base_path = parsed_path;
                    }
                }
            }
            Ok(())
        });
        base_path
    } else {
        syn::parse_str("::polybox").unwrap()
    };

    let invoke_attr = &input.attrs.iter().find(|attr| attr.path().is_ident("msg"));

    let kind_type = if let Some(attr) = invoke_attr {
        let mut kind_type = quote!(#base_path::FireAndForget);
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("reply") {
                let value = meta.value()?;
                if let Ok(parsed_type) = value.parse::<Type>() {
                    kind_type = quote! {
                        #base_path::Request<#parsed_type>
                    }
                }
            } else {
                panic!("Only `reply` is expected")
            }
            Ok(())
        });
        kind_type
    } else {
        quote!(#base_path::FireAndForget)
    };

    // Handle generics if the struct/enum is generic
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = quote! {
        impl #impl_generics #base_path::Message for #name #ty_generics #where_clause {
            type Kind = #kind_type;
        }
    };

    TokenStream::from(expanded)
}
