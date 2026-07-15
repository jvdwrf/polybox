extern crate proc_macro;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Expr, Fields, Lit, Type};

#[proc_macro_derive(Interface, attributes(zestors))]
pub fn derive_interface(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;

    // 1. Determine the base path (default: ::zestors)
    let mut base_path: syn::Path = syn::parse_str("::zestors").unwrap();

    for attr in &input.attrs {
        if attr.path().is_ident("zestors") {
            // Correct way to parse nested meta (e.g. #[zestors(crate = "...")] ) in syn 2.0
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
                    Self::#variant_name(payload) => #base_path::AnyPayload::new::<#inner_type>(payload),
                });

                from_impls.push(quote! {
                    impl From<#field_type> for #enum_name {
                        fn from(payload: #field_type) -> Self {
                            Self::#variant_name(payload)
                        }
                    }
                });
            }
            _ => panic!("Interface derive only supports variants with a single unnamed field, e.g., A(Payload<T>)"),
        }
    }

    let expanded = quote! {
        impl #base_path::Interface for #enum_name {
            fn try_from_any_payload(payload: #base_path::AnyPayload) -> Result<Self, #base_path::AnyPayload> {
                #(#try_from_matches)*
                Err(payload)
            }

            // Could be added to improve performance, but would require unsafe transmute to avoid double downcasting.
            // fn try_into_payload<I: #base_path::Invocation>(self) -> Result<#base_path::Payload<I>, Self> {
            //     let id = std::any::TypeId::of::<I>();
            //     #(#try_into_matches)*
            //     Err(self)
            // }

            fn into_any_payload(self) -> #base_path::AnyPayload {
                match self {
                    #(#into_matches)*
                }
            }
        }

        impl #base_path::AsSet for #enum_name {
            type Set = #base_path::Set![#(#inner_types),*];
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

#[proc_macro_derive(Invocation, attributes(zestors, invoke))]
pub fn derive_invocation(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let name = &input.ident;

    // // 1. Determine the base path (default: ::zestors)
    // let mut base_path: syn::Path = syn::parse_str("::zestors").unwrap();

    // // 2. Determine the default Kind (default: ::zestors::FireAndForget)
    // let mut kind_type = quote!(::zestors::FireAndForget);

    let zestors_attr = &input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("zestors"));

    let base_path = if let Some(attr) = zestors_attr {
        let mut base_path: syn::Path = syn::parse_str("::zestors").unwrap();
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
        syn::parse_str("::zestors").unwrap()
    };

    let invoke_attr = &input
        .attrs
        .iter()
        .find(|attr| attr.path().is_ident("invoke"));

    let kind_type = if let Some(attr) = invoke_attr {
        let mut kind_type = quote!(#base_path::FireAndForget);
        let _ = attr.parse_nested_meta(|meta| {
            if meta.path.is_ident("request") {
                let value = meta.value()?;
                if let Ok(parsed_type) = value.parse::<Type>() {
                    kind_type = quote! {
                        #base_path::Request<#parsed_type>
                    }
                }
            } else {
                panic!("Only `request` is expected")
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
        impl #impl_generics #base_path::Invocation for #name #ty_generics #where_clause {
            type Kind = #kind_type;
        }
    };

    TokenStream::from(expanded)
}
