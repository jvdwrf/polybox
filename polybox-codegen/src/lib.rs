//! Macros for the `polybox` crate.
//!
//! See [GitHub](https://github.com/jvdwrf/polybox) for more information.

extern crate proc_macro;
use darling::FromAttributes;
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
#[proc_macro_derive(Interface, attributes(interface))]
pub fn derive_interface_polybox(input: TokenStream) -> TokenStream {
    derive_interface(input, "::polybox")
}

#[proc_macro_derive(InterfaceZestors, attributes(interface))]
pub fn derive_interface_zestors(input: TokenStream) -> TokenStream {
    derive_interface(input, "::zestors")
}

fn derive_interface(input: TokenStream, base: &str) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;

    let base_path: syn::Path = extract_base_path(&input.attrs, "interface", base);
    let polybox_path: syn::Path =
        syn::parse_str(&format!("{}::polybox", quote!(#base_path))).unwrap();

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

                let inner_type = extract_inner_payload_type(field_type)
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
                    Self::#variant_name(payload) => #polybox_path::BoxedPayload::new::<#inner_type>(payload),
                });

                from_impls.push(quote! {
                    impl #polybox_path::FromPayload<#inner_type> for #enum_name {
                        fn from_payload(payload: #polybox_path::Payload<#inner_type>) -> Self {
                            Self::#variant_name(payload)
                        }
                    }

                    impl #polybox_path::TryIntoPayload<#inner_type> for #enum_name {
                        fn try_into_payload(self) -> Result<#polybox_path::Payload<#inner_type>, Self> {
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
        impl #polybox_path::Interface for #enum_name {
            fn try_from_boxed_payload(payload: #polybox_path::BoxedPayload) -> Result<Self, #polybox_path::BoxedPayload> {
                #(#try_from_matches)*
                Err(payload)
            }

            // Could be added to improve performance, but would require unsafe transmute to avoid double downcasting.
            // fn try_into_payload<I: #base_path::Message>(self) -> Result<#base_path::Payload<I>, Self> {
            //     let id = std::any::TypeId::of::<I>();
            //     #(#try_into_matches)*
            //     Err(self)
            // }

            fn into_boxed_payload(self) -> #polybox_path::BoxedPayload {
                match self {
                    #(#into_matches)*
                }
            }
        }


        impl #polybox_path::Message for #enum_name {
            type Kind = #polybox_path::FireAndForget;
        }

        impl #polybox_path::type_sets::AsSet for #enum_name {
            type Set = #polybox_path::type_sets::Set![#(#inner_types),*];
        }

        impl #polybox_path::TryIntoPayload<#enum_name> for #enum_name {
            fn try_into_payload(self) -> Result<#polybox_path::Payload<#enum_name>, Self> {
                Ok(self)
            }
        }

        impl #polybox_path::FromPayload<#enum_name> for #enum_name {
            fn from_payload(payload: #polybox_path::Payload<#enum_name>) -> Self {
                payload
            }
        }

        #(#from_impls)*
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(ActorInterface, attributes(interface))]
pub fn derive_actor_interface(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;

    // Ensure we are working with an enum
    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => panic!("ActorInterface derive can only be used on enums"),
    };

    let base_path = extract_base_path(&input.attrs, "interface", "::zestors");

    let mut handle_matches = Vec::new();
    let mut inner_types = Vec::new();

    for variant in variants {
        let variant_name = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed[0].ty;

                let inner_type = extract_inner_payload_type(field_type)
                    .expect("ActorInterface variants must be of type Payload<T>");

                handle_matches.push(quote! {
                    Self::#variant_name(payload) => {
                        <T as #base_path::actor::HandleMessage<#inner_type>>::handle_message(actor, state, payload).await
                    }
                });
                inner_types.push(inner_type);
            }
            _ => panic!("ActorInterface derive only supports variants with a single unnamed field, e.g., A(Payload<T>)"),
        }
    }

    let expanded = quote! {
        impl<T> #base_path::actor::ActorInterface<T> for #enum_name
        where
            T: #base_path::actor::Actor + #( #base_path::actor::HandleMessage<#inner_types> + )*
        {
            async fn handle_with(self, state: &mut #base_path::state::ActorState<T>, actor: &mut T) -> Result<(), T::Error> {
                match self {
                    #(#handle_matches)*
                }
            }
        }
    };

    TokenStream::from(expanded)
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
#[proc_macro_derive(Message, attributes(msg))]
pub fn derive_message(input: TokenStream) -> TokenStream {
    _derive_message(input, "::polybox")
}

#[proc_macro_derive(MessageZestors, attributes(msg))]
pub fn derive_message_zestors(input: TokenStream) -> TokenStream {
    _derive_message(input, "::zestors")
}

#[derive(darling::FromAttributes)]
#[darling(attributes(msg))]
struct MessageAttrs {
    reply: Option<syn::Type>,
    path: Option<syn::Path>,
}

fn _derive_message(input: TokenStream, base: &str) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = match MessageAttrs::from_attributes(&input.attrs) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };
    let name = &input.ident;

    let base_path: syn::Path = attrs.path.unwrap_or_else(|| syn::parse_str(base).unwrap());
    let kind_type = if let Some(reply_type) = attrs.reply {
        quote!( #base_path::Request<#reply_type> )
    } else {
        quote!( #base_path::FireAndForget )
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

// #[proc_macro_derive(MessageMethodZestors, attributes(msg))]
// pub fn derive_message_method_zestors(input: TokenStream) -> TokenStream {
//     _derive_message_method(input, "::zestors")
// }

// #[proc_macro_derive(MessageMethod, attributes(msg))]
// pub fn derive_message_method(input: TokenStream) -> TokenStream {
//     _derive_message_method(input, "::polybox")
// }

// fn _derive_message_method(input: TokenStream, base: &str) -> TokenStream {
//     let input = parse_macro_input!(input as DeriveInput);
//     let name = &input.ident;
//     let base_path = extract_base_path(&input.attrs, "msg", base);
//     let trait_name = syn::Ident::new(&format!("Send{}", name), name.span());
//     let method_name = syn::Ident::new(
//         &format!("send_{}", name.to_string().to_lowercase()),
//         name.span(),
//     );
//     let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

//     let expanded = quote! {
//         pub trait #trait_name #impl_generics #where_clause {
//             fn #method_name(&self, msg: ) -> #base_path::SendFuture<'_, Result<#base_path::Output<#name #ty_generics>, #base_path::SendError<#name #ty_generics>>>;
//         }
//     };

//     TokenStream::from(expanded)
// }

fn extract_base_path(attrs: &[syn::Attribute], attr_name: &str, default_path: &str) -> syn::Path {
    let mut base_path: syn::Path = syn::parse_str(default_path).unwrap();

    for attr in attrs {
        if attr.path().is_ident(attr_name) {
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

    base_path
}

fn extract_inner_payload_type(ty: &Type) -> Option<&Type> {
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
