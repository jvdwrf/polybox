//! Macros for the `zestors` crate.

extern crate proc_macro;
use darling::FromAttributes;
use proc_macro::TokenStream;
use quote::quote;
use syn::{parse_macro_input, Data, DeriveInput, Fields, Type};

/// Derives the `Interface` trait for an enum, allowing it to be used as a message
/// interface in the Polybox framework.
///
/// Under the hood, this macro generates implementations for the `Interface`, `Message`, and `TypeSet` traits,
/// as well as `FromEnvelope` and `TryIntoEnvelope` for each variant of the enum.
///
/// The macro expects the enum variants to be of the form `Variant(Envelope<T>)`,
/// where `T` is a type that implements the `Message` trait.
#[proc_macro_derive(Interface, attributes(interface))]
pub fn derive_interface_polybox(input: TokenStream) -> TokenStream {
    derive_interface(input, "::zestors")
}

#[derive(darling::FromAttributes)]
#[darling(attributes(interface))]
struct InterfaceAttrs {
    path: Option<syn::Path>,
}

fn derive_interface(input: TokenStream, base: &str) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let attrs = match InterfaceAttrs::from_attributes(&input.attrs) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };
    let enum_name = &input.ident;

    let base_path: syn::Path = attrs.path.unwrap_or_else(|| syn::parse_str(base).unwrap());
    let msg_path: syn::Path =
        syn::parse_str(&format!("{}::messaging", quote!(#base_path))).unwrap();

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

                let inner_type = extract_inner_envelope_type(field_type)
                    .expect("Interface variants must be of type Envelope<T>");

                inner_types.push(inner_type);

                try_from_matches.push(quote! {
                    let envelope = match envelope.downcast::<#inner_type>() {
                        Ok(envelope) => return Ok(Self::#variant_name(envelope)),
                        Err(envelope) => envelope,
                    };
                });

                try_into_matches.push(quote! {
                    if id == std::any::TypeId::of::<#inner_type>() {
                        if let Self::#variant_name(envelope) = self {
                            // SAFETY: Verified type matches dynamic I parameter.
                            let converted = unsafe {
                                std::mem::transmute_copy::<#base_path::Envelope<#inner_type>, #base_path::Envelope<I>>(&envelope)
                            };
                            std::mem::forget(envelope);
                            return Ok(converted);
                        }
                    }
                });

                into_matches.push(quote! {
                    Self::#variant_name(envelope) => #msg_path::BoxedEnvelope::new::<#inner_type>(envelope),
                });

                from_impls.push(quote! {
                    impl #msg_path::FromEnvelope<#inner_type> for #enum_name {
                        fn from_envelope(envelope: #msg_path::Envelope<#inner_type>) -> Self {
                            Self::#variant_name(envelope)
                        }
                    }

                    impl #msg_path::TryIntoEnvelope<#inner_type> for #enum_name {
                        fn try_into_envelope(self) -> Result<#msg_path::Envelope<#inner_type>, Self> {
                            if let #enum_name::#variant_name(envelope) = self {
                                Ok(envelope)
                            } else {
                                Err(self)
                            }
                        }
                    }
                });
            }
            _ => panic!("Interface derive only supports variants with a single unnamed field, e.g., A(Envelope<T>)"),
        }
    }

    let expanded = quote! {
        impl #msg_path::Interface for #enum_name {
            fn try_from_boxed_envelope(envelope: #msg_path::BoxedEnvelope) -> Result<Self, #msg_path::BoxedEnvelope> {
                #(#try_from_matches)*
                Err(envelope)
            }

            // Could be added to improve performance, but would require unsafe transmute to avoid double downcasting.
            // fn try_into_envelope<I: #base_path::Message>(self) -> Result<#base_path::Envelope<I>, Self> {
            //     let id = std::any::TypeId::of::<I>();
            //     #(#try_into_matches)*
            //     Err(self)
            // }

            fn into_boxed_envelope(self) -> #msg_path::BoxedEnvelope {
                match self {
                    #(#into_matches)*
                }
            }

            type Set = #msg_path::type_sets::Set![#(#inner_types),*];
        }


        impl #msg_path::Message for #enum_name {
            type Receipt = ();
            type Outcome = ();
            type Resolver = ();
        }

        // impl #polybox_path::type_sets::TypeSet for #enum_name {
        //     type Set = #polybox_path::type_sets::Set![#(#inner_types),*];
        // }

        impl #msg_path::TryIntoEnvelope<#enum_name> for #enum_name {
            fn try_into_envelope(self) -> Result<#msg_path::Envelope<#enum_name>, Self> {
                Ok(#msg_path::Envelope::new(self, ()))
            }
        }

        impl #msg_path::FromEnvelope<#enum_name> for #enum_name {
            fn from_envelope(envelope: #msg_path::Envelope<#enum_name>) -> Self {
                envelope.msg
            }
        }

        #(#from_impls)*
    };

    TokenStream::from(expanded)
}

#[proc_macro_derive(HandlerInterface, attributes(interface))]
pub fn derive_actor_interface(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    let enum_name = &input.ident;
    let attrs = match InterfaceAttrs::from_attributes(&input.attrs) {
        Ok(attrs) => attrs,
        Err(err) => return err.write_errors().into(),
    };

    // Ensure we are working with an enum
    let variants = match &input.data {
        Data::Enum(data_enum) => &data_enum.variants,
        _ => panic!("HandlerInterface derive can only be used on enums"),
    };

    let base_path = attrs
        .path
        .unwrap_or_else(|| syn::parse_str("::zestors").unwrap());

    let mut handle_matches = Vec::new();
    let mut inner_types = Vec::new();

    for variant in variants {
        let variant_name = &variant.ident;

        match &variant.fields {
            Fields::Unnamed(fields) if fields.unnamed.len() == 1 => {
                let field_type = &fields.unnamed[0].ty;

                let inner_type = extract_inner_envelope_type(field_type)
                    .expect("HandlerInterface variants must be of type Envelope<T>");

                handle_matches.push(quote! {
                    Self::#variant_name(envelope) => {
                        <T as #base_path::handler::Handle<#inner_type>>::handle(actor, state, envelope).await
                    }
                });
                inner_types.push(inner_type);
            }
            _ => panic!("HandlerInterface derive only supports variants with a single unnamed field, e.g., A(Envelope<T>)"),
        }
    }

    let expanded = quote! {
        impl<T> #base_path::handler::HandlerInterface<T> for #enum_name
        where
            T: #base_path::handler::Handler + #( #base_path::handler::Handle<#inner_types> + )*
        {
            async fn handle_with(self, state: &mut #base_path::handler::HandlerState<T>, actor: &mut T) -> Result<(), T::Error> {
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
/// struct MessageWithOutcome;
/// ```
#[proc_macro_derive(Message, attributes(msg))]
pub fn derive_message(input: TokenStream) -> TokenStream {
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
    let (impl_generics, ty_generics, where_clause) = input.generics.split_for_impl();

    let expanded = if let Some(reply_type) = attrs.reply {
        quote!(
            impl #impl_generics #base_path::messaging::Message for #name #ty_generics #where_clause
            {
                type Receipt = #base_path::messaging::oneshot::Rx<#reply_type>;
                type Outcome = #reply_type;
                type Resolver = #base_path::messaging::oneshot::Tx<#reply_type>;
            }
        )
    } else {
        quote!(
            impl #impl_generics #base_path::messaging::Message for #name #ty_generics #where_clause
            {
                type Receipt = ();
                type Outcome = ();
                type Resolver = ();
            }
        )
    };

    TokenStream::from(expanded)
}

fn extract_inner_envelope_type(ty: &Type) -> Option<&Type> {
    if let Type::Path(type_path) = ty {
        let segment = type_path.path.segments.last()?;
        if segment.ident == "Envelope" {
            if let syn::PathArguments::AngleBracketed(args) = &segment.arguments {
                if let Some(syn::GenericArgument::Type(inner_ty)) = args.args.first() {
                    return Some(inner_ty);
                }
            }
        }
    }
    None
}
