use proc_macro2::TokenStream;
use syn::{parse2, Error, Item, Type};

pub fn derive_handler(item: TokenStream) -> Result<TokenStream, Error> {
    let (ident, generics, attrs) = match parse2::<Item>(item)? {
        Item::Enum(item) => (item.ident, item.generics, item.attrs),
        Item::Struct(item) => (item.ident, item.generics, item.attrs),
        Item::Union(item) => (item.ident, item.generics, item.attrs),
        item => Err(Error::new_spanned(item, "Must be an Enum, Struct or Union"))?,
    };

    let (impl_generics, ty_generics, where_clause) = generics.split_for_impl();

    let Some(state) = attrs.iter().find(|attr| attr.path.is_ident("state")) else {
        return Err(Error::new_spanned(ident, "No #[state(..)] attribute found"))?
    };
    let state = state.parse_args::<Type>()?;

    Ok(quote! {
        #[::polybox::export::async_trait]
        impl #impl_generics ::polybox::handler::Handler for #ident #ty_generics #where_clause
        {
            type State = #state;
            type Exception = ::polybox::export::Report;
            type Stop = ();
            type Exit = Result<Self, ::polybox::export::Report>;

            async fn handle_exit(
                self,
                state: &mut Self::State,
                reason: Result<Self::Stop, Self::Exception>,
            ) -> ::polybox::handler::ExitFlow<Self> {
                match reason {
                    Ok(()) => ::polybox::handler::ExitFlow::Exit(Ok(self)),
                    Err(exception) => ::polybox::handler::ExitFlow::Exit(Err(exception))
                }
            }

            async fn handle_event(
                &mut self,
                state: &mut Self::State,
                event: ::polybox::handler::Event
            ) -> ::polybox::handler::HandlerResult<Self> {
                match event {
                    ::polybox::handler::Event::Halted => {
                        state.close();
                        Ok(::polybox::handler::Flow::Continue)
                    }
                    ::polybox::handler::Event::ClosedAndEmpty => Ok(::polybox::handler::Flow::Stop(())),
                    ::polybox::handler::Event::Dead => Ok(::polybox::handler::Flow::Stop(())),
                }
            }
        }
    })
}
