impl crate::client::Client {
    /// GET /tree
    #[cfg_attr(
        feature = "tracing",
        ::tracing::instrument(
            skip_all,
            fields(
                otel.name = "GET /tree",
                otel.kind = "client",
                url.template = "/tree",
                http.request.method = "GET",
                server.address,
                server.port,
                url.full,
                http.response.status_code,
                error.type
            )
        )
    )]
    pub async fn get_tree(
        &self,
        query: &parameters::GetTreeQuery,
    ) -> Result<crate::types::SupervisionTree, crate::error::Error> {
        let result: Result<_, crate::error::Error> = async move {
            let url = {
                let mut url = self.base_url.clone();
                url.path_segments_mut()
                    .map_err(|()| {
                        ::ploidy_util::url::PathAndQueryError::UrlCannotBeABase
                    })?
                    .pop_if_empty()
                    .push("tree");
                let url = ::ploidy_util::serde::Serialize::serialize(
                    query,
                    ::ploidy_util::QuerySerializer::new(
                        url,
                        parameters::GetTreeQuery::STYLES,
                    ),
                )?;
                #[cfg(feature = "tracing")]
                {
                    ::tracing::record_all!(
                        ::tracing::Span::current(), server.address = url.host_str(),
                        server.port = url.port_or_known_default(), url.full = url
                        .as_str(),
                    );
                }
                url
            };
            let request = {
                let request = self.client.get(url).headers(self.headers.clone());
                #[cfg(feature = "trace-context")]
                let request = ::ploidy_util::trace::propagate(
                    ::tracing::Span::current(),
                    request,
                );
                request
            };
            let response = request.send().await?;
            #[cfg(feature = "tracing")]
            {
                ::tracing::record_all!(
                    ::tracing::Span::current(), http.response.status_code = response
                    .status().as_u16()
                );
            }
            let response = response.error_for_status()?;
            let body = response.bytes().await?;
            let deserializer = &mut ::ploidy_util::serde_json::Deserializer::from_slice(
                &body,
            );
            let result = ::ploidy_util::serde_path_to_error::deserialize(deserializer)?;
            Ok(result)
        }
            .await;
        #[cfg(feature = "tracing")]
        if let Err(err) = &result {
            ::tracing::record_all!(
                ::tracing::Span::current(), error. type = % err.category(),
            );
        }
        result
    }
}
pub mod parameters {
    mod get_tree_query {
        #[derive(
            Debug,
            Clone,
            PartialEq,
            Eq,
            Hash,
            Default,
            ::ploidy_util::serde::Serialize,
            ::ploidy_util::serde::Deserialize
        )]
        #[serde(crate = "::ploidy_util::serde")]
        pub struct GetTreeQuery {
            pub include_debug: ::std::option::Option<bool>,
            pub pid: ::std::option::Option<crate::types::String>,
        }
        impl GetTreeQuery {
            pub const STYLES: &[(&str, ::ploidy_util::QueryStyle)] = &[];
        }
    }
    pub use get_tree_query::*;
}
