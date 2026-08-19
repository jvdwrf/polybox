use reqwest::StatusCode;
use rootcause::report;
use zestors::supervision::SupervisionTree;

pub struct Client {
    client: reqwest::Client,
    base_url: reqwest::Url,
}

impl Client {
    pub fn new(base_url: impl AsRef<str>) -> rootcause::Result<Self> {
        Ok(Self {
            client: reqwest::Client::new(),
            base_url: reqwest::Url::parse(base_url.as_ref())?,
        })
    }

    pub async fn get_tree(&self) -> rootcause::Result<Option<SupervisionTree>> {
        let response = self.client.get(self.base_url.join("/tree")?).send().await?;

        match response.status() {
            StatusCode::OK => {
                let supervision_tree = response.json::<SupervisionTree>().await?;
                Ok(Some(supervision_tree))
            }
            StatusCode::NOT_FOUND => Ok(None),
            status => {
                let error_text = response.text().await?;

                Err(report!(
                    "Unexpected status: {}. Response text: {}",
                    status,
                    error_text
                ))
            }
        }
    }
}
