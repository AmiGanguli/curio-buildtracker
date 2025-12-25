use aws_sdk_dynamodb::{Client, config::Region};
use aws_config::meta::region::RegionProviderChain;
use aws_sdk_dynamodb::types::AttributeValue;
use std::env;

pub mod dependency_graph;
pub use dependency_graph::DependencyGraph;

pub mod storage;
pub use storage::ArtifactStorage;

pub mod config;
pub use config::CurioConfig;

pub struct CurioDbClient {
    client: Client,
    table_name: String,
}

impl CurioDbClient {
    pub async fn new(table_name: String) -> Self {
        let region_provider = RegionProviderChain::default_provider().or_else(Region::new("us-east-1"));
        let shared_config = aws_config::from_env().region(region_provider).load().await;

        let client = if let Ok(endpoint) = env::var("DYNAMODB_ENDPOINT") {
            tracing::info!("Using local DynamoDB endpoint: {}", endpoint);
            let conf = aws_sdk_dynamodb::config::Builder::from(&shared_config)
                .endpoint_url(endpoint)
                .build();
            Client::from_conf(conf)
        } else {
            Client::new(&shared_config)
        };

        Self { client, table_name }
    }

    pub async fn save_record(&self, id: &str, data: &str) -> Result<(), aws_sdk_dynamodb::Error> {
        self.client.put_item()
            .table_name(&self.table_name)
            .item("id", AttributeValue::S(id.to_string()))
            .item("data", AttributeValue::S(data.to_string()))
            .send()
            .await?;
        Ok(())
    }

    pub async fn get_record(&self, id: &str) -> Result<Option<String>, aws_sdk_dynamodb::Error> {
        let resp = self.client.get_item()
            .table_name(&self.table_name)
            .key("id", AttributeValue::S(id.to_string()))
            .send()
            .await?;
        
        if let Some(item) = resp.item {
            if let Some(data) = item.get("data") {
                if let Ok(s) = data.as_s() {
                    return Ok(Some(s.clone()));
                }
            }
        }
        Ok(None)
    }
}
