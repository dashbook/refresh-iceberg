use std::{fs, sync::Arc};

use dashtool_common::ObjectStoreConfig;
use datafusion_iceberg::materialized_view::refresh_materialized_view;
use iceberg_catalog_sql::SqlCatalogList;
use iceberg_rust::{
    catalog::{identifier::Identifier, tabular::Tabular, CatalogList},
    error::Error,
};
use object_store::{aws::AmazonS3Builder, memory::InMemory, ObjectStore};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config_json = fs::read_to_string("/tmp/refresh.json")?;
    let config: Config = serde_json::from_str(&config_json)?;

    let mut parts = config.identifier.split('.');
    let catalog_name = parts
        .next()
        .ok_or(Error::InvalidFormat("Identifier".to_string()))?
        .to_owned();
    let namespace_name = parts
        .next()
        .ok_or(Error::InvalidFormat(format!(
            "Identifier {} has only one part",
            &config.identifier
        )))?
        .to_owned();
    let table_name = parts
        .next()
        .ok_or(Error::InvalidFormat(format!(
            "Identifier {} has only two parts",
            &config.identifier
        )))?
        .to_owned();

    let object_store: Arc<dyn ObjectStore> = match &config.object_store {
        ObjectStoreConfig::Memory => Arc::new(InMemory::new()),
        ObjectStoreConfig::S3(s3_config) => {
            let builder = AmazonS3Builder::new()
                .with_region(&s3_config.aws_region)
                .with_bucket_name(config.bucket.clone().expect("No bucket specified."))
                .with_access_key_id(&s3_config.aws_access_key_id);

            let builder = if let Some(aws_secret_access_key) = &s3_config.aws_secret_access_key {
                builder.with_secret_access_key(aws_secret_access_key)
            } else {
                builder
            };
            Arc::new(builder.build()?)
        }
    };

    let catalog_list = Arc::new(SqlCatalogList::new(&config.url, object_store).await?);

    let catalog = catalog_list
        .catalog(&catalog_name)
        .await
        .ok_or(Error::NotFound(
            "Catalog".to_string(),
            "catalog".to_string(),
        ))?;

    let matview = if let Tabular::MaterializedView(matview) = catalog
        .load_table(&Identifier::try_new(&[
            namespace_name.to_owned(),
            table_name.to_owned(),
        ])?)
        .await
        .expect("Failed to load tabular.")
    {
        matview
    } else {
        panic!("Not a materialized view.");
    };

    refresh_materialized_view(&matview, catalog_list, config.branch.as_deref())
        .await
        .expect("Failed to refresh materialized view.");

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(flatten)]
    pub object_store: ObjectStoreConfig,
    pub url: String,
    pub identifier: String,
    pub bucket: Option<String>,
    pub branch: Option<String>,
}
