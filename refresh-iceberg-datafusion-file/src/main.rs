use std::{fs, sync::Arc};

use dashtool_common::ObjectStoreConfig;
use datafusion_iceberg::materialized_view::refresh_materialized_view;
use iceberg_file_catalog::FileCatalogList;
use iceberg_rust::{
    catalog::{bucket::ObjectStoreBuilder, identifier::Identifier, tabular::Tabular, CatalogList},
    error::Error,
};
use object_store::aws::AmazonS3Builder;
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config_json = fs::read_to_string("/tmp/config/refresh.json")?;
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

    let object_store = match &config.object_store {
        ObjectStoreConfig::Memory => ObjectStoreBuilder::memory(),
        ObjectStoreConfig::S3(s3_config) => {
            let mut builder = AmazonS3Builder::new()
                .with_region(&s3_config.aws_region)
                .with_bucket_name(
                    config
                        .bucket
                        .map(|x| x.trim_start_matches("s3://").to_owned())
                        .clone()
                        .expect("No bucket specified."),
                )
                .with_access_key_id(&s3_config.aws_access_key_id)
                .with_secret_access_key(s3_config.aws_secret_access_key.as_ref().ok_or(
                    Error::NotFound("Aws".to_owned(), "secret access key".to_owned()),
                )?);

            if let Some(endpoint) = &s3_config.aws_endpoint {
                builder = builder.with_endpoint(endpoint);
            }

            if let Some(allow_http) = &s3_config.aws_allow_http {
                builder = builder.with_allow_http(allow_http.parse().unwrap());
            }

            ObjectStoreBuilder::S3(builder)
        }
    };

    let catalog_list = Arc::new(FileCatalogList::new(&config.catalog_url, object_store).await?);

    let catalog = catalog_list.catalog(&catalog_name).ok_or(Error::NotFound(
        "Catalog".to_string(),
        "catalog".to_string(),
    ))?;

    let mut matview = if let Tabular::MaterializedView(matview) = catalog
        .load_tabular(&Identifier::try_new(
            &[namespace_name.to_owned(), table_name.to_owned()],
            None,
        )?)
        .await
        .expect("Failed to load tabular.")
    {
        matview
    } else {
        panic!("Not a materialized view.");
    };

    refresh_materialized_view(&mut matview, catalog_list, config.branch.as_deref())
        .await
        .expect("Failed to refresh materialized view.");

    Ok(())
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct Config {
    #[serde(flatten)]
    pub object_store: ObjectStoreConfig,
    pub catalog_url: String,
    pub identifier: String,
    pub bucket: Option<String>,
    pub branch: Option<String>,
}
