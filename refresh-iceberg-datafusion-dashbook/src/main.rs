use std::{fs, sync::Arc};

use dashbook_catalog::DashbookS3CatalogList;
use datafusion_iceberg::materialized_view::refresh_materialized_view;
use iceberg_rust::{
    catalog::{identifier::Identifier, tabular::Tabular, CatalogList},
    error::Error,
};
use serde::{Deserialize, Serialize};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config_json = fs::read_to_string("/tmp/config/refresh.json")?;
    let config: Config = serde_json::from_str(&config_json)?;

    let mut parts = config.identifier.split(".");
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

    let catalog_list = Arc::new(
        DashbookS3CatalogList::new(&config.access_token, &config.id_token)
            .await
            .unwrap(),
    );

    let catalog = catalog_list
        .catalog(&catalog_name)
        .await
        .ok_or(Error::NotFound(format!("Catalog"), format!("catalog")))?;

    let matview = if let Tabular::MaterializedView(matview) = catalog
        .load_table(&Identifier::try_new(&vec![
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
    pub access_token: String,
    pub id_token: String,
    pub identifier: String,
    pub bucket: Option<String>,
    pub branch: Option<String>,
}
