use dashbook_catalog::{get_catalog, get_role};
use datafusion_iceberg::materialized_view::refresh_materialized_view;
use iceberg_rust::catalog::{identifier::Identifier, tabular::Tabular};

#[tokio::main]
async fn main() {
    let access_token =
        std::env::var("ACCESS_TOKEN").expect("Environment variable ACCESS_TOKEN not set.");
    let id_token = std::env::var("ID_TOKEN").expect("Environment variable ID_TOKEN not set.");
    let catalog = std::env::var("CATALOG").expect("Environment variable CATALOG not set.");
    let matview_name =
        std::env::var("MATVIEW_NAME").expect("Environment variable MATVIEW_NAME not set.");
    let branch = std::env::var("BRANCH").ok();

    let identifier =
        Identifier::parse(&matview_name).expect("Failed to parse materialized view identifier.");

    let catalog_name = catalog.split("/").last().expect("Empty catalog");
    let table_namespace = identifier.namespace().to_string();
    let table_name = identifier.name();

    let role = get_role(
        &access_token,
        &catalog_name,
        &table_namespace,
        &table_name,
        "write",
    )
    .await
    .expect("Failed to get permissions for table.");

    let catalog = get_catalog(
        &catalog,
        &access_token,
        &id_token,
        &table_namespace,
        &table_name,
        &role,
    )
    .await
    .expect("Failed to load catalog.");

    let matview = if let Tabular::MaterializedView(matview) = catalog
        .load_table(&identifier)
        .await
        .expect("Failed to load tabular.")
    {
        matview
    } else {
        panic!("Not a materialized view.");
    };

    refresh_materialized_view(&matview, branch.as_deref())
        .await
        .expect("Failed to refresh materialized view.");
}
