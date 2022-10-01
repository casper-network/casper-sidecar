mod errors;
mod filters;
mod handlers;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use anyhow::Error;

use crate::{sqlite_database::SqliteDatabase, utils::resolve_address};

pub async fn run_server(
    ip_address: String,
    port: u16,
    path_to_database: PathBuf,
    max_read_connections: u32,
) -> Result<(), Error> {
    let db = SqliteDatabase::new_read_only(&path_to_database, max_read_connections)?;

    let api = filters::combined_filters(db);

    let address = format!("{}:{}", ip_address, port);
    let socket_address = resolve_address(&address)?;

    warp::serve(api).run(socket_address).await;

    Ok(())
}
