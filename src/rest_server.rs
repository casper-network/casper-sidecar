mod errors;
mod filters;
mod handlers;
#[cfg(test)]
mod tests;

use std::path::PathBuf;

use anyhow::Error;

use crate::{sqlite_database::SqliteDatabase, utils::resolve_address};

pub async fn run_server(db_path: PathBuf, ip_address: String, port: u16) -> Result<(), Error> {
    let db = SqliteDatabase::new_read_only(&db_path)?;

    let api = filters::combined_filters(db);

    let address = format!("{}:{}", ip_address, port);
    let socket_address = resolve_address(&address)?;

    warp::serve(api).run(socket_address).await;

    Ok(())
}
