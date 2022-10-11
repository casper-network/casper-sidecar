mod errors;
mod filters;
mod handlers;
#[cfg(test)]
mod tests;

use anyhow::Error;

use crate::{sqlite_database::SqliteDatabase, utils::resolve_address};

pub async fn run_server(
    ip_address: String,
    port: u16,
    sqlite_db: SqliteDatabase,
) -> Result<(), Error> {
    let api = filters::combined_filters(sqlite_db);

    let address = format!("{}:{}", ip_address, port);
    let socket_address = resolve_address(&address)?;

    warp::serve(api).run(socket_address).await;

    Ok(())
}
