mod errors;
mod filters;
mod handlers;
#[cfg(test)]
mod tests;

use anyhow::Error;

use crate::{sqlite_database::SqliteDatabase, utils::resolve_address};

const LOOPBACK: &str = "127.0.0.1";

pub async fn run_server(port: u16, sqlite_database: SqliteDatabase) -> Result<(), Error> {
    let api = filters::combined_filters(sqlite_database);

    let address = format!("{}:{}", LOOPBACK, port);
    let socket_address = resolve_address(&address)?;

    warp::serve(api).run(socket_address).await;

    Ok(())
}
