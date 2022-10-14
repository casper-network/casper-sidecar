mod errors;
mod filters;
mod handlers;
#[cfg(test)]
mod tests;

use anyhow::Error;

use crate::{sqlite_database::SqliteDatabase, utils::resolve_address};

pub async fn run_server(port: u16, sqlite_db: SqliteDatabase) -> Result<(), Error> {
    let api = filters::combined_filters(sqlite_db);

    let address = format!("0.0.0.0:{}", port);
    let socket_address = resolve_address(&address)?;

    // todo add QPS limit - make configurable - default 50
    // https://github.com/seanmonstar/reqwest/issues/491
    warp::serve(api).run(socket_address).await;

    Ok(())
}
