use std::time::Duration;

use super::database::{DatabaseWriteError, DatabaseWriter};
use tokio::time::sleep;
use tracing::error;
pub async fn start_assembling_aggregates<T: 'static + DatabaseWriter + Clone + Send + Sync>(
    writer: T,
) {
    let _ = tokio::spawn(async move {
        let db_clone = writer.clone();
        loop {
            let _ = assemble_aggregates(db_clone.clone()).await;
        }
    });
}

async fn assemble_aggregates<T: DatabaseWriter>(
    writer: T,
) -> Result<(), DatabaseWriteError> {
    match writer.update_pending_deploy_aggregates().await {
        Err(e) => {
            error!(
                "Error when processing assemble_aggregates: {:?}",
                e
            );
            sleep(Duration::from_secs(5)).await;
            Err(e)
        }
        Ok(0) => {
            //We are currently up-to-date, all the aggregates are assembled. No need to push another reassembly right away.
            sleep(Duration::from_secs(30)).await;
            Ok(())
        }
        Ok(_) => {
            //No timeout, we need to do more batches
            sleep(Duration::from_secs(1)).await;
            Ok(())
        }
    }
}
