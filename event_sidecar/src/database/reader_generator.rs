#[macro_export]
macro_rules! database_reader_implementation {
    ($extended_type:ty,
     $row_type: ty,
     $query_materializer_expr:expr) => {
        use anyhow::Error;
        use async_trait::async_trait;
        use casper_types::FinalitySignature as FinSig;
        use metrics::db::observe_raw_data_size;
        use serde::Deserialize;
        use sqlx::{Executor, Row};
        use $crate::{
            database::{
                errors::{wrap_query_error, DbError},
                types::SseEnvelope,
            },
            sql::tables,
            types::{
                database::{
                    DatabaseReadError, DatabaseReader, TransactionAggregate, TransactionTypeId,
                },
                sse_events::*,
            },
        };

        #[async_trait]
        impl DatabaseReader for $extended_type {
            async fn get_latest_block(&self) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::block_added::create_get_latest_stmt()
                    .to_string($query_materializer_expr);
                let row = fetch_optional_with_error_check(db_connection, stmt).await?;

                parse_block_from_row(row)
            }

            async fn get_block_by_height(
                &self,
                height: u64,
            ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::block_added::create_get_by_height_stmt(height)
                    .to_string($query_materializer_expr);

                let row = fetch_optional_with_error_check(db_connection, stmt).await?;

                parse_block_from_row(row)
            }

            async fn get_block_by_hash(
                &self,
                hash: &str,
            ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::block_added::create_get_by_hash_stmt(hash.to_string())
                    .to_string($query_materializer_expr);

                db_connection
                    .fetch_optional(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(|maybe_row| match maybe_row {
                        None => Err(DatabaseReadError::NotFound),
                        Some(row) => parse_block_from_row(row),
                    })
            }

            async fn get_transaction_aggregate_by_identifier(
                &self,
                transaction_type: &TransactionTypeId,
                hash: &str,
            ) -> Result<TransactionAggregate, DatabaseReadError> {
                // We may return here with NotFound because if there's no accepted record then theoretically there should be no other records for the given hash.
                let transaction_accepted = self
                    .get_transaction_accepted_by_hash(transaction_type.clone(), hash)
                    .await?;

                // However we handle the Err case for TransactionProcessed explicitly as we don't want to return NotFound when we've got a TransactionAccepted to return
                match self
                    .get_transaction_processed_by_hash(transaction_type, hash)
                    .await
                {
                    Ok(transaction_processed) => Ok(TransactionAggregate {
                        transaction_hash: hash.to_string(),
                        transaction_accepted: Some(transaction_accepted),
                        transaction_processed: Some(transaction_processed),
                        transaction_expired: false,
                    }),
                    Err(err) => {
                        // If the error is anything other than NotFound return the error.
                        if !matches!(DatabaseReadError::NotFound, _err) {
                            return Err(err);
                        }
                        match self
                            .get_transaction_expired_by_hash(transaction_type, hash)
                            .await
                        {
                            Ok(_) => Ok(TransactionAggregate {
                                transaction_hash: hash.to_string(),
                                transaction_accepted: Some(transaction_accepted),
                                transaction_processed: None,
                                transaction_expired: true,
                            }),
                            Err(err) => {
                                // If the error is anything other than NotFound return the error.
                                if !matches!(DatabaseReadError::NotFound, _err) {
                                    return Err(err);
                                }
                                Ok(TransactionAggregate {
                                    transaction_hash: hash.to_string(),
                                    transaction_accepted: Some(transaction_accepted),
                                    transaction_processed: None,
                                    transaction_expired: false,
                                })
                            }
                        }
                    }
                }
            }

            async fn get_transaction_accepted_by_hash(
                &self,
                transaction_type: &TransactionTypeId,
                hash: &str,
            ) -> Result<SseEnvelope<TransactionAccepted>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::transaction_accepted::create_get_by_hash_stmt(
                    transaction_type.into(),
                    hash.to_string(),
                )
                .to_string($query_materializer_expr);

                db_connection
                    .fetch_optional(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(|maybe_row| match maybe_row {
                        None => Err(DatabaseReadError::NotFound),
                        Some(row) => {
                            let (raw, api_version, network_name) =
                                fetch_envelope_data_from_row(row, "TransactionAccepted")?;
                            let sse_event = deserialize_data::<TransactionAccepted>(&raw)
                                .map_err(wrap_query_error)?;
                            Ok(SseEnvelope::new(sse_event, api_version, network_name))
                        }
                    })
            }

            async fn get_transaction_processed_by_hash(
                &self,
                transaction_type: &TransactionTypeId,
                hash: &str,
            ) -> Result<SseEnvelope<TransactionProcessed>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::transaction_processed::create_get_by_hash_stmt(
                    transaction_type.into(),
                    hash.to_string(),
                )
                .to_string($query_materializer_expr);

                db_connection
                    .fetch_optional(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(|maybe_row| match maybe_row {
                        None => Err(DatabaseReadError::NotFound),
                        Some(row) => {
                            let (raw, api_version, network_name) =
                                fetch_envelope_data_from_row(row, "TransactionProcessed")?;
                            let sse_event = deserialize_data::<TransactionProcessed>(&raw)
                                .map_err(wrap_query_error)?;
                            Ok(SseEnvelope::new(sse_event, api_version, network_name))
                        }
                    })
            }

            async fn get_transaction_expired_by_hash(
                &self,
                transaction_type: &TransactionTypeId,
                hash: &str,
            ) -> Result<SseEnvelope<TransactionExpired>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::transaction_expired::create_get_by_hash_stmt(
                    transaction_type.into(),
                    hash.to_string(),
                )
                .to_string($query_materializer_expr);

                db_connection
                    .fetch_optional(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(|maybe_row| match maybe_row {
                        None => Err(DatabaseReadError::NotFound),
                        Some(row) => {
                            let (raw, api_version, network_name) =
                                fetch_envelope_data_from_row(row, "TransactionExpired")?;
                            let sse_event = deserialize_data::<TransactionExpired>(&raw)
                                .map_err(wrap_query_error)?;
                            Ok(SseEnvelope::new(sse_event, api_version, network_name))
                        }
                    })
            }

            async fn get_faults_by_public_key(
                &self,
                public_key: &str,
            ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt =
                    tables::fault::create_get_faults_by_public_key_stmt(public_key.to_string())
                        .to_string($query_materializer_expr);

                db_connection
                    .fetch_all(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(parse_faults_from_rows)
            }

            async fn get_faults_by_era(
                &self,
                era: u64,
            ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::fault::create_get_faults_by_era_stmt(era)
                    .to_string($query_materializer_expr);

                db_connection
                    .fetch_all(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(parse_faults_from_rows)
            }

            async fn get_finality_signatures_by_block(
                &self,
                block_hash: &str,
            ) -> Result<Vec<SseEnvelope<FinSig>>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt =
                    tables::finality_signature::create_get_finality_signatures_by_block_stmt(
                        block_hash.to_string(),
                    )
                    .to_string($query_materializer_expr);

                db_connection
                    .fetch_all(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(parse_finality_signatures_from_rows)
            }

            async fn get_step_by_era(
                &self,
                era: u64,
            ) -> Result<SseEnvelope<Step>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt =
                    tables::step::create_get_by_era_stmt(era).to_string($query_materializer_expr);

                db_connection
                    .fetch_optional(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(|maybe_row| match maybe_row {
                        None => Err(DatabaseReadError::NotFound),
                        Some(row) => {
                            let (raw, api_version, network_name) =
                                fetch_envelope_data_from_row(row, "Step")?;
                            let sse_event =
                                deserialize_data::<Step>(&raw).map_err(wrap_query_error)?;
                            Ok(SseEnvelope::new(sse_event, api_version, network_name))
                        }
                    })
            }

            async fn get_number_of_events(&self) -> Result<u64, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::event_log::count().to_string($query_materializer_expr);

                db_connection
                    .fetch_one(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(|row| {
                        row.try_get::<i64, _>(0)
                            .map(|i| i as u64) //this should never be negative
                            .map_err(|sqlx_error| wrap_query_error(sqlx_error.into()))
                    })
            }

            async fn get_newest_migration_version(
                &self,
            ) -> Result<Option<(u32, bool)>, DatabaseReadError> {
                let db_connection = &self.connection_pool;

                let stmt = tables::migration::create_get_newest_migration_stmt()
                    .to_string($query_materializer_expr);

                db_connection
                    .fetch_optional(stmt.as_str())
                    .await
                    .map_err(|sql_err| DatabaseReadError::Unhandled(Error::from(sql_err)))
                    .and_then(parse_migration_row)
            }
        }

        fn deserialize_data<'de, T: Deserialize<'de>>(data: &'de str) -> Result<T, DbError> {
            serde_json::from_str::<T>(data).map_err(DbError::SerdeJson)
        }

        fn parse_block_from_row(
            row: $row_type,
        ) -> Result<SseEnvelope<BlockAdded>, DatabaseReadError> {
            let (raw_data, api_version, network_name) =
                fetch_envelope_data_from_row(row, "BlockAdded")?;
            let sse_event = deserialize_data::<BlockAdded>(&raw_data).map_err(wrap_query_error)?;
            Ok(SseEnvelope::new(sse_event, api_version, network_name))
        }

        fn parse_finality_signatures_from_rows(
            rows: Vec<$row_type>,
        ) -> Result<Vec<SseEnvelope<FinSig>>, DatabaseReadError> {
            let mut finality_signatures = Vec::new();
            for row in rows {
                let (raw, api_version, network_name) =
                    fetch_envelope_data_from_row(row, "FinalitySignature")?;
                let sse_event =
                    deserialize_data::<FinalitySignature>(&raw).map_err(wrap_query_error)?;
                finality_signatures.push(SseEnvelope::new(
                    sse_event.inner(),
                    api_version,
                    network_name,
                ));
            }

            if finality_signatures.is_empty() {
                return Err(DatabaseReadError::NotFound);
            }
            Ok(finality_signatures)
        }

        fn parse_faults_from_rows(
            rows: Vec<$row_type>,
        ) -> Result<Vec<SseEnvelope<Fault>>, DatabaseReadError> {
            let mut faults = Vec::new();
            for row in rows {
                let (raw, api_version, network_name) = fetch_envelope_data_from_row(row, "Fault")?;
                let sse_event = deserialize_data::<Fault>(&raw).map_err(wrap_query_error)?;
                faults.push(SseEnvelope::new(sse_event, api_version, network_name));
            }

            if faults.is_empty() {
                return Err(DatabaseReadError::NotFound);
            }
            Ok(faults)
        }

        fn fetch_envelope_data_from_row(
            row: $row_type,
            message_type: &str,
        ) -> Result<(String, String, String), DatabaseReadError> {
            let raw_data = row
                .try_get::<String, &str>("raw")
                .map_err(|sqlx_err| wrap_query_error(sqlx_err.into()))?;
            observe_raw_data_size(message_type, raw_data.len());
            let api_version = row
                .try_get::<String, &str>("api_version")
                .map_err(|sqlx_err| wrap_query_error(sqlx_err.into()))?;
            let network_name = row
                .try_get::<String, &str>("network_name")
                .map_err(|sqlx_err| wrap_query_error(sqlx_err.into()))?;
            Ok((raw_data, api_version, network_name))
        }
    };
}
