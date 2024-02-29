use crate::{
    sql::tables,
    types::{
        database::{DatabaseReader, DatabaseWriteError, DatabaseWriter, TransactionTypeId},
        sse_events::*,
    },
};
use casper_types::{testing::TestRng, AsymmetricType, EraId};
use rand::Rng;
use sea_query::{Query, SelectStatement};

pub async fn should_save_and_retrieve_block_added<DB: DatabaseReader + DatabaseWriter>(db: DB) {
    let mut test_rng = TestRng::new();
    let block_added = BlockAdded::random(&mut test_rng);

    db.save_block_added(
        block_added.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving block_added");

    db.get_latest_block()
        .await
        .expect("Error getting latest block_added");

    db.get_block_by_hash(&block_added.hex_encoded_hash())
        .await
        .expect("Error getting block_added by hash");

    db.get_block_by_height(block_added.get_height())
        .await
        .expect("Error getting block_added by height");
}

pub async fn should_save_and_retrieve_transaction_accepted<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();

    let transaction_accepted = TransactionAccepted::random(&mut test_rng);
    let transaction_type_id = match transaction_accepted.transaction_type_id() {
        crate::sql::tables::transaction_type::TransactionTypeId::Deploy => {
            TransactionTypeId::Deploy
        }
        crate::sql::tables::transaction_type::TransactionTypeId::Version1 => {
            TransactionTypeId::Version1
        }
    };
    db.save_transaction_accepted(
        transaction_accepted.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_accepted");

    db.get_transaction_accepted_by_hash(
        &transaction_type_id,
        &transaction_accepted.hex_encoded_hash(),
    )
    .await
    .expect("Error getting transaction_accepted by hash");
}

pub async fn should_save_and_retrieve_transaction_processed<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_processed = TransactionProcessed::random(&mut test_rng, None);
    let transaction_type_id = match transaction_processed.transaction_type_id() {
        crate::sql::tables::transaction_type::TransactionTypeId::Deploy => {
            TransactionTypeId::Deploy
        }
        crate::sql::tables::transaction_type::TransactionTypeId::Version1 => {
            TransactionTypeId::Version1
        }
    };
    db.save_transaction_processed(
        transaction_processed.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_processed");

    db.get_transaction_processed_by_hash(
        &transaction_type_id,
        &transaction_processed.hex_encoded_hash(),
    )
    .await
    .expect("Error getting transaction_processed by hash");
}

pub async fn should_save_and_retrieve_transaction_expired<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_expired = TransactionExpired::random(&mut test_rng, None);
    let transaction_type_id = match transaction_expired.transaction_type_id() {
        crate::sql::tables::transaction_type::TransactionTypeId::Deploy => {
            TransactionTypeId::Deploy
        }
        crate::sql::tables::transaction_type::TransactionTypeId::Version1 => {
            TransactionTypeId::Version1
        }
    };
    db.save_transaction_expired(
        transaction_expired.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_expired");

    db.get_transaction_expired_by_hash(
        &transaction_type_id,
        &transaction_expired.hex_encoded_hash(),
    )
    .await
    .expect("Error getting transaction_expired by hash");
}

pub async fn should_retrieve_transaction_aggregate_of_accepted<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();

    let transaction_accepted = TransactionAccepted::random(&mut test_rng);
    let transaction_type_id = match transaction_accepted.transaction_type_id() {
        crate::sql::tables::transaction_type::TransactionTypeId::Deploy => {
            TransactionTypeId::Deploy
        }
        crate::sql::tables::transaction_type::TransactionTypeId::Version1 => {
            TransactionTypeId::Version1
        }
    };

    db.save_transaction_accepted(
        transaction_accepted.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_accepted");

    db.get_transaction_aggregate_by_identifier(
        &transaction_type_id,
        &transaction_accepted.hex_encoded_hash(),
    )
    .await
    .expect("Error getting transaction aggregate by hash");
}

pub async fn should_retrieve_transaction_aggregate_of_processed<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_accepted = TransactionAccepted::random(&mut test_rng);
    let transaction_processed =
        TransactionProcessed::random(&mut test_rng, Some(transaction_accepted.transaction_hash()));
    let transaction_type_id = match transaction_accepted.transaction_type_id() {
        crate::sql::tables::transaction_type::TransactionTypeId::Deploy => {
            TransactionTypeId::Deploy
        }
        crate::sql::tables::transaction_type::TransactionTypeId::Version1 => {
            TransactionTypeId::Version1
        }
    };

    db.save_transaction_accepted(
        transaction_accepted.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_accepted");

    db.save_transaction_processed(
        transaction_processed,
        2,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_processed");

    db.get_transaction_aggregate_by_identifier(
        &transaction_type_id,
        &transaction_accepted.hex_encoded_hash(),
    )
    .await
    .expect("Error getting transaction aggregate by hash");
}

pub async fn should_retrieve_transaction_aggregate_of_expired<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_accepted = TransactionAccepted::random(&mut test_rng);
    let transaction_expired =
        TransactionExpired::random(&mut test_rng, Some(transaction_accepted.transaction_hash()));
    let transaction_type_id = match transaction_accepted.transaction_type_id() {
        crate::sql::tables::transaction_type::TransactionTypeId::Deploy => {
            TransactionTypeId::Deploy
        }
        crate::sql::tables::transaction_type::TransactionTypeId::Version1 => {
            TransactionTypeId::Version1
        }
    };

    db.save_transaction_accepted(
        transaction_accepted.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_accepted");

    db.save_transaction_expired(
        transaction_expired,
        2,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving transaction_expired");

    db.get_transaction_aggregate_by_identifier(
        &transaction_type_id,
        &transaction_accepted.hex_encoded_hash(),
    )
    .await
    .expect("Error getting transaction aggregate by hash");
}

pub async fn should_save_and_retrieve_fault<DB: DatabaseReader + DatabaseWriter>(db: DB) {
    let mut test_rng = TestRng::new();
    let fault = Fault::random(&mut test_rng);

    db.save_fault(
        fault.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving fault");

    db.get_faults_by_era(fault.era_id.value())
        .await
        .expect("Error getting faults by era");

    db.get_faults_by_public_key(&fault.public_key.to_hex())
        .await
        .expect("Error getting faults by public key");
}

pub async fn should_save_and_retrieve_fault_with_a_u64max<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let mut fault = Fault::random(&mut test_rng);
    fault.era_id = EraId::new(u64::MAX);

    db.save_fault(
        fault.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving fault with a u64::MAX era id");

    let faults = db
        .get_faults_by_era(u64::MAX)
        .await
        .expect("Error getting faults by era with u64::MAX era id");

    assert_eq!(faults[0].era_id.value(), u64::MAX);

    let faults = db
        .get_faults_by_public_key(&fault.public_key.to_hex())
        .await
        .expect("Error getting faults by public key with u64::MAX era id");

    assert_eq!(faults[0].era_id.value(), u64::MAX);
}

pub async fn should_save_and_retrieve_finality_signature<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let finality_signature = FinalitySignature::random(&mut test_rng);

    db.save_finality_signature(
        finality_signature.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving finality_signature");

    db.get_finality_signatures_by_block(&finality_signature.hex_encoded_block_hash())
        .await
        .expect("Error getting finality signatures by block_hash");
}

pub async fn should_save_and_retrieve_step<DB: DatabaseReader + DatabaseWriter>(db: DB) {
    let mut test_rng = TestRng::new();
    let step = Step::random(&mut test_rng);

    db.save_step(
        step.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving step");

    db.get_step_by_era(step.era_id.value())
        .await
        .expect("Error getting step by era");
}

pub async fn should_save_and_retrieve_a_step_with_u64_max_era<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let mut step = Step::random(&mut test_rng);
    step.era_id = EraId::new(u64::MAX);

    db.save_step(
        step.clone(),
        1,
        "127.0.0.1".to_string(),
        "1.1.1".to_string(),
        "network-1".to_string(),
    )
    .await
    .expect("Error saving Step with u64::MAX era id");

    let retrieved_step = db
        .get_step_by_era(u64::MAX)
        .await
        .expect("Error retrieving Step with u64::MAX era id");

    assert_eq!(retrieved_step.era_id.value(), u64::MAX)
}

pub async fn should_disallow_duplicate_event_id_from_source<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let event_id = test_rng.gen::<u32>();
    let block_added = BlockAdded::random(&mut test_rng);

    assert!(db
        .save_block_added(
            block_added.clone(),
            event_id,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());
    let res = db
        .save_block_added(
            block_added,
            event_id,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await;
    assert!(matches!(res, Err(DatabaseWriteError::UniqueConstraint(_))));
    // This check is to ensure that the UNIQUE constraint being broken is from the event_log table rather than from the raw event table.
    if let Err(DatabaseWriteError::UniqueConstraint(uc_err)) = res {
        assert_eq!(uc_err.table, "event_log")
    }
}

pub async fn should_disallow_insert_of_existing_block_added<DB: DatabaseReader + DatabaseWriter>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let block_added = BlockAdded::random(&mut test_rng);

    assert!(db
        .save_block_added(
            block_added.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_block_added(
            block_added,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "BlockAdded")
    }
}

pub async fn should_disallow_insert_of_existing_transaction_accepted<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_accepted = TransactionAccepted::random(&mut test_rng);

    assert!(db
        .save_transaction_accepted(
            transaction_accepted.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_transaction_accepted(
            transaction_accepted,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "TransactionAccepted")
    }
}

pub async fn should_disallow_insert_of_existing_transaction_expired<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_expired = TransactionExpired::random(&mut test_rng, None);

    assert!(db
        .save_transaction_expired(
            transaction_expired.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_transaction_expired(
            transaction_expired,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "TransactionExpired")
    }
}

pub async fn should_disallow_insert_of_existing_transaction_processed<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let transaction_processed = TransactionProcessed::random(&mut test_rng, None);

    assert!(db
        .save_transaction_processed(
            transaction_processed.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_transaction_processed(
            transaction_processed,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "TransactionProcessed")
    }
}

pub async fn should_disallow_insert_of_existing_fault<DB: DatabaseReader + DatabaseWriter>(db: DB) {
    let mut test_rng = TestRng::new();
    let fault = Fault::random(&mut test_rng);

    assert!(db
        .save_fault(
            fault.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_fault(
            fault,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "Fault")
    }
}

pub async fn should_disallow_insert_of_existing_finality_signature<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let finality_signature = FinalitySignature::random(&mut test_rng);

    assert!(db
        .save_finality_signature(
            finality_signature.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_finality_signature(
            finality_signature,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "FinalitySignature")
    }
}

pub async fn should_disallow_insert_of_existing_step<DB: DatabaseReader + DatabaseWriter>(db: DB) {
    let mut test_rng = TestRng::new();
    let step = Step::random(&mut test_rng);

    assert!(db
        .save_step(
            step.clone(),
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());

    let db_err = db
        .save_step(
            step,
            2,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .unwrap_err();

    assert!(matches!(db_err, DatabaseWriteError::UniqueConstraint(_)));

    // This check is to ensure that the UNIQUE constraint error is originating from the raw event table rather than the event_log
    if let DatabaseWriteError::UniqueConstraint(uc_err) = db_err {
        assert_eq!(uc_err.table, "Step")
    }
}

pub async fn get_number_of_events_should_return_0<DB: DatabaseReader + DatabaseWriter>(db: DB) {
    assert_eq!(db.get_number_of_events().await.unwrap(), 0);
}

pub async fn get_number_of_events_should_return_1_when_event_stored<
    DB: DatabaseReader + DatabaseWriter,
>(
    db: DB,
) {
    let mut test_rng = TestRng::new();
    let fault = Fault::random(&mut test_rng);

    assert!(db
        .save_fault(
            fault,
            1,
            "127.0.0.1".to_string(),
            "1.1.1".to_string(),
            "network-1".to_string(),
        )
        .await
        .is_ok());
    assert_eq!(db.get_number_of_events().await.unwrap(), 1);
}

pub fn fetch_event_log_data_query() -> SelectStatement {
    let mut select = Query::select();
    select
        .column(tables::event_log::EventLog::EventTypeId)
        .column(tables::event_log::EventLog::ApiVersion)
        .column(tables::event_log::EventLog::NetworkName)
        .column(tables::event_log::EventLog::EventSourceAddress)
        .from(tables::event_log::EventLog::Table);
    select
}
