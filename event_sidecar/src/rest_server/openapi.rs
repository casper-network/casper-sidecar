mod schema_transformation_visitor;
use crate::{
    database::types::*,
    types::{
        database::TransactionAggregate,
        sse_events::{
            BlockAdded, Fault, Step, TransactionAccepted, TransactionExpired, TransactionProcessed,
        },
    },
};
use casper_types::{
    contract_messages::Messages,
    execution::{execution_result_v1::ExecutionEffect, Effects, ExecutionResult},
    Block, BlockHash, FinalitySignature, RuntimeArgs, Transaction,
};
use schemars::{schema::SchemaObject, schema_for, visit::Visitor};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use utoipa::{
    openapi::{Components, Contact, RefOr, Schema},
    Modify, OpenApi,
};
use warp::Filter;

use self::schema_transformation_visitor::SchemaTransformationVisitor;

#[derive(OpenApi)]
#[openapi(
        modifiers(&AuthorsModification),
        paths(crate::rest_server::filters::latest_block,
            crate::rest_server::filters::block_by_hash,
            crate::rest_server::filters::block_by_height,
            crate::rest_server::filters::transaction_by_hash,
            crate::rest_server::filters::transaction_accepted_by_hash,
            crate::rest_server::filters::transaction_expired_by_hash,
            crate::rest_server::filters::transaction_processed_by_hash,
            crate::rest_server::filters::faults_by_public_key,
            crate::rest_server::filters::faults_by_era,
            crate::rest_server::filters::finality_signatures_by_block,
            crate::rest_server::filters::step_by_era,


        ),
        components(
            schemas(EnvelopeHeader, BlockAddedEnveloped, TransactionAcceptedEnveloped, TransactionExpiredEnveloped, TransactionProcessedEnveloped, FaultEnveloped, FinalitySignatureEnveloped, StepEnveloped, Step, Fault, TransactionExpired, TransactionAggregate, TransactionAccepted, TransactionProcessed, BlockAdded)
        ),
        tags(
            (name = "event-sidecar", description = "Event-sidecar rest API")
        )
    )]
struct ApiDoc;

struct AuthorsModification;

impl Modify for AuthorsModification {
    fn modify(&self, openapi: &mut utoipa::openapi::OpenApi) {
        let mut contact = Contact::new();
        contact.name = Some("Sidecar team".to_string());
        contact.url = Some("https://github.com/casper-network/casper-sidecar".to_string());
        openapi.info.contact = Some(contact);
    }
}

fn extend_open_api_with_schemars_schemas(
    components: &mut Components,
    names_and_schemas: Vec<(String, schemars::schema::RootSchema)>,
) {
    for (name, schema) in names_and_schemas {
        let (execution_result, additional_components) = force_produce_utoipa_schemas(schema);
        components.schemas.insert(name, execution_result);
        for (key, value) in additional_components {
            components.schemas.insert(key, value);
        }
    }
}

pub fn build_open_api_filters(
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let mut doc = ApiDoc::openapi();
    let mut components = doc.components.unwrap();
    extend_open_api_with_schemars_schemas(
        &mut components,
        vec![
            ("Block".to_string(), schema_for!(Block)),
            ("BlockHash".to_string(), schema_for!(BlockHash)),
            ("RuntimeArgs".to_string(), schema_for!(RuntimeArgs)),
            (
                "FinalitySignature".to_string(),
                schema_for!(FinalitySignature),
            ),
            ("ExecutionEffect".to_string(), schema_for!(ExecutionEffect)),
            ("Effects".to_string(), schema_for!(Effects)),
            ("Transaction".to_string(), schema_for!(Transaction)),
            ("ExecutionResult".to_string(), schema_for!(ExecutionResult)),
            ("Messages".to_string(), schema_for!(Messages)),
        ],
    );
    doc.components = Some(components);

    warp::path("api-doc.json")
        .and(warp::get())
        .map(move || warp::reply::json(&doc))
}

fn force_produce_utoipa_schemas(
    mut root_schema: schemars::schema::RootSchema,
) -> (RefOr<Schema>, HashMap<String, RefOr<Schema>>) {
    let mut visitor = SchemaTransformationVisitor {
        skip_additional_properties: true,
    };
    visitor.visit_root_schema(&mut root_schema);

    let schema_wrapper = RefOr::from(rebuild_schema_object("RootSchema", root_schema.schema));
    let mut rebuilt_schema_objects = HashMap::new();
    for (key, value) in root_schema.definitions {
        rebuilt_schema_objects.insert(
            key.clone(),
            RefOr::from(rebuild_schema_object(&key, value.into_object())),
        );
    }
    (schema_wrapper, rebuilt_schema_objects)
}

fn rebuild_schema_object(key: &str, schemars_schema_obj: SchemaObject) -> utoipa::openapi::Schema {
    let schema_str = serde_json::to_string(&schemars_schema_obj).unwrap();
    match serde_json::from_str::<utoipa::openapi::Schema>(&schema_str) {
        Ok(x) => x,
        Err(e) => {
            panic!(
                "Failed handling schema for type {}. Err: {}\n\n\n{}",
                key, e, schema_str
            );
        }
    }
}

#[derive(Deserialize, Serialize)]
struct ApiError {
    code: u16,
    message: String,
}
