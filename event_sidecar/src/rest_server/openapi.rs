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
    Block, BlockHash, FinalitySignature, RuntimeArgs, Transaction,
    contract_messages::Messages,
    execution::{Effects, ExecutionResult, execution_result_v1::ExecutionEffect},
};
use schemars::{
    schema::{RootSchema, SchemaObject},
    schema_for,
};
use std::collections::HashMap;
use utoipa::{
    Modify, OpenApi, ToSchema,
    openapi::{Components, Contact, RefOr, Schema},
};
use warp::{Filter, Rejection, Reply};

#[allow(dead_code)]
#[derive(ToSchema)]
#[schema(value_type = Object)]
struct FinalitySignatureWithSchema(FinalitySignature);

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
            schemas(EnvelopeHeader, SseEnvelope<BlockAdded>, SseEnvelope<TransactionAccepted>, SseEnvelope<TransactionExpired>, SseEnvelope<TransactionProcessed>, SseEnvelope<Fault>, SseEnvelope<FinalitySignatureWithSchema>, SseEnvelope<Step>, Step, Fault, TransactionExpired, TransactionAggregate, TransactionAccepted, TransactionProcessed, BlockAdded)
        ),
        tags(
            (name = "event-sidecar", description = "Event-sidecar REST API")
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
    names_and_schemas: Vec<(&str, RootSchema)>,
) {
    for (name, schema) in names_and_schemas {
        let (execution_result, additional_components) = force_produce_utoipa_schemas(schema);
        components
            .schemas
            .insert(name.to_string(), execution_result);
        for (key, value) in additional_components {
            components.schemas.insert(key, value);
        }
    }
}

pub fn build_open_api_filters() -> impl Filter<Extract = (impl Reply,), Error = Rejection> + Clone {
    let mut doc = ApiDoc::openapi();
    let mut components = doc.components.unwrap();
    extend_open_api_with_schemars_schemas(
        &mut components,
        vec![
            ("Block", schema_for!(Block)),
            ("BlockHash", schema_for!(BlockHash)),
            ("Effects", schema_for!(Effects)),
            ("ExecutionEffect", schema_for!(ExecutionEffect)),
            ("ExecutionResult", schema_for!(ExecutionResult)),
            ("FinalitySignature", schema_for!(FinalitySignature)),
            ("Messages", schema_for!(Messages)),
            ("RuntimeArgs", schema_for!(RuntimeArgs)),
            ("Transaction", schema_for!(Transaction)),
        ],
    );
    doc.components = Some(components);

    warp::path("api-doc.json")
        .and(warp::get())
        .map(move || warp::reply::json(&doc))
}

fn force_produce_utoipa_schemas(
    root_schema: RootSchema,
) -> (RefOr<Schema>, HashMap<String, RefOr<Schema>>) {
    let schema_wrapper = RefOr::from(rebuild_schema_object("RootSchema", &root_schema.schema));
    let mut rebuilt_schema_objects = HashMap::new();
    for (key, value) in root_schema.definitions {
        // FIXME: hack to avoid "data did not match any variant of untagged enum Schema"
        if key == "CLValue" {
            continue;
        }
        rebuilt_schema_objects.insert(
            key.clone(),
            RefOr::from(rebuild_schema_object(&key, &value.into_object())),
        );
    }
    (schema_wrapper, rebuilt_schema_objects)
}

fn rebuild_schema_object(key: &str, schemars_schema_obj: &SchemaObject) -> Schema {
    let schema_str = serde_json::to_string(&schemars_schema_obj).unwrap();
    match serde_json::from_str::<Schema>(&schema_str) {
        Ok(x) => x,
        Err(e) => {
            panic!(
                "Failed handling schema for type {}. Err: {}\n\n{}",
                key, e, schema_str
            );
        }
    }
}
