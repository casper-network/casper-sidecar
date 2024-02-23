mod schema_transformation_visitor;
use crate::types::{
    database::DeployAggregate,
    sse_events::{BlockAdded, DeployAccepted, DeployExpired, DeployProcessed, Fault, Step},
};
use casper_event_types::{
    block::json_compatibility::{
        JsonBlockBody, JsonBlockHeader, JsonEraEnd, JsonEraReport, JsonProof, Reward,
        ValidatorWeight,
    },
    deploy::{Approval, DeployHeader},
    BlockHash, Deploy, DeployHash, Digest, ExecutableDeployItem, FinalitySignature, JsonBlock,
};
use casper_types::{
    ContractHash, ContractPackageHash, ContractVersion, ExecutionEffect, ExecutionResult,
    RuntimeArgs,
};
use http::Uri;
use schemars::{schema::SchemaObject, schema_for, visit::Visitor};
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc};
use utoipa::{
    openapi::{Components, Contact, RefOr, Schema},
    Modify, OpenApi,
};
use utoipa_swagger_ui::Config;
use warp::{
    hyper::{Response, StatusCode},
    path::{FullPath, Tail},
    Filter, Rejection, Reply,
};

use self::schema_transformation_visitor::SchemaTransformationVisitor;

#[derive(OpenApi)]
#[openapi(
        modifiers(&AuthorsModification),
        paths(crate::rest_server::filters::latest_block,
            crate::rest_server::filters::block_by_hash,
            crate::rest_server::filters::block_by_height,
            crate::rest_server::filters::deploy_by_hash,
            crate::rest_server::filters::deploy_accepted_by_hash,
            crate::rest_server::filters::deploy_expired_by_hash,
            crate::rest_server::filters::deploy_processed_by_hash,
            crate::rest_server::filters::faults_by_public_key,
            crate::rest_server::filters::faults_by_era,
            crate::rest_server::filters::finality_signatures_by_block,
            crate::rest_server::filters::step_by_era,


        ),
        components(
            schemas(Step, FinalitySignature, Fault, DeployExpired, Deploy, DeployHeader, ExecutableDeployItem, Approval, DeployAggregate, DeployAccepted, DeployProcessed, BlockAdded, JsonBlock, BlockHash, JsonEraEnd, JsonEraReport, JsonBlockBody, JsonBlockHeader, JsonProof, Digest, DeployHash, ValidatorWeight, Reward)
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
        contact.url = Some("https://github.com/CasperLabs/event-sidecar".to_string());
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
        for (key, value) in additional_components.into_iter() {
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
            ("ExecutionResult".to_string(), schema_for!(ExecutionResult)),
            ("RuntimeArgs".to_string(), schema_for!(RuntimeArgs)),
            ("ContractHash".to_string(), schema_for!(ContractHash)),
            (
                "ContractPackageHash".to_string(),
                schema_for!(ContractPackageHash),
            ),
            ("ContractVersion".to_string(), schema_for!(ContractVersion)),
            ("ExecutionEffect".to_string(), schema_for!(ExecutionEffect)),
        ],
    );
    doc.components = Some(components);
    let api_doc = warp::path("api-doc.json")
        .and(warp::get())
        .map(move || warp::reply::json(&doc));
    let config = Arc::new(Config::from("/api-doc.json"));
    let swagger_ui = warp::path("swagger-ui")
        .and(warp::get())
        .and(warp::path::full())
        .and(warp::path::tail())
        .and(warp::any().map(move || config.clone()))
        .and_then(serve_swagger);
    api_doc.or(swagger_ui)
}

fn force_produce_utoipa_schemas(
    mut root_schema: schemars::schema::RootSchema,
) -> (RefOr<Schema>, HashMap<String, RefOr<Schema>>) {
    let mut visitor = SchemaTransformationVisitor {
        skip_additional_properties: true,
    };
    visitor.visit_root_schema(&mut root_schema);

    let schema_wrapper = RefOr::from(rebuild_schema_object(
        "RootSchema".to_string(),
        root_schema.schema,
    ));
    let mut rebuilt_schema_objects = HashMap::new();
    for (key, value) in root_schema.definitions.into_iter() {
        rebuilt_schema_objects.insert(
            key.clone(),
            RefOr::from(rebuild_schema_object(key, value.into_object())),
        );
    }
    (schema_wrapper, rebuilt_schema_objects)
}

fn rebuild_schema_object(
    key: String,
    schemars_schema_obj: SchemaObject,
) -> utoipa::openapi::Schema {
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

async fn serve_swagger(
    full_path: FullPath,
    tail: Tail,
    config: Arc<Config<'static>>,
) -> Result<Box<dyn Reply + 'static>, Rejection> {
    if full_path.as_str() == "/swagger-ui" {
        return Ok(Box::new(warp::redirect::found(Uri::from_static(
            "/swagger-ui/",
        ))));
    }

    let path = tail.as_str();
    match utoipa_swagger_ui::serve(path, config) {
        Ok(file) => {
            if let Some(file) = file {
                Ok(Box::new(
                    Response::builder()
                        .header("Content-Type", file.content_type)
                        .body(file.bytes),
                ))
            } else {
                Ok(Box::new(StatusCode::NOT_FOUND))
            }
        }
        Err(error) => Ok(Box::new(
            Response::builder()
                .status(StatusCode::INTERNAL_SERVER_ERROR)
                .body(error.to_string()),
        )),
    }
}

#[derive(Deserialize, Serialize)]
struct ApiError {
    code: u16,
    message: String,
}
