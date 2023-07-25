use std::{collections::HashMap, sync::Arc};

use crate::types::{
    database::DeployAggregate,
    sse_events::{BlockAdded, DeployAccepted, DeployProcessed},
};
use casper_event_types::{
    block::json_compatibility::{
        JsonBlockBody, JsonBlockHeader, JsonEraEnd, JsonEraReport, JsonProof, Reward,
        ValidatorWeight,
    },
    deploy::{Approval, DeployHeader},
    BlockHash, Deploy, DeployHash, Digest, ExecutableDeployItem, JsonBlock,
};
use casper_types::{
    ContractHash, ContractPackageHash, ContractVersion, ExecutionResult, RuntimeArgs,
};
use http::Uri;
use schemars::{
    schema::{InstanceType, SchemaObject, SubschemaValidation},
    schema_for,
    visit::{visit_schema_object, Visitor},
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use utoipa::{
    openapi::{RefOr, Schema},
    OpenApi,
};
use utoipa_swagger_ui::Config;
use warp::{
    hyper::{Response, StatusCode},
    path::{FullPath, Tail},
    Filter, Rejection, Reply,
};

#[derive(OpenApi)]
#[openapi(
        paths(crate::rest_server::filters::latest_block,
            crate::rest_server::filters::block_by_hash,
            crate::rest_server::filters::block_by_height,
            crate::rest_server::filters::deploy_by_hash,
            crate::rest_server::filters::deploy_accepted_by_hash),
        components(
            schemas(Deploy, DeployHeader, ExecutableDeployItem, Approval, DeployAggregate, DeployAccepted, DeployProcessed, BlockAdded, JsonBlock, BlockHash, JsonEraEnd, JsonEraReport, JsonBlockBody, JsonBlockHeader, JsonProof, Digest, DeployHash, ValidatorWeight, Reward)
        ),
        tags(
            (name = "event-sidecar", description = "Event-sidecar rest API")
        )
    )]
struct ApiDoc;

pub fn build_open_api_filters(
) -> impl Filter<Extract = (impl warp::Reply,), Error = warp::Rejection> + Clone {
    let mut doc = ApiDoc::openapi();
    let mut components = doc.components.unwrap();
    let (execution_result, additional_components) =
        force_produce_utoipa_schemas(schema_for!(ExecutionResult));

    components
        .schemas
        .insert("ExecutionResult".to_string(), execution_result);
    for (key, value) in additional_components.into_iter() {
        components.schemas.insert(key, value);
    }
    let (execution_result, additional_components) =
        force_produce_utoipa_schemas(schema_for!(RuntimeArgs));
    components
        .schemas
        .insert("RuntimeArgs".to_string(), execution_result);
    for (key, value) in additional_components.into_iter() {
        components.schemas.insert(key, value);
    }
    let (execution_result, additional_components) =
        force_produce_utoipa_schemas(schema_for!(ContractHash));
    components
        .schemas
        .insert("ContractHash".to_string(), execution_result);
    for (key, value) in additional_components.into_iter() {
        components.schemas.insert(key, value);
    }
    let (execution_result, additional_components) =
        force_produce_utoipa_schemas(schema_for!(ContractPackageHash));
    components
        .schemas
        .insert("ContractPackageHash".to_string(), execution_result);
    for (key, value) in additional_components.into_iter() {
        components.schemas.insert(key, value);
    }
    let (execution_result, additional_components) =
        force_produce_utoipa_schemas(schema_for!(ContractVersion));
    components
        .schemas
        .insert("ContractVersion".to_string(), execution_result);
    for (key, value) in additional_components.into_iter() {
        components.schemas.insert(key, value);
    }

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
    let mut visitor = MyVisitor {
        skip_additional_properties: true,
    };
    visitor.visit_root_schema(&mut root_schema);

    let schema_wrapper = RefOr::from(rebuild_schema_object(
        "RootSchema".to_string(),
        root_schema.schema,
    ));
    let mut v = HashMap::new();
    for (key, value) in root_schema.definitions.into_iter() {
        let z = key.clone();
        let schema_obj = value.into_object();
        let temp = RefOr::from(rebuild_schema_object(z, schema_obj));
        v.insert(key, temp);
    }
    (schema_wrapper, v)
}

pub struct MyVisitor {
    /// When set to `true`, a schema's `additionalProperties` property will not be changed from a boolean.
    pub skip_additional_properties: bool,
}

impl Visitor for MyVisitor {
    fn visit_schema(&mut self, schema: &mut schemars::schema::Schema) {
        if let schemars::schema::Schema::Bool(b) = *schema {
            let mut object_schema: SchemaObject = schemars::schema::Schema::Bool(b).into_object();
            // The following is a hack for utoipa - utoipa doesn't accept schemas that don't have any "type" field.
            // For a generic schema that doesn't put restraints on the type of the field utoipa uses a special "type": "value" which is,
            // AFAIK, non-standard
            object_schema
                .extensions
                .insert("type".to_string(), json!("value"));
            *schema = object_schema.into()
        }
        schemars::visit::visit_schema(self, schema);
    }

    fn visit_schema_object(&mut self, schema: &mut SchemaObject) {
        if let Some(r) = &schema.reference {
            // Schemars produces RootSchemas with "\"$ref\": \"#/definitions/(...)\"". Utoipas open api
            // endpoint schema expects reference definitions as "\"$ref\": \"#/components/schemas/(...)\""
            let utoipa_reference = r.replace("#/definitions", "#/components/schemas");
            schema.reference = Some(utoipa_reference);
        }
        //The following code does two things:
        // * utoipa doesn't support "type": "null" -> we need to get rid of that
        // * utoipa doesn't support "type": [] -> we need to destructure it to a "one_of" requirement
        let cloned = schema.clone();
        let instance_type = cloned.instance_type.clone();
        if schema.instance_type.is_some() {
            let instance_or_object = instance_type.unwrap();
            match instance_or_object {
                schemars::schema::SingleOrVec::Single(_) if schema.has_type(InstanceType::Null) => {
                    //type here is null, we need to transform it to object which is nullable
                    schema.instance_type = Some(schemars::schema::SingleOrVec::Single(Box::new(
                        InstanceType::Object,
                    )));
                    let x = json!(true);
                    schema.extensions.insert("nullable".to_string(), x);
                }
                schemars::schema::SingleOrVec::Single(_) => {
                    //type here is not null, we don't need to do anything to it
                }
                schemars::schema::SingleOrVec::Vec(types) => {
                    let mut new_schema = SchemaObject::default();
                    let mut subschema_validation = SubschemaValidation::default();
                    let mut vals: Vec<schemars::schema::Schema> = vec![];
                    for t in types.iter() {
                        let mut cloned = cloned.clone();
                        if *t == InstanceType::Null {
                            cloned.instance_type = Some(schemars::schema::SingleOrVec::Single(
                                Box::new(InstanceType::Object),
                            ));
                            let x = json!(true);
                            cloned.extensions.insert("nullable".to_string(), x);
                        } else {
                            cloned.instance_type =
                                Some(schemars::schema::SingleOrVec::Single(Box::new(*t)))
                        }
                        vals.push(schemars::schema::Schema::Object(cloned));
                    }
                    subschema_validation.one_of = Some(vals);
                    *new_schema.subschemas() = subschema_validation;
                    *schema = new_schema;
                }
            }
        }

        if self.skip_additional_properties {
            // We need to make sure that the `additionalProperties` property doesn't get changed into a SchemaObject
            if let Some(obj) = &mut schema.object {
                if let Some(ap) = &obj.additional_properties {
                    if let schemars::schema::Schema::Bool(_) = ap.as_ref() {
                        let additional_properties = obj.additional_properties.take();
                        visit_schema_object(self, schema);
                        schema.object().additional_properties = additional_properties;
                        return;
                    }
                }
            }
        }
        visit_schema_object(self, schema);
    }
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
