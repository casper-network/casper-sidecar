use std::ops::ControlFlow;

use schemars::{
    schema::{InstanceType, SchemaObject, SubschemaValidation},
    visit::{visit_schema_object, Visitor},
};
use serde_json::json;

/// Implementation of schemars Visitor trait. It's purpose is to transform schema that comes from schemars so it can be deserialized as utoipas entity schema.
/// Utoipa uses OpenApi style schemas, which isn't 100% compatible with the more general json schema.
/// The main transformations are:
/// * adding `"type": "value"` to object schemas that have not `type` field. Object schemas without `type` field are legal in json schema, but utoipa doesn't work with that (whenever an object schema doesn't specify `type` it uses the generic `value`)
/// * changing references from "#/definitions" to "#/components/schemas"
/// * replacing `"type": "nullable"` with `"type": "object"` and property `"nullable": true` -> again, utoipa doesn't support `"type": "null"`
/// * replacing validators with array types, like `"type": ["object","string"]` with "anyOf"
/// * replacing bool validators with schema object ones
pub struct SchemaTransformationVisitor {
    /// When set to `true`, a schema's `additionalProperties` property will not be changed from a boolean.
    pub skip_additional_properties: bool,
}

impl Visitor for SchemaTransformationVisitor {
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
        rename_refs(schema);
        replace_null_and_multi_type(schema);
        if let ControlFlow::Break(_) = self.handle_skip_additional_properties(schema) {
            return;
        }
        visit_schema_object(self, schema);
    }
}

fn replace_null_and_multi_type(schema: &mut SchemaObject) {
    //The following code does two things:
    // * utoipa doesn't support "type": "null" -> we need to get rid of that
    // * utoipa doesn't support "type": [] -> we need to destructure it to a "one_of" requirement
    let instance_type = schema.instance_type.clone();
    if schema.instance_type.is_some() {
        let instance_or_object = instance_type.unwrap();
        match instance_or_object {
            schemars::schema::SingleOrVec::Single(_) if schema.has_type(InstanceType::Null) => {
                force_schema_into_opean_api_nullable(schema);
            }
            schemars::schema::SingleOrVec::Single(_) => {
                //type here is not null, we don't need to do anything to it
            }
            schemars::schema::SingleOrVec::Vec(types) => {
                //OpenApi schemas don't support multiple types definitions, we need to change it to a collection of "anyOf"
                let mut new_schema = SchemaObject::default();
                let mut subschema_validation = SubschemaValidation::default();
                let mut vals: Vec<schemars::schema::Schema> = vec![];
                for t in types.iter() {
                    let mut single_type_schema = schema.clone();
                    if *t == InstanceType::Null {
                        force_schema_into_opean_api_nullable(&mut single_type_schema);
                    } else {
                        single_type_schema.instance_type =
                            Some(schemars::schema::SingleOrVec::Single(Box::new(*t)))
                    }
                    vals.push(schemars::schema::Schema::Object(single_type_schema));
                }
                subschema_validation.any_of = Some(vals);
                *new_schema.subschemas() = subschema_validation;
                *schema = new_schema;
            }
        }
    }
}

fn rename_refs(schema: &mut SchemaObject) {
    if let Some(r) = &schema.reference {
        // Schemars produces RootSchemas with "\"$ref\": \"#/definitions/(...)\"". Utoipas open api
        // endpoint schema expects reference definitions as "\"$ref\": \"#/components/schemas/(...)\""
        let utoipa_reference = r.replace("#/definitions", "#/components/schemas");
        schema.reference = Some(utoipa_reference);
    }
}

impl SchemaTransformationVisitor {
    fn handle_skip_additional_properties(&mut self, schema: &mut SchemaObject) -> ControlFlow<()> {
        if self.skip_additional_properties {
            // We need to make sure that the `additionalProperties` property doesn't get changed into a SchemaObject
            if let Some(obj) = &mut schema.object {
                if let Some(ap) = &obj.additional_properties {
                    if let schemars::schema::Schema::Bool(_) = ap.as_ref() {
                        let additional_properties = obj.additional_properties.take();
                        visit_schema_object(self, schema);
                        schema.object().additional_properties = additional_properties;
                        return ControlFlow::Break(());
                    }
                }
            }
        }
        ControlFlow::Continue(())
    }
}

fn force_schema_into_opean_api_nullable(schema: &mut SchemaObject) {
    schema.instance_type = Some(schemars::schema::SingleOrVec::Single(Box::new(
        InstanceType::Object,
    )));
    // OpenApi schema doesn't support type "null" - it uses property `"nullable": true` instead
    schema
        .extensions
        .insert("nullable".to_string(), json!(true));
    // The following two lines make sure that the resulting json schema will allow value `null` but not allow empty object `{}`
    schema.object().additional_properties = Some(Box::new(schemars::schema::Schema::Bool(false)));
    schema.object().properties.clear();
    schema.object().min_properties = Some(1);
}

#[cfg(test)]
mod tests {
    use super::SchemaTransformationVisitor;
    use schemars::{schema::Schema, visit::Visitor};

    #[test]
    fn should_change_bool_requirement_into_schema_object() {
        let json_schema = r#"{
            "$id": "https://example.com/person.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "properties": {
                "firstName": true
            },
            "additionalProperties": true
        }"#;
        let mut visitor = SchemaTransformationVisitor {
            skip_additional_properties: false,
        };
        let transformed = transform_schema(json_schema, &mut visitor);
        let expected = "{\n  \"$id\": \"https://example.com/person.schema.json\",\n  \"type\": \"object\",\n  \"properties\": {\n    \"firstName\": {\n      \"type\": \"value\"\n    }\n  },\n  \"additionalProperties\": {\n    \"type\": \"value\"\n  },\n  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\"\n}".to_string();
        assert_eq!(transformed, expected);
    }

    #[test]
    fn should_not_change_additional_properties_if_flag_is_set() {
        let json_schema = r#"{
            "$id": "https://example.com/person.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "object",
            "additionalProperties": true
        }"#;
        let mut visitor = SchemaTransformationVisitor {
            skip_additional_properties: true,
        };
        let transformed = transform_schema(json_schema, &mut visitor);
        let expected = "{\n  \"$id\": \"https://example.com/person.schema.json\",\n  \"type\": \"object\",\n  \"additionalProperties\": true,\n  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\"\n}".to_string();
        assert_eq!(transformed, expected);
    }

    #[test]
    fn should_change_multiple_types_in_object_to_any_of() {
        let json_schema = r#"{
            "$id": "https://example.com/person.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": ["string", "number", "object"],
            "properties": {
              "x": {"type": "string"}
            },
            "required": ["x"],
            "additionalProperties": true
        }"#;
        let mut visitor = SchemaTransformationVisitor {
            skip_additional_properties: true,
        };
        let transformed = transform_schema(json_schema, &mut visitor);
        let expected = "{\n  \"anyOf\": [\n    {\n      \"$id\": \"https://example.com/person.schema.json\",\n      \"type\": \"string\",\n      \"required\": [\n        \"x\"\n      ],\n      \"properties\": {\n        \"x\": {\n          \"type\": \"string\"\n        }\n      },\n      \"additionalProperties\": true,\n      \"$schema\": \"https://json-schema.org/draft/2020-12/schema\"\n    },\n    {\n      \"$id\": \"https://example.com/person.schema.json\",\n      \"type\": \"number\",\n      \"required\": [\n        \"x\"\n      ],\n      \"properties\": {\n        \"x\": {\n          \"type\": \"string\"\n        }\n      },\n      \"additionalProperties\": true,\n      \"$schema\": \"https://json-schema.org/draft/2020-12/schema\"\n    },\n    {\n      \"$id\": \"https://example.com/person.schema.json\",\n      \"type\": \"object\",\n      \"required\": [\n        \"x\"\n      ],\n      \"properties\": {\n        \"x\": {\n          \"type\": \"string\"\n        }\n      },\n      \"additionalProperties\": true,\n      \"$schema\": \"https://json-schema.org/draft/2020-12/schema\"\n    }\n  ]\n}".to_string();
        assert_eq!(transformed, expected);
    }

    #[test]
    fn should_change_null_type_into_nullable() {
        let json_schema = r#"{
            "$id": "https://example.com/person.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": "null"
        }"#;
        let mut visitor = SchemaTransformationVisitor {
            skip_additional_properties: true,
        };
        let transformed = transform_schema(json_schema, &mut visitor);
        let expected = "{\n  \"$id\": \"https://example.com/person.schema.json\",\n  \"type\": \"object\",\n  \"minProperties\": 1,\n  \"additionalProperties\": false,\n  \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n  \"nullable\": true\n}".to_string();
        assert_eq!(transformed, expected);
    }

    #[test]
    fn should_change_null_type_into_nullable_if_multiple_types() {
        let json_schema = r#"{
            "$id": "https://example.com/person.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "type": ["null", "object"],
            "properties": {"x": true}
        }"#;
        let mut visitor = SchemaTransformationVisitor {
            skip_additional_properties: true,
        };
        let transformed = transform_schema(json_schema, &mut visitor);
        let expected = "{\n  \"anyOf\": [\n    {\n      \"$id\": \"https://example.com/person.schema.json\",\n      \"type\": \"object\",\n      \"minProperties\": 1,\n      \"additionalProperties\": false,\n      \"$schema\": \"https://json-schema.org/draft/2020-12/schema\",\n      \"nullable\": true\n    },\n    {\n      \"$id\": \"https://example.com/person.schema.json\",\n      \"type\": \"object\",\n      \"properties\": {\n        \"x\": {\n          \"type\": \"value\"\n        }\n      },\n      \"$schema\": \"https://json-schema.org/draft/2020-12/schema\"\n    }\n  ]\n}".to_string();
        assert_eq!(transformed, expected);
    }

    fn transform_schema(schema: &str, visitor: &mut SchemaTransformationVisitor) -> String {
        let mut obj = serde_json::from_str::<Schema>(schema).unwrap();
        visitor.visit_schema(&mut obj);
        serde_json::to_string_pretty(&obj).unwrap()
    }
}
