//! RPCs related to finding information about currently supported RPCs.

use std::sync::Arc;

use async_trait::async_trait;
use derive_new::new;
use once_cell::sync::Lazy;
use schemars::{
    gen::{SchemaGenerator, SchemaSettings},
    schema::Schema,
    JsonSchema, Map, MapEntry,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use super::{
    account::{PutDeploy, PutTransaction},
    chain::{
        GetBlock, GetBlockTransfers, GetEraInfoBySwitchBlock, GetEraSummary, GetStateRootHash,
    },
    info::{
        GetChainspec, GetDeploy, GetPeers, GetReward, GetStatus, GetTransaction,
        GetValidatorChanges,
    },
    state::{
        GetAccountInfo, GetAddressableEntity, GetAuctionInfo, GetBalance, GetDictionaryItem,
        GetItem, GetPackage, QueryBalance, QueryBalanceDetails, QueryGlobalState,
    },
    state_get_auction_info_v2::GetAuctionInfo as GetAuctionInfoV2,
    ApiVersion, NodeClient, RpcError, RpcWithOptionalParams, RpcWithParams, RpcWithoutParams,
    CURRENT_API_VERSION,
};

pub(crate) const DOCS_EXAMPLE_API_VERSION: ApiVersion = CURRENT_API_VERSION;

const DEFINITIONS_PATH: &str = "#/components/schemas/";
pub(crate) const OPEN_RPC_VERSION: &str = "1.0.0-rc1";

pub(crate) static CONTACT: Lazy<OpenRpcContactField> = Lazy::new(|| OpenRpcContactField {
    name: "Casper Labs".to_string(),
    url: "https://casperlabs.io".to_string(),
});

pub(crate) static LICENSE: Lazy<OpenRpcLicenseField> = Lazy::new(|| OpenRpcLicenseField {
    name: "APACHE LICENSE, VERSION 2.0".to_string(),
    url: "https://www.apache.org/licenses/LICENSE-2.0".to_string(),
});

static SERVER: Lazy<OpenRpcServerEntry> = Lazy::new(|| {
    OpenRpcServerEntry::new(
        "any Sidecar with JSON RPC API enabled".to_string(),
        "http://IP:PORT/rpc/".to_string(),
    )
});

// As per https://spec.open-rpc.org/#service-discovery-method.
pub(crate) static OPEN_RPC_SCHEMA: Lazy<OpenRpcSchema> = Lazy::new(|| {
    let info = OpenRpcInfoField {
        version: DOCS_EXAMPLE_API_VERSION.to_string(),
        title: "Client API of Casper Node".to_string(),
        description: "This describes the JSON-RPC 2.0 API of a node on the Casper network."
            .to_string(),
        contact: CONTACT.clone(),
        license: LICENSE.clone(),
    };
    let mut schema = OpenRpcSchema::new(OPEN_RPC_VERSION.to_string(), info, vec![SERVER.clone()]);

    schema.push_with_params::<PutDeploy>(
        "receives a Deploy to be executed by the network (DEPRECATED: use \
        `account_put_transaction` instead)",
    );
    schema
        .push_with_params::<PutTransaction>("receives a Transaction to be executed by the network");
    schema.push_with_params::<GetDeploy>(
        "returns a Deploy from the network (DEPRECATED: use `info_get_transaction` instead)",
    );
    schema.push_with_params::<GetTransaction>("returns a Transaction from the network");
    schema.push_with_params::<GetAccountInfo>("returns an Account from the network");
    schema
        .push_with_params::<GetAddressableEntity>("returns an AddressableEntity from the network");
    schema.push_with_params::<GetPackage>("returns a Package from the network");
    schema.push_with_params::<GetDictionaryItem>("returns an item from a Dictionary");
    schema.push_with_params::<QueryGlobalState>(
        "a query to global state using either a Block hash or state root hash",
    );
    schema.push_with_params::<QueryBalance>(
        "query for a balance using a purse identifier and a state identifier",
    );
    schema.push_with_params::<QueryBalanceDetails>(
        "query for full balance information using a purse identifier and a state identifier",
    );
    schema.push_without_params::<GetPeers>("returns a list of peers connected to the node");
    schema.push_without_params::<GetStatus>("returns the current status of the node");
    schema.push_with_params::<GetReward>(
        "returns the reward for a given era and a validator or a delegator",
    );
    schema
        .push_without_params::<GetValidatorChanges>("returns status changes of active validators");
    schema.push_without_params::<GetChainspec>(
        "returns the raw bytes of the chainspec.toml, genesis accounts.toml, and \
        global_state.toml files",
    );
    schema.push_with_optional_params::<GetBlock>("returns a Block from the network");
    schema.push_with_optional_params::<GetBlockTransfers>(
        "returns all transfers for a Block from the network",
    );
    schema.push_with_optional_params::<GetStateRootHash>(
        "returns a state root hash at a given Block",
    );
    schema.push_with_params::<GetItem>(
        "returns a stored value from the network. This RPC is deprecated, use \
        `query_global_state` instead.",
    );
    schema.push_with_params::<GetBalance>("returns a purse's balance from the network");
    schema.push_with_optional_params::<GetEraInfoBySwitchBlock>(
        "returns an EraInfo from the network",
    );
    schema.push_with_optional_params::<GetAuctionInfo>(
        "returns the bids and validators as of either a specific block (by height or hash), or \
        the most recently added block. This is a casper 1.x retro-compatibility endpoint. For blocks created in 1.x protocol it will work exactly the same as it used to.
        For 2.x blocks it will try to retrofit the changed data structure into previous schema - but it is a lossy process. Use `state_get_auction_info_v2` endpoint to get data in new format. *IMPORTANT* This method is deprecated, has been added only for compatibility with retired nodes json-rpc API and will be removed in a future release of sidecar.",
    );
    schema.push_with_optional_params::<GetAuctionInfoV2>(
        "returns the bids and validators as of either a specific block (by height or hash), or \
        the most recently added block. It works for blocks created in 1.x and 2.x",
    );
    schema.push_with_optional_params::<GetEraSummary>(
        "returns the era summary at either a specific block (by height or hash), or the most \
        recently added block",
    );

    schema
});
static LIST_RPCS_RESULT: Lazy<RpcDiscoverResult> = Lazy::new(|| RpcDiscoverResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    name: "OpenRPC Schema".to_string(),
    schema: OPEN_RPC_SCHEMA.clone(),
});

/// A trait used to generate a static hardcoded example of `Self`.
pub trait DocExample {
    /// Generates a hardcoded example of `Self`.
    fn doc_example() -> &'static Self;
}

/// The main schema for the casper node's RPC server, compliant with
/// [the OpenRPC Specification](https://spec.open-rpc.org).
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
pub struct OpenRpcSchema {
    openrpc: String,
    info: OpenRpcInfoField,
    servers: Vec<OpenRpcServerEntry>,
    methods: Vec<Method>,
    components: Components,
}

impl OpenRpcSchema {
    pub fn new(openrpc: String, info: OpenRpcInfoField, servers: Vec<OpenRpcServerEntry>) -> Self {
        OpenRpcSchema {
            openrpc,
            info,
            servers,
            methods: Vec::new(),
            components: Components::default(),
        }
    }

    fn new_generator() -> SchemaGenerator {
        let settings = SchemaSettings::default().with(|settings| {
            settings.definitions_path = DEFINITIONS_PATH.to_string();
        });
        settings.into_generator()
    }

    pub(crate) fn push_with_params<T: RpcWithParams>(&mut self, summary: &str) {
        let mut generator = Self::new_generator();

        let params_schema = T::RequestParams::json_schema(&mut generator);
        let params = Self::make_params(params_schema);

        let result_schema = T::ResponseResult::json_schema(&mut generator);
        let result = ResponseResult {
            name: format!("{}_result", T::METHOD),
            schema: result_schema,
        };

        let examples = vec![Example::from_rpc_with_params::<T>()];

        let method = Method {
            name: T::METHOD.to_string(),
            summary: summary.to_string(),
            params,
            result,
            examples,
        };

        self.methods.push(method);
        self.update_schemas::<T::RequestParams>();
        self.update_schemas::<T::ResponseResult>();
    }

    pub(crate) fn push_without_params<T: RpcWithoutParams>(&mut self, summary: &str) {
        let mut generator = Self::new_generator();

        let result_schema = T::ResponseResult::json_schema(&mut generator);
        let result = ResponseResult {
            name: format!("{}_result", T::METHOD),
            schema: result_schema,
        };

        let examples = vec![Example::from_rpc_without_params::<T>()];

        let method = Method {
            name: T::METHOD.to_string(),
            summary: summary.to_string(),
            params: Vec::new(),
            result,
            examples,
        };

        self.methods.push(method);
        self.update_schemas::<T::ResponseResult>();
    }

    fn push_with_optional_params<T: RpcWithOptionalParams>(&mut self, summary: &str) {
        let mut generator = Self::new_generator();

        let params_schema = T::OptionalRequestParams::json_schema(&mut generator);
        let params = Self::make_optional_params(params_schema);

        let result_schema = T::ResponseResult::json_schema(&mut generator);
        let result = ResponseResult {
            name: format!("{}_result", T::METHOD),
            schema: result_schema,
        };

        let examples = vec![Example::from_rpc_with_optional_params::<T>()];

        // TODO - handle adding a description that the params may be omitted if desired.
        let method = Method {
            name: T::METHOD.to_string(),
            summary: summary.to_string(),
            params,
            result,
            examples,
        };

        self.methods.push(method);
        self.update_schemas::<T::OptionalRequestParams>();
        self.update_schemas::<T::ResponseResult>();
    }

    /// Convert the schema for the params type for T into the OpenRpc-compatible map of name, value
    /// pairs.
    ///
    /// As per the standard, the required params must be sorted before the optional ones.
    fn make_params(schema: Schema) -> Vec<SchemaParam> {
        let schema_object = schema.into_object().object.expect("should be object");
        let mut required_params = schema_object
            .properties
            .iter()
            .filter(|(name, _)| schema_object.required.contains(*name))
            .map(|(name, schema)| SchemaParam {
                name: name.clone(),
                schema: schema.clone(),
                required: true,
            })
            .collect::<Vec<_>>();
        let optional_params = schema_object
            .properties
            .iter()
            .filter(|(name, _)| !schema_object.required.contains(*name))
            .map(|(name, schema)| SchemaParam {
                name: name.clone(),
                schema: schema.clone(),
                required: false,
            })
            .collect::<Vec<_>>();
        required_params.extend(optional_params);
        required_params
    }

    /// Convert the schema for the optional params type for T into the OpenRpc-compatible map of
    /// name, value pairs.
    ///
    /// Since all params must be unanimously optional, mark all incorrectly tagged "required" fields
    /// as false.
    fn make_optional_params(schema: Schema) -> Vec<SchemaParam> {
        let schema_object = schema.into_object().object.expect("should be object");
        schema_object
            .properties
            .iter()
            .filter(|(name, _)| schema_object.required.contains(*name))
            .map(|(name, schema)| SchemaParam {
                name: name.clone(),
                schema: schema.clone(),
                required: false,
            })
            .collect::<Vec<_>>()
    }

    /// Insert the new entries into the #/components/schemas/ map.  Panic if we try to overwrite an
    /// entry with a different value.
    fn update_schemas<S: JsonSchema>(&mut self) {
        let generator = Self::new_generator();
        let mut root_schema = generator.into_root_schema_for::<S>();
        for (key, value) in root_schema.definitions.drain(..) {
            match self.components.schemas.entry(key) {
                MapEntry::Occupied(current_value) => {
                    assert_eq!(
                        current_value.get().clone().into_object().metadata,
                        value.into_object().metadata
                    );
                }
                MapEntry::Vacant(vacant) => {
                    let _ = vacant.insert(value);
                }
            }
        }
    }

    #[cfg(test)]
    fn give_params_schema<T: RpcWithOptionalParams>(&self) -> Schema {
        let mut generator = Self::new_generator();
        T::OptionalRequestParams::json_schema(&mut generator)
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema, new)]
pub(crate) struct OpenRpcInfoField {
    version: String,
    title: String,
    description: String,
    contact: OpenRpcContactField,
    license: OpenRpcLicenseField,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct OpenRpcContactField {
    name: String,
    url: String,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
pub(crate) struct OpenRpcLicenseField {
    name: String,
    url: String,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema, new)]
pub(crate) struct OpenRpcServerEntry {
    name: String,
    url: String,
}

/// The struct containing the documentation for the RPCs.
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
struct Method {
    name: String,
    summary: String,
    params: Vec<SchemaParam>,
    result: ResponseResult,
    examples: Vec<Example>,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
struct SchemaParam {
    name: String,
    schema: Schema,
    required: bool,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
struct ResponseResult {
    name: String,
    schema: Schema,
}

/// An example pair of request params and response result.
#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
pub struct Example {
    name: String,
    params: Vec<ExampleParam>,
    result: ExampleResult,
}

impl Example {
    fn new(method_name: &str, maybe_params_obj: Option<Value>, result_value: Value) -> Self {
        // Break the params struct into an array of param name and value pairs.
        let params = match maybe_params_obj {
            Some(params_obj) => params_obj
                .as_object()
                .unwrap()
                .iter()
                .map(|(name, value)| ExampleParam {
                    name: name.clone(),
                    value: value.clone(),
                })
                .collect(),
            None => Vec::new(),
        };

        Example {
            name: format!("{method_name}_example"),
            params,
            result: ExampleResult {
                name: format!("{method_name}_example_result"),
                value: result_value,
            },
        }
    }

    fn from_rpc_with_params<T: RpcWithParams>() -> Self {
        Self::new(
            T::METHOD,
            Some(json!(T::RequestParams::doc_example())),
            json!(T::ResponseResult::doc_example()),
        )
    }

    fn from_rpc_without_params<T: RpcWithoutParams>() -> Self {
        Self::new(T::METHOD, None, json!(T::ResponseResult::doc_example()))
    }

    fn from_rpc_with_optional_params<T: RpcWithOptionalParams>() -> Self {
        Self::new(
            T::METHOD,
            Some(json!(T::OptionalRequestParams::doc_example())),
            json!(T::ResponseResult::doc_example()),
        )
    }
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
struct ExampleParam {
    name: String,
    value: Value,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema)]
struct ExampleResult {
    name: String,
    value: Value,
}

#[derive(Clone, PartialEq, Serialize, Deserialize, Debug, JsonSchema, Default)]
pub struct Components {
    schemas: Map<String, Schema>,
}

/// Result for "rpc.discover" RPC response.
//
// Fields named as per https://spec.open-rpc.org/#service-discovery-method.
#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema, Debug)]
#[serde(deny_unknown_fields)]
pub struct RpcDiscoverResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    api_version: ApiVersion,
    name: String,
    /// The list of supported RPCs.
    #[schemars(skip)]
    schema: OpenRpcSchema,
}

impl DocExample for RpcDiscoverResult {
    fn doc_example() -> &'static Self {
        &LIST_RPCS_RESULT
    }
}

/// "rpc.discover" RPC.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct RpcDiscover {}

#[async_trait]
impl RpcWithoutParams for RpcDiscover {
    // Named as per https://spec.open-rpc.org/#service-discovery-method.
    const METHOD: &'static str = "rpc.discover";
    type ResponseResult = RpcDiscoverResult;

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        Ok(RpcDiscoverResult::doc_example().clone())
    }
}

mod doc_example_impls {
    #[allow(deprecated)]
    use casper_types::AuctionState;
    use casper_types::{
        account::Account, Deploy, EraEndV1, EraEndV2, EraReport, PublicKey, Timestamp, Transaction,
    };

    use super::DocExample;

    impl DocExample for Deploy {
        fn doc_example() -> &'static Self {
            Deploy::example()
        }
    }

    impl DocExample for Transaction {
        fn doc_example() -> &'static Self {
            Transaction::example()
        }
    }

    impl DocExample for Account {
        fn doc_example() -> &'static Self {
            Account::example()
        }
    }

    impl DocExample for EraEndV1 {
        fn doc_example() -> &'static Self {
            EraEndV1::example()
        }
    }

    impl DocExample for EraEndV2 {
        fn doc_example() -> &'static Self {
            EraEndV2::example()
        }
    }

    impl DocExample for EraReport<PublicKey> {
        fn doc_example() -> &'static Self {
            EraReport::<PublicKey>::example()
        }
    }

    impl DocExample for Timestamp {
        fn doc_example() -> &'static Self {
            Timestamp::example()
        }
    }

    #[allow(deprecated)]
    impl DocExample for AuctionState {
        fn doc_example() -> &'static Self {
            AuctionState::example()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn check_optional_params_fields<T: RpcWithOptionalParams>() -> Vec<SchemaParam> {
        let contact = OpenRpcContactField {
            name: "Casper Labs".to_string(),
            url: "https://casperlabs.io".to_string(),
        };
        let license = OpenRpcLicenseField {
            name: "APACHE LICENSE, VERSION 2.0".to_string(),
            url: "https://www.apache.org/licenses/LICENSE-2.0".to_string(),
        };
        let info = OpenRpcInfoField {
            version: DOCS_EXAMPLE_API_VERSION.to_string(),
            title: "Client API of Casper Node".to_string(),
            description: "This describes the JSON-RPC 2.0 API of a node on the Casper network."
                .to_string(),
            contact,
            license,
        };
        let schema = OpenRpcSchema::new("1.0.0-rc1".to_string(), info, vec![SERVER.clone()]);
        let params = schema.give_params_schema::<T>();
        let schema_object = params.into_object().object.expect("should be object");
        schema_object
            .properties
            .iter()
            .filter(|(name, _)| !schema_object.required.contains(*name))
            .map(|(name, schema)| SchemaParam {
                name: name.clone(),
                schema: schema.clone(),
                required: false,
            })
            .collect::<Vec<_>>()
    }

    #[test]
    fn check_chain_get_block_required_fields() {
        let incorrect_optional_params = check_optional_params_fields::<GetBlock>();
        assert!(incorrect_optional_params.is_empty())
    }

    #[test]
    fn check_chain_get_block_transfers_required_fields() {
        let incorrect_optional_params = check_optional_params_fields::<GetBlockTransfers>();
        assert!(incorrect_optional_params.is_empty())
    }

    #[test]
    fn check_chain_get_state_root_hash_required_fields() {
        let incorrect_optional_params = check_optional_params_fields::<GetStateRootHash>();
        assert!(incorrect_optional_params.is_empty())
    }

    #[test]
    fn check_chain_get_era_info_by_switch_block_required_fields() {
        let incorrect_optional_params = check_optional_params_fields::<GetEraInfoBySwitchBlock>();
        assert!(incorrect_optional_params.is_empty())
    }

    #[test]
    fn check_state_get_auction_info_required_fields() {
        let incorrect_optional_params = check_optional_params_fields::<GetAuctionInfo>();
        assert!(incorrect_optional_params.is_empty())
    }

    #[test]
    fn check_state_get_auction_info_v2_required_fields() {
        let incorrect_optional_params = check_optional_params_fields::<GetAuctionInfoV2>();
        assert!(incorrect_optional_params.is_empty())
    }
}
