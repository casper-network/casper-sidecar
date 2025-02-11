//! RPC related to speculative execution.

use std::{str, sync::Arc};

use async_trait::async_trait;
use casper_binary_port::SpeculativeExecutionResult;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use casper_types::{Deploy, Transaction};

use super::{
    docs::{DocExample, OpenRpcSchema, DOCS_EXAMPLE_API_VERSION},
    speculative_open_rpc_schema::SPECULATIVE_OPEN_RPC_SCHEMA,
    ApiVersion, Error, NodeClient, RpcError, RpcWithParams, RpcWithoutParams, CURRENT_API_VERSION,
};

static SPECULATIVE_EXEC_TXN_PARAMS: Lazy<SpeculativeExecTxnParams> =
    Lazy::new(|| SpeculativeExecTxnParams {
        transaction: Transaction::doc_example().clone(),
    });
static SPECULATIVE_EXEC_TXN_RESULT: Lazy<SpeculativeExecTxnResult> =
    Lazy::new(|| SpeculativeExecTxnResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        execution_result: SpeculativeExecutionResult::example().clone(),
    });
static SPECULATIVE_EXEC_PARAMS: Lazy<SpeculativeExecParams> = Lazy::new(|| SpeculativeExecParams {
    deploy: Deploy::doc_example().clone(),
});

/// Params for "speculative_exec_txn" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpeculativeExecTxnParams {
    /// Transaction to execute.
    pub transaction: Transaction,
}

impl DocExample for SpeculativeExecTxnParams {
    fn doc_example() -> &'static Self {
        &SPECULATIVE_EXEC_TXN_PARAMS
    }
}

/// Result for "speculative_exec_txn" and "speculative_exec" RPC responses.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpeculativeExecTxnResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// Result of the speculative execution.
    #[schemars(with = "crate::rpcs::types::SpeculativeExecutionResultSchema")]
    pub execution_result: SpeculativeExecutionResult,
}

impl DocExample for SpeculativeExecTxnResult {
    fn doc_example() -> &'static Self {
        &SPECULATIVE_EXEC_TXN_RESULT
    }
}

/// "speculative_exec_txn" RPC
pub struct SpeculativeExecTxn {}

#[async_trait]
impl RpcWithParams for SpeculativeExecTxn {
    const METHOD: &'static str = "speculative_exec_txn";
    type RequestParams = SpeculativeExecTxnParams;
    type ResponseResult = SpeculativeExecTxnResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        handle_request(node_client, params.transaction).await
    }
}

/// Params for "speculative_exec" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpeculativeExecParams {
    /// Deploy to execute.
    pub deploy: Deploy,
}

impl DocExample for SpeculativeExecParams {
    fn doc_example() -> &'static Self {
        &SPECULATIVE_EXEC_PARAMS
    }
}

/// "speculative_exec" RPC
pub struct SpeculativeExec {}

#[async_trait]
impl RpcWithParams for SpeculativeExec {
    const METHOD: &'static str = "speculative_exec";
    type RequestParams = SpeculativeExecParams;
    type ResponseResult = SpeculativeExecTxnResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        handle_request(node_client, params.deploy.into()).await
    }
}

async fn handle_request(
    node_client: Arc<dyn NodeClient>,
    transaction: Transaction,
) -> Result<SpeculativeExecTxnResult, RpcError> {
    let speculative_execution_result = node_client
        .exec_speculatively(transaction)
        .await
        .map_err(|err| Error::NodeRequest("speculatively executing a transaction", err))?;

    Ok(SpeculativeExecTxnResult {
        api_version: CURRENT_API_VERSION,
        execution_result: speculative_execution_result,
    })
}

#[derive(Clone, PartialEq, Serialize, Deserialize, JsonSchema, Debug)]
#[serde(deny_unknown_fields)]
pub struct SpeculativeRpcDiscoverResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    api_version: ApiVersion,
    name: String,
    /// The list of supported RPCs.
    #[schemars(skip)]
    schema: OpenRpcSchema,
}

static SPECULATIVE_DISCOVER_RPC_RESULT: Lazy<SpeculativeRpcDiscoverResult> =
    Lazy::new(|| SpeculativeRpcDiscoverResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        name: "OpenRPC Schema for speculative exectution server".to_string(),
        schema: SPECULATIVE_OPEN_RPC_SCHEMA.clone(),
    });

impl DocExample for SpeculativeRpcDiscoverResult {
    fn doc_example() -> &'static Self {
        &SPECULATIVE_DISCOVER_RPC_RESULT
    }
}

/// "rpc.discover" RPC.
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize, Debug)]
pub struct SpeculativeRpcDiscover {}

#[async_trait]
impl RpcWithoutParams for SpeculativeRpcDiscover {
    const METHOD: &'static str = "rpc.discover";
    type ResponseResult = SpeculativeRpcDiscoverResult;

    async fn do_handle_request(
        _node_client: Arc<dyn NodeClient>,
    ) -> Result<Self::ResponseResult, RpcError> {
        Ok(SpeculativeRpcDiscoverResult::doc_example().clone())
    }
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use casper_binary_port::{
        BinaryResponse, BinaryResponseAndRequest, Command, GetRequest, InformationRequestTag,
        SpeculativeExecutionResult,
    };
    use casper_types::{bytesrepr::Bytes, testing::TestRng};
    use pretty_assertions::assert_eq;

    use crate::ClientError;

    use super::*;

    #[tokio::test]
    async fn should_spec_exec() {
        let rng = &mut TestRng::new();
        let deploy = Deploy::random(rng);
        let execution_result = SpeculativeExecutionResult::random(rng);

        let res = SpeculativeExec::do_handle_request(
            Arc::new(ValidSpecExecMock {
                execution_result: execution_result.clone(),
            }),
            SpeculativeExecParams { deploy },
        )
        .await
        .expect("should handle request");
        assert_eq!(
            res,
            SpeculativeExecTxnResult {
                execution_result,
                api_version: CURRENT_API_VERSION,
            }
        )
    }

    #[tokio::test]
    async fn should_spec_exec_txn() {
        let rng = &mut TestRng::new();
        let transaction = Transaction::random(rng);
        let execution_result = SpeculativeExecutionResult::random(rng);

        let res = SpeculativeExecTxn::do_handle_request(
            Arc::new(ValidSpecExecMock {
                execution_result: execution_result.clone(),
            }),
            SpeculativeExecTxnParams { transaction },
        )
        .await
        .expect("should handle request");
        assert_eq!(
            res,
            SpeculativeExecTxnResult {
                execution_result,
                api_version: CURRENT_API_VERSION,
            }
        )
    }

    struct ValidSpecExecMock {
        execution_result: SpeculativeExecutionResult,
    }

    #[async_trait]
    impl NodeClient for ValidSpecExecMock {
        async fn send_request(
            &self,
            req: Command,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                Command::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::BlockHeader) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(self.execution_result.clone()),
                        Bytes::from(vec![]),
                    ))
                }
                Command::TrySpeculativeExec { .. } => Ok(BinaryResponseAndRequest::new(
                    BinaryResponse::from_value(self.execution_result.clone()),
                    Bytes::from(vec![]),
                )),
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }
}
