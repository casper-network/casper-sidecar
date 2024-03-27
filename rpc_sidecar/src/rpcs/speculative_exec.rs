//! RPC related to speculative execution.

use std::{str, sync::Arc};

use async_trait::async_trait;
use casper_binary_port::SpeculativeExecutionResult;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use casper_types::{Deploy, Transaction};

use super::{
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, Error, NodeClient, RpcError, RpcWithParams, CURRENT_API_VERSION,
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

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use casper_binary_port::{
        BinaryRequest, BinaryResponse, BinaryResponseAndRequest, GetRequest, InformationRequestTag,
        SpeculativeExecutionResult,
    };
    use casper_types::testing::TestRng;
    use pretty_assertions::assert_eq;

    use crate::{ClientError, SUPPORTED_PROTOCOL_VERSION};

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
            req: BinaryRequest,
        ) -> Result<BinaryResponseAndRequest, ClientError> {
            match req {
                BinaryRequest::Get(GetRequest::Information { info_type_tag, .. })
                    if InformationRequestTag::try_from(info_type_tag)
                        == Ok(InformationRequestTag::BlockHeader) =>
                {
                    Ok(BinaryResponseAndRequest::new(
                        BinaryResponse::from_value(
                            self.execution_result.clone(),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                    ))
                }
                BinaryRequest::TrySpeculativeExec { .. } => Ok(BinaryResponseAndRequest::new(
                    BinaryResponse::from_value(
                        self.execution_result.clone(),
                        SUPPORTED_PROTOCOL_VERSION,
                    ),
                    &[],
                )),
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }
}
