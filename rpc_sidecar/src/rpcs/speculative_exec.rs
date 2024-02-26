//! RPC related to speculative execution.

use std::{str, sync::Arc};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use casper_types::{
    contract_messages::Messages, execution::ExecutionResultV2, BlockHash, BlockIdentifier, Deploy,
    Transaction,
};

use super::{
    common,
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, Error, NodeClient, RpcError, RpcWithParams, CURRENT_API_VERSION,
};

static SPECULATIVE_EXEC_TXN_PARAMS: Lazy<SpeculativeExecTxnParams> =
    Lazy::new(|| SpeculativeExecTxnParams {
        block_identifier: Some(BlockIdentifier::Hash(*BlockHash::example())),
        transaction: Transaction::doc_example().clone(),
    });
static SPECULATIVE_EXEC_TXN_RESULT: Lazy<SpeculativeExecTxnResult> =
    Lazy::new(|| SpeculativeExecTxnResult {
        api_version: DOCS_EXAMPLE_API_VERSION,
        block_hash: *BlockHash::example(),
        execution_result: ExecutionResultV2::example().clone(),
        messages: Vec::new(),
    });
static SPECULATIVE_EXEC_PARAMS: Lazy<SpeculativeExecParams> = Lazy::new(|| SpeculativeExecParams {
    block_identifier: Some(BlockIdentifier::Hash(*BlockHash::example())),
    deploy: Deploy::doc_example().clone(),
});

/// Params for "speculative_exec_txn" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpeculativeExecTxnParams {
    /// Block hash on top of which to execute the transaction.
    pub block_identifier: Option<BlockIdentifier>,
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
    /// Hash of the block on top of which the transaction was executed.
    pub block_hash: BlockHash,
    /// Result of the execution.
    pub execution_result: ExecutionResultV2,
    /// Messages emitted during execution.
    pub messages: Messages,
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
        handle_request(node_client, params.block_identifier, params.transaction).await
    }
}

/// Params for "speculative_exec" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct SpeculativeExecParams {
    /// Block hash on top of which to execute the deploy.
    pub block_identifier: Option<BlockIdentifier>,
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
        handle_request(node_client, params.block_identifier, params.deploy.into()).await
    }
}

async fn handle_request(
    node_client: Arc<dyn NodeClient>,
    identifier: Option<BlockIdentifier>,
    transaction: Transaction,
) -> Result<SpeculativeExecTxnResult, RpcError> {
    let block_header = common::get_block_header(&*node_client, identifier).await?;
    let block_hash = block_header.block_hash();
    let state_root_hash = *block_header.state_root_hash();
    let block_time = block_header.timestamp();
    let protocol_version = block_header.protocol_version();

    let (execution_result, messages) = node_client
        .exec_speculatively(
            state_root_hash,
            block_time,
            protocol_version,
            transaction,
            block_header,
        )
        .await
        .map_err(|err| Error::NodeRequest("speculatively executing a transaction", err))?
        .into_inner()
        .ok_or(Error::SpecExecReturnedNothing)?;

    Ok(SpeculativeExecTxnResult {
        api_version: CURRENT_API_VERSION,
        block_hash,
        execution_result,
        messages,
    })
}

#[cfg(test)]
mod tests {
    use std::convert::TryFrom;

    use casper_types::{
        binary_port::{
            BinaryRequest, BinaryResponse, BinaryResponseAndRequest, GetRequest,
            InformationRequestTag, SpeculativeExecutionResult,
        },
        testing::TestRng,
        Block, TestBlockBuilder,
    };

    use crate::{ClientError, SUPPORTED_PROTOCOL_VERSION};

    use super::*;

    #[tokio::test]
    async fn should_spec_exec() {
        let rng = &mut TestRng::new();
        let deploy = Deploy::random(rng);
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let execution_result = ExecutionResultV2::random(rng);

        let res = SpeculativeExec::do_handle_request(
            Arc::new(ValidSpecExecMock {
                block: block.clone(),
                execution_result: execution_result.clone(),
            }),
            SpeculativeExecParams {
                block_identifier: Some(BlockIdentifier::Hash(*block.hash())),
                deploy,
            },
        )
        .await
        .expect("should handle request");
        assert_eq!(
            res,
            SpeculativeExecTxnResult {
                block_hash: *block.hash(),
                execution_result,
                messages: Messages::new(),
                api_version: CURRENT_API_VERSION,
            }
        )
    }

    #[tokio::test]
    async fn should_spec_exec_txn() {
        let rng = &mut TestRng::new();
        let transaction = Transaction::random(rng);
        let block = Block::V2(TestBlockBuilder::new().build(rng));
        let execution_result = ExecutionResultV2::random(rng);

        let res = SpeculativeExecTxn::do_handle_request(
            Arc::new(ValidSpecExecMock {
                block: block.clone(),
                execution_result: execution_result.clone(),
            }),
            SpeculativeExecTxnParams {
                block_identifier: Some(BlockIdentifier::Hash(*block.hash())),
                transaction,
            },
        )
        .await
        .expect("should handle request");
        assert_eq!(
            res,
            SpeculativeExecTxnResult {
                block_hash: *block.hash(),
                execution_result,
                messages: Messages::new(),
                api_version: CURRENT_API_VERSION,
            }
        )
    }

    struct ValidSpecExecMock {
        block: Block,
        execution_result: ExecutionResultV2,
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
                            self.block.clone_header(),
                            SUPPORTED_PROTOCOL_VERSION,
                        ),
                        &[],
                    ))
                }
                BinaryRequest::TrySpeculativeExec { .. } => Ok(BinaryResponseAndRequest::new(
                    BinaryResponse::from_value(
                        SpeculativeExecutionResult::new(Some((
                            self.execution_result.clone(),
                            Messages::new(),
                        ))),
                        SUPPORTED_PROTOCOL_VERSION,
                    ),
                    &[],
                )),
                req => unimplemented!("unexpected request: {:?}", req),
            }
        }
    }
}
