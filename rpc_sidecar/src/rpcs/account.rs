//! RPCs related to accounts.

use std::{str, sync::Arc};

use async_trait::async_trait;
use once_cell::sync::Lazy;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use casper_types::{Deploy, DeployHash, Transaction, TransactionHash};

use super::{
    docs::{DocExample, DOCS_EXAMPLE_API_VERSION},
    ApiVersion, ClientError, Error, NodeClient, RpcError, RpcWithParams, CURRENT_API_VERSION,
};

static PUT_DEPLOY_PARAMS: Lazy<PutDeployParams> = Lazy::new(|| PutDeployParams {
    deploy: Deploy::doc_example().clone(),
});
static PUT_DEPLOY_RESULT: Lazy<PutDeployResult> = Lazy::new(|| PutDeployResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    deploy_hash: *Deploy::doc_example().hash(),
});

static PUT_TRANSACTION_PARAMS: Lazy<PutTransactionParams> = Lazy::new(|| PutTransactionParams {
    transaction: Transaction::doc_example().clone(),
});
static PUT_TRANSACTION_RESULT: Lazy<PutTransactionResult> = Lazy::new(|| PutTransactionResult {
    api_version: DOCS_EXAMPLE_API_VERSION,
    transaction_hash: Transaction::doc_example().hash(),
});

/// Params for "account_put_deploy" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PutDeployParams {
    /// The `Deploy`.
    pub deploy: Deploy,
}

impl DocExample for PutDeployParams {
    fn doc_example() -> &'static Self {
        &PUT_DEPLOY_PARAMS
    }
}

/// Result for "account_put_deploy" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PutDeployResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The deploy hash.
    pub deploy_hash: DeployHash,
}

impl DocExample for PutDeployResult {
    fn doc_example() -> &'static Self {
        &PUT_DEPLOY_RESULT
    }
}

/// "account_put_deploy" RPC
pub struct PutDeploy {}

#[async_trait]
impl RpcWithParams for PutDeploy {
    const METHOD: &'static str = "account_put_deploy";
    type RequestParams = PutDeployParams;
    type ResponseResult = PutDeployResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let deploy_hash = *params.deploy.hash();
        match node_client
            .try_accept_transaction(params.deploy.into())
            .await
        {
            Ok(()) => Ok(Self::ResponseResult {
                api_version: CURRENT_API_VERSION,
                deploy_hash,
            }),
            Err(err @ ClientError::InvalidTransaction) => {
                Err(Error::InvalidDeploy(err.to_string()).into())
            }
            Err(err) => Err(Error::NodeRequest("submitting a deploy", err).into()),
        }
    }
}

/// Params for "account_put_transaction" RPC request.
#[derive(Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PutTransactionParams {
    /// The `Transaction`.
    pub transaction: Transaction,
}

impl DocExample for PutTransactionParams {
    fn doc_example() -> &'static Self {
        &PUT_TRANSACTION_PARAMS
    }
}

/// Result for "account_put_transaction" RPC response.
#[derive(PartialEq, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct PutTransactionResult {
    /// The RPC API version.
    #[schemars(with = "String")]
    pub api_version: ApiVersion,
    /// The transaction hash.
    pub transaction_hash: TransactionHash,
}

impl DocExample for PutTransactionResult {
    fn doc_example() -> &'static Self {
        &PUT_TRANSACTION_RESULT
    }
}

/// "account_put_transaction" RPC
pub struct PutTransaction {}

#[async_trait]
impl RpcWithParams for PutTransaction {
    const METHOD: &'static str = "account_put_transaction";
    type RequestParams = PutTransactionParams;
    type ResponseResult = PutTransactionResult;

    async fn do_handle_request(
        node_client: Arc<dyn NodeClient>,
        params: Self::RequestParams,
    ) -> Result<Self::ResponseResult, RpcError> {
        let transaction_hash = params.transaction.hash();
        match node_client.try_accept_transaction(params.transaction).await {
            Ok(()) => Ok(Self::ResponseResult {
                api_version: CURRENT_API_VERSION,
                transaction_hash,
            }),
            Err(err @ ClientError::InvalidTransaction) => {
                Err(Error::InvalidTransaction(err.to_string()).into())
            }
            Err(err) => Err(Error::NodeRequest("submitting a transaction", err).into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use casper_types::{
        binary_port::{
            BinaryRequest, BinaryResponse, BinaryResponseAndRequest,
            ErrorCode as BinaryPortErrorCode,
        },
        testing::TestRng,
    };

    use crate::{rpcs::ErrorCode, SUPPORTED_PROTOCOL_VERSION};

    use super::*;

    #[tokio::test]
    async fn should_put_deploy() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::TryAcceptTransaction { .. } => {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_empty(SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    _ => unimplemented!(),
                }
            }
        }

        let rng = &mut TestRng::new();
        let deploy = Deploy::random(rng);
        let res = PutDeploy::do_handle_request(
            Arc::new(ClientMock),
            PutDeployParams {
                deploy: deploy.clone(),
            },
        )
        .await
        .expect("should handle request");
        assert_eq!(
            res,
            PutDeployResult {
                api_version: CURRENT_API_VERSION,
                deploy_hash: *deploy.hash(),
            }
        )
    }

    #[tokio::test]
    async fn should_put_transaction() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::TryAcceptTransaction { .. } => {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_empty(SUPPORTED_PROTOCOL_VERSION),
                            &[],
                        ))
                    }
                    _ => unimplemented!(),
                }
            }
        }

        let rng = &mut TestRng::new();
        let transaction = Transaction::random(rng);
        let res = PutTransaction::do_handle_request(
            Arc::new(ClientMock),
            PutTransactionParams {
                transaction: transaction.clone(),
            },
        )
        .await
        .expect("should handle request");
        assert_eq!(
            res,
            PutTransactionResult {
                api_version: CURRENT_API_VERSION,
                transaction_hash: transaction.hash(),
            }
        )
    }

    #[tokio::test]
    async fn should_handle_transaction_error() {
        struct ClientMock;

        #[async_trait]
        impl NodeClient for ClientMock {
            async fn send_request(
                &self,
                req: BinaryRequest,
            ) -> Result<BinaryResponseAndRequest, ClientError> {
                match req {
                    BinaryRequest::TryAcceptTransaction { .. } => {
                        Ok(BinaryResponseAndRequest::new(
                            BinaryResponse::new_error(
                                BinaryPortErrorCode::InvalidTransaction,
                                SUPPORTED_PROTOCOL_VERSION,
                            ),
                            &[],
                        ))
                    }
                    _ => unimplemented!(),
                }
            }
        }

        let rng = &mut TestRng::new();
        let transaction = Transaction::random(rng);
        let err = PutTransaction::do_handle_request(
            Arc::new(ClientMock),
            PutTransactionParams {
                transaction: transaction.clone(),
            },
        )
        .await
        .expect_err("should reject request");

        assert_eq!(err.code(), ErrorCode::InvalidTransaction as i64,)
    }
}
