use casper_types::{
    contract_messages::Messages, execution::Effects, BlockHash, Digest, EraId, Gas, PublicKey,
    Timestamp, Transfer,
};
use schemars::JsonSchema;

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(deny_unknown_fields, rename = "MinimalBlockInfo")]
/// Minimal info about a `Block` needed to satisfy the node status request.
pub(crate) struct MinimalBlockInfoSchema {
    hash: BlockHash,
    timestamp: Timestamp,
    era_id: EraId,
    height: u64,
    state_root_hash: Digest,
    creator: PublicKey,
}

#[allow(dead_code)]
#[derive(JsonSchema)]
#[schemars(rename = "SpeculativeExecutionResult")]
pub(crate) struct SpeculativeExecutionResultSchema {
    /// Block hash against which the execution was performed.
    block_hash: BlockHash,
    /// List of transfers that happened during execution.
    transfers: Vec<Transfer>,
    /// Gas limit.
    limit: Gas,
    /// Gas consumed.
    consumed: Gas,
    /// Execution effects.
    effects: Effects,
    /// Messages emitted during execution.
    messages: Messages,
    /// Did the wasm execute successfully?
    error: Option<String>,
}

#[cfg(test)]
mod tests {
    use casper_binary_port::SpeculativeExecutionResult;
    use casper_types::testing::TestRng;
    use schemars::schema_for;
    use serde_json::json;

    use crate::rpcs::types::SpeculativeExecutionResultSchema;

    #[test]
    pub fn speculative_execution_result_should_validate_against_schema() {
        let mut test_rng = TestRng::new();
        let ser = SpeculativeExecutionResult::random(&mut test_rng);
        let schema_struct = schema_for!(SpeculativeExecutionResultSchema);

        let schema = json!(schema_struct);
        let instance = serde_json::to_value(&ser).expect("should json-serialize result");

        assert!(jsonschema::is_valid(&schema, &instance));
    }
}
