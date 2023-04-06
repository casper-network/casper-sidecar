/// A deploy; an item containing a smart contract along with the requester's signature(s).
#[derive(Clone, DataSize, Eq, Serialize, Deserialize, Debug, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Deploy {
    hash: DeployHash,
    header: DeployHeader,
    payment: ExecutableDeployItem,
    session: ExecutableDeployItem,
    approvals: BTreeSet<Approval>,
    #[serde(skip)]
    #[data_size(with = ds::once_cell)]
    is_valid: OnceCell<Result<(), DeployConfigurationFailure>>,
}


#[serde(deny_unknown_fields)]
pub struct DeployHash(#[schemars(skip)] Digest);