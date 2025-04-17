use std::sync::LazyLock;

use super::{
    docs::{
        CONTACT, DOCS_EXAMPLE_API_VERSION, LICENSE, OPEN_RPC_VERSION, OpenRpcInfoField,
        OpenRpcSchema, OpenRpcServerEntry,
    },
    speculative_exec::{SpeculativeExec, SpeculativeExecTxn},
};

pub(crate) static SERVER: LazyLock<OpenRpcServerEntry> = LazyLock::new(|| {
    OpenRpcServerEntry::new(
        "any Sidecar with speculative JSON RPC API enabled".to_string(),
        "http://IP:PORT/rpc/".to_string(),
    )
});

pub(crate) static SPECULATIVE_OPEN_RPC_SCHEMA: LazyLock<OpenRpcSchema> = LazyLock::new(|| {
    let info = OpenRpcInfoField::new(
        DOCS_EXAMPLE_API_VERSION.to_string(),
        "Speculative execution client API of Casper Node".to_string(),
        "This describes the JSON-RPC 2.0 API of the speculative execution functinality of a node on the Casper network."
            .to_string(),
        CONTACT.clone(),
        LICENSE.clone(),
    );
    let mut schema = OpenRpcSchema::new(OPEN_RPC_VERSION.to_string(), info, vec![SERVER.clone()]);
    schema.push_with_params::<SpeculativeExec>(
        "receives a Deploy to be executed by the network (DEPRECATED: use \
        `account_put_transaction` instead)",
    );
    schema.push_with_params::<SpeculativeExecTxn>(
        "receives a Deploy to be executed by the network (DEPRECATED: use \
        `account_put_transaction` instead)",
    );
    schema
});
