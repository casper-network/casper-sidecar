use super::database::DatabaseWriteError;
use async_trait::async_trait;

#[allow(dead_code)] //Allowing dead code here because the Raw enum is used only in ITs
pub enum TransactionStatement {
    SelectStatement(sea_query::SelectStatement),
    InsertStatement(sea_query::InsertStatement),
    Raw(String),
}

/// This enum wraps sql statements so we can pass them to async functions. We can't do that with
/// [TransactionStatement] because the sea_query library has structs that aren't Send.
#[allow(dead_code)] //Allowing dead code here because the Raw enum is used only in ITs
pub enum MaterializedTransactionStatementWrapper {
    SelectStatement(String),
    InsertStatement(String),
    Raw(String),
}

#[allow(dead_code)]
/// This enum wraps possible results of a transaction execution in the migration mechanism
pub enum TransactionStatementResult {
    RawData(Vec<String>),
    U64(u64),
    NoResult(),
}

#[async_trait]
pub trait TransactionWrapper: Send + Sync {
    fn materialize(
        &self,
        statement: TransactionStatement,
    ) -> MaterializedTransactionStatementWrapper;
    async fn execute(
        &self,
        sql: MaterializedTransactionStatementWrapper,
    ) -> Result<TransactionStatementResult, DatabaseWriteError>;
}
