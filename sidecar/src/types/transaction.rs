use async_trait::async_trait;

use super::database::DatabaseWriteError;


#[allow(dead_code)] //Allowing dead code here because the Raw enum is used only in ITs
pub enum TransactionStatement{
    SelectStatement(sea_query::SelectStatement),
    InsertStatement(sea_query::InsertStatement),
    MultiInsertStatement(Vec<sea_query::InsertStatement>),
    Raw(String),
}

#[allow(dead_code)] //Allowing dead code here because the Raw enum is used only in ITs
pub enum MaterializedTransactionStatementWrapper{
    SelectStatement(String),
    InsertStatement(String),
    Raw(String),
}

#[allow(dead_code)] 
//Allowing dead code here because the Raw enum is used only in ITs
pub enum TransactionStatementResult {
    //json representation of results
    SelectResult(Vec<String>),
    //json representation of results
    InsertStatement(u32),
    //json representation of results
    Raw(),
}

#[async_trait]
pub trait TransactionWrapper: Send + Sync {
    fn materialize(&self, statement: TransactionStatement) -> MaterializedTransactionStatementWrapper;
    async fn execute(&self, sql: MaterializedTransactionStatementWrapper) -> Result<TransactionStatementResult, DatabaseWriteError>;
}
