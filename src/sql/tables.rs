// use sea_query::{Expr, JoinType, Query, SelectStatement, SqliteQueryBuilder};
// use std::path::Path;

pub mod block_added;
pub mod deploy_accepted;
pub mod deploy_event;
pub mod deploy_event_type;
pub mod deploy_expired;
pub mod deploy_processed;
pub mod event_log;
pub mod event_source;
pub mod event_source_of_event;
pub mod event_type;
pub mod fault;
pub mod finality_signature;
pub mod step;

// use crate::SqliteDb;
// use deploy_accepted::DeployAccepted;
// use deploy_expired::DeployExpired;
// use deploy_processed::DeployProcessed;

// pub fn create_get_deploy_by_hash_aggregate_stmt(hash: &str) -> SelectStatement {
//     Query::select()
//         .column(DeployAccepted::Raw)
//         .from(DeployAccepted::Table)
//         .and_where(Expr::col(DeployAccepted::DeployHash).eq(hash))
//         .join(
//             JoinType::InnerJoin,
//             DeployProcessed::Table,
//             Expr::tbl(DeployAccepted::Table, DeployAccepted::DeployHash)
//                 .equals(DeployProcessed::Table, DeployProcessed::DeployHash),
//         )
//         .join(
//             JoinType::InnerJoin,
//             DeployExpired::Table,
//             Expr::tbl(DeployAccepted::Table, DeployAccepted::DeployHash)
//                 .equals(DeployExpired::Table, DeployExpired::DeployHash),
//         )
//         .to_owned()
// }

// #[test]
// fn check() {
//     let stmt = create_get_deploy_by_hash_aggregate_stmt("example").to_string(SqliteQueryBuilder);
//
//     println!("{}", stmt);
// }
