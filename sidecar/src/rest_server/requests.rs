use crate::rest_server::errors::InvalidParam;
use crate::types::database::{DeployAggregateSortColumn, SortOrder};
use anyhow::Error;
use serde::{Deserialize, Serialize};
use warp::Rejection;
const DEFAULT_LIMIT: u32 = 10;
const MAX_LIMIT: u32 = 100;

// The query parameters for list_deploys.
// Serialize is used to satisfy tests
#[derive(Debug, Deserialize, Clone, Serialize)]
pub struct ListDeploysRequest {
    pub exclude_expired: Option<bool>,
    pub exclude_not_processed: Option<bool>,
    pub limit: Option<u32>,
    pub offset: Option<u32>,
    pub sort_column: Option<DeployAggregateSortColumn>,
    pub sort_order: Option<SortOrder>,
}

impl ListDeploysRequest {
    pub fn get_limit(&self) -> u32 {
        self.limit.unwrap_or(DEFAULT_LIMIT)
    }

    pub fn validate(&self) -> Result<(), Rejection> {
        let limit = self.limit.unwrap_or(0);
        if limit > MAX_LIMIT {
            return Err(warp::reject::custom(InvalidParam(Error::msg(format!(
                "Expected limit to be less than {}, received: {}",
                limit, MAX_LIMIT
            )))));
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, Clone)]
pub struct Page<T> {
    pub data: Vec<T>,
    pub offset: u32,
    pub limit: u32,
    pub item_count: u32,
}
