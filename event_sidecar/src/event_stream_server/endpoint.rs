#[cfg(test)]
use std::fmt::{Display, Formatter};

/// Enum representing all possible endpoints sidecar can have.
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Endpoint {
    Events,
    Sidecar,
}

#[cfg(test)]
impl Display for Endpoint {
    /// This implementation is for test only and created to mimick how Display is implemented for Filter.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Events => write!(f, "events"),
            Endpoint::Sidecar => write!(f, "events/sidecar"),
        }
    }
}
