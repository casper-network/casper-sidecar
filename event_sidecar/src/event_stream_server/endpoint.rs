use std::fmt::{Display, Formatter};

/// Enum representing all possible endpoints sidecar can have.
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Endpoint {
    Events,
    Main,
    Deploys,
    Sigs,
    Sidecar,
}

impl Display for Endpoint {
    /// This implementation is for test only and created to mimick how Display is implemented for Filter.
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Endpoint::Events => write!(f, "events"),
            Endpoint::Main => write!(f, "events/main"),
            Endpoint::Deploys => write!(f, "events/deploys"),
            Endpoint::Sigs => write!(f, "events/sigs"),
            Endpoint::Sidecar => write!(f, "events/sidecar"),
        }
    }
}
