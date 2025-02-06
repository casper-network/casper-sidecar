use std::fmt::{Display, Formatter};

/// Enum representing all the possible endpoints a node can have.
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Filter {
    Events,
}

impl Display for Filter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Filter::Events => write!(f, "events"),
        }
    }
}
