use std::fmt::{Display, Formatter};


#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Filter {
    Events,
    Main,
    Deploys,
    Sigs,
}

impl Display for Filter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Filter::Events => write!(f, "events"),
            Filter::Main => write!(f, "events/main"),
            Filter::Deploys => write!(f, "events/deploys"),
            Filter::Sigs => write!(f, "events/sigs"),
        }
    }
}