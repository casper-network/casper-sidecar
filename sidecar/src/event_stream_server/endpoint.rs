use casper_event_types::Filter;
#[cfg(test)]
use std::fmt::{Display, Formatter};

/// Enum representing all possible endpoints sidecar can have.
/// Be advised that extending variants in this enum requires
/// an update in `is_corresponding_to` function.
#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub enum Endpoint {
    Events,
    Main,
    Deploys,
    Sigs,
    Sidecar,
}

impl Endpoint {
    pub fn is_corresponding_to(&self, filter: &Filter) -> bool {
        matches!(
            (self, filter.clone()),
            (Endpoint::Events, Filter::Events)
                | (Endpoint::Main, Filter::Main)
                | (Endpoint::Deploys, Filter::Deploys)
                | (Endpoint::Sigs, Filter::Sigs)
        )
    }
}

#[cfg(test)]
impl Display for Endpoint {
    /// This implementation is for test only and created to mimick how Display is implemented for Filter.
    /// We use this trick to easily test `is_corresponding_to` with all possible inputs.
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

#[cfg(test)]
mod tests {
    use super::Endpoint;
    use casper_event_types::Filter;

    #[test]
    fn try_resolve_version_should_interpret_correct_build_version() {
        let all_filters = vec![Filter::Events, Filter::Main, Filter::Deploys, Filter::Sigs];
        let all_endpoints = vec![
            Endpoint::Events,
            Endpoint::Main,
            Endpoint::Deploys,
            Endpoint::Sigs,
            Endpoint::Sidecar,
        ];
        for endpoint in all_endpoints.iter() {
            for filter in all_filters.iter() {
                let endpoint_str = endpoint.to_string();
                let filter_str = filter.to_string();
                let should_be_correspodning = endpoint_str == filter_str;
                assert_eq!(
                    should_be_correspodning,
                    endpoint.is_corresponding_to(filter)
                );
            }
        }
    }
}
