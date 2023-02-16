use casper_event_types::SseData;
use std::fmt::{Display, Formatter};

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum EventType {
    ApiVersion,
    BlockAdded,
    DeployAccepted,
    DeployExpired,
    DeployProcessed,
    Fault,
    FinalitySignature,
    Step,
    Shutdown,
}

impl From<SseData> for EventType {
    fn from(sse_data: SseData) -> Self {
        match sse_data {
            SseData::ApiVersion(_) => EventType::ApiVersion,
            SseData::BlockAdded { .. } => EventType::BlockAdded,
            SseData::DeployAccepted { .. } => EventType::DeployAccepted,
            SseData::DeployProcessed { .. } => EventType::DeployProcessed,
            SseData::DeployExpired { .. } => EventType::DeployExpired,
            SseData::Fault { .. } => EventType::Fault,
            SseData::FinalitySignature(_) => EventType::FinalitySignature,
            SseData::Step { .. } => EventType::Step,
            SseData::Shutdown => EventType::Shutdown,
        }
    }
}

impl Display for EventType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let string = match self {
            EventType::ApiVersion => "ApiVersion",
            EventType::BlockAdded => "BlockAdded",
            EventType::DeployAccepted => "DeployAccepted",
            EventType::DeployExpired => "DeployExpired",
            EventType::DeployProcessed => "DeployProcessed",
            EventType::Fault => "Fault",
            EventType::FinalitySignature => "FinalitySignature",
            EventType::Step => "Step",
            EventType::Shutdown => "Shutdown",
        };
        write!(f, "{}", string)
    }
}
