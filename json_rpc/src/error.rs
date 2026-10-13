use std::{borrow::Cow, fmt::Debug};

use serde::{Deserialize, Serialize};
use serde_json::Value;
use tracing::{error, warn};

/// A marker trait for a type suitable for use as an error code when constructing an [`Error`].
///
/// The implementing type must also implement `Into<(i64, &'static str)>` where the tuple represents
/// the "code" and "message" fields of the `Error`.
///
/// As per the JSON-RPC specification, the code must not fall in the reserved range, i.e. it must
/// not be between -32768 and -32000 inclusive.
///
/// Generally the "message" will be a brief const &str, where additional request-specific info can
/// be provided via the `additional_info` parameter of [`Error::new`].
///
/// # Example
///
/// ```
/// use serde::{Deserialize, Serialize};
/// use casper_json_rpc::ErrorCodeT;
///
/// #[derive(Copy, Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
/// #[repr(i64)]
/// pub enum ErrorCode {
///     /// The requested item was not found.
///     NoSuchItem = -1,
///     /// Failed to put the requested item to storage.
///     FailedToPutItem = -2,
/// }
///
/// impl From<ErrorCode> for (i64, &'static str) {
///     fn from(error_code: ErrorCode) -> Self {
///         match error_code {
///             ErrorCode::NoSuchItem => (error_code as i64, "No such item"),
///             ErrorCode::FailedToPutItem => (error_code as i64, "Failed to put item"),
///         }
///     }
/// }
///
/// impl ErrorCodeT for ErrorCode {}
/// ```
pub trait ErrorCodeT:
    Into<(i64, &'static str)> + for<'de> Deserialize<'de> + Copy + Eq + Debug
{
    /// Whether this type represents reserved error codes or not.
    ///
    /// This should normally be left with the default return value of `false`.
    #[doc(hidden)]
    #[must_use]
    fn is_reserved() -> bool {
        false
    }
}

/// The various reserved codes which can be returned in the JSON-RPC response's "error" object.
///
/// See [the JSON-RPC Specification](https://www.jsonrpc.org/specification#error_object) for further
/// details.
#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq)]
#[repr(i64)]
pub enum ReservedErrorCode {
    /// Invalid JSON was received by the server.
    ParseError = -32700,
    /// The JSON sent is not a valid Request object.
    InvalidRequest = -32600,
    /// The method does not exist or is not available.
    MethodNotFound = -32601,
    /// Invalid method parameter(s).
    InvalidParams = -32602,
    /// Internal JSON-RPC error.
    InternalError = -32603,
}

impl From<ReservedErrorCode> for (i64, &'static str) {
    fn from(error_code: ReservedErrorCode) -> Self {
        match error_code {
            ReservedErrorCode::ParseError => (error_code as i64, "Parse error"),
            ReservedErrorCode::InvalidRequest => (error_code as i64, "Invalid Request"),
            ReservedErrorCode::MethodNotFound => (error_code as i64, "Method not found"),
            ReservedErrorCode::InvalidParams => (error_code as i64, "Invalid params"),
            ReservedErrorCode::InternalError => (error_code as i64, "Internal error"),
        }
    }
}

impl ErrorCodeT for ReservedErrorCode {
    fn is_reserved() -> bool {
        true
    }
}

/// Custom RPC error codes.
#[derive(Copy, Clone, Debug, Deserialize, Eq, PartialEq)]
#[repr(i64)]
pub enum RpcErrorCode {
    /// Request throttled.
    RequestThrottled = 429,
}

impl ErrorCodeT for RpcErrorCode {}

impl From<RpcErrorCode> for (i64, &'static str) {
    fn from(error_code: RpcErrorCode) -> Self {
        match error_code {
            RpcErrorCode::RequestThrottled => (error_code as i64, "Request throttled"),
        }
    }
}

/// An object suitable to be returned in a JSON-RPC response as the "error" field.
///
/// See [the JSON-RPC Specification](https://www.jsonrpc.org/specification#error_object) for further
/// details.
#[derive(Clone, Eq, PartialEq, Serialize, Deserialize, Debug)]
#[serde(deny_unknown_fields)]
pub struct Error {
    /// A number that indicates the error type that occurred.
    code: i64,
    /// A short description of the error.
    message: Cow<'static, str>,
    /// Additional information about the error.
    data: Option<Value>,
    /// An HTTP status code to use for the response carrying this error, in place of the default
    /// `200 OK`. Not part of the JSON-RPC error object itself, so it's never serialized onto the
    /// wire - it only affects the outer HTTP response status (see
    /// [`Error::with_http_status_override`]).
    #[serde(skip)]
    http_status_override: Option<u16>,
}

impl Error {
    /// Returns a new `Error`, converting `error_code` to the "code" and "message" fields, and
    /// JSON-encoding `additional_info` as the "data" field.
    ///
    /// Other than when providing a [`ReservedErrorCode`], the converted "code" must not fall in the
    /// reserved range as defined in the JSON-RPC specification, i.e. it must not be between -32768
    /// and -32100 inclusive.
    ///
    /// Note that in an upcoming release, the restriction will be tightened to disallow error codes
    /// in the implementation-defined server-errors range.  I.e. codes in the range -32768 to -32000
    /// inclusive will be disallowed.
    ///
    /// If the converted code is within the reserved range when it should not be, or if
    /// JSON-encoding `additional_data` fails, the returned `Self` is built from
    /// [`ReservedErrorCode::InternalError`] with the "data" field being a String providing more
    /// info on the underlying error.
    pub fn new<C: ErrorCodeT, T: Serialize>(error_code: C, additional_info: T) -> Self {
        let (mut code, mut message): (i64, &'static str) = error_code.into();

        if !C::is_reserved() && (-32768..=-32100).contains(&code) {
            warn!(%code, "provided json-rpc error code is reserved; returning internal error");
            let (code, message) = ReservedErrorCode::InternalError.into();
            return Error {
                code,
                message: Cow::Borrowed(message),
                data: Some(Value::String(format!(
                    "attempted to return reserved error code {code}"
                ))),
                http_status_override: None,
            };
        }

        let data = match serde_json::to_value(additional_info) {
            Ok(Value::Null) => None,
            Ok(value) => Some(value),
            Err(error) => {
                error!(%error, "failed to json-encode additional info in json-rpc error");
                (code, message) = ReservedErrorCode::InternalError.into();
                Some(Value::String(format!(
                    "failed to json-encode additional info in json-rpc error: {error}"
                )))
            }
        };

        Error {
            code,
            message: Cow::Borrowed(message),
            data,
            http_status_override: None,
        }
    }

    /// Returns the code of the error.
    #[must_use]
    pub fn code(&self) -> i64 {
        self.code
    }

    /// Attaches an HTTP status code to use for the response carrying this error, in place of the
    /// default `200 OK`. This only changes the outer HTTP response status.
    #[must_use]
    pub fn with_http_status_override(mut self, status: u16) -> Self {
        self.http_status_override = Some(status);
        self
    }

    /// Returns the HTTP status code to use for the response carrying this error, if one was set
    /// via [`Error::with_http_status_override`].
    #[must_use]
    pub fn http_status_override(&self) -> Option<u16> {
        self.http_status_override
    }
}

#[cfg(test)]
mod tests {
    use serde::ser::{Error as _, Serializer};
    use serde_json::json;

    use super::*;

    #[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Serialize, Deserialize, Debug)]
    struct TestErrorCode {
        // If `true` the error code will be one in the reserved range.
        in_reserved_range: bool,
    }

    impl From<TestErrorCode> for (i64, &'static str) {
        fn from(error_code: TestErrorCode) -> Self {
            if error_code.in_reserved_range {
                (-32768, "Invalid test error")
            } else {
                (-123, "Valid test error")
            }
        }
    }

    impl ErrorCodeT for TestErrorCode {}

    #[derive(Serialize)]
    struct AdditionalInfo {
        id: u64,
        context: &'static str,
    }

    impl AdditionalInfo {
        #[cfg(test)]
        pub fn test_default() -> Self {
            AdditionalInfo {
                id: 1314,
                context: "TEST",
            }
        }
    }

    struct FailToEncode;

    impl Serialize for FailToEncode {
        fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
            Err(S::Error::custom("won't encode"))
        }
    }

    #[test]
    fn should_construct_reserved_error() {
        let error_with_data = Error::new(
            ReservedErrorCode::ParseError,
            AdditionalInfo::test_default(),
        );
        assert_eq!(
            serde_json::to_value(error_with_data).unwrap(),
            json!({
                "code": -32700,
                "message": "Parse error",
                "data": {"id": 1314, "context": "TEST"},
            })
        );

        let error_without_data = Error::new(ReservedErrorCode::MethodNotFound, None::<u8>);
        assert_eq!(
            serde_json::to_value(error_without_data).unwrap(),
            json!({"code": -32601, "message": "Method not found", "data": null})
        );

        let error_with_bad_data = Error::new(ReservedErrorCode::InvalidParams, FailToEncode);
        assert_eq!(
            serde_json::to_value(error_with_bad_data).unwrap(),
            json!({
                "code": -32603,
                "message": "Internal error",
                "data": "failed to json-encode additional info in json-rpc error: won't encode",
            })
        );
    }

    #[test]
    fn should_construct_custom_error() {
        let good_error_code = TestErrorCode {
            in_reserved_range: false,
        };

        let error_with_data = Error::new(good_error_code, AdditionalInfo::test_default());
        assert_eq!(
            serde_json::to_value(error_with_data).unwrap(),
            json!({
                "code": -123,
                "message": "Valid test error",
                "data": {"id": 1314, "context": "TEST"},
            })
        );

        let error_without_data = Error::new(good_error_code, ());
        assert_eq!(
            serde_json::to_value(error_without_data).unwrap(),
            json!({"code": -123, "message": "Valid test error", "data": null})
        );

        let error_with_bad_data = Error::new(good_error_code, FailToEncode);
        assert_eq!(
            serde_json::to_value(error_with_bad_data).unwrap(),
            json!({
                "code": -32603,
                "message": "Internal error",
                "data": "failed to json-encode additional info in json-rpc error: won't encode",
            })
        );
    }

    #[test]
    fn should_fall_back_to_internal_error_on_bad_custom_error() {
        let bad_error_code = TestErrorCode {
            in_reserved_range: true,
        };
        let expected = json!({
            "code": -32603,
            "message": "Internal error",
            "data": "attempted to return reserved error code -32603",
        });

        let error_with_data = Error::new(bad_error_code, AdditionalInfo::test_default());
        assert_eq!(serde_json::to_value(error_with_data).unwrap(), expected);

        let error_without_data = Error::new(bad_error_code, None::<u8>);
        assert_eq!(serde_json::to_value(error_without_data).unwrap(), expected);

        let error_with_bad_data = Error::new(bad_error_code, FailToEncode);
        assert_eq!(serde_json::to_value(error_with_bad_data).unwrap(), expected);
    }
}
