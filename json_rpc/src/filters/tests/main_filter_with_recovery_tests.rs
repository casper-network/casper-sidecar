use http::StatusCode;
use serde::{
    Deserialize, Serialize,
    ser::{Error as _, Serializer},
};
use serde_json::{Value, json};
use warp::{Filter, Reply, filters::BoxedFilter};

use crate::{
    ConfigLimit, Error, JsonRpcOptions, Params, RequestHandlersBuilder, ReservedErrorCode,
    Response,
    filters::{handle_rejection, main_filter},
};

const GET_GOOD_THING: &str = "get good thing";
const GET_BAD_THING: &str = "get bad thing";
const GET_THROTTLED_THING: &str = "get throttled thing";

#[derive(PartialEq, Eq, Serialize, Deserialize, Debug)]
struct GoodThing {
    good_thing: String,
}

/// A type which always errors when being serialized.
struct BadThing;

impl Serialize for BadThing {
    fn serialize<S: Serializer>(&self, _serializer: S) -> Result<S::Ok, S::Error> {
        Err(S::Error::custom("won't encode"))
    }
}

async fn get_good_thing(params: Option<Params>) -> Result<GoodThing, Error> {
    match params {
        Some(Params::Array(array)) => Ok(GoodThing {
            good_thing: array[0].as_str().unwrap().to_string(),
        }),
        _ => Err(Error::new(ReservedErrorCode::InvalidParams, "no params")),
    }
}

async fn get_bad_thing(_params: Option<Params>) -> Result<BadThing, Error> {
    Ok(BadThing)
}

async fn get_throttled_thing(_params: Option<Params>) -> Result<GoodThing, Error> {
    Err(Error::new(ReservedErrorCode::InternalError, "throttled")
        .with_http_status_override(StatusCode::TOO_MANY_REQUESTS.as_u16()))
}

async fn from_http_response(response: http::Response<hyper::Body>) -> Response {
    let body_bytes = hyper::body::to_bytes(response.into_body()).await.unwrap();
    serde_json::from_slice(&body_bytes).unwrap()
}

fn main_filter_with_recovery() -> BoxedFilter<(impl Reply,)> {
    let mut handlers = RequestHandlersBuilder::new();
    handlers.register_handler(GET_GOOD_THING, get_good_thing, &ConfigLimit::default());
    handlers.register_handler(GET_BAD_THING, get_bad_thing, &ConfigLimit::default());
    handlers.register_handler(
        GET_THROTTLED_THING,
        get_throttled_thing,
        &ConfigLimit::default(),
    );
    let handlers = handlers.build();

    main_filter(handlers, JsonRpcOptions::default())
        .recover(handle_rejection)
        .boxed()
}

#[tokio::test]
async fn should_handle_valid_request() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `fn get_good_thing` and return `Ok` as "params" is Some, causing a
    // Response::Success to be returned to the client.
    let http_response = warp::test::request()
        .body(
            json!({"jsonrpc":"2.0","id":"a","method":"get good thing","params":["one"]})
                .to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), "a");
    assert_eq!(
        rpc_response.result(),
        Some(GoodThing {
            good_thing: "one".to_string()
        })
    );
}

#[tokio::test]
async fn should_handle_valid_request_where_rpc_returns_error() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `fn get_good_thing` and return `Err` as "params" is None, causing
    // a Response::Failure (invalid params) to be returned to the client.
    let http_response = warp::test::request()
        .body(json!({"jsonrpc":"2.0","id":"a","method":"get good thing"}).to_string())
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), "a");
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(ReservedErrorCode::InvalidParams, "no params")
    );
}

#[tokio::test]
async fn should_handle_valid_request_where_result_encoding_fails() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `fn get_bad_thing` which returns a type which fails to encode,
    // causing a Response::Failure (internal error) to be returned to the client.
    let http_response = warp::test::request()
        .body(json!({"jsonrpc":"2.0","id":"a","method":"get bad thing"}).to_string())
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), "a");
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(
            ReservedErrorCode::InternalError,
            "failed to encode json-rpc response value: won't encode"
        )
    );
}

#[tokio::test]
async fn should_handle_request_for_method_not_registered() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `filters::handle_body` and return Response::Failure (invalid
    // request) to the client as the ID has fractional parts.
    let http_response = warp::test::request()
        .body(
            json!({"jsonrpc":"2.0","id":1,"method":"not registered","params":["one"]}).to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), 1);
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(
            ReservedErrorCode::MethodNotFound,
            "'not registered' is not a supported json-rpc method on this server"
        )
    );
}

#[tokio::test]
async fn should_handle_request_with_fractional_id() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // JSON number IDs, including fractional values, are accepted and echoed.
    let http_response = warp::test::request()
        .body(
            json!({"jsonrpc":"2.0","id":1.1,"method":"get good thing","params":["one"]})
                .to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), 1.1);
    assert_eq!(
        rpc_response.result(),
        Some(GoodThing {
            good_thing: "one".to_string()
        })
    );
}

#[tokio::test]
async fn should_handle_request_with_no_id() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // A structurally valid request without an ID is a notification. It executes but has no body.
    let http_response = warp::test::request()
        .body(json!({"jsonrpc":"2.0","method":"get good thing","params":["one"]}).to_string())
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::NO_CONTENT);
    let body = hyper::body::to_bytes(http_response.into_body())
        .await
        .unwrap();
    assert!(body.is_empty());
}

#[tokio::test]
async fn should_handle_request_with_extra_field() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `filters::handle_body` and return Response::Failure (invalid
    // request) to the client as the request has an extra field.
    let http_response = warp::test::request()
        .body(
            json!({"jsonrpc":"2.0","id":1,"method":"get good thing","params":[2],"extra":"field"})
                .to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), 1);
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(
            ReservedErrorCode::InvalidRequest,
            "Unexpected field: 'extra'"
        )
    );
}

#[tokio::test]
async fn should_handle_malformed_request_with_valid_id() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `filters::handle_body` and return Response::Failure (invalid
    // request) to the client, but with the ID included in the response as it was able to be parsed.
    let http_response = warp::test::request()
        .body(json!({"jsonrpc":"2.0","id":1,"method":{"not":"a string"}}).to_string())
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), 1);
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(
            ReservedErrorCode::InvalidRequest,
            "Expected 'method' to be a String"
        )
    );
}

#[tokio::test]
async fn should_handle_malformed_request_but_valid_json() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `filters::handle_body` and return Response::Failure (invalid
    // request) to the client as it can't be parsed as a JSON-RPC request.
    let http_response = warp::test::request()
        .body(json!({"a":1}).to_string())
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), &Value::Null);
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(ReservedErrorCode::InvalidRequest, "Missing 'jsonrpc' field")
    );
}

#[tokio::test]
async fn should_handle_invalid_json() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // This should get handled by `filters::handle_body` and return Response::Failure (parse error)
    // to the client as it cannot be parsed as JSON.
    let http_response = warp::test::request()
        .body("a")
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), &Value::Null);
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(
            ReservedErrorCode::ParseError,
            "expected value at line 1 column 1"
        )
    );
}

#[tokio::test]
async fn should_use_http_status_carried_by_the_error() {
    let _ = env_logger::try_init();

    let filter = main_filter_with_recovery();

    // `get_throttled_thing` attaches an HTTP status override to its error - the outer HTTP
    // response must use it instead of the JSON-RPC default of `200 OK`, while the JSON-RPC error
    // object on the wire is unaffected by it.
    let http_response = warp::test::request()
        .body(json!({"jsonrpc":"2.0","id":"a","method":"get throttled thing"}).to_string())
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::TOO_MANY_REQUESTS);
    let rpc_response = from_http_response(http_response).await;
    assert_eq!(rpc_response.id(), "a");
    assert_eq!(
        rpc_response.error().unwrap(),
        &Error::new(ReservedErrorCode::InternalError, "throttled")
    );
}

#[tokio::test]
async fn should_use_http_status_from_any_entry_in_a_batch() {
    let filter = main_filter_with_recovery();

    // A batch shares one outer HTTP response - if any entry's error carries a status override,
    // the whole response must use it, even though the other entry succeeds.
    let http_response = warp::test::request()
        .body(
            json!([
                {"jsonrpc":"2.0","id":1,"method":"get good thing","params":["one"]},
                {"jsonrpc":"2.0","id":2,"method":"get throttled thing"}
            ])
            .to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::TOO_MANY_REQUESTS);
    let body = hyper::body::to_bytes(http_response.into_body())
        .await
        .unwrap();
    let responses: Vec<Response> = serde_json::from_slice(&body).unwrap();
    assert_eq!(responses.len(), 2);
}

#[tokio::test]
async fn should_return_an_array_for_a_mixed_batch() {
    let filter = main_filter_with_recovery();
    let http_response = warp::test::request()
        .body(
            json!([
                {"jsonrpc":"2.0","id":1,"method":"get good thing","params":["one"]},
                {"jsonrpc":"2.0","method":"get good thing","params":["notification"]},
                false
            ])
            .to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::OK);
    let body = hyper::body::to_bytes(http_response.into_body())
        .await
        .unwrap();
    let responses: Vec<Response> = serde_json::from_slice(&body).unwrap();
    assert_eq!(responses.len(), 2);
    assert_eq!(responses[0].id(), 1);
    assert_eq!(responses[1].id(), &Value::Null);
    assert_eq!(responses[1].error().unwrap().code(), -32600);
}

#[tokio::test]
async fn should_return_no_content_for_an_all_notification_batch() {
    let filter = main_filter_with_recovery();
    let http_response = warp::test::request()
        .body(
            json!([
                {"jsonrpc":"2.0","method":"get good thing","params":["one"]},
                {"jsonrpc":"2.0","method":"get good thing","params":["two"]}
            ])
            .to_string(),
        )
        .filter(&filter)
        .await
        .unwrap()
        .into_response();

    assert_eq!(http_response.status(), StatusCode::NO_CONTENT);
    let body = hyper::body::to_bytes(http_response.into_body())
        .await
        .unwrap();
    assert!(body.is_empty());
}
