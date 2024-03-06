use crate::rest_server::path_abstraction_for_metrics_inner;

pub mod integration_tests;
pub mod integration_tests_version_switch;
pub mod performance_tests;

#[test]
fn path_abstraction_for_metrics_inner_should_handle_endpoints() {
    test_single_nested_path("block");
    test_single_nested_path("step");
    test_single_nested_path("faults");
    test_single_nested_path("signatures");
    expect_output("/transaction/deploy/123", "/transaction/(...)");
    expect_output(
        "/transaction/accepted/deploy/123",
        "/transaction/accepted/(...)",
    );
    expect_output(
        "/transaction/accepted/version1/123",
        "/transaction/accepted/(...)",
    );
    expect_output(
        "/transaction/processed/deploy/123",
        "/transaction/processed/(...)",
    );
    expect_output(
        "/transaction/processed/version1/123",
        "/transaction/processed/(...)",
    );
    expect_output(
        "/transaction/expired/deploy/123",
        "/transaction/expired/(...)",
    );
    expect_output(
        "/transaction/expired/version1/123",
        "/transaction/expired/(...)",
    );
    expect_output("/xyz", "unknown");
    expect_output("/", "unknown");
    expect_output("", "unknown");
}

fn test_single_nested_path(part: &str) {
    let expected_output = &format!("/{}/(...)", part);
    expect_output(part, expected_output);
    expect_output(&format!("/{part}"), expected_output);
    expect_output(&format!("/{part}/"), expected_output);
    expect_output(&format!("/{part}/abc/def/ghi"), expected_output);
    expect_output(&format!("{part}/abc/def/ghi"), expected_output);
}

fn expect_output(input: &str, output: &str) {
    assert_eq!(path_abstraction_for_metrics_inner(input), output);
}
