#!/usr/bin/env bash

set -eu -o pipefail

CRATES_URL=https://crates.io/api/v1/crates
ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." >/dev/null 2>&1 && pwd)"

run_curl() {
    set +e
    CURL_OUTPUT=$(curl -s $1)
    set -e
    local EXIT_CODE=$?
    if [[ $EXIT_CODE -ne 0 ]]; then
        printf "curl -s %s failed with exit code %d\n\n" $1 $EXIT_CODE
        exit 1
    fi
}

max_version_in_crates_io() {
    local CRATE=$1
    printf "Max published version: "
    run_curl "$CRATES_URL/$CRATE"
    if [[ "$CURL_OUTPUT" == "{\"errors\":[{\"detail\":\"Not Found\"}]}" ]]; then
        CRATES_IO_VERSION="N/A (not found in crates.io)"
    else
        CRATES_IO_VERSION=$(echo "$CURL_OUTPUT" | jq -r .crate.max_version)
    fi
    printf "%s\n" "$CRATES_IO_VERSION"
}

publish() {
    CRATE_DIR="$1"
    local CRATE_DIR
    pushd "$ROOT_DIR/$CRATE_DIR" >/dev/null

    CRATE_NAME=$(cargo read-manifest  | jq -r '.name' | tr -d '\n')
    local CRATE_NAME
    printf "%s\n" "$CRATE_NAME"

    max_version_in_crates_io "$CRATE_NAME"

    printf "Local version:         "
    LOCAL_VERSION=$(cargo read-manifest  | jq -r '.version' | tr -d '\n')
    printf "%s\n$LOCAL_VERSION"

    if [[ "$LOCAL_VERSION" == "$CRATES_IO_VERSION" ]]; then
        printf "Skipping\n"
    else
        printf "Publishing...\n"
        set +u
        cargo publish "${@:2}" --token "${CARGO_TOKEN}"
        set -u
        printf "Published version %s\n$LOCAL_VERSION"
        printf "Sleeping for 60 seconds...\n"
        sleep 60
    fi
    printf "================================================================================\n\n"
    popd >/dev/null
}

# These are the subdirs of casper-node which contain packages for publishing.  They should remain ordered from
# least-dependent to most.
publish types
publish listener
publish sidecar
