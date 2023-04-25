#!/bin/bash

count=20
for i in $(seq $count); do
    cowsay "On run ${i}"
    RUST_BACKTRACE=1 cargo test -- --nocapture
    retVal=$?
    if [ $retVal -ne 0 ]; then
        echo "TESTS FAILED ON RUN ${i}"
        spd-say "it failed you fucking moron"
        exit $retVal
    fi
done;
echo "ALL TESTS OK!";
spd-say "get back to work you lazy asshole"