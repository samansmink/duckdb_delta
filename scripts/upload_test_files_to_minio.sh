#!/bin/bash

aws s3 cp --recursive ./build/release/rust/src/delta_kernel/acceptance/tests/dat/out/reader_tests/generated "s3://test-bucket/dat"
aws s3 cp --recursive ./build/release/rust/src/delta_kernel/acceptance/tests/dat/out/reader_tests/generated "s3://test-bucket-public/dat"