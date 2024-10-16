PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=deltatable
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Set test paths
test_release: export DELTA_KERNEL_TESTS_PATH=./build/release/rust/src/delta_kernel/kernel/tests/data
test_release: export DAT_PATH=./build/release/rust/src/delta_kernel/acceptance/tests/dat

test_debug: export DELTA_KERNEL_TESTS_PATH=./build/debug/rust/src/delta_kernel/kernel/tests/data
test_debug: export DAT_PATH=./build/debug/rust/src/delta_kernel/acceptance/tests/dat

# Core extensions that we need for testing
CORE_EXTENSIONS='tpcds;tpch;aws;azure;httpfs'

# Set this flag during building to enable the benchmark runner
ifeq (${BUILD_BENCHMARK}, 1)
	TOOLCHAIN_FLAGS:=${TOOLCHAIN_FLAGS} -DBUILD_BENCHMARKS=1
endif

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Include the Makefile from the benchmark directory
include benchmark/benchmark.Makefile

# Generate some test data to test with
generate-data:
	python3 -m pip install delta-spark duckdb pandas deltalake pyspark
	python3 scripts/generate_test_data.py
