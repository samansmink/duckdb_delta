import duckdb
import os

# This file demonstrates an MVP for an idempotent write UX by:
# - describing the use of 2 (to be implemented functions) in duckdb
#    - int delta_get_transaction_version(string delta_path, int version)
#    - void delta_set_transaction_version(app_id, new_version_old_version)
g

# Generates TPC-H lineitem SF0.01 table as multiple parquet files, creating a batch for each portion of data
# e.g. with num_batches 3 this creates:
# - path/1/lineitem.parquet
# - path/2/lineitem.parquet
# - path/3/lineitem.parquet
def generate_test_data(num_batches, path):
    for batch_num in range(0,10):
        con = duckdb.connect()
        con.execute(f"call dbgen(sf=0.01, children={num_batches}, step={batch_num})")
        os.makedirs(f"{path}/{batch_num}", exist_ok=True)
        con.execute(f"COPY lineitem TO '{path}/{batch_num}/lineitem.parquet'")

# DuckDB Idempotency Primitive 1: get the current transaction version
def get_delta_transaction_version(delta_path, app_id):
    current_version = duckdb.query(f"SELECT version FROM delta_get_transaction_version('{delta_path}', '{app_id}');").fetchall()[0][0]
    if current_version is None:
        return 0
    return current_version

# DuckDB Idempotency Primitive 2: set the current transaction version to the currently running transaction
def set_delta_transaction_version(con, app_id, new_version, old_version):
    con.execute(f"CALL delta_set_transaction_version({app_id}, {new_version}, {old_version});")

# This is a basic idempotent stream demo that will write batches found in `input_path`/<batch_num>/lineitem.parquet to `output_delta_path`
def idempotent_stream_job(input_path, output_delta_path, total_batches, app_id):
    con = duckdb.connect()

    # Loop while there are still batches to process
    while True:
        # Get current version
        current_version = get_delta_transaction_version(output_delta_path, app_id);

        # All batches processed?
        if current_version >= total_batches:
            break

        # Let's begin processing the batch!
        con.execute(f"BEGIN TRANSACTION")

        # Register the compare-and-swap operation that will be performed on COMMIT of current transaction: when committing,
        # DuckDB will check that the version of `APP_ID` is still equal to `current_version` and change it to current_version + 1
        # on a successful commit.
        set_delta_transaction_version(con, app_id, 1, current_version + 1, current_version)

        # Write the batch to the delta table
        con.execute(f"COPY (FROM '{input_path}/{current_version}/lineitem.parquet') TO '{output_delta_path}'")

        # Commit!
        con.execute(f"COMMIT")

# Validates the output by checking the result of query 6 from TPC-H sf0.01 on the lineitem table
def validate_output(delta_dir):
    con = duckdb.connect()
    con.execute(f"CREATE VIEW lineitem AS FROM delta_scan('{delta_dir}');")
    result = con.sql("pragma tpch(6)").fetchall()[0][0];

    if result != 1193053.2253:
        raise Exception("Incorrect result!")

def main():
    PARQUET_OUTPUT_DIR = "/tmp/idempotency_demo_parquet"
    DELTA_OUTPUT_DIR = "/tmp/idempotency_demo_delta"
    APP_ID = 'demo_app_id'

    # Generate Lineitem TPCH SF-0.01 split in 10 batches
    generate_test_data(0.01, 10, PARQUET_OUTPUT_DIR)

    # Will write the first 2 batches
    idempotent_stream_job(PARQUET_OUTPUT_DIR, DELTA_OUTPUT_DIR, 2, APP_ID)
    # Will skip batch 0 and 1 and write 3 more batches
    idempotent_stream_job(PARQUET_OUTPUT_DIR, DELTA_OUTPUT_DIR, 5, APP_ID)
    # Will skip first 5 batches and write remaining 5
    idempotent_stream_job(PARQUET_OUTPUT_DIR, DELTA_OUTPUT_DIR, 10, APP_ID)

    # Validate output
    validate_output(DELTA_OUTPUT_DIR)

if __name__ == "__main__":
    main()
