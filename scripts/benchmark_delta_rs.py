from deltalake import DeltaTable, write_deltalake
from deltalake.query import QueryBuilder
import time
import duckdb

####  Config  ####
# Run the delta-rs + datafusion benchmark
RUN_DATAFUSION = True
# Run the duckdb + duckdb-delta benchmark
RUN_DUCKDB_BENCH = False
# 'sf1' or 'sf10'
SCALE_FACTOR = 'sf10'
# tpcds or tpch
BENCHMARK = 'tpcds'
##################

# TPCH
tpch_customer = f'./data/generated/tpch_{SCALE_FACTOR}/customer/delta_lake'
tpch_lineitem = f'./data/generated/tpch_{SCALE_FACTOR}/lineitem/delta_lake'
tpch_nation = f'./data/generated/tpch_{SCALE_FACTOR}/nation/delta_lake'
tpch_orders = f'./data/generated/tpch_{SCALE_FACTOR}/orders/delta_lake'
tpch_part = f'./data/generated/tpch_{SCALE_FACTOR}/part/delta_lake'
tpch_partsupp = f'./data/generated/tpch_{SCALE_FACTOR}/partsupp/delta_lake'
tpch_region = f'./data/generated/tpch_{SCALE_FACTOR}/region/delta_lake'
tpch_supplier = f'./data/generated/tpch_{SCALE_FACTOR}/supplier/delta_lake'

# TPCDS
tpcds_call_center = f'./data/generated/tpcds_{SCALE_FACTOR}/call_center/delta_lake'
tpcds_catalog_page = f'./data/generated/tpcds_{SCALE_FACTOR}/catalog_page/delta_lake'
tpcds_catalog_returns = f'./data/generated/tpcds_{SCALE_FACTOR}/catalog_returns/delta_lake'
tpcds_catalog_sales = f'./data/generated/tpcds_{SCALE_FACTOR}/catalog_sales/delta_lake'
tpcds_customer = f'./data/generated/tpcds_{SCALE_FACTOR}/customer/delta_lake'
tpcds_customer_demographics = f'./data/generated/tpcds_{SCALE_FACTOR}/customer_demographics/delta_lake'
tpcds_customer_address = f'./data/generated/tpcds_{SCALE_FACTOR}/customer_address/delta_lake'
tpcds_date_dim = f'./data/generated/tpcds_{SCALE_FACTOR}/date_dim/delta_lake'
tpcds_household_demographics = f'./data/generated/tpcds_{SCALE_FACTOR}/household_demographics/delta_lake'
tpcds_inventory = f'./data/generated/tpcds_{SCALE_FACTOR}/inventory/delta_lake'
tpcds_income_band = f'./data/generated/tpcds_{SCALE_FACTOR}/income_band/delta_lake'
tpcds_item = f'./data/generated/tpcds_{SCALE_FACTOR}/item/delta_lake'
tpcds_promotion = f'./data/generated/tpcds_{SCALE_FACTOR}/promotion/delta_lake'
tpcds_reason = f'./data/generated/tpcds_{SCALE_FACTOR}/reason/delta_lake'
tpcds_ship_mode = f'./data/generated/tpcds_{SCALE_FACTOR}/ship_mode/delta_lake'
tpcds_store = f'./data/generated/tpcds_{SCALE_FACTOR}/store/delta_lake'
tpcds_store_returns = f'./data/generated/tpcds_{SCALE_FACTOR}/store_returns/delta_lake'
tpcds_store_sales = f'./data/generated/tpcds_{SCALE_FACTOR}/store_sales/delta_lake'
tpcds_time_dim = f'./data/generated/tpcds_{SCALE_FACTOR}/time_dim/delta_lake'
tpcds_warehouse = f'./data/generated/tpcds_{SCALE_FACTOR}/warehouse/delta_lake'
tpcds_web_page = f'./data/generated/tpcds_{SCALE_FACTOR}/web_page/delta_lake'
tpcds_web_returns = f'./data/generated/tpcds_{SCALE_FACTOR}/web_returns/delta_lake'
tpcds_web_sales = f'./data/generated/tpcds_{SCALE_FACTOR}/web_sales/delta_lake'
tpcds_web_site = f'./data/generated/tpcds_{SCALE_FACTOR}/web_site/delta_lake'

def get_query_tpch(query):
    if query < 10:
        file = f"./duckdb/extension/tpch/dbgen/queries/Q0{query}.sql"
    else:
        file = f"./duckdb/extension/tpch/dbgen/queries/Q{query}.sql"

    with open(file) as f:
        return f.read()

def get_query_tpcds(query):
    if query < 10:
        file = f"./duckdb/extension/tpcds/dsdgen/queries/0{query}.sql"
    else:
        file = f"./duckdb/extension/tpcds/dsdgen/queries/{query}.sql"

    with open(file) as f:
        return f.read()

def run_query_n_times(qb, query, querynum, label, n=5):
    # Dry run
    qb.execute(query).fetchall()

    runs = []

    querynum_prefix = "0" if querynum<10 else ""

    for i in range(1,n+1):
        start_time = time.time()
        res = qb.execute(query).fetchall()
        elapsed = time.time() - start_time
        print(f"{label}/q{querynum_prefix}{querynum}.benchmark\t{i}\t{elapsed}")

def duckdb_load_tpch(con):
    con.query(f"create view customer as from delta_scan('{tpch_customer}');")
    con.query(f"create view lineitem as from delta_scan('{tpch_lineitem}');")
    con.query(f"create view nation as from delta_scan('{tpch_nation}');")
    con.query(f"create view orders as from delta_scan('{tpch_orders}');")
    con.query(f"create view part as from delta_scan('{tpch_part}');")
    con.query(f"create view partsupp as from delta_scan('{tpch_partsupp}');")
    con.query(f"create view region as from delta_scan('{tpch_region}');")
    con.query(f"create view supplier as from delta_scan('{tpch_supplier}');")

def duckdb_bench():
    duckdb_conn = duckdb.connect()
    if BENCHMARK == 'tpch':
        duckdb_load_tpch(duckdb_conn)
        get_query_fun = get_query_tpch
        num_queries = 22
    elif BENCHMARK == 'tpcds':
        duckdb_load_tpcds(duckdb_conn)
        get_query_fun = get_query_tpcds
        num_queries = 99
    else:
        raise Error(f'unknown benchmark {BENCHMARK}')

    print("name\trun\ttiming")
    for i in range(1,num_queries+1):
        run_query_n_times(duckdb_conn, get_query_fun(i), i, f"duckdb-tpch-{SCALE_FACTOR}")

def datafusion_get_querybuilder_tpch():
    customer_dt = DeltaTable(tpch_customer)
    lineitem_dt = DeltaTable(tpch_lineitem)
    nation_dt = DeltaTable(tpch_nation)
    orders_dt = DeltaTable(tpch_orders)
    part_dt = DeltaTable(tpch_part)
    partsupp_dt = DeltaTable(tpch_partsupp)
    region_dt = DeltaTable(tpch_region)
    supplier_dt = DeltaTable(tpch_supplier)

    # Register delta tables to query builder
    return (QueryBuilder().register("customer", customer_dt)
          .register("lineitem", lineitem_dt)
          .register("nation", nation_dt)
          .register("orders", orders_dt)
          .register("part", part_dt)
          .register("partsupp", partsupp_dt)
          .register("region", region_dt)
          .register("supplier", supplier_dt))

def datafusion_get_querybuilder_tpcds():
    tpcds_call_center_dt = DeltaTable(tpcds_call_center)
    tpcds_catalog_page_dt = DeltaTable(tpcds_catalog_page)
    tpcds_catalog_returns_dt = DeltaTable(tpcds_catalog_returns)
    tpcds_catalog_sales_dt = DeltaTable(tpcds_catalog_sales)
    tpcds_customer_dt = DeltaTable(tpcds_customer)
    tpcds_customer_demographics_dt = DeltaTable(tpcds_customer_demographics)
    tpcds_customer_address_dt = DeltaTable(tpcds_customer_address)
    tpcds_date_dim_dt = DeltaTable(tpcds_date_dim)
    tpcds_household_demographics_dt = DeltaTable(tpcds_household_demographics)
    tpcds_inventory_dt = DeltaTable(tpcds_inventory)
    tpcds_income_band_dt = DeltaTable(tpcds_income_band)
    tpcds_item_dt = DeltaTable(tpcds_item)
    tpcds_promotion_dt = DeltaTable(tpcds_promotion)
    tpcds_reason_dt = DeltaTable(tpcds_reason)
    tpcds_ship_mode_dt = DeltaTable(tpcds_ship_mode)
    tpcds_store_dt = DeltaTable(tpcds_store)
    tpcds_store_returns_dt = DeltaTable(tpcds_store_returns)
    tpcds_store_sales_dt = DeltaTable(tpcds_store_sales)
    tpcds_time_dim_dt = DeltaTable(tpcds_time_dim)
    tpcds_warehouse_dt = DeltaTable(tpcds_warehouse)
    tpcds_web_page_dt = DeltaTable(tpcds_web_page)
    tpcds_web_returns_dt = DeltaTable(tpcds_web_returns)
    tpcds_web_sales_dt = DeltaTable(tpcds_web_sales)
    tpcds_web_site_dt = DeltaTable(tpcds_web_site)

    # Register delta tables to query builder
    return (QueryBuilder().register("call_center", tpcds_call_center_dt)
          .register("catalog_page", tpcds_catalog_page_dt)
          .register("catalog_returns", tpcds_catalog_returns_dt)
          .register("catalog_sales", tpcds_catalog_sales_dt)
          .register("customer", tpcds_customer_dt)
          .register("customer_demographics", tpcds_customer_demographics_dt)
          .register("customer_address", tpcds_customer_address_dt)
          .register("date_dim", tpcds_date_dim_dt)
          .register("household_demographics", tpcds_household_demographics_dt)
          .register("inventory", tpcds_inventory_dt)
          .register("income_band", tpcds_income_band_dt)
          .register("item", tpcds_item_dt)
          .register("promotion", tpcds_promotion_dt)
          .register("reason", tpcds_reason_dt)
          .register("ship_mode", tpcds_ship_mode_dt)
          .register("store", tpcds_store_dt)
          .register("store_returns", tpcds_store_returns_dt)
          .register("store_sales", tpcds_store_sales_dt)
          .register("time_dim", tpcds_time_dim_dt)
          .register("warehouse", tpcds_warehouse_dt)
          .register("web_page", tpcds_web_page_dt)
          .register("web_returns", tpcds_web_returns_dt)
          .register("web_sales", tpcds_web_sales_dt)
          .register("web_site", tpcds_web_site_dt))

def datafusion_bench():
    if BENCHMARK == 'tpch':
        qb = datafusion_get_querybuilder_tpch()
        get_query_fun = get_query_tpch
        num_queries = 22
        skipped = []
    elif BENCHMARK == 'tpcds':
        qb = datafusion_get_querybuilder_tpcds()
        get_query_fun = get_query_tpcds
        num_queries = 99
        skipped = [47, 48, 49, 70, 72, 86]
    else:
        raise Error(f'unknown benchmark {BENCHMARK}')

    print("name\trun\ttiming")
    for i in range(1,num_queries+1):
        if i in skipped:
            continue
        run_query_n_times(qb, get_query_fun(i), i, f"delta-rs-datafusion-{BENCHMARK}-{SCALE_FACTOR}")

if RUN_DATAFUSION:
    datafusion_bench()

if RUN_DUCKDB_BENCH:
    duckdb_bench()