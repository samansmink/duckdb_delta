-- INPUT
CREATE TABLE parquet_results AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf100/parquet_scan (using glob).csv";
CREATE TABLE delta_results AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf100/delta_scan (using ATTACH).csv";

-- PROCESS
CREATE TABLE parquet_results_avg as SELECT query, median(timing) as timing FROM parquet_results GROUP BY query ORDER BY query;
CREATE TABLE delta_results_avg as SELECT query, median(timing) as timing FROM delta_results GROUP BY query ORDER BY query;

CREATE TABLE result AS
    SELECT
        query:                      delta.query,
        parquet_scan:               parquet.timing,
        delta_scan:                 delta.timing,
        relative_delta_overhead:    ( delta.timing - parquet.timing ) / parquet.timing,
        absolute_delta_overhead:    delta.timing - parquet.timing
    FROM
        parquet_results_avg as parquet
    JOIN
        delta_results_avg as delta ON delta.query=parquet.query
    ORDER BY
        relative_delta_overhead DESC;

-- GENERATE RESULT

-- SWITCH TO MARKDOWN HERE
-- .mode markdown
SELECT
    parquet_scan:            format('{:,.2f}sec', parquet_scan),
    delta_scan:              format('{:,.2f}sec', delta_scan),
    delta_absolute_overhead: format('{:,.2f}sec', delta_scan - parquet_scan),
    delta_relative_overhead: format('{:,.2f}%', (delta_scan - parquet_scan) / parquet_scan * 100),
FROM (SELECT parquet_scan: sum(parquet_scan), delta_scan: sum(delta_scan)
      FROM result
      GROUP BY ALL
);

-- BEST 5
SELECT
    query,
    overhead: format('{:,.2f}%', relative_delta_overhead * 100),
    absolute_delta_overhead
FROM
    result
ORDER BY
    relative_delta_overhead
LIMIT 5
;

-- WORST 5
SELECT
    query,
    overhead: format('{:,.2f}%', relative_delta_overhead * 100),
    absolute_delta_overhead
FROM
    result
ORDER BY
    relative_delta_overhead DESC
    LIMIT 5
;

-- MEAN overhead
SELECT
    format('{:,.2f}%', mean(relative_delta_overhead*100)) as mean_overhead
FROM
    result
GROUP BY ALL
;

-- Q54
SELECT
    *
FROM result
WHERE query='q54'