-- INPUT
CREATE TABLE old_results AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf1_remote_take_2/delta_scan v0.1.0.csv";
CREATE TABLE new_results AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf1_remote_take_2/delta_scan v0.3.0.csv";

-- PROCESS
CREATE TABLE old_results_median as SELECT query, median(timing) as timing FROM old_results GROUP BY query ORDER BY query;
CREATE TABLE new_results_median as SELECT query, median(timing) as timing FROM new_results GROUP BY query ORDER BY query;

-- from old_results_median;
-- from new_results_median;

CREATE TABLE result AS
    SELECT
        query:                      delta.query,
        old_result:                 old_result.timing,
        new_result:                 delta.timing,
        relative_delta_overhead:    ( delta.timing - old_result.timing ) / old_result.timing,
        absolute_delta_overhead:    delta.timing - old_result.timing
    FROM
        old_results_median as old_result
    JOIN
        new_results_median as delta ON delta.query=old_result.query
    ORDER BY
        relative_delta_overhead DESC;

-- GENERATE RESULT

-- SWITCH TO MARKDOWN HERE
.mode markdown

-- Overall Results

SELECT
    result: 'Delta v0.1.0',
    total_runtime:  format('{:,.2f}', sum(timing)),
    min_runtime:  format('{:,.2f}', min(timing)),
    max_runtime:  format('{:,.2f}', max(timing)),
    median_runtime:  format('{:,.2f}', median(timing)),
    queries_timed_out: 99-count(*)
FROM
    old_results_median
UNION ALL
SELECT
    result: 'Delta v0.3.0',
        total_runtime:  format('{:,.2f}', sum(timing)),
        min_runtime:  format('{:,.2f}', min(timing)),
        max_runtime:  format('{:,.2f}', max(timing)),
        median_runtime:  format('{:,.2f}', median(timing)),
        queries_timed_out: 99-count(*)
FROM
    new_results_median;

-- Comparing directly
SELECT
    result:         format('{:,.2f}', old_result),
    new_result:              format('{:,.2f}', new_result),
    speedup:                 format('{:,.2f}x', old_result/new_result),
FROM (SELECT old_result: sum(old_result), new_result: sum(new_result)
      FROM result
      GROUP BY ALL
);