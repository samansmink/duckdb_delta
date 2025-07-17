-- INPUT
CREATE TABLE baseline AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf1_remote_take_2/delta_scan v0.3.0.csv";
CREATE TABLE attach_results AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf1_remote_take_2/delta_attach.csv";
CREATE TABLE attach_pin_results AS
    SELECT regexp_extract(name, '.*(q[0-9][0-9])\.benchmark', 1) as query, run, timing
    FROM "benchmark_results_saved/blogpost_nr2/tpcds_sf1_remote_take_2/delta_attach_pin.csv";

-- PROCESS
CREATE TABLE baseline_avg as SELECT query, median(timing) as timing FROM baseline GROUP BY query ORDER BY query;
CREATE TABLE attach_results_avg as SELECT query, median(timing) as timing FROM attach_results GROUP BY query ORDER BY query;
CREATE TABLE attach_pin_results_avg as SELECT query, median(timing) as timing FROM attach_pin_results GROUP BY query ORDER BY query;

-- GENERATE RESULT

-- SWITCH TO MARKDOWN HERE
.mode markdown

-- Overall Results
SELECT
    result: 'delta_scan',
    total_runtime:  format('{:,.2f}', sum(timing)),
    min_runtime:  format('{:,.2f}', min(timing)),
    max_runtime:  format('{:,.2f}', max(timing)),
    median_runtime:  format('{:,.2f}', median(timing))
FROM
    baseline_avg
UNION ALL
SELECT
    result: 'ATTACH',
        total_runtime:  format('{:,.2f}', sum(timing)),
        min_runtime:  format('{:,.2f}', min(timing)),
        max_runtime:  format('{:,.2f}', max(timing)),
        median_runtime:  format('{:,.2f}', median(timing))
FROM
    attach_results_avg
UNION ALL
SELECT
    result: 'ATTACH (PIN_SNAPSHOT)',
        total_runtime:  format('{:,.2f}', sum(timing)),
        min_runtime:  format('{:,.2f}', min(timing)),
        max_runtime:  format('{:,.2f}', max(timing)),
        median_runtime:  format('{:,.2f}', median(timing))
FROM
    attach_pin_results_avg;