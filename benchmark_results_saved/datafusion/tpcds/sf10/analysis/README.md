# Q72 analysis
Explain analyze output shows that there's a huge difference. This is caused by the cardinality estimate of
the parquet scan being wildly off.

The is in turn caused by pyspark generating some empty parquet files which fuck up the query plan 