CREATE TABLE billyswitzer.daily_stock_split (
    execution_date VARCHAR(20),
    split_from DOUBLE,
    split_to DOUBLE,
    ticker VARCHAR(10),
    as_of_date DATE
  )
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['as_of_date']
)