CREATE TABLE billyswitzer.staging_daily_stock_price_cumulative (
  ticker VARCHAR(10),
  price_array ARRAY(ROW(
      volume DOUBLE,
      open DOUBLE,
      close DOUBLE,
      high DOUBLE,
      low DOUBLE,
      transactions BIGINT,
      snapshot_date DATE
    )),
  as_of_date DATE
  )
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['as_of_date']
)