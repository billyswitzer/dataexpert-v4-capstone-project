CREATE TABLE billyswitzer.dim_daily_stock_price (
  symbol VARCHAR(10),
  close_price_last_day DOUBLE,
  close_price_avg_last_90_days DOUBLE,
  close_price_avg_last_365_days DOUBLE,
  as_of_date DATE
  )
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['as_of_date']
)