CREATE TABLE billyswitzer.daily_stock_price_cumulative (
  symbol VARCHAR(10),
  price_array ARRAY(ROW(
      close_price DOUBLE,
      high_price DOUBLE,
      low_price DOUBLE,
      trade_count BIGINT,
      open_price DOUBLE,
      bar_date DATE,
      volume BIGINT,
      volume_weighted_average_price DOUBLE
    )),
  as_of_date DATE
  )
WITH (
  format = 'PARQUET',
  partitioning = ARRAY['as_of_date']
)