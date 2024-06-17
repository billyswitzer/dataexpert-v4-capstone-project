INSERT INTO billyswitzer.daily_stock_price_cumulative
SELECT symbol,
  ARRAY_AGG(ROW(close_price, high_price, low_price, trade_count, open_price, bar_date, volume, volume_weighted_average_price)) AS price_array,
  as_of_date
FROM billyswitzer.staging_daily_stock_price_backfill
WHERE close_price > 0
  AND high_price > 0
  AND low_price > 0
  AND trade_count > 0
  AND open_price > 0
  AND volume > 0
  AND volume_weighted_average_price > 0
GROUP BY symbol,
  as_of_date