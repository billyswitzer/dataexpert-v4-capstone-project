INSERT INTO billyswitzer.daily_stock_price_cumulative
WITH RECURSIVE combined_split_factor (execution_date, split_from, split_to, ticker, as_of_date, row_number) AS
(
  SELECT execution_date,
    split_from,
    split_to,
    ticker,
    as_of_date,
    row_number
  FROM (
    SELECT execution_date,
      split_from,
      split_to,
      ticker,
      as_of_date,
      ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY execution_date DESC) AS row_number
    FROM billyswitzer.stock_splits_backfill
  ) raw_split_table
  WHERE row_number = 1

  UNION ALL

  SELECT r.execution_date,
    ROUND(c.split_from * r.split_from, 4) AS split_from,
    ROUND(c.split_to * r.split_to, 4) AS split_to,
    r.ticker,
    r.as_of_date,
    r.row_number
  FROM (
    SELECT execution_date,
      split_from,
      split_to,
      ticker,
      as_of_date,
      ROW_NUMBER() OVER (PARTITION BY ticker ORDER BY execution_date DESC) AS row_number
    FROM billyswitzer.stock_splits_backfill
  ) r
    JOIN combined_split_factor c ON r.ticker = c.ticker
      AND r.row_number = c.row_number + 1
), stock_split_factor_map (ticker, volume, open, close, high, low, transactions, snapshot_date, split_from, split_to, stock_to_split_rn) AS
(
  SELECT s.ticker,
    s.volume,
    s.open,
    s.close,
    s.high,
    s.low,
    s.transactions,
    s.snapshot_date,
    c.split_from,
    c.split_to,
    ROW_NUMBER() OVER (PARTITION BY s.ticker, s.snapshot_date ORDER BY c.row_number DESC) AS stock_to_split_rn
  FROM billyswitzer.staging_daily_stock_price_backfill s
    LEFT JOIN combined_split_factor c ON s.ticker = c.ticker
      AND s.snapshot_date < DATE(c.execution_date)
), max_date(as_of_date) AS
(
  SELECT MAX(snapshot_date) AS as_of_date
  FROM billyswitzer.staging_daily_stock_price_backfill
), adjusted_stock_source (ticker, volume, open, close, high, low, transactions, snapshot_date, as_of_date) AS
(
  SELECT ticker,
    ROUND(volume * COALESCE(split_to, 1) / COALESCE(split_from, 1), 4) AS volume,
    ROUND(open * COALESCE(split_from, 1) / COALESCE(split_to, 1), 4) AS open,
    ROUND(close * COALESCE(split_from, 1) / COALESCE(split_to, 1), 4) AS close,
    ROUND(high * COALESCE(split_from, 1) / COALESCE(split_to, 1), 4) AS high,
    ROUND(low * COALESCE(split_from, 1) / COALESCE(split_to, 1), 4) AS low,
    transactions,
    snapshot_date,
    as_of_date
  FROM stock_split_factor_map s
    JOIN max_date m ON 1=1
  WHERE stock_to_split_rn = 1
)
SELECT ticker,
  ARRAY_AGG(ROW(volume, open, close, high, low, transactions, snapshot_date) ORDER BY snapshot_date DESC) FILTER (WHERE snapshot_date > as_of_date - INTERVAL '365' DAY) AS price_array,
  as_of_date
FROM adjusted_stock_source
GROUP BY ticker,
  as_of_date