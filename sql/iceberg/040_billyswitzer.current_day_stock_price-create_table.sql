CREATE TABLE billyswitzer.current_day_stock_price (
  ticker VARCHAR(10),
  close_price_last_day DOUBLE,
  close_price_avg_last_90_days DOUBLE,
  close_price_avg_last_365_days DOUBLE,
  current_price DOUBLE,
  m_price_change_last_day DOUBLE,
  m_price_change_last_day_pct DOUBLE,
  m_price_change_last_90_days DOUBLE,
  m_price_change_last_90_days_pct DOUBLE,
  m_price_change_last_365_days DOUBLE,
  m_price_change_last_365_days_pct DOUBLE,
  market_cap_description VARCHAR(25),
  last_updated_datetime TIMESTAMP
  )
WITH (
  format = 'PARQUET'
)
