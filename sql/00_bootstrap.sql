CREATE SCHEMA IF NOT EXISTS bi AUTHORIZATION metabase;

CREATE TABLE IF NOT EXISTS bi.kpi_daily (
  date                    date        NOT NULL,       -- CSV: day -> date
  entity                  varchar(8)  NOT NULL,       -- PF/PJ
  product                 varchar(64) NOT NULL,
  price_tier              varchar(32),
  anticipation_method     varchar(32),
  payment_method          varchar(32) NOT NULL,
  installments            int,
  amount_transacted       numeric(18,2),
  quantity_transactions   int,
  quantity_of_merchants   int,
  PRIMARY KEY (
    date, entity, product, price_tier,
    anticipation_method, payment_method, installments
  )
);

CREATE INDEX IF NOT EXISTS kpi_daily_idx_date ON bi.kpi_daily(date);
CREATE INDEX IF NOT EXISTS kpi_daily_idx_dims ON bi.kpi_daily(entity, product, payment_method);
