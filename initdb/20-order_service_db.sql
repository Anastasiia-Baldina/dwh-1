\connect order_service_db;

CREATE TABLE IF NOT EXISTS public.orders (
  order_id                    serial PRIMARY KEY,
  order_external_id           uuid UNIQUE,
  user_external_id            uuid,          -- cross-db (FK НЕ делаем)
  order_number                varchar UNIQUE,
  order_date                  timestamp,
  status                      varchar,
  subtotal                    decimal,
  tax_amount                  decimal,
  shipping_cost               decimal,
  discount_amount             decimal,
  total_amount                decimal,
  currency                    varchar,
  delivery_address_external_id uuid,         -- cross-db (FK НЕ делаем)
  delivery_type               varchar,
  expected_delivery_date      date,
  actual_delivery_date        date,
  payment_method              varchar,
  payment_status              varchar,
  effective_from              timestamp,
  effective_to                timestamp,
  is_current                  boolean,
  created_at                  timestamp,
  updated_at                  timestamp,
  created_by                  varchar,
  updated_by                  varchar
);

CREATE TABLE IF NOT EXISTS public.products (
  product_id              serial PRIMARY KEY,
  product_sku             varchar UNIQUE,
  product_name            varchar,
  category                varchar,
  brand                   varchar,
  price                   decimal,
  currency                varchar,
  weight_grams            integer,
  dimensions_length_cm    decimal,
  dimensions_width_cm     decimal,
  dimensions_height_cm    decimal,
  is_active               boolean,
  effective_from          timestamp,
  effective_to            timestamp,
  is_current              boolean,
  created_at              timestamp,
  updated_at              timestamp,
  created_by              varchar,
  updated_by              varchar
);

CREATE TABLE IF NOT EXISTS public.order_items (
  order_item_id               serial PRIMARY KEY,
  order_external_id           uuid,
  product_sku                 varchar,
  quantity                    integer,
  unit_price                  decimal,
  total_price                 decimal,
  product_name_snapshot       varchar,
  product_category_snapshot   varchar,
  product_brand_snapshot      varchar,
  created_at                  timestamp,
  updated_at                  timestamp,
  created_by                  varchar,
  updated_by                  varchar,
  CONSTRAINT fk_order_items_order_external_id
    FOREIGN KEY (order_external_id) REFERENCES public.orders(order_external_id),
  CONSTRAINT fk_order_items_product_sku
    FOREIGN KEY (product_sku) REFERENCES public.products(product_sku)
);

CREATE TABLE IF NOT EXISTS public.order_status_history (
  history_id       serial PRIMARY KEY,
  order_external_id uuid,
  old_status       varchar,
  new_status       varchar,
  change_reason    varchar,
  changed_at       timestamp,
  changed_by       varchar,
  session_id       varchar,
  ip_address       inet,
  notes            text,
  CONSTRAINT fk_order_status_history_order_external_id
    FOREIGN KEY (order_external_id) REFERENCES public.orders(order_external_id)
);

CREATE OR REPLACE VIEW public.v_cohort_mart AS
WITH base AS (
  SELECT
    user_external_id,
    date_trunc('month', order_date)::date AS order_month,
    total_amount
  FROM public.orders
  WHERE user_external_id IS NOT NULL
    AND order_date IS NOT NULL
    AND (is_current IS TRUE OR is_current IS NULL)
),
cohorts AS (
  SELECT
    user_external_id,
    MIN(order_month) AS cohort_month
  FROM base
  GROUP BY 1
),
activity AS (
  SELECT
    c.cohort_month,
    b.user_external_id,
    b.order_month,
    (
      (EXTRACT(YEAR FROM age(b.order_month, c.cohort_month))::int * 12)
      + EXTRACT(MONTH FROM age(b.order_month, c.cohort_month))::int
    ) AS months_since,
    b.total_amount
  FROM base b
  JOIN cohorts c USING (user_external_id)
  WHERE b.order_month >= c.cohort_month
),
cohort_sizes AS (
  SELECT cohort_month, COUNT(*) AS cohort_size
  FROM cohorts
  GROUP BY 1
),
active_by_period AS (
  SELECT
    cohort_month,
    months_since,
    COUNT(DISTINCT user_external_id) AS active_customers
  FROM activity
  WHERE months_since BETWEEN 0 AND 5
  GROUP BY 1,2
),
revenue AS (
  SELECT
    cohort_month,
    COALESCE(SUM(total_amount), 0)::numeric AS total_cohort_revenue
  FROM activity
  GROUP BY 1
)
SELECT
  cs.cohort_month,
  cs.cohort_size,

  ROUND(100.0 * COALESCE(MAX(CASE WHEN abp.months_since=0 THEN abp.active_customers END),0) / cs.cohort_size, 2) AS period_0_pct,
  ROUND(100.0 * COALESCE(MAX(CASE WHEN abp.months_since=1 THEN abp.active_customers END),0) / cs.cohort_size, 2) AS period_1_pct,
  ROUND(100.0 * COALESCE(MAX(CASE WHEN abp.months_since=2 THEN abp.active_customers END),0) / cs.cohort_size, 2) AS period_2_pct,
  ROUND(100.0 * COALESCE(MAX(CASE WHEN abp.months_since=3 THEN abp.active_customers END),0) / cs.cohort_size, 2) AS period_3_pct,
  ROUND(100.0 * COALESCE(MAX(CASE WHEN abp.months_since=4 THEN abp.active_customers END),0) / cs.cohort_size, 2) AS period_4_pct,
  ROUND(100.0 * COALESCE(MAX(CASE WHEN abp.months_since=5 THEN abp.active_customers END),0) / cs.cohort_size, 2) AS period_5_pct,

  r.total_cohort_revenue,
  ROUND((r.total_cohort_revenue / cs.cohort_size)::numeric, 2) AS avg_revenue_per_customer
FROM cohort_sizes cs
LEFT JOIN active_by_period abp ON abp.cohort_month = cs.cohort_month
LEFT JOIN revenue r ON r.cohort_month = cs.cohort_month
GROUP BY cs.cohort_month, cs.cohort_size, r.total_cohort_revenue
ORDER BY cs.cohort_month;