WITH src AS (
  -- Silver fact at grain (order_id, product_id)
  -- Replace #slv_orders with your actual Silver model name if different
  SELECT
    sk_order_product,
    order_id,
    product_id,
    user_id,
    order_status,
    created_at,
    delivered_at,
    cancelled_at,
    refunded_at,
    discount,
    delivery_attempts,
    item_values
  FROM {{ #slv_fct_orders }}
),

enriched AS (
  SELECT
    s.*,

    -- Date keys for role-playing date dimension joins
    CAST(s.created_at AS date)   AS created_date_key,
    CAST(s.delivered_at AS date) AS delivered_date_key,
    CAST(s.cancelled_at AS date) AS cancelled_date_key,
    CAST(s.refunded_at AS date)  AS refunded_date_key,

    -- Revenue fields (line grain)
    COALESCE(s.item_values, 0) AS gross_revenue,

    -- Keep discount as numeric; if it's null, treat as zero
    COALESCE(s.discount, 0) AS discount_amount,

    -- Net revenue (safe default)
    (COALESCE(s.item_values, 0) - COALESCE(s.discount, 0)) AS net_revenue

  FROM src s
)

SELECT
  -- Surrogate key at the line grain
  sk_order_product AS sk_order_line,

  order_id,
  product_id,
  user_id,

  order_status,
  created_at,
  delivered_at,
  cancelled_at,
  refunded_at,

  created_date_key,
  delivered_date_key,
  cancelled_date_key,
  refunded_date_key,

  delivery_attempts,

  gross_revenue,
  discount_amount,
  net_revenue
FROM enriched