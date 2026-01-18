WITH lines AS (
  -- Gold order lines at grain (order_id, product_id)
  SELECT
    sk_order_line,
    order_id,
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
    gross_revenue,
    discount_amount,
    net_revenue
  FROM {{ #gld_fct_order_lines }}
),

/* ---------------------------------------------------
   Aggregate to one row per order_id.
   Because lines share the same order timestamps/status,
   we can safely use MAX() for timestamps and flags.
--------------------------------------------------- */
agg AS (
  SELECT
    order_id,

    -- User should be consistent per order; MAX is safe here.
    MAX(user_id) AS user_id,

    -- Use MAX timestamps to keep the latest known event timestamp per order
    MAX(created_at)   AS created_at,
    MAX(delivered_at) AS delivered_at,
    MAX(cancelled_at) AS cancelled_at,
    MAX(refunded_at)  AS refunded_at,

    -- Date keys (align with timestamps)
    MAX(created_date_key)   AS created_date_key,
    MAX(delivered_date_key) AS delivered_date_key,
    MAX(cancelled_date_key) AS cancelled_date_key,
    MAX(refunded_date_key)  AS refunded_date_key,

    -- Order-level amounts
    SUM(gross_revenue)    AS order_gross_revenue,
    SUM(discount_amount)  AS order_discount_amount,
    SUM(net_revenue)      AS order_net_revenue,

    -- Line count (useful for diagnostics / analysis)
    COUNT(*) AS order_line_count,

    -- Flags (helpful for filtering / QA)
    CASE WHEN MAX(delivered_at) IS NOT NULL THEN 1 ELSE 0 END AS is_delivered,
    CASE WHEN MAX(cancelled_at) IS NOT NULL THEN 1 ELSE 0 END AS is_cancelled,
    CASE WHEN MAX(refunded_at) IS NOT NULL THEN 1 ELSE 0 END AS is_refunded

  FROM lines
  WHERE order_id IS NOT NULL
  GROUP BY order_id
),

/* ---------------------------------------------------
   Normalize order status at order grain.
   Priority:
   - refunded if refunded exists
   - cancelled if cancelled exists
   - delivered if delivered exists
   - else 'created' (or 'in_progress' depending on your semantics)
--------------------------------------------------- */
final AS (
  SELECT
    a.*,
    CASE
      WHEN is_refunded = 1 THEN 'refunded'
      WHEN is_cancelled = 1 THEN 'cancelled'
      WHEN is_delivered = 1 THEN 'delivered'
      ELSE 'created'
    END AS order_status
  FROM agg a
)

SELECT
  -- Surrogate key at order grain
  MD5(CAST(order_id AS varchar)) AS sk_order,

  order_id,
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

  order_line_count,

  order_gross_revenue,
  order_discount_amount,
  order_net_revenue,

  is_delivered,
  is_cancelled,
  is_refunded

FROM final;
