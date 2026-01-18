WITH base AS (
  SELECT
    product_id,
    order_id,
    user_id,
    order_status,
    created_at,
    discount,
    delivery_attempts,
    item_values,
    cancelled_at,
    delivered_at,
    refunded_at
  FROM {{ #fct_orders }}
),

/* ---------------------------------------------------
   Fix ONLY the real conflict: delivered_at + cancelled_at
   - refunded_at is allowed with either delivered/cancelled
   Rule:
   - If both delivered and cancelled exist, keep the latest one
--------------------------------------------------- */
fixed AS (
  SELECT
    b.*,

    CASE
      WHEN delivered_at IS NOT NULL
       AND cancelled_at IS NOT NULL
       AND cancelled_at > delivered_at
        THEN NULL  -- cancelled happened after delivery, so delivery is invalid
      ELSE delivered_at
    END AS delivered_at_clean,

    CASE
      WHEN delivered_at IS NOT NULL
       AND cancelled_at IS NOT NULL
       AND delivered_at >= cancelled_at
        THEN NULL  -- delivery happened after/at cancellation, so cancellation is invalid
      ELSE cancelled_at
    END AS cancelled_at_clean
  FROM base b
),

/* ---------------------------------------------------
   Normalize status (optional but useful)
   Priority:
   - refunded if refunded_at exists
   - cancelled if cancelled_at_clean exists
   - delivered if delivered_at_clean exists
   - else keep original order_status (lowercase)
--------------------------------------------------- */
final AS (
  SELECT
    -- Surrogate key at the correct grain (order_id + product_id)
    MD5(
      CAST(order_id AS varchar) || '|' ||
      CAST(product_id AS varchar)
    ) AS sk_order_product,

    product_id,
    order_id,
    user_id,
    created_at,
    discount,
    delivery_attempts,
    item_values,

    cancelled_at_clean AS cancelled_at,
    delivered_at_clean AS delivered_at,
    refunded_at,

    CASE
      WHEN refunded_at IS NOT NULL THEN 'refunded'
      WHEN cancelled_at_clean IS NOT NULL THEN 'cancelled'
      WHEN delivered_at_clean IS NOT NULL THEN 'delivered'
      ELSE LOWER(order_status)
    END AS order_status,

    -- Debug flag so you can track what was “fixed”
    CASE
      WHEN delivered_at IS NOT NULL AND cancelled_at IS NOT NULL THEN 1
      ELSE 0
    END AS was_delivered_cancelled_conflict
  FROM fixed
)

SELECT *
FROM final;
