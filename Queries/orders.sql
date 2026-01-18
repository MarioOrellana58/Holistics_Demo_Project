WITH base AS (
  SELECT *
  FROM {{ #fct_orders }}
),

dq AS (
  -- 1) Nulls en claves principales / FKs esperadas
  SELECT 'NULL_ORDER_ID' AS issue, COUNT(*) AS rows_affected
  FROM base
  WHERE order_id IS NULL

  UNION ALL
  SELECT 'NULL_USER_ID', COUNT(*)
  FROM base
  WHERE user_id IS NULL

  UNION ALL
  SELECT 'NULL_PRODUCT_ID', COUNT(*)
  FROM base
  WHERE product_id IS NULL

  UNION ALL
  SELECT 'NULL_CREATED_AT', COUNT(*)
  FROM base
  WHERE created_at IS NULL

  -- 2) Duplicados de order_id (si esperas 1 fila por orden)
  UNION ALL
  SELECT 'DUPLICATE_ORDER_ID', COUNT(*)
  FROM (
    SELECT order_id
    FROM base
    WHERE order_id IS NOT NULL
    GROUP BY order_id
    HAVING COUNT(*) > 1
  ) d

  -- 3) Valores inválidos de status (según tu descripción: cancelled/delivered/refunded)
  UNION ALL
  SELECT 'INVALID_ORDER_STATUS', COUNT(*)
  FROM base
  WHERE order_status IS NULL
     OR LOWER(order_status) NOT IN ('cancelled', 'delivered', 'refunded')

  -- 4) Reglas status vs timestamps (consistencia)
  UNION ALL
  SELECT 'DELIVERED_WITHOUT_DELIVERED_AT', COUNT(*)
  FROM base
  WHERE LOWER(order_status) = 'delivered' AND delivered_at IS NULL

  UNION ALL
  SELECT 'CANCELLED_WITHOUT_CANCELLED_AT', COUNT(*)
  FROM base
  WHERE LOWER(order_status) = 'cancelled' AND cancelled_at IS NULL

  UNION ALL
  SELECT 'REFUNDED_WITHOUT_REFUNDED_AT', COUNT(*)
  FROM base
  WHERE LOWER(order_status) = 'refunded' AND refunded_at IS NULL

  -- 5) Timestamps “imposibles” / orden temporal
  UNION ALL
  SELECT 'DELIVERED_BEFORE_CREATED', COUNT(*)
  FROM base
  WHERE delivered_at IS NOT NULL AND created_at IS NOT NULL AND delivered_at < created_at

  UNION ALL
  SELECT 'CANCELLED_BEFORE_CREATED', COUNT(*)
  FROM base
  WHERE cancelled_at IS NOT NULL AND created_at IS NOT NULL AND cancelled_at < created_at

  UNION ALL
  SELECT 'REFUNDED_BEFORE_CREATED', COUNT(*)
  FROM base
  WHERE refunded_at IS NOT NULL AND created_at IS NOT NULL AND refunded_at < created_at

  -- 6) Conflictos: multiples estados terminales en el mismo registro
  UNION ALL
  SELECT 'MULTIPLE_TERMINAL_TIMESTAMPS_SET', COUNT(*)
  FROM base
  WHERE (CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN refunded_at IS NOT NULL THEN 1 ELSE 0 END) > 1

  -- 7) Valores numéricos inválidos
  UNION ALL
  SELECT 'NEGATIVE_ITEM_VALUES', COUNT(*)
  FROM base
  WHERE item_values IS NOT NULL AND item_values < 0

  UNION ALL
  SELECT 'NEGATIVE_DISCOUNT', COUNT(*)
  FROM base
  WHERE discount IS NOT NULL AND discount < 0

  UNION ALL
  SELECT 'DISCOUNT_GREATER_THAN_ITEM_VALUES', COUNT(*)
  FROM base
  WHERE discount IS NOT NULL AND item_values IS NOT NULL AND discount > item_values

  UNION ALL
  SELECT 'NEGATIVE_DELIVERY_ATTEMPTS', COUNT(*)
  FROM base
  WHERE delivery_attempts IS NOT NULL AND delivery_attempts < 0
)

SELECT *
FROM dq
ORDER BY rows_affected DESC, issue;


















WITH base AS (
  SELECT *
  FROM {{ #fct_orders }}
),

violations AS (
  SELECT 'NULL_ORDER_ID' AS issue, order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE order_id IS NULL

  UNION ALL
  SELECT 'DUPLICATE_ORDER_ID', b.order_id, b.user_id, b.product_id, b.order_status, b.created_at, b.item_values, b.discount, b.delivery_attempts
  FROM base b
  JOIN (
    SELECT order_id
    FROM base
    WHERE order_id IS NOT NULL
    GROUP BY order_id
    HAVING COUNT(*) > 1
  ) d ON b.order_id = d.order_id

  UNION ALL
  SELECT 'INVALID_ORDER_STATUS', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE order_status IS NULL
     OR LOWER(order_status) NOT IN ('cancelled', 'delivered', 'refunded')

  UNION ALL
  SELECT 'DELIVERED_WITHOUT_DELIVERED_AT', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE LOWER(order_status) = 'delivered' AND delivered_at IS NULL

  UNION ALL
  SELECT 'CANCELLED_WITHOUT_CANCELLED_AT', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE LOWER(order_status) = 'cancelled' AND cancelled_at IS NULL

  UNION ALL
  SELECT 'REFUNDED_WITHOUT_REFUNDED_AT', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE LOWER(order_status) = 'refunded' AND refunded_at IS NULL

  UNION ALL
  SELECT 'MULTIPLE_TERMINAL_TIMESTAMPS_SET', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE (CASE WHEN delivered_at IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN cancelled_at IS NOT NULL THEN 1 ELSE 0 END
       + CASE WHEN refunded_at IS NOT NULL THEN 1 ELSE 0 END) > 1

  UNION ALL
  SELECT 'DELIVERED_BEFORE_CREATED', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE delivered_at IS NOT NULL AND created_at IS NOT NULL AND delivered_at < created_at

  UNION ALL
  SELECT 'CANCELLED_BEFORE_CREATED', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE cancelled_at IS NOT NULL AND created_at IS NOT NULL AND cancelled_at < created_at

  UNION ALL
  SELECT 'REFUNDED_BEFORE_CREATED', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE refunded_at IS NOT NULL AND created_at IS NOT NULL AND refunded_at < created_at

  UNION ALL
  SELECT 'NEGATIVE_ITEM_VALUES', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE item_values IS NOT NULL AND item_values < 0

  UNION ALL
  SELECT 'NEGATIVE_DISCOUNT', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE discount IS NOT NULL AND discount < 0

  UNION ALL
  SELECT 'DISCOUNT_GREATER_THAN_ITEM_VALUES', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE discount IS NOT NULL AND item_values IS NOT NULL AND discount > item_values

  UNION ALL
  SELECT 'NEGATIVE_DELIVERY_ATTEMPTS', order_id, user_id, product_id, order_status, created_at, item_values, discount, delivery_attempts
  FROM base
  WHERE delivery_attempts IS NOT NULL AND delivery_attempts < 0
)

SELECT *
FROM violations
ORDER BY issue, created_at DESC;





WITH base AS (
  SELECT order_id, product_id, user_id
  FROM {{ #fct_orders }}
)
SELECT
  (SELECT COUNT(*) FROM base) AS total_rows,

  (SELECT COUNT(DISTINCT order_id) FROM base) AS distinct_order_id,

  (SELECT COUNT(DISTINCT CAST(order_id AS varchar) || '|' || CAST(product_id AS varchar))
   FROM base
   WHERE order_id IS NOT NULL AND product_id IS NOT NULL
  ) AS distinct_order_product,

  (SELECT COUNT(DISTINCT CAST(order_id AS varchar) || '|' || CAST(product_id AS varchar) || '|' || CAST(user_id AS varchar))
   FROM base
   WHERE order_id IS NOT NULL AND product_id IS NOT NULL AND user_id IS NOT NULL
  ) AS distinct_order_product_user;
