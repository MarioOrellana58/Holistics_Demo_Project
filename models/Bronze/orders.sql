-- Explore for cells
-- Dimensions:
--   fct_orders.cancelled_at
--   fct_orders.created_at
--   fct_orders.delivered_at
--   fct_orders.delivery_attempts
--   fct_orders.discount
--   fct_orders.item_values
--   fct_orders.order_id
--   fct_orders.order_status
--   fct_orders.product_id
--   fct_orders.refunded_at
--   fct_orders.user_id
-- [Aggregate Awareness - missed] (no valid PreAggregate in this Dataset)
WITH "fct_orders" AS (
  with a as (
    SELECT 
      p.id as product_id
      , o.id as order_id 
      , o.status as order_status
      , o.created_at::date
      , o.discount
      , o.delivery_attempts
      , o.user_id
      , oi.quantity * p.price as item_values
      , CASE WHEN o.status = 'cancelled' THEN o.created_at::date + (floor(random() * 10 + 1)::int || ' day')::interval END as cancelled_at
      , CASE WHEN o.status in ('delivered', 'refunded') THEN o.created_at::date + (floor(random() * 10 + 1)::int || ' day')::interval END as delivered_at
  FROM 
    ecommerce.order_items oi 
    LEFT JOIN ecommerce.orders o on oi.order_id = o.id
    LEFT JOIN ecommerce.products p on oi.product_id = p.id
  )
  select *
    , CASE 
        WHEN order_status = 'refunded' THEN delivered_at::date + (floor(random() * 10 + 1)::int || ' day')::interval
      END as refunded_at
  from a
)
SELECT
  "fct_orders"."product_id" AS "fo_pi_ce7683",
  "fct_orders"."order_id" AS "fo_oi_bd8fea",
  "fct_orders"."user_id" AS "fo_ui_beefbc",
  "fct_orders"."order_status" AS "fo_os_bc5faa",
  CAST ( "fct_orders"."created_at" AS date ) AS "fo_ca_659313",
  "fct_orders"."discount" AS "fo_d_87d562",
  "fct_orders"."delivery_attempts" AS "fo_da_3ad71b",
  "fct_orders"."item_values" AS "fo_iv_7fae4a",
  TO_CHAR(CAST ( "fct_orders"."cancelled_at" AS timestamptz ) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS "fo_ca_1c6e27",
  TO_CHAR(CAST ( "fct_orders"."delivered_at" AS timestamptz ) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS "fo_da_0e729a",
  TO_CHAR(CAST ( "fct_orders"."refunded_at" AS timestamptz ) AT TIME ZONE 'Etc/UTC', 'YYYY-MM-DD HH24:MI:SS.US') AS "fo_ra_9ade50"
FROM
  "fct_orders"
GROUP BY
  9,
  5,
  10,
  7,
  6,
  8,
  2,
  4,
  1,
  11,
  3
--LIMIT 5000
