-- Explore for cells
-- Dimensions:
--   fct_inventory.created_at
--   fct_inventory.product_id
--   fct_inventory.quantity_avaiable
--   fct_inventory.quantity_on_hand
--   fct_inventory.quantity_reserved
-- [Aggregate Awareness - missed] (no valid PreAggregate in this Dataset)
WITH "fct_inventory" AS (
  select p.id as product_id
    , case when o.status = 'delivered' then oi.quantity else 0 end as quantity_reserved
    , case when o.status = 'cancelled' then oi.quantity else 0 end as quantity_avaiable
    , case when o.status = 'refunded' then oi.quantity else 0 end as quantity_on_hand
    , o.created_at::date 
  from ecommerce.order_items oi
    left join ecommerce.orders o on oi.order_id = o.id
    left join ecommerce.products p on oi.product_id = p.id
)
SELECT
  "fct_inventory"."product_id" AS "fi_pi_a72ea1",
  "fct_inventory"."quantity_reserved" AS "fi_qr_610933",
  "fct_inventory"."quantity_avaiable" AS "fi_qa_c61503",
  "fct_inventory"."quantity_on_hand" AS "fi_qoh_127996",
  CAST ( "fct_inventory"."created_at" AS date ) AS "fi_ca_1633f9"
FROM
  "fct_inventory"
GROUP BY
  5,
  1,
  3,
  4,
  2
LIMIT 5000
