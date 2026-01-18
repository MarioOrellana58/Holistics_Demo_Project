WITH p AS (
  SELECT
    id,
    category_id,
    name,
    merchant_id,
    price,
    created_at,
    product_image_url,
    cost
  FROM {{ #dim_products }}
)

SELECT
  -- Volume / grain
  (SELECT COUNT(*) FROM p) AS total_rows,
  (SELECT COUNT(DISTINCT id) FROM p WHERE id IS NOT NULL) AS distinct_product_id,
  (SELECT COUNT(*)
   FROM (
     SELECT id
     FROM p
     WHERE id IS NOT NULL
     GROUP BY id
     HAVING COUNT(*) > 1
   ) AS dup_id
  ) AS duplicate_product_id,

  -- Null / empty checks
  (SELECT SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) FROM p) AS null_product_id,
  (SELECT SUM(CASE WHEN name IS NULL OR BTRIM(name) = '' THEN 1 ELSE 0 END) FROM p) AS null_or_empty_product_name,
  (SELECT SUM(CASE WHEN category_id IS NULL THEN 1 ELSE 0 END) FROM p) AS null_category_id,
  (SELECT SUM(CASE WHEN merchant_id IS NULL THEN 1 ELSE 0 END) FROM p) AS null_merchant_id,
  (SELECT SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) FROM p) AS null_created_at,

  -- Price / cost sanity
  (SELECT COUNT(*) FROM p WHERE price IS NOT NULL AND price < 0) AS negative_price,
  (SELECT COUNT(*) FROM p WHERE cost  IS NOT NULL AND cost  < 0) AS negative_cost,
  (SELECT COUNT(*) FROM p WHERE price IS NOT NULL AND cost IS NOT NULL AND cost > price) AS cost_greater_than_price,

  -- Image URL completeness (usually optional)
  (SELECT SUM(CASE WHEN product_image_url IS NULL OR BTRIM(product_image_url) = '' THEN 1 ELSE 0 END) FROM p) AS null_or_empty_image_url,

  -- Timestamp sanity
  (SELECT COUNT(*) FROM p WHERE created_at IS NOT NULL AND created_at > CURRENT_TIMESTAMP) AS created_at_in_future,

  -- Potential natural duplicates (same product name within same merchant)
  (SELECT COUNT(*)
   FROM (
     SELECT
       LOWER(BTRIM(name)) AS product_name_norm,
       merchant_id
     FROM p
     WHERE name IS NOT NULL AND BTRIM(name) <> ''
       AND merchant_id IS NOT NULL
     GROUP BY LOWER(BTRIM(name)), merchant_id
     HAVING COUNT(*) > 1
   ) AS dup_name_merchant
  ) AS duplicate_product_name_per_merchant
;
