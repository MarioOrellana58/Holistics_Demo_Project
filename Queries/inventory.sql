-- Is it one row per product?
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT product_id) AS distinct_product_id
FROM {{ #inventory }};



-- Are there duplicates per product_id?
SELECT
  COUNT(*) AS duplicate_products
FROM (
  SELECT product_id
  FROM {{ #inventory }}
  WHERE product_id IS NOT NULL
  GROUP BY 1
  HAVING COUNT(*) > 1
) d;




-- If duplicates exist: is it a snapshot table (product_id + created_at)?
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT CAST(product_id AS varchar) || '|' || CAST(created_at AS varchar)) AS distinct_product_created
FROM {{ #inventory }}
WHERE product_id IS NOT NULL AND created_at IS NOT NULL;




-- Completeness checks (nulls)
SELECT
  SUM(CASE WHEN product_id IS NULL THEN 1 ELSE 0 END) AS null_product_id,
  SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) AS null_created_at,
  SUM(CASE WHEN quantity_on_hand IS NULL THEN 1 ELSE 0 END) AS null_quantity_on_hand,
  SUM(CASE WHEN quantity_available IS NULL THEN 1 ELSE 0 END) AS null_quantity_available,
  SUM(CASE WHEN quantity_reserved IS NULL THEN 1 ELSE 0 END) AS null_quantity_reserved
FROM {{ #inventory }};



-- Valid range checks (negatives)
SELECT
  SUM(CASE WHEN quantity_on_hand < 0 THEN 1 ELSE 0 END) AS negative_on_hand,
  SUM(CASE WHEN quantity_available < 0 THEN 1 ELSE 0 END) AS negative_available,
  SUM(CASE WHEN quantity_reserved < 0 THEN 1 ELSE 0 END) AS negative_reserved
FROM {{ #inventory }}
WHERE quantity_on_hand IS NOT NULL
   OR quantity_available IS NOT NULL
   OR quantity_reserved IS NOT NULL;




-- I'm ignoring if we sold more than we have in stock, because it might be a valid business case.




-- Future timestamps
SELECT
  COUNT(*) AS created_at_in_future
FROM {{ #inventory }}
WHERE created_at > CURRENT_TIMESTAMP;




-- Multiple records per product at same created_at (true duplicates)
SELECT
  COUNT(*) AS duplicate_product_created_at
FROM (
  SELECT product_id, created_at
  FROM {{ #inventory }}
  WHERE product_id IS NOT NULL AND created_at IS NOT NULL
  GROUP BY 1,2
  HAVING COUNT(*) > 1
) d;
