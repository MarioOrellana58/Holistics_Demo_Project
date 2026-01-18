WITH m AS (
  SELECT
    id,
    name,
    city_id,
    admin_id,
    created_at
  FROM {{ #dim_merchants }}
)

SELECT
  -- Volume / grain
  (SELECT COUNT(*) FROM m) AS total_rows,
  (SELECT COUNT(DISTINCT id) FROM m WHERE id IS NOT NULL) AS distinct_merchant_id,
  (SELECT COUNT(*)
   FROM (
     SELECT id
     FROM m
     WHERE id IS NOT NULL
     GROUP BY id
     HAVING COUNT(*) > 1
   ) AS dup_id
  ) AS duplicate_merchant_id,

  -- Null / empty checks
  (SELECT SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) FROM m) AS null_merchant_id,
  (SELECT SUM(CASE WHEN name IS NULL OR BTRIM(name) = '' THEN 1 ELSE 0 END) FROM m) AS null_or_empty_merchant_name,
  (SELECT SUM(CASE WHEN city_id IS NULL THEN 1 ELSE 0 END) FROM m) AS null_city_id,
  (SELECT SUM(CASE WHEN created_at IS NULL THEN 1 ELSE 0 END) FROM m) AS null_created_at,

  -- Potential natural-key duplicates (same merchant name in same city)
  (SELECT COUNT(*)
   FROM (
     SELECT
       LOWER(BTRIM(name)) AS merchant_name_norm,
       city_id
     FROM m
     WHERE name IS NOT NULL AND BTRIM(name) <> ''
       AND city_id IS NOT NULL
     GROUP BY LOWER(BTRIM(name)), city_id
     HAVING COUNT(*) > 1
   ) AS dup_name_city
  ) AS duplicate_merchant_name_city,

  -- Timestamp sanity (future dates)
  (SELECT COUNT(*)
   FROM m
   WHERE created_at IS NOT NULL
     AND created_at > CURRENT_TIMESTAMP
  ) AS created_at_in_future

;
