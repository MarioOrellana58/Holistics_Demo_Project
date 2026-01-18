WITH base AS (
  -- Bronze categories table is already clean (1 row per category_id)
  SELECT
    id,
    name,
    parent_id,
    min_price,
    max_price,
    category_image_url
  FROM {{ #dim_categories }}
)

SELECT
  -- Surrogate key for category (stable, deterministic)
  MD5(CAST(id AS varchar)) AS sk_category,

  id,
  name,
  parent_id,
  min_price,
  max_price,

  -- Normalize blank strings to NULL (optional cleanliness)
  CASE
    WHEN category_image_url IS NULL OR TRIM(category_image_url) = '' THEN NULL
    ELSE category_image_url
  END AS category_image_url
FROM base;
