WITH base AS (
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
  -- Surrogate key (stable, deterministic)
  MD5(CAST(id AS varchar)) AS sk_product,

  id,
  category_id,
  merchant_id,

  -- Normalize whitespace
  NULLIF(BTRIM(name), '') AS name,

  -- Optional helper field for matching/searching (does not change grain)
  LOWER(NULLIF(BTRIM(name), '')) AS name_normalized,

  price,
  cost,
  created_at,

  -- Normalize blank strings to NULL
  CASE
    WHEN product_image_url IS NULL OR BTRIM(product_image_url) = '' THEN NULL
    ELSE product_image_url
  END AS product_image_url
FROM base;
