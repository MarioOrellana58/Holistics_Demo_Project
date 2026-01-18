WITH base AS (
  SELECT
    id,
    name,
    city_id,
    admin_id,
    created_at
  FROM {{ #dim_merchants }}
)

SELECT
  -- Surrogate key (stable, deterministic)
  MD5(CAST(id AS varchar)) AS sk_merchant,

  id,
  NULLIF(BTRIM(name), '') AS name,
  city_id,
  admin_id,
  created_at
FROM base;
