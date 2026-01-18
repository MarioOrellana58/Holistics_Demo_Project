WITH base AS (
  SELECT
    id,
    name,
    country_code
  FROM {{ #dim_cities }}
)

SELECT
  -- Surrogate key (stable, deterministic)
  MD5(CAST(id AS varchar)) AS sk_city,

  id,

  -- Normalize whitespace
  NULLIF(BTRIM(name), '') AS name,

  -- Standardize to uppercase + trim
  NULLIF(UPPER(BTRIM(country_code)), '') AS country_code
FROM base;
