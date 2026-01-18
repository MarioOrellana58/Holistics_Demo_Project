WITH base AS (
  SELECT
    code,
    name,
    continent_name
  FROM {{ #dim_countries }}
)

SELECT
  -- Surrogate key (stable, deterministic)
  MD5(CAST(UPPER(BTRIM(code)) AS varchar)) AS sk_country,

  -- Standardize to uppercase + trim
  NULLIF(UPPER(BTRIM(code)), '') AS code,

  -- Normalize whitespace
  NULLIF(BTRIM(name), '') AS name,
  NULLIF(BTRIM(continent_name), '') AS continent_name
FROM base;
