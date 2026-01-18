WITH c AS (
  SELECT
    id,
    name,
    country_code
  FROM {{ #dim_cities }}
)

SELECT
  -- Volume / grain
  (SELECT COUNT(*) FROM c) AS total_rows,

  (SELECT COUNT(DISTINCT id)
   FROM c
   WHERE id IS NOT NULL) AS distinct_city_id,

  (SELECT COUNT(*)
   FROM (
     SELECT id
     FROM c
     WHERE id IS NOT NULL
     GROUP BY id
     HAVING COUNT(*) > 1
   ) AS dup_id
  ) AS duplicate_city_id,

  -- Null / empty checks
  (SELECT SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) FROM c) AS null_city_id,

  (SELECT SUM(
     CASE
       WHEN name IS NULL OR BTRIM(name) = '' THEN 1
       ELSE 0
     END
   ) FROM c
  ) AS null_or_empty_city_name,

  (SELECT SUM(
     CASE
       WHEN country_code IS NULL OR BTRIM(country_code) = '' THEN 1
       ELSE 0
     END
   ) FROM c
  ) AS null_or_empty_country_code,

  -- Formatting checks
  (SELECT SUM(
     CASE
       WHEN country_code IS NOT NULL AND LENGTH(BTRIM(country_code)) <> 2 THEN 1
       ELSE 0
     END
   ) FROM c
  ) AS country_code_not_2_chars,

  (SELECT SUM(
     CASE
       WHEN country_code IS NOT NULL AND BTRIM(country_code) <> UPPER(BTRIM(country_code)) THEN 1
       ELSE 0
     END
   ) FROM c
  ) AS country_code_not_uppercase,

  -- Natural-key duplicates: city name + country
  (SELECT COUNT(*)
   FROM (
     SELECT
       LOWER(BTRIM(name)) AS city_name_norm,
       UPPER(BTRIM(country_code)) AS country_code_norm
     FROM c
     WHERE name IS NOT NULL AND BTRIM(name) <> ''
       AND country_code IS NOT NULL AND BTRIM(country_code) <> ''
     GROUP BY
       LOWER(BTRIM(name)),
       UPPER(BTRIM(country_code))
     HAVING COUNT(*) > 1
   ) AS dup_city_country
  ) AS duplicate_city_name_country;
