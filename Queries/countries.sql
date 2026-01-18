WITH c AS (
  SELECT
    code,
    name,
    continent_name
  FROM {{ #dim_countries }}
)

SELECT
  -- Volume / grain
  (SELECT COUNT(*) FROM c) AS total_rows,

  (SELECT COUNT(DISTINCT code)
   FROM c
   WHERE code IS NOT NULL) AS distinct_country_code,

  (SELECT COUNT(*)
   FROM (
     SELECT code
     FROM c
     WHERE code IS NOT NULL
     GROUP BY code
     HAVING COUNT(*) > 1
   ) AS dup_code
  ) AS duplicate_country_code,

  -- Null / empty checks
  (SELECT SUM(CASE WHEN code IS NULL OR BTRIM(code) = '' THEN 1 ELSE 0 END)
   FROM c
  ) AS null_or_empty_code,

  (SELECT SUM(CASE WHEN name IS NULL OR BTRIM(name) = '' THEN 1 ELSE 0 END)
   FROM c
  ) AS null_or_empty_name,

  (SELECT SUM(CASE WHEN continent_name IS NULL OR BTRIM(continent_name) = '' THEN 1 ELSE 0 END)
   FROM c
  ) AS null_or_empty_continent_name,

  -- Formatting checks
  (SELECT SUM(
     CASE
       WHEN code IS NOT NULL AND LENGTH(BTRIM(code)) <> 2 THEN 1
       ELSE 0
     END
   ) FROM c
  ) AS country_code_not_2_chars,

  (SELECT SUM(
     CASE
       WHEN code IS NOT NULL AND BTRIM(code) <> UPPER(BTRIM(code)) THEN 1
       ELSE 0
     END
   ) FROM c
  ) AS country_code_not_uppercase,

  -- Natural key duplicates (country name)
  (SELECT COUNT(*)
   FROM (
     SELECT LOWER(BTRIM(name)) AS country_name_norm
     FROM c
     WHERE name IS NOT NULL AND BTRIM(name) <> ''
     GROUP BY LOWER(BTRIM(name))
     HAVING COUNT(*) > 1
   ) AS dup_country_name
  ) AS duplicate_country_name;
