WITH u AS (
  SELECT
    id,
    sign_up_date,
    sign_up_at,
    first_name,
    last_name,
    email,
    birth_date,
    gender,
    city_id,
    full_name
  FROM {{ #dim_users }}
)

SELECT
  -- Volume / grain
  (SELECT COUNT(*) FROM u) AS total_rows,
  (SELECT COUNT(DISTINCT id) FROM u WHERE id IS NOT NULL) AS distinct_user_id,
  (SELECT COUNT(*)
   FROM (
     SELECT id
     FROM u
     WHERE id IS NOT NULL
     GROUP BY id
     HAVING COUNT(*) > 1
   ) AS dup_user_id
  ) AS duplicate_user_id,

  -- Null / empty core fields
  (SELECT SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) FROM u) AS null_user_id,
  (SELECT SUM(CASE WHEN email IS NULL OR BTRIM(email) = '' THEN 1 ELSE 0 END) FROM u) AS null_or_empty_email,
  (SELECT SUM(CASE WHEN sign_up_at IS NULL AND sign_up_date IS NULL THEN 1 ELSE 0 END) FROM u) AS missing_signup_datetime,
  (SELECT SUM(CASE WHEN city_id IS NULL THEN 1 ELSE 0 END) FROM u) AS null_city_id,

  -- Email formatting (lightweight)
  (SELECT COUNT(*)
   FROM u
   WHERE email IS NOT NULL
     AND BTRIM(email) <> ''
     AND POSITION('@' IN email) = 0
  ) AS email_missing_at_sign,

  (SELECT COUNT(*)
   FROM u
   WHERE email IS NOT NULL
     AND BTRIM(email) <> ''
     AND email ~ '\s'
  ) AS email_contains_whitespace,

  -- Duplicate emails (case-insensitive)
  (SELECT COUNT(*)
   FROM (
     SELECT LOWER(BTRIM(email)) AS email_norm
     FROM u
     WHERE email IS NOT NULL AND BTRIM(email) <> ''
     GROUP BY LOWER(BTRIM(email))
     HAVING COUNT(*) > 1
   ) AS dup_email
  ) AS duplicate_email,

  -- Signup consistency: sign_up_date should match date(sign_up_at) when both exist
  (SELECT COUNT(*)
   FROM u
   WHERE sign_up_date IS NOT NULL
     AND sign_up_at IS NOT NULL
     AND sign_up_date <> CAST(sign_up_at AS date)
  ) AS signup_date_mismatch,

  -- Birth date sanity
  (SELECT COUNT(*)
   FROM u
   WHERE birth_date IS NOT NULL
     AND birth_date > CURRENT_DATE
  ) AS birth_date_in_future,

  (SELECT COUNT(*)
   FROM u
   WHERE birth_date IS NOT NULL
     AND birth_date < DATE '1900-01-01'
  ) AS birth_date_too_old,

  -- Gender values (informational; adjust allowed values if you want)
  (SELECT COUNT(*)
   FROM u
   WHERE gender IS NOT NULL
     AND BTRIM(gender) <> ''
     AND LOWER(BTRIM(gender)) NOT IN ('male','female','m','f','other','unknown','non-binary','nonbinary')
  ) AS unexpected_gender_values,

  -- Name completeness
  (SELECT SUM(CASE WHEN (first_name IS NULL OR BTRIM(first_name) = '') AND (last_name IS NULL OR BTRIM(last_name) = '') THEN 1 ELSE 0 END)
   FROM u
  ) AS missing_first_and_last_name,

  (SELECT SUM(CASE WHEN full_name IS NULL OR BTRIM(full_name) = '' THEN 1 ELSE 0 END)
   FROM u
  ) AS missing_full_name

;
