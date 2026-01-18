WITH u AS (
  SELECT
    id,
    sign_up_at,
    first_name,
    last_name,
    email,
    birth_date,
    gender,
    city_id,
    full_name
  FROM {{ #dim_users }}
),
dups AS (
  SELECT LOWER(BTRIM(email)) AS email_norm
  FROM u
  WHERE email IS NOT NULL AND BTRIM(email) <> ''
  GROUP BY 1
  HAVING COUNT(*) > 1
),
ranked AS (
  SELECT
    u.*,
    LOWER(BTRIM(u.email)) AS email_norm,
    ROW_NUMBER() OVER (
      PARTITION BY LOWER(BTRIM(u.email))
      ORDER BY sign_up_at DESC, id DESC
    ) AS rn
  FROM u
  LEFT JOIN dups d ON LOWER(BTRIM(u.email)) = d.email_norm
)
SELECT
  email_norm,
  rn AS email_rank,
  id,
  sign_up_at,
  first_name,
  last_name,
  birth_date,
  gender,
  city_id,
  full_name
FROM ranked
ORDER BY email_norm, rn;
