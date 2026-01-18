WITH src AS (
  SELECT
    date_key,
    date,
    day_of_week,
    day_of_month,
    day_of_year,
    weekday_name,
    weekday_name_abbr,
    week_number,
    week_number_iso,
    year_week_iso,
    month_name,
    month_name_abbr,
    month_number,
    year_month,
    quarter,
    year_quarter,
    year
  FROM {{ #dim_dates }}
)

SELECT
  -- Surrogate key (optional, deterministic). date_key itself is already a PK.
  MD5(CAST(date_key AS varchar)) AS sk_date,

  -- Keys
  date_key::date AS date_key,
  date::date AS date,

  -- Numeric fields (cast safely from text)
  NULLIF(BTRIM(day_of_week), '')::int      AS day_of_week,
  NULLIF(BTRIM(day_of_month), '')::int     AS day_of_month,
  NULLIF(BTRIM(day_of_year), '')::int      AS day_of_year,
  NULLIF(BTRIM(week_number), '')::int      AS week_number,
  NULLIF(BTRIM(week_number_iso), '')::int  AS week_number_iso,
  NULLIF(BTRIM(month_number), '')::int     AS month_number,
  NULLIF(BTRIM(quarter), '')::int          AS quarter,
  NULLIF(BTRIM(year), '')::int             AS year,

  -- Name fields (trim padding from to_char('Day'/'Month'))
  NULLIF(BTRIM(weekday_name), '')          AS weekday_name,
  NULLIF(BTRIM(weekday_name_abbr), '')     AS weekday_name_abbr,
  NULLIF(BTRIM(month_name), '')            AS month_name,
  NULLIF(BTRIM(month_name_abbr), '')       AS month_name_abbr,

  -- Derived string fields (keep as-is but trim)
  NULLIF(BTRIM(year_week_iso), '')         AS year_week_iso,
  NULLIF(BTRIM(year_month), '')            AS year_month,
  NULLIF(BTRIM(year_quarter), '')          AS year_quarter

FROM src