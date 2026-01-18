WITH base AS (
  -- Bronze inventory snapshots (may contain duplicates per product_id + created_at)
  SELECT
    product_id,
    quantity_reserved,
    quantity_avaiable AS quantity_available,
    quantity_on_hand,
    created_at
  FROM {{ #fct_inventory }}
),

/* ---------------------------------------------------
   Silver rule:
   - One row per (product_id, created_at)
   - Remove exact duplicates by aggregating
--------------------------------------------------- */
dedup_snapshot AS (
  SELECT
    product_id,
    created_at,
    MAX(quantity_reserved)  AS quantity_reserved, -- I used MAX because I thought dups might be late arriving values
    MAX(quantity_available) AS quantity_available, -- I used MAX because I thought dups might be late arriving values
    MAX(quantity_on_hand)   AS quantity_on_hand, -- I used MAX because I thought dups might be late arriving values
    COUNT(*) AS source_rows_in_snapshot
  FROM base
  WHERE product_id IS NOT NULL
    AND created_at IS NOT NULL
  GROUP BY 1,2
)

SELECT
  -- Surrogate key at snapshot grain
  MD5(
    CAST(product_id AS varchar) || '|' ||
    CAST(created_at AS varchar)
  ) AS sk_inventory_snapshot,

  product_id,
  quantity_reserved,
  quantity_available,
  quantity_on_hand,
  created_at,

  -- Audit field: how many bronze rows collapsed into this snapshot row
  source_rows_in_snapshot
FROM dedup_snapshot;
