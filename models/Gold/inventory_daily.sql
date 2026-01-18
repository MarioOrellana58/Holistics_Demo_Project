WITH snapshots AS (
  -- Silver inventory snapshots (one row per product_id + created_at)
  -- Replace #slv_fct_inventory with your actual Silver model name if different
  SELECT
    sk_inventory_snapshot,
    product_id,
    quantity_reserved,
    quantity_available,
    quantity_on_hand,
    created_at,
    CAST(created_at AS date) AS snapshot_date_key
  FROM {{ #slv_fct_inventory }}
  WHERE product_id IS NOT NULL
    AND created_at IS NOT NULL
),

ranked AS (
  /* ---------------------------------------------------
     Keep ONE snapshot per product per day:
     - latest snapshot in the day wins
  --------------------------------------------------- */
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY product_id, snapshot_date_key
      ORDER BY created_at DESC, sk_inventory_snapshot DESC
    ) AS rn
  FROM snapshots s
)

SELECT
  -- Surrogate key at daily grain
  MD5(
    CAST(product_id AS varchar) || '|' ||
    CAST(snapshot_date_key AS varchar)
  ) AS sk_inventory_daily,

  product_id,
  snapshot_date_key,

  quantity_on_hand,
  quantity_available,
  quantity_reserved,

  created_at AS snapshot_at
FROM ranked
WHERE rn = 1;
