WITH snapshots AS (
  -- Silver inventory snapshots (one row per product_id + created_at)
  -- Replace #slv_fct_inventory with your actual Silver model name if different
  SELECT
    sk_inventory_snapshot,
    product_id,
    quantity_reserved,
    quantity_available,
    quantity_on_hand,
    created_at
  FROM {{ #slv_fct_inventory }}
),

ranked AS (
  /* ---------------------------------------------------
     Keep the latest snapshot per product_id.
     Tie-breaker:
     - if multiple rows have the same created_at (shouldn't happen after silver),
       we keep the one with the highest on_hand/available/reserved.
  --------------------------------------------------- */
  SELECT
    s.*,
    ROW_NUMBER() OVER (
      PARTITION BY product_id
      ORDER BY
        created_at DESC,
        quantity_on_hand DESC,
        quantity_available DESC,
        quantity_reserved DESC,
        sk_inventory_snapshot DESC
    ) AS rn
  FROM snapshots s
  WHERE product_id IS NOT NULL
    AND created_at IS NOT NULL
)

SELECT
  -- Surrogate key at current grain (product)
  MD5(CAST(product_id AS varchar)) AS sk_inventory_current,

  product_id,

  quantity_on_hand,
  quantity_available,
  quantity_reserved,

  created_at AS latest_snapshot_at,

  -- Useful for joining to dim_dates if needed
  CAST(created_at AS date) AS snapshot_date_key

FROM ranked
WHERE rn = 1