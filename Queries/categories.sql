-- Row count + distinct id
SELECT
  COUNT(*) AS total_rows,
  COUNT(DISTINCT id) AS distinct_id
FROM {{ #dim_categories }};




-- Duplicate ids (should be 0)
SELECT
  COUNT(*) AS duplicate_ids
FROM (
  SELECT id
  FROM {{ #dim_categories }}
  WHERE id IS NOT NULL
  GROUP BY 1
  HAVING COUNT(*) > 1
) d;



-- Completeness checks (nulls)
SELECT
  SUM(CASE WHEN id IS NULL THEN 1 ELSE 0 END) AS null_id,
  SUM(CASE WHEN name IS NULL OR TRIM(name) = '' THEN 1 ELSE 0 END) AS null_or_empty_name,
  SUM(CASE WHEN parent_id IS NULL THEN 1 ELSE 0 END) AS null_parent_id,
  SUM(CASE WHEN min_price IS NULL THEN 1 ELSE 0 END) AS null_min_price,
  SUM(CASE WHEN max_price IS NULL THEN 1 ELSE 0 END) AS null_max_price,
  SUM(CASE WHEN category_image_url IS NULL OR TRIM(category_image_url) = '' THEN 1 ELSE 0 END) AS null_or_empty_image_url
FROM {{ #dim_categories }};



-- parent_id references that don’t exist (orphans)
SELECT
  COUNT(*) AS orphan_parent_refs
FROM (SELECT * FROM {{ #dim_categories }}) c
LEFT JOIN (SELECT * FROM {{ #dim_categories }}) p
  ON c.parent_id = p.id
WHERE c.parent_id IS NOT NULL
  AND p.id IS NULL;




-- Self-parenting (category points to itself)
SELECT
  COUNT(*) AS self_parenting_rows
FROM {{ #dim_categories }}
WHERE parent_id IS NOT NULL
  AND parent_id = id;




-- Negative prices (should be 0)
SELECT
  SUM(CASE WHEN min_price < 0 THEN 1 ELSE 0 END) AS negative_min_price,
  SUM(CASE WHEN max_price < 0 THEN 1 ELSE 0 END) AS negative_max_price
FROM {{ #dim_categories }}
WHERE min_price IS NOT NULL OR max_price IS NOT NULL;




-- min_price > max_price (invalid range)
SELECT
  COUNT(*) AS min_gt_max
FROM {{ #dim_categories }}
WHERE min_price IS NOT NULL
  AND max_price IS NOT NULL
  AND min_price > max_price;



-- Extreme spreads (informational, can reveal data bugs)
SELECT
  COUNT(*) AS very_large_spread
FROM {{ #dim_categories }}
WHERE min_price IS NOT NULL
  AND max_price IS NOT NULL
  AND (max_price - min_price) > 100000;  -- adjust threshold if needed



-- Two-node cycle (A -> B and B -> A)

SELECT
  COUNT(*) AS two_node_cycles
FROM (SELECT * FROM {{ #dim_categories }}) a
JOIN (SELECT * FROM {{ #dim_categories }}) b
  ON a.parent_id = b.id
WHERE b.parent_id = a.id;