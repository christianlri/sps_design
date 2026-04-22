CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_price_index_rough` AS
WITH 
  pim AS (
    SELECT 
      pim.product_id, 
      pim.brand_owner_name, 
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product` AS pim
    WHERE TRUE
    GROUP BY ALL
  ), 
  qc AS (
    SELECT 
      qc.global_entity_id,
      qc.country_code,
      qc.sku AS sku_id,
      qc.pim_product_id, 
    FROM `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qc
    WHERE TRUE
      AND REGEXP_CONTAINS( qc.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY ALL
  ),
  products AS (
    SELECT 
      qc.global_entity_id,
      qc.country_code,
      qc.sku_id,
      COALESCE(pim.brand_owner_name, 'Unbranded') AS brand_owner_name, 
    FROM qc
    LEFT JOIN pim
    ON qc.pim_product_id = pim.product_id
  ),
    sku_supplier_mapping_for_price_index AS (
      SELECT
        ssm.global_entity_id,
        ssm.sku_id,
        CAST(ssm.supplier_id AS STRING) AS supplier_id,
        ssm.supplier_name
      FROM
        `{{ params.project_id }}.{{ params.dataset.cl }}.sku_supplier_id_mapping` AS ssm
      WHERE TRUE
        AND REGEXP_CONTAINS( ssm.global_entity_id, {{ params.param_global_entity_id }})
      GROUP BY ALL
    ),
  principal_supplier AS (
    SELECT
      ps.country_code,
      ps.global_entity_id,
      CAST(ps.supplier_id AS STRING) as supplier_id,
      CAST(ps.principal_supplier_id AS STRING) AS principal_supplier_id,
      ps.supplier_name,
      ps.division_type
    FROM
      `{{ params.project_id }}.{{ params.dataset.cl }}._srm_supplier_supplier_hierarchy` AS ps
    WHERE TRUE
    AND REGEXP_CONTAINS( ps.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY ALL
  ),
  competitor_benchmark_base AS (
    SELECT
      cb.region,
      cb.global_entity_id,
      cb.country_name,
      CAST(cb.stamp_week AS STRING) AS stamp_week,
      CAST (DATE_TRUNC(cb.stamp_week, MONTH) AS STRING) AS price_index_month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM cb.stamp_week), '-', EXTRACT(YEAR FROM cb.stamp_week)) AS STRING) AS price_index_quarter_year,
      cb.dmart_sku AS sku,
      COALESCE(cb.l1_master_category, 'Unknown') AS l1_master_category,
      COALESCE(cb.l2_master_category, 'Unknown') AS l2_master_category,
      COALESCE(cb.l3_master_category, 'Unknown') AS l3_master_category, 
      cb.median_bp_index,
      cb.sku_gpv_eur,
      p.brand_owner_name,
    FROM
      `{{ params.project_id }}.{{ params.dataset.rl }}.competitor_benchmark_indices` AS cb
    LEFT JOIN
      products AS p
      ON cb.dmart_sku = p.sku_id
      AND cb.global_entity_id = p.global_entity_id
    WHERE TRUE
      AND cb.stamp_week BETWEEN DATE_SUB(CURRENT_DATE(), INTERVAL 52 WEEK) AND CURRENT_DATE()
      AND REGEXP_CONTAINS( cb.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY ALL
  ),
  preaggregated AS (
    SELECT
      cbb.region,
      cbb.global_entity_id,
      cbb.country_name,
      ps.division_type,
      cbb.l1_master_category,
      cbb.l2_master_category,
      cbb.l3_master_category,
      cbb.brand_owner_name,
      ps.principal_supplier_id,
      ssm.supplier_id,
      cbb.sku,
      cbb.price_index_quarter_year,
      cbb.price_index_month,
      cbb.median_bp_index,
      cbb.sku_gpv_eur
    FROM
      competitor_benchmark_base AS cbb
    LEFT JOIN
      sku_supplier_mapping_for_price_index AS ssm
      ON cbb.sku = ssm.sku_id
      AND cbb.global_entity_id = ssm.global_entity_id
    LEFT JOIN
      principal_supplier AS ps
      ON ssm.global_entity_id = ps.global_entity_id
      AND ssm.supplier_id = ps.supplier_id
    WHERE TRUE
    GROUP BY ALL
  )
SELECT
  global_entity_id,
  CASE
    WHEN GROUPING(price_index_month) = 0 THEN price_index_month
    ELSE price_index_quarter_year
  END AS time_period,
  COALESCE(principal_supplier_id, supplier_id, brand_owner_name) AS brand_sup,
  COALESCE(l3_master_category, l2_master_category, l1_master_category, principal_supplier_id, supplier_id, brand_owner_name) AS entity_key,
  CASE
    WHEN supplier_id IS NOT NULL AND l1_master_category IS NOT NULL THEN 'division'
    WHEN brand_owner_name IS NOT NULL AND l1_master_category IS NOT NULL THEN 'brand_owner'
    WHEN principal_supplier_id IS NOT NULL AND l1_master_category IS NOT NULL THEN 'principal'
    WHEN supplier_id IS NOT NULL THEN 'division'
    WHEN principal_supplier_id IS NOT NULL THEN 'principal'
    WHEN brand_owner_name IS NOT NULL THEN 'brand_owner'
    ELSE NULL
  END AS division_type,
  CASE
    WHEN l3_master_category IS NOT NULL THEN 'level_three'
    WHEN l2_master_category IS NOT NULL THEN 'level_two'
    WHEN l1_master_category IS NOT NULL THEN 'level_one'
    WHEN supplier_id IS NOT NULL OR principal_supplier_id IS NOT NULL OR brand_owner_name IS NOT NULL THEN 'supplier'
    ELSE NULL
  END AS supplier_level,
  CASE
    WHEN GROUPING(price_index_month) = 0 THEN 'Monthly'
    ELSE 'Quarterly'
  END AS time_granularity,
  ROUND(SAFE_DIVIDE(SUM(median_bp_index * sku_gpv_eur), SUM(sku_gpv_eur)), 2) AS median_price_index
FROM
  preaggregated
GROUP BY
  GROUPING SETS (
    -- Grouping by Category L1 (Monthly, Quarterly)
    (price_index_month, global_entity_id, supplier_id, l1_master_category),
    (price_index_quarter_year, global_entity_id, supplier_id, l1_master_category),
    (price_index_month, global_entity_id, brand_owner_name, l1_master_category),
    (price_index_quarter_year, global_entity_id, brand_owner_name, l1_master_category),
    (price_index_month, global_entity_id, principal_supplier_id, l1_master_category),
    (price_index_quarter_year, global_entity_id, principal_supplier_id, l1_master_category),
    -- Grouping by Category L2 (Monthly, Quarterly)
    (price_index_month, global_entity_id, supplier_id, l2_master_category),
    (price_index_quarter_year, global_entity_id, supplier_id, l2_master_category),
    (price_index_month, global_entity_id, brand_owner_name, l2_master_category),
    (price_index_quarter_year, global_entity_id, brand_owner_name, l2_master_category),
    (price_index_month, global_entity_id, principal_supplier_id, l2_master_category),
    (price_index_quarter_year, global_entity_id, principal_supplier_id, l2_master_category),
    -- Grouping by Category L3 (Monthly, Quarterly)
    (price_index_month, global_entity_id, supplier_id, l3_master_category),
    (price_index_quarter_year, global_entity_id, supplier_id, l3_master_category),
    (price_index_month, global_entity_id, brand_owner_name, l3_master_category),
    (price_index_quarter_year, global_entity_id, brand_owner_name, l3_master_category),
    (price_index_month, global_entity_id, principal_supplier_id, l3_master_category),
    (price_index_quarter_year, global_entity_id, principal_supplier_id, l3_master_category),
    -- Grouping by Supplier/Brand/Principal (Monthly, Quarterly)
    (price_index_month, global_entity_id, supplier_id),
    (price_index_quarter_year, global_entity_id, supplier_id),
    (price_index_month, global_entity_id, brand_owner_name),
    (price_index_quarter_year, global_entity_id, brand_owner_name),
    (price_index_month, global_entity_id, principal_supplier_id),
    (price_index_quarter_year, global_entity_id, principal_supplier_id)
  )
