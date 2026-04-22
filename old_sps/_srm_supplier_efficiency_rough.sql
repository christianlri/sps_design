CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_efficiency_rough` AS

WITH recent_supplier AS (
  SELECT
    warehouse_id,
    sku_id,
    supplier_id,
    supplier_name,
    global_entity_id,
    month,
    ROW_NUMBER() OVER (
      PARTITION BY warehouse_id, sku_id, global_entity_id, supplier_id
      ORDER BY month DESC
    ) AS rn
  FROM
    `fulfillment-dwh-production.cl_dmart.sku_supplier_id_mapping`

  WHERE global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
),
filtered_supplier AS (
  SELECT
    *
  FROM
    recent_supplier
  WHERE
    rn = 1
    AND warehouse_id IS NOT NULL
    AND global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
),

 products AS (

    SELECT
    global_entity_id, sku_id, brand_owner_name

    FROM
    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as o

    WHERE
    o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL

  ),
final AS (
  SELECT
    e.*, 
    p.brand_owner_name,
    CONCAT('Q', EXTRACT(QUARTER FROM e.month), '-', EXTRACT(YEAR FROM e.month)) AS quarter_year,
    fs.supplier_id,
    fs.supplier_name
  FROM
    `fulfillment-dwh-production.rl_dmart._aqs_v5_sku_efficiency_detail` AS e
  LEFT JOIN
    filtered_supplier AS fs
  ON
    e.global_entity_id = fs.global_entity_id
    AND e.sku = fs.sku_id
    AND e.warehouse_id = fs.warehouse_id

  LEFT JOIN
    products AS p
  ON
    e.global_entity_id = p.global_entity_id
    AND e.sku = p.sku_id

  WHERE
    e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
    AND e.is_listed
),

efficiency AS (
SELECT
f.* except(supplier_id, supplier_name),
CAST(h.supplier_id AS STRING) as supplier_id,
h.supplier_name, 
principal_supplier_id,
h.parent_name,

FROM
  final as f

 LEFT JOIN
  `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` as h

  ON
  f.global_entity_id = h.global_entity_id
  AND
  CAST(f.supplier_id AS STRING) = CAST(h.supplier_id AS STRING)

WHERE
  f.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  ),



--------------------------------------------------------------------------------------------------- DIVISION LEVEL ------------------------------------------------------------------------

division_wi_monthly AS (
  SELECT e.global_entity_id
    , s.country_code
    , 'division' division_type
    , 'Monthly' as period
    , e.month
    , e.supplier_id
    , e.supplier_name
    , CAST(NULL AS STRING) AS brand_owner_name
    , COUNT(DISTINCT e.sku) sku_listed
    , COUNT(DISTINCT CASE WHEN date_diff >=90 THEN e.sku END) AS sku_mature
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN e.sku END) AS sku_probation
    , COUNT(DISTINCT CASE WHEN date_diff <=30 THEN e.sku END) AS sku_new
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND e.new_availability = 1)  THEN e.sku END) zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND (e.new_availability < 1 OR e.new_availability IS NULL))  THEN e.sku END) la_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability >= 0.8) THEN e.sku END) slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability < 0.8 OR e.new_availability IS NULL)  THEN e.sku END) la_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) efficient_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL)  THEN e.sku END) new_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold < 1)  THEN e.sku END) new_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) new_efficient_movers
    , ROUND(SUM(e.sold_items),1) AS sold_items
    , ROUND(SUM(e.gpv_eur),1) AS gpv_eur
  FROM  efficiency AS e
  LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
--   #Excluding Closing Warehouses
--   LEFT OUTER JOIN `{{ params.project_id }}.dl_dmart.gsheet_aqs_closed_warehouses` AS w ON w.warehouse_id = e.warehouse_id AND w.global_entity_id = e.global_entity_id
--   WHERE  w.warehouse_id IS NULL
  WHERE
  e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
  AND e.is_listed
  GROUP BY ALL
)
,

division_wi_quarterly AS (
    SELECT
    e.global_entity_id
    -- , s.country_code
    , 'division' division_type
    , 'Quarterly' as period
    , e.quarter_year
    , e.supplier_id
    , e.supplier_name
    , CAST(NULL AS STRING) AS brand_owner_name
    , COUNT(DISTINCT e.sku) sku_listed
    , COUNT(DISTINCT CASE WHEN date_diff >=90 THEN e.sku END) AS sku_mature
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN e.sku END) AS sku_probation
    , COUNT(DISTINCT CASE WHEN date_diff <=30 THEN e.sku END) AS sku_new
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND e.new_availability = 1)  THEN e.sku END) zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND (e.new_availability < 1 OR e.new_availability IS NULL))  THEN e.sku END) la_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability >= 0.8) THEN e.sku END) slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability < 0.8 OR e.new_availability IS NULL)  THEN e.sku END) la_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) efficient_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL)  THEN e.sku END) new_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold < 1)  THEN e.sku END) new_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) new_efficient_movers
    , ROUND(SUM(e.sold_items),1) AS sold_items
    , ROUND(SUM(e.gpv_eur),1) AS gpv_eur
   FROM  efficiency AS e
  LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
--   #Excluding Closing Warehouses
--   LEFT OUTER JOIN `{{ params.project_id }}.dl_dmart.gsheet_aqs_closed_warehouses` AS w ON w.warehouse_id = e.warehouse_id AND w.global_entity_id = e.global_entity_id
--   WHERE  w.warehouse_id IS NULL
  WHERE
  e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
  AND e.is_listed


  GROUP BY ALL
),

--------------------------------------------------------------------------------------------------- PARENT LEVEL ------------------------------------------------------------------------

principal_wi_monthly AS (
  SELECT 
    e.global_entity_id
    , s.country_code
    , 'principal' division_type
    , 'Monthly' as period
    , e.month
    , e.principal_supplier_id as supplier_id
    , e.parent_name as supplier_name
    , CAST(NULL AS STRING) AS brand_owner_name
    , COUNT(DISTINCT e.sku) sku_listed
    , COUNT(DISTINCT CASE WHEN date_diff >=90 THEN e.sku END) AS sku_mature
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN e.sku END) AS sku_probation
    , COUNT(DISTINCT CASE WHEN date_diff <=30 THEN e.sku END) AS sku_new
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND e.new_availability = 1)  THEN e.sku END) zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND (e.new_availability < 1 OR e.new_availability IS NULL))  THEN e.sku END) la_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability >= 0.8) THEN e.sku END) slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability < 0.8 OR e.new_availability IS NULL)  THEN e.sku END) la_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) efficient_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL)  THEN e.sku END) new_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold < 1)  THEN e.sku END) new_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) new_efficient_movers
    , ROUND(SUM(e.sold_items),1) AS sold_items
    , ROUND(SUM(e.gpv_eur),1) AS gpv_eur
  FROM  efficiency AS e
  LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
  #Excluding Closing Warehouses
  -- LEFT OUTER JOIN `{{ params.project_id }}.dl_dmart.gsheet_aqs_closed_warehouses` AS w ON w.warehouse_id = e.warehouse_id AND w.global_entity_id = e.global_entity_id
  -- WHERE  w.warehouse_id IS NULL
  WHERE
  e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
  AND e.is_listed
  GROUP BY ALL
),

principal_wi_quarterly AS (
  SELECT 
      e.global_entity_id
    , s.country_code
    , 'principal' division_type
    , 'Quarterly' as period
    , e.quarter_year
    , e.principal_supplier_id as supplier_id
    , e.parent_name as supplier_name
    , CAST(NULL AS STRING) AS brand_owner_name
    , COUNT(DISTINCT e.sku) sku_listed
    , COUNT(DISTINCT CASE WHEN date_diff >=90 THEN e.sku END) AS sku_mature
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN e.sku END) AS sku_probation
    , COUNT(DISTINCT CASE WHEN date_diff <=30 THEN e.sku END) AS sku_new
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND e.new_availability = 1)  THEN e.sku END) zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND (e.new_availability < 1 OR e.new_availability IS NULL))  THEN e.sku END) la_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability >= 0.8) THEN e.sku END) slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability < 0.8 OR e.new_availability IS NULL)  THEN e.sku END) la_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) efficient_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL)  THEN e.sku END) new_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold < 1)  THEN e.sku END) new_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) new_efficient_movers
    , ROUND(SUM(e.sold_items),1) AS sold_items
    , ROUND(SUM(e.gpv_eur),1) AS gpv_eur
   FROM  efficiency AS e
  LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
--   #Excluding Closing Warehouses
--   LEFT OUTER JOIN `{{ params.project_id }}.dl_dmart.gsheet_aqs_closed_warehouses` AS w ON w.warehouse_id = e.warehouse_id AND w.global_entity_id = e.global_entity_id
--   WHERE  w.warehouse_id IS NULL
  WHERE
  e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
  AND e.is_listed
  GROUP BY ALL
),

--------------------------------------------------------------------------------------------------- BRAND OWNER LEVEL ------------------------------------------------------------------------

brand_owner_wi_monthly AS (
  SELECT 
    e.global_entity_id
    , s.country_code
    , 'brand_owner' division_type
    , 'Monthly' as period
    , e.month
    , CAST(NULL AS STRING) as supplier_id
    , CAST(NULL AS STRING) supplier_name
    , e.brand_owner_name
    , COUNT(DISTINCT e.sku) sku_listed
    , COUNT(DISTINCT CASE WHEN date_diff >=90 THEN e.sku END) AS sku_mature
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN e.sku END) AS sku_probation
    , COUNT(DISTINCT CASE WHEN date_diff <=30 THEN e.sku END) AS sku_new
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND e.new_availability = 1)  THEN e.sku END) zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND (e.new_availability < 1 OR e.new_availability IS NULL))  THEN e.sku END) la_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability >= 0.8) THEN e.sku END) slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability < 0.8 OR e.new_availability IS NULL)  THEN e.sku END) la_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) efficient_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL)  THEN e.sku END) new_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold < 1)  THEN e.sku END) new_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) new_efficient_movers
    , ROUND(SUM(e.sold_items),1) AS sold_items
    , ROUND(SUM(e.gpv_eur),1) AS gpv_eur
  FROM  efficiency AS e
  LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
  #Excluding Closing Warehouses
  -- LEFT OUTER JOIN `{{ params.project_id }}.dl_dmart.gsheet_aqs_closed_warehouses` AS w ON w.warehouse_id = e.warehouse_id AND w.global_entity_id = e.global_entity_id
  -- WHERE  w.warehouse_id IS NULL
  WHERE
  e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
  AND e.is_listed
  GROUP BY ALL
),

brand_owner_wi_quarterly AS (
  SELECT 
      e.global_entity_id
    , s.country_code
    , 'brand_owner' division_type
    , 'Quarterly' as period
    , e.quarter_year
    , CAST(NULL AS STRING) as supplier_id
    , CAST(NULL AS STRING) supplier_name
    , e.brand_owner_name
    , COUNT(DISTINCT e.sku) sku_listed
    , COUNT(DISTINCT CASE WHEN date_diff >=90 THEN e.sku END) AS sku_mature
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND date_diff >30 THEN e.sku END) AS sku_probation
    , COUNT(DISTINCT CASE WHEN date_diff <=30 THEN e.sku END) AS sku_new
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND e.new_availability = 1)  THEN e.sku END) zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND ((e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL) AND (e.new_availability < 1 OR e.new_availability IS NULL))  THEN e.sku END) la_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability >= 0.8) THEN e.sku END) slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold < 1 AND e.avg_qty_sold > 0) AND (e.new_availability < 0.8 OR e.new_availability IS NULL)  THEN e.sku END) la_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff >=90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) efficient_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold = 0 OR e.avg_qty_sold IS NULL)  THEN e.sku END) new_zero_movers
    , COUNT(DISTINCT CASE WHEN date_diff <30 AND (e.avg_qty_sold < 1)  THEN e.sku END) new_slow_movers
    , COUNT(DISTINCT CASE WHEN date_diff <90 AND (e.avg_qty_sold >= 1)  THEN e.sku END) new_efficient_movers
    , ROUND(SUM(e.sold_items),1) AS sold_items
    , ROUND(SUM(e.gpv_eur),1) AS gpv_eur
   FROM  efficiency AS e
  LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
--   #Excluding Closing Warehouses
--   LEFT OUTER JOIN `{{ params.project_id }}.dl_dmart.gsheet_aqs_closed_warehouses` AS w ON w.warehouse_id = e.warehouse_id AND w.global_entity_id = e.global_entity_id
--   WHERE  w.warehouse_id IS NULL
  WHERE
  e.partition_month BETWEEN '2024-07-01' AND CURRENT_DATE()
  AND e.is_listed
  GROUP BY ALL
),


--------------------------------------------------------------------------------------------------- Time period aggregation  ------------------------------------------------------------------------

--------------------------------------------------------------------------------------------------- DIVISION LEVEL ------------------------------------------------------------------------

division_monthly AS (SELECT
  s.management_entity
  , wim.global_entity_id
  , s.country_code
  , division_type
  , s.common_name as country_name
  , s.region
  , period as time_granularity
  , CAST(month AS STRING) as time_period
  , CAST(wim.supplier_id AS STRING) AS supplier_id
  , wim.supplier_name
  , CAST(NULL AS STRING) AS brand_owner_name
  , sku_listed
  , sku_mature
  , sku_probation
  , sku_new
  , zero_movers
  , la_zero_movers
  , slow_movers
  , la_slow_movers
  , efficient_movers
  , new_zero_movers
  , new_slow_movers
  , new_efficient_movers
  , sold_items
  , gpv_eur
  --#Since new Listed SKU Definition for countries with Status Hierarchy started on 2023-07-01, this case when defines which Book Value to take
  , ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency
  , ROUND(SAFE_DIVIDE(new_efficient_movers,sku_listed),2) AS perc_newness
  ,ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency
  , ROUND((SAFE_DIVIDE(new_efficient_movers,sku_listed)*gpv_eur),2) AS weight_newness

FROM division_wi_monthly as wim
LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

GROUP BY ALL),

division_quarterly AS (
  
  SELECT
  s.management_entity
  , wim.global_entity_id
  , s.country_code
  , division_type
  , s.common_name as country_name
  , s.region
  , period as time_granularity
  , CAST(quarter_year AS STRING) as time_period
  , CAST(wim.supplier_id AS STRING) AS supplier_id
  , wim.supplier_name
  , CAST(NULL AS STRING) AS brand_owner_name
  , sku_listed
  , sku_mature
  , sku_probation
  , sku_new
  , zero_movers
  , la_zero_movers
  , slow_movers
  , la_slow_movers
  , efficient_movers
  , new_zero_movers
  , new_slow_movers
  , new_efficient_movers
  , sold_items
  , gpv_eur
  --#Since new Listed SKU Definition for countries with Status Hierarchy started on 2023-07-01, this case when defines which Book Value to take
  , ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency
  , ROUND(SAFE_DIVIDE(new_efficient_movers,sku_listed),2) AS perc_newness
  ,ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency
  , ROUND((SAFE_DIVIDE(new_efficient_movers,sku_listed)*gpv_eur),2) AS weight_newness

FROM division_wi_quarterly as wim

LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

GROUP BY ALL

),

--------------------------------------------------------------------------------------------------- PRINCIPAL LEVEL ------------------------------------------------------------------------

principal_monthly AS (SELECT
  s.management_entity
  , wim.global_entity_id
  , s.country_code
  , division_type
  , s.common_name as country_name
  , s.region
  , period as time_granularity
  , CAST(month AS STRING) as time_period
  , CAST(wim.supplier_id AS STRING) AS supplier_id
  , wim.supplier_name
  , CAST(NULL AS STRING) AS brand_owner_name
  , sku_listed
  , sku_mature
  , sku_probation
  , sku_new
  , zero_movers
  , la_zero_movers
  , slow_movers
  , la_slow_movers
  , efficient_movers
  , new_zero_movers
  , new_slow_movers
  , new_efficient_movers
  , sold_items
  , gpv_eur
  --#Since new Listed SKU Definition for countries with Status Hierarchy started on 2023-07-01, this case when defines which Book Value to take
  , ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency
  , ROUND(SAFE_DIVIDE(new_efficient_movers,sku_listed),2) AS perc_newness
  ,ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency
  , ROUND((SAFE_DIVIDE(new_efficient_movers,sku_listed)*gpv_eur),2) AS weight_newness

FROM principal_wi_monthly as wim
LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

GROUP BY ALL),

principal_quarterly AS (SELECT
  s.management_entity
  , wim.global_entity_id
  , s.country_code
  , division_type
  , s.common_name as country_name
  , s.region
  , period as time_granularity
  , CAST(quarter_year AS STRING) as time_period
  , wim.supplier_id
  , wim.supplier_name
  , CAST(NULL AS STRING) AS brand_owner_name
  , sku_listed
  , sku_mature
  , sku_probation
  , sku_new
  , zero_movers
  , la_zero_movers
  , slow_movers
  , la_slow_movers
  , efficient_movers
  , new_zero_movers
  , new_slow_movers
  , new_efficient_movers
  , sold_items
  , gpv_eur
  --#Since new Listed SKU Definition for countries with Status Hierarchy started on 2023-07-01, this case when defines which Book Value to take
  , ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency
  , ROUND(SAFE_DIVIDE(new_efficient_movers,sku_listed),2) AS perc_newness
  ,ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency
  , ROUND((SAFE_DIVIDE(new_efficient_movers,sku_listed)*gpv_eur),2) AS weight_newness

FROM principal_wi_quarterly as wim
LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

GROUP BY ALL),

--------------------------------------------------------------------------------------------------- BRAND OWNER LEVEL ------------------------------------------------------------------------

brand_owner_monthly AS (SELECT
  s.management_entity
  , wim.global_entity_id
  , s.country_code
  , division_type
  , s.common_name as country_name
  , s.region
  , period as time_granularity
  , CAST(month AS STRING) as time_period
  , CAST(wim.supplier_id AS STRING) AS supplier_id
  , wim.supplier_name
  , wim.brand_owner_name
  , sku_listed
  , sku_mature
  , sku_probation
  , sku_new
  , zero_movers
  , la_zero_movers
  , slow_movers
  , la_slow_movers
  , efficient_movers
  , new_zero_movers
  , new_slow_movers
  , new_efficient_movers
  , sold_items
  , gpv_eur
  --#Since new Listed SKU Definition for countries with Status Hierarchy started on 2023-07-01, this case when defines which Book Value to take
  , ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency
  , ROUND(SAFE_DIVIDE(new_efficient_movers,sku_listed),2) AS perc_newness
  ,ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency
  , ROUND((SAFE_DIVIDE(new_efficient_movers,sku_listed)*gpv_eur),2) AS weight_newness

FROM brand_owner_wi_monthly as wim
LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

GROUP BY ALL),

brand_owner_quarterly AS (SELECT
  s.management_entity
  , wim.global_entity_id
  , s.country_code
  , division_type
  , s.common_name as country_name
  , s.region
  , period as time_granularity
  , CAST(quarter_year AS STRING) as time_period
  , wim.supplier_id
  , wim.supplier_name
  , brand_owner_name
  , sku_listed
  , sku_mature
  , sku_probation
  , sku_new
  , zero_movers
  , la_zero_movers
  , slow_movers
  , la_slow_movers
  , efficient_movers
  , new_zero_movers
  , new_slow_movers
  , new_efficient_movers
  , sold_items
  , gpv_eur
  --#Since new Listed SKU Definition for countries with Status Hierarchy started on 2023-07-01, this case when defines which Book Value to take
  , ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency
  , ROUND(SAFE_DIVIDE(new_efficient_movers,sku_listed),2) AS perc_newness
  ,ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency
  , ROUND((SAFE_DIVIDE(new_efficient_movers,sku_listed)*gpv_eur),2) AS weight_newness

FROM brand_owner_wi_quarterly as wim
LEFT JOIN `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
)

SELECT
'supplier' as level,
*,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING))
    ) AS unique_code
FROM
division_monthly

UNION ALL

SELECT
'supplier' as level,
*,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING))

    ) AS unique_code
FROM
division_quarterly

UNION ALL

SELECT
'supplier' as level,
*,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING))
    ) AS unique_code
FROM
principal_monthly

UNION ALL

SELECT
'supplier' as level,
*,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING))

    ) AS unique_code
FROM
principal_quarterly

UNION ALL

SELECT
'supplier' as level,
*,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING))
    ) AS unique_code
FROM
brand_owner_monthly

UNION ALL

SELECT
'supplier' as level,
*,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING))

    ) AS unique_code
FROM
brand_owner_quarterly

GROUP BY ALL
