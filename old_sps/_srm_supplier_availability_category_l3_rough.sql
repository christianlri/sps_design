CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_availability_category_l3_rough` AS


WITH 

 products AS (

    SELECT
    global_entity_id, sku_id,supplier_id, brand_owner_name

    FROM
    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    WHERE
    o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND month BETWEEN '2023-07-01' AND CURRENT_DATE()

    GROUP BY ALL

  ),

recent_supplier AS (
  SELECT
    warehouse_id,
    m.sku_id,
    m.supplier_id,
    supplier_name,
    p.brand_owner_name,
    m.global_entity_id,
    month,
    ROW_NUMBER() OVER (
      PARTITION BY warehouse_id, m.sku_id, m.global_entity_id, m.supplier_id
      ORDER BY month DESC
    ) AS rn
  FROM
    `fulfillment-dwh-production.cl_dmart.sku_supplier_id_mapping` as m

    LEFT JOIN
  products AS p

  ON
  m.global_entity_id = p.global_entity_id
  AND
  m.sku_id = p.sku_id
  AND
  CAST(m.supplier_id AS STRING) = p.supplier_id

  WHERE 
  m.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
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
final AS (
  SELECT
    a.global_entity_id,
    a.management_entity,
    DATE_TRUNC(a.period_day, MONTH) as MONTH,
    CONCAT('Q', EXTRACT(QUARTER FROM a.period_day), '-', EXTRACT(YEAR FROM a.period_day)) AS quarter_year,
    fs.supplier_id,
    fs.supplier_name,
    fs.brand_owner_name,
    a.level_one_category as level_one,
    a.level_two_category as level_two,
    a.level_three_category as level_three,
    SUM(available_events_weightage * sales_forecast_quantity) availability_num, 
    NULLIF(SUM(total_events_weightage*sales_forecast_quantity),0) availability_den

  FROM `fulfillment-dwh-production.rl_dmart.tableau_weighted_availability_report` a

  LEFT JOIN
    filtered_supplier AS fs
  ON
    a.global_entity_id = fs.global_entity_id
    AND a.sku = fs.sku_id
    AND a.warehouse_id = fs.warehouse_id
  
  WHERE 
  a.period_day BETWEEN '2023-7-01' AND CURRENT_DATE()
  AND 
  a.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

GROUP BY ALL
),

availability AS (
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

division_monthly AS (

SELECT
a.global_entity_id,
'division' division_type,
'Monthly' as time_granularity,
CAST(a.month AS STRING) as time_period,
a.supplier_id,
a.supplier_name,
a.level_one,
a.level_two,
a.level_three,
CAST(NULL AS STRING) as brand_owner_name,
SUM(a.availability_num) availability_num,
SUM(a.availability_den) availability_den,
ROUND(SAFE_DIVIDE(SUM(a.availability_num), SUM(a.availability_den)),3) AS availability

FROM
availability as a

GROUP BY ALL
)
,

division_quarterly AS (

SELECT
a.global_entity_id,
'division' division_type,
'Quarterly' as time_granularity,
a.quarter_year as time_period,
a.supplier_id,
a.supplier_name,
a.level_one,
a.level_two,
a.level_three,
CAST(NULL AS STRING) as brand_owner_name,
SUM(a.availability_num) availability_num,
SUM(a.availability_den) availability_den,
ROUND(SAFE_DIVIDE(SUM(a.availability_num), SUM(a.availability_den)),3) AS availability

FROM
availability as a

GROUP BY ALL
),

--------------------------------------------------------------------------------------------------- PARENT LEVEL ------------------------------------------------------------------------

principal_monthly AS (

SELECT
a.global_entity_id,
'principal' division_type,
'Monthly' as time_granularity,
CAST(a.month AS STRING) AS time_period,
a.principal_supplier_id as supplier_id,
a.parent_name as supplier_name,
a.level_one,
a.level_two,
a.level_three,
CAST(NULL AS STRING) as brand_owner_name,
SUM(a.availability_num) availability_num,
SUM(a.availability_den) availability_den,
ROUND(SAFE_DIVIDE(SUM(a.availability_num), SUM(a.availability_den)),3) AS availability

FROM
availability as a

GROUP BY ALL
)
,

principal_quarterly AS (

SELECT
a.global_entity_id,
'principal' division_type,
'Quarterly' as time_granularity,
a.quarter_year AS time_period,
a.principal_supplier_id as supplier_id,
a.parent_name as supplier_name,
a.level_one,
a.level_two,
a.level_three,
CAST(NULL AS STRING) as brand_owner_name,
SUM(a.availability_num) availability_num,
SUM(a.availability_den) availability_den,
ROUND(SAFE_DIVIDE(SUM(a.availability_num), SUM(a.availability_den)),3) AS availability

FROM
availability as a

GROUP BY ALL
),

--------------------------------------------------------------------------------------------------- BRAND OWNER LEVEL ------------------------------------------------------------------------

brand_owner_monthly AS (

SELECT
a.global_entity_id,
'brand_owner' division_type,
'Monthly' as time_granularity,
CAST(a.month AS STRING) AS time_period,
CAST(NULL AS STRING) as supplier_id,
CAST(NULL AS STRING) as supplier_name,
a.level_one,
a.level_two,
a.level_three,
brand_owner_name,
SUM(a.availability_num) availability_num,
SUM(a.availability_den) availability_den,
ROUND(SAFE_DIVIDE(SUM(a.availability_num), SUM(a.availability_den)),3) AS availability

FROM
availability as a

GROUP BY ALL
)
,

brand_owner_quarterly AS (

SELECT
a.global_entity_id,
'brand_owner' division_type,
'Quarterly' as time_granularity,
a.quarter_year AS time_period,
CAST(NULL AS STRING) as supplier_id,
CAST(NULL AS STRING) as supplier_name,
a.level_one,
a.level_two,
a.level_three,
brand_owner_name,
SUM(a.availability_num) availability_num,
SUM(a.availability_den) availability_den,
ROUND(SAFE_DIVIDE(SUM(a.availability_num), SUM(a.availability_den)),3) AS availability

FROM
availability as a

GROUP BY ALL
)

SELECT
'supplier' as level,
*,

FROM
division_monthly

UNION ALL

SELECT
'supplier' as level,
*
FROM
division_quarterly

UNION ALL

SELECT
'supplier' as level,
*
FROM
principal_monthly

UNION ALL

SELECT
'supplier' as level,
*
FROM
principal_quarterly

UNION ALL

SELECT
'supplier' as level,
*
FROM
brand_owner_monthly

UNION ALL

SELECT
'supplier' as level,
*
FROM
brand_owner_quarterly

GROUP BY ALL
