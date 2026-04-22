CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_scorecard_brand_orders_rough` AS

WITH 
---------------The below orders refer to the customer orders on. a brand / supplier and brand owner level -------------------------------------------------------------------

------------------------------Division supplier level -------------------------------------------------------------------

  division_supplier_level_monthly AS
  (
  SELECT 
    'supplier' as level,
    'division' as division_type,
    a.global_entity_id,
    a.country_code,
    a.country_name,
    'Monthly' as time_granularity,
    CAST(a.month AS STRING) as time_period,
    CAST(a.supplier_id AS STRING) AS supplier_id,
    a.supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT analytical_customer_id) AS customers,
  
  FROM 
  `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as a
  WHERE
  DATE(a.order_date) BETWEEN '2023-7-1' AND CURRENT_DATE()
  AND global_entity_id in ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  GROUP BY 1,2,3,4,5,6,7,8,9
  ),

  division_supplier_level_quarterly AS
  (
  SELECT 
    'supplier' as level,
    'division' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    'Quarterly' as time_granularity,
    o.quarter_year as time_period,
    CAST(o.supplier_id AS STRING) AS supplier_id,
    o.supplier_name
  , CAST(NULL AS STRING) AS brand_owner_name
  ,COUNT(DISTINCT order_id) AS orders
  ,COUNT(DISTINCT analytical_customer_id) AS customers
  FROM 
  `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as o
  WHERE
  DATE(o.order_date) BETWEEN '2023-7-1' AND CURRENT_DATE()
  AND global_entity_id in ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  GROUP BY 1,2,3,4,5,6,7,8,9
  ),

  ------------------------------Principal supplier level -------------------------------------------------------------------

    principal_supplier_level_monthly AS
  (
  SELECT 
    'supplier' as level,
    'principal' as division_type,
    a.global_entity_id,
    a.country_code,
    a.country_name,
    'Monthly' as time_granularity,
    CAST(a.month AS STRING) as time_period,
    CAST(a.principal_supplier_id AS STRING) as supplier_id,
    a.parent_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT analytical_customer_id) AS customers,
  
  FROM 
  `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as a
  WHERE
  DATE(a.order_date) BETWEEN '2023-7-1' AND CURRENT_DATE()
  AND global_entity_id in ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  GROUP BY 1,2,3,4,5,6,7,8,9
  ),

  principal_supplier_level_quarterly AS
  (
  SELECT 
    'supplier' as level,
    'principal' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    'Quarterly' as time_granularity,
    o.quarter_year as time_period,
    CAST(o.principal_supplier_id AS STRING) as supplier_id,
    o.parent_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT analytical_customer_id) AS customers,
  FROM 
  `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as o
  WHERE
  DATE(o.order_date) BETWEEN '2023-7-1' AND CURRENT_DATE()
  AND global_entity_id in ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  GROUP BY 1,2,3,4,5,6,7,8,9
  ),

    ------------------------------Brand Owner level -------------------------------------------------------------------

    brand_owner_level_monthly AS
  (
  SELECT 
    'supplier' as level,
    'brand_owner' as division_type,
    a.global_entity_id,
    a.country_code,
    a.country_name,
    'Monthly' as time_granularity,
    CAST(a.month AS STRING) as time_period,
    CAST(NULL AS STRING) AS  supplier_id,
    CAST(NULL AS STRING) AS  supplier_name,
    brand_owner_name,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT analytical_customer_id) AS customers,
  
  FROM 
  `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as a
  WHERE
  DATE(a.order_date) BETWEEN '2023-7-1' AND CURRENT_DATE()
  AND global_entity_id in ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  ),

  brand_owner_level_quarterly AS
  (
  SELECT 
    'supplier' as level,
    'brand_owner' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    'Quarterly' as time_granularity,
    o.quarter_year as time_period,
    CAST(NULL AS STRING) AS supplier_id,
    CAST(NULL AS STRING) AS supplier_name,
    o.brand_owner_name,
    COUNT(DISTINCT order_id) AS orders,
    COUNT(DISTINCT analytical_customer_id) AS customers,
  FROM 
  `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers`  as o
  WHERE
  DATE(o.order_date) BETWEEN '2023-7-1' AND CURRENT_DATE()
  AND global_entity_id in ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  GROUP BY 1,2,3,4,5,6,7,8,9,10
  ),


  FINAL AS
  (
  SELECT
  *
  FROM
  division_supplier_level_quarterly

  UNION ALL

    SELECT
  *
  FROM
  division_supplier_level_monthly

    UNION ALL

    SELECT
  *
  FROM
  principal_supplier_level_monthly

    UNION ALL

    SELECT
  *
  FROM
  principal_supplier_level_quarterly

    UNION ALL

    SELECT
  *
  FROM
  brand_owner_level_monthly

    UNION ALL

    SELECT
  *
  FROM
  brand_owner_level_quarterly
  )

  SELECT
  *
  FROM
  FINAL

  GROUP BY ALL
