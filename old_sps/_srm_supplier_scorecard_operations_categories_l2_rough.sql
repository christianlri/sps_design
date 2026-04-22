CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_scorecard_operations_categories_l2_rough` AS

WITH

 products_base AS (

    SELECT
    global_entity_id, sku_id,supplier_id, brand_owner_name, level_one,level_two

    FROM
    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    WHERE
    o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND month BETWEEN '2023-07-01' AND CURRENT_DATE()

    GROUP BY ALL

  ),

  ops_orders AS (

  SELECT
  r.*, 
  h.principal_supplier_id,
  h.parent_name as parent_name,
  pb.brand_owner_name,
  pb.level_one,
  pb.level_two
  
  
  FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` r

  INNER JOIN
    products_base as pb

    on 
    r.global_entity_id = pb.global_entity_id
    AND r.sku_id = pb.sku_id

   LEFT JOIN
  `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` as h

  ON
  r.global_entity_id = h.global_entity_id
  AND
  CAST(r.supplier_id AS STRING) = CAST(h.supplier_id AS STRING)


  WHERE
  r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
  AND r.combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()


  GROUP BY ALL
  ),

-- calculating the rejected_purchase_orders 


--------------------------------------------------------------------------------------------------- DIVISION LEVEL ------------------------------------------------------------------------

division_supplier_level_rejected_purchase_orders_monthly AS (

SELECT
    'supplier' as level,
    op.global_entity_id,
    op.country_code,
    'division' as division_type,
    op.region,
    created_by,
    op.supplier_id,
    op.supplier_name  as supplier_name, 
    CAST(NULL AS STRING) AS brand_owner_name,
    op.level_one,
    op.level_two,
    'Monthly' as time_granularity,
    CAST(DATE_TRUNC(combined_date, MONTH) AS STRING) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM ops_orders as op

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()

GROUP BY ALL
),

division_supplier_level_rejected_purchase_orders_quarterly AS (

SELECT
    'supplier' as level,
    op.global_entity_id,
    op.country_code,
    'division' as division_type,
    op.region,
    created_by,
    op.supplier_id,
    op.supplier_name  as supplier_name, 
    CAST(NULL AS STRING) AS brand_owner_name,
    op.level_one,
    op.level_two,
    'Quarterly' as time_granularity,
    CONCAT('Q', EXTRACT(QUARTER FROM combined_date), '-', EXTRACT(YEAR FROM combined_date)) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM ops_orders as op

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()

GROUP BY ALL
),

--------------------------------------------------------------------------------------------------- PRINCIPAL LEVEL ------------------------------------------------------------------------


principal_supplier_level_rejected_purchase_orders_monthly AS (

SELECT
    'supplier' as level,
    op.global_entity_id,
    op.country_code,
    'principal' as division_type,
    op.region,
    created_by,
    op.principal_supplier_id as supplier_id,
    op.parent_name  as supplier_name, 
    CAST(NULL AS STRING) AS brand_owner_name,
    op.level_one,
    op.level_two,
    'Monthly' as time_granularity,
    CAST(DATE_TRUNC(combined_date, MONTH) AS STRING) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM ops_orders as op

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()

GROUP BY ALL
),

principal_supplier_level_rejected_purchase_orders_quarterly AS (

SELECT
    'supplier' as level,
    op.global_entity_id,
    op.country_code,
    'principal' as division_type,
    op.region,
    created_by,
    op.principal_supplier_id as supplier_id,
    op.parent_name  as supplier_name, 
    CAST(NULL AS STRING) AS brand_owner_name,
    op.level_one,
    op.level_two,
    'Quarterly' as time_granularity,
    CONCAT('Q', EXTRACT(QUARTER FROM combined_date), '-', EXTRACT(YEAR FROM combined_date)) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM ops_orders as op

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()

GROUP BY ALL
),

--------------------------------------------------------------------------------------------------- brand_owner LEVEL ------------------------------------------------------------------------


brand_owner_level_rejected_purchase_orders_monthly AS (

SELECT
    'supplier' as level,
    op.global_entity_id,
    op.country_code,
    'brand_owner' as division_type,
    op.region,
    created_by,
    CAST(NULL AS STRING) AS supplier_id,
    CAST(NULL AS STRING) AS supplier_name, 
    op.brand_owner_name,
    op.level_one,
    op.level_two,
    'Monthly' as time_granularity,
    CAST(DATE_TRUNC(combined_date, MONTH) AS STRING) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM ops_orders as op

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()

GROUP BY ALL
),

brand_owner_level_rejected_purchase_orders_quarterly AS (

SELECT
    'supplier' as level,
    op.global_entity_id,
    op.country_code,
    'brand_owner' as division_type,
    op.region,
    created_by,
    CAST(NULL AS STRING) AS supplier_id,
    CAST(NULL AS STRING) AS supplier_name, 
    op.brand_owner_name,
    op.level_one,
    op.level_two,
    'Quarterly' as time_granularity,
    CONCAT('Q', EXTRACT(QUARTER FROM combined_date), '-', EXTRACT(YEAR FROM combined_date)) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM ops_orders as op

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-7-01' AND CURRENT_DATE()

GROUP BY ALL
),



FINAL AS
(

SELECT
*,
CONCAT(
LOWER('supplier'), '-', 
LOWER(global_entity_id), '-', 
LOWER(country_code), '-', 
LOWER(region), '-', 
LOWER(CAST(time_granularity AS STRING)), '-', 
LOWER(CAST(time_period AS STRING)), '-', 
LOWER(CAST(supplier_id AS STRING))

) AS unique_code
FROM
division_supplier_level_rejected_purchase_orders_monthly

UNION ALL

SELECT
*,
CONCAT(
LOWER('supplier'), '-', 
LOWER(global_entity_id), '-', 
LOWER(country_code), '-', 
LOWER(region), '-', 
LOWER(CAST(time_granularity AS STRING)), '-', 
LOWER(CAST(time_period AS STRING)), '-', 
LOWER(CAST(supplier_id AS STRING))

) AS unique_code

FROM
division_supplier_level_rejected_purchase_orders_quarterly

UNION ALL

SELECT
*,
CONCAT(
LOWER('supplier'), '-', 
LOWER(global_entity_id), '-', 
LOWER(country_code), '-', 
LOWER(region), '-', 
LOWER(CAST(time_granularity AS STRING)), '-', 
LOWER(CAST(time_period AS STRING)), '-', 
LOWER(CAST(supplier_id AS STRING))

) AS unique_code

FROM
principal_supplier_level_rejected_purchase_orders_monthly

UNION ALL

SELECT
*,
CONCAT(
LOWER('supplier'), '-', 
LOWER(global_entity_id), '-', 
LOWER(country_code), '-', 
LOWER(region), '-', 
LOWER(CAST(time_granularity AS STRING)), '-', 
LOWER(CAST(time_period AS STRING)), '-', 
LOWER(CAST(supplier_id AS STRING))

) AS unique_code

FROM
principal_supplier_level_rejected_purchase_orders_quarterly

UNION ALL

SELECT
*,
CONCAT(
LOWER('supplier'), '-', 
LOWER(global_entity_id), '-', 
LOWER(country_code), '-', 
LOWER(region), '-', 
LOWER(CAST(time_granularity AS STRING)), '-', 
LOWER(CAST(time_period AS STRING)), '-', 
LOWER(CAST(supplier_id AS STRING))

) AS unique_code

FROM
brand_owner_level_rejected_purchase_orders_monthly

UNION ALL

SELECT
*,
CONCAT(
LOWER('supplier'), '-', 
LOWER(global_entity_id), '-', 
LOWER(country_code), '-', 
LOWER(region), '-', 
LOWER(CAST(time_granularity AS STRING)), '-', 
LOWER(CAST(time_period AS STRING)), '-', 
LOWER(CAST(supplier_id AS STRING))

) AS unique_code

FROM
brand_owner_level_rejected_purchase_orders_quarterly
)


SELECT
*
FROM
FINAL

GROUP BY ALL
