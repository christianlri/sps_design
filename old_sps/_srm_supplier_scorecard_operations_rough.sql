CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_scorecard_operations_rough` AS

--------------------------------------------------------------------------------------------------- DIVISION LEVEL ------------------------------------------------------------------------
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

division_supplier_level_rejected_purchase_orders_monthly AS (

SELECT
    'supplier' as level,
    'division' as division_type,
    op.global_entity_id,
    op.country_code,
    s.common_name as country_name,
    op.region,
    created_by,
    op.supplier_id,
    op.supplier_name  as supplier_name, 
    CAST(NULL AS STRING) AS brand_owner_name,
    'Monthly' as time_granularity,
    CAST(DATE_TRUNC(combined_date, MONTH) AS STRING) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` as op

  LEFT JOIN 
 `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)


  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND op.combined_date BETWEEN '2023-07-01' AND CURRENT_DATE()



GROUP BY ALL
ORDER BY 3,2),

division_supplier_level_rejected_purchase_orders_quarterly AS (
SELECT
    'supplier' as level,
    'division' as division_type,
    op.global_entity_id,
    op.country_code,
    s.common_name as country_name,
    op.region,
    created_by,
    op.supplier_id,
    op.supplier_name AS supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    'Quarterly' as time_granularity,
    CONCAT('Q', EXTRACT(QUARTER FROM combined_date), '-', EXTRACT(YEAR FROM combined_date)) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

    FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` as op


 LEFT JOIN 
 `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)
  

WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-07-01' AND CURRENT_DATE()

   
GROUP BY ALL
ORDER BY 3,2),

--------------------------------------------------------------------------------------------------- PRINCIPAL LEVEL ------------------------------------------------------------------------

principal_supplier_level_rejected_purchase_orders_monthly AS (

SELECT
    'supplier' as level,
    'principal' as division_type,
    op.global_entity_id,
    op.country_code,
    s.common_name as country_name,
    op.region,
    created_by,
    principal_supplier_id as supplier_id,
    h.parent_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    'Monthly' as time_granularity,
    CAST(DATE_TRUNC(combined_date, MONTH) AS STRING) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` as op

  LEFT JOIN 
 `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

  LEFT JOIN
  `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` as h

  ON
  op.global_entity_id = h.global_entity_id
  AND
  CAST(op.supplier_id AS STRING) = CAST(h.supplier_id AS STRING)

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND op.combined_date BETWEEN '2023-07-01' AND CURRENT_DATE()



GROUP BY ALL
ORDER BY 3,2),

principal_supplier_level_rejected_purchase_orders_quarterly AS (
SELECT
    'supplier' as level,
    'principal' as division_type,
    op.global_entity_id,
    op.country_code,
    s.common_name as country_name,
    op.region,
    created_by,
    principal_supplier_id as supplier_id,
    h.parent_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    'Quarterly' as time_granularity,
    CONCAT('Q', EXTRACT(QUARTER FROM combined_date), '-', EXTRACT(YEAR FROM combined_date)) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

    FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` as op


 LEFT JOIN 
 `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

 LEFT JOIN
  `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` as h

  ON
  op.global_entity_id = h.global_entity_id
  AND
  CAST(op.supplier_id AS STRING) = CAST(h.supplier_id AS STRING)

  

WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-07-01' AND CURRENT_DATE()

   
GROUP BY ALL
ORDER BY 3,2),

--------------------------------------------------------------------------------------------------- BRAND OWNER LEVEL ------------------------------------------------------------------------

brand_owner_level_rejected_purchase_orders_monthly AS (

SELECT
    'supplier' as level,
    'brand_owner' as division_type,
    op.global_entity_id,
    op.country_code,
    s.common_name as country_name,
    op.region,
    created_by,
    CAST(NULL AS STRING) as supplier_id,
    CAST(NULL AS STRING) as supplier_name,
    p.brand_owner_name,
    'Monthly' as time_granularity,
    CAST(DATE_TRUNC(combined_date, MONTH) AS STRING) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

  FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` as op

  LEFT JOIN 
 `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

  LEFT JOIN
  products as p

  ON
  op.global_entity_id = p.global_entity_id
  AND
  CAST(op.supplier_id AS STRING) = CAST(p.supplier_id AS STRING)
  AND
  op.sku_id = p.sku_id

  WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND op.combined_date BETWEEN '2023-07-01' AND CURRENT_DATE()



GROUP BY ALL
ORDER BY 3,2),

brand_owner_level_rejected_purchase_orders_quarterly AS (
SELECT
    'supplier' as level,
    'brand_owner' as division_type,
    op.global_entity_id,
    op.country_code,
    s.common_name as country_name,
    op.region,
    created_by,
    CAST(NULL AS STRING) as supplier_id,
    CAST(NULL AS STRING) as supplier_name,
    p.brand_owner_name,
    'Quarterly' as time_granularity,
    CONCAT('Q', EXTRACT(QUARTER FROM combined_date), '-', EXTRACT(YEAR FROM combined_date)) AS time_period,
    COUNT(DISTINCT(CASE WHEN is_ontime_numerator = 1 THEN po_order_id END)) AS on_time_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IN ('Supplier non fulfillment', 'AUTO CANCELLED', 'PO rejected on site (Quality issue)', 'Stock not available')  THEN po_order_id END)) AS total_cancelled_po_orders,
    COUNT(DISTINCT(CASE WHEN cancel_reason IS NULL THEN po_order_id END)) AS total_non_cancelled__po_orders,
    COALESCE(SUM(quantity_delivered_final),0) AS total_received_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) ,0) as total_demANDed_qty_per_po_order,
    COALESCE(SUM(quantity_ordered_final) - SUM(quantity_delivered_final),0) as supplier_non_fulfilled_order_qty

    FROM `fulfillment-dwh-production.rl_dmart.supplier_performance_management_report` as op


 LEFT JOIN 
 `fulfillment-dwh-production.cl_dmart.sources` AS s USING (global_entity_id)

  LEFT JOIN
  products as p

  ON
  op.global_entity_id = p.global_entity_id
  AND
  CAST(op.supplier_id AS STRING) = CAST(p.supplier_id AS STRING)
  AND
  op.sku_id = p.sku_id

WHERE
    op.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND combined_date BETWEEN '2023-07-01' AND CURRENT_DATE()

   
GROUP BY ALL
ORDER BY 3,2),


FINAL AS
(

SELECT
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
division_supplier_level_rejected_purchase_orders_monthly

UNION ALL

SELECT
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
division_supplier_level_rejected_purchase_orders_quarterly

UNION ALL

SELECT
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
principal_supplier_level_rejected_purchase_orders_monthly

UNION ALL

SELECT
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
principal_supplier_level_rejected_purchase_orders_quarterly

UNION ALL

SELECT
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
brand_owner_level_rejected_purchase_orders_monthly

UNION ALL

SELECT
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
brand_owner_level_rejected_purchase_orders_quarterly

)

SELECT
*
FROM
FINAL

GROUP BY ALL
