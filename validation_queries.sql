-- ============================================================
-- VALIDACIÓN DE TABLAS SPS - Ejecución hasta financial_metrics_prev_year
-- ============================================================
-- Ejecuta esto en BigQuery después de cada tabla para validar data

-- 1. SUPPLIER_HIERARCHY - Verificar estructura base
SELECT 'supplier_hierarchy' AS tabla, COUNT(*) AS row_count, COUNT(DISTINCT global_entity_id) AS entities
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_hierarchy`;

-- 2. PRODUCT - Verificar cobertura por país/entity
SELECT 'product' AS tabla,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entities,
  COUNT(DISTINCT country_code) AS countries,
  COUNT(DISTINCT sku_id) AS skus,
  COUNT(DISTINCT CAST(supplier_id AS STRING)) AS suppliers
FROM `dh-darkstores-live.csm_automated_tables.sps_product`;

-- 3. CUSTOMER_ORDERS - Base de datos transaccionales
SELECT 'customer_orders' AS tabla,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entities,
  COUNT(DISTINCT country_code) AS countries,
  MIN(order_date) AS fecha_min,
  MAX(order_date) AS fecha_max,
  COUNT(DISTINCT CAST(supplier_id AS STRING)) AS suppliers
FROM `dh-darkstores-live.csm_automated_tables.sps_customer_order`;

-- 4. LINE_REBATE_METRICS_MONTH - Primeras métricas
SELECT 'line_rebate_metrics_month' AS tabla,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entities,
  COUNT(DISTINCT country_code) AS countries,
  COUNT(DISTINCT month) AS months,
  ROUND(AVG(sku_rebate), 2) AS avg_rebate,
  ROUND(SUM(sku_rebate), 2) AS total_rebate
FROM `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics_month`;

-- 5. FINANCIAL_METRICS_MONTH - Verificar montos
SELECT 'financial_metrics_month' AS tabla,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entities,
  COUNT(DISTINCT country_code) AS countries,
  COUNT(DISTINCT month) AS months,
  ROUND(SUM(amt_total_price_paid_net_eur), 2) AS total_sales_eur,
  ROUND(SUM(COGS_eur), 2) AS total_cogs_eur,
  COUNT(DISTINCT CAST(supplier_id AS STRING)) AS suppliers
FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`;

-- 6. FINANCIAL_METRICS_PREV_YEAR - Lookback (año pasado comparativo)
SELECT 'financial_metrics_prev_year' AS tabla,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entities,
  ROUND(SUM(Net_Sales_eur_LY), 2) AS total_ly_sales,
  COUNT(DISTINCT join_time_period) AS time_periods,
  COUNT(DISTINCT division_type) AS division_types
FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_prev_year`;

-- ============================================================
-- VALIDACIONES POR PAÍS (ver si están todos)
-- ============================================================

-- Cobertura en PRODUCT
SELECT
  global_entity_id,
  country_code,
  COUNT(*) AS sku_count,
  COUNT(DISTINCT CAST(supplier_id AS STRING)) AS suppliers
FROM `dh-darkstores-live.csm_automated_tables.sps_product`
GROUP BY global_entity_id, country_code
ORDER BY global_entity_id;

-- Cobertura en CUSTOMER_ORDERS
SELECT
  global_entity_id,
  country_code,
  COUNT(DISTINCT CAST(supplier_id AS STRING)) AS suppliers,
  COUNT(*) AS orders,
  COUNT(DISTINCT analytical_customer_id) AS unique_customers
FROM `dh-darkstores-live.csm_automated_tables.sps_customer_order`
GROUP BY global_entity_id, country_code
ORDER BY global_entity_id;

-- ============================================================
-- VALIDACIÓN DE PARÁMETROS: ¿Se aplicaron correctamente?
-- ============================================================

-- Debería ver TODOS los 20 países en product si param_global_entity_id funciona
SELECT
  COUNT(DISTINCT country_code) AS countries,
  STRING_AGG(DISTINCT country_code ORDER BY country_code) AS country_list
FROM `dh-darkstores-live.csm_automated_tables.sps_product`;

-- Validar range de fechas en customer_orders
SELECT
  MIN(order_date) AS min_date,
  MAX(order_date) AS max_date,
  DATE_DIFF(DATE(MAX(order_date)), DATE(MIN(order_date)), DAY) AS days_span
FROM `dh-darkstores-live.csm_automated_tables.sps_customer_order`;

-- ============================================================
-- SANITY CHECKS: Detectar anomalías
-- ============================================================

-- Line Rebate - Valores Negativos?
SELECT
  COUNT(*) AS negative_rebate_rows,
  MIN(sku_rebate) AS min_rebate,
  MAX(sku_rebate) AS max_rebate
FROM `dh-darkstores-live.csm_automated_tables.sps_line_rebate_metrics_month`
WHERE sku_rebate < 0;

-- Financial Metrics - Montos Negativos?
SELECT
  COUNT(*) AS negative_sales_rows,
  MIN(amt_total_price_paid_net_eur) AS min_sales,
  MAX(amt_total_price_paid_net_eur) AS max_sales
FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`
WHERE amt_total_price_paid_net_eur < 0;

-- Comparativa Prev Year vs Month (proporciones lógicas)
SELECT
  (SELECT COUNT(*) FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`) AS month_rows,
  (SELECT COUNT(*) FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_prev_year`) AS prev_year_rows,
  ROUND((SELECT COUNT(*) FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_prev_year`) /
         (SELECT COUNT(*) FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`), 2) AS ratio;

-- ============================================================
-- DETALLES: Distribución por mes en financial_metrics_month
-- ============================================================
SELECT
  month,
  COUNT(*) AS rows,
  COUNT(DISTINCT global_entity_id) AS entities,
  COUNT(DISTINCT CAST(supplier_id AS STRING)) AS suppliers,
  ROUND(SUM(amt_total_price_paid_net_eur), 2) AS total_sales_eur
FROM `dh-darkstores-live.csm_automated_tables.sps_financial_metrics_month`
GROUP BY month
ORDER BY month DESC;
