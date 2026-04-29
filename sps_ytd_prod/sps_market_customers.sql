-- This table extracts market customers metrics required for generating Supplier Scorecards.
-- SPS Execution: Position No. 13
-- Propósito: denominador para customer_penetration en segmentación
-- Grain: global_entity_id × time_period × time_granularity
-- Monthly:  clientes únicos en el mes (sin filtro de supplier)
-- Quarterly: clientes únicos en el trimestre (sin filtro de supplier)
-- YTD: clientes únicos en el año acumulado hasta hoy
-- Nota: quarterly/YTD NO es suma de meses — es COUNT DISTINCT sobre el periodo completo
-- Un cliente que compró en oct+nov+dic = 1 cliente en Q4, no 3
-- ============================================================

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_market_customers`
AS

WITH
date_config AS (
  SELECT
    CURRENT_DATE() AS today,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS current_year,
    EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS prior_year
),

-- Base: todas las órdenes del periodo, sin filtro de supplier
-- Grain: order_id × analytical_customer_id × fecha
base AS (
  SELECT
    global_entity_id,
    analytical_customer_id,
    order_id,
    order_date,
    CAST(DATE_TRUNC(order_date, MONTH) AS STRING) AS month,
    CAST(CONCAT(
      'Q', EXTRACT(QUARTER FROM order_date),
      '-', EXTRACT(YEAR FROM order_date)
    ) AS STRING) AS quarter_year,
    EXTRACT(YEAR FROM order_date) AS ytd_year
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_customer_order`
  WHERE (
    (EXTRACT(YEAR FROM order_date) = (SELECT current_year FROM date_config)
     AND order_date <= (SELECT today FROM date_config))
    OR
    (EXTRACT(YEAR FROM order_date) = (SELECT prior_year FROM date_config))
  )
),

-- Monthly: una fila por entidad × mes
monthly AS (
  SELECT
    global_entity_id,
    month                                                AS time_period,
    'Monthly'                                            AS time_granularity,
    APPROX_COUNT_DISTINCT(analytical_customer_id)        AS total_market_customers,
    APPROX_COUNT_DISTINCT(order_id)                      AS total_market_orders
  FROM base
  GROUP BY global_entity_id, month
),

-- Quarterly: una fila por entidad × trimestre
-- COUNT DISTINCT sobre todo el quarter — no suma de meses
quarterly AS (
  SELECT
    global_entity_id,
    quarter_year                                         AS time_period,
    'Quarterly'                                          AS time_granularity,
    APPROX_COUNT_DISTINCT(analytical_customer_id)        AS total_market_customers,
    APPROX_COUNT_DISTINCT(order_id)                      AS total_market_orders
  FROM base
  GROUP BY global_entity_id, quarter_year
),

-- YTD: una fila por entidad × año acumulado
-- COUNT DISTINCT sobre todos los meses del año hasta hoy
ytd AS (
  SELECT
    global_entity_id,
    CONCAT('YTD-', CAST(ytd_year AS STRING))             AS time_period,
    'YTD'                                                AS time_granularity,
    APPROX_COUNT_DISTINCT(analytical_customer_id)        AS total_market_customers,
    APPROX_COUNT_DISTINCT(order_id)                      AS total_market_orders
  FROM base
  GROUP BY global_entity_id, ytd_year
)

SELECT * FROM monthly
UNION ALL
SELECT * FROM quarterly
UNION ALL
SELECT * FROM ytd
