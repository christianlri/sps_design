-- This table extracts and maintains the metrics mapping required for generating Supplier Scorecards.
-- SPS Execution: Final Position
-- DML SCRIPT: SPS Refact Incremental Refresh for {{ params.project_id }}.{{ params.dataset.cl }}.sps_score_tableau

CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.cl }}.sps_score_tableau`
CLUSTER BY
   global_entity_id
AS
-- NEW (architectural refactor): all_keys UNION pattern
-- build the complete key space
-- by collecting DISTINCT key combinations from ALL source tables. This ensures coverage of
-- all unique (global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity)
-- combinations that exist in any of the 8 source tables, even if not in sps_purchase_order.
WITH all_keys AS (
  SELECT DISTINCT * FROM (
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_days_payable`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_line_rebate_metrics`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_listed_sku`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_shrinkage`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_delivery_costs`
    UNION ALL
    SELECT global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_purchase_order`
  )
),
-- ── Supplier names mapping: extract distinct supplier_id → supplier_name from sps_product ─
sps_product_clean AS (
  SELECT DISTINCT
    CAST(supplier_id AS STRING) as supplier_id,
    supplier_name,
    global_entity_id
  FROM `{{ params.project_id }}.{{ params.dataset.cl }}.sps_product`
  WHERE supplier_id IS NOT NULL
)
SELECT
  o.*,
  -- Supplier name: join brand_sup with sps_product for division/principal suppliers, else fallback to brand_sup
  CASE
    WHEN o.division_type IN ('division', 'principal') THEN COALESCE(p.supplier_name, o.brand_sup)
    ELSE o.brand_sup
  END AS supplier_name,
  -- Purchase order metrics (from sps_purchase_order)
  po.on_time_orders,
  po.total_received_qty_per_po_order,
  po.total_demanded_qty_per_po_order,
  po.total_cancelled_po_orders,
  po.total_non_cancelled__po_orders,
  po.fill_rate,
  po.otd,
  po.supplier_non_fulfilled_order_qty,
  -- NEW: Debug fields from purchase_order
  po.total_po_orders,
  po.total_compliant_po_orders,
  po.total_received_qty_ALL,
  po.total_demanded_qty_ALL,
  -- Financial metrics (from sps_financial_metrics)
  sfm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup),
  -- Line rebate metrics (from sps_line_rebate_metrics)
  slrm.* EXCEPT (global_entity_id, time_period, time_granularity, division_type, supplier_level, entity_key, brand_sup, net_purchase, calc_net_delivered, calc_net_return),
  -- NEW: Ingredientes from sps_line_rebate_metrics
  slrm.calc_net_delivered,
  slrm.calc_net_return,
  -- Price index metrics (from sps_price_index)
  p.median_price_index,
  -- NEW: Ingredientes from sps_price_index
  p.price_index_numerator,
  p.price_index_weight,
  -- Days payable metrics (from sps_days_payable)
  dpo.payment_days,
  dpo.doh,
  dpo.dpo,
  -- NEW: Ingredientes from sps_days_payable
  dpo.stock_value_eur,
  dpo.cogs_monthly_eur,
  dpo.days_in_month,
  dpo.days_in_quarter,
  -- Efficiency metrics (from sps_efficiency)
  -- Portfolio cluster: SKU counts and availability ingredients
  se.sku_listed,
  se.sku_mature,
  se.sku_probation,
  se.sku_new,
  se.efficient_movers,
  se.new_zero_movers,
  se.new_slow_movers,
  se.new_efficient_movers,
  se.sold_items,
  se.gpv_eur,
  -- NEW: Availability ingredients (Tableau: SUM(numerator) / SUM(denom))
  se.numerator_new_avail,
  se.denom_new_avail,
  -- NEW: Efficiency weighted ingredient (AQS v7 methodology, Tableau: SUM(weight_efficiency) / SUM(gpv_eur))
  se.weight_efficiency,
  -- Listed SKU metrics (from sps_listed_sku)
  listed.listed_skus,
  -- Shrinkage metrics (from sps_shrinkage)
  -- NEW: Renamed fields for clarity (spoilage_value → spoilage_value_eur, etc.)
  shrink.spoilage_value_eur,
  shrink.spoilage_value_lc,
  shrink.retail_revenue_eur,
  shrink.retail_revenue_lc,
  shrink.spoilage_rate,
  -- Delivery costs (from sps_delivery_costs)
  deliv.delivery_cost_eur,
  deliv.delivery_cost_local
FROM all_keys o
-- Supplier names (from sps_product, filtered to division/principal only)
LEFT JOIN sps_product_clean p
  ON o.global_entity_id = p.global_entity_id
  AND CAST(o.brand_sup AS STRING) = p.supplier_id
  AND o.division_type IN ('division', 'principal')
-- Purchase order (all keys start here, but now joined from all_keys instead)
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_purchase_order` AS po
  ON o.global_entity_id = po.global_entity_id AND o.time_period = po.time_period AND o.time_granularity = po.time_granularity AND o.division_type = po.division_type AND o.supplier_level = po.supplier_level AND o.entity_key = po.entity_key AND o.brand_sup = po.brand_sup
-- Financial metrics
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_financial_metrics` AS sfm
  ON o.global_entity_id = sfm.global_entity_id AND o.time_period = sfm.time_period AND o.time_granularity = sfm.time_granularity AND o.division_type = sfm.division_type AND o.supplier_level = sfm.supplier_level AND o.entity_key = sfm.entity_key AND o.brand_sup = sfm.brand_sup
-- Line rebate metrics
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_line_rebate_metrics` AS slrm
  ON o.global_entity_id = slrm.global_entity_id AND o.time_period = slrm.time_period AND o.time_granularity = slrm.time_granularity AND o.division_type = slrm.division_type AND o.supplier_level = slrm.supplier_level AND o.entity_key = slrm.entity_key AND o.brand_sup = slrm.brand_sup
-- Price index
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_price_index` AS p
  ON o.global_entity_id = p.global_entity_id AND o.time_period = p.time_period AND o.time_granularity = p.time_granularity AND o.division_type = p.division_type AND o.supplier_level = p.supplier_level AND o.entity_key = p.entity_key AND o.brand_sup = p.brand_sup
-- Days payable
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_days_payable` AS dpo
  ON o.global_entity_id = dpo.global_entity_id AND o.time_period = dpo.time_period AND o.time_granularity = dpo.time_granularity AND o.division_type = dpo.division_type AND o.supplier_level = dpo.supplier_level AND o.entity_key = dpo.entity_key AND o.brand_sup = dpo.brand_sup
-- Efficiency
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_efficiency` AS se
  ON o.global_entity_id = se.global_entity_id AND o.time_period = se.time_period AND o.time_granularity = se.time_granularity AND o.division_type = se.division_type AND o.supplier_level = se.supplier_level AND o.entity_key = se.entity_key AND o.brand_sup = se.brand_sup
-- Listed SKU
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_listed_sku` AS listed
  ON o.global_entity_id = listed.global_entity_id AND o.time_period = listed.time_period AND o.time_granularity = listed.time_granularity AND o.division_type = listed.division_type AND o.supplier_level = listed.supplier_level AND o.entity_key = listed.entity_key AND o.brand_sup = listed.brand_sup
-- Shrinkage
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_shrinkage` AS shrink
  ON o.global_entity_id = shrink.global_entity_id AND o.time_period = shrink.time_period AND o.time_granularity = shrink.time_granularity AND o.division_type = shrink.division_type AND o.supplier_level = shrink.supplier_level AND o.entity_key = shrink.entity_key AND o.brand_sup = shrink.brand_sup
-- Delivery costs
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.cl }}.sps_delivery_costs` AS deliv
  ON o.global_entity_id = deliv.global_entity_id AND o.time_period = deliv.time_period AND o.time_granularity = deliv.time_granularity AND o.division_type = deliv.division_type AND o.supplier_level = deliv.supplier_level AND o.entity_key = deliv.entity_key AND o.brand_sup = deliv.brand_sup
