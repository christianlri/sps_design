-- ============================================================
-- SUPPLIER NAME INJECTION VALIDATION QUERIES
-- Purpose: Validate that supplier_name was correctly injected
--          into sps_supplier_master from sps_product_clean
-- ============================================================

-- ── QUERY 1: Sample supplier names by entity (Top 10 by Net_Sales) ──────────
-- Esperado: supplier_name es legible (no numeric entity_key)
SELECT
  global_entity_id,
  time_period,
  entity_key,
  supplier_name,
  division_type,
  Net_Sales_lc,
  total_score,
  CASE
    WHEN supplier_name = entity_key THEN 'FALLBACK'
    ELSE 'MATCHED'
  END AS injection_status
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
WHERE time_period = '202604'
  AND division_type IN ('division', 'principal')
  AND Net_Sales_lc > 5000
ORDER BY global_entity_id, Net_Sales_lc DESC
LIMIT 50;

-- ── QUERY 2: Injection match rate by entity ──────────────────────────────
-- Esperado: matched >> fallback (~90% match rate)
SELECT
  global_entity_id,
  division_type,
  COUNT(*) AS total_suppliers,
  COUNT(CASE WHEN supplier_name != entity_key THEN 1 END) AS matched_count,
  COUNT(CASE WHEN supplier_name = entity_key THEN 1 END) AS fallback_count,
  ROUND(100.0 * COUNT(CASE WHEN supplier_name != entity_key THEN 1 END)
    / NULLIF(COUNT(*), 0), 2) AS match_rate_pct
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
WHERE time_period = '202604'
  AND division_type IN ('division', 'principal')
GROUP BY global_entity_id, division_type
ORDER BY global_entity_id, match_rate_pct DESC;

-- ── QUERY 3: Null validation (must be 0) ─────────────────────────────────
-- Esperado: 0 null counts (COALESCE fallback garantiza esto)
SELECT
  time_period,
  COUNT(*) AS total_rows,
  COUNT(CASE WHEN supplier_name IS NULL THEN 1 END) AS null_supplier_name_count,
  COUNT(CASE WHEN entity_key IS NULL THEN 1 END) AS null_entity_key_count,
  CASE
    WHEN COUNT(CASE WHEN supplier_name IS NULL THEN 1 END) = 0 THEN '✓ PASS'
    ELSE '✗ FAIL - NULLs detected'
  END AS null_validation
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
WHERE time_period = '202604'
GROUP BY time_period;

-- ── QUERY 4: Cross-table validation (sps_supplier_master vs sps_supplier_segmentation) ──
-- Esperado: supplier_name matches in both tables (same source)
SELECT
  m.global_entity_id,
  m.time_period,
  m.entity_key,
  m.supplier_name AS master_supplier_name,
  COALESCE(s.supplier_name, 'N/A') AS segmentation_supplier_name,
  CASE
    WHEN m.supplier_name = s.supplier_name THEN 'Match'
    WHEN s.supplier_name IS NULL THEN 'Segmentation NULL'
    ELSE 'Mismatch'
  END AS validation_result
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master` m
LEFT JOIN `dh-darkstores-live.csm_automated_tables.sps_supplier_segmentation` s
  ON  m.global_entity_id = s.global_entity_id
  AND m.time_period      = s.time_period
  AND m.entity_key       = s.entity_key
  AND m.division_type    = s.division_type
WHERE m.time_period = '202604'
  AND m.division_type IN ('division', 'principal')
ORDER BY m.global_entity_id, validation_result
LIMIT 50;

-- ── QUERY 5: Injection performance check ─────────────────────────────────
-- Esperado: match_rate alta, fallback bajo, no anomalías
SELECT
  global_entity_id,
  COUNT(*) AS supplier_count,
  ROUND(AVG(Net_Sales_lc), 2) AS avg_sales_lc,
  COUNT(DISTINCT supplier_name) AS unique_supplier_names,
  COUNT(DISTINCT entity_key) AS unique_entity_keys,
  CASE
    WHEN COUNT(DISTINCT supplier_name) > 0.8 * COUNT(DISTINCT entity_key) THEN '✓ Healthy diversity'
    ELSE '⚠ Warning: low name diversity'
  END AS diversity_check
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
WHERE time_period = '202604'
  AND division_type IN ('division', 'principal')
GROUP BY global_entity_id
ORDER BY global_entity_id;

-- ── QUERY 6: Ready for Tableau — sample dashboard data ──────────────────
-- Esperado: Datos listos para usar en Tableau con nombres legibles
SELECT
  global_entity_id,
  time_period,
  supplier_name,
  entity_key,
  Net_Sales_lc,
  total_score,
  operations_score,
  commercial_score,
  ratio_otd,
  ratio_fill_rate,
  ratio_efficiency,
  segment_lc,
  importance_score_lc,
  productivity_score_lc
FROM `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
WHERE time_period = '202604'
  AND global_entity_id = 'FP_SG'
  AND division_type IN ('division', 'principal')
  AND Net_Sales_lc > 10000
ORDER BY total_score DESC
LIMIT 25;
