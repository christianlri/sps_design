# Technical Notes & Known Quirks

**Document Date**: April 28, 2026  
**Status**: Reference only

---

## GROUP BY Anti-Pattern: Metrics in GROUP BY Clause

### Location
- `flat_sps_supplier_scoring.sql` (lines 32-35)
- `ytd_sps_supplier_scoring.sql` (lines 32-35)

### Code
```sql
ratios AS (
  SELECT
    st.global_entity_id, st.time_period, st.time_granularity,
    st.division_type, st.supplier_level, st.entity_key, st.brand_sup,
    st.fill_rate,                    ← Metric, not dimension
    st.otd,                          ← Metric, not dimension
    SAFE_DIVIDE(SUM(...), ...) AS yoy_growth,
    ...
  FROM sps_score_tableau st
  WHERE ...
  GROUP BY
    st.global_entity_id, st.time_period, st.time_granularity,
    st.division_type, st.supplier_level, st.entity_key, st.brand_sup,
    st.fill_rate, st.otd             ← These shouldn't be here
)
```

### Issue
`fill_rate` and `otd` are pre-calculated metrics from `sps_score_tableau`, not grouping dimensions. Including them in GROUP BY is semantically incorrect.

### Why It Works Anyway
In BigQuery, `fill_rate` and `otd` come as constant values (calculated once per row from upstream table). When included in GROUP BY, they're treated as fixed dimensions, effectively a no-op. The aggregation proceeds correctly because:
1. Row already has fill_rate = 0.92 (calculated in score_tableau)
2. GROUP BY includes fill_rate = 0.92
3. All rows with same (entity_key, time_period, ..., fill_rate, otd) group together
4. SUM(Net_Sales) aggregates correctly
5. fill_rate stays as constant 0.92 in output

**Net effect**: Correct output, incorrect pattern.

### Why Not Fix?
1. **It works** — No functional impact, output is correct
2. **Production precedent** — Pattern inherited from `flat_sps_supplier_scoring.sql`, which is in production
3. **Risk of side effects** — Changing GROUP BY can have unexpected interactions with other columns or aggregations
4. **Consistency** — YTD version replicates flat version to ensure behavior parity

### Recommendation
- ✅ Leave as-is for now (consistency with production)
- 📝 Document pattern (done here)
- 🔮 Future refactor: Remove fill_rate and otd from GROUP BY (lower priority)

---

## Parameter Mapping Strategy: Why Monthly Thresholds for YTD

### Problem
When YTD rows are generated, scoring parameters (p25, p75, IQR mean) don't exist as "YTD-2026 parameters" — they're only calculated for Monthly periods.

### Solution
Map YTD rows to latest available monthly period:
```sql
params_key AS (
  SELECT global_entity_id, MAX(time_period) AS latest_monthly_period
  FROM sps_scoring_params
  GROUP BY global_entity_id
)

LEFT JOIN sps_scoring_params p ON
  p.time_period = CASE
    WHEN r.time_granularity = 'YTD' THEN pk.latest_monthly_period
    ELSE r.time_period
  END
```

### Why This Is Correct

**Percentiles reflect true market behavior when computed on monthly data.**

If you computed percentiles on YTD-accumulated data:
- Supplier with bad month 1 + great months 2-4 would average up in YTD
- Percentile distribution would be flattened (less variance)
- p25 and p75 thresholds would move, losing their meaning
- "Top 25% performers" threshold becomes meaningless for cumulative data

**For YTD scoring, we want**: "How does this supplier rank vs the market's actual monthly behavior?"

Not: "How does this supplier's YTD aggregate rank vs other YTD aggregates?"

The former question uses monthly thresholds. The latter would need YTD-specific percentiles (which we don't compute, nor should we).

### Data Consistency
- **scoring_params**: Computed on Monthly rows only
- **market_yoy**: Computed on Monthly rows only (YTD market_yoy would distort growth rates)
- **supplier_scoring**: Applies monthly params to both Monthly and YTD rows
- **supplier_master**: Inherits scoring, extends to YTD
- **supplier_segmentation**: Generates segments for both, uses monthly percentiles

This creates a "mental model" where:
- Monthly values are "raw market readings"
- YTD values are "cumulative progress readings"
- Thresholds are always "market readings" (monthly basis)

---

## Non-Additive Metrics: Why Separate Numerator/Denominator

### Pattern 1: Price Index

**Table**: `ytd_sps_price_index`

```sql
SELECT
  ...
  ROUND(SAFE_DIVIDE(SUM(median_bp_index * sku_gpv_eur), SUM(sku_gpv_eur)), 2) AS median_price_index,
  ROUND(SUM(median_bp_index * sku_gpv_eur), 4) AS price_index_numerator,
  ROUND(SUM(sku_gpv_eur), 4) AS price_index_weight,
```

**Why separate**:
- `median_price_index` is a ratio (weighted average), not a sum
- You cannot sum ratios across multiple rows and get the right answer
- In Tableau: `SUM(price_index_numerator) / SUM(price_index_weight)` correctly aggregates across suppliers/brands
- If you only sent `median_price_index`, Tableau would do `SUM(102, 103, 104) / 3 = 103` which is wrong

### Pattern 2: Shrinkage Rate

**Table**: `ytd_sps_shrinkage`

```sql
SELECT
  ...
  SUM(spoilage_value_eur) AS spoilage_value_eur,
  SUM(retail_revenue_eur) AS retail_revenue_eur,
  SAFE_DIVIDE(SUM(spoilage_value_eur), SUM(retail_revenue_eur)) AS spoilage_rate,
```

**Why separate**:
- Same reason as price index
- `spoilage_rate` = spoilage / revenue (a ratio)
- In Tableau: `SUM(spoilage_value_eur) / SUM(retail_revenue_eur)` correctly computes shrink across multiple rows

### Pattern NOT Applied: fill_rate, otd

**Table**: `ytd_sps_purchase_order`

```sql
SELECT
  ...
  ROUND(SAFE_DIVIDE(received_qty, demanded_qty), 4) AS fill_rate,
  ROUND(SAFE_DIVIDE(on_time, non_cancelled), 4) AS otd,
```

These are pre-calculated in the aggregation layer and stored as single values per row. They're treated as "constants" that come along with each entity/time_period combo, not something that needs Tableau-level re-aggregation.

**This is inconsistent with price_index and shrinkage patterns.** They should also separate numerator/denominator. But they don't in the original flat_sps code, so we replicated the pattern for consistency.

---

## Known Data Latency Issues

### Apr 2026 Rebate Data

**Status**: ⚠️ Awaiting source refresh  
**Affected**: `ytd_sps_line_rebate_metrics.ytd_*` rows  
**Impact**: YTD-2026 `total_rebate` incomplete (missing Apr component)  
**Symptom**: Score_tableau shows Jan-Mar rebate only for YTD-2026  
**Timeline**: 
- Apr 27: Financial data complete
- Apr 27-28: Rebate source not yet updated
- Apr 29-30 (expected): rb_line_rebate receives Apr 2026
- May 1+: Next aggregation run includes complete YTD rebate ✅

**Workaround**: Use Jan-Mar only for YTD comparisons until Apr data arrives.

---

## Testing Checklist for Future Modifications

When maintaining these scripts, verify:

```sql
-- 1. YTD sums match Monthly sums
SELECT 
  brand_sup,
  SUM(CASE WHEN time_granularity = 'Monthly' THEN Net_Sales_eur ELSE 0 END) AS monthly_sum,
  SUM(CASE WHEN time_granularity = 'YTD' THEN Net_Sales_eur ELSE 0 END) AS ytd_sum,
  ROUND(monthly_sum - ytd_sum, 2) AS diff
FROM ytd_sps_financial_metrics
WHERE global_entity_id = 'PY_PE'
GROUP BY brand_sup
-- Expected: diff ≈ 0 for additive metrics

-- 2. Scoring parameters exist for latest monthly
SELECT DISTINCT time_period 
FROM ytd_sps_scoring_params
WHERE global_entity_id = 'PY_PE'
ORDER BY time_period DESC LIMIT 1
-- Expected: Most recent month (e.g., '2026-04')

-- 3. YTD rows have scores via parameter mapping
SELECT COUNT(*) 
FROM ytd_sps_supplier_scoring
WHERE time_granularity = 'YTD'
  AND global_entity_id = 'PY_PE'
  AND threshold_yoy_max IS NOT NULL
-- Expected: > 0 (non-null thresholds mean mapping succeeded)

-- 4. Non-additive metrics stored correctly
SELECT COUNT(DISTINCT price_index_numerator) > 0 AS has_numerator,
       COUNT(DISTINCT price_index_weight) > 0 AS has_weight
FROM ytd_sps_price_index
WHERE time_granularity = 'YTD' AND global_entity_id = 'PY_PE'
-- Expected: both TRUE
```

---

**Status**: Reference document  
**Last Review**: April 28, 2026
