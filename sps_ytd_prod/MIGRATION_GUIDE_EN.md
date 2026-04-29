# SPS Pipeline Migration Guide: sps_sql_new --> sps_ytd_prod

**Author**: Christian La Rosa  
**Audience**: Ion (Analytics Engineer, production pipeline owner)  
**Date**: April 29, 2026  
**Version**: 1.0

---

## 1. Executive Summary

This document describes the migration from the current SPS production pipeline (`sps_sql_new/`) to the new production-ready pipeline (`sps_ytd_prod/`). The new pipeline introduces three major capabilities: (1) Year-to-Date (YTD) time granularity across all aggregation scripts, allowing stakeholders to see cumulative annual performance alongside monthly and quarterly views; (2) front-facing category dimensions that expose the customer-visible product taxonomy used by Talabat/UAE alongside the internal master category hierarchy; and (3) a complete automated scoring and segmentation layer that replaces what was previously done in manual Excel workflows. Additionally, the new pipeline applies performance optimizations (APPROX_COUNT_DISTINCT), introduces a per-entity date-capping mechanism for fair year-over-year comparisons, and restructures the final score_tableau output to include market context, individual scores, weighted scores, and segmentation data.

**Who is this document for?** Ion, the analytics engineer responsible for loading these scripts into the production Airflow DAG and validating outputs in BigQuery.

**What is the end result?** A production pipeline of 29 SQL scripts (up from 24), organized in 6 layers (Gathering, Grouping Sets, Union All, Segmentation, Scoring, Master), that produces the same monthly and quarterly data as before, plus YTD aggregations, front-facing category breakdowns, automated supplier scoring (0-200 scale), and supplier segmentation (4-quadrant classification). The final output table `sps_score_tableau` grows from approximately 73 columns to over 100 columns, with all new fields being additive -- no existing columns are removed or renamed.

---

## 2. Pipeline Architecture Overview

### sps_sql_new (Current Production)

The current pipeline consists of 24 scripts organized in four layers:

- **Gathering layer** (4 scripts): Extract raw data from source tables (supplier_hierarchy, product, customer_order, score_tableau_init)
- **Month layer** (8 scripts): Granular monthly aggregations at SKU/order/supplier level
- **Aggregation layer** (10 scripts): GROUPING SETS rollups producing Monthly and Quarterly views
- **Consumer layer** (1 script): score_tableau joins all aggregation tables into one wide table

**What it lacks:**
- No YTD (Year-to-Date) time granularity
- No front-facing category dimensions (only master categories: l1, l2, l3)
- No automated scoring or segmentation
- No market-level denominators (total market customers/orders)
- Uses COUNT(DISTINCT) everywhere (expensive for multi-entity runs)
- Prev-year comparison uses fixed quarter-based date windows, not per-entity capping

### sps_ytd_prod (New Production)

The new pipeline consists of 29 scripts organized in six layers:

- **Gathering layer** (12 scripts): Collects raw data from external sources + internal mapping (hierarchy, product, customer_order, 9 _month scripts)
- **Grouping Sets layer** (11 scripts): Two-CTE architecture aggregating _month --> Monthly + Quarterly + YTD + Front-Facing, 81 GROUPING SETS per script, APPROX_COUNT_DISTINCT
- **Union All layer** (1 script): Assembles star schema (all_keys UNION + 10 LEFT JOINs)
- **Segmentation layer** (1 script): 4-quadrant supplier classification (Importance x Productivity)
- **Scoring layer** (3 scripts): 200-point scoring model + market benchmarks
- **Master** (1 script): Final denormalized table joining metrics + scores + segments

### Complete Script Comparison Table

| # | Script Name | In sps_sql_new? | In sps_ytd_prod? | What Changed |
|---|-------------|:---------------:|:----------------:|--------------|
| 1 | sps_supplier_hierarchy.sql | Yes | Yes | Entity filter removed (Jinja param handles it) |
| 2 | sps_product.sql | Yes | Yes | Date range: hardcoded dates replaced with 24-month rolling window |
| 3 | sps_customer_order.sql | Yes | Yes | partition_month formula change; front_facing columns propagated |
| 4 | sps_score_tableau_init.sql | Yes | No | Removed -- no longer needed |
| 5 | sps_financial_metrics_month.sql | Yes | Yes | Added front_facing_level_one/two columns and ytd_year |
| 6 | sps_line_rebate_metrics_month.sql | Yes | Yes | Added front_facing dimensions and ytd_year |
| 7 | sps_efficiency_month.sql | Yes | Yes | Added front_facing_level_one/two to product CTE and output |
| 8 | sps_listed_sku_month.sql | Yes | Yes | Added front_facing dimensions and ytd_year |
| 9 | sps_shrinkage_month.sql | Yes | Yes | Added front_facing dimensions and ytd_year |
| 10 | sps_delivery_costs_month.sql | Yes | Yes | rdvr_dc_cmt table swap; added front_facing dims and ytd_year |
| 11 | sps_purchase_order_month.sql | Yes | Yes | Added front_facing dimensions and ytd_year |
| 12 | sps_price_index_month.sql | Yes | Yes | Added front_facing dimensions and ytd_year |
| 13 | sps_days_payable_month.sql | Yes | Yes | Added front_facing dimensions and ytd_year |
| 14 | sps_financial_metrics.sql | Yes | Yes | Two-CTE split; APPROX_COUNT_DISTINCT; front-facing GROUPING SETS; removed line_rebate JOIN |
| 15 | sps_financial_metrics_prev_year.sql | Yes | Yes | Pattern F: per-entity max_date capping; YTD GROUPING SETS added; front-facing sets |
| 16 | sps_line_rebate_metrics.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 17 | sps_delivery_costs.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 18 | sps_shrinkage.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 19 | sps_efficiency.sql | Yes | Yes | Two-CTE split; front-facing in sku_counts and combined CTEs |
| 20 | sps_price_index.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 21 | sps_days_payable.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 22 | sps_purchase_order.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 23 | sps_listed_sku.sql | Yes | Yes | Two-CTE split; front-facing GROUPING SETS |
| 24 | sps_score_tableau.sql | Yes | Yes | Major expansion: market customers JOIN, scoring/segmentation JOINs, weighted scores, ratios, entity name mapping |
| 25 | sps_market_customers.sql | No | Yes | NEW: Total market customers/orders per entity per period |
| 26 | sps_market_yoy.sql | No | Yes | NEW: Market-level YoY growth for scoring thresholds |
| 27 | sps_scoring_params.sql | No | Yes | NEW: Percentile-based scoring thresholds per entity per period |
| 28 | sps_supplier_scoring.sql | No | Yes | NEW: Individual supplier scores (0-200 scale, operations + commercial) |
| 29 | sps_supplier_segmentation.sql | No | Yes | NEW: 4-quadrant supplier classification |
| 30 | sps_supplier_master.sql | No | Yes | NEW: Consolidated view with supplier names, ratios, scores, segments |

**Note**: sps_score_tableau_init.sql is removed in the new pipeline -- its initialization logic is no longer needed because score_tableau uses the all_keys UNION pattern directly.

---

## 3. New Features Introduced

### 3.1 YTD (Year-to-Date) Granularity

#### What Problem It Solves

Stakeholders (Commercial Directors, Category Managers) needed a cumulative annual view to answer questions like "How are we performing year-to-date vs. the same period last year?" The existing pipeline only offered Monthly (e.g., "2026-03-01") and Quarterly (e.g., "Q1-2026") views. To get a YTD number, analysts had to manually sum months in Tableau, which (a) is error-prone for COUNT DISTINCT metrics (customers are not additive across months) and (b) prevents proper YTD-vs-LY comparison because the prior year window must be symmetrically capped.

#### How It Works: Two-CTE Pattern

Every Grouping Sets layer script now splits its logic into two CTEs that are combined via UNION ALL:

```
CTE 1: monthly_quarterly_data
  - WHERE: (current_year AND month <= today) OR (prior_year -- uncapped)
  - GROUPING SETS: all (month, ...) and (quarter_year, ...) combinations
  - time_granularity: 'Monthly' or 'Quarterly'
  - Purpose: Full year view for seasonal analysis (shows all 12 months of prior year)

CTE 2: ytd_data
  - WHERE: (current_year AND month <= today)
           OR (prior_year AND month <= DATE_SUB(today, INTERVAL 1 YEAR))
  - GROUPING SETS: all (ytd_year, ...) combinations
  - time_granularity: 'YTD' (literal string)
  - time_period: 'YTD-2026' or 'YTD-2025'
  - Purpose: Symmetric YTD window for fair YoY comparison

Final SELECT:
  SELECT * FROM monthly_quarterly_data
  UNION ALL
  SELECT * FROM ytd_data
```

**Why two CTEs instead of one?** If you put YTD, Monthly, and Quarterly in the same GROUPING SETS block with one WHERE clause, the YTD rows for the prior year would aggregate the full 12 months (because the WHERE lets all PY data through for Monthly/Quarterly needs). This breaks YoY: YTD-2026 (Jan-Apr) would be compared against all of 2025 (Jan-Dec). By separating into two CTEs, each gets its own WHERE clause with the correct date capping logic.

#### Which Scripts It Affects

All 10 Grouping Sets layer scripts:

| Script | GROUPING SETS in CTE1 | GROUPING SETS in CTE2 | Total Sets |
|--------|----------------------:|----------------------:|-----------:|
| sps_financial_metrics | 54 | 27 | 81 |
| sps_line_rebate_metrics | 54 | 27 | 81 |
| sps_delivery_costs | 54 | 27 | 81 |
| sps_shrinkage | 54 | 27 | 81 |
| sps_efficiency | 54 | 27 | 81 |
| sps_price_index | 54 | 27 | 81 |
| sps_days_payable | 54 | 27 | 81 |
| sps_purchase_order | 54 | 27 | 81 |
| sps_listed_sku | 54 | 27 | 81 |
| sps_financial_metrics_prev_year | 54 | 27 | 81 |

#### The time_period Format

- Monthly: `'2026-04-01'` (CAST of DATE_TRUNC to STRING)
- Quarterly: `'Q1-2026'`
- YTD: `'YTD-2026'` (CONCAT of 'YTD-' and year)

#### WHERE Clause Differences

```sql
-- CTE 1 (monthly_quarterly_data): PY is uncapped
WHERE (EXTRACT(YEAR FROM DATE(src.month)) = current_year
       AND DATE(src.month) <= today)
   OR (EXTRACT(YEAR FROM DATE(src.month)) = prior_year)

-- CTE 2 (ytd_data): PY is symmetrically capped
WHERE (EXTRACT(YEAR FROM DATE(src.month)) = current_year
       AND DATE(src.month) <= today)
   OR (EXTRACT(YEAR FROM DATE(src.month)) = prior_year
       AND DATE(src.month) <= DATE_SUB(today, INTERVAL 1 YEAR))
```

---

### 3.2 Front-Facing Categories

#### What Problem It Solves

Talabat (UAE) and some Pandora entities use a customer-facing category taxonomy ("front-facing") that differs from the internal master category hierarchy. For example, a product might be classified as "Dairy > Yogurt" in master categories but appear as "Breakfast > Morning Essentials" to the customer in the app. Commercial teams needed scorecards that reflect how customers see the product, not just how the warehouse manages it.

#### How It Works

Two new columns are added to the _month layer scripts and propagated through the entire pipeline:

- `front_facing_level_one`: Top-level customer-facing category
- `front_facing_level_two`: Second-level customer-facing category

These are sourced from the `sps_product` table (which in turn reads from the PIM product catalog).

#### COALESCE Chain for entity_key

The entity_key column uses a COALESCE chain that determines priority among dimension levels. The front_facing levels are inserted between master categories and brand/supplier dimensions:

```sql
COALESCE(
  IF(GROUPING(l3_master_category) = 0,       l3_master_category, NULL),       -- highest granularity
  IF(GROUPING(l2_master_category) = 0,       l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0,       l1_master_category, NULL),
  IF(GROUPING(front_facing_level_two) = 0,   front_facing_level_two, NULL),   -- NEW
  IF(GROUPING(front_facing_level_one) = 0,   front_facing_level_one, NULL),   -- NEW
  IF(GROUPING(brand_name) = 0,               brand_name, NULL),
  IF(GROUPING(brand_owner_name) = 0,         brand_owner_name, NULL),
  IF(GROUPING(supplier_id) = 0,              supplier_id, NULL),
  principal_supplier_id                                                        -- fallback
) AS entity_key
```

#### CASE for supplier_level

Similarly, the supplier_level CASE statement now includes front_facing levels:

```sql
CASE
  WHEN GROUPING(l3_master_category) = 0      THEN 'level_three'
  WHEN GROUPING(l2_master_category) = 0      THEN 'level_two'
  WHEN GROUPING(l1_master_category) = 0      THEN 'level_one'
  WHEN GROUPING(front_facing_level_two) = 0  THEN 'front_facing_level_two'    -- NEW
  WHEN GROUPING(front_facing_level_one) = 0  THEN 'front_facing_level_one'    -- NEW
  WHEN GROUPING(brand_name) = 0              THEN 'brand_name'
  ELSE 'supplier'
END AS supplier_level
```

#### GROUPING SETS Count Increase

| Component | sps_sql_new | sps_ytd_prod | Delta |
|-----------|:-----------:|:------------:|:-----:|
| Monthly GROUPING SETS (master cat only) | 19 | 19 | 0 |
| Monthly GROUPING SETS (front-facing) | 0 | 8 | +8 |
| Quarterly GROUPING SETS (master cat only) | 19 | 19 | 0 |
| Quarterly GROUPING SETS (front-facing) | 0 | 8 | +8 |
| YTD GROUPING SETS (all) | 0 | 27 | +27 |
| **Total per script** | **38** | **81** | **+43** |

The 8 front-facing sets per time granularity correspond to:
- 4 owner types (principal, division, brand_owner, brand_name) x 2 front_facing levels

#### Which Scripts It Affects

- **All 8 _month scripts**: Added front_facing_level_one and front_facing_level_two to SELECT, propagated from sps_product
- **All 10 Grouping Sets scripts**: Added front-facing GROUPING SETS in both CTE1 and CTE2
- **sps_financial_metrics_prev_year**: Added front-facing GROUPING SETS in all three blocks (monthly, quarterly, YTD)

---

### 3.3 Scoring Layer (NEW Scripts)

#### What Problem It Solves

Previously, supplier scoring was done manually in Excel by Commercial Directors for each entity. This was time-consuming, inconsistent across entities, and impossible to automate for 19 entities simultaneously. The new scoring layer computes individual supplier scores on a 0-200 scale (0-100 Operations + 0-100 Commercial) automatically, with full explainability (every threshold and ratio is stored alongside the score).

#### The 4 New Scripts

| Script | Purpose | Grain | Key Outputs |
|--------|---------|-------|-------------|
| `sps_scoring_params` | Compute percentile-based thresholds per entity per time_period | global_entity_id x time_period | bm_starting, bm_ending, fm_starting, fm_ending, gbd_target/lower/upper |
| `sps_supplier_scoring` | Score each supplier (0-200) across 7 metrics | supplier x entity x time_period | score_fill_rate (60pts), score_otd (40pts), score_yoy (10pts), score_efficiency (30pts), score_gbd (20pts), score_back_margin (25pts), score_front_margin (15pts), operations_score, commercial_score, total_score |
| `sps_supplier_segmentation` | Classify suppliers into 4 quadrants | supplier x entity x time_period | segment_lc (Key Accounts / Standard / Niche / Long Tail), importance_score_lc, productivity_score_lc |
| `sps_supplier_master` | Consolidated view with names, ratios, scores, segments | supplier x entity x time_period | All supplier_scoring + segmentation + financial context + entity brand name |

#### How sps_scoring_params Works

For each entity and time_period (Monthly granularity), it computes:

- **Back Margin thresholds**: P25 and IQR-based ceiling from suppliers with active rebates
- **Front Margin thresholds**: Starting point floored at 12%, ceiling from IQR + weighted average
- **GBD targets**: Per-entity hardcoded targets (19 entities), with lower = target * 0.5 and upper = target * 2.0

These thresholds are used by sps_supplier_scoring to normalize raw ratios into point scores. For YTD scoring, the parameters from the latest available Monthly period are used (not YTD-level parameters, which would distort percentile distributions).

#### How sps_supplier_scoring Works

Seven metrics are scored individually:

| Metric | Max Points | Scoring Logic |
|--------|:----------:|---------------|
| Fill Rate | 60 | Linear: fill_rate * 60 (capped at 1.0) |
| OTD | 40 | Linear: otd * 40 (capped at 1.0) |
| YoY Growth | 10 | Linear ramp from 0 to market YoY * 1.2 |
| Efficiency | 30 | Linear ramp from 40% to 100% |
| GBD | 20 | Asymmetric bell curve around entity target |
| Back Margin | 25 | Linear between P25 and IQR-based ceiling |
| Front Margin | 15 | Linear between 12% floor and IQR-based ceiling |

Sub-totals:
- operations_score = score_fill_rate + score_otd (max 100)
- commercial_score = score_yoy + score_efficiency + score_gbd + score_back_margin + score_front_margin (max 100)
- total_score = (operations_score + commercial_score) / 2.0 (max 100)

#### How sps_supplier_segmentation Works

Uses two axes from sps_score_tableau data:

- **Importance** (0-100): Percentile-based score from net_profit_lc (P15 to P95 range)
- **Productivity** (0-100): Weighted composite of:
  - Customer Penetration: 40 points (PRIMARY signal, r=0.2718 correlation with profitability)
  - Frequency: 30 points (orders per customer)
  - ABV: 30 points (average basket value)

Classification:

| Quadrant | Importance | Productivity |
|----------|:----------:|:------------:|
| Key Accounts | > 15 | >= 40 |
| Standard | > 15 | < 40 |
| Niche | <= 15 | >= 40 |
| Long Tail | <= 15 | < 40 |

Suppliers with Net_Sales_eur < 1,000 receive score 0 for both axes (excluded from percentile calculation).

#### Execution Order Dependency

```
Union All (sps_score_tableau)
  |
  +--> Segmentation: sps_supplier_segmentation  (reads from score_tableau)
  |
  +--> Scoring:
  |      sps_market_yoy       (reads from score_tableau)
  |      sps_scoring_params   (reads from score_tableau)
  |        |
  |        +--> sps_supplier_scoring  (reads from score_tableau + scoring_params + market_yoy)
  |
  +--> Master: sps_supplier_master  (reads from score_tableau + scoring + segmentation)
```

**Important**: Segmentation and Scoring are PARALLEL -- both read from score_tableau, no dependency on each other. sps_supplier_master is the FINAL table that joins everything at the end.

---

### 3.4 Market Scripts (NEW)

#### sps_market_customers

- **Purpose**: Provides the denominator for customer penetration calculations (how many total customers exist in the market, regardless of supplier)
- **Grain**: global_entity_id x time_period x time_granularity
- **Metrics**: `total_market_customers` (APPROX_COUNT_DISTINCT), `total_market_orders`
- **Time granularities**: Monthly, Quarterly, YTD (each computed as true COUNT DISTINCT over the period, not sum of months)
- **Data source**: sps_customer_order (no supplier filter)
- **Critical note**: A customer who ordered in Oct + Nov + Dec = 1 customer in Q4 (not 3). This is why APPROX_COUNT_DISTINCT is computed per period, not summed.

#### sps_market_yoy

- **Purpose**: Market-level YoY growth rate used as ceiling for the YoY scoring component in sps_supplier_scoring
- **Grain**: global_entity_id x time_period x time_granularity
- **Formula**: `(SUM(Net_Sales_lc) - SUM(Net_Sales_lc_Last_Year)) / SUM(Net_Sales_lc_Last_Year)`
- **Filters**: supplier_level='supplier', division_type='division', Monthly only, Net_Sales_eur > 1000, LY > 0
- **Used by**: sps_supplier_scoring as `yoy_max = LEAST(GREATEST(market_yoy * 1.2, 0.20), 0.70)`

---

### 3.5 APPROX_COUNT_DISTINCT Optimization

#### Why

When running the pipeline for all 19 entities simultaneously, COUNT(DISTINCT) on high-cardinality columns (analytical_customer_id, order_id, sku_id, warehouse_id) consumes excessive BigQuery slots. APPROX_COUNT_DISTINCT uses HyperLogLog++ and processes the same data with significantly fewer resources.

#### Where Applied

| Script | Occurrences | Columns |
|--------|:-----------:|---------|
| sps_financial_metrics.sql | 8 (4 in CTE1 + 4 in CTE2) | analytical_customer_id, sku_id, order_id, warehouse_id |
| sps_market_customers.sql | 6 (2 in monthly + 2 in quarterly + 2 in ytd) | analytical_customer_id, order_id |

#### Expected Variance

APPROX_COUNT_DISTINCT guarantees < 2% error rate for typical cardinalities (> 1000 distinct values). For the SPS use case (commercial scorecards, not billing), this level of precision is acceptable and was explicitly approved by stakeholders.

**What was NOT changed**: The _month scripts (Gathering layer) still use exact COUNT(DISTINCT) where needed, because the _month grain is small enough that exact counts are affordable. The optimization applies only at the Grouping Sets layer where cross-month GROUPING SETS amplify the cardinality.

---

### 3.6 Prev Year Pattern F (Per-Entity Date Capping)

#### What Problem It Solves

Different entities (countries) may have different data freshness. For example, TB_AE might have data through April 28 while PY_AR might only have data through April 15. If we use a global date cutoff for the prior year comparison, entities with less-fresh data get an unfair YoY comparison (their LY window is wider than their CY window).

#### How max_date_cy Works

```sql
-- Step 1: Find the latest month with data per entity in the current year
max_date_cy AS (
  SELECT
    global_entity_id,
    MAX(CAST(month AS DATE)) AS max_month_cy
  FROM sps_financial_metrics_month
  WHERE EXTRACT(YEAR FROM CAST(month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY global_entity_id
)

-- Step 2: Filter prior year data, capping at equivalent date per entity
filtered_ly AS (
  SELECT m.*
  FROM sps_financial_metrics_month m
  JOIN max_date_cy mx ON m.global_entity_id = mx.global_entity_id
  WHERE EXTRACT(YEAR FROM CAST(m.month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    AND CAST(m.month AS DATE) <= DATE_SUB(LAST_DAY(mx.max_month_cy), INTERVAL 1 YEAR)
)
```

#### join_time_period Shift

The prev_year script shifts time_period forward by 1 year so it can JOIN to current year data:

```sql
CASE
  WHEN GROUPING(month) = 0        THEN CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING)
  WHEN GROUPING(quarter_year) = 0 THEN CONCAT(SUBSTR(quarter_year,1,2), '-',
                                        CAST(CAST(SUBSTR(quarter_year,4) AS INT64) + 1 AS STRING))
  WHEN GROUPING(ytd_year) = 0     THEN CONCAT('YTD-',
                                        CAST(CAST(ytd_year AS INT64) + 1 AS STRING))
END AS join_time_period
```

Example: LY data for "2025-03-01" becomes join_time_period "2026-03-01", which matches CY's time_period.

#### Key Architectural Change: No More line_rebate_metrics JOIN

In `sps_sql_new`, the financial_metrics final SELECT joined three tables:

```sql
-- OLD (sps_sql_new/sps_financial_metrics.sql)
FROM current_year_data cy
LEFT JOIN sps_financial_metrics_prev_year ly ...
LEFT JOIN sps_line_rebate_metrics r ...          -- back_margin added here
```

In `sps_ytd_prod`, the line_rebate JOIN is removed from financial_metrics. Instead, back_margin_amt_lc is now added in sps_score_tableau (the Union All layer) where it is joined from sps_line_rebate_metrics as a separate LEFT JOIN. This change improves separation of concerns: financial_metrics is now purely about financial data, and the Union All table handles metric assembly.

```sql
-- NEW (sps_ytd_prod/sps_financial_metrics.sql)
FROM (
  SELECT * FROM monthly_quarterly_data
  UNION ALL
  SELECT * FROM ytd_data
) cy
LEFT JOIN sps_financial_metrics_prev_year ly ... -- only prev_year, no line_rebate
```

### 3.7 Supplier Name Resolution in score_tableau

#### What Problem It Solves

The `brand_sup` field in score_tableau contains supplier IDs (e.g., "SUP-12345") for rows where `division_type IN ('division', 'principal')`. These IDs are not human-readable. Tableau users, the scoring layer, and the segmentation layer all need actual supplier names. Brand owners and brand names already have readable values in `brand_sup`, so no resolution is needed for those rows.

#### How It Works

1. A CTE `sps_product_clean` extracts distinct `supplier_id -> supplier_name` mappings from `sps_product`.
2. A LEFT JOIN matches `brand_sup` to `supplier_id` **only** for rows where `division_type IN ('division', 'principal')`.
3. A new column `supplier_name` uses a CASE expression:

```sql
CASE
  WHEN division_type IN ('division', 'principal')
    THEN COALESCE(prod.supplier_name, brand_sup)  -- resolved name with fallback
  ELSE brand_sup                                   -- already readable (brand_owner, brand_name, total)
END AS supplier_name
```

**Note:** This feature did NOT exist in `sps_sql_new` (current production). It is a new addition from `sps_ytd_prod`.

---

## 4. Jinja Parameterization

### All Parameters Used

| Parameter | Type | Example Value | Purpose | Used In |
|-----------|------|---------------|---------|---------|
| `params.project_id` | string | `fulfillment-dwh-production` | BigQuery project ID | All 29 scripts |
| `params.dataset.cl` | string | `cl_supplier_scorecard` | Target dataset | All 29 scripts |
| `params.dataset.curated_data_shared_salesforce_srm` | string | `curated_data_shared_salesforce_srm` | Source dataset for supplier hierarchy | sps_supplier_hierarchy |
| `params.backfill` | boolean | `false` | Toggles backfill mode | _month scripts, gathering scripts |
| `params.is_backfill_chunks_enabled` | boolean | `false` | Enables chunked backfill | _month scripts, gathering scripts |
| `params.backfill_start_date` | string | `'2025-01-01'` | Backfill chunk start | _month scripts (backfill mode) |
| `params.backfill_end_date` | string | `'2025-03-31'` | Backfill chunk end | _month scripts (backfill mode) |
| `params.stream_look_back_days` | int | `90` | Days to look back in incremental mode | _month scripts |
| `next_ds` | string | `'2026-04-29'` | Airflow next execution date | _month scripts, gathering scripts |
| `params.param_global_entity_id` | regex | `r'TB_AE\|PY_AR'` | Entity filter (regex) | _month scripts (WHERE clause) |
| `params.param_country_code` | regex | `r'ae\|ar'` | Country code filter | _month scripts (WHERE clause) |

### Date Pattern: Gathering Layer (_month) vs Grouping Sets Layer

**_month scripts** use the `date_in / date_fin` CTE pattern with `next_ds`:

```sql
WITH date_in AS (
  SELECT DATE(DATE_TRUNC(DATE_SUB('{{ next_ds }}', INTERVAL {{ params.stream_look_back_days }} DAY), MONTH)) AS date_in
),
date_fin AS (
  SELECT CAST('{{ next_ds }}' AS DATE) AS date_fin
)
```

**Grouping Sets scripts** use `CURRENT_DATE()` via a `date_config` CTE:

```sql
WITH date_config AS (
  SELECT
    CURRENT_DATE() AS today,
    EXTRACT(YEAR FROM CURRENT_DATE()) AS current_year,
    EXTRACT(YEAR FROM CURRENT_DATE()) - 1 AS prior_year
)
```

**Why the difference?** The _month scripts (Gathering layer) run incrementally (appending new data for recent months), so they need an explicit execution date boundary (`next_ds`). The Grouping Sets scripts are full-refresh (CREATE OR REPLACE) and always aggregate over whatever data exists in the _month tables, so they use CURRENT_DATE() to determine the current year boundary.

---

## 5. Script-by-Script Changes (for Ion's Review)

### Gathering Layer (12 scripts)

| Script | Summary of Changes |
|--------|-------------------|
| sps_supplier_hierarchy | Entity filter (`WHERE REGEXP_CONTAINS(global_entity_id, ...)`) removed from the SQL; filtering is now handled exclusively by Jinja params at the _month layer. No structural changes. |
| sps_product | Date range logic changed from hardcoded dates to a 24-month rolling window using `DECLARE run_anchor_date` and `date_start_current`. Ensures consistent historical depth regardless of execution date. |
| sps_customer_order | partition_month formula updated; front_facing columns now propagated from sps_product JOIN. |
| sps_score_tableau_init | **REMOVED** -- no longer exists in sps_ytd_prod. |
| sps_financial_metrics_month | Added `front_facing_level_one`, `front_facing_level_two` to SELECT list. Added `ytd_year` column (`EXTRACT(YEAR FROM order_date)`). No structural changes to the DML pattern. |
| sps_line_rebate_metrics_month | Same as financial_metrics_month: added front_facing dims and ytd_year. |
| sps_efficiency_month | Added `front_facing_level_one`, `front_facing_level_two` to the tmp_sp_product CTE and propagated through ranked_global_product and all downstream joins. GROUP BY updated to include the two new columns. |
| sps_listed_sku_month | Added front_facing dimensions and ytd_year. |
| sps_shrinkage_month | Added front_facing dimensions and ytd_year. |
| sps_delivery_costs_month | Source table swapped from old delivery cost table to `rdvr_dc_cmt`. Added front_facing dims and ytd_year. |
| sps_purchase_order_month | Added front_facing dimensions and ytd_year. |
| sps_price_index_month | Added front_facing dimensions and ytd_year. |
| sps_days_payable_month | Added front_facing dimensions and ytd_year. |

### Grouping Sets Layer (11 scripts)

| Script | Summary of Changes |
|--------|-------------------|
| sps_financial_metrics_prev_year | Restructured with Pattern F: 3-CTE architecture (max_date_cy + filtered_ly + aggregated). YTD GROUPING SETS added. Front-facing GROUPING SETS added. join_time_period now handles YTD shift. Must run first within the financial chain. |
| sps_financial_metrics | Single CTE split into two-CTE (monthly_quarterly_data + ytd_data). COUNT(DISTINCT) replaced with APPROX_COUNT_DISTINCT for 4 columns. Front-facing GROUPING SETS added (8 per time granularity). line_rebate_metrics JOIN removed from final SELECT. JOINs prev_year for YoY. |
| sps_purchase_order | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_efficiency | Two-CTE split applied to the final aggregation stage (after sku_counts and efficiency_by_warehouse CTEs). Front-facing added to sku_counts GROUP BY, efficiency_by_warehouse GROUP BY, combined JOIN, and both GROUPING SETS blocks. |
| sps_line_rebate_metrics | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_price_index | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_shrinkage | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_days_payable | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_listed_sku | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_delivery_costs | Two-CTE split. Front-facing GROUPING SETS added. |
| sps_market_customers | Total market customers and orders per entity per period (Monthly, Quarterly, YTD). Uses APPROX_COUNT_DISTINCT. Reads from sps_customer_order without supplier filter. |

### Union All Layer (1 script)

| Script | Summary of Changes |
|--------|-------------------|
| sps_score_tableau | Major expansion. All_keys UNION now includes all 9 source tables (same as before). New LEFT JOIN to sps_market_customers (on entity + time_period + time_granularity only, since market data is not supplier-specific). New CTE `sps_product_clean` and LEFT JOIN to resolve `supplier_name` for division/principal rows (see 3.7). Added computed ratio columns (ratio_otd, ratio_fill_rate, ratio_efficiency, etc.). Added weighted score numerators (wscore_num_*) for proper Tableau aggregation. Added entity brand name mapping (CASE on global_entity_id prefix). Removed calc_net_delivered and calc_net_return (moved to line_rebate_metrics). |

### Segmentation Layer (1 script)

| Script | Summary |
|--------|---------|
| sps_supplier_segmentation | 4-quadrant classification: Key Accounts, Standard, Niche, Long Tail. Two-axis model (importance 0-100 + productivity 0-100). Reads from score_tableau + market_customers. |

### Scoring Layer (3 scripts)

| Script | Summary |
|--------|---------|
| sps_scoring_params | Percentile-based thresholds for back margin (IQR-trimmed), front margin (12% floor), and GBD (per-entity targets). Reads from sps_score_tableau Monthly data only. |
| sps_market_yoy | Market-level YoY growth for Monthly data. Reads from sps_score_tableau. Used as scoring ceiling in sps_supplier_scoring. |
| sps_supplier_scoring | Individual supplier scores: 7 metrics, operations subtotal, commercial subtotal, total_score. Reads from score_tableau + scoring_params + market_yoy. |

### Master (1 script)

| Script | Summary |
|--------|---------|
| sps_supplier_master | Consolidated view joining base financials from score_tableau with supplier names (from sps_product), entity brand names, computed ratios, scores, thresholds, segments, and weighted scores. This is the "one table to rule them all" for Tableau. |

---

## 6. Execution Order

The pipeline runs in 6 layers. Scripts within a layer can run in parallel (unless noted); layers must run sequentially. Segmentation and Scoring are parallel -- both read from score_tableau with no dependency on each other.

```
Gathering --> Grouping Sets --> Union All --+--> Segmentation --+--> Master
                                           +--> Scoring -------+
```

### Layer 1: Gathering Layer (12 scripts)

Collects raw data from external sources + internal mapping.

```
L0: sps_supplier_hierarchy           (Salesforce SRM --> hierarchy, no dependencies)
L1: sps_product                      (catalog + PO --> SKU mapping, no dependencies)
L2: sps_customer_order               (depends on: sps_product)
    sps_financial_metrics_month      (depends on: sps_customer_order)
    sps_line_rebate_metrics_month    (depends on: sps_product)
    sps_efficiency_month             (depends on: sps_product, sps_customer_order)
    sps_listed_sku_month             (depends on: sps_product)
    sps_shrinkage_month              (depends on: sps_product)
    sps_delivery_costs_month         (depends on: sps_product)
    sps_purchase_order_month         (depends on: sps_product)
    sps_price_index_month            (depends on: sps_product)
    sps_days_payable_month           (depends on: sps_product)
```

Internal constraint: hierarchy --> product --> [9 _month + customer_order] (all parallel after product).

### Layer 2: Grouping Sets Layer (11 scripts)

Aggregates _month --> Monthly + Quarterly + YTD + Front-Facing. Two-CTE pattern, 81 GROUPING SETS per script.

```
Step 2a (parallel):
  sps_financial_metrics_prev_year    (Pattern F, must run first within financial chain)
  sps_purchase_order                 (depends on: sps_purchase_order_month)
  sps_efficiency                     (depends on: sps_efficiency_month)
  sps_line_rebate_metrics            (depends on: sps_line_rebate_metrics_month)
  sps_price_index                    (depends on: sps_price_index_month)
  sps_shrinkage                      (depends on: sps_shrinkage_month)
  sps_days_payable                   (depends on: sps_days_payable_month)
  sps_listed_sku                     (depends on: sps_listed_sku_month)
  sps_delivery_costs                 (depends on: sps_delivery_costs_month)
  sps_market_customers               (Monthly + Quarterly + YTD, depends on: sps_customer_order)

Step 2b (after 2a):
  sps_financial_metrics              (JOINs prev_year --> YoY, depends on: sps_financial_metrics_month + sps_financial_metrics_prev_year)
```

Internal constraint: financial_metrics_month --> prev_year --> financial_metrics (others run in parallel).

### Layer 3: Union All Layer (1 script)

Assembles star schema: all_keys UNION + 10 LEFT JOINs.

```
  sps_score_tableau                  (depends on: ALL Layer 2 tables + sps_market_customers + sps_product [supplier name lookup])
```

### Layer 4: Segmentation Layer (1 script) -- PARALLEL with Layer 5

Classifies suppliers into 4 quadrants (Importance x Productivity).

```
  sps_supplier_segmentation          (depends on: sps_score_tableau)
```

### Layer 5: Scoring Layer (3 scripts) -- PARALLEL with Layer 4

200-point scoring model + market benchmarks.

```
Step 5a (parallel):
  sps_scoring_params                 (thresholds IQR/percentiles, depends on: sps_score_tableau)
  sps_market_yoy                     (YoY ceiling per market, depends on: sps_score_tableau)

Step 5b (after 5a):
  sps_supplier_scoring               (7 KPIs --> score 0-200, depends on: sps_score_tableau + sps_scoring_params + sps_market_yoy)
```

### Layer 6: Master (1 script)

Final denormalized table: metrics + scores + segments.

```
  sps_supplier_master                (depends on: sps_score_tableau + sps_supplier_scoring + sps_supplier_segmentation)
```

**Layer dependencies**: Gathering --> Grouping Sets --> Union All --> [Segmentation || Scoring] --> Master.

Segmentation and Scoring are PARALLEL -- both read from score_tableau, no dependency on each other. Master joins them at the end.

---

## 7. Attention Points (ytd_test --> sps_ytd_prod Differences)

The `sps_ytd_prod/` directory is the production-ready version derived from `ytd_test/`. Several changes were made during the transition from debug (ytd_test/ytd_sps_*) to production (sps_ytd_prod/sps_*):

### 7.1 Naming Convention

- **ytd_test**: Files prefixed with `ytd_sps_*` (e.g., `ytd_sps_financial_metrics.sql`)
- **sps_ytd_prod**: Files use standard `sps_*` prefix (e.g., `sps_financial_metrics.sql`)
- Table names in SQL also drop the `ytd_` prefix to match production naming

### 7.2 delivery_costs_month: Source Table Swap

The delivery costs _month script now reads from `rdvr_dc_cmt` instead of the previous delivery cost source table. This reflects a data platform migration to a consolidated delivery cost model.

### 7.3 customer_order: partition_month Formula

The partition_month calculation was updated in sps_ytd_prod. Verify that the new formula produces the same monthly partitioning as the old one for your backfill validation.

### 7.4 product: Date Range Logic

- **sps_sql_new**: Used hardcoded date values for the historical window
- **sps_ytd_prod**: Uses a 24-month rolling window computed from `next_ds`:

```sql
DECLARE run_anchor_date DATE DEFAULT DATE_SUB(DATE_TRUNC(CAST('{{ next_ds }}' AS DATE), MONTH), INTERVAL 1 DAY);
DECLARE date_start_current DATE DEFAULT DATE_SUB(DATE_TRUNC(run_anchor_date, MONTH), INTERVAL 23 MONTH);
```

This ensures the product dimension table always covers exactly 24 months of product data regardless of when the pipeline runs.

### 7.5 supplier_hierarchy: Entity Filter Removed

- **sps_sql_new**: Had a `WHERE REGEXP_CONTAINS(global_entity_id, ...)` filter
- **sps_ytd_prod**: Removed -- the entity filter is applied only at the _month layer via Jinja params. This ensures the hierarchy table contains ALL entities, allowing the pipeline to run for any subset without rebuilding the hierarchy.

### 7.6 All _month Scripts: CREATE OR REPLACE Removed

- **sps_sql_new / ytd_test**: Some _month scripts used `CREATE OR REPLACE TABLE` (full refresh DML pattern)
- **sps_ytd_prod**: _month scripts are designed for Airflow DML (DELETE + INSERT or MERGE), so the `CREATE OR REPLACE` is typically not used in production. However, the Grouping Sets layer scripts still use `CREATE OR REPLACE` because they are full-refresh by design.

### 7.7 Entity Filters: DECLARE --> Jinja Params

- **ytd_test**: Used BigQuery `DECLARE` statements for entity filtering
- **sps_ytd_prod**: Uses Jinja `{{ params.param_global_entity_id }}` and `{{ params.param_country_code }}` for entity filtering, compatible with Airflow's Jinja templating engine

---

## 8. Validation Queries

After deploying the pipeline, run these 5 queries to verify correctness. Replace `{project}` and `{dataset}` with your production values.

### Query 1: Entity Coverage

Verify all expected entities produce data across all three time granularities.

```sql
-- Expected: 19 entities x 3 granularities = 57 rows (if all entities active)
SELECT
  global_entity_id,
  time_granularity,
  COUNT(DISTINCT time_period) AS period_count,
  COUNT(*) AS row_count,
  MIN(time_period) AS first_period,
  MAX(time_period) AS last_period
FROM `{project}.{dataset}.sps_score_tableau`
GROUP BY global_entity_id, time_granularity
ORDER BY global_entity_id, time_granularity;
```

### Query 2: YTD Equals Sum of Monthly (for Additive Metrics)

For additive metrics (Net_Sales_eur), YTD should equal the sum of monthly values within the same year. Differences indicate a WHERE clause or GROUPING SETS issue.

```sql
-- For each entity, compare YTD total vs sum of monthly values
-- Delta should be < 0.01 (rounding) for additive metrics
WITH monthly_sum AS (
  SELECT
    global_entity_id,
    EXTRACT(YEAR FROM CAST(time_period AS DATE)) AS yr,
    brand_sup,
    entity_key,
    division_type,
    supplier_level,
    SUM(Net_Sales_eur) AS monthly_total
  FROM `{project}.{dataset}.sps_financial_metrics`
  WHERE time_granularity = 'Monthly'
  GROUP BY 1,2,3,4,5,6
),
ytd_val AS (
  SELECT
    global_entity_id,
    CAST(REPLACE(time_period, 'YTD-', '') AS INT64) AS yr,
    brand_sup,
    entity_key,
    division_type,
    supplier_level,
    Net_Sales_eur AS ytd_total
  FROM `{project}.{dataset}.sps_financial_metrics`
  WHERE time_granularity = 'YTD'
)
SELECT
  m.global_entity_id,
  m.yr,
  m.supplier_level,
  ROUND(m.monthly_total, 2) AS sum_monthly,
  ROUND(y.ytd_total, 2) AS ytd_value,
  ROUND(ABS(m.monthly_total - y.ytd_total), 2) AS delta
FROM monthly_sum m
JOIN ytd_val y USING (global_entity_id, yr, brand_sup, entity_key, division_type, supplier_level)
WHERE ABS(m.monthly_total - y.ytd_total) > 1.0
ORDER BY delta DESC
LIMIT 20;
```

### Query 3: Last Year Values Not Zero

Verify that YTD rows have non-zero Net_Sales_eur_Last_Year (proving the prev_year JOIN is working).

```sql
-- Expected: current year YTD rows should have LY populated
-- Prior year YTD may have LY = 0 if no 2-year-ago data exists
SELECT
  global_entity_id,
  time_period,
  COUNT(*) AS total_rows,
  COUNTIF(Net_Sales_eur_Last_Year > 0) AS rows_with_ly,
  ROUND(COUNTIF(Net_Sales_eur_Last_Year > 0) / COUNT(*) * 100, 1) AS pct_with_ly,
  ROUND(AVG(Net_Sales_eur_Last_Year), 2) AS avg_ly_value
FROM `{project}.{dataset}.sps_financial_metrics`
WHERE time_granularity = 'YTD'
  AND supplier_level = 'supplier'
GROUP BY global_entity_id, time_period
ORDER BY global_entity_id, time_period;
```

### Query 4: Front-Facing Data Present

Verify that front_facing_level_one and front_facing_level_two supplier_level values exist in the output.

```sql
-- Expected: rows with supplier_level IN ('front_facing_level_one', 'front_facing_level_two')
-- At minimum for entities that have front-facing categories (TB_AE, TB_KW, etc.)
SELECT
  global_entity_id,
  supplier_level,
  time_granularity,
  COUNT(*) AS row_count,
  COUNT(DISTINCT entity_key) AS distinct_categories,
  ROUND(SUM(Net_Sales_eur), 2) AS total_gpv_eur
FROM `{project}.{dataset}.sps_score_tableau`
WHERE supplier_level LIKE 'front_facing%'
GROUP BY global_entity_id, supplier_level, time_granularity
ORDER BY global_entity_id, supplier_level, time_granularity;
```

### Query 5: Scoring Output Verification

Verify that scoring and segmentation are producing non-NULL values for the expected scope.

```sql
-- Scoring: check total_score distribution
SELECT
  global_entity_id,
  time_granularity,
  COUNT(*) AS suppliers_scored,
  ROUND(AVG(total_score), 1) AS avg_score,
  ROUND(MIN(total_score), 1) AS min_score,
  ROUND(MAX(total_score), 1) AS max_score,
  COUNTIF(total_score IS NULL) AS null_scores
FROM `{project}.{dataset}.sps_supplier_scoring`
GROUP BY global_entity_id, time_granularity
ORDER BY global_entity_id, time_granularity;

-- Segmentation: check segment distribution
SELECT
  global_entity_id,
  time_granularity,
  segment_lc,
  COUNT(*) AS supplier_count,
  ROUND(AVG(importance_score_lc), 1) AS avg_importance,
  ROUND(AVG(productivity_score_lc), 1) AS avg_productivity
FROM `{project}.{dataset}.sps_supplier_segmentation`
GROUP BY global_entity_id, time_granularity, segment_lc
ORDER BY global_entity_id, time_granularity, segment_lc;
```

---

**End of Migration Guide**

For questions or clarifications, contact Christian La Rosa (SQL logic owner) or the CSM Analytics team.
