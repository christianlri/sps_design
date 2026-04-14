# SPS Score Tableau - 87 Fields (NEW Version)

## Aggregation Keys (7 fields)

| # | Field | Type | Source | Description |
|---|-------|------|--------|-------------|
| 1 | global_entity_id | STRING | all_keys | Country/entity identifier |
| 2 | time_period | STRING | all_keys | YYYY-MM for monthly, YYYY-Qx for quarterly |
| 3 | brand_sup | STRING | all_keys | Principal supplier ID / Supplier ID / Brand owner / Brand name / 'total' |
| 4 | entity_key | STRING | all_keys | L3 category / L2 category / L1 category / Brand name / Brand owner / Supplier ID / Principal supplier ID |
| 5 | division_type | STRING | all_keys | 'principal' / 'division' / 'brand_owner' / 'brand_name' / 'total' |
| 6 | supplier_level | STRING | all_keys | 'level_three' / 'level_two' / 'level_one' / 'brand_name' / 'supplier' |
| 7 | time_granularity | STRING | all_keys | 'Monthly' or 'Quarterly' |

## Price Index Metrics (3 fields)

| # | Field | Type | Source | Description | Ingredient |
|---|-------|------|--------|-------------|-----------|
| 8 | median_price_index | FLOAT | sps_price_index | Deprecated. Use price_index_numerator/price_index_weight | |
| 9 | price_index_numerator | FLOAT | sps_price_index | Weighted price index numerator. Tableau: SUM(price_index_numerator) / SUM(price_index_weight) | ✓ |
| 10 | price_index_weight | FLOAT | sps_price_index | GPV-based weight for price index denominator | ✓ |

## Days Payable Metrics (7 fields)

| # | Field | Type | Source | Description | Ingredient |
|---|-------|------|--------|-------------|-----------|
| 11 | payment_days | INTEGER | sps_days_payable | Days Payable Outstanding (raw) | |
| 12 | doh | FLOAT | sps_days_payable | Deprecated. Use stock_value_eur/cogs_monthly_eur | |
| 13 | dpo | FLOAT | sps_days_payable | Deprecated. Use payment_days - calculated_doh | |
| 14 | stock_value_eur | FLOAT | sps_days_payable | Stock value in EUR for DOH calculation | ✓ |
| 15 | cogs_monthly_eur | NUMERIC | sps_days_payable | Monthly COGS in EUR for DOH calculation | ✓ |
| 16 | days_in_month | INTEGER | sps_days_payable | Days in month for monthly DOH calculation | ✓ |
| 17 | days_in_quarter | INTEGER | sps_days_payable | Days in quarter for quarterly DOH calculation | ✓ |

## Financial Metrics (31 fields)

| # | Field | Type | Source | Description |
|---|-------|------|--------|-------------|
| 18 | total_customers | INTEGER | sps_financial_metrics | Distinct analytical customers |
| 19 | total_skus_sold | INTEGER | sps_financial_metrics | Distinct SKUs sold |
| 20 | total_orders | INTEGER | sps_financial_metrics | Distinct orders |
| 21 | total_warehouses_sold | INTEGER | sps_financial_metrics | Distinct warehouses |
| 22 | Total_Net_Sales_eur_order | NUMERIC | sps_financial_metrics | Order-level net sales EUR (aggregated) |
| 23 | Net_Sales_eur | NUMERIC | sps_financial_metrics | Net sales EUR |
| 24 | COGS_eur | NUMERIC | sps_financial_metrics | Cost of goods sold EUR |
| 25 | Net_Sales_from_promo_eur | NUMERIC | sps_financial_metrics | Net sales from promotional discounts EUR |
| 26 | total_supplier_funding_eur | NUMERIC | sps_financial_metrics | Supplier funding EUR |
| 27 | front_margin_amt_eur | NUMERIC | sps_financial_metrics | Front margin EUR (Net_Sales - COGS) |
| 28 | total_discount_eur | NUMERIC | sps_financial_metrics | Total discounts EUR |
| 29 | Promo_GPV_contribution_eur | NUMERIC | sps_financial_metrics | Promotional GPV contribution EUR |
| 30 | Total_Net_Sales_lc_order | NUMERIC | sps_financial_metrics | Order-level net sales LC (local currency) |
| 31 | Net_Sales_lc | NUMERIC | sps_financial_metrics | Net sales LC |
| 32 | COGS_lc | NUMERIC | sps_financial_metrics | COGS LC |
| 33 | Net_Sales_from_promo_lc | NUMERIC | sps_financial_metrics | Net sales from promo LC |
| 34 | total_supplier_funding_lc | NUMERIC | sps_financial_metrics | Supplier funding LC |
| 35 | front_margin_amt_lc | NUMERIC | sps_financial_metrics | Front margin LC |
| 36 | total_discount_lc | NUMERIC | sps_financial_metrics | Total discounts LC |
| 37 | Promo_GPV_contribution_lc | NUMERIC | sps_financial_metrics | Promotional GPV contribution LC |
| 38 | total_GBV | NUMERIC | sps_financial_metrics | Gross Booking Value EUR |
| 39 | fulfilled_quantity | NUMERIC | sps_financial_metrics | Total fulfilled quantity |
| 40 | Net_Sales_eur_Last_Year | NUMERIC | sps_financial_metrics | YoY comparison: Net sales EUR from same period last year |
| 41 | Net_Sales_lc_Last_Year | NUMERIC | sps_financial_metrics | YoY comparison: Net sales LC from same period last year |
| 42 | YoY_GPV_Growth_eur | NUMERIC | sps_financial_metrics | Year-over-year GPV growth EUR (calculated) |
| 43 | YoY_GPV_Growth_lc | NUMERIC | sps_financial_metrics | Year-over-year GPV growth LC (calculated) |
| 44 | back_margin_amt_lc | FLOAT | sps_financial_metrics | Back margin (rebates) LC |
| 45 | back_margin_wo_dist_allowance_amt_lc | FLOAT | sps_financial_metrics | Back margin excluding distribution allowance LC |
| 46 | Total_Margin_LC | NUMERIC | sps_financial_metrics | Total margin (front + back) LC |
| 47 | Front_Margin_eur | NUMERIC | sps_financial_metrics | Front margin % EUR |
| 48 | Front_Margin_lc | NUMERIC | sps_financial_metrics | Front margin % LC |

## Line Rebate Metrics (6 fields)

| # | Field | Type | Source | Description | Ingredient |
|---|-------|------|--------|-------------|-----------|
| 49 | total_rebate | FLOAT | sps_line_rebate_metrics | Total rebate amount | |
| 50 | total_rebate_wo_dist_allowance_lc | FLOAT | sps_line_rebate_metrics | Total rebate without distribution allowance LC | |
| 51 | calc_gross_delivered | FLOAT | sps_line_rebate_metrics | Gross delivered quantity (ingredient) | ✓ |
| 52 | calc_gross_return | FLOAT | sps_line_rebate_metrics | Gross return quantity (ingredient) | ✓ |
| 53 | calc_net_delivered | FLOAT | sps_line_rebate_metrics | Net delivered quantity (ingredient) | ✓ |
| 54 | calc_net_return | FLOAT | sps_line_rebate_metrics | Net return quantity (ingredient) | ✓ |

## Efficiency Metrics (13 fields) ⭐ NEW adds 3 fields here

| # | Field | Type | Source | Description | Ingredient | Status |
|---|-------|------|--------|-------------|-----------|--------|
| 55 | sku_listed | INTEGER | sps_efficiency | Universe of listed SKUs (denominator for movers analysis) | | |
| 56 | sku_mature | INTEGER | sps_efficiency | Listed SKUs age >= 90 days (denominator for efficiency %) | | |
| 57 | sku_new | INTEGER | sps_efficiency | Listed SKUs age <= 30 days | | |
| 58 | sku_probation | INTEGER | sps_efficiency | Listed SKUs age 31-89 days | | |
| 59 | efficient_movers | INTEGER | sps_efficiency | Mature SKUs selling at/above category threshold | | |
| 60 | new_zero_movers | INTEGER | sps_efficiency | Non-mature SKUs with zero sales (AQS v7) | | ⭐ NEW |
| 61 | new_slow_movers | INTEGER | sps_efficiency | Non-mature SKUs with slow sales (AQS v7) | | ⭐ NEW |
| 62 | new_efficient_movers | INTEGER | sps_efficiency | Non-mature SKUs with efficient sales (AQS v7) | | ⭐ NEW |
| 63 | sold_items | INTEGER | sps_efficiency | Items sold (note: duplicates sps_financial_metrics, FLAT excludes) | | |
| 64 | gpv_eur | FLOAT | sps_efficiency | GPV EUR (denominator for weighted efficiency) | ✓ |
| 65 | numerator_new_avail | FLOAT | sps_efficiency | Weighted availability numerator (AQS v7). Tableau: SUM(num)/SUM(denom) | ✓ |
| 66 | denom_new_avail | FLOAT | sps_efficiency | Weighted availability denominator (AQS v7) | ✓ |
| 67 | weight_efficiency | FLOAT | sps_efficiency | GPV-weighted efficiency numerator (AQS v7). Tableau: SUM(weight_efficiency)/SUM(gpv_eur) | ✓ |

## Listed SKU Metrics (1 field)

| # | Field | Type | Source | Description |
|---|-------|------|--------|-------------|
| 68 | listed_skus | INTEGER | sps_listed_sku | Count of listed SKUs |

## Shrinkage Metrics (5 fields)

| # | Field | Type | Source | Description |
|---|-------|------|--------|-------------|
| 69 | spoilage_value_eur | FLOAT | sps_shrinkage | Spoilage value EUR |
| 70 | spoilage_value_lc | NUMERIC | sps_shrinkage | Spoilage value LC (multi-currency support) |
| 71 | retail_revenue_eur | FLOAT | sps_shrinkage | Retail revenue EUR |
| 72 | retail_revenue_lc | FLOAT | sps_shrinkage | Retail revenue LC |
| 73 | spoilage_rate | FLOAT | sps_shrinkage | Spoilage rate % (calculated: SUM(spoilage_value_eur) / SUM(retail_revenue_eur)) |

## Delivery Costs (2 fields)

| # | Field | Type | Source | Description |
|---|-------|------|--------|-------------|
| 74 | delivery_cost_eur | FLOAT | sps_delivery_costs | Delivery cost EUR |
| 75 | delivery_cost_local | FLOAT | sps_delivery_costs | Delivery cost LC |

## Purchase Order Metrics (12 fields)

| # | Field | Type | Source | Description | Debug |
|---|-------|------|--------|-------------|-------|
| 76 | on_time_orders | INTEGER | sps_purchase_order | Count of orders received on time (is_compliant=1) | |
| 77 | total_received_qty_per_po_order | INTEGER | sps_purchase_order | Total received quantity for compliant done orders | |
| 78 | total_demanded_qty_per_po_order | INTEGER | sps_purchase_order | Total demanded quantity for compliant done orders | |
| 79 | total_cancelled_po_orders | INTEGER | sps_purchase_order | Count of cancelled POs (supplier non-fulfillment, auto-cancel, quality, stock) | |
| 80 | total_non_cancelled__po_orders | INTEGER | sps_purchase_order | Count of non-cancelled compliant POs | |
| 81 | fill_rate | FLOAT | sps_purchase_order | Fill rate % (SAFE_DIVIDE: received / demanded for compliant orders) | |
| 82 | otd | FLOAT | sps_purchase_order | On-Time Delivery % (SAFE_DIVIDE: on_time / non_cancelled) | |
| 83 | supplier_non_fulfilled_order_qty | INTEGER | sps_purchase_order | Quantity from cancelled/rejected/stock POs | |
| 84 | total_po_orders | INTEGER | sps_purchase_order | Total POs without filters (debugging ingredient) | ✓ Debug |
| 85 | total_compliant_po_orders | INTEGER | sps_purchase_order | Total compliant POs (debugging ingredient) | ✓ Debug |
| 86 | total_received_qty_ALL | INTEGER | sps_purchase_order | Total received qty across ALL orders (debugging ingredient) | ✓ Debug |
| 87 | total_demanded_qty_ALL | INTEGER | sps_purchase_order | Total demanded qty across ALL orders (debugging ingredient) | ✓ Debug |

---

## Summary

- **Total: 87 fields**
- **Aggregation keys: 7**
- **Metric fields: 80**
- **Ingredient fields (for Tableau aggregation): 15** (marked with ✓)
- **Debug fields: 4** (marked with ✓ Debug in PO section)
- **NEW-only fields (vs FLAT): 3** (new_zero_movers, new_slow_movers, new_efficient_movers)

## Key Architectural Patterns

1. **All-keys UNION**: Keys come from UNION DISTINCT across all 9 source tables (ensures complete key coverage)
2. **Ingredients pattern**: Fields marked as "ingredient" are decomposed values meant for Tableau aggregation
   - Example: price_index_numerator + price_index_weight (not pre-calculated ratio)
   - Example: stock_value_eur + cogs_monthly_eur + days_in_month (not pre-calculated DOH)
   - Example: weight_efficiency + gpv_eur (not pre-calculated efficiency %)
3. **Multi-currency support**: EUR and LC variants for most financial metrics
4. **AQS v7 methodology**: New efficiency fields for non-mature SKU tracking (lines 60-62)
