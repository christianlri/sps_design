# SPS Score Tableau FLAT - 73 Fields (Current Production Version)

## Aggregation Keys (7 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 1 | global_entity_id | STRING | Identifier for the global entity (e.g., 'TB_KW' for Talabat Kuwait) |
| 2 | time_period | STRING | The reporting period (YYYY-MM-DD) for detail rows or Quarter-Year (Qx-YYYY) for roll-up rows |
| 3 | brand_sup | STRING | Unique identifier for the supplier entity, prioritized by Principal ID, then Division ID, then Brand Owner |
| 4 | entity_key | STRING | The specific aggregation key (Category L1/L2/L3 or Supplier ID) |
| 5 | division_type | STRING | Classification of the record: 'principal', 'division', or 'brand_owner' |
| 6 | supplier_level | STRING | The hierarchy level of the aggregation: 'level_three', 'level_two', 'level_one', or 'supplier' |
| 7 | time_granularity | STRING | Label indicating if the record is 'Monthly' or 'Quarterly' |

## Purchase Order Metrics (8 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 8 | on_time_orders | INTEGER | Count of purchase orders delivered within the requested time window |
| 9 | total_cancelled_po_orders | INTEGER | Total number of purchase orders that were cancelled |
| 10 | total_non_cancelled__po_orders | INTEGER | Count of non-cancelled compliant POs |
| 11 | total_received_qty_per_po_order | INTEGER | Total quantity of items physically received across all POs |
| 12 | total_demanded_qty_per_po_order | INTEGER | Total quantity of items originally requested/ordered from the supplier |
| 13 | fill_rate | FLOAT | Supplier fulfillment performance (Received Qty / Demanded Qty) |
| 14 | otd | FLOAT | On-Time Delivery percentage |
| 15 | supplier_non_fulfilled_order_qty | INTEGER | The quantity of items that were ordered but never received (Shortfall) |

## Financial Metrics (31 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 16 | total_customers | INTEGER | Count of unique analytical customers for the entity/period |
| 17 | total_skus_sold | INTEGER | Count of unique SKUs with recorded sales |
| 18 | total_orders | INTEGER | Total volume of unique customer orders |
| 19 | total_warehouses_sold | INTEGER | Count of warehouses that generated sales in this bucket |
| 20 | Total_Net_Sales_eur_order | NUMERIC | Sum of order-level net sales in Euros |
| 21 | Net_Sales_eur | NUMERIC | Sum of item-level net sales in Euros |
| 22 | COGS_eur | NUMERIC | Cost of Goods Sold in Euros |
| 23 | Net_Sales_from_promo_eur | NUMERIC | Sales generated from promotional items in Euros |
| 24 | total_supplier_funding_eur | NUMERIC | Total supplier funding contributions in Euros |
| 25 | front_margin_amt_eur | NUMERIC | Gross profit amount in Euros (Net Sales - COGS) |
| 26 | total_discount_eur | NUMERIC | Total customer discounts applied in Euros |
| 27 | Promo_GPV_contribution_eur | NUMERIC | Percentage of total sales coming from promotions in Euros |
| 28 | Total_Net_Sales_lc_order | NUMERIC | Sum of order-level net sales in local currency |
| 29 | Net_Sales_lc | NUMERIC | Sum of item-level net sales in local currency |
| 30 | COGS_lc | NUMERIC | Cost of Goods Sold in local currency |
| 31 | Net_Sales_from_promo_lc | NUMERIC | Sales generated from promotional items in local currency |
| 32 | total_supplier_funding_lc | NUMERIC | Total supplier funding contributions in local currency |
| 33 | front_margin_amt_lc | NUMERIC | Gross profit amount in local currency (Net Sales - COGS) |
| 34 | total_discount_lc | NUMERIC | Total customer discounts applied in local currency |
| 35 | Promo_GPV_contribution_lc | NUMERIC | Percentage of total sales coming from promotions in local currency |
| 36 | total_GBV | NUMERIC | Total Gross Basket Value in Euros |
| 37 | fulfilled_quantity | NUMERIC | Total number of units sold |
| 38 | Net_Sales_eur_Last_Year | NUMERIC | Net Sales from the equivalent period in the previous year (YoY Comparison) |
| 39 | Net_Sales_lc_Last_Year | NUMERIC | Net Sales from the equivalent period in the previous year in local currency |
| 40 | YoY_GPV_Growth_eur | NUMERIC | Growth percentage of sales in EUR compared to the previous year |
| 41 | YoY_GPV_Growth_lc | NUMERIC | Growth percentage of sales in LC compared to the previous year |
| 42 | back_margin_amt_lc | FLOAT | Total rebate/back-margin amount in local currency |
| 43 | back_margin_wo_dist_allowance_amt_lc | FLOAT | Back-margin excluding distribution allowances in local currency |
| 44 | Total_Margin_LC | NUMERIC | Combined margin percentage (Front + Back) in local currency |
| 45 | Front_Margin_eur | NUMERIC | Front margin percentage in Euros |
| 46 | Front_Margin_lc | NUMERIC | Front margin percentage in local currency |

## Line Rebate Metrics (2 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 47 | total_rebate | FLOAT | Total accumulated rebate value |
| 48 | total_rebate_wo_dist_allowance_lc | FLOAT | Total accumulated rebate excluding distribution allowances in local currency |

## Price Index Metrics (1 field)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 49 | median_price_index | FLOAT | Weighted median price index benchmarked against competitors |

## Days Payable Metrics (3 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 50 | payment_days | INTEGER | Maximum payment terms allowed for the supplier in days |
| 51 | doh | FLOAT | Days on Hand; calculated as stock value relative to COGS |
| 52 | dpo | FLOAT | Days Payable Outstanding; calculated as the gap between payment terms and inventory age |

## Time Reference (1 field)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 53 | last_year_time_period | STRING | Reference string for the same period in the previous year |

## Efficiency Metrics (10 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 54 | sku_listed | INTEGER | Number of distinct SKUs available in the catalog for this period |
| 55 | sku_mature | INTEGER | Count of SKUs active for >= 90 days |
| 56 | sku_probation | INTEGER | Count of SKUs active for 31-89 days |
| 57 | sku_new | INTEGER | Count of SKUs active for <= 30 days |
| 58 | zero_movers | INTEGER | Count of mature SKUs with no sales despite availability |
| 59 | la_zero_movers | INTEGER | Count of mature SKUs with no sales and low availability |
| 60 | slow_movers | INTEGER | Count of mature SKUs with low sales velocity (avg_qty < 1) |
| 61 | la_slow_movers | INTEGER | Count of mature SKUs with low sales velocity and poor availability |
| 62 | efficient_movers | INTEGER | Count of mature SKUs with high sales velocity (avg_qty >= 1) |
| 63 | new_zero_movers | INTEGER | Count of new SKUs with zero sales |
| 64 | new_slow_movers | INTEGER | Count of new SKUs with slow sales velocity |
| 65 | new_efficient_movers | INTEGER | Count of new/probation SKUs with high sales velocity |
| 66 | sold_items | NUMERIC | Total quantity of items sold |
| 67 | gpv_eur | FLOAT | Total Gross Product Value in Euros |

## Listed SKU Metrics (1 field)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 68 | listed_skus | INTEGER | Number of SKUs listed for the supplier |

## Shrinkage Metrics (3 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 69 | spoilage_value | FLOAT | Value of spoiled goods (non-currency-specific) |
| 70 | retail_revenue | FLOAT | Total retail revenue (non-currency-specific) |
| 71 | spoilage_rate | FLOAT | The rate of spoilage (calculated: SUM(spoilage_value) / SUM(retail_revenue)) |

## Delivery Costs (2 fields)

| # | Field | Type | Description |
|---|-------|------|-------------|
| 72 | delivery_cost_eur | FLOAT | Total delivery costs associated with the supplier's orders in Euros |
| 73 | delivery_cost_local | FLOAT | Total delivery costs associated with the supplier's orders in local currency |

---

## Summary

- **Total: 73 fields** (actual production schema)
- **Aggregation keys: 7**
- **Metric fields: 66**
- **Key groupings:**
  - PO metrics: 8 fields
  - Financial metrics: 31 fields (multi-currency: EUR + LC)
  - Efficiency metrics: 14 fields (includes mature + non-mature SKU tracking)
  - Shrinkage metrics: 3 fields (non-currency-specific)
  - Line rebate: 2 fields (aggregated only)
  - Price index: 1 field (deprecated median_price_index)
  - Days payable: 3 fields (deprecated doh/dpo)
  - Delivery costs: 2 fields
  - Listed SKUs: 1 field
  - Time reference: 1 field (last_year_time_period for YoY comparisons)

## Key Characteristics

1. **Deprecated fields present**: median_price_index, doh, dpo (calculated fields, not ingredients)
2. **No ingredient fields**: FLAT uses aggregated/calculated values directly, not decomposed for Tableau re-aggregation
3. **Efficiency at mature/non-mature levels**: Tracks mature SKU performance (zero_movers, slow_movers, efficient_movers) separately from non-mature (new_zero_movers, new_slow_movers, new_efficient_movers)
4. **Simple shrinkage**: Uses spoilage_value and retail_revenue without EUR/LC splits
5. **No debug fields**: No explicit debugging ingredients; audit/validation happens via code
6. **Currency pairs**: EUR + LC (local currency) variants for financial metrics, not for rebates or shrinkage

## Differences from NEW (87 fields)

| Aspect | FLAT (73) | NEW (87) |
|--------|-----------|----------|
| **Shrinkage** | spoilage_value, retail_revenue (2) | spoilage_value_eur, spoilage_value_lc, retail_revenue_eur, retail_revenue_lc (4) |
| **Ingredients** | None (pre-calculated) | 15 ingredient fields for Tableau aggregation |
| **Line rebate** | total_rebate, total_rebate_wo_dist_allowance_lc (2) | +4 ingredient fields (calc_gross_*, calc_net_*) |
| **Price index** | median_price_index only (1) | +2 ingredient fields (price_index_numerator, price_index_weight) |
| **Days payable** | payment_days, doh, dpo (3) | +4 ingredient fields (stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter) |
| **Efficiency** | mature + non-mature (14 total) | +3 availability ingredients (numerator_new_avail, denom_new_avail, weight_efficiency) |
| **Listed SKUs** | listed_skus (1) | +listed_skus_efficiency alias (2) |
| **Time reference** | last_year_time_period (1) | Removed in NEW |

**Net delta: +14 fields (73 → 87)**
