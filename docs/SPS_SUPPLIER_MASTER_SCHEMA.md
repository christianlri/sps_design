# SPS Supplier Master - Complete Field Schema

**Table**: `dh-darkstores-live.csm_automated_tables.sps_supplier_master`
**Rows**: ~182,745
**Updated**: Daily (pipeline execution)
**Clustering**: `(global_entity_id, time_period)`

---

## Identity Fields (7 fields)

### `global_entity_id`
- **Type**: `STRING`
- **Source**: `sps_score_tableau`
- **Description**: Regional entity identifier (2-letter prefix + country code)
- **Examples**: `FP_SG`, `TB_AE`, `PY_AR`, `GV_ES`
- **Cardinality**: 19 unique values
- **Nullable**: No

### `time_period`
- **Type**: `STRING` (YYYYMM format)
- **Source**: `sps_score_tableau`
- **Description**: Year-month identifier for aggregation period
- **Format**: `202604` (April 2026)
- **Range**: 24 months rolling window
- **Nullable**: No

### `time_granularity`
- **Type**: `STRING`
- **Source**: `sps_score_tableau`
- **Description**: Temporal aggregation level
- **Fixed Values**: `'Monthly'` (current)
- **Nullable**: No

### `division_type`
- **Type**: `STRING`
- **Source**: `sps_score_tableau`
- **Description**: Organizational hierarchy level
- **Allowed Values**: `'division'`, `'principal'`
- **Note**: Pre-filtered in query; both include supplier performance data
- **Nullable**: No

### `supplier_level`
- **Type**: `STRING`
- **Source**: `sps_score_tableau`
- **Description**: Classification of supplier in hierarchy
- **Fixed Value**: `'supplier'` (pre-filtered)
- **Nullable**: No

### `entity_key`
- **Type**: `STRING`
- **Source**: `sps_score_tableau`
- **Description**: Supplier identifier within entity
- **Examples**: `12345`, `67890`
- **Note**: If NULL, defaults to 'Unknown' in supplier_name
- **Nullable**: Yes (rare)

### `brand_sup`
- **Type**: `STRING`
- **Source**: `sps_score_tableau`
- **Description**: Brand classification of supplier (often same as supplier_name)
- **Examples**: `'Supplier A'`, `'Brand X'`
- **Nullable**: Yes

---

## Supplementary Fields (2 fields)

### `supplier_name`
- **Type**: `STRING`
- **Source**: `sps_product` (via sps_product_clean LEFT JOIN)
- **Description**: Human-readable supplier name
- **Examples**: `'DKSH Singapore Pte Ltd'`, `'Coca Cola distributor'`
- **Fallback**: `entity_key` if not found in sps_product
- **Default**: `'Unknown'` if both missing
- **Nullable**: No (zero NULLs guaranteed)
- **Match Rate**: 95% across entities

### `global_entity_name`
- **Type**: `STRING`
- **Source**: Mapped from `global_entity_id` prefix
- **Description**: Brand/platform name
- **Allowed Values**:
  - `'PedidosYa'` (PY prefix)
  - `'HungerStation'` (HS prefix)
  - `'Talabat'` (TB, HF prefixes)
  - `'Pandora'` (FP, FD, YS, NP prefixes)
  - `'Glovo'` (GV prefix)
  - `'Instashop'` (IN prefix)
  - `'Unknown'` (fallback)
- **Nullable**: No

---

## Financial Base (13 fields)

### `Net_Sales_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total net sales in local currency
- **Unit**: Currency varies by entity (SGD, AED, ARG, EUR, etc.)
- **Range**: 0 to 50,000,000+
- **Nullable**: Yes
- **Note**: Used in ratio calculations; SUM aggregated

### `Net_Sales_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM, currency-converted)
- **Description**: Total net sales in EUR (standardized currency)
- **Unit**: EUR
- **Range**: 0 to 10,000,000+
- **Nullable**: Yes
- **Note**: Used for weighted score calculations (wscore_denom)

### `Net_Sales_lc_Last_Year`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Net sales in local currency for same period last year
- **Purpose**: YoY growth calculation
- **Nullable**: Yes

### `Net_Sales_eur_Last_Year`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Net sales in EUR for same period last year
- **Nullable**: Yes

### `COGS_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Cost of Goods Sold in local currency
- **Unit**: Local currency
- **Range**: 0 to 40,000,000+
- **Nullable**: Yes
- **Note**: Used in net profit margin calculation

### `front_margin_amt_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Retail/customer-facing margin amount in local currency
- **Unit**: Local currency
- **Nullable**: Yes
- **Note**: Component of front_margin_ratio; includes platform rebates

### `back_margin_amt_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Supplier rebate/margin amount in local currency
- **Unit**: Local currency
- **Nullable**: Yes
- **Note**: Used in ratio_back_margin; score_back_margin (0-25 pts)

### `total_supplier_funding_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total funding/subsidies provided by supplier in local currency
- **Unit**: Local currency
- **Nullable**: Yes
- **Note**: Component of front_margin_ratio; non-rebate margin

### `total_discount_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total discounts given to customer in local currency
- **Unit**: Local currency
- **Nullable**: Yes
- **Note**: Used to calculate ratio_gbd (Gross Basket Discount)

### `Net_Sales_from_promo_lc`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Net sales attributed to promotional activity in local currency
- **Unit**: Local currency
- **Nullable**: Yes
- **Note**: Used in ratio_promo_contribution

### `total_orders`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total number of orders placed by customers to this supplier
- **Range**: 0 to 100,000+
- **Nullable**: Yes
- **Note**: Used in ratio_frequency, ratio_abv

### `total_customers`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total unique customers who purchased from this supplier
- **Range**: 0 to 50,000+
- **Nullable**: Yes
- **Note**: Used in ratio_frequency, ratio_customer_penetration

### `total_market_customers`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (MAX)
- **Description**: Total customers in the platform (market size)
- **Range**: 100,000 to 10,000,000
- **Nullable**: Yes
- **Note**: Used in ratio_customer_penetration (denominator)

### `Total_Net_Sales_lc_order`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Average Basket Value (ABV) = total sales / orders
- **Unit**: Local currency per order
- **Nullable**: Yes
- **Note**: Already aggregated; used in ratio_abv

---

## Efficiency Ingredients (2 fields)

### `weight_efficiency`
- **Type**: `NUMERIC`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Aggregated weight efficiency score
- **Formula**: SUM(orders.weight * efficiency_score)
- **Note**: Ingredient for ratio_efficiency
- **Nullable**: Yes

### `gpv_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Gross Product Value in EUR (denominator for efficiency)
- **Unit**: EUR
- **Nullable**: Yes
- **Note**: Denominator in ratio_efficiency = weight_efficiency / gpv_eur

---

## Price Index Ingredients (2 fields)

### `price_index_numerator`
- **Type**: `NUMERIC`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Aggregated price weighting numerator
- **Note**: Ingredient for ratio_price_index
- **Nullable**: Yes

### `price_index_weight`
- **Type**: `NUMERIC`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total weight for price index calculation
- **Note**: Denominator in ratio_price_index = numerator / weight
- **Nullable**: Yes

---

## Purchase Order Ingredients (5 fields)

### `on_time_orders`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Count of orders delivered on or before promised time
- **Range**: 0 to total_non_cancelled_po_orders
- **Nullable**: Yes
- **Note**: Numerator in ratio_otd

### `total_non_cancelled_po_orders`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total purchase orders not cancelled
- **Range**: 0 to 100,000+
- **Nullable**: Yes
- **Note**: Denominator in ratio_otd

### `total_received_qty`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM of total_received_qty_per_po_order)
- **Description**: Total quantity of items received from supplier
- **Unit**: Items
- **Nullable**: Yes
- **Note**: Numerator in ratio_fill_rate

### `total_demanded_qty`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM of total_demanded_qty_per_po_order)
- **Description**: Total quantity of items demanded/ordered
- **Unit**: Items
- **Nullable**: Yes
- **Note**: Denominator in ratio_fill_rate

### `total_cancelled_po_orders`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Count of cancelled purchase orders
- **Range**: 0 to total_non_cancelled_po_orders
- **Nullable**: Yes

---

## Listed SKUs (1 field)

### `listed_skus`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (MAX)
- **Description**: Number of distinct products/SKUs this supplier maintains
- **Range**: 0 to 10,000+
- **Nullable**: Yes
- **Note**: MAX used (SKU count doesn't aggregate)

---

## Shrinkage (2 fields)

### `spoilage_value_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Value of spoiled/damaged goods in EUR
- **Unit**: EUR
- **Range**: 0 to 1,000,000+
- **Nullable**: Yes
- **Note**: Numerator in ratio_spoilage

### `retail_revenue_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total retail revenue in EUR (denominator for spoilage %)
- **Unit**: EUR
- **Nullable**: Yes
- **Note**: Denominator in ratio_spoilage

---

## Days Payable Ingredients (3 fields)

### `stock_value_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Inventory value of this supplier's stock in EUR
- **Unit**: EUR
- **Range**: 0 to 5,000,000+
- **Nullable**: Yes
- **Note**: Numerator in ratio_doh (Days On Hand)

### `cogs_monthly_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Cost of goods sold per day in EUR (monthly average)
- **Unit**: EUR per day
- **Nullable**: Yes
- **Note**: Used in ratio_doh = stock_value / (cogs_monthly / days_in_month)

### `days_in_month`
- **Type**: `INT64`
- **Source**: `sps_score_tableau` (MAX)
- **Description**: Number of days in the reporting period
- **Fixed Value**: 28-31 (depending on month)
- **Nullable**: Yes

---

## Delivery Costs (2 fields)

### `delivery_cost_eur`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total delivery cost in EUR
- **Unit**: EUR
- **Range**: 0 to 500,000+
- **Nullable**: Yes

### `delivery_cost_local`
- **Type**: `NUMERIC`
- **Scale**: 2 decimal places
- **Source**: `sps_score_tableau` (SUM)
- **Description**: Total delivery cost in local currency
- **Unit**: Local currency
- **Nullable**: Yes
- **Note**: Used in ratio_delivery_cost (as percentage of Net_Sales_lc)

---

## Calculated Ratios (16 fields)

All ratios are calculated via `SAFE_DIVIDE()` and rounded to 4 decimal places.

### `ratio_otd` (On Time Delivery)
- **Type**: `NUMERIC` (0.0000 - 1.0000)
- **Formula**: `on_time_orders / total_non_cancelled_po_orders`
- **Range**: 0.0 to 1.0
- **Description**: Percentage of orders delivered on time
- **Used In**: score_otd (max 40 pts)

### `ratio_fill_rate`
- **Type**: `NUMERIC` (0.0000 - 1.0000)
- **Formula**: `total_received_qty / total_demanded_qty`
- **Range**: 0.0 to 1.0
- **Description**: Percentage of requested items fulfilled
- **Used In**: score_fill_rate (max 60 pts)

### `ratio_efficiency`
- **Type**: `NUMERIC` (0.0000+)
- **Formula**: `weight_efficiency / gpv_eur`
- **Range**: 0.0 to 3.0+
- **Description**: Weight efficiency per EUR of gross product value
- **Used In**: score_efficiency (max 30 pts)
- **Threshold**: 0.40-1.0 range

### `ratio_price_index`
- **Type**: `NUMERIC` (0.0000+)
- **Formula**: `price_index_numerator / price_index_weight`
- **Range**: 0.0 to 2.0
- **Description**: Price index vs market basket
- **Note**: >1.0 means more expensive than average

### `ratio_yoy` (Year-over-Year growth)
- **Type**: `NUMERIC` (-1.0000 to 5.0000+)
- **Formula**: `(Net_Sales_lc - Net_Sales_lc_Last_Year) / Net_Sales_lc_Last_Year`
- **Range**: -100% to +500%+
- **Description**: Annual percentage growth
- **Used In**: score_yoy (max 10 pts, market-relative)
- **Note**: Negative = decline, >0 = growth

### `ratio_back_margin`
- **Type**: `NUMERIC` (-0.5000 to 1.0000)
- **Formula**: `back_margin_amt_lc / Net_Sales_lc`
- **Range**: -50% to 100%
- **Description**: Back margin as % of sales
- **Used In**: score_back_margin (max 25 pts)
- **Note**: Often 0 if supplier has no rebate

### `ratio_front_margin`
- **Type**: `NUMERIC` (0.0000 to 1.0000)
- **Formula**: `(front_margin_amt_lc + total_supplier_funding_lc) / Net_Sales_lc`
- **Range**: 0% to 100%
- **Description**: Front margin (retail + funding) as % of sales
- **Used In**: score_front_margin (max 15 pts)
- **Threshold**: 0.12-0.70 range (12%-70%)

### `ratio_gbd` (Gross Basket Discount)
- **Type**: `NUMERIC` (0.0000 to 0.5000)
- **Formula**: `total_discount_lc / (Net_Sales_lc + total_discount_lc)`
- **Range**: 0% to 50%
- **Description**: Discount as % of pre-discount revenue
- **Used In**: score_gbd (max 20 pts, bell curve)
- **Asymmetric Scoring**: Penalizes very low AND very high discounts

### `ratio_promo_contribution`
- **Type**: `NUMERIC` (0.0000 to 1.0000)
- **Formula**: `Net_Sales_from_promo_lc / Net_Sales_lc`
- **Range**: 0% to 100%
- **Description**: Percentage of sales from promotional activities
- **Note**: Informational; not used in scoring

### `ratio_abv` (Average Basket Value)
- **Type**: `NUMERIC` (0.0000+)
- **Formula**: `Total_Net_Sales_lc_order / total_orders`
- **Unit**: Local currency per order
- **Range**: 0.0 to 1,000,000+
- **Description**: Average value per order
- **Used In**: Segmentation (productivity component)

### `ratio_frequency`
- **Type**: `NUMERIC` (0.0000+)
- **Formula**: `total_orders / total_customers`
- **Range**: 0.0 to 100.0+
- **Description**: Average orders per customer
- **Used In**: Segmentation (productivity, freq_score)

### `ratio_customer_penetration`
- **Type**: `NUMERIC` (0.0000 to 100.0000)
- **Formula**: `(total_customers / total_market_customers) * 100`
- **Range**: 0.0 to 100.0 (%)
- **Description**: Percentage of market customers served
- **Used In**: Segmentation (penetration_score, PRIMARY signal r=0.2718)

### `ratio_spoilage`
- **Type**: `NUMERIC` (0.0000 to 1.0000)
- **Formula**: `spoilage_value_eur / retail_revenue_eur`
- **Range**: 0% to 100%
- **Description**: Spoilage/waste as % of retail revenue
- **Note**: Lower is better; quality metric

### `ratio_doh` (Days On Hand)
- **Type**: `NUMERIC` (0.0 to 1000.0)
- **Formula**: `stock_value_eur / (cogs_monthly_eur / days_in_month)`
- **Unit**: Days
- **Range**: 0 to 365+
- **Description**: Number of days of inventory available
- **Note**: High = slow-moving inventory; low = high turnover

### `ratio_delivery_cost`
- **Type**: `NUMERIC` (0.0000 to 1.0000)
- **Formula**: `delivery_cost_local / Net_Sales_lc`
- **Range**: 0% to 100%
- **Description**: Delivery cost as % of sales
- **Note**: Cost efficiency metric

### `ratio_net_profit_margin`
- **Type**: `NUMERIC` (-1.0000 to 1.0000)
- **Formula**: `(Net_Sales_lc + total_supplier_funding_lc - COGS_lc + back_margin_amt_lc) / Net_Sales_lc`
- **Range**: -100% to 100%
- **Description**: Net profit margin including all margins and funding
- **Note**: Comprehensive profitability metric

---

## Individual Scores (7 fields)

All scores are calculated in `sps_supplier_scoring` table and joined here.

### `score_fill_rate`
- **Type**: `NUMERIC` (0.00 to 60.00)
- **Source**: `sps_supplier_scoring`
- **Description**: Fill rate performance score (max 60 points)
- **Formula**: `MIN(ratio_fill_rate, 1.0) * 60`
- **Range**: 0 to 60

### `score_otd`
- **Type**: `NUMERIC` (0.00 to 40.00)
- **Source**: `sps_supplier_scoring`
- **Description**: On-time delivery performance score (max 40 points)
- **Formula**: `MIN(ratio_otd, 1.0) * 40`
- **Range**: 0 to 40

### `score_yoy`
- **Type**: `NUMERIC` (0.00 to 10.00)
- **Source**: `sps_supplier_scoring`
- **Description**: Year-over-year growth score (max 10 points)
- **Formula**: Market-relative: `(ratio_yoy / market_yoy_max) * 10`
- **Range**: 0 to 10
- **Note**: Benchmarked against market YoY in `sps_market_yoy`

### `score_efficiency`
- **Type**: `NUMERIC` (0.00 to 30.00)
- **Source**: `sps_supplier_scoring`
- **Description**: Weight efficiency score (max 30 points)
- **Range**: 0 to 30
- **Threshold**: 0.40-1.0 (below 0.40 = 0 pts, above 1.0 = 30 pts)

### `score_gbd`
- **Type**: `NUMERIC` (0.00 to 20.00)
- **Source**: `sps_supplier_scoring`
- **Description**: Gross Basket Discount score (max 20 points)
- **Range**: 0 to 20
- **Scoring**: Asymmetric bell curve (peaks at target, penalizes extremes)

### `score_back_margin`
- **Type**: `NUMERIC` (0.00 to 25.00)
- **Source**: `sps_supplier_scoring`
- **Description**: Back margin (rebate) score (max 25 points)
- **Range**: 0 to 25
- **Threshold**: Requires `has_rebate = 1`
- **Note**: 0 if no rebate; linear between bm_starting and bm_ending

### `score_front_margin`
- **Type**: `NUMERIC` (0.00 to 15.00)
- **Source**: `sps_supplier_scoring`
- **Description**: Front margin score (max 15 points)
- **Range**: 0 to 15
- **Threshold**: 0.12-0.70 (12%-70% optimal range)
- **Linear Scaling**: Within range

---

## Aggregate Scores (3 fields)

### `operations_score`
- **Type**: `NUMERIC` (0.00 to 100.00)
- **Formula**: `score_fill_rate + score_otd`
- **Range**: 0 to 100
- **Description**: Operational excellence score (supply chain focus)
- **Components**: 2 metrics (fill_rate 60 pts + otd 40 pts)

### `commercial_score`
- **Type**: `NUMERIC` (0.00 to 95.00)
- **Formula**: `score_yoy + score_efficiency + score_gbd + score_back_margin + score_front_margin`
- **Range**: 0 to 95
- **Description**: Commercial partnership score (profitability & growth)
- **Components**: 5 metrics (yoy 10 + efficiency 30 + gbd 20 + back_margin 25 + front_margin 15)

### `total_score`
- **Type**: `NUMERIC` (0.00 to 97.50)
- **Formula**: `(operations_score + commercial_score) / 2`
- **Range**: 0 to 97.5
- **Description**: Overall supplier performance score (equal weight)
- **Note**: 50% ops, 50% commercial
- **Interpretation**: 
  - 80-97: Excellent
  - 60-80: Good
  - 40-60: Fair
  - 20-40: Poor
  - 0-20: Critical

---

## Thresholds (8 fields)

All thresholds are calculated dynamically in `sps_scoring_params` and joined here for explainability.

### `threshold_yoy_max`
- **Type**: `NUMERIC`
- **Description**: Maximum YoY growth threshold for score_yoy
- **Formula**: `LEAST(GREATEST(market_yoy_lc * 1.2, 0.20), 0.70)`
- **Range**: 0.20 to 0.70
- **Purpose**: Normalize score_yoy against market growth

### `threshold_bm_start`
- **Type**: `NUMERIC`
- **Description**: Back margin starting threshold (minimum to score points)
- **Formula**: p25 of suppliers with rebate
- **Range**: Varies by entity (0.05 to 0.20)
- **Purpose**: Dynamic floor for bm scoring

### `threshold_bm_end`
- **Type**: `NUMERIC`
- **Description**: Back margin ending threshold (max points achieved)
- **Formula**: Blend of (IQR_mean * 1.5 + p75) / 2, capped at 0.70
- **Range**: Varies by entity (0.15 to 0.70)
- **Purpose**: Scaling range for score_back_margin

### `threshold_fm_start`
- **Type**: `NUMERIC`
- **Description**: Front margin starting threshold (minimum to score points)
- **Formula**: MAX(0.12, p25 of suppliers with fm_ratio > 0)
- **Range**: 0.12 to 0.30
- **Purpose**: Floor at 12% minimum; dynamic p25 upper bound

### `threshold_fm_end`
- **Type**: `NUMERIC`
- **Description**: Front margin ending threshold (max points achieved)
- **Formula**: Blend of (IQR_mean * 1.25 + p75) / 2, capped at 0.70
- **Range**: Varies by entity (0.20 to 0.70)
- **Purpose**: Scaling range for score_front_margin

### `threshold_gbd_target`
- **Type**: `NUMERIC`
- **Description**: Gross Basket Discount target for this entity
- **Source**: Hardcoded by global_entity_id
- **Examples**: FP_SG=0.130, TB_AE=0.080, PY_AR=0.150
- **Range**: 0.034 to 0.200
- **Purpose**: Peak of asymmetric bell curve for score_gbd

### `threshold_gbd_lower`
- **Type**: `NUMERIC`
- **Formula**: `gbd_target * 0.5`
- **Range**: 0.017 to 0.100
- **Purpose**: Lower bound (scores 0 below this)

### `threshold_gbd_upper`
- **Type**: `NUMERIC`
- **Formula**: `gbd_target * 2.0`
- **Range**: 0.068 to 0.400
- **Purpose**: Upper bound (scores 0 above this)

---

## Segmentation (6 fields)

All segmentation fields are calculated in `sps_supplier_segmentation` and joined here.

### `segment_lc`
- **Type**: `STRING`
- **Source**: `sps_supplier_segmentation`
- **Description**: BCG-style segment classification
- **Allowed Values**: `'Key Accounts'`, `'Standard'`, `'Niche'`, `'Long Tail'`
- **Axes**: 
  - X: importance_score_lc (profitability)
  - Y: productivity_score_lc (growth/efficiency)
- **Thresholds**: 
  - Key Accounts: importance > 15 AND productivity >= 40
  - Standard: importance > 15 AND productivity < 40
  - Niche: importance <= 15 AND productivity >= 40
  - Long Tail: importance <= 15 AND productivity < 40

### `importance_score_lc`
- **Type**: `NUMERIC` (0.00 to 100.00)
- **Formula**: Percentile rank of net_profit_lc within entity
- **Range**: 0 to 100
- **Description**: Importance to platform based on profitability
- **Percentile**: p15 = 0, p95 = 100, linear between

### `productivity_score_lc`
- **Type**: `NUMERIC` (0.00 to 100.00)
- **Formula**: `(abv_score_lc + frequency_score + customer_penetration_score)`
- **Range**: 0 to 100
- **Description**: Supplier productivity (operations & customer impact)
- **Components**: 3 metrics (abv 30 + frequency 30 + penetration 40)

### `abv_score_lc`
- **Type**: `NUMERIC` (0.00 to 30.00)
- **Formula**: Percentile rank of abv_lc_order, scaled to 30 points
- **Range**: 0 to 30
- **Description**: Average Basket Value performance
- **Note**: Higher ABV = premium/specialty supplier

### `frequency_score`
- **Type**: `NUMERIC` (0.00 to 30.00)
- **Formula**: Percentile rank of frequency, scaled to 30 points
- **Range**: 0 to 30
- **Description**: Customer repeat purchase rate
- **Note**: Higher frequency = loyalty/stickiness

### `customer_penetration_score`
- **Type**: `NUMERIC` (0.00 to 40.00)
- **Formula**: Percentile rank of customer_penetration, scaled to 40 points
- **Range**: 0 to 40
- **Description**: Market reach (% of platform customers served)
- **Weight**: 40 points (PRIMARY signal, r=0.2718)
- **Note**: Strongest predictor of overall performance

---

## Weighted Scores (11 fields)

All weighted score fields are calculated here for correct Tableau aggregation.

### `wscore_num_fill_rate`
- **Type**: `NUMERIC`
- **Formula**: `score_fill_rate * Net_Sales_eur`
- **Purpose**: Numerator for weighted average in Tableau
- **Aggregation**: `SUM(wscore_num_fill_rate) / SUM(wscore_denom)` = weighted avg

### `wscore_num_otd`
- **Type**: `NUMERIC`
- **Formula**: `score_otd * Net_Sales_eur`

### `wscore_num_yoy`
- **Type**: `NUMERIC`
- **Formula**: `score_yoy * Net_Sales_eur`

### `wscore_num_efficiency`
- **Type**: `NUMERIC`
- **Formula**: `score_efficiency * Net_Sales_eur`

### `wscore_num_gbd`
- **Type**: `NUMERIC`
- **Formula**: `score_gbd * Net_Sales_eur`

### `wscore_num_back_margin`
- **Type**: `NUMERIC`
- **Formula**: `score_back_margin * Net_Sales_eur`

### `wscore_num_front_margin`
- **Type**: `NUMERIC`
- **Formula**: `score_front_margin * Net_Sales_eur`

### `wscore_num_operations`
- **Type**: `NUMERIC`
- **Formula**: `operations_score * Net_Sales_eur`

### `wscore_num_commercial`
- **Type**: `NUMERIC`
- **Formula**: `commercial_score * Net_Sales_eur`

### `wscore_num_total`
- **Type**: `NUMERIC`
- **Formula**: `total_score * Net_Sales_eur`

### `wscore_denom`
- **Type**: `NUMERIC`
- **Value**: `Net_Sales_eur`
- **Purpose**: Common denominator for all weighted averages
- **Note**: EUR standardization prevents LCY currency effects

---

## Typical Tableau Usage

```sql
-- Weighted average score by entity
SELECT
  global_entity_id,
  global_entity_name,
  SUM(wscore_num_total) / SUM(wscore_denom) AS weighted_avg_total_score,
  COUNT(DISTINCT supplier_name) AS supplier_count
GROUP BY global_entity_id, global_entity_name
ORDER BY weighted_avg_total_score DESC
```

```sql
-- Segmentation distribution
SELECT
  global_entity_name,
  segment_lc,
  COUNT(*) AS supplier_count,
  ROUND(AVG(total_score), 2) AS avg_score,
  ROUND(SUM(Net_Sales_eur), 0) AS total_sales_eur
GROUP BY global_entity_name, segment_lc
ORDER BY global_entity_name, segment_lc
```

---

## Key Metrics Summary

| Metric | Max Points | Weight | Component | Notes |
|--------|-----------|---------|-----------|-------|
| Fill Rate | 60 | 31.0% | Operations | Supply completeness |
| OTD | 40 | 20.6% | Operations | Delivery timeliness |
| YoY | 10 | 5.2% | Commercial | Growth trajectory |
| Efficiency | 30 | 15.5% | Commercial | Weight per EUR value |
| GBD | 20 | 10.3% | Commercial | Asymmetric (optimal target) |
| Back Margin | 25 | 12.9% | Commercial | Rebate performance |
| Front Margin | 15 | 7.7% | Commercial | Funding strategy |
| **TOTAL** | **97.5** | **100%** | | Normalized to /2 = 48.75 avg |

---

## Data Quality Notes

- **Nullability**: Most fields nullable (SAFE_DIVIDE ensures no NaN)
- **Aggregation**: All amounts are SUM except MAX for market-level and SKU counts
- **Currency**: LCY for local calculations, EUR for standardized weights
- **Updates**: Daily refresh; 24-month rolling window
- **Cardinality**: ~182,745 rows (suppliers × periods)
- **Clustering**: Optimized for (global_entity_id, time_period) queries

---

## Version History

| Date | Change | Author |
|------|--------|--------|
| 2026-04-27 | Initial schema documentation | Claude |
| 2026-04-27 | Added global_entity_name mapping | Claude |
| 2026-04-27 | Fixed wscore to use Net_Sales_eur | Claude |
