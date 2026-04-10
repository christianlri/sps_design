# SPS SQL New | Fișiere Modificate

Acest folder conține fișierele originale (sps_originals) cu modificările critice documentate aplicate. **Doar 8 fișiere au fost modificate**, restul sunt copii fără modificări.

## 🔴 Fișiere de revizuit (8 total)

### Nivel 1: Critic (Revizuiți mai întâi)

#### 1. **sps_efficiency_month.sql**
- **Schimbare principală:** AQS v5 → AQS v2/v7
- **Sursă:** `_aqs_v5_sku_efficiency_detail` → `sku_efficiency_detail_v2`
- **Câmpuri eliminate:** date_diff, avg_qty_sold, new_availability
- **Câmpuri noi (8):** sku_efficiency (ENUM), updated_sku_age, available_hours, potential_hours, numerator_new_avail, denom_new_avail, sku_status, is_listed
- **Impact:** Toate tabelele downstream depind de aceasta

#### 2. **sps_efficiency.sql**
- **Schimbare principală:** Refactorizare de la 1 CTE → 3 CTE (sku_counts, efficiency_by_warehouse, combined)
- **Metrică nouă:** weight_efficiency (metodologie AQS v7 = efficient_skus / qualified_skus * gpv_eur)
- **Ingrediente noi:** numerator_new_avail, denom_new_avail, weight_efficiency
- **Impact:** Alimentează coloanele finale din sps_score_tableau

#### 3. **sps_score_tableau.sql**
- **Schimbare principală:** Arhitectură de la JOIN-uri secvențiale → model all_keys UNION
- **De ce:** Asigură acoperire a TUTUROR combinațiilor de chei (global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity) care există în oricare din cele 8 tabele sursă
- **Impact:** TABEL FINAL - schimbare de acoperire/completitudine

### Nivel 2: Important (Revizuiți al doilea)

#### 4. **sps_price_index.sql**
- **Ingrediente noi:** price_index_numerator, price_index_weight
- **Formula Tableau:** median_price_index = SUM(price_index_numerator) / SUM(price_index_weight)

#### 5. **sps_days_payable.sql**
- **Ingrediente noi:** stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
- **Formula Tableau:** doh_monthly = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))

#### 6. **sps_shrinkage.sql**
- **Schimbări:** Redenumire câmpuri (spoilage_value → spoilage_value_eur, retail_revenue → retail_revenue_eur)
- **Nou:** spoilage_value_lc, retail_revenue_lc (suport multi-valută)

#### 7. **sps_line_rebate_metrics.sql**
- **Ingrediente noi:** calc_net_delivered, calc_net_return
- **Bază pentru:** Calcule de rabat în Tableau (net_purchase = SUM(calc_net_delivered) - SUM(calc_net_return))

### Nivel 3: Debug (Revizuiți pentru validare)

#### 8. **sps_purchase_order.sql**
- **Câmpuri debug noi:** total_po_orders, total_compliant_po_orders, total_received_qty_ALL, total_demanded_qty_ALL
- **Utilizare:** Validare și debugging al agregărilor

## 📋 Fișiere neschimbate (nu revizuiți)

Următoarele fișiere sunt copii directe fără modificări:
- sps_product.sql
- sps_customer_order.sql
- sps_delivery_costs.sql
- sps_days_payable_month.sql (doar modificări de hardcoding)
- sps_delivery_costs_month.sql (doar modificări de hardcoding)
- sps_efficiency_simple.sql
- sps_financial_metrics.sql
- sps_financial_metrics_month.sql
- sps_financial_metrics_prev_year.sql
- sps_listed_sku.sql
- sps_listed_sku_month.sql
- sps_line_rebate_metrics_month.sql (doar modificări de hardcoding)
- sps_price_index_month.sql (doar modificări de hardcoding)
- sps_product.sql
- sps_purchase_order_month.sql (doar modificări de hardcoding)
- sps_shrinkage_month.sql (doar modificări de hardcoding)

## ✅ Recomandare pentru ordinea de revizuire

1. **Primul:** Citiți sps_efficiency_month.sql (înțelegeți schimbarea AQS v5→v7)
2. **Al doilea:** Citiți sps_efficiency.sql (înțelegeți 3 CTE + weight_efficiency)
3. **Al treilea:** Citiți sps_score_tableau.sql (înțelegeți all_keys UNION)
4. **Apoi:** Revizuiți sps_price_index, sps_days_payable, sps_shrinkage, sps_line_rebate_metrics (ingrediente)
5. **În final:** sps_purchase_order (câmpuri debug)

**Notă:** Toate schimbările au comentarii inline `-- NEW` sau `-- MODIFIED` pentru ușurință de urmărire.
