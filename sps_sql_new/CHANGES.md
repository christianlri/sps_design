# SPS SQL New | Archivos Modificados

Esta carpeta contiene los archivos originales (sps_originals) con los cambios críticos documentados aplicados. **Solo 8 archivos fueron modificados**, el resto son copias sin cambios.

## 🔴 Archivos a revisar (8 totales)

### Tier 1: Crítico (Revisar primero)

#### 1. **sps_efficiency_month.sql**
- **Cambio principal:** AQS v5 → AQS v2/v7
- **Fuente:** `_aqs_v5_sku_efficiency_detail` → `sku_efficiency_detail_v2`
- **Campos removidos:** date_diff, avg_qty_sold, new_availability
- **Campos nuevos (8):** sku_efficiency (ENUM), updated_sku_age, available_hours, potential_hours, numerator_new_avail, denom_new_avail, sku_status, is_listed
- **Impacto:** Todo lo downstream depende de esto

#### 2. **sps_efficiency.sql**
- **Cambio principal:** Refactor de 1 CTE → 3 CTEs (sku_counts, efficiency_by_warehouse, combined)
- **Nueva métrica:** weight_efficiency (AQS v7 methodology = efficient_skus / qualified_skus * gpv_eur)
- **Ingredientes nuevos:** numerator_new_avail, denom_new_avail, weight_efficiency
- **Impacto:** Alimenta sps_score_tableau finales

#### 3. **sps_score_tableau.sql**
- **Cambio principal:** Arquitectura JOINs secuenciales → all_keys UNION pattern
- **Por qué:** Garantiza cobertura de TODAS las key combinations (global_entity_id, time_period, brand_sup, entity_key, division_type, supplier_level, time_granularity) que existan en cualquiera de las 8 tablas source
- **Impacto:** TABLA FINAL - cambio de cobertura/completitud

### Tier 2: Importante (Revisar segundo)

#### 4. **sps_price_index.sql**
- **Ingredientes nuevos:** price_index_numerator, price_index_weight
- **Fórmula Tableau:** median_price_index = SUM(price_index_numerator) / SUM(price_index_weight)

#### 5. **sps_days_payable.sql**
- **Ingredientes nuevos:** stock_value_eur, cogs_monthly_eur, days_in_month, days_in_quarter
- **Fórmula Tableau:** doh_monthly = SUM(stock_value_eur) / (SUM(cogs_monthly_eur) / MAX(days_in_month))

#### 6. **sps_shrinkage.sql**
- **Cambios:** Rename fields (spoilage_value → spoilage_value_eur, retail_revenue → retail_revenue_eur)
- **Nuevos:** spoilage_value_lc, retail_revenue_lc (multi-currency support)

#### 7. **sps_line_rebate_metrics.sql**
- **Ingredientes nuevos:** calc_net_delivered, calc_net_return
- **Base para:** Cálculos de rebate en Tableau (net_purchase = SUM(calc_net_delivered) - SUM(calc_net_return))

### Tier 3: Debug (Revisar para validación)

#### 8. **sps_purchase_order.sql**
- **Debug fields nuevos:** total_po_orders, total_compliant_po_orders, total_received_qty_ALL, total_demanded_qty_ALL
- **Uso:** Validación y debugging de agregaciones

## 📋 Archivos sin cambios (no revisar)

Los siguientes archivos son copias directas sin modificaciones:
- sps_product.sql
- sps_customer_order.sql
- sps_delivery_costs.sql
- sps_days_payable_month.sql (solo cambios de hardcoding)
- sps_delivery_costs_month.sql (solo cambios de hardcoding)
- sps_financial_metrics.sql
- sps_financial_metrics_month.sql
- sps_financial_metrics_prev_year.sql
- sps_listed_sku.sql
- sps_listed_sku_month.sql
- sps_line_rebate_metrics_month.sql (solo cambios de hardcoding)
- sps_price_index_month.sql (solo cambios de hardcoding)
- sps_product.sql
- sps_purchase_order_month.sql (solo cambios de hardcoding)
- sps_shrinkage_month.sql (solo cambios de hardcoding)

## ✅ Recomendación para Ionut

1. **Primero:** Lee sps_efficiency_month.sql (entiende el cambio AQS v5→v7)
2. **Segundo:** Lee sps_efficiency.sql (entiende los 3 CTEs + weight_efficiency)
3. **Tercero:** Lee sps_score_tableau.sql (entiende el all_keys UNION)
4. **Luego:** Revisa sps_price_index, sps_days_payable, sps_shrinkage, sps_line_rebate_metrics (ingredientes)
5. **Finalmente:** sps_purchase_order (debug fields)

**Nota:** Todos los cambios tienen comentarios `-- NEW` o `-- MODIFIED` inline para facilitar el follow-up.
