# Análisis de Parámetros en sps_sql_new

## 1. Parámetros Jinja2 (Template Variables)

Todos los archivos SQL en `sps_sql_new` usan parámetros Jinja2 para inyección dinámica. Se necesita un sistema de orquestación (Airflow, Cloud Composer, etc.) que proporcione estos valores.

### 1.1 Parámetros Principales

| Parámetro | Tipo | Uso | Ejemplo |
|-----------|------|-----|---------|
| `{{ params.project_id }}` | STRING | Project ID de BigQuery donde se crean las tablas | `fulfillment-dwh-development` |
| `{{ params.dataset.cl }}` | STRING | Dataset destino para todas las tablas SPS | `csm_automated_tables` |
| `{{ params.dataset.curated_data_shared_salesforce_srm }}` | STRING | Dataset fuente de datos curados Salesforce SRM | `curated_data_shared_salesforce_srm` |
| `{{ params.dataset.curated_data_shared_supply_chain }}` | STRING | Dataset fuente de datos curados Supply Chain | `curated_data_shared_supply_chain` |

**Usado en:**
- [sps_score_tableau_init.sql:3-36](sps_sql_new/sps_score_tableau_init.sql#L3-L36) - Tabla final consolidada
- [sps_supplier_hierarchy.sql:4,20,33,66,72,76](sps_sql_new/sps_supplier_hierarchy.sql#L4) - Jerarquía de proveedores
- [sps_purchase_order.sql:3-4,59](sps_sql_new/sps_purchase_order.sql#L3-L4) - Métricas de órdenes
- Todos los demás archivos de métricas

---

## 2. Parámetros Lógicos (Business Logic Parameters)

### 2.1 Parámetros de Granularidad Temporal

Definidos en CTEs `date_config`, se usan para controlar el rango histórico de datos.

| Parámetro | Lógica | Valor Actual | Propósito |
|-----------|--------|--------------|----------|
| `lookback_limit` | `DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER)` | 4 trimestres atrás | Limita datos a últimos 4 trimestres (1 año) |
| `time_granularity` | CASE statement | `'Monthly'` o `'Quarterly'` | Define si la fila es mensual o trimestral |
| `time_period` | CASE statement | `YYYYMM` (monthly) o `Q#-YYYY` (quarterly) | Identificador temporal único |
| `last_year_time_period` | Derivado | Same period año anterior | Comparación YoY |

**Archivos afectados:**
- [sps_efficiency.sql:10-12,154,187-189](sps_sql_new/sps_efficiency.sql#L10-L12)
- [sps_financial_metrics.sql:71-72](sps_sql_new/sps_financial_metrics.sql#L71-L72)
- [sps_purchase_order.sql:9-12,15,46](sps_sql_new/sps_purchase_order.sql#L9-L12)
- [sps_days_payable.sql](sps_sql_new/sps_days_payable.sql) (análogo)

---

### 2.2 Parámetros de Segmentación (Granularity Levels)

Controlan a qué nivel se agreguen los datos. Son permutaciones de GROUPING SETS en BigQuery.

#### **Division Type** (4 niveles + total)
```
CASE WHEN GROUPING(principal_supplier_id) = 0 THEN 'principal' 
     WHEN GROUPING(supplier_id) = 0 THEN 'division'
     WHEN GROUPING(brand_owner_name) = 0 THEN 'brand_owner'
     WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
     ELSE 'total'
END
```

| division_type | Dimensión | Descripción |
|---------------|-----------|-------------|
| `principal` | Principal Supplier ID | Nivel de proveedor principal (raíz) |
| `division` | Supplier ID | División/proveedor directo |
| `brand_owner` | Brand Owner Name | Propietario de marca |
| `brand_name` | Brand Name | Marca individual |
| `total` | (ninguno) | Agregado total sin segmentación |

#### **Supplier Level** (5 niveles)
```
CASE WHEN GROUPING(l3_master_category) = 0 THEN 'level_three'
     WHEN GROUPING(l2_master_category) = 0 THEN 'level_two'
     WHEN GROUPING(l1_master_category) = 0 THEN 'level_one'
     WHEN GROUPING(brand_name) = 0 THEN 'brand_name'
     ELSE 'supplier'
END
```

| supplier_level | Dimensión | Descripción |
|---|---|---|
| `level_three` | L3 Master Category | Categoría más específica |
| `level_two` | L2 Master Category | Categoría intermedia |
| `level_one` | L1 Master Category | Categoría general |
| `brand_name` | Brand Name | Por marca |
| `supplier` | (ninguno) | Nivel de proveedor puro |

#### **Brand & Supplier ID** (brand_sup)
```
CASE WHEN GROUPING(principal_supplier_id) = 0 THEN principal_supplier_id
     WHEN GROUPING(supplier_id) = 0 THEN supplier_id
     WHEN GROUPING(brand_owner_name) = 0 THEN brand_owner_name
     WHEN GROUPING(brand_name) = 0 THEN brand_name
     ELSE 'total'
END
```
Clave compuesta que representa: "¿A quién se le atribuye esta métrica?"

#### **Entity Key** (Jerarquía fallback)
```
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  IF(GROUPING(brand_name) = 0, brand_name, NULL),
  IF(GROUPING(brand_owner_name) = 0, brand_owner_name, NULL),
  IF(GROUPING(supplier_id) = 0, supplier_id, NULL),
  principal_supplier_id
)
```

Identifica la entidad concreta responsable de la métrica (siempre hay un valor).

---

### 2.3 Parámetros de Métricas (Fórmulas & Thresholds)

#### **SKU Efficiency Thresholds**
[sps_efficiency.sql:38-50](sps_sql_new/sps_efficiency.sql#L38-L50)

| Parámetro | Valor | Impacto |
|-----------|-------|--------|
| SKU Age (Mature) | `>= 90 días` | Determina qué SKUs contar como "maduros" |
| SKU Age (Probation) | `30 < age < 90 días` | Ventana de prueba |
| SKU Age (New) | `<= 30 días` | SKUs recién listados |
| Efficiency Category | `zero_mover`, `slow_mover`, `efficient_sku` | Clasificación de rotación (desde tabla upstream) |

**Uso:**
- `sku_mature`: Conteo de SKUs maduros
- `sku_probation`: SKUs en período de evaluación
- `sku_new`: SKUs recién agregados
- `efficient_movers`: SKUs maduros + eficientes
- `new_zero_movers`: SKUs nuevos sin movimiento

#### **Weight Efficiency Formula**
[sps_efficiency.sql:72-92](sps_sql_new/sps_efficiency.sql#L72-L92)

```sql
ROUND(
  SAFE_DIVIDE(
    [Conteo de SKUs maduros eficientes],
    [SKUs maduros eficientes O (slow_movers con availability >= 0.8) O (zero_movers con availability = 1)]
  ) * SUM(gpv_eur)
, 4)
```

**Thresholds empleados:**
- Availability threshold (slow_movers): `>= 0.8` (80%)
- Availability threshold (zero_movers): `= 1` (100%)
- Precision: `4 decimales`

#### **Availability Metrics**
[sps_efficiency.sql:201-202](sps_sql_new/sps_efficiency.sql#L201-L202)

```
numerator_new_avail: SUM([stock disponible])
denom_new_avail:     SUM([stock demandado])
```

En Tableau: `SUM(numerator_new_avail) / SUM(denom_new_avail)` = disponibilidad final

#### **Promo & Discount Thresholds**
[sps_financial_metrics.sql:53,57,62,66](sps_sql_new/sps_financial_metrics.sql#L53-L66)

| Parámetro | Lógica | Propósito |
|-----------|--------|----------|
| `unit_discount_amount_eur > 0` | Booleano | Identifica transacciones con promoción |
| `unit_discount_amount_lc > 0` | Booleano | Equivalente en local currency |

Usado para segregar:
- `Net_Sales_from_promo_eur` = Ventas en transacciones con descuento
- `Promo_GPV_contribution_eur` = (Promo Sales / Total Sales)

---

### 2.4 Parámetros de Cumplimiento (PO Quality)
[sps_purchase_order.sql:47-58](sps_sql_new/sps_purchase_order.sql#L47-L58)

| Parámetro | Lógica | Propósito |
|-----------|--------|----------|
| `order_received_on_time = 1` | Booleano | OTD (On-Time Delivery) |
| `is_compliant_flag` | Booleano | Filtro de calidad |
| `order_status = 'done'` | STRING | Solo órdenes completadas |
| `cancel_reason IS NULL` | Booleano | Órdenes no canceladas |
| `cancel_reason IN (...)` | Enum | Razones de cancelación por supplier |

**Razones de cancelación rastreadas:**
- `'Supplier non fulfillment'`
- `'AUTO CANCELLED'`
- `'PO rejected on site (Quality issue)'`
- `'Stock not available'`

---

### 2.5 Parámetros de Costos & Pagos
[sps_days_payable.sql](sps_sql_new/sps_days_payable.sql) (implícito)

| Parámetro | Cálculo | Propósito |
|-----------|---------|----------|
| `payment_days` | DPO (Days Payable Outstanding) | Plazo de pago promedio |
| `doh` | DOH (Days on Hand) | Rotación de inventario |
| `dpo` | DPO (Days Payable Outstanding) | Flujo de caja |

---

### 2.6 Parámetros de Penalización / Rebates
[sps_line_rebate_metrics.sql](sps_sql_new/sps_line_rebate_metrics.sql) (implícito)

Incluye métricas de:
- Rebates lineales confirmados vs. proyectados
- Deducibles vs. PFC
- Leakage rates

---

## 3. Tabla de Mapeo: Parámetros → Archivos

| Parámetro | Tipo | Archivos Afectados |
|-----------|------|-------------------|
| `project_id`, `dataset.*` | Jinja2 | TODOS (29 archivos) |
| `lookback_limit` | Lógico | efficiency, financial_metrics, purchase_order, days_payable, delivery_costs |
| `time_granularity` | Lógico | TODOS los "sps_*_month.sql" agrupadores |
| `division_type` | Lógico | efficiency, financial_metrics, purchase_order, days_payable, delivery_costs, listed_sku, shrinkage |
| `supplier_level` | Lógico | (same as division_type) |
| `entity_key` | Lógico | (same as division_type) |
| SKU Age thresholds (90, 30 días) | Lógico | efficiency.sql, efficiency_month.sql |
| Availability thresholds (0.8, 1.0) | Lógico | efficiency.sql |
| Discount detection | Lógico | financial_metrics.sql |
| OTD/Compliance logic | Lógico | purchase_order.sql |
| Cancel reason list | Lógico | purchase_order.sql |

---

## 4. Jerarquía de Ejecución y Dependencias

Los parámetros se aplican en cadena:

```
1. JINJA2 PARAMS (inyección)
   ↓
2. DATE_CONFIG (lookback_limit)
   ↓
3. GRANULARITY PARAMS (time_period, time_granularity)
   ↓
4. GROUPING SETS (division_type, supplier_level, entity_key, brand_sup)
   ↓
5. METRIC FILTERS (SKU Age, Availability, Discounts, Compliance)
   ↓
6. FINAL AGGREGATION (SUM, COUNT, ROUND)
```

---

## 5. Configuración por Ambiente

### Development (`fulfillment-dwh-development`)
```yaml
params:
  project_id: fulfillment-dwh-development
  dataset:
    cl: csm_automated_tables
    curated_data_shared_salesforce_srm: curated_data_shared_salesforce_srm
    curated_data_shared_supply_chain: curated_data_shared_supply_chain
```

### Production (`fulfillment-dwh-production`)
```yaml
params:
  project_id: fulfillment-dwh-production
  dataset:
    cl: csm_automated_tables
    curated_data_shared_salesforce_srm: curated_data_shared_salesforce_srm
    curated_data_shared_supply_chain: curated_data_shared_supply_chain
```

> **Nota:** Los IDs de proyecto cambian, pero los nombres de dataset son típicamente iguales en todos los ambientes.

---

## 6. Cambios Potenciales & Notas de Tuning

### Tunable Parameters (sin cambiar queries)
- ✅ `lookback_limit`: Cambiar de 4 a N trimestres
- ✅ Dates en reportes Tableau (via parámetro de Tableau)

### Parameters que Requieren Cambio de Query
- ⚠️ `SKU Age thresholds` (90, 30 días) → Modificar líneas 26-50 en efficiency.sql
- ⚠️ `Availability thresholds` (0.8, 1.0) → Modificar líneas 84-87 en efficiency.sql
- ⚠️ `Cancel reason list` → Modificar líneas 48, 54 en purchase_order.sql
- ⚠️ Pesos de métricas (si se aplicaran) → Aún no parametrizados en eficiency

---

## 7. Resumen Ejecutivo

**Total de parámetros identificados:** ~30+

**Categorías principales:**
1. **Jinja2** (4): Project ID + 3 datasets
2. **Temporal** (4): lookback_limit, time_period, time_granularity, last_year_time_period
3. **Granularidad** (4): division_type, supplier_level, entity_key, brand_sup
4. **SKU Efficiency** (5): Age thresholds + categorías
5. **Disponibilidad** (2): numerator/denom availability
6. **Financiero** (2): Discount detection (EUR/LC)
7. **Cumplimiento** (5): OTD, compliance, cancel reasons
8. **Otros** (2): Payment days, shrinkage rates

**Punto crítico:** Todos los parámetros lógicos están **hardcoded en las queries**. Para flexibilidad operacional, considerar externalizar como tabla de configuración en BQ.
