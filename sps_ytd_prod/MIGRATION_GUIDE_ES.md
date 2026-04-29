# Guia de Migracion: sps_sql_new/ --> sps_ytd_prod/

**Pipeline:** Supplier Performance Scorecard (SPS)
**Autor:** Christian La Rosa
**Fecha:** 2026-04-29
**Destinatario:** Ion (Analytics Engineer - carga a produccion)

---

## 1. Resumen Ejecutivo

Esta migracion lleva el pipeline SPS de su version actual (`sps_sql_new/`, 24 scripts) a una nueva version (`sps_ytd_prod/`, 29 scripts). Los cambios principales son:

| Dimension | sps_sql_new (ACTUAL) | sps_ytd_prod (NUEVO) |
|---|---|---|
| **Scripts** | 24 | 29 (+5 nuevos) |
| **Granularidad temporal** | Monthly + Quarterly | Monthly + Quarterly + **YTD** |
| **Categorias** | master_category L1/L2/L3 | master_category L1/L2/L3 + **front_facing L1/L2** |
| **Ventana de datos** | Rolling 4 quarters (fijo) | **Ano calendario actual + ano anterior completo** |
| **COUNT DISTINCT** | Exacto (`COUNT(DISTINCT ...)`) | **Aproximado** (`APPROX_COUNT_DISTINCT`) |
| **Prev Year (LY)** | Rolling quarter offset (sin cap) | **Pattern F**: cap por max_date de la entidad |
| **Scoring Layer** | No existe | **Nuevo**: `sps_scoring_params`, `sps_supplier_scoring`, `sps_market_yoy` |
| **Segmentation + Master** | No existe | **Nuevo**: `sps_market_customers`, `sps_supplier_segmentation`, `sps_supplier_master` |
| **Segmentacion** | No existe | **Nuevo**: `sps_supplier_segmentation` |

**Por que estos cambios?** El pipeline actual solo ofrece vistas mensuales y trimestrales. Los equipos comerciales necesitan comparaciones Year-to-Date (YTD) para negociaciones con suppliers, y las categorias front-facing reflejan la taxonomia visible al cliente en la app. El Scoring Layer centraliza la logica de puntuacion que antes vivia dispersa en Tableau.

---

## 2. Arquitectura del Pipeline

### 2.1 Comparacion completa de scripts (29 archivos)

| # | Script | sps_sql_new | sps_ytd_prod | Cambio |
|---|---|---|---|---|
| 1 | `sps_product.sql` | Si | Si | Minimo (cosmetic) |
| 2 | `sps_supplier_hierarchy.sql` | Si | Si | Minimo (cosmetic) |
| 3 | `sps_customer_order.sql` | Si | Si | front_facing columns agregadas |
| 4 | `sps_financial_metrics_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 5 | `sps_efficiency_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 6 | `sps_purchase_order_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 7 | `sps_line_rebate_metrics_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 8 | `sps_listed_sku_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 9 | `sps_shrinkage_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 10 | `sps_delivery_costs_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 11 | `sps_days_payable_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 12 | `sps_price_index_month.sql` | Si | Si | front_facing + `ytd_year` column |
| 13 | `sps_financial_metrics.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + APPROX + front_facing |
| 14 | `sps_financial_metrics_prev_year.sql` | Si | Si | **Refactor mayor**: Pattern F + YTD + front_facing |
| 15 | `sps_efficiency.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 16 | `sps_purchase_order.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 17 | `sps_line_rebate_metrics.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 18 | `sps_listed_sku.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 19 | `sps_shrinkage.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 20 | `sps_delivery_costs.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 21 | `sps_days_payable.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 22 | `sps_price_index.sql` | Si | Si | **Refactor mayor**: 2-CTE + YTD + front_facing |
| 23 | `sps_score_tableau.sql` | Si | Si | **Refactor**: + `sps_market_customers` JOIN + campos limpiados |
| 24 | `sps_score_tableau_init.sql` | Si | **ELIMINADO** | Absorbido por `sps_score_tableau.sql` |
| 25 | `sps_market_customers.sql` | No | **NUEVO** | Denominador de penetracion de mercado |
| 26 | `sps_scoring_params.sql` | No | **NUEVO** | Umbrales dinamicos por entidad y periodo |
| 27 | `sps_supplier_scoring.sql` | No | **NUEVO** | Scores individuales (7 dimensiones, 200 pts) |
| 28 | `sps_market_yoy.sql` | No | **NUEVO** | YoY de mercado para techo de score YoY |
| 29 | `sps_supplier_master.sql` | No | **NUEVO** | Vista consolidada supplier-level para Tableau |
| 30 | `sps_supplier_segmentation.sql` | No | **NUEVO** | Segmentacion 4 cuadrantes (Key/Standard/Niche/Long Tail) |

### 2.2 Diagrama de dependencias

```
Layer 1 - Gathering (12 scripts):
  L0: sps_supplier_hierarchy       -- Salesforce SRM -> jerarquia
  L1: sps_product                  -- catalogo + PO -> SKU mapping
  L2 (paralelo despues de product):
    sps_customer_order             -- lee sps_product + fuentes externas
    sps_financial_metrics_month
    sps_efficiency_month
    sps_purchase_order_month
    sps_line_rebate_metrics_month
    sps_listed_sku_month
    sps_shrinkage_month
    sps_delivery_costs_month
    sps_days_payable_month
    sps_price_index_month

Layer 2 - Grouping Sets (11 scripts):
  sps_financial_metrics_prev_year  -- Pattern F: cap por max_date (primero)
  sps_financial_metrics            -- 2-CTE: monthly_quarterly + ytd_data (JOIN prev_year -> YoY)
  sps_efficiency
  sps_purchase_order
  sps_line_rebate_metrics
  sps_listed_sku
  sps_shrinkage
  sps_delivery_costs
  sps_days_payable
  sps_price_index
  sps_market_customers             -- NUEVA: denominador de penetracion

Layer 3 - Union All (1 script):
  sps_score_tableau                -- all_keys UNION + 10 LEFT JOINs

Layer 4 - Segmentation (1 script):
  sps_supplier_segmentation        -- NUEVA: 4 cuadrantes

Layer 5 - Scoring (3 scripts):
  sps_scoring_params               -- NUEVA: umbrales
  sps_market_yoy                   -- NUEVA: YoY de mercado
  sps_supplier_scoring             -- NUEVA: scores (200 pts)

Layer 6 - Master (1 script):
  sps_supplier_master              -- NUEVA: vista consolidada

Dependencias: Gathering -> Grouping Sets -> Union All -> [Segmentation // Scoring] -> Master
```

---

## 3. Nuevas Funcionalidades

### 3.1 Granularidad YTD (Year-to-Date)

**Que es?** Ademas de Monthly y Quarterly, el pipeline nuevo agrega una tercera granularidad: **YTD** (Year-to-Date). Esto permite comparar todo lo acumulado en el ano actual contra el mismo periodo del ano anterior.

**Por que?** Los Account Managers necesitan comparar el desempeno acumulado del ano para negociaciones contractuales. "Este supplier lleva X% de crecimiento en el ano" es una metrica central en revisiones de negocio trimestrales.

**Como funciona en el SQL?**

En la version actual (`sps_sql_new/`), cada script de la Grouping Sets Layer tiene un solo query con `GROUPING SETS` que solo contiene combinaciones `(month, ...)` y `(quarter_year, ...)`.

En la version nueva (`sps_ytd_prod/`), cada script usa el patron **2-CTE**:

```sql
-- CTE 1: monthly_quarterly_data (misma logica que antes, expandida con front_facing)
-- CTE 2: ytd_data (nueva, agrega por ytd_year con GROUPING SETS)
-- Final: UNION ALL de ambos CTEs
SELECT * FROM monthly_quarterly_data
UNION ALL
SELECT * FROM ytd_data
```

**Por que dos CTEs separados en lugar de un solo GROUPING SETS?**

La razon es que el periodo YTD tiene una ventana de datos diferente a Monthly/Quarterly:

| Granularidad | Ventana de datos (WHERE) |
|---|---|
| Monthly | Ano actual + ano anterior completo |
| Quarterly | Ano actual + ano anterior completo |
| **YTD** | Ano actual **hasta hoy** + ano anterior **hasta la misma fecha del ano pasado** |

Si mezclamos YTD en el mismo GROUPING SETS que Monthly/Quarterly, la restriccion `WHERE` de YTD excluiria meses futuros que Monthly/Quarterly si necesitan (del ano anterior). Por eso se separan en dos CTEs con WHEREs independientes.

**Formato del campo `time_period` para YTD:**

```
YTD-2026    (ano actual acumulado)
YTD-2025    (ano anterior acumulado)
```

**Campos nuevos en las tablas _month:**

Todos los scripts `*_month.sql` agregan una columna:
```sql
EXTRACT(YEAR FROM order_date) AS ytd_year
```

Esta columna es la que permite el `GROUP BY` del CTE ytd_data.

### 3.2 Categorias Front-Facing

**Que es?** Dos nuevos niveles de categoria que reflejan la taxonomia visible al cliente en la app de delivery:

| Campo | Ejemplo | Fuente |
|---|---|---|
| `front_facing_level_one` | "Beverages", "Snacks" | `sps_product` via `sps_customer_order` |
| `front_facing_level_two` | "Soft Drinks", "Energy Drinks" | `sps_product` via `sps_customer_order` |

**Por que?** Las categorias `l1/l2/l3_master_category` son internas (taxonomia de warehouse). Los equipos comerciales quieren ver el rendimiento por la categoria que el cliente ve en la app, que a menudo difiere del nombre interno.

**Impacto en GROUPING SETS:**

Cada script de la Grouping Sets Layer pasa de ~36 grouping sets (Monthly + Quarterly) a ~56 grouping sets (Monthly + Quarterly + front_facing Monthly + front_facing Quarterly + front_facing YTD + YTD).

Los nuevos grouping sets siguen el patron:
```sql
-- FRONT-FACING CATEGORY DEEP-DIVE (Monthly)
(month, global_entity_id, principal_supplier_id, front_facing_level_one),
(month, global_entity_id, principal_supplier_id, front_facing_level_two),
(month, global_entity_id, supplier_id, front_facing_level_one),
(month, global_entity_id, supplier_id, front_facing_level_two),
(month, global_entity_id, brand_owner_name, front_facing_level_one),
(month, global_entity_id, brand_owner_name, front_facing_level_two),
(month, global_entity_id, brand_name, front_facing_level_one),
(month, global_entity_id, brand_name, front_facing_level_two),
-- (mismo patron para quarter_year y ytd_year)
```

**Impacto en `entity_key` y `supplier_level`:**

El campo `entity_key` (COALESCE de niveles) ahora incluye los niveles front_facing:

```sql
-- ANTES (sps_sql_new):
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  IF(GROUPING(brand_name) = 0, brand_name, NULL),
  ...

-- DESPUES (sps_ytd_prod):
COALESCE(
  IF(GROUPING(l3_master_category) = 0, l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0, l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0, l1_master_category, NULL),
  IF(GROUPING(front_facing_level_two) = 0, front_facing_level_two, NULL),  -- NUEVO
  IF(GROUPING(front_facing_level_one) = 0, front_facing_level_one, NULL),  -- NUEVO
  IF(GROUPING(brand_name) = 0, brand_name, NULL),
  ...
```

Y `supplier_level` agrega dos valores posibles:

```sql
CASE
  WHEN GROUPING(l3_master_category) = 0      THEN 'level_three'
  WHEN GROUPING(l2_master_category) = 0      THEN 'level_two'
  WHEN GROUPING(l1_master_category) = 0      THEN 'level_one'
  WHEN GROUPING(front_facing_level_two) = 0  THEN 'front_facing_level_two'  -- NUEVO
  WHEN GROUPING(front_facing_level_one) = 0  THEN 'front_facing_level_one'  -- NUEVO
  WHEN GROUPING(brand_name) = 0              THEN 'brand_name'
  ELSE 'supplier'
END AS supplier_level
```

### 3.3 Scoring Layer (scripts nuevos)

Tres scripts completamente nuevos que centralizan la logica de scoring:

#### sps_scoring_params.sql

**Proposito:** Calcular umbrales dinamicos (starting, ending) por `global_entity_id x time_period` para las dimensiones de score: back margin, front margin, GBD.

**Como funciona:** Lee de `sps_score_tableau` filtrando `supplier_level='supplier'`, `division_type='division'`, `Net_Sales_eur > 1000`. Calcula percentiles (P25, P75), IQR mean, y weighted averages para definir:

| Parametro | Significado |
|---|---|
| `bm_starting` | Umbral minimo back margin (P25 de suppliers con rebate) |
| `bm_ending` | Umbral maximo back margin (blend IQR + weighted, cap 0.70) |
| `fm_starting` | Umbral minimo front margin (max de P25 y 0.12 floor) |
| `fm_ending` | Umbral maximo front margin (blend, min starting+0.08, cap 0.70) |
| `gbd_target` | Target de GBD por entidad (hardcoded por pais, default 0.05) |
| `gbd_lower` | 50% del target |
| `gbd_upper` | 200% del target |

**Por que es necesario?** Antes, los umbrales estaban hardcodeados en Tableau o se calculaban manualmente. Esto centraliza la logica y la hace auditable.

#### sps_supplier_scoring.sql

**Proposito:** Asignar puntajes individuales por supplier en 7 dimensiones, con un total de 200 puntos:

| Dimension | Puntos | Formula |
|---|---|---|
| Fill Rate | 60 | `MIN(fill_rate, 1.0) * 60` |
| OTD (On-Time Delivery) | 40 | `MIN(otd, 1.0) * 40` |
| YoY Growth | 10 | Lineal 0-10, cap en `market_yoy * 1.2` |
| Efficiency | 30 | Lineal 0-30 entre 0.40 y 1.0 |
| GBD (Gross Buying Discount) | 20 | Campana asimetrica alrededor de `gbd_target` |
| Back Margin | 25 | Lineal entre `bm_starting` y `bm_ending` |
| Front Margin | 15 | Lineal entre `fm_starting` y `fm_ending` |

**Detalle importante:** Las filas YTD usan los parametros del ultimo periodo Monthly disponible (no se calculan parametros YTD propios). Esto se resuelve con el CTE `params_key`:

```sql
params_key AS (
  SELECT global_entity_id,
    MAX(time_period) AS latest_monthly_period
  FROM sps_scoring_params
  GROUP BY global_entity_id
)
```

#### sps_market_yoy.sql

**Proposito:** Calcular el YoY de mercado por entidad y periodo. Se usa como techo del score de YoY Growth (un supplier no puede obtener 10/10 por crecer 5% si el mercado crecio 20%).

```sql
-- Formula del techo:
LEAST(GREATEST(market_yoy_lc * 1.2, 0.20), 0.70) AS yoy_max
```

### 3.4 Grouping Sets Layer + Master (scripts nuevos)

#### sps_market_customers.sql

**Proposito:** Calcular el total de clientes unicos y ordenes en el mercado (sin filtro de supplier) por `global_entity_id x time_period x time_granularity`. Es el denominador para customer_penetration:

```
penetration = total_customers(supplier) / total_market_customers(mercado)
```

**Estructura del script:**

| CTE | Granularidad | Metrica |
|---|---|---|
| `monthly` | Por mes | `APPROX_COUNT_DISTINCT(analytical_customer_id)` |
| `quarterly` | Por trimestre | COUNT DISTINCT sobre todo el quarter (NO suma de meses) |
| `ytd` | Por ano acumulado | COUNT DISTINCT sobre todos los meses del ano hasta hoy |

**Nota critica para Ion:** Quarterly y YTD NO son la suma de los meses. Un cliente que compro en octubre, noviembre y diciembre cuenta como 1 cliente en Q4, no como 3. Es por eso que se usa `APPROX_COUNT_DISTINCT` directamente sobre el periodo completo.

**Join en sps_score_tableau:** Este script se une a `sps_score_tableau` via:
```sql
LEFT JOIN sps_market_customers AS mc
  ON o.global_entity_id = mc.global_entity_id
  AND o.time_period = mc.time_period
  AND o.time_granularity = mc.time_granularity
```

Solo se joinea por 3 campos (no por las 7 claves habituales) porque `sps_market_customers` es a nivel de mercado, no de supplier.

#### sps_supplier_master.sql

**Proposito:** Vista consolidada supplier-level que agrega `sps_score_tableau` al nivel `supplier_level='supplier'` con ratios pre-calculados, nombre del supplier (de `sps_product`), y nombre de marca de la entidad (PedidosYa, Talabat, etc).

**Campos calculados incluidos:**
- `ratio_otd`, `ratio_fill_rate`, `ratio_efficiency`, `ratio_spoilage`
- `ratio_front_margin`, `ratio_back_margin`, `ratio_gbd`
- `ratio_penetration`, `ratio_yoy_growth`
- `supplier_name` (de sps_product), `global_entity_name` (mapeo de prefijo)

#### sps_supplier_segmentation.sql

**Proposito:** Clasificar suppliers en 4 segmentos usando dos ejes:

| Eje | Metrica | Calculo |
|---|---|---|
| **Importancia** (eje X) | Net Profit LC | `Net_Sales_lc + funding_lc - COGS_lc + back_margin_amt_lc` |
| **Productividad** (eje Y) | Score ponderado | Penetration(40) + Frequency(30) + ABV(30) |

Pesos de productividad validados el 2026-04-25:
- Penetration = 40 puntos (correlacion r=0.2718 con Net Sales)
- Frequency = 30 puntos (correlacion r=0.2686)
- ABV = 30 puntos (correlacion r=-0.0790, desacoplado de tamano)

Segmentos resultantes:

| Segmento | Importancia | Productividad |
|---|---|---|
| Key Account | >= P75 | >= P50 |
| Standard | >= P50 | >= P50 |
| Niche | < P50 | >= P50 |
| Long Tail | * | < P50 |

### 3.5 Optimizacion APPROX_COUNT_DISTINCT

**Que cambio?** En todos los scripts de la Grouping Sets Layer, `COUNT(DISTINCT x)` se reemplazo por `APPROX_COUNT_DISTINCT(x)`.

```sql
-- ANTES (sps_sql_new):
COUNT(DISTINCT analytical_customer_id) AS total_customers,
COUNT(DISTINCT sku_id)                 AS total_skus_sold,
COUNT(DISTINCT order_id)               AS total_orders,
COUNT(DISTINCT warehouse_id)           AS total_warehouses_sold,

-- DESPUES (sps_ytd_prod):
APPROX_COUNT_DISTINCT(analytical_customer_id) AS total_customers,
APPROX_COUNT_DISTINCT(sku_id)                 AS total_skus_sold,
APPROX_COUNT_DISTINCT(order_id)               AS total_orders,
APPROX_COUNT_DISTINCT(warehouse_id)           AS total_warehouses_sold,
```

**Por que?** `APPROX_COUNT_DISTINCT` usa HyperLogLog++ (error tipico < 1%) y es significativamente mas rapido en queries con muchos GROUPING SETS. Con la adicion de YTD y front_facing (que expanden los grouping sets de ~36 a ~56+), la diferencia de rendimiento se vuelve material (estimacion: 30-50% menos de slot-time en BigQuery).

**Impacto en los numeros:** Las metricas afectadas (`total_customers`, `total_skus_sold`, `total_orders`, `total_warehouses_sold`) pueden diferir en < 1% vs el conteo exacto. Para efectos de scorecard, esta precision es suficiente. Los montos financieros (Net Sales, COGS, etc.) NO usan APPROX -- siguen siendo SUM exactos.

### 3.7 Resolucion de Nombre de Supplier en score_tableau

**Que problema resuelve?** El campo `brand_sup` en score_tableau contiene IDs de supplier (ej. "SUP-12345") para las filas donde `division_type IN ('division', 'principal')`. Estos IDs no son legibles para humanos. Los usuarios de Tableau y las capas de scoring/segmentacion necesitan nombres reales de supplier. Las filas de brand_owner y brand_name ya tienen valores legibles en `brand_sup`, por lo que no necesitan resolucion.

**Como funciona?**

1. Un CTE `sps_product_clean` extrae mappings distintos `supplier_id -> supplier_name` de `sps_product`.
2. Un LEFT JOIN cruza `brand_sup` con `supplier_id` **solo** para filas donde `division_type IN ('division', 'principal')`.
3. Una nueva columna `supplier_name` usa una expresion CASE:

```sql
CASE
  WHEN division_type IN ('division', 'principal')
    THEN COALESCE(prod.supplier_name, brand_sup)  -- nombre resuelto con fallback
  ELSE brand_sup                                   -- ya legible (brand_owner, brand_name, total)
END AS supplier_name
```

**Nota:** Esta funcionalidad NO existia en `sps_sql_new` (produccion actual). Es una adicion nueva de `sps_ytd_prod`.

### 3.6 Patron F de Prev Year (cap por entidad)

**Que es?** En la version actual (`sps_sql_new/`), `sps_financial_metrics_prev_year` usa un rango fijo basado en quarters:

```sql
-- ACTUAL (sps_sql_new):
WHERE DATE(month) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 8 QUARTER)
  AND DATE(month) < DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER)
```

Esto tiene un problema: si una entidad empezo a reportar en marzo 2026, pero el pipeline corre en abril 2026, el YTD del ano anterior incluiria enero-diciembre 2025 completos. Pero el ano actual solo tiene enero-abril 2026. La comparacion seria injusta: 12 meses vs 4 meses.

**Pattern F** resuelve esto con un cap por entidad:

```sql
-- NUEVO (sps_ytd_prod):
-- Paso 1: encontrar la ultima fecha con datos por entidad en el ano actual
max_date_cy AS (
  SELECT global_entity_id,
    MAX(CAST(month AS DATE)) AS max_month_cy
  FROM sps_financial_metrics_month
  WHERE EXTRACT(YEAR FROM CAST(month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY global_entity_id
),

-- Paso 2: filtrar datos LY con cap por entidad
filtered_ly AS (
  SELECT m.*
  FROM sps_financial_metrics_month m
  JOIN max_date_cy mx ON m.global_entity_id = mx.global_entity_id
  WHERE EXTRACT(YEAR FROM CAST(m.month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    AND CAST(m.month AS DATE) <= DATE_SUB(LAST_DAY(mx.max_month_cy), INTERVAL 1 YEAR)
)
```

**Ejemplo concreto:**
- Entidad `PY_AR` tiene datos hasta marzo 2026
- `max_month_cy` para PY_AR = 2026-03-01
- Cap del ano anterior = `LAST_DAY(2026-03-01) - 1 year` = 2025-03-31
- Resultado: LY solo incluye enero-marzo 2025 (no el ano completo)

Adicionalmente, el `join_time_period` para YTD se construye con shift de +1 ano:
```sql
WHEN GROUPING(ytd_year) = 0 THEN CONCAT('YTD-', CAST(CAST(ytd_year AS INT64) + 1 AS STRING))
```

Esto hace que `YTD-2025` (datos LY) se mapee al join key `YTD-2026` para unirse con el CY.

---

## 4. Parametrizacion Jinja

La parametrizacion Jinja es **identica** entre ambas versiones. No hay cambios en las variables.

| Variable | Uso | Ejemplo |
|---|---|---|
| `{{ params.project_id }}` | ID del proyecto GCP | `fulfillment-dwh-production` |
| `{{ params.dataset.cl }}` | Dataset de destino | `cl_supplier_performance` |
| `{{ next_ds }}` | Fecha de ejecucion del DAG (Airflow) | `2026-04-29` |
| `{{ params.stream_look_back_days }}` | Dias de look-back para sps_customer_order | `90` |
| `{{ params.backfill }}` | Flag de backfill | `true/false` |
| `{{ params.is_backfill_chunks_enabled }}` | Flag de backfill por chunks | `true/false` |
| `{{ params.backfill_start_date }}` | Fecha inicio backfill | `2025-01-01` |
| `{{ params.backfill_end_date }}` | Fecha fin backfill | `2025-12-31` |

**Nota importante:** Los scripts nuevos (scoring, market, segmentation, supplier_master) NO usan Jinja para parametros de backfill porque son tablas full-refresh sin logica incremental. Solo usan `{{ params.project_id }}` y `{{ params.dataset.cl }}`.

---

## 5. Cambios Script por Script

### Gathering Layer: L0-L1 Fuentes (sin cambios funcionales)

| Script | Cambio |
|---|---|
| `sps_product.sql` | Sin cambios funcionales. Cosmetic only. |
| `sps_supplier_hierarchy.sql` | Sin cambios funcionales. Cosmetic only. |

### Gathering Layer: L2 Transaccional

| Script | Cambio |
|---|---|
| `sps_customer_order.sql` | Agrega `front_facing_level_one`, `front_facing_level_two` al SELECT desde `sps_product`. Estas columnas se propagan a todos los scripts downstream. |

### Gathering Layer: L2 Scripts _month (9 scripts)

Todos los scripts `*_month.sql` reciben los mismos tres cambios:

| Cambio | Detalle |
|---|---|
| `front_facing_level_one` | Columna agregada al SELECT y propagada downstream |
| `front_facing_level_two` | Columna agregada al SELECT y propagada downstream |
| `ytd_year` | `EXTRACT(YEAR FROM ...)` para habilitar GROUPING SETS por ytd_year |

Scripts afectados:
- `sps_financial_metrics_month.sql`
- `sps_efficiency_month.sql`
- `sps_purchase_order_month.sql`
- `sps_line_rebate_metrics_month.sql`
- `sps_listed_sku_month.sql`
- `sps_shrinkage_month.sql`
- `sps_delivery_costs_month.sql`
- `sps_days_payable_month.sql`
- `sps_price_index_month.sql`

### Grouping Sets Layer: Scripts de agregacion -- 10 scripts

#### sps_financial_metrics.sql

| Aspecto | ACTUAL (sps_sql_new) | NUEVO (sps_ytd_prod) |
|---|---|---|
| **Estructura** | 1 CTE `current_year_data` | 3 CTEs: `date_config` + `monthly_quarterly_data` + `ytd_data` + UNION ALL |
| **Ventana WHERE** | `DATE(month) >= DATE_SUB(DATE_TRUNC(CURRENT_DATE(), QUARTER), INTERVAL 4 QUARTER)` | CY: `EXTRACT(YEAR) = current_year AND month <= today` + LY: `EXTRACT(YEAR) = prior_year` |
| **time_granularity** | Monthly, Quarterly | Monthly, Quarterly, **YTD** |
| **GROUPING SETS** | ~36 (Monthly + Quarterly) | ~56 (Monthly + Quarterly + front_facing Monthly + front_facing Quarterly) + YTD separado |
| **COUNT DISTINCT** | `COUNT(DISTINCT x)` | `APPROX_COUNT_DISTINCT(x)` |
| **entity_key COALESCE** | 7 niveles | 9 niveles (+front_facing_level_one, +front_facing_level_two) |
| **supplier_level** | 5 valores posibles | 7 valores posibles (+front_facing_level_one, +front_facing_level_two) |
| **Deduplicacion order-level** | `SUM(amt_total_price_paid_net_eur)` directo | ROW_NUMBER + `_dedup` fields para evitar inflacion de basket-level metrics |
| **Merge final** | JOIN directo con prev_year + line_rebate | JOIN directo con prev_year + line_rebate (identica logica) |

**Nota sobre la deduplicacion:** En la version nueva se aplica un `ROW_NUMBER()` para desduplicar campos a nivel de orden (`amt_total_price_paid_net_eur`, `amt_total_price_paid_net_lc`, `amt_gbv_eur`) que en la version antigua se sumaban directamente, lo que podia inflar las metricas cuando un pedido tenia multiples SKUs del mismo supplier:

```sql
CASE WHEN ROW_NUMBER() OVER (
  PARTITION BY src.global_entity_id, src.order_id, src.supplier_id, src.month
  ORDER BY src.sku_id
) = 1
THEN src.amt_total_price_paid_net_eur ELSE 0 END AS amt_total_price_paid_net_eur_dedup
```

#### sps_financial_metrics_prev_year.sql

| Aspecto | ACTUAL (sps_sql_new) | NUEVO (sps_ytd_prod) |
|---|---|---|
| **Ventana WHERE** | Rolling 8-4 quarters | **Pattern F**: cap por `max_date_cy` por entidad |
| **CTEs** | Ninguno (query directo) | 3 CTEs: `max_date_cy` + `filtered_ly` + `aggregated` |
| **Granularidad** | Monthly, Quarterly | Monthly, Quarterly, **YTD** |
| **entity_key** | 7 niveles | 9 niveles (+front_facing) |
| **YTD join key** | No aplica | `CONCAT('YTD-', CAST(ytd_year + 1 AS STRING))` |

#### Scripts restantes del Grouping Sets Layer

Los siguientes scripts reciben exactamente el mismo patron de cambio (2-CTE, YTD, front_facing, APPROX):

| Script | Cambio especifico adicional |
|---|---|
| `sps_efficiency.sql` | CTE `sku_counts` agrega `front_facing_level_one/two`, `ytd_year`. La CTE `sumable_metrics` agrega los mismos campos. |
| `sps_purchase_order.sql` | Patron 2-CTE aplicado. front_facing + YTD. |
| `sps_line_rebate_metrics.sql` | Patron 2-CTE aplicado. front_facing + YTD. |
| `sps_listed_sku.sql` | Patron 2-CTE aplicado. front_facing + YTD. |
| `sps_shrinkage.sql` | Patron 2-CTE aplicado. front_facing + YTD. |
| `sps_delivery_costs.sql` | Patron 2-CTE aplicado. front_facing + YTD. |
| `sps_days_payable.sql` | Patron 2-CTE aplicado. front_facing + YTD. |
| `sps_price_index.sql` | Patron 2-CTE aplicado. front_facing + YTD. |

### Grouping Sets Layer: Mercado

| Script | Estado | Notas |
|---|---|---|
| `sps_market_customers.sql` | **NUEVO** | Full-refresh. Sin Jinja de backfill. Lee `sps_customer_order`. |

### Union All Layer: Consolidacion

#### sps_score_tableau.sql

| Aspecto | ACTUAL (sps_sql_new) | NUEVO (sps_ytd_prod) |
|---|---|---|
| **all_keys UNION** | 9 tablas (mismas) | 9 tablas (mismas) |
| **JOINs** | 8 LEFT JOINs (7 keys) | 9 LEFT JOINs (7 keys) + 1 LEFT JOIN `sps_market_customers` (3 keys) + 1 LEFT JOIN `sps_product_clean` (supplier_name, ver 3.7) |
| **Campos efficiency** | Incluye `new_zero_movers`, `new_slow_movers`, `new_efficient_movers`, `sold_items`, `gpv_eur` como columnas separadas | **Limpiados**: elimina `new_zero/slow/efficient_movers`, `sold_items`. Conserva `gpv_eur` solo como ingrediente para weighted efficiency. |
| **back_margin** | Viene de `sps_financial_metrics` via join con `sps_line_rebate_metrics` | Viene de `sps_line_rebate_metrics` directamente en el JOIN de `sps_score_tableau` |
| **Campos nuevos** | -- | `total_market_customers`, `total_market_orders` (de `sps_market_customers`) |
| **sps_score_tableau_init** | Existe como script separado (version legacy) | **ELIMINADO** (absorbido en `sps_score_tableau`) |

**Columnas eliminadas del output (limpieza):**

Las siguientes columnas de `sps_efficiency` se excluyen en la version nueva porque no son utiles para el scorecard:

| Columna eliminada | Razon |
|---|---|
| `new_zero_movers` | SKUs no maduros -- injusto penalizar supplier |
| `new_slow_movers` | SKUs no maduros -- injusto penalizar supplier |
| `new_efficient_movers` | SKUs no maduros -- no relevante para scoring |
| `sold_items` | Duplicado con metricas de `sps_financial_metrics` |
| `la_zero_movers` | Low availability -- supplier puede culpar al stock |
| `la_slow_movers` | Low availability -- supplier puede culpar al stock |
| `new_availability` | Ratio pre-calculado -- siempre usar ingredientes en Tableau |

### Segmentation + Scoring + Master Layers

| Script | Estado | Fuente principal |
|---|---|---|
| `sps_market_yoy.sql` | **NUEVO** | Lee `sps_score_tableau` |
| `sps_scoring_params.sql` | **NUEVO** | Lee `sps_score_tableau` |
| `sps_supplier_scoring.sql` | **NUEVO** | Lee `sps_score_tableau` + `sps_scoring_params` + `sps_market_yoy` |
| `sps_supplier_segmentation.sql` | **NUEVO** | Lee `sps_score_tableau` |
| `sps_supplier_master.sql` | **NUEVO** | Lee `sps_score_tableau` + `sps_product` |

---

## 6. Orden de Ejecucion

El pipeline tiene 6 layers con 29 scripts. Cada layer depende de la anterior. Los scripts dentro del mismo paso pueden ejecutarse en paralelo salvo las constraints internas indicadas.

```
=== LAYER 1: Gathering Layer (12 scripts) ===
Recolecta data cruda de fuentes externas + mapping interno.
Constraint interno: hierarchy -> product -> [9 _month + customer_order] (paralelo)

  L0: sps_supplier_hierarchy         (Salesforce SRM -> jerarquia)
  L1: sps_product                    (catalogo + PO -> SKU mapping)
  L2 (paralelo despues de product):
      sps_customer_order
      sps_financial_metrics_month
      sps_efficiency_month
      sps_purchase_order_month
      sps_line_rebate_metrics_month
      sps_listed_sku_month
      sps_shrinkage_month
      sps_delivery_costs_month
      sps_days_payable_month
      sps_price_index_month

=== LAYER 2: Grouping Sets Layer (11 scripts) ===
Agrega _month -> Monthly + Quarterly + YTD + Front-Facing.
Patron two-CTE, 81 GROUPING SETS por script.
Constraint interno: financial_metrics_month -> prev_year -> financial_metrics

  Primero (secuencial):
      sps_financial_metrics_prev_year  (Pattern F, debe correr primero en la cadena financiera)
  Segundo (secuencial despues de prev_year + line_rebate_metrics):
      sps_financial_metrics            (JOIN prev_year -> YoY)
  Paralelo (despues de _month):
      sps_efficiency
      sps_purchase_order
      sps_line_rebate_metrics
      sps_listed_sku
      sps_shrinkage
      sps_delivery_costs
      sps_days_payable
      sps_price_index
  Paralelo (despues de customer_order):
      sps_market_customers             (Monthly + Quarterly + YTD)

=== LAYER 3: Union All Layer (1 script) ===
Ensambla star schema: all_keys UNION + 10 LEFT JOINs.

      sps_score_tableau                (lee TODAS las tablas de Layer 2 + sps_market_customers + sps_product [lookup supplier_name])

=== LAYER 4: Segmentation Layer (1 script) ===
Clasifica suppliers en 4 cuadrantes (Importancia x Productividad).

      sps_supplier_segmentation        (lee sps_score_tableau)

=== LAYER 5: Scoring Layer (3 scripts) ===
Modelo de 200 puntos + benchmarks de mercado.

      sps_scoring_params               (lee sps_score_tableau)
      sps_market_yoy                   (lee sps_score_tableau)
      sps_supplier_scoring             (lee sps_score_tableau + sps_scoring_params + sps_market_yoy)
      *** NOTA: sps_supplier_scoring depende de sps_scoring_params y sps_market_yoy.
      *** Ejecutar sps_scoring_params y sps_market_yoy ANTES de sps_supplier_scoring.

=== LAYER 6: Master (1 script) ===
Tabla final denormalizada: metricas + scores + segmentos.

      sps_supplier_master              (lee sps_score_tableau + sps_product)
```

**Dependencias entre layers:**

```
Gathering -> Grouping Sets -> Union All -> [Segmentation // Scoring] -> Master
```

Segmentation (Layer 4) y Scoring (Layer 5) pueden ejecutarse en paralelo. Master (Layer 6) espera a ambos.

**Detalle de constraints internos en Grouping Sets Layer:**

```
sps_financial_metrics_month
       |
       +---> sps_financial_metrics_prev_year  (primero en la cadena)
       |
       +---> sps_line_rebate_metrics          (paralelo con prev_year)
       |
       +---> sps_financial_metrics            (despues de prev_year + line_rebate)
```

Esto es identico a la version actual. La dependencia de `sps_financial_metrics` sobre `sps_financial_metrics_prev_year` y `sps_line_rebate_metrics` no es nueva.

---

## 7. Puntos de Atencion (ytd_test --> sps_ytd_prod)

### 7.1 Nombre del dataset

Los scripts usan `{{ params.dataset.cl }}` para el dataset. Asegurarse de que el dataset de produccion apunte a `sps_ytd_prod` en los parametros de Airflow, no a `ytd_test` que se uso durante desarrollo.

### 7.2 sps_score_tableau_init eliminado

En la version actual existe `sps_score_tableau_init.sql` como un join legacy (sin el patron `all_keys`). En la version nueva **no existe**. Si algun DAG o dashboard referencia `sps_score_tableau_init`, debe actualizarse para apuntar a `sps_score_tableau`.

### 7.3 Columnas eliminadas en sps_score_tableau

Si algun dashboard Tableau o script downstream referencia las siguientes columnas, fallara:

| Columna eliminada | Alternativa |
|---|---|
| `new_zero_movers` | No disponible (eliminada por diseno) |
| `new_slow_movers` | No disponible (eliminada por diseno) |
| `new_efficient_movers` | No disponible (eliminada por diseno) |
| `sold_items` | Usar `total_orders` o `fulfilled_quantity` de `sps_financial_metrics` |
| `la_zero_movers` | No disponible (eliminada por diseno) |
| `la_slow_movers` | No disponible (eliminada por diseno) |
| `new_availability` | Calcular como `SUM(numerator_new_avail) / SUM(denom_new_avail)` |

### 7.4 Nuevos valores en time_granularity

Las tablas ahora contienen filas con `time_granularity = 'YTD'`. Si algun dashboard filtra con `WHERE time_granularity IN ('Monthly', 'Quarterly')`, seguira funcionando sin cambios. Pero si usa `WHERE time_granularity != 'Monthly'` asumiendo que solo queda 'Quarterly', ahora incluira YTD tambien.

### 7.5 Nuevos valores en supplier_level

Dos valores nuevos: `'front_facing_level_one'` y `'front_facing_level_two'`. Misma logica: filtros explicitos siguen funcionando, filtros por exclusion pueden incluir datos inesperados.

### 7.6 Volumen de datos incrementado

Los nuevos GROUPING SETS (front_facing L1/L2 + YTD) incrementan significativamente el numero de filas. Estimacion conservadora: 50-80% mas filas en cada tabla de la Grouping Sets Layer. Esto impacta:
- Almacenamiento en BigQuery
- Tiempo de ejecucion de queries en Tableau
- Costo de slot-hours en BigQuery

### 7.7 APPROX_COUNT_DISTINCT vs COUNT(DISTINCT)

Las metricas `total_customers`, `total_skus_sold`, `total_orders`, `total_warehouses_sold` ahora usan conteo aproximado. Si algun proceso downstream espera coincidencia exacta con el pipeline anterior, habra discrepancias de hasta ~1%.

### 7.8 Tablas nuevas que deben crearse

Las siguientes tablas no existen en produccion actual y deben crearse la primera vez:

| Tabla | Tipo |
|---|---|
| `sps_market_customers` | Full refresh (CREATE OR REPLACE) |
| `sps_market_yoy` | Full refresh (CREATE OR REPLACE) |
| `sps_scoring_params` | Full refresh (CREATE OR REPLACE) |
| `sps_supplier_scoring` | Full refresh (CREATE OR REPLACE) |
| `sps_supplier_segmentation` | Full refresh (CREATE OR REPLACE) |
| `sps_supplier_master` | Full refresh (CREATE OR REPLACE) |

Todas usan `CREATE OR REPLACE TABLE`, por lo que no requieren un DDL previo.

### 7.9 Pattern F solo aplica a sps_financial_metrics_prev_year

El patron de cap por `max_date_cy` solo se implementa en `sps_financial_metrics_prev_year`. Los demas scripts `*_prev_year` (si existieran) no lo tienen porque los demas scripts no tienen prev_year separado -- el join YoY se resuelve en `sps_financial_metrics` y se propaga via `sps_score_tableau`.

---

## 8. Queries de Validacion

### 8.1 Verificar que YTD existe en las tablas de agregacion

```sql
-- Ejecutar para cada tabla de la Grouping Sets Layer
SELECT
  'sps_financial_metrics' AS tabla,
  time_granularity,
  COUNT(*) AS filas
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
GROUP BY time_granularity
ORDER BY time_granularity;

-- Resultado esperado: 3 filas (Monthly, Quarterly, YTD)
```

### 8.2 Verificar front_facing en supplier_level

```sql
SELECT
  supplier_level,
  COUNT(*) AS filas
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
GROUP BY supplier_level
ORDER BY supplier_level;

-- Resultado esperado: 7 valores (brand_name, front_facing_level_one,
--   front_facing_level_two, level_one, level_two, level_three, supplier)
```

### 8.3 Validar Pattern F: YTD LY no excede CY

```sql
-- Para cada entidad, max_month de LY debe ser <= max_month de CY (shifted)
WITH cy AS (
  SELECT global_entity_id, MAX(time_period) AS max_cy
  FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
  WHERE time_granularity = 'Monthly'
    AND time_period LIKE '2026%'
  GROUP BY global_entity_id
),
ly AS (
  SELECT global_entity_id, MAX(join_time_period) AS max_ly_join
  FROM `{{ project }}.{{ dataset }}.sps_financial_metrics_prev_year`
  WHERE time_granularity = 'Monthly'
    AND join_time_period LIKE '2026%'
  GROUP BY global_entity_id
)
SELECT cy.global_entity_id, cy.max_cy, ly.max_ly_join,
  CASE WHEN ly.max_ly_join <= cy.max_cy THEN 'OK' ELSE 'ALERTA' END AS status
FROM cy
LEFT JOIN ly USING (global_entity_id)
ORDER BY global_entity_id;

-- Resultado esperado: todas las filas deben tener status = 'OK'
```

### 8.4 Validar sps_market_customers tiene todas las granularidades

```sql
SELECT
  time_granularity,
  COUNT(DISTINCT global_entity_id) AS entidades,
  COUNT(*) AS filas,
  SUM(total_market_customers) AS total_clientes
FROM `{{ project }}.{{ dataset }}.sps_market_customers`
GROUP BY time_granularity
ORDER BY time_granularity;

-- Resultado esperado: 3 filas (Monthly, Quarterly, YTD)
```

### 8.5 Validar sps_score_tableau tiene market_customers joineado

```sql
SELECT
  time_granularity,
  COUNTIF(total_market_customers IS NOT NULL) AS con_market,
  COUNTIF(total_market_customers IS NULL) AS sin_market,
  COUNT(*) AS total
FROM `{{ project }}.{{ dataset }}.sps_score_tableau`
GROUP BY time_granularity
ORDER BY time_granularity;

-- Nota: es normal que filas con supplier_level != 'supplier' tengan
-- total_market_customers NULL porque market_customers es a nivel de mercado,
-- no de categoria. Las filas con supplier_level = 'supplier' deben tener
-- total_market_customers NOT NULL en la gran mayoria.
```

### 8.6 Validar sps_supplier_scoring tiene 7 scores

```sql
SELECT
  global_entity_id,
  time_period,
  COUNT(*) AS suppliers,
  AVG(score_fill_rate) AS avg_fill,
  AVG(score_otd) AS avg_otd,
  AVG(score_yoy) AS avg_yoy,
  AVG(score_efficiency) AS avg_eff,
  AVG(score_gbd) AS avg_gbd,
  AVG(score_back_margin) AS avg_bm,
  AVG(score_front_margin) AS avg_fm
FROM `{{ project }}.{{ dataset }}.sps_supplier_scoring`
WHERE time_granularity = 'Monthly'
GROUP BY global_entity_id, time_period
ORDER BY global_entity_id, time_period;

-- Verificar que ningun score promedio sea 0.00 para todas las entidades
-- (indicaria que el calculo esta roto o los datos no llegan)
```

### 8.7 Validar que sps_score_tableau_init ya no se referencia

```sql
-- Este query debe fallar o retornar 0 filas en produccion nueva
SELECT COUNT(*) FROM `{{ project }}.{{ dataset }}.sps_score_tableau_init`;

-- Si la tabla existe, es residual de la version anterior.
-- Puede eliminarse con DROP TABLE una vez confirmado que nada la referencia.
```

### 8.8 Comparar volumenes entre version actual y nueva

```sql
-- Ejecutar en ambos datasets y comparar
SELECT
  'sps_financial_metrics' AS tabla,
  time_granularity,
  COUNT(*) AS filas,
  COUNT(DISTINCT global_entity_id) AS entidades,
  SUM(Net_Sales_eur) AS total_net_sales
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
GROUP BY time_granularity
ORDER BY time_granularity;

-- Las filas Monthly y Quarterly deben tener numeros similares (diferencia < 2%
-- por APPROX_COUNT_DISTINCT en campos auxiliares, pero Net_Sales_eur debe coincidir
-- exactamente). Las filas YTD son completamente nuevas.
```

### 8.9 Validar segmentacion cubre todas las entidades

```sql
SELECT
  global_entity_id,
  segment,
  COUNT(*) AS suppliers
FROM `{{ project }}.{{ dataset }}.sps_supplier_segmentation`
WHERE time_granularity = 'Monthly'
GROUP BY global_entity_id, segment
ORDER BY global_entity_id, segment;

-- Resultado esperado: 4 segmentos por entidad (Key Account, Standard, Niche, Long Tail)
-- Es normal que alguna entidad no tenga los 4 si no hay suficientes suppliers.
```

---

*Documento generado para facilitar la migracion del pipeline SPS a produccion. Ante cualquier duda sobre la logica de negocio, contactar a Christian La Rosa.*
