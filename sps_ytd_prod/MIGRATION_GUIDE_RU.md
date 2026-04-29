# Руководство по миграции SPS: sps_sql_new --> sps_ytd_prod

> **Версия:** 1.0  
> **Дата:** 2026-04-29  
> **Автор:** Christian La Rosa  
> **Целевая аудитория:** Ion (Analytics Engineer), команда Data Engineering  
> **Цель:** Загрузка `sps_ytd_prod/` в production-окружение, замена текущих скриптов из `sps_sql_new/`.

---

## 1. Краткое резюме (Executive Summary)

Пайплайн SPS (Supplier Performance Scorecard) переходит с архитектуры `sps_sql_new/` (24 скрипта, Monthly + Quarterly) на архитектуру `sps_ytd_prod/` (29 скриптов, Monthly + Quarterly + YTD).

**Основные изменения:**

| Аспект | sps_sql_new (текущий) | sps_ytd_prod (новый) |
|--------|----------------------|----------------------|
| Временная гранулярность | Monthly + Quarterly | Monthly + Quarterly + **YTD** |
| Скрипты | 24 файла | 29 файлов (+5 новых) |
| Категории | master_category (L1/L2/L3) | + **front_facing** (L1/L2) |
| COUNT DISTINCT | Точный (`COUNT(DISTINCT ...)`) | **APPROX_COUNT_DISTINCT** |
| Prev Year | Прямая агрегация | **Pattern F** (лимит по сущности) |
| Окно данных | Скользящее 4 квартала | **Год календарный** (CY + PY) |
| Дедупликация basket-level | `SUM(amt_*)` напрямую | **ROW_NUMBER() dedup** |
| Слой скоринга | Отсутствует | **3 новых скрипта** |
| Слой рынка | Отсутствует | **2 новых скрипта** |

**Обратная совместимость:** Полная. Все поля из `sps_sql_new` присутствуют в `sps_ytd_prod`. Новые поля добавлены аддитивно. Tableau-дашборды продолжат работать без изменений, получая дополнительно YTD-данные.

---

## 2. Архитектура пайплайна (Pipeline Architecture)

### 2.1 Полная таблица скриптов (29 файлов)

| # | Скрипт | sps_sql_new | sps_ytd_prod | Изменения |
|---|--------|:-----------:|:------------:|-----------|
| 1 | `sps_customer_order.sql` | Есть | Есть | Без изменений (upstream) |
| 2 | `sps_product.sql` | Есть | Есть | Без изменений (upstream) |
| 3 | `sps_supplier_hierarchy.sql` | Есть | Есть | Без изменений (upstream) |
| 4 | `sps_financial_metrics_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 5 | `sps_financial_metrics.sql` | Есть | Есть | **Два CTE** (monthly_quarterly + ytd), APPROX_COUNT_DISTINCT, front_facing, ROW_NUMBER dedup |
| 6 | `sps_financial_metrics_prev_year.sql` | Есть | Есть | **Pattern F** (max_date_per_entity), +YTD-гранулярность, +front_facing |
| 7 | `sps_efficiency_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 8 | `sps_efficiency.sql` | Есть | Есть | **Два CTE** (sku_counts + gpv_data), front_facing, date_config |
| 9 | `sps_line_rebate_metrics_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 10 | `sps_line_rebate_metrics.sql` | Есть | Есть | **Два CTE**, front_facing, APPROX_COUNT_DISTINCT |
| 11 | `sps_price_index_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 12 | `sps_price_index.sql` | Есть | Есть | **Два CTE**, front_facing |
| 13 | `sps_days_payable_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 14 | `sps_days_payable.sql` | Есть | Есть | **Два CTE**, front_facing |
| 15 | `sps_listed_sku_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 16 | `sps_listed_sku.sql` | Есть | Есть | **Два CTE**, front_facing |
| 17 | `sps_shrinkage_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 18 | `sps_shrinkage.sql` | Есть | Есть | **Два CTE**, front_facing |
| 19 | `sps_delivery_costs_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 20 | `sps_delivery_costs.sql` | Есть | Есть | **Два CTE**, front_facing |
| 21 | `sps_purchase_order_month.sql` | Есть | Есть | +`ytd_year`, +`front_facing_level_one/two` |
| 22 | `sps_purchase_order.sql` | Есть | Есть | **Два CTE**, front_facing |
| 23 | `sps_score_tableau.sql` | Есть | Есть | +`sps_market_customers` JOIN, +`total_market_customers/orders`, +`Total_Margin_LC`, field cleanup |
| 24 | `sps_score_tableau_init.sql` | Есть | -- | **Удален** (заменен новой архитектурой all_keys) |
| 25 | `sps_market_customers.sql` | -- | **Новый** | Клиенты/заказы на уровне рынка (Monthly/Quarterly/YTD) |
| 26 | `sps_market_yoy.sql` | -- | **Новый** | YoY-рост рынка (для потолка YoY в скоринге) |
| 27 | `sps_scoring_params.sql` | -- | **Новый** | Параметры скоринга: пороги BM, FM, GBD по сущности |
| 28 | `sps_supplier_scoring.sql` | -- | **Новый** | Расчет score по 7 KPI (200 баллов) |
| 29 | `sps_supplier_segmentation.sql` | -- | **Новый** | 4 сегмента: Key Account / Standard / Niche / Long Tail |
| 30 | `sps_supplier_master.sql` | -- | **Новый** | Мастер-таблица поставщиков с ratios, именами, scoring |

**Итого:** 24 --> 29 скриптов. Добавлено 6, удален 1 (`sps_score_tableau_init`).

### 2.2 Архитектурная диаграмма

```
GATHERING LAYER (12 скриптов) — Сбор данных из внешних источников + внутренний маппинг
  L0: sps_supplier_hierarchy           Salesforce SRM → иерархия
  L1: sps_product                      каталог + PO → SKU маппинг
  L2: sps_customer_order + 9 _month    все параллельно после product
    sps_financial_metrics_month          +ytd_year, +front_facing
    sps_efficiency_month                 +ytd_year, +front_facing
    sps_line_rebate_metrics_month        +ytd_year, +front_facing
    sps_price_index_month                +ytd_year, +front_facing
    sps_days_payable_month               +ytd_year, +front_facing
    sps_listed_sku_month                 +ytd_year, +front_facing
    sps_shrinkage_month                  +ytd_year, +front_facing
    sps_delivery_costs_month             +ytd_year, +front_facing
    sps_purchase_order_month             +ytd_year, +front_facing

GROUPING SETS LAYER (11 скриптов) — Агрегация _month → Monthly + Quarterly + YTD + Front-Facing
  sps_financial_metrics                Два CTE: monthly_quarterly + ytd
  sps_financial_metrics_prev_year      Pattern F + YTD
  sps_efficiency                       Два CTE: sku_counts + gpv_data
  sps_line_rebate_metrics              Два CTE
  sps_price_index                      Два CTE
  sps_days_payable                     Два CTE
  sps_listed_sku                       Два CTE
  sps_shrinkage                        Два CTE
  sps_delivery_costs                   Два CTE
  sps_purchase_order                   Два CTE
  Ограничение: financial_metrics_month → prev_year → financial_metrics

UNION ALL LAYER (1 скрипт) — Сборка star schema: all_keys UNION + 10 LEFT JOINs
  sps_score_tableau                    all_keys UNION + 10 LEFT JOIN

SEGMENTATION LAYER (1 скрипт) — Классификация поставщиков в 4 квадранта
  sps_supplier_segmentation            4 сегмента

SCORING LAYER (3 скрипта) — Модель 200 баллов + рыночные бенчмарки
  sps_scoring_params                   Пороги BM/FM/GBD
  sps_market_yoy                       YoY рынка по сущности
  sps_supplier_scoring                 7 KPI x 200 баллов

MASTER (1 скрипт) — Финальная денормализованная таблица
  sps_supplier_master                  Финальная таблица для downstream

Зависимости: Gathering → Grouping Sets → Union All → [Segmentation ∥ Scoring] → Master
```

---

## 3. Новые функции (New Features)

### 3.1 Гранулярность YTD (Year-to-Date)

**Проблема:** Текущий пайплайн поддерживает только Monthly и Quarterly гранулярности. Бизнес-команды просят YTD-метрики для оценки эффективности поставщиков за накопленный период с начала года.

**Решение:** Во всех группировочных скриптах применен паттерн «два CTE»:

```sql
-- CTE 1: Monthly + Quarterly (через GROUPING SETS)
monthly_quarterly_data AS (
  SELECT ...
  GROUP BY GROUPING SETS (
    (month, ...),      -- Monthly
    (quarter_year, ...) -- Quarterly
  )
),

-- CTE 2: YTD (отдельный GROUPING SETS с ytd_year)
ytd_data AS (
  SELECT ...
  WHERE EXTRACT(YEAR FROM ...) = current_year  -- Только текущий год
  GROUP BY GROUPING SETS (
    (ytd_year, ...)    -- YTD
  )
)

-- Объединение
SELECT * FROM monthly_quarterly_data
UNION ALL
SELECT * FROM ytd_data
```

**Почему два CTE, а не три GROUPING в одном блоке?**

COUNT DISTINCT не аддитивен. Нельзя просто добавить `ytd_year` в существующий GROUPING SETS — результат был бы математически некорректным. YTD COUNT DISTINCT клиентов за весь год != SUM(COUNT DISTINCT клиентов по месяцам). Клиент, купивший в январе и марте, должен считаться **один раз** в YTD, а не дважды.

**Значения time_period для YTD:**

| time_granularity | time_period (пример) | Описание |
|-----------------|---------------------|----------|
| Monthly | 2026-01-01 | Строка даты |
| Quarterly | Q1-2026 | Формат Qn-YYYY |
| **YTD** | **YTD-2026** | Формат YTD-YYYY |

**Каждый _month-скрипт** получил новое поле:

```sql
EXTRACT(YEAR FROM os.order_date) AS ytd_year
```

Это поле используется как ось агрегации в CTE `ytd_data`.

### 3.2 Front-Facing категории

**Проблема:** Текущий пайплайн группирует данные только по master_category (L1/L2/L3). Бизнес-пользователи работают с front-facing категориями, которые отображаются клиентам в приложении.

**Решение:** Во всех `_month`-скриптах добавлены два новых поля:

```sql
COALESCE(os.front_facing_level_one, '_unknown_') AS front_facing_level_one,
COALESCE(os.front_facing_level_two, '_unknown_') AS front_facing_level_two
```

В группировочных скриптах добавлены GROUPING SETS для front_facing:

```sql
-- 4. FRONT-FACING CATEGORY DEEP-DIVE (новый блок)
(month, global_entity_id, principal_supplier_id, front_facing_level_one),
(month, global_entity_id, principal_supplier_id, front_facing_level_two),
(month, global_entity_id, supplier_id, front_facing_level_one),
(month, global_entity_id, supplier_id, front_facing_level_two),
(month, global_entity_id, brand_owner_name, front_facing_level_one),
(month, global_entity_id, brand_owner_name, front_facing_level_two),
(month, global_entity_id, brand_name, front_facing_level_one),
(month, global_entity_id, brand_name, front_facing_level_two),
```

**Влияние на entity_key и supplier_level:**

```sql
-- entity_key: добавлены два новых уровня в COALESCE-цепочку
COALESCE(
  IF(GROUPING(l3_master_category) = 0,       l3_master_category, NULL),
  IF(GROUPING(l2_master_category) = 0,       l2_master_category, NULL),
  IF(GROUPING(l1_master_category) = 0,       l1_master_category, NULL),
  IF(GROUPING(front_facing_level_two) = 0,   front_facing_level_two, NULL),  -- НОВЫЙ
  IF(GROUPING(front_facing_level_one) = 0,   front_facing_level_one, NULL),  -- НОВЫЙ
  ...
) AS entity_key,

-- supplier_level: два новых значения
CASE
  ...
  WHEN GROUPING(front_facing_level_two) = 0 THEN 'front_facing_level_two'  -- НОВЫЙ
  WHEN GROUPING(front_facing_level_one) = 0 THEN 'front_facing_level_one'  -- НОВЫЙ
  ...
END AS supplier_level,
```

**Количество GROUPING SETS на скрипт:**

| Период | sps_sql_new | sps_ytd_prod | Дельта |
|--------|:-----------:|:------------:|:------:|
| Monthly | 19 комбинаций | 27 комбинаций | +8 (front_facing) |
| Quarterly | 19 комбинаций | 27 комбинаций | +8 (front_facing) |
| YTD | -- | 27 комбинаций | +27 (новый) |
| **Итого** | 38 | **81** | +43 |

### 3.3 Слой скоринга (новые скрипты)

Три полностью новых скрипта формируют слой скоринга поставщиков.

#### sps_scoring_params.sql

**Назначение:** Расчет динамических порогов для каждого KPI по каждой сущности (global_entity_id) и периоду.

**Метрики и методология:**

| Параметр | Методология | Описание |
|----------|-------------|----------|
| `bm_starting` | P25 back_margin_ratio (только поставщики с ребейтами) | Нижняя граница |
| `bm_ending` | `LEAST(GREATEST((IQR_mean * 1.5 + P75) / 2, weighted_avg), 0.70)` | Верхняя граница (потолок 70%) |
| `fm_starting` | `GREATEST(0.12, P25)` (минимум 12%) | Нижняя граница front margin |
| `fm_ending` | `LEAST(GREATEST((IQR_mean * 1.25 + P75) / 2, weighted_avg, starting + 0.08), 0.70)` | Верхняя граница |
| `gbd_target` | Хардкод по сущности (19 сущностей) | Целевой GBD |
| `gbd_lower` | `gbd_target * 0.5` | Нижняя граница GBD |
| `gbd_upper` | `gbd_target * 2.0` | Верхняя граница GBD |

**Фильтры входных данных:** `supplier_level = 'supplier'`, `time_granularity = 'Monthly'`, `division_type = 'division'`, `Net_Sales_eur > 1000`, `COGS_lc > 0`.

#### sps_supplier_scoring.sql

**Назначение:** Расчет баллов по 7 KPI для каждого поставщика. Максимум: 200 баллов.

| KPI | Макс. баллы | Формула |
|-----|:-----------:|---------|
| Fill Rate | 60 | `MIN(fill_rate, 1.0) * 60` |
| OTD | 40 | `MIN(otd, 1.0) * 40` |
| YoY Growth | 10 | Линейная шкала 0 --> `yoy_max` (динамический потолок) |
| Efficiency | 30 | Линейная шкала 0.40 --> 1.0 |
| GBD | 20 | Асимметричная колоколообразная кривая вокруг target |
| Back Margin | 25 | Линейная шкала `bm_starting` --> `bm_ending` |
| Front Margin | 15 | Линейная шкала `fm_starting` --> `fm_ending` |

**YTD-scoring:** Для YTD-периодов параметры берутся из последнего Monthly-периода (через CTE `params_key`), а не из несуществующих YTD-параметров.

**Динамический потолок YoY:** `yoy_max = LEAST(GREATEST(market_yoy * 1.2, 0.20), 0.70)` -- привязан к росту рынка из `sps_market_yoy`.

#### sps_supplier_segmentation.sql

**Назначение:** Классификация поставщиков в 4 сегмента по двум осям.

**Ось 1 -- Важность (Importance):**
```
Net_Profit_LC = Net_Sales_lc + total_supplier_funding_lc - COGS_lc + back_margin_amt_lc
```
Ранг определяется через PERCENT_RANK() по Net_Profit_LC в пределах (global_entity_id, time_period).

**Ось 2 -- Продуктивность (Productivity):**

| Компонент | Вес | Сигнал |
|-----------|:---:|--------|
| Customer Penetration | 40 | Охват рынка (r=0.2718) |
| Frequency | 30 | Лояльность (r=0.2686) |
| ABV (Average Basket Value) | 30 | Стратегический тип (r=-0.079) |

**Матрица сегментации:**

| | Importance >= P50 | Importance < P50 |
|--|:-----------------:|:----------------:|
| **Productivity >= P50** | Key Account | Standard |
| **Productivity < P50** | Niche | Long Tail |

### 3.4 Слой рынка (новые скрипты)

#### sps_market_customers.sql

**Назначение:** Количество уникальных клиентов и заказов на уровне платформы (без фильтра по поставщику). Это знаменатель для customer_penetration в сегментации.

**Гранулярность:** `global_entity_id x time_period x time_granularity`

**Важный нюанс:** Quarterly и YTD метрики рассчитываются через COUNT DISTINCT по всему периоду, а НЕ как сумма месячных значений. Клиент, купивший в январе и марте, = 1 клиент в YTD, а не 2.

**Структура:**

```sql
SELECT * FROM monthly      -- По месяцам
UNION ALL
SELECT * FROM quarterly    -- По кварталам (COUNT DISTINCT за квартал)
UNION ALL
SELECT * FROM ytd          -- За YTD (COUNT DISTINCT за весь год)
```

#### sps_market_yoy.sql

**Назначение:** YoY-рост Net_Sales по рынку (все поставщики). Используется для динамического потолка `yoy_max` в `sps_supplier_scoring`.

**Фильтры:** `supplier_level = 'supplier'`, `division_type = 'division'`, `time_granularity = 'Monthly'`, `Net_Sales_eur > 1000`, `Net_Sales_eur_Last_Year > 0`.

#### sps_supplier_master.sql

**Назначение:** Финальная мастер-таблица, объединяющая `sps_score_tableau` с именами поставщиков (из `sps_product`), названием бренда-сущности (mapped от `global_entity_id` prefix), и ключевыми ratios.

**Функции:**
- Маппинг `supplier_id` --> `supplier_name` через `sps_product`
- Маппинг `global_entity_id` --> `global_entity_name` (PedidosYa, Talabat, Pandora, Glovo, и т.д.)
- Расчет ratios: OTD, Fill Rate, Efficiency, Price Index, YoY, Back Margin, Front Margin, GBD, Promo Contribution, Spoilage Rate

### 3.5 Оптимизация APPROX_COUNT_DISTINCT

**Проблема:** `COUNT(DISTINCT x)` в BigQuery при большом объеме данных и множестве GROUPING SETS значительно увеличивает потребление слотов и время выполнения.

**Решение:** Все COUNT DISTINCT в группировочных скриптах заменены на `APPROX_COUNT_DISTINCT`:

```sql
-- sps_sql_new (текущий):
COUNT(DISTINCT analytical_customer_id) AS total_customers,
COUNT(DISTINCT sku_id) AS total_skus_sold,
COUNT(DISTINCT order_id) AS total_orders,
COUNT(DISTINCT warehouse_id) AS total_warehouses_sold,

-- sps_ytd_prod (новый):
APPROX_COUNT_DISTINCT(analytical_customer_id) AS total_customers,
APPROX_COUNT_DISTINCT(sku_id) AS total_skus_sold,
APPROX_COUNT_DISTINCT(order_id) AS total_orders,
APPROX_COUNT_DISTINCT(warehouse_id) AS total_warehouses_sold,
```

**Точность:** APPROX_COUNT_DISTINCT в BigQuery гарантирует погрешность менее 1% для большинства распределений. Для SPS-метрик (тысячи-миллионы уникальных значений) погрешность пренебрежимо мала.

**Почему это безопасно:** SPS-метрики используются для сравнительной оценки поставщиков (percentile rank), а не для точного учета. Погрешность 0.5% в total_customers не влияет на ранжирование.

### 3.6 Паттерн F для Prev Year (лимит по сущности)

**Проблема:** В текущем `sps_financial_metrics_prev_year.sql` прошлогодние данные агрегируются за полный прошлый год. Но если сущность запустилась в марте текущего года, сравнение YTD-2026 (янв-мар) с полным LY (все 12 месяцев) дает искаженный YoY.

**Решение (Pattern F):** Для каждой сущности определяется максимальная дата в текущем году (`max_month_cy`), и прошлогодние данные обрезаются до аналогичного периода:

```sql
-- Шаг 1: Найти max_date по сущности в текущем году
max_date_cy AS (
  SELECT
    global_entity_id,
    MAX(CAST(month AS DATE)) AS max_month_cy
  FROM sps_financial_metrics_month
  WHERE EXTRACT(YEAR FROM CAST(month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY global_entity_id
),

-- Шаг 2: Отфильтровать LY-данные по лимиту каждой сущности
filtered_ly AS (
  SELECT m.*
  FROM sps_financial_metrics_month m
  JOIN max_date_cy mx ON m.global_entity_id = mx.global_entity_id
  WHERE
    EXTRACT(YEAR FROM CAST(m.month AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE()) - 1
    AND CAST(m.month AS DATE) <= DATE_SUB(LAST_DAY(mx.max_month_cy), INTERVAL 1 YEAR)
),

-- Шаг 3: Агрегация с GROUPING SETS (включая ytd_year)
```

**Сдвиг join_time_period:** Прошлогодние периоды сдвигаются на +1 год для корректного JOIN:

```sql
-- Monthly: "2025-03-01" --> "2026-03-01"
CAST(DATE_ADD(DATE(month), INTERVAL 1 YEAR) AS STRING) AS join_time_period,

-- Quarterly: "Q1-2025" --> "Q1-2026"
CONCAT(SUBSTR(quarter_year, 1, 2), '-', CAST(CAST(SUBSTR(quarter_year, 4) AS INT64) + 1 AS STRING))

-- YTD: "YTD-2025" --> "YTD-2026" (НОВЫЙ)
CONCAT('YTD-', CAST(CAST(ytd_year AS INT64) + 1 AS STRING))
```

### 3.7 Разрешение имени поставщика в score_tableau

**Проблема:** Поле `brand_sup` в score_tableau содержит ID поставщиков (например, "SUP-12345") для строк с `division_type IN ('division', 'principal')`. Эти ID нечитаемы для пользователей. Tableau-дашборды, слой скоринга и слой сегментации требуют человекочитаемых имен поставщиков. Строки brand_owner и brand_name уже содержат читаемые значения в `brand_sup`, поэтому для них разрешение не нужно.

**Как работает:**

1. CTE `sps_product_clean` извлекает уникальные маппинги `supplier_id -> supplier_name` из `sps_product`.
2. LEFT JOIN сопоставляет `brand_sup` с `supplier_id` **только** для строк с `division_type IN ('division', 'principal')`.
3. Новая колонка `supplier_name` использует CASE-выражение:

```sql
CASE
  WHEN division_type IN ('division', 'principal')
    THEN COALESCE(prod.supplier_name, brand_sup)  -- разрешенное имя с fallback
  ELSE brand_sup                                   -- уже читаемое (brand_owner, brand_name, total)
END AS supplier_name
```

**Примечание:** Эта функциональность НЕ существовала в `sps_sql_new` (текущий production). Это новое дополнение из `sps_ytd_prod`.

---

## 4. Параметризация Jinja

Все скрипты используют Jinja-шаблоны для параметризации. **Формат не изменился:**

| Параметр | Описание | Пример |
|----------|----------|--------|
| `{{ params.project_id }}` | ID проекта GCP | `fulfillment-dwh-production` |
| `{{ params.dataset.cl }}` | Имя датасета | `csm_automated_tables` |
| `{{ next_ds }}` | Следующая дата выполнения (Airflow) | `2026-04-29` |
| `{{ params.stream_look_back_days }}` | Lookback для инкрементальной загрузки | `90` |
| `{{ params.backfill }}` | Флаг backfill-режима | `true/false` |
| `{{ params.backfill_start_date }}` | Начало backfill | `2025-01-01` |
| `{{ params.backfill_end_date }}` | Конец backfill | `2025-12-31` |

**Без изменений в Jinja-параметрах.** Все новые скрипты используют те же `params.project_id` и `params.dataset.cl`.

---

## 5. Изменения по скриптам

### 5.1 _month-скрипты (Gathering Layer)

Все 9 `_month`-скриптов получили одинаковые изменения:

| Изменение | Описание |
|-----------|----------|
| +`ytd_year` | `EXTRACT(YEAR FROM os.order_date) AS ytd_year` |
| +`front_facing_level_one` | `COALESCE(os.front_facing_level_one, '_unknown_')` |
| +`front_facing_level_two` | `COALESCE(os.front_facing_level_two, '_unknown_')` |

**Затронутые файлы:**
- `sps_financial_metrics_month.sql`
- `sps_efficiency_month.sql`
- `sps_line_rebate_metrics_month.sql`
- `sps_price_index_month.sql`
- `sps_days_payable_month.sql`
- `sps_listed_sku_month.sql`
- `sps_shrinkage_month.sql`
- `sps_delivery_costs_month.sql`
- `sps_purchase_order_month.sql`

### 5.2 Группировочные скрипты (Grouping Sets Layer)

Все 10 группировочных скриптов перешли на архитектуру двух CTE:

| Паттерн | sps_sql_new | sps_ytd_prod |
|---------|-------------|--------------|
| CTE | Один `current_year_data` | `monthly_quarterly_data` + `ytd_data` |
| GROUPING SETS | Monthly + Quarterly | Monthly + Quarterly (CTE1) / YTD (CTE2) |
| Фильтр данных | `lookback_limit` (4 квартала) | `date_config` (CY + PY по calendar year) |
| COUNT DISTINCT | Точный | APPROX_COUNT_DISTINCT |
| UNION | Нет | `UNION ALL` (CTE1 + CTE2) |

**Затронутые файлы:**
- `sps_financial_metrics.sql`
- `sps_efficiency.sql`
- `sps_line_rebate_metrics.sql`
- `sps_price_index.sql`
- `sps_days_payable.sql`
- `sps_listed_sku.sql`
- `sps_shrinkage.sql`
- `sps_delivery_costs.sql`
- `sps_purchase_order.sql`
- `sps_financial_metrics_prev_year.sql`

### 5.3 sps_financial_metrics.sql -- детальное сравнение

| Аспект | sps_sql_new | sps_ytd_prod |
|--------|-------------|--------------|
| Структура | Один CTE `current_year_data` | Два CTE: `monthly_quarterly_data` + `ytd_data` |
| date_config CTE | Нет | `SELECT CURRENT_DATE() AS today, EXTRACT(YEAR) AS current_year, current_year - 1 AS prior_year` |
| Фильтр данных | `WHERE DATE(month) >= lookback_limit` | `WHERE EXTRACT(YEAR) = current_year AND month <= today OR EXTRACT(YEAR) = prior_year` |
| entity_key COALESCE | 7 уровней | **9 уровней** (+front_facing_level_two, +front_facing_level_one) |
| supplier_level CASE | 5 значений | **7 значений** (+front_facing_level_two, +front_facing_level_one) |
| Basket-level dedup | `SUM(amt_total_price_paid_net_eur)` | **ROW_NUMBER() dedup** через `amt_total_price_paid_net_eur_dedup` |
| COUNT DISTINCT | `COUNT(DISTINCT ...)` | `APPROX_COUNT_DISTINCT(...)` |
| Quarterly time_period | `ELSE quarter_year` | `WHEN GROUPING(quarter_year) = 0 THEN quarter_year` (явный) |
| GROUPING SETS | 38 (19 Monthly + 19 Quarterly) | **81** (27 Monthly + 27 Quarterly + 27 YTD) |

**ROW_NUMBER() дедупликация (критический баг-фикс):**

В старом пайплайне `amt_total_price_paid_net_eur` (basket-level метрика) суммировалась по всем SKU заказа, что приводило к инфляции при наличии нескольких строк для одного заказа+поставщика+месяца. Новый пайплайн использует:

```sql
CASE WHEN ROW_NUMBER() OVER (
  PARTITION BY src.global_entity_id, src.order_id, src.supplier_id, src.month
  ORDER BY src.sku_id
) = 1
THEN src.amt_total_price_paid_net_eur ELSE 0 END AS amt_total_price_paid_net_eur_dedup
```

Только первая строка каждого (order, supplier, month) несет basket-level значение; остальные получают 0.

### 5.4 sps_financial_metrics_prev_year.sql -- детальное сравнение

| Аспект | sps_sql_new | sps_ytd_prod |
|--------|-------------|--------------|
| Фильтр LY | Прямая фильтрация по году | **Pattern F**: per-entity max_date capping |
| CTE | Нет (прямой SELECT) | 3 CTE: `max_date_cy` + `filtered_ly` + `aggregated` |
| join_time_period | Monthly + Quarterly | Monthly + Quarterly + **YTD** |
| front_facing | Нет | **Есть** (в entity_key и supplier_level) |
| time_granularity | Monthly / Quarterly | Monthly / Quarterly / **YTD** |

### 5.5 sps_score_tableau.sql -- детальное сравнение

| Аспект | sps_sql_new | sps_ytd_prod |
|--------|-------------|--------------|
| all_keys UNION | 9 таблиц | 10 таблиц (те же 9 + `sps_delivery_costs`) |
| LEFT JOIN | 9 JOIN-ов | 10 JOIN-ов (+`sps_market_customers`) + CTE `sps_product_clean` (supplier_name, см. 3.7) |
| `supplier_name` | Нет | **Новый** -- CASE: division/principal -> разрешенное имя из sps_product, остальные -> brand_sup |
| `total_market_customers` | Нет | **Есть** (из sps_market_customers) |
| `total_market_orders` | Нет | **Есть** (из sps_market_customers) |
| `Total_Margin_LC` | Нет | **Есть** (рассчитан как ratio) |
| `back_margin_amt_lc` | Из EXCEPT wildcard | **Явно** = `slrm.total_rebate` |
| `back_margin_wo_dist_allowance_amt_lc` | Нет | **Есть** |
| `listed_skus_efficiency` | Нет | **Есть** (= `se.sku_listed`, дубль для Tableau) |
| Efficiency fields | `se.*` (wildcard) | **Явный список** (sku_listed, sku_mature, sku_new, weight_efficiency, gpv_eur, ...) |
| Удаленные поля | -- | `new_zero_movers`, `new_slow_movers`, `new_efficient_movers`, `sold_items`, `new_availability` |
| `sps_score_tableau_init` | Используется как альтернатива | **Удален** |

**Формула Total_Margin_LC:**

```sql
CAST(ROUND(SAFE_DIVIDE(
  sfm.Net_Sales_lc + sfm.total_supplier_funding_lc - sfm.COGS_lc + COALESCE(slrm.total_rebate, 0.0),
  NULLIF(sfm.Net_Sales_lc, 0)
), 4) AS NUMERIC) AS Total_Margin_LC
```

### 5.6 sps_score_tableau_init.sql -- удаление

Скрипт `sps_score_tableau_init.sql` из `sps_sql_new/` **удален** в новом пайплайне. Его функциональность полностью покрыта архитектурой `all_keys` в `sps_score_tableau.sql`. Старая версия использовала `sps_financial_metrics` как базовую таблицу (FROM), что означало потерю строк, существующих только в других таблицах. Новая архитектура `all_keys` UNION гарантирует полное покрытие ключевого пространства.

---

## 6. Порядок выполнения

### 6.1 Слои и порядок загрузки

```
1. GATHERING LAYER (12 скриптов) — Сбор данных из внешних источников + внутренний маппинг
   L0: sps_supplier_hierarchy             (Salesforce SRM → иерархия)
   L1: sps_product                        (каталог + PO → SKU маппинг)
   L2: sps_customer_order + 9 _month      (все параллельно после product)
     - sps_financial_metrics_month
     - sps_efficiency_month
     - sps_line_rebate_metrics_month
     - sps_price_index_month
     - sps_days_payable_month
     - sps_listed_sku_month
     - sps_shrinkage_month
     - sps_delivery_costs_month
     - sps_purchase_order_month
                    |
                    v
2. GROUPING SETS LAYER (11 скриптов) — Агрегация _month → Monthly + Quarterly + YTD + Front-Facing
   Паттерн two-CTE, 81 GROUPING SETS на скрипт
   Внутреннее ограничение: financial_metrics_month → prev_year → financial_metrics
     - sps_financial_metrics
     - sps_financial_metrics_prev_year
     - sps_efficiency
     - sps_line_rebate_metrics
     - sps_price_index
     - sps_days_payable
     - sps_listed_sku
     - sps_shrinkage
     - sps_delivery_costs
     - sps_purchase_order
     - sps_market_customers
                    |
                    v
3. UNION ALL LAYER (1 скрипт) — Сборка star schema: all_keys UNION + 10 LEFT JOINs
     - sps_score_tableau
                    |
                    v
4-5. [SEGMENTATION ∥ SCORING] (параллельно)

   4. SEGMENTATION LAYER (1 скрипт) — Классификация поставщиков в 4 квадранта
        - sps_supplier_segmentation

   5. SCORING LAYER (3 скрипта) — Модель 200 баллов + рыночные бенчмарки
        - sps_scoring_params
        - sps_market_yoy
        - sps_supplier_scoring
                    |
                    v
6. MASTER (1 скрипт) — Финальная денормализованная таблица
     - sps_supplier_master
```

**Зависимости:** Gathering → Grouping Sets → Union All → [Segmentation ∥ Scoring] → Master

**Критический путь:** sps_customer_order → sps_financial_metrics_month → sps_financial_metrics → sps_score_tableau → sps_scoring_params → sps_supplier_scoring → sps_supplier_master

### 6.2 Таблица зависимостей

| Слой | Скрипт | Зависит от |
|------|--------|-----------|
| Gathering | `sps_supplier_hierarchy` | Salesforce SRM (upstream) |
| Gathering | `sps_product` | каталог + PO (upstream) |
| Gathering | `sps_customer_order` | `sps_product`, `sps_supplier_hierarchy` |
| Gathering | `sps_financial_metrics_month` | `sps_customer_order`, `sps_supplier_hierarchy` |
| Gathering | 8 других `*_month` | `sps_customer_order` |
| Grouping Sets | `sps_financial_metrics` | `sps_financial_metrics_month` |
| Grouping Sets | `sps_financial_metrics_prev_year` | `sps_financial_metrics_month` |
| Grouping Sets | 8 других группировочных | соответствующий `*_month` |
| Grouping Sets | `sps_market_customers` | `sps_customer_order` |
| Union All | `sps_score_tableau` | Все 11 скриптов Grouping Sets Layer + `sps_product` (lookup supplier_name) |
| Segmentation | `sps_supplier_segmentation` | `sps_score_tableau`, `sps_market_customers` |
| Scoring | `sps_scoring_params` | `sps_score_tableau` |
| Scoring | `sps_market_yoy` | `sps_score_tableau` |
| Scoring | `sps_supplier_scoring` | `sps_score_tableau`, `sps_scoring_params`, `sps_market_yoy` |
| Master | `sps_supplier_master` | `sps_score_tableau`, `sps_product` |

---

## 7. Точки внимания (ytd_test --> sps_ytd_prod)

### 7.1 Переименование dataset

При загрузке в production все ссылки на `{{ params.dataset.cl }}` будут указывать на production-датасет. Убедись, что:

- Production-датасет существует и доступен.
- Все таблицы создаются через `CREATE OR REPLACE TABLE` (идемпотентность).
- Старые таблицы из `sps_sql_new` НЕ удаляются до завершения валидации.

### 7.2 Новые таблицы в production

Следующие таблицы **не существуют** в текущем production-окружении и будут созданы впервые:

| Таблица | Назначение |
|---------|-----------|
| `sps_market_customers` | Клиенты рынка |
| `sps_market_yoy` | YoY рынка |
| `sps_scoring_params` | Параметры скоринга |
| `sps_supplier_scoring` | Баллы поставщиков |
| `sps_supplier_segmentation` | Сегментация поставщиков |
| `sps_supplier_master` | Мастер-таблица |

### 7.3 Удаленная таблица

| Таблица | Действие |
|---------|----------|
| `sps_score_tableau_init` | Больше не создается. Можно удалить после валидации. |

### 7.4 Увеличение объема данных

Добавление YTD-гранулярности и front-facing категорий значительно увеличивает количество строк. Ожидаемое увеличение:

| Таблица | sps_sql_new (строки) | sps_ytd_prod (оценка) | Фактор |
|---------|:--------------------:|:--------------------:|:------:|
| `sps_financial_metrics` | ~X | ~X * 2.1 | x2.1 |
| `sps_score_tableau` | ~X | ~X * 2.1 | x2.1 |
| Другие групп. скрипты | ~X | ~X * 2.1 | x2.1 |

Причина: +8 front-facing GROUPING SETS на каждый временной период + полный YTD-блок.

### 7.5 Проверка front_facing в upstream

Поля `front_facing_level_one` и `front_facing_level_two` должны присутствовать в `sps_customer_order`. Если upstream-таблица не содержит этих полей:
- Скрипты `_month` упадут с ошибкой.
- Решение: добавить поля в `sps_customer_order.sql` upstream (SELECT из order_item или product).

### 7.6 Backfill

При первом запуске рекомендуется выполнить backfill за последние 2 года для корректного расчета YoY и LY-метрик. Используй стандартный механизм Airflow с `params.backfill = true`.

---

## 8. Запросы валидации

### 8.1 Проверка time_granularity

```sql
-- Проверить наличие всех трех гранулярностей
SELECT
  time_granularity,
  COUNT(*) AS row_count,
  COUNT(DISTINCT global_entity_id) AS entity_count
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
GROUP BY time_granularity
ORDER BY time_granularity;

-- Ожидаемый результат:
-- Monthly    | N      | 19
-- Quarterly  | N      | 19
-- YTD        | N      | 19
```

### 8.2 Проверка front_facing supplier_level

```sql
-- Проверить наличие front_facing уровней
SELECT
  supplier_level,
  COUNT(*) AS row_count
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
WHERE supplier_level IN ('front_facing_level_one', 'front_facing_level_two')
GROUP BY supplier_level;

-- Должно вернуть строки для обоих уровней
```

### 8.3 Проверка YTD time_period format

```sql
-- Проверить формат YTD time_period
SELECT DISTINCT time_period
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
WHERE time_granularity = 'YTD';

-- Ожидаемый результат: YTD-2026, YTD-2025
```

### 8.4 Проверка Pattern F (LY capping)

```sql
-- Сравнить max_month CY vs LY для каждой сущности
WITH cy AS (
  SELECT global_entity_id, MAX(CAST(time_period AS DATE)) AS max_cy
  FROM `{{ project }}.{{ dataset }}.sps_financial_metrics`
  WHERE time_granularity = 'Monthly'
    AND EXTRACT(YEAR FROM CAST(time_period AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY global_entity_id
),
ly AS (
  SELECT global_entity_id, MAX(CAST(join_time_period AS DATE)) AS max_ly_shifted
  FROM `{{ project }}.{{ dataset }}.sps_financial_metrics_prev_year`
  WHERE time_granularity = 'Monthly'
  GROUP BY global_entity_id
)
SELECT c.global_entity_id, c.max_cy, l.max_ly_shifted,
  (c.max_cy = l.max_ly_shifted) AS dates_match
FROM cy c LEFT JOIN ly l USING (global_entity_id);

-- dates_match должен быть TRUE для всех сущностей
```

### 8.5 Проверка APPROX_COUNT_DISTINCT отклонения

```sql
-- Сравнить точный vs приблизительный подсчет для sample-сущности
SELECT
  global_entity_id,
  time_period,
  total_customers AS approx_customers,
  (SELECT COUNT(DISTINCT analytical_customer_id)
   FROM `{{ project }}.{{ dataset }}.sps_financial_metrics_month`
   WHERE global_entity_id = fm.global_entity_id
     AND month = fm.time_period) AS exact_customers
FROM `{{ project }}.{{ dataset }}.sps_financial_metrics` fm
WHERE time_granularity = 'Monthly'
  AND global_entity_id = 'FP_SG'  -- Выбрать любую сущность
LIMIT 5;

-- Отклонение должно быть < 1%
```

### 8.6 Проверка sps_market_customers

```sql
-- Проверить, что YTD total_market_customers != SUM(Monthly)
WITH ytd AS (
  SELECT global_entity_id, total_market_customers AS ytd_customers
  FROM `{{ project }}.{{ dataset }}.sps_market_customers`
  WHERE time_granularity = 'YTD'
),
monthly_sum AS (
  SELECT global_entity_id, SUM(total_market_customers) AS sum_monthly
  FROM `{{ project }}.{{ dataset }}.sps_market_customers`
  WHERE time_granularity = 'Monthly'
    AND EXTRACT(YEAR FROM CAST(time_period AS DATE)) = EXTRACT(YEAR FROM CURRENT_DATE())
  GROUP BY global_entity_id
)
SELECT y.global_entity_id, y.ytd_customers, m.sum_monthly,
  (y.ytd_customers < m.sum_monthly) AS ytd_less_than_sum
FROM ytd y JOIN monthly_sum m USING (global_entity_id);

-- ytd_less_than_sum должен быть TRUE (YTD всегда меньше суммы Monthly)
```

### 8.7 Проверка sps_scoring_params

```sql
-- Проверить, что пороги рассчитаны для всех сущностей
SELECT
  global_entity_id,
  time_period,
  bm_starting, bm_ending,
  fm_starting, fm_ending,
  gbd_target, gbd_lower, gbd_upper
FROM `{{ project }}.{{ dataset }}.sps_scoring_params`
ORDER BY global_entity_id, time_period;

-- bm_ending > bm_starting (всегда)
-- fm_ending > fm_starting (всегда)
-- gbd_upper = gbd_target * 2.0
-- gbd_lower = gbd_target * 0.5
```

### 8.8 Проверка sps_supplier_scoring

```sql
-- Проверить, что сумма баллов не превышает 200
SELECT
  global_entity_id, time_period, entity_key,
  score_fill_rate + score_otd + score_yoy + score_efficiency
    + score_gbd + score_back_margin + score_front_margin AS total_score
FROM `{{ project }}.{{ dataset }}.sps_supplier_scoring`
WHERE (score_fill_rate + score_otd + score_yoy + score_efficiency
  + score_gbd + score_back_margin + score_front_margin) > 200;

-- Должно вернуть 0 строк
```

### 8.9 Проверка sps_supplier_segmentation

```sql
-- Проверить распределение сегментов
SELECT
  global_entity_id,
  time_period,
  segment,
  COUNT(*) AS supplier_count
FROM `{{ project }}.{{ dataset }}.sps_supplier_segmentation`
WHERE time_granularity = 'Monthly'
GROUP BY global_entity_id, time_period, segment
ORDER BY global_entity_id, time_period, segment;

-- Ожидаемые значения segment: Key Account, Standard, Niche, Long Tail
```

### 8.10 Обратная совместимость -- сравнение с текущим production

```sql
-- Сравнить старый и новый sps_score_tableau по Monthly+Quarterly
-- (YTD-строки исключены для fair comparison)
WITH old AS (
  SELECT global_entity_id, time_period, brand_sup, entity_key,
    Net_Sales_eur, total_customers, COGS_eur
  FROM `{{ project }}.{{ old_dataset }}.sps_score_tableau`
),
new AS (
  SELECT global_entity_id, time_period, brand_sup, entity_key,
    Net_Sales_eur, total_customers, COGS_eur
  FROM `{{ project }}.{{ new_dataset }}.sps_score_tableau`
  WHERE time_granularity IN ('Monthly', 'Quarterly')
    AND supplier_level NOT IN ('front_facing_level_one', 'front_facing_level_two')
)
SELECT
  o.global_entity_id, o.time_period, o.brand_sup, o.entity_key,
  o.Net_Sales_eur AS old_sales,
  n.Net_Sales_eur AS new_sales,
  ROUND(ABS(o.Net_Sales_eur - n.Net_Sales_eur) / NULLIF(o.Net_Sales_eur, 0) * 100, 2) AS pct_diff
FROM old o
FULL OUTER JOIN new n USING (global_entity_id, time_period, brand_sup, entity_key)
WHERE ABS(COALESCE(o.Net_Sales_eur, 0) - COALESCE(n.Net_Sales_eur, 0)) > 1
ORDER BY pct_diff DESC
LIMIT 20;

-- Все различия должны быть < 1% (из-за APPROX_COUNT_DISTINCT влияет только total_customers/orders/skus)
-- Net_Sales_eur должны совпадать на 100% (SUM-агрегации не затронуты)
```

---

## Приложение A: Глоссарий терминов

| Термин | Описание |
|--------|----------|
| **CTE** | Common Table Expression -- подзапрос, объявленный через WITH |
| **GROUPING SETS** | SQL-конструкция для множественных агрегаций в одном запросе |
| **APPROX_COUNT_DISTINCT** | Функция BigQuery для приблизительного подсчета уникальных значений (HyperLogLog++) |
| **Pattern F** | Паттерн фильтрации LY-данных по max_date текущего года для каждой сущности |
| **YTD** | Year-to-Date -- накопительный показатель с начала года |
| **entity_key** | Ключ идентификации сущности в иерархии (supplier_id, brand_name, category, и т.д.) |
| **supplier_level** | Уровень глубины категории (supplier, brand_name, level_one, level_two, level_three, front_facing) |
| **division_type** | Тип владения (principal, division, brand_owner, brand_name, total) |
| **time_granularity** | Временная гранулярность строки (Monthly, Quarterly, YTD) |
| **ROW_NUMBER() dedup** | Техника дедупликации: только первая строка в partition несет значение |
| **all_keys UNION** | Архитектурный паттерн: UNION DISTINCT всех ключей из всех таблиц |
| **GBD** | Gross Basket Discount -- валовая скидка на корзину |
| **BM** | Back Margin -- задняя маржа (ребейты) |
| **FM** | Front Margin -- передняя маржа (наценка) |
| **OTD** | On-Time Delivery -- своевременная доставка |
| **AQS v7** | Assortment Quality Scorecard версия 7 -- методология эффективности SKU |

## Приложение B: Контакт

По вопросам миграции обращайся к:
- **Christian La Rosa** -- владелец SPS-пайплайна, архитектура и бизнес-логика
- Slack: #csm-data-engineering (для технических вопросов загрузки)
