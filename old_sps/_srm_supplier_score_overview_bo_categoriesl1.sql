CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_score_overview_bo_categoriesl1` AS 
-- FORMER: _srm_supplier_scorecard_supplier_score_overview_bo_categories_l1
WITH base AS (
    
SELECT 

level,
division_type,
country_code,
global_entity_id,
country_name,
region, 
time_granularity,
time_period,
supplier_id,
supplier_name,
brand_owner_name,
unique_code,
name_missing,
level_one,
--------------------- EUROS ---------------------
ROUND(IFNULL(SUM(Net_Sales_EUR),0), 2) AS Net_Sales_EUR,
ROUND(IFNULL(SUM(COGS_EUR),0), 2) AS COGS_EUR,
ROUND(IFNULL(SUM(Net_Sales_from_promo_eur),0), 2) AS GPV_from_promo_eur,
ROUND(IFNULL(SUM(front_margin_eur),0), 2) AS front_margin_amt_eur,
ROUND(IFNULL(SUM(total_supplier_funding_eur),0), 2) AS total_supplier_funding_eur,
--------------------- LC ---------------------
ROUND(IFNULL(SUM(Net_Sales_LC),0), 2) AS Net_Sales_LC,
ROUND(IFNULL(SUM(COGS_LC),0), 2) AS COGS_LC,
ROUND(IFNULL(SUM(Net_Sales_from_promo_lc),0), 2) AS GPV_from_promo_lc,
ROUND(IFNULL(SUM(front_margin_lc),0), 2) AS front_margin_amt_lc,
ROUND(IFNULL(SUM(total_supplier_funding_lc),0), 2) AS total_supplier_funding_lc,
ROUND(IFNULL(SUM(total_rebate_lc),0), 2) AS back_margin_amt_lc,
ROUND(IFNULL(SUM(total_rebate_wo_dist_allowance_lc),0), 2) AS back_margin_wo_dist_allowance_amt_lc,
ROUND(IFNULL(SUM(total_discount_lc),0), 2) AS total_discount_lc,
ROUND(IFNULL(SUM(Net_Purchases),0), 2) AS Net_Purchases,

FROM
 `fulfillment-dwh-production.rl_dmart._srm_supplier_scorecard_supplier_categories_l1_rough` as o



GROUP BY ALL),


efficiency AS (

SELECT
country_code,
division_type,
global_entity_id,
country_name,
region, 
time_granularity,
time_period,
unique_code,
supplier_id,
supplier_name,
brand_owner_name,
supplier_name as segment_name,
level_one,
slow_movers,
efficient_movers,
zero_movers,
ROUND(gpv_eur,2) AS gpv_eur,
perc_efficiency


FROM
`fulfillment-dwh-production.rl_dmart._srm_supplier_efficiency_category_l1_rough`  as o

GROUP BY ALL
),

supplier_orders AS (

SELECT
country_code,
global_entity_id,
region, 
time_granularity,
division_type,
time_period,
unique_code,
CAST(supplier_id AS INT64) as supplier_id,
supplier_name,
created_by,
level_one,
brand_owner_name,
SUM(total_received_qty_per_po_order) AS total_received_qty_per_po_order,
SUM(total_demANDed_qty_per_po_order) AS total_demANDed_qty_per_po_order,
SUM(supplier_non_fulfilled_order_qty) AS supplier_non_fulfilled_order_qty,
SUM(on_time_orders) AS on_time_orders,
SUM(total_non_cancelled__po_orders) AS total_non_cancelled__po_orders,
SUM(total_cancelled_po_orders) AS total_cancelled_po_orders

FROM  `fulfillment-dwh-production.rl_dmart._srm_supplier_scorecard_operations_categories_l1_rough` as o

WHERE
level in ('supplier')

GROUP BY ALL

),

availability AS (
SELECT

level,
global_entity_id,
division_type,
time_granularity,
time_period,
supplier_id,
supplier_name, 
brand_owner_name,
level_one,
availability_num,
availability_den,
availability

FROM
`fulfillment-dwh-production.rl_dmart._srm_supplier_availability_category_l1_rough`

GROUP BY ALL
),

Intermedia AS (

SELECT

b.level,
b.country_code,
b.division_type,
b.global_entity_id,
b.country_name,
b.region, 
b.time_granularity,
b.time_period,
b.unique_code,
b.supplier_id,
b.supplier_name,
b.brand_owner_name,
b.level_one,
--------------------- This is just to check if the Supplier is a VMI, partial-VMI or Non-VMI. (This is only applicable to Talabat and Hunger Station) ---------------------
MAX(CASE WHEN s.created_by = 'Store Ops' THEN 1 ELSE 0 END) AS has_store_ops,
MAX(CASE WHEN s.created_by = 'Demand Planner' THEN 1 ELSE 0 END) AS has_demand_planner,
---------------------------------------------------- Operations metrics ----------------------------------------------------

SUM(CASE 
            WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                     AND s.created_by = 'Demand Planner' 
                THEN COALESCE(total_received_qty_per_po_order, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                THEN COALESCE(total_received_qty_per_po_order, 0) 
                ELSE 0 
            END) as total_received_qty_per_po_order, ---- numerator for the fill_rate

SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA')  AND s.created_by = 'Demand Planner' 
                    THEN COALESCE(total_demANDed_qty_per_po_order, 0) + COALESCE(supplier_non_fulfilled_order_qty, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                    THEN COALESCE(total_demANDed_qty_per_po_order, 0) + COALESCE(supplier_non_fulfilled_order_qty, 0) 
                ELSE 0 
            END) as total_demanded_plus_supplier_non_fulfilled_order_qty,---- denominator for the fill_rate



ROUND(
    SAFE_DIVIDE(
        SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                     AND s.created_by = 'Demand Planner' 
                THEN COALESCE(total_received_qty_per_po_order, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                THEN COALESCE(total_received_qty_per_po_order, 0) 
                ELSE 0 
            END),
        SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA')  AND s.created_by = 'Demand Planner' 
                    THEN COALESCE(total_demANDed_qty_per_po_order, 0) + COALESCE(supplier_non_fulfilled_order_qty, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                    THEN COALESCE(total_demANDed_qty_per_po_order, 0) + COALESCE(supplier_non_fulfilled_order_qty, 0) 
                ELSE 0 
            END)
    ),
4) AS fill_rate,

SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                     AND s.created_by = 'Demand Planner' 
                THEN COALESCE(on_time_orders, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                THEN COALESCE(on_time_orders, 0) 
                ELSE 0 
            END) as total_on_time_orders,

SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                     AND s.created_by = 'Demand Planner' 
                THEN COALESCE(total_non_cancelled__po_orders, 0) + COALESCE(total_cancelled_po_orders, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                THEN COALESCE(total_non_cancelled__po_orders, 0) + COALESCE(total_cancelled_po_orders, 0) 
                ELSE 0 
            END) as total_non_cancelled_plus_cancelled_po_orders,



ROUND(
    SAFE_DIVIDE(
        SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                     AND s.created_by = 'Demand Planner' 
                THEN COALESCE(on_time_orders, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                THEN COALESCE(on_time_orders, 0) 
                ELSE 0 
            END),
        SUM(CASE 
                WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                     AND s.created_by = 'Demand Planner' 
                THEN COALESCE(total_non_cancelled__po_orders, 0) + COALESCE(total_cancelled_po_orders, 0) 
                WHEN b.global_entity_id NOT IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') 
                THEN COALESCE(total_non_cancelled__po_orders, 0) + COALESCE(total_cancelled_po_orders, 0) 
                ELSE 0 
            END)
    ),
4) AS OTD,

-- ---------------------------------------------------- Commercial metrics ----------------------------------------------------


    -------------------------------------------------- EUROS ---------------------------------------------
Net_Sales_EUR,
front_margin_amt_eur,
GPV_from_promo_eur,
COGS_EUR,
total_supplier_funding_eur,
------------------------------ EFFICIENCY ------------------------------------------------------------
slow_movers,
efficient_movers,
zero_movers,
ROUND(gpv_eur,2) AS gpv_eur,
ROUND((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers))), 2) AS perc_efficiency,
ROUND( ((SAFE_DIVIDE(efficient_movers,(efficient_movers+slow_movers+zero_movers)))*gpv_eur),2) AS weight_efficiency,

-------------------------------------AVAILABAILITY------------------------------------------------------
a.availability_num,
a.availability_den,
a.availability,
------------------------------------------------------------------------------------------------------------------------

    -- YoY GPV Growth EUR Calculation
    ROUND(Net_Sales_EUR,2) AS Net_Sales_EUR_Current_Year,

    CASE 
        -- Use LAG with offset 4 for Quarterly granularity
        WHEN b.time_granularity = 'Quarterly' THEN 
            LAG(b.Net_Sales_EUR, 4) OVER (
                PARTITION BY b.country_code, b.supplier_name, b.supplier_id, b.time_granularity, b.division_type, b.level_one, b.brand_owner_name
                ORDER BY 
                    CAST(SUBSTR(b.time_period, 4, 4) AS STRING), -- Extract and order by year (e.g., 2024)
                    CAST(SUBSTR(b.time_period, 2, 1) AS STRING)  -- Extract and order by quarter (e.g., Q3 -> 3)
            )


        -- Use LAG with offset 12 for Monthly granularity
        WHEN b.time_granularity = 'Monthly' THEN 
            LAG(b.Net_Sales_EUR, 12) OVER (
                PARTITION BY b.country_code, b.supplier_name, b.supplier_id, b.time_granularity, b.division_type, b.level_one, b.brand_owner_name
                ORDER BY b.time_period
            )
    END AS Net_Sales_EUR_Last_Year,

    ROUND((SUM(GPV_from_promo_eur) / NULLIF(SUM(Net_Sales_EUR), 0)), 4) AS Promo_GPV_contribution_eur,
 
(ROUND(IF(SUM(Net_Sales_EUR) = 0, 1, SUM(Net_Sales_EUR)),4) + ROUND(SUM(total_supplier_funding_eur),4) - ROUND(IF(SUM(COGS_EUR) = 0, 1, SUM(COGS_EUR)),4)) / ROUND(IF(SUM(Net_Sales_EUR) = 0, 1, SUM(Net_Sales_EUR)),4) as Front_Margin_EUR,

-- ROUND(SAFE_DIVIDE(SUM(back_margin_amt_eur),SUM(Net_Sales_EUR)),4) as Back_Margin_EUR,

-- ROUND(SAFE_DIVIDE(SUM(back_margin_wo_dist_allowance_amt_eur),SUM(Net_Sales_EUR)),4) as Back_Margin_EUR_WO_WA,

-- ROUND(SAFE_DIVIDE(SUM(Net_Sales_EUR + total_supplier_funding_eur - COGS_EUR + IFNULL(back_margin_wo_dist_allowance_amt_eur,0)) , ROUND(IFNULL(Net_Sales_EUR,0),2)),2) AS Total_Margin_EUR,


          -------------------------------------------------- LC ---------------------------------------------

Net_Sales_lc,
front_margin_amt_lc,
GPV_from_promo_lc,
COGS_lc,
total_supplier_funding_lc,
back_margin_amt_lc,
back_margin_wo_dist_allowance_amt_lc,
total_discount_lc,
Net_Purchases,

    -- YoY GPV Growth LC Calculation
    ROUND(Net_Sales_LC,2) AS Net_Sales_LC_Current_Year,

    CASE 
        -- Use LAG with offset 4 for Quarterly granularity
        WHEN b.time_granularity = 'Quarterly' THEN 
            LAG(b.Net_Sales_LC, 4) OVER (
                PARTITION BY b.country_code, b.supplier_name, b.supplier_id, b.brand_owner_name, b.time_granularity, b.division_type, b.level_one
                ORDER BY 
                    CAST(SUBSTR(b.time_period, 4, 4) AS STRING), -- Extract and order by year (e.g., 2024)
                    CAST(SUBSTR(b.time_period, 2, 1) AS STRING)  -- Extract and order by quarter (e.g., Q3 -> 3)
            )

        -- Use LAG with offset 12 for Monthly granularity
        WHEN b.time_granularity = 'Monthly' THEN 
            LAG(b.Net_Sales_LC, 12) OVER (
                PARTITION BY b.country_code, b.supplier_name, b.supplier_id, b.brand_owner_name, b.time_granularity, b.division_type, b.level_one
                ORDER BY b.time_period
            )
    END AS Net_Sales_LC_Last_Year,

ROUND((SUM(GPV_from_promo_lc) / NULLIF(SUM(Net_Sales_lc), 0)), 4) AS Promo_GPV_contribution_LC,
 
(ROUND(IF(SUM(Net_Sales_lc) = 0, 1, SUM(Net_Sales_lc)),4) + ROUND(SUM(total_supplier_funding_lc),4) - ROUND(IF(SUM(COGS_lc) = 0, 1, SUM(COGS_lc)),4)) / ROUND(IF(SUM(Net_Sales_lc) = 0, 1, SUM(Net_Sales_lc)),4) as Front_Margin_LC,

ROUND(SAFE_DIVIDE(SUM(back_margin_amt_lc),SUM(Net_Sales_lc)),4) as Back_Margin_LC,

ROUND(SAFE_DIVIDE(SUM(back_margin_wo_dist_allowance_amt_lc),SUM(Net_Sales_lc)),4) as Back_Margin_LC_WO_WA,

ROUND(
  SAFE_DIVIDE(
    SUM(Net_Sales_lc + total_supplier_funding_lc) 
    - SUM(COGS_lc) 
    + SUM(IFNULL(back_margin_amt_lc, 0)), 
    NULLIF(SUM(Net_Sales_lc), 0) -- Use NULLIF to avoid dividing by zero
  ), 
  4
) AS Total_Margin_LC


-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

FROM base as b
 
LEFT JOIN supplier_orders AS s
ON 
b.country_code = s.country_code
AND b.time_period = s.time_period
AND b.division_type = s.division_type
AND 
(
    -- If division_type is 'principal' or 'division', join on supplier_id and supplier_name
    (
        b.division_type IN ('principal', 'division') AND
        (
            CAST(b.supplier_id AS STRING) = CAST(s.supplier_id AS STRING)
            AND b.supplier_name = s.supplier_name
        )
    )
    -- Else, join on brand_owner_name
    OR
    (
        b.division_type NOT IN ('principal', 'division') AND
        b.brand_owner_name = s.brand_owner_name
    )
)
AND
b.level_one = s.level_one




LEFT JOIN
efficiency as e

ON
b.country_code = e.country_code
AND
b.time_period = e.time_period
AND
b.division_type = e.division_type
AND
b.time_granularity = e.time_granularity
AND 
(
    -- If division_type is 'principal' or 'division', join on supplier_id and supplier_name
    (
        b.division_type IN ('principal', 'division') AND
        (
            CAST(b.supplier_id AS STRING) = CAST(e.supplier_id AS STRING)
            AND b.supplier_name = e.supplier_name
        )
    )
    -- Else, join on brand_owner_name
    OR
    (
        b.division_type NOT IN ('principal', 'division') AND
        b.brand_owner_name = e.brand_owner_name
    )
)
AND
b.level_one = e.level_one

LEFT JOIN
availability as a

ON
b.global_entity_id = a.global_entity_id
AND
b.time_period = a.time_period
AND
b.division_type = a.division_type
AND 
(
    -- If division_type is 'principal' or 'division', join on supplier_id and supplier_name
    (
        b.division_type IN ('principal', 'division') AND
        (
             CAST(b.supplier_id AS STRING) = CAST(a.supplier_id AS STRING)
             AND b.supplier_name = a.supplier_name
        )
    )
    -- Else, join on brand_owner_name
    OR
    (
        b.division_type NOT IN ('principal', 'division') AND
        b.brand_owner_name = a.brand_owner_name
    )
)
AND
b.level_one = a.level_one



GROUP BY ALL
 ),

    FINAL AS (
SELECT  

b.level,
b.country_code,
b.global_entity_id,
b.country_name,
b.region, 
b.division_type,
b.time_granularity,
b.time_period,
b.unique_code,
b.supplier_id,
b.supplier_name,
b.brand_owner_name,
--------------------- This is just to check if the Supplier is a VMI, partial-VMI or Non-VMI. (This is only applicable to Talabat and Hunger Station) ---------------------
CASE 
        WHEN b.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_BH', 'HF_EG', 'TB_IQ', 'TB_JO', 'TB_OM', 'TB_QA', 'HS_SA') THEN
            CASE 
                WHEN has_store_ops = 1 AND has_demand_planner = 1 THEN 'Partial'
                WHEN has_store_ops = 1 AND has_demand_planner = 0 THEN 'Yes'
                WHEN has_store_ops = 0 AND has_demand_planner = 1 THEN 'No'
                ELSE NULL
            END
        ELSE NULL
    END AS is_supplier_VMI,
--------------------- This is just to check if the Supplier is a VMI, partial-VMI or Non-VMI. (This is only applicable to Talabat and Hunger Station) ---------------------
b.level_one,
b.fill_rate,
b.total_received_qty_per_po_order,
b.total_demanded_plus_supplier_non_fulfilled_order_qty,
b.total_on_time_orders,
b.total_non_cancelled_plus_cancelled_po_orders,
b.OTD,
------------------------------------------------------------------------------------------------------- EUR -------------------------------------------------------------------------------
b.Net_Sales_EUR,
b.front_margin_amt_eur,
b.GPV_from_promo_eur,
b.COGS_EUR,
b.total_supplier_funding_eur,
-- back_margin_amt_eur,
-- back_margin_wo_dist_allowance_amt_eur,

----------- EFFICIENCY ---------------
b.perc_efficiency,
b.weight_efficiency,
b.slow_movers,
b.efficient_movers,
b.zero_movers,
b.gpv_eur,

----------- AVAILABILITY ---------------
b.availability_num,
b.availability_den,
b.availability,
b.Net_Sales_EUR_Current_Year,
b.Net_Sales_EUR_Last_Year,
b.Promo_GPV_contribution_eur,
b.Front_Margin_EUR,
ROUND(SAFE_DIVIDE((b.Net_Sales_EUR_Current_Year - b.Net_Sales_EUR_Last_Year), b.Net_Sales_EUR_Last_Year),3) AS YoY_GPV_Growth_EUR,
-- b.Back_Margin_EUR,
-- b.Back_Margin_EUR_WO_WA,
------------------------------------------------------------------------------------------------------- LC -------------------------------------------------------------------------------
b.Net_Sales_lc,
b.front_margin_amt_lc,
b.GPV_from_promo_lc,
b.COGS_lc,
b.total_supplier_funding_lc,
b.back_margin_amt_lc,
b.back_margin_wo_dist_allowance_amt_lc,
b.Net_Sales_LC_Current_Year,
b.Net_Sales_LC_Last_Year,
ROUND(SAFE_DIVIDE((b.Net_Sales_LC_Current_Year - b.Net_Sales_LC_Last_Year), b.Net_Sales_LC_Last_Year),3) AS YoY_GPV_Growth_LC,
b.Promo_GPV_contribution_LC,
b.Front_Margin_LC,
b.Back_Margin_LC,
b.Back_Margin_LC_WO_WA,
b.Total_Margin_LC,
b.total_discount_lc,
b.Net_Purchases,

FROM 

Intermedia as b

GROUP BY ALL)
,

scoring_final AS (

SELECT

b.level,
'level_one' as supplier_level,
b.country_code,
b.global_entity_id,
b.country_name,
b.region, 
b.division_type,
b.time_granularity,
b.time_period,
b.unique_code,
b.supplier_id,
b.supplier_name,
b.brand_owner_name,
b.is_supplier_VMI,
b.level_one,
CAST(NULL AS STRING) AS level_two,
CAST(NULL AS STRING) AS level_three,
COALESCE(b.total_received_qty_per_po_order,0) AS total_received_qty_per_po_order,
COALESCE(b.total_demanded_plus_supplier_non_fulfilled_order_qty,0) AS total_demanded_plus_supplier_non_fulfilled_order_qty,
COALESCE(b.total_on_time_orders,0) AS total_on_time_orders,
COALESCE(b.total_non_cancelled_plus_cancelled_po_orders,0) AS total_non_cancelled_plus_cancelled_po_orders,
COALESCE(b.fill_rate, 0) AS fill_rate,
COALESCE(b.OTD, 0) AS OTD,
COALESCE(b.Net_Sales_EUR, 0) AS Net_Sales_EUR,
COALESCE(b.Net_Sales_EUR_Last_Year, 0) AS Net_Sales_EUR_Last_Year,
COALESCE(b.YoY_GPV_Growth_EUR, 0) AS YoY_GPV_Growth_EUR,
COALESCE(b.front_margin_amt_eur, 0) AS front_margin_amt_eur,
COALESCE(b.GPV_from_promo_eur, 0) AS GPV_from_promo_eur,
COALESCE(b.COGS_EUR, 0) AS COGS_EUR,
COALESCE(b.availability_num,0) AS availability_num,
COALESCE(b.availability_den,0) AS availability_den,
COALESCE(b.total_supplier_funding_eur, 0) AS total_supplier_funding_eur,
-- COALESCE(b.back_margin_amt_eur, 0) AS back_margin_amt_eur,
-- COALESCE(b.back_margin_wo_dist_allowance_amt_eur, 0) AS back_margin_wo_dist_allowance_amt_eur,
COALESCE(b.slow_movers, 0) AS slow_movers,
COALESCE(b.efficient_movers, 0) AS efficient_movers,
COALESCE(b.zero_movers, 0) AS zero_movers,
COALESCE(b.gpv_eur, 0) AS gpv_eur,
COALESCE(b.perc_efficiency, 0) AS perc_efficiency,
COALESCE(b.weight_efficiency, 0) AS weight_efficiency,
COALESCE(b.availability, 0) AS availability,
COALESCE(b.Promo_GPV_contribution_eur, 0) AS Promo_GPV_contribution_eur,
COALESCE(b.Front_Margin_EUR, 0) AS Front_Margin_EUR,
-- COALESCE(b.Back_Margin_EUR, 0) AS Back_Margin_EUR,
-- COALESCE(b.Back_Margin_EUR_WO_WA, 0) AS Back_Margin_EUR_WO_WA,
-- COALESCE(b.Total_Margin_EUR, 0) AS Total_Margin_EUR,
COALESCE(b.Net_Sales_LC, 0) AS Net_Sales_LC,
COALESCE(b.Net_Sales_LC_Last_Year, 0) AS Net_Sales_LC_Last_Year,
COALESCE(b.YoY_GPV_Growth_LC, 0) AS YoY_GPV_Growth_LC,
COALESCE(b.front_margin_amt_lc, 0) AS front_margin_amt_lc,
COALESCE(b.GPV_from_promo_lc, 0) AS GPV_from_promo_lc,
COALESCE(b.COGS_lc, 0) AS COGS_lc,
COALESCE(b.total_supplier_funding_lc, 0) AS total_supplier_funding_lc,
COALESCE(b.back_margin_amt_lc, 0) AS back_margin_amt_lc,
COALESCE(b.back_margin_wo_dist_allowance_amt_lc, 0) AS back_margin_wo_dist_allowance_amt_lc,
COALESCE(b.Promo_GPV_contribution_LC, 0) AS Promo_GPV_contribution_LC,
COALESCE(b.Front_Margin_LC, 0) AS Front_Margin_LC,
COALESCE(b.Back_Margin_LC, 0) AS Back_Margin_LC,
COALESCE(b.Back_Margin_LC_WO_WA, 0) AS Back_Margin_LC_WO_WA,
COALESCE(b.Total_Margin_LC, 0) AS Total_Margin_LC,
COALESCE(b.total_discount_lc, 0) AS total_discount_lc,
COALESCE(b.Net_Purchases, 0) AS Net_Purchases,


    -- Operations Metrics
    IFNULL(CAST(ROUND((( 
        CASE 
            WHEN fill_rate >= 1 THEN 1
            WHEN fill_rate <= 0 THEN 0
            ELSE (fill_rate / 1)
        END
    ) * 0.60 * 100), 2) AS INT64),0) AS score_Supplier_Fill_Rate,

    IFNULL(CAST(ROUND((( 
        CASE 
            WHEN OTD >= 1 THEN 1
            WHEN OTD <= 0 THEN 0
            ELSE (OTD / 1)
        END
    ) * 0.40 * 100), 2) AS INT64),0) AS score_OTD,

    -- Commercial Metrics
    IFNULL(CAST(ROUND((( 
        CASE 
            WHEN YoY_GPV_Growth_LC >= 0.50 THEN 1
            WHEN YoY_GPV_Growth_LC <= 0 THEN 0
            ELSE (YoY_GPV_Growth_LC / 0.50)
        END
    ) * 0.10 * 100), 2) AS INT64),0) AS score_YoY_GPV_Growth,

    IFNULL(CAST(ROUND((( 
        CASE 
            WHEN perc_efficiency >= 1 THEN 1
            WHEN perc_efficiency <= 0.40 THEN 0
            ELSE ((perc_efficiency - 0.40) / (1 - 0.40))
        END
    ) * 0.30 * 100), 2) AS INT64),0) AS score_Efficiency,

    IFNULL(CAST( ROUND((( 
        CASE 
            WHEN Promo_GPV_Contribution_LC >= 0.50 THEN 1
            WHEN Promo_GPV_Contribution_LC <= 0 THEN 0
            ELSE (Promo_GPV_Contribution_LC / 0.50)
        END
    ) * 0.20 * 100), 2) AS INT64),0) AS score_Promo_GPV_Contribution,

    IFNULL(CAST(ROUND((( 
        CASE 
            WHEN Back_margin_LC > 0.15 THEN 1
            WHEN Back_margin_LC < 0.0 THEN 0
            ELSE ((Back_margin_LC - 0) / (0.15 - 0.0))
        END
    ) * 0.25 * 100), 2) AS INT64),0) AS score_Back_Margin,

        IFNULL(CAST(ROUND((( 
        CASE 
            WHEN Front_margin_LC > 0.4 THEN 1
            WHEN Front_margin_LC < 0.15 THEN 0
            ELSE ((Front_margin_LC - 0.15) / (0.40 - 0.15))
        END
    ) * 0.15 * 100), 2) AS INT64),0) AS score_Front_Margin,


FROM FINAL b

GROUP BY ALL

)

SELECT

s.*,
ROUND(SUM(score_Supplier_Fill_Rate + score_OTD),2) AS operations_score,
ROUND(SUM(score_YoY_GPV_Growth + score_Efficiency + score_Promo_GPV_Contribution + score_Front_Margin + score_Back_Margin),2) AS commercial_score,
ROUND(SAFE_DIVIDE(SUM(score_Supplier_Fill_Rate+ score_OTD + score_YoY_GPV_Growth + score_Efficiency + score_Promo_GPV_Contribution + score_Front_Margin + score_Back_Margin),2),2) AS Total_Score,

FROM
scoring_final AS s

GROUP BY ALL
