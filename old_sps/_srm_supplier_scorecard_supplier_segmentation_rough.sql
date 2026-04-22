CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_scorecard_supplier_segmentation_rough`  AS

    WITH base AS (

    SELECT 
    *,
    segment_name AS funnel_1_segmentation, 
    CASE WHEN brand_owner_name IN  ('Unknown', 'Sin Brand Owner', 'PedidosYa') OR brand_owner_name IS NULL THEN FALSE ELSE TRUE END AS brand_owner_available,
    CASE WHEN brand_owner_name IN  ('Unknown', 'Sin Brand Owner', 'PedidosYa') OR brand_owner_name IS NULL THEN "supplier_name" ELSE "brand_owner_name" END AS funnel_1_segmentation_,
        CASE 
            WHEN Net_Sales_EUR < 1000 
            THEN 'Not Applicable' 
            ELSE 'OK' 
        END AS IS_GPV_LESS_THAN_1K

    FROM 
    `fulfillment-dwh-production.rl_dmart._srm_supplier_scorecard_supplier_rough`  as o

    WHERE level IN ('supplier', 'brand_owner')

    ),

    brand_owner_orders AS (

    SELECT
    *
    FROM
    `fulfillment-dwh-production.rl_dmart._srm_supplier_scorecard_brand_orders_rough` as o

    WHERE level IN ('supplier', 'brand_owner')
    ),

    availability AS (

    SELECT
    *
    FROM
    `fulfillment-dwh-production.rl_dmart._srm_supplier_availability_rough` as a

    WHERE level IN ('supplier', 'brand_owner')
    )
    
    ,

    Intermedia AS (
    SELECT

        'supplier' as level,
        b.division_type,
        b.global_entity_id,
        b.country_code,
        b.country_name,
        b.region,
        b.time_granularity,
        b.time_period,
        b.supplier_id,
        b.supplier_name,
        b.IS_GPV_LESS_THAN_1K,
        CAST(NULL AS STRING) AS brand_name,
        b.brand_owner_name,
        b.supplier_name as segment_name,
        total_orders_for_the_month,
        total_customers_for_month,
        skus_sold,
        warehouses_sold,
    ---------------------------------------------------Availability--------------------------------------------------------------------   
    a.availability_num,
    a.availability_den,
    a.availability,
    ---------------------------------------------------EUR---------------------------------------------------------------------
    ROUND(IFNULL(Net_SALES_EUR,0),4) AS Conditional_GPV_EUR,

    ROUND(IFNULL(SUM(Net_Sales_from_promo_eur),0), 2) AS GPV_from_promo_eur,

    Total_Net_Sales_EUR_order,

    ROUND(IFNULL(SUM(COGS_EUR),0),4) AS COGS_EUR,

    ROUND(SAFE_DIVIDE(Total_Net_Sales_EUR_order, SUM (count_of_orders)),4) AS ABV_EUR_order,

    ROUND(SAFE_DIVIDE(SUM(Net_Sales_EUR),SUM (count_of_orders)),4) AS ABV_EUR, 

    -- ROUND(SUM(total_rebate_eur),2) AS total_rebate_eur, 

    ROUND(SUM (total_supplier_funding_eur),4) AS total_supplier_funding_eur,

    ROUND(SUM (total_discount_eur),4) AS total_discount_EUR,

    ROUND(SUM(Net_Sales_EUR + total_supplier_funding_eur - COGS_EUR),4) AS Gross_Margin_with_SF_EUR,

    -- ROUND(SUM(total_rebate_wo_dist_allowance_eur),2) AS total_rebate_wo_dist_allowance_eur, 

    SAFE_DIVIDE(SUM(Net_Sales_EUR + total_supplier_funding_eur - COGS_EUR) , ROUND(IFNULL(Net_SALES_EUR,0),4)) AS FM_EUR,

    -- ROUND(SAFE_DIVIDE(SUM(total_rebate_wo_dist_allowance_eur), SUM(Net_Sales_eur)),2) AS Back_Margin_Eur,

    -- SAFE_DIVIDE(SUM(Net_Sales_EUR + total_supplier_funding_eur + IFNULL(total_rebate_wo_dist_allowance_eur,0) - COGS_EUR) , ROUND(IFNULL(Net_SALES_EUR,0),2)) AS Total_margin_EUR,

    ---------------------------------------------------LC---------------------------------------------------------------------

    ROUND(IFNULL(SUM(Net_SALES_LC),0),4) AS Conditional_GPV_LC,

    ROUND(IFNULL(SUM(Net_Sales_from_promo_lc),0), 2) AS GPV_from_promo_LC,

    Total_Net_Sales_LC_order,

    ROUND(IFNULL(SUM(COGS_LC),0),4) AS COGS_LC,

    ROUND(SAFE_DIVIDE(Total_Net_Sales_LC_order, SUM (count_of_orders)),4) AS ABV_LC_order,

    ROUND(SAFE_DIVIDE(SUM(Net_Sales_LC),SUM (count_of_orders)),4) AS ABV_LC, 

    ROUND(SUM (total_supplier_funding_LC),4) AS total_supplier_funding_LC,

    ROUND(SUM (total_discount_lc),4) AS total_discount_LC,

    ROUND(SUM(Net_Sales_LC + total_supplier_funding_LC - COGS_LC),4) AS Gross_Margin_with_SF_LC,

    ROUND(SUM(total_rebate_lc),4) AS total_rebate_LC, 

    ROUND(SUM(total_rebate_wo_dist_allowance_lc),4) AS total_rebate_wo_dist_allowance_lc, 

    ROUND(SAFE_DIVIDE(SUM(Net_Sales_LC + total_supplier_funding_LC - COGS_LC) , ROUND(IFNULL(Net_SALES_LC,0),2)),4) AS FM_LC,

    ROUND(SAFE_DIVIDE(SUM(total_rebate_lc), SUM(Net_Sales_LC)),4) AS Back_Margin_LC,

    ROUND(SAFE_DIVIDE(SUM(total_rebate_wo_dist_allowance_lc), SUM(Net_Sales_LC)),4) AS Back_Margin_LC_WO_WA,

    ROUND(
    SAFE_DIVIDE(
        SUM(Net_Sales_lc + total_supplier_funding_lc) 
        - SUM(COGS_lc) 
        + SUM(IFNULL(total_rebate_wo_dist_allowance_lc, 0)), 
        NULLIF(SUM(Net_Sales_lc), 0) -- Use NULLIF to avoid dividing by zero
    ), 
    4
    ) AS Total_Margin_LC,

    ROUND((ROUND(IFNULL(Net_SALES_LC,0),2) * ((SAFE_DIVIDE(SUM(Net_Sales_LC + total_supplier_funding_LC - COGS_LC) , ROUND(IFNULL(Net_SALES_LC,0),2))) + IFNULL((ROUND(SAFE_DIVIDE(SUM(total_rebate_LC), SUM(Net_Sales_LC)),2)),0))),4) Net_profit_LC,

    ROUND((ROUND(IFNULL(Net_SALES_LC,0),2) * ((SAFE_DIVIDE(SUM(Net_Sales_LC + total_supplier_funding_LC - COGS_LC) , ROUND(IFNULL(Net_SALES_LC,0),2))) + IFNULL((ROUND(SAFE_DIVIDE(SUM(total_rebate_wo_dist_allowance_lc), SUM(Net_Sales_LC)),2)),0))),4) Net_profit_LC_WO_WA,

    ----------------------------------------------------------------------------------------------------------------------------------

    ROUND((ROUND(SAFE_DIVIDE(SAFE_CAST(SUM(count_of_orders) AS INT64), IF(total_orders_for_the_month = 0, 1, total_orders_for_the_month)), 4) * 100),4) AS Basket_penetration,

    ROUND((ROUND(SAFE_DIVIDE(SAFE_CAST(SUM(count_of_customers) AS INT64), IF(total_customers_for_month = 0, 1, total_customers_for_month)), 4) * 100),4) AS customer_penetration,

    ROUND(SAFE_DIVIDE(SAFE_CAST(SUM(count_of_orders) AS INT64), IF(SAFE_CAST(SUM(count_of_customers) AS INT64) = 0, 1, SAFE_CAST(SUM(count_of_customers) AS INT64))), 4) AS Frequency,

    ROUND(SAFE_DIVIDE(SUM(fulfilled_quantity), IFNULL(NULLIF(skus_sold * warehouses_sold, 0), 1)), 4) AS Velocity,
    ----------------------------------------------------------------------------------------------------------------------------------
    SUM(count_of_customers) as count_of_customers,
    ROUND(SUM(count_of_orders),0) AS count_of_orders,
    skus_sold as distinct_skus_sold,
    SUM(fulfilled_quantity) as fulfilled_quantity,
    ROUND(IFNULL(SUM(Net_Purchases),0), 2) AS Net_Purchases,

    ----------------------------------------------------------------------------------------------------------------------------------


    FROM base as b

    
LEFT JOIN
brand_owner_orders as bo

ON
b.country_code = bo.country_code
AND b.time_period = bo.time_period
AND b.time_granularity = bo.time_granularity
AND b.division_type = bo.division_type
AND 
(
    -- If division_type is 'principal' or 'division', join on supplier_id and supplier_name
    (
        b.division_type IN ('principal', 'division') AND
        (
            b.supplier_id = bo.supplier_id 
        )
        AND b.supplier_name = bo.supplier_name
    )
    -- If division_type is 'brand_owner', join on brand_owner_name
    OR
    (
        b.division_type = 'brand_owner' AND
        b.brand_owner_name = bo.brand_owner_name
    )
)

LEFT JOIN
availability as a

ON
b.global_entity_id = a.global_entity_id
AND b.time_period = a.time_period
AND b.time_granularity = a.time_granularity
AND b.division_type = a.division_type
AND 
(
    -- If division_type is 'principal' or 'division', join on supplier_id and supplier_name
    (
        b.division_type IN ('principal', 'division') AND
        (
           b.supplier_id = a.supplier_id 
        )
        AND b.supplier_name = a.supplier_name
    )
    -- If division_type is 'brand_owner', join on brand_owner_name
    OR
    (
        b.division_type = 'brand_owner' AND
        b.brand_owner_name = a.brand_owner_name
    )
)

    GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,orders, Net_SALES_EUR, Net_SALES_LC, skus_sold, warehouses_sold,availability_num, availability_den, availability, total_GBV, Total_Net_Sales_LC_order, Total_Net_Sales_EUR_order 

    )

    , percentiles AS (

    SELECT

        'supplier' as level,
        division_type,
        global_entity_id,
        country_code,
        country_name,
        region,
        time_granularity,
        time_period,
        IS_GPV_LESS_THAN_1K,

        --------------------------------------------------------- EUR ---------------------------------------------------------

        --------------------------- IMPORTANCE TO BUSINESS. ---------------------------

        -- ROUND(PERCENTILE_CONT(Intermedia.Net_profit_EUR, 0.95) OVER(PARTITION BY 
        --     'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _95th_percentile_net_profit_EUR,
        -- ROUND(PERCENTILE_CONT(Intermedia.Net_profit_EUR, 0.15) OVER(PARTITION BY 
        --     'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _15th_percentile_net_profit_EUR,

        --------------------------- Supplier productivity and importance to customers ---------------------------
        ROUND(PERCENTILE_CONT(ABV_EUR_order, 0.95) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _95th_percentile_ABV_EUR,
        ROUND(PERCENTILE_CONT(ABV_EUR_order, 0.15) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _15th_percentile_ABV_EUR,
        
        --------------------------------------------------------- LC ---------------------------------------------------------

        --------------------------- IMPORTANCE TO BUSINESS. ---------------------------

        ROUND(PERCENTILE_CONT(Intermedia.Net_profit_LC, 0.95) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _95th_percentile_net_profit_LC,
        ROUND(PERCENTILE_CONT(Intermedia.Net_profit_LC, 0.15) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _15th_percentile_net_profit_LC,

        --------------------------- Supplier productivity and importance to customers ---------------------------
        ROUND(PERCENTILE_CONT(ABV_LC_order, 0.95) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _95th_percentile_ABV_LC,
        ROUND(PERCENTILE_CONT(ABV_LC_order, 0.15) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _15th_percentile_ABV_LC,

        -------------------------------------------------------------------------------------
        ROUND(PERCENTILE_CONT(Frequency, 0.95) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _95th_percentile_Frequency,
        ROUND(PERCENTILE_CONT(Frequency, 0.15) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _15th_percentile_Frequency,

        -------------------------------------------------------------------------------------
        ROUND(PERCENTILE_CONT(customer_penetration, 0.95) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _95th_percentile_customer_penetration,
        ROUND(PERCENTILE_CONT(customer_penetration, 0.15) OVER(PARTITION BY 
            'supplier', division_type, global_entity_id, country_code, country_name, region, time_granularity, time_period), 2) AS _15th_percentile_customer_penetration

    FROM 
        Intermedia

        WHERE
        IS_GPV_LESS_THAN_1K = 'OK'

    GROUP BY 
        1,2,3,4,5,6,7,8,9,Intermedia.Conditional_GPV_EUR,Intermedia.Net_profit_LC,ABV_LC_order,ABV_EUR_order,Frequency, customer_penetration

    )
    
    ,
     Scoring AS (

    SELECT 

        'supplier' as level,
        i.division_type,
        i.global_entity_id,
        i.country_code,
        i.country_name,
        i.region,
        i.time_granularity,
        i.time_period,
        i.IS_GPV_LESS_THAN_1K,
        i.supplier_id,
        i.supplier_name,
        CAST(NULL AS STRING) AS brand_name,
        brand_owner_name,
        i.supplier_name as segment_name,
        COALESCE(total_orders_for_the_month, 0) AS total_orders_for_the_month,
        COALESCE(total_customers_for_month, 0) AS total_customers_for_month,
        
        -- --------- fundamental levers that make the segmentation of the scorecard ------------
        COALESCE(Net_Purchases, 0) AS Net_Purchases,
        COALESCE(Conditional_GPV_EUR, 0) AS Conditional_GPV_EUR,
        COALESCE(Total_Net_Sales_EUR_order, 0) AS Total_Net_Sales_EUR_order,
        COALESCE(Conditional_GPV_LC, 0) AS Conditional_GPV_LC,
        COALESCE(total_supplier_funding_EUR, 0) AS total_supplier_funding_EUR,
        COALESCE(total_supplier_funding_LC, 0) AS total_supplier_funding_LC,
        COALESCE(total_discount_eur, 0) AS total_discount_eur,
        COALESCE(total_discount_lc, 0) AS total_discount_lc,
        COALESCE(GPV_from_promo_EUR, 0) AS GPV_from_promo_EUR,
        COALESCE(GPV_from_promo_LC, 0) AS GPV_from_promo_LC,
        COALESCE(Total_Net_Sales_LC_order, 0) AS Total_Net_Sales_LC_order,
        COALESCE(COGS_EUR, 0) AS COGS_EUR,
        COALESCE(COGS_LC, 0) AS COGS_LC,
        COALESCE(count_of_customers, 0) AS count_of_customers,
        COALESCE(count_of_orders, 0) AS count_of_orders,
        COALESCE(distinct_skus_sold, 0) AS distinct_skus_sold,
        COALESCE(fulfilled_quantity, 0) AS fulfilled_quantity,
        COALESCE(availability_num, 0) AS availability_num,
        COALESCE(availability_den, 0) AS availability_den,
        COALESCE(availability, 0) AS availability,

        -- --------- fundamental levers that make the segmentation of the scorecard ------------
        COALESCE(ABV_EUR, 0) AS ABV_EUR,
        COALESCE(ABV_EUR_order, 0) AS ABV_EUR_order,
        COALESCE(ABV_LC, 0) AS ABV_LC,
        COALESCE(ABV_LC_order, 0) AS ABV_LC_order,
        COALESCE(FM_EUR, 0) AS FM_EUR,
        COALESCE(FM_LC, 0) AS FM_LC,
        -- COALESCE(Back_Margin_Eur, 0) AS Back_Margin_Eur,
        COALESCE(Back_Margin_LC, 0) AS Back_Margin_LC,
        COALESCE(Back_Margin_LC_WO_WA, 0) AS Back_Margin_LC_WO_WA,
        
        -- COALESCE(total_rebate_eur, 0) AS total_rebate_eur,
        -- COALESCE(total_rebate_wo_dist_allowance_eur, 0) AS total_rebate_wo_dist_allowance_eur,
        COALESCE(total_rebate_lc, 0) AS total_rebate_lc,
        COALESCE(total_rebate_wo_dist_allowance_lc, 0) AS total_rebate_wo_dist_allowance_lc,

        -- COALESCE(Total_margin_EUR, 0) AS Total_margin_EUR,
        COALESCE(Total_margin_LC, 0) AS Total_margin_LC,
        COALESCE(Basket_penetration, 0) AS Basket_penetration,
        COALESCE(customer_penetration, 0) AS customer_penetration,
        COALESCE(Frequency, 0) AS Frequency,

        -- --------- Net Profit ------------
        COALESCE(Net_profit_LC, 0) AS Net_profit_LC,
        COALESCE(Net_profit_LC_WO_WA, 0) AS Net_profit_LC_WO_WA,

        -- -- Importance to Business Scoring ------------
       -- Calculate the score for Retail Profit
        ROUND(COALESCE(CASE 
        WHEN i.Net_profit_LC >= p._95th_percentile_net_profit_lc THEN 100 * 1.0 -- Full weight for Retail Profit is 100%
        WHEN i.Net_profit_LC <= p._15th_percentile_net_profit_lc THEN 0
        ELSE SAFE_DIVIDE((i.Net_profit_LC - p._15th_percentile_net_profit_lc), (p._95th_percentile_net_profit_lc - p._15th_percentile_net_profit_lc))* 100 END, 0), 3) AS retail_profit_score_lc,


        -- Importance to Business Scoring ------------
        -- Calculate the score for ABV (Weight 50%)
        ROUND(COALESCE(CASE 
                WHEN ABV_LC_order >= p._95th_percentile_ABV_lc THEN 50 * 1.0
                WHEN ABV_LC_order <= p._15th_percentile_ABV_lc THEN 0
                ELSE SAFE_DIVIDE( (ABV_LC_order - p._15th_percentile_ABV_lc), (p._95th_percentile_ABV_lc - p._15th_percentile_ABV_lc))* 50 END, 0), 3) AS abv_score_lc,

        -- Calculate the score for ABV (Weight 50%)
        ROUND(COALESCE(CASE 
                WHEN ABV_EUR_order >= p._95th_percentile_ABV_eur THEN 50 * 1.0
                WHEN ABV_EUR_order <= p._15th_percentile_ABV_eur THEN 0
                ELSE SAFE_DIVIDE((ABV_EUR_order - p._15th_percentile_ABV_eur), (p._95th_percentile_ABV_eur - p._15th_percentile_ABV_eur))* 50 END, 0), 3) AS abv_score_eur,

        -- Calculate the score for Frequency (Weight 30%)
        ROUND(COALESCE(CASE 
                WHEN Frequency >= p._95th_percentile_frequency THEN 30 * 1.0
                WHEN Frequency <= p._15th_percentile_frequency THEN 0
                ELSE SAFE_DIVIDE((Frequency - p._15th_percentile_frequency), (p._95th_percentile_frequency - p._15th_percentile_frequency)) * 30 END, 0), 3) AS frequency_score,

        -- Calculate the score for Customer Penetration (Weight 20%)
        ROUND(COALESCE(CASE 
                WHEN customer_penetration >= p._95th_percentile_customer_penetration THEN 20 * 1.0
                WHEN customer_penetration <= p._15th_percentile_customer_penetration THEN 0
                ELSE ( SAFE_DIVIDE((customer_penetration - p._15th_percentile_customer_penetration), 
        (p._95th_percentile_customer_penetration - p._15th_percentile_customer_penetration)) ) * 20
            END, 0), 3) AS customer_penetration_score

    FROM Intermedia i

    LEFT JOIN percentiles p
    ON
    i.country_code = p.country_code
    AND
    i.time_period = p.time_period
    AND
    i.time_granularity = p.time_granularity
    AND
    i.division_type = p.division_type

    GROUP BY ALL
    )
    ,


    processed AS (

    SELECT

    *,
    retail_profit_score_lc AS supplier_importance_score_lc,
    ROUND(SUM(abv_score_lc + frequency_score + customer_penetration_score),3) AS supplier_productivity_score_lc,
    NULL AS supplier_importance_score_eur,
    ROUND(SUM(abv_score_eur + frequency_score + customer_penetration_score),3) AS supplier_productivity_score_eur,


    FROM
    Scoring

    GROUP BY ALL
    
    )

    SELECT

    *,
    CASE 
    WHEN supplier_importance_score_lc > 15 AND supplier_productivity_score_lc >= 40 THEN 'Key Accounts'
    WHEN supplier_importance_score_lc > 15 AND supplier_productivity_score_lc <= 40 THEN 'Standard'
    WHEN supplier_importance_score_lc <= 15 AND supplier_productivity_score_lc > 40 THEN 'Niche'
    WHEN supplier_importance_score_lc <= 15 AND supplier_productivity_score_lc <= 40 THEN 'Long Tail'
    END
    AS Segmentation_LC,
    CASE 
    WHEN supplier_importance_score_eur > 15 AND supplier_productivity_score_eur >= 40 THEN 'Key Accounts'
    WHEN supplier_importance_score_eur > 15 AND supplier_productivity_score_eur <= 40 THEN 'Standard'
    WHEN supplier_importance_score_eur <= 15 AND supplier_productivity_score_eur > 40 THEN 'Niche'
    WHEN supplier_importance_score_eur <= 15 AND supplier_productivity_score_eur <= 40 THEN 'Long Tail'
    END
    AS Segmentation_EUR,
    
    FROM processed  as o
        
    GROUP BY ALL
