CREATE OR REPLACE TABLE `{{ params.project_id }}.rl_dmart._srm_supplier_scorecard_supplier_rough`  AS 

WITH 
  ----------------------- monthly and quarterly orders AND customers table ---------------------------------------------------------------
  orders_month AS
  (
    SELECT

    o.global_entity_id,
    s.common_name as country_name,
    s.region,
    DATE(DATE_TRUNC(o.order_created_date_lt, MONTH)) AS order_month,
    count(distinct order_id) as total_orders,
    count(distinct analytical_customer_id) as total_customers

    FROM
    `fulfillment-dwh-production.cl_dmart.qc_orders` o

    LEFT JOIN
    `fulfillment-dwh-production.cl_dmart.sources` as s
    
    ON
    o.global_entity_id = s.global_entity_id

    WHERE
    
    DATE(o.order_created_date_lt) BETWEEN '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND o.is_dmart IS TRUE
    AND o.is_successful IS TRUE

    GROUP BY ALL
    ORDER BY 2,4 ASC
  ),

    orders_quarterly AS
  (
    SELECT
    
    o.global_entity_id,
    s.common_name as country_name,
    s.region,
    CONCAT('Q', EXTRACT(QUARTER FROM o.order_created_date_lt), '-', EXTRACT(YEAR FROM o.order_created_date_lt)) AS quarter_year,
    count(distinct order_id) as total_orders,
    count(distinct analytical_customer_id) as total_customers

    FROM
    `fulfillment-dwh-production.cl_dmart.qc_orders` o

    LEFT JOIN
    `fulfillment-dwh-production.cl_dmart.sources` as s
    
    ON
    o.global_entity_id = s.global_entity_id

    WHERE
    
    DATE(o.order_created_date_lt) BETWEEN '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND o.is_dmart IS TRUE
    AND o.is_successful IS TRUE

    GROUP BY ALL
    ORDER BY 2,4 ASC),

  ----------------------- monthly and quarterly order_level GPV where supplier's sku was present at least once in the orders ---------------------------------------------------------------

    order_level_gpv AS (
    
    SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    CAST(o.supplier_id AS STRING) as supplier_id,
    CAST(o.principal_supplier_id AS STRING) as principal_supplier_id,
    o.parent_name as principal_supplier_name,
    o.supplier_name,
    o.brand_owner_name,
    CAST(o.month AS STRING) as month,
    o.quarter_year,
    o.order_id,
    MAX(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    MAX(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    WHERE
    o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
    AND month BETWEEN '2023-7-1' AND CURRENT_DATE()
    GROUP BY ALL),

    ----------------------------------------------------------------------- DIVISION LEVEL -----------------------------------------------------------------------

    division_order_level_gpv_monthly AS (

     SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    CAST(o.supplier_id AS STRING) as supplier_id,
    o.supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    CAST(o.month AS STRING) as time_period,
    SUM(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    SUM(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    order_level_gpv as o

    GROUP BY ALL),

    division_order_level_gpv_quarterly AS (

     SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    CAST(o.supplier_id AS STRING) as supplier_id,
    o.supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    quarter_year as time_period,
    SUM(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    SUM(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    order_level_gpv as o

    GROUP BY ALL),

    ----------------------------------------------------------------------- PRINCIPAL LEVEL -----------------------------------------------------------------------

    principal_order_level_gpv_monthly AS (

     SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    principal_supplier_id as supplier_id,
    o.principal_supplier_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    CAST(o.month AS STRING) as time_period,
    SUM(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    SUM(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    order_level_gpv as o

    GROUP BY ALL),



    principal_order_level_gpv_quarterly AS (

     SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    principal_supplier_id as supplier_id,
    o.principal_supplier_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    quarter_year as time_period,
    SUM(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    SUM(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    order_level_gpv as o

    GROUP BY ALL),

    ----------------------------------------------------------------------- BRANDOWNER LEVEL -----------------------------------------------------------------------


    brand_owner_order_level_gpv_monthly AS (

     SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    CAST(NULL AS STRING) AS supplier_id,
    CAST(NULL AS STRING) AS supplier_name,
    o.brand_owner_name, 
    CAST(o.month AS STRING) as time_period,
    SUM(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    SUM(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    order_level_gpv as o

    GROUP BY ALL),



    brand_owner_order_level_gpv_quarterly AS (

     SELECT
    o.global_entity_id,
    o.country_code,
    o.country_name,
    CAST(NULL AS STRING) AS supplier_id,
    CAST(NULL AS STRING) AS supplier_name,
    o.brand_owner_name, 
    quarter_year as time_period,
    SUM(o.amt_total_price_paid_net_eur) AS amt_total_price_paid_net_eur,
    SUM(o.amt_total_price_paid_net_lc) AS amt_total_price_paid_net_lc,

    FROM
    order_level_gpv as o

    GROUP BY ALL),
  
 products AS (

    SELECT
    global_entity_id, country_code, sku_id, brand_owner_name

    FROM
    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    WHERE
    o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL

  ),

----------------------- back_margin_calculation in euros and local_currency on monthly and quarterly level --------------------------------------------------------------



  ----------------------- Division Supplier back_margin_calculation in euros and local_currency on monthly and quarterly level --------------------------------------------------------------
  
  division_net_purchases_monthly AS (
    SELECT  
    r.country_code,
    r.sup_id AS supplier_id,
    h.supplier_name,
    DATE(r.received_local_month) as month,    
    ROUND(SUM(COALESCE(r.delivered_net_amount, 0) - COALESCE(r.returned_net_amount, 0)), 2) AS Net_Purchases,

  FROM
    `fulfillment-dwh-production.cl_dmart.rb_monthly_sku_amount` AS r

  LEFT JOIN
  `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` AS h
  
  ON
        r.country_code = h.country_code
        AND CAST(r.sup_id AS STRING) = CAST(h.supplier_id AS STRING)

  WHERE
  r.country_code IN ('kw', 'pe', 'ar', 'qa', 'ae', 'sg')

  GROUP BY ALL
  ),

   division_back_margins_monthly AS (

SELECT  
    global_entity_id,
    supplier_id,
    supplier_name,
    month,
    CAST(NULL AS STRING) AS brand_owner_name,
    ROUND(SUM(total_rebate_lc), 4) AS total_rebate_lc,
    ROUND(SUM(total_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc

FROM (
    SELECT
        r.global_entity_id,
        r.sup_id AS supplier_id,
        r.month,
        r.supplier_name,
        r.term_value_type,
        ROUND(SUM(r.rebate), 4) AS total_rebate_lc,
        ROUND(SUM(CASE WHEN r.trading_term_name != 'Distribution Allowance' THEN r.rebate ELSE 0 END), 4) AS total_rebate_wo_dist_allowance_lc

    FROM 
        `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS r

    WHERE
        r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
        AND r.trading_term_type NOT IN ('Frontmargin')

    GROUP BY ALL
) AS grouped_data

GROUP BY ALL),

----------------------------------------------------------------------------------------------------------------------------------------------------------------

  division_net_purchases_quarterly AS (
    SELECT  
    r.country_code,
    r.sup_id AS supplier_id,
    h.supplier_name,
    CONCAT('Q', EXTRACT(QUARTER FROM DATE(r.received_local_month)), '-', EXTRACT(YEAR FROM DATE(r.received_local_month))) AS quarter_year,    
    ROUND(SUM(COALESCE(r.delivered_net_amount, 0) - COALESCE(r.returned_net_amount, 0)), 2) AS Net_Purchases,

  FROM
    `fulfillment-dwh-production.cl_dmart.rb_monthly_sku_amount` AS r

  LEFT JOIN
  `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` AS h
  
  ON
        r.country_code = h.country_code
        AND CAST(r.sup_id AS STRING) = CAST(h.supplier_id AS STRING)

  WHERE
  r.country_code IN ('kw', 'pe', 'ar', 'qa', 'ae', 'sg')

  GROUP BY ALL
  ),

    division_back_margins_quarterly AS (

   SELECT
    global_entity_id,
    quarter_year,
    supplier_id,
    supplier_name,
    brand_owner_name,
    ROUND(SUM(total_rebate_lc), 4) AS total_rebate_lc,
    ROUND(SUM(total_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc

FROM (
    SELECT
        r.global_entity_id,
        CONCAT('Q', EXTRACT(QUARTER FROM DATE(r.month)), '-', EXTRACT(YEAR FROM DATE(r.month))) AS quarter_year,
        r.sup_id AS supplier_id,
        r.supplier_name,
        CAST(NULL AS STRING) AS brand_owner_name,
        r.term_value_type,  
        ROUND(SUM(r.rebate), 4) AS total_rebate_lc,
        ROUND(SUM(CASE WHEN r.trading_term_name != 'Distribution Allowance' THEN r.rebate ELSE 0 END), 4) AS total_rebate_wo_dist_allowance_lc

    FROM 
        `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS r

    WHERE
        r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
        AND r.trading_term_type NOT IN ('Frontmargin')

    GROUP BY ALL
) AS grouped_data

GROUP BY ALL),

    ----------------------- Principal Supplier back_margin_calculation in euros and local_currency on monthly and quarterly level --------------------------------------------------------------

  principal_net_purchases_monthly AS (

    SELECT  
    r.country_code,
    h.principal_supplier_id as supplier_id,
    h.parent_name as supplier_name,
    DATE(r.received_local_month) as month,    
    ROUND(SUM(COALESCE(r.delivered_net_amount, 0) - COALESCE(r.returned_net_amount, 0)), 2) AS Net_Purchases,

  FROM
    `fulfillment-dwh-production.cl_dmart.rb_monthly_sku_amount` AS r

   LEFT JOIN
        `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` AS h
    ON
        r.country_code = h.country_code
        AND CAST(r.sup_id AS STRING) = CAST(h.supplier_id AS STRING)

  WHERE
  r.country_code IN ('kw', 'pe', 'ar', 'qa', 'ae', 'sg')

  GROUP BY ALL
  ),


  principal_back_margins_monthly AS (

SELECT
    global_entity_id,
    month,
    supplier_id,
    supplier_name,
    brand_owner_name,
    ROUND(SUM(total_rebate_lc), 4) AS total_rebate_lc,
    ROUND(SUM(total_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc

FROM (
    SELECT
        r.global_entity_id,
        r.month,
        h.principal_supplier_id AS supplier_id,
        h.parent_name AS supplier_name,
        CAST(NULL AS STRING) AS brand_owner_name,
        r.term_value_type,
        ROUND(SUM(r.rebate), 4) AS total_rebate_lc,
        ROUND(SUM(CASE WHEN r.trading_term_name != 'Distribution Allowance' THEN r.rebate ELSE 0 END), 4) AS total_rebate_wo_dist_allowance_lc

    FROM 
        `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS r

    LEFT JOIN
        `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` AS h
    ON
        r.global_entity_id = h.global_entity_id
        AND CAST(r.sup_id AS STRING) = CAST(h.supplier_id AS STRING)

    WHERE
        r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
        AND r.trading_term_type NOT IN ('Frontmargin')

    GROUP BY ALL
) AS grouped_data

GROUP BY ALL),


----------------------------------------------------------------------------------------------------------------------------------------------------------------
  principal_net_purchases_quarterly AS (

    SELECT  
    r.country_code,
    h.principal_supplier_id as supplier_id,
    h.parent_name as supplier_name,
    CONCAT('Q', EXTRACT(QUARTER FROM DATE(r.received_local_month)), '-', EXTRACT(YEAR FROM DATE(r.received_local_month))) AS quarter_year,    
    ROUND(SUM(COALESCE(r.delivered_net_amount, 0) - COALESCE(r.returned_net_amount, 0)), 2) AS Net_Purchases,

  FROM
    `fulfillment-dwh-production.cl_dmart.rb_monthly_sku_amount` AS r

   LEFT JOIN
        `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` AS h
    ON
        r.country_code = h.country_code
        AND CAST(r.sup_id AS STRING) = CAST(h.supplier_id AS STRING)

  WHERE
  r.country_code IN ('kw', 'pe', 'ar', 'qa', 'ae', 'sg')

  GROUP BY ALL
  ),


    principal_back_margins_quarterly AS (

    SELECT
    global_entity_id,
    quarter_year,
    supplier_id,
    supplier_name,
    brand_owner_name,
    ROUND(SUM(total_rebate_lc), 4) AS total_rebate_lc,
    ROUND(SUM(total_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc

FROM (
    SELECT
        r.global_entity_id,
        CONCAT('Q', EXTRACT(QUARTER FROM DATE(r.month)), '-', EXTRACT(YEAR FROM DATE(r.month))) AS quarter_year,
        h.principal_supplier_id AS supplier_id,
        h.parent_name AS supplier_name,
        CAST(NULL AS STRING) AS brand_owner_name,
        r.term_value_type,
        ROUND(SUM(r.rebate), 4) AS total_rebate_lc,
        ROUND(SUM(CASE WHEN r.trading_term_name != 'Distribution Allowance' THEN r.rebate ELSE 0 END), 4) AS total_rebate_wo_dist_allowance_lc

    FROM 
        `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS r

    LEFT JOIN
        `fulfillment-dwh-production.cl_dmart._srm_supplier_supplier_hierarchy` AS h
    ON
        r.global_entity_id = h.global_entity_id
        AND CAST(r.sup_id AS STRING) = CAST(h.supplier_id AS STRING)

    WHERE
        r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
        AND r.trading_term_type NOT IN ('Frontmargin')

    GROUP BY ALL
) AS grouped_data

GROUP BY ALL),

      ----------------------- Brandowner back_margin_calculation in euros and local_currency on monthly and quarterly level --------------------------------------------------------------
 
 brand_owner_net_purchases_monthly AS (

      SELECT  
    r.country_code,
    h.brand_owner_name,
    DATE(r.received_local_month) as month,   
    ROUND(SUM(COALESCE(r.delivered_net_amount, 0) - COALESCE(r.returned_net_amount, 0)), 2) AS Net_Purchases,

  FROM
    `fulfillment-dwh-production.cl_dmart.rb_monthly_sku_amount` AS r

   LEFT JOIN
    products AS h
    ON
        r.country_code = h.country_code
        AND CAST(r.sku AS STRING) = CAST(h.sku_id AS STRING)

  WHERE
  r.country_code IN ('kw', 'pe', 'ar', 'qa', 'ae', 'sg')

  GROUP BY ALL
 ),


  brandowner_back_margins_monthly AS (

SELECT
    global_entity_id,
    month,
    brand_owner_name,
    ROUND(SUM(total_rebate_lc), 4) AS total_rebate_lc,
    ROUND(SUM(total_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc

FROM (
    SELECT
        r.global_entity_id,
        r.month,
        p.brand_owner_name,
        r.term_value_type,
        ROUND(SUM(r.rebate), 4) AS total_rebate_lc,
        ROUND(SUM(CASE WHEN r.trading_term_name != 'Distribution Allowance' THEN r.rebate ELSE 0 END), 4) AS total_rebate_wo_dist_allowance_lc

    FROM 
        `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS r

    LEFT JOIN
        products AS p
    ON
        r.global_entity_id = p.global_entity_id
        AND r.sku = p.sku_id

    WHERE
        r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
        AND r.trading_term_type NOT IN ('Frontmargin')

    GROUP BY ALL
) AS grouped_data

GROUP BY ALL),

------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

 brand_owner_net_purchases_quarterly AS (

      SELECT  
    r.country_code,
    h.brand_owner_name,
    CONCAT('Q', EXTRACT(QUARTER FROM DATE(r.received_local_month)), '-', EXTRACT(YEAR FROM DATE(r.received_local_month))) AS quarter_year,    
    ROUND(SUM(COALESCE(r.delivered_net_amount, 0) - COALESCE(r.returned_net_amount, 0)), 2) AS Net_Purchases,

  FROM
    `fulfillment-dwh-production.cl_dmart.rb_monthly_sku_amount` AS r

   LEFT JOIN
    products AS h
    ON
        r.country_code = h.country_code
        AND CAST(r.sku AS STRING) = CAST(h.sku_id AS STRING)

  WHERE
  r.country_code IN ('kw', 'pe', 'ar', 'qa', 'ae', 'sg')

  GROUP BY ALL
 ),


  brandowner_back_margins_quarterly AS (

   SELECT
    global_entity_id,
    quarter_year,
    supplier_id,
    supplier_name,
    brand_owner_name,

    ROUND(SUM(total_rebate_lc), 4) AS total_rebate_lc,
    ROUND(SUM(total_rebate_wo_dist_allowance_lc), 4) AS total_rebate_wo_dist_allowance_lc

FROM (
    SELECT
        r.global_entity_id,
        CONCAT('Q', EXTRACT(QUARTER FROM DATE(r.month)), '-', EXTRACT(YEAR FROM DATE(r.month))) AS quarter_year,
        CAST(NULL AS STRING) AS supplier_id,  -- Supplier ID is set to NULL
        CAST(NULL AS STRING) AS supplier_name,  -- Supplier Name is set to NULL
        p.brand_owner_name,
        r.term_value_type,
        
        ROUND(SUM(r.rebate), 4) AS total_rebate_lc,
        ROUND(SUM(CASE WHEN r.trading_term_name != 'Distribution Allowance' THEN r.rebate ELSE 0 END), 4) AS total_rebate_wo_dist_allowance_lc

    FROM 
        `fulfillment-dwh-production.cl_dmart.rb_line_rebate` AS r

    LEFT JOIN
        products AS p
    ON
        r.global_entity_id = p.global_entity_id
        AND r.sku = p.sku_id

    WHERE
        r.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')
        AND r.trading_term_type NOT IN ('Frontmargin')

    GROUP BY ALL
) AS grouped_data

GROUP BY ALL),

  ------------------------------------ pre_final table on quarterly and monthly leve -----------------------------------


  ------------------------------------ Division Supplier Level -----------------------------------


  division_supplier_final_monthly AS (

    SELECT
    'supplier' as level,
    'division' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    os.region,
    'Monthly' as time_granularity,
    CAST(o.month AS STRING) as time_period,
    CAST(o.supplier_id AS STRING) as supplier_id,
    o.supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    o.supplier_name as segment_name,
    (CASE WHEN o.supplier_name IS NULL THEN TRUE ELSE FALSE END) as name_missing,
    ---------- Monthly orders and customers ---------------
    os.total_orders as total_orders_for_the_month, 
    os.total_customers as total_customers_for_month,
    ---------- Total Net Sales (net amount paid by customer) for all the orders that supplier was a part of ------------
    ROUND((olg.amt_total_price_paid_net_eur),2) as Total_Net_Sales_EUR_order,
    ROUND((olg.amt_total_price_paid_net_lc),2) as Total_Net_Sales_LC_order,
  --------- Commercial Metrics------------------------
    COUNT(DISTINCT o.sku_id) as skus_sold,
    ------------ EUR ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_eur),0), 2) AS Net_Sales_EUR,
    ROUND(IFNULL(SUM(o.COGS_EUR),0)) AS COGS_EUR,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_eur > 0 THEN o.total_price_paid_net_eur END),0),2) AS Net_Sales_from_promo_eur,
    ROUND(IFNULL(SUM(o.total_supplier_funding_eur),0),2) AS total_supplier_funding_eur,
    ROUND((SUM(o.total_price_paid_net_eur) - SUM(o.COGS_EUR)), 2) AS front_margin_eur,
    ROUND(IFNULL(SUM(o.total_discount_eur),0)) AS total_discount_eur,
    ------------ LC ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_lc),0), 2) AS Net_Sales_LC,
    ROUND(IFNULL(SUM(o.COGS_LC),0)) AS COGS_LC,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_lc > 0 THEN o.total_price_paid_net_lc END),0),2) AS Net_Sales_from_promo_lc,
    ROUND(IFNULL(SUM(o.total_supplier_funding_lc),0),2) AS total_supplier_funding_lc,
    ROUND((SUM(o.total_price_paid_net_lc) - SUM(o.COGS_lc)), 2) AS front_margin_lc,
    ROUND(IFNULL(SUM(o.total_discount_lc),0)) AS total_discount_lc,
    ------- Order level data for the supplier---------------
    count(distinct o.order_id) as count_of_orders,
    count(distinct o.analytical_customer_id) as count_of_customers,
    ROUND(SUM (o.amt_gbv_eur),2) as total_GBV,
    ROUND(SUM (o.fulfilled_quantity),2) as fulfilled_quantity,
    COUNT(DISTINCT warehouse_id) as warehouses_sold,
    ------- Back-margin data for the supplier ---------------
    ROUND((total_rebate_lc),3) as total_rebate_lc,
    ROUND((total_rebate_wo_dist_allowance_lc),3) as total_rebate_wo_dist_allowance_lc,
    ROUND((Net_Purchases),3) as Net_Purchases

    FROM

    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    INNER JOIN

    orders_month as os

    ON 

    o.month = os.order_month
    AND o.global_entity_id = os.global_entity_id


    INNER JOIN

    division_order_level_gpv_monthly as olg

    ON 

    olg.global_entity_id = o.global_entity_id
    AND CAST(olg.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND CAST(olg.time_period AS STRING) = CAST(o.month AS STRING)
    AND CAST(olg.time_period AS STRING) = CAST(o.month AS STRING)



    LEFT JOIN

    division_back_margins_monthly as bm

    ON

    bm.global_entity_id = o.global_entity_id
    AND CAST(bm.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(bm.supplier_name)) = LOWER(TRIM(o.supplier_name))
    AND bm.month = o.month

    LEFT JOIN

    division_net_purchases_monthly as np

    ON

    np.country_code = o.country_code
    AND CAST(np.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(np.supplier_name)) = LOWER(TRIM(o.supplier_name))
    AND np.month = o.month
      
      
    WHERE
    o.order_date between '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL),



  division_supplier_final_quarterly AS (  

    SELECT
    'supplier' as level,
    'division' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    os.region,
    'Quarterly' as time_granularity,
    o.quarter_year as time_period,
    CAST(o.supplier_id AS STRING) as supplier_id,
    o.supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    o.supplier_name as segment_name,
    (CASE WHEN o.supplier_name IS NULL THEN TRUE ELSE FALSE END) as name_missing,
    ---------- Monthly orders and customers ---------------
    os.total_orders as total_orders_for_the_month, 
    os.total_customers as total_customers_for_month,
    ---------- Total Net Sales (net amount paid by customer) for all the orders that supplier was a part of ------------
    ROUND((olg.amt_total_price_paid_net_eur),2) as Total_Net_Sales_EUR_order,
    ROUND((olg.amt_total_price_paid_net_lc),2) as Total_Net_Sales_LC_order,
  --------- Commercial Metrics------------------------
    COUNT(DISTINCT o.sku_id) as skus_sold,
    ------------ EUR ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_eur),0), 2) AS Net_Sales_EUR,
    ROUND(IFNULL(SUM(o.COGS_EUR),0)) AS COGS_EUR,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_eur > 0 THEN o.total_price_paid_net_eur END),0),2) AS Net_Sales_from_promo_eur,
    ROUND(IFNULL(SUM(o.total_supplier_funding_eur),0),2) AS total_supplier_funding_eur,
    ROUND((SUM(o.total_price_paid_net_eur) - SUM(o.COGS_EUR)), 2) AS front_margin_eur,
    ROUND(IFNULL(SUM(o.total_discount_eur),0)) AS total_discount_eur,
    ------------ LC ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_lc),0), 2) AS Net_Sales_LC,
    ROUND(IFNULL(SUM(o.COGS_LC),0)) AS COGS_LC,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_lc > 0 THEN o.total_price_paid_net_lc END),0),2) AS Net_Sales_from_promo_lc,
    ROUND(IFNULL(SUM(o.total_supplier_funding_lc),0),2) AS total_supplier_funding_lc,
    ROUND((SUM(o.total_price_paid_net_lc) - SUM(o.COGS_lc)), 2) AS front_margin_lc,
    ROUND(IFNULL(SUM(o.total_discount_lc),0)) AS total_discount_lc,
    ------- Order level data for the supplier---------------
    count(distinct o.order_id) as count_of_orders,
    count(distinct o.analytical_customer_id) as count_of_customers,
    ROUND(SUM (o.amt_gbv_eur),2) as total_GBV,
    ROUND(SUM (o.fulfilled_quantity),2) as fulfilled_quantity,
    COUNT(DISTINCT warehouse_id) as warehouses_sold,
    ------- Back-margin data for the supplier ---------------
    ROUND((total_rebate_lc),3) as total_rebate_lc,
    ROUND((total_rebate_wo_dist_allowance_lc),3) as total_rebate_wo_dist_allowance_lc,
    ROUND((Net_Purchases),3) as Net_Purchases  

    FROM

    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    INNER JOIN

    orders_quarterly as os

    ON 

    o.quarter_year = os.quarter_year
    AND o.global_entity_id = os.global_entity_id

    
    INNER JOIN

    division_order_level_gpv_quarterly as olg

    ON 

    olg.global_entity_id = o.global_entity_id
    AND CAST(olg.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND olg.time_period = o.quarter_year


    LEFT JOIN

    division_back_margins_quarterly as bm

    ON

    bm.global_entity_id = o.global_entity_id
    AND CAST(bm.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(bm.supplier_name)) = LOWER(TRIM(o.supplier_name))
    AND bm.quarter_year = o.quarter_year

    LEFT JOIN

    division_net_purchases_quarterly as np

    ON

    np.country_code = o.country_code
    AND CAST(np.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(bm.supplier_name)) = LOWER(TRIM(o.supplier_name))
    AND np.quarter_year = o.quarter_year
      
      
    WHERE
    o.order_date between '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL),


    ------------------------------------ Principal Supplier Level -----------------------------------

    -------------------------------------- Making a base table for principal level so that the level of the table is the same when joining for principal level tables. -----


    principal_supplier_final_base AS (  

    SELECT

    o.global_entity_id,
    o.country_code,
    o.country_name,
    os.region,
    CAST(month as STRING) as month,
    CAST(quarter_year AS STRING) AS quarter_year,
    CAST(o.principal_supplier_id AS STRING) AS supplier_id,
    o.parent_name as supplier_name,
    CAST(NULL AS STRING) AS brand_owner_name,
    o.parent_name as segment_name,
    (CASE WHEN o.parent_name IS NULL THEN TRUE ELSE FALSE END) as name_missing,
    ---------- Monthly orders and customers ---------------
    os.total_orders as total_orders_for_the_month, 
    os.total_customers as total_customers_for_month,
  --------- Commercial Metrics------------------------
    COUNT(DISTINCT o.sku_id) as skus_sold,
    ------------ EUR ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_eur),0), 2) AS Net_Sales_EUR,
    ROUND(IFNULL(SUM(o.COGS_EUR),0)) AS COGS_EUR,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_eur > 0 THEN o.total_price_paid_net_eur END),0),2) AS Net_Sales_from_promo_eur,
    ROUND(IFNULL(SUM(o.total_supplier_funding_eur),0),2) AS total_supplier_funding_eur,
    ROUND((SUM(o.total_price_paid_net_eur) - SUM(o.COGS_EUR)), 2) AS front_margin_eur,
    ROUND(IFNULL(SUM(o.total_discount_eur),0)) AS total_discount_eur,
    ------------ LC ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_lc),0), 2) AS Net_Sales_LC,
    ROUND(IFNULL(SUM(o.COGS_LC),0)) AS COGS_LC,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_lc > 0 THEN o.total_price_paid_net_lc END),0),2) AS Net_Sales_from_promo_lc,
    ROUND(IFNULL(SUM(o.total_supplier_funding_lc),0),2) AS total_supplier_funding_lc,
    ROUND((SUM(o.total_price_paid_net_lc) - SUM(o.COGS_lc)), 2) AS front_margin_lc,
    ROUND(IFNULL(SUM(o.total_discount_lc),0)) AS total_discount_lc,
    ------- Order level data for the supplier---------------
    count(distinct o.order_id) as count_of_orders,
    count(distinct o.analytical_customer_id) as count_of_customers,
    ROUND(SUM (o.amt_gbv_eur),2) as total_GBV,
    ROUND(SUM (o.fulfilled_quantity),2) as fulfilled_quantity,
    COUNT(DISTINCT warehouse_id) as warehouses_sold,

    FROM

    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    INNER JOIN

    orders_month as os

    ON 

    o.month = os.order_month
    AND o.global_entity_id = os.global_entity_id
      
    WHERE
    o.order_date between '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL),

    principal_supplier_final_monthly AS (

    SELECT

    'supplier' as level,
    'principal' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    o.region,
    'Monthly' as time_granularity,
    o.month as time_period,
    o.supplier_id,
    o.supplier_name,
    o.brand_owner_name,
    o.segment_name,
    o.name_missing,
    ---------- Monthly orders and customers ---------------
    o.total_orders_for_the_month, 
    o.total_customers_for_month,
    ---------- Total Net Sales (net amount paid by customer) for all the orders that supplier was a part of ------------
    ROUND((olg.amt_total_price_paid_net_eur),2) as Total_Net_Sales_EUR_order,
    ROUND((olg.amt_total_price_paid_net_lc),2) as Total_Net_Sales_LC_order,
  --------- Commercial Metrics------------------------
    o.skus_sold,
    ------------ EUR ----------------------------------------
    o.Net_Sales_EUR,
    o.COGS_EUR,
    o.Net_Sales_from_promo_eur,
    o.total_supplier_funding_eur,
    o.front_margin_eur,
    o.total_discount_eur,
    ------------ LC ----------------------------------------
    o.Net_Sales_LC,
    o.COGS_LC,
    o.Net_Sales_from_promo_lc,
    o.total_supplier_funding_lc,
    o.front_margin_lc,
    o.total_discount_lc,
    ------- Order level data for the supplier---------------
    o.count_of_orders,
    o.count_of_customers,
    o.total_GBV,
    o.fulfilled_quantity,
    o.warehouses_sold,
    ------- Back-margin data for the supplier ---------------
    ROUND((total_rebate_lc),3) as total_rebate_lc,
    ROUND((total_rebate_wo_dist_allowance_lc),3) as total_rebate_wo_dist_allowance_lc,
    ROUND((Net_Purchases),3) as Net_Purchases

    FROM
    principal_supplier_final_base as o


    INNER JOIN

    principal_order_level_gpv_monthly as olg

    ON 

    CAST(o.month AS STRING) = olg.time_period
    AND 
    o.global_entity_id = olg.global_entity_id
    AND CAST(o.supplier_id AS STRING) = CAST(olg.supplier_id AS STRING)

    LEFT JOIN

    principal_back_margins_monthly as bm

    ON

    bm.global_entity_id = o.global_entity_id
    AND CAST(bm.month AS STRING) = o.month
    AND CAST(bm.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(bm.supplier_name)) = LOWER(TRIM(o.supplier_name))

    LEFT JOIN

    principal_net_purchases_monthly as np

    ON

    np.country_code = o.country_code
    AND CAST(np.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(np.supplier_name)) = LOWER(TRIM(o.supplier_name))
    AND CAST(np.month AS STRING) = o.month

    GROUP BY ALL
    ),

  principal_supplier_final_quarterly AS (  

    SELECT
    'supplier' as level,
    'principal' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    o.region,
    'Quarterly' as time_granularity,
    CAST(o.quarter_year AS STRING) as time_period,
    o.supplier_id,
    o.supplier_name,
    o.brand_owner_name,
    CAST(NULL AS STRING) AS segment_name,
    CAST(NULL AS BOOL) AS name_missing,
    ---------- Monthly orders and customers ---------------
    SUM(o.total_orders_for_the_month) as total_orders_for_the_month, 
    SUM(o.total_customers_for_month) as total_customers_for_month,
       ---------- Total Net Sales (net amount paid by customer) for all the orders that supplier was a part of ------------
    ROUND((olg.amt_total_price_paid_net_eur),2) as Total_Net_Sales_EUR_order,
    ROUND((olg.amt_total_price_paid_net_lc),2) as Total_Net_Sales_LC_order,
  --------- Commercial Metrics------------------------
    ROUND(AVG(skus_sold),0) as skus_sold,
    ------------ EUR ----------------------------------------
    ROUND(IFNULL(SUM(o.Net_Sales_EUR),0), 2) AS Net_Sales_EUR,
    ROUND(IFNULL(SUM(o.COGS_EUR),0)) AS COGS_EUR,
    ROUND(IFNULL(SUM(Net_Sales_from_promo_eur),0),2) AS Net_Sales_from_promo_eur,
    ROUND(IFNULL(SUM(o.total_supplier_funding_eur),0),2) AS total_supplier_funding_eur,
    ROUND(IFNULL(SUM(o.front_margin_eur),0),2) AS front_margin_eur,
    ROUND(IFNULL(SUM(o.total_discount_eur),0)) AS total_discount_eur,
    ------------ LC ----------------------------------------
    ROUND(IFNULL(SUM(o.Net_Sales_LC),0), 2) AS Net_Sales_LC,
    ROUND(IFNULL(SUM(o.COGS_LC),0)) AS COGS_LC,
    ROUND(IFNULL(SUM(Net_Sales_from_promo_lc),0),2) AS Net_Sales_from_promo_lc,
    ROUND(IFNULL(SUM(o.total_supplier_funding_lc),0),2) AS total_supplier_funding_lc,
    ROUND(IFNULL(SUM(o.front_margin_lc),0),2) AS front_margin_lc,
    ROUND(IFNULL(SUM(o.total_discount_lc),0)) AS total_discount_lc,
    ------- Order level data for the supplier---------------
    ROUND(IFNULL(SUM(o.count_of_orders),0),0) AS count_of_orders,
    ROUND(AVG(count_of_customers),0) AS count_of_customers,
    ROUND(SUM (o.total_GBV),2) as total_GBV,
    ROUND(SUM (o.fulfilled_quantity),2) as fulfilled_quantity,
    ROUND(AVG(warehouses_sold),0) as warehouses_sold,
    ------- Back-margin data for the supplier ---------------
    ROUND((total_rebate_lc),3) as total_rebate_lc,
    ROUND((total_rebate_wo_dist_allowance_lc),3) as total_rebate_wo_dist_allowance_lc,
    ROUND((Net_Purchases),3) as Net_Purchases

    FROM
    principal_supplier_final_base as o


    INNER JOIN

    principal_order_level_gpv_quarterly as olg

    ON 

    CAST(o.quarter_year AS STRING) = olg.time_period
    AND 
    o.global_entity_id = olg.global_entity_id
    AND CAST(o.supplier_id AS STRING) = CAST(olg.supplier_id AS STRING)

    LEFT JOIN

    principal_back_margins_quarterly as bm

    ON

    bm.global_entity_id = o.global_entity_id
    AND CAST(bm.quarter_year AS STRING) = o.quarter_year
    AND CAST(bm.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(bm.supplier_name)) = LOWER(TRIM(o.supplier_name))

    LEFT JOIN

    principal_net_purchases_quarterly as np

    ON

    np.country_code = o.country_code
    AND CAST(np.supplier_id AS STRING) = CAST(o.supplier_id AS STRING)
    AND LOWER(TRIM(np.supplier_name)) = LOWER(TRIM(o.supplier_name))
    AND CAST(np.quarter_year AS STRING) = o.quarter_year
    
    
    GROUP BY ALL),

    ------------------------------------------------------------------------ Brand owner Level -----------------------------------------------------------------------------------------------------------


    brand_owner_final_monthly AS (  

    SELECT
    'supplier' as level,
    'brand_owner' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    os.region,
    'Monthly' as time_granularity,
    CAST(o.month AS STRING) as time_period,
    CAST(NULL AS STRING) as supplier_id,
    CAST(NULL AS STRING) AS supplier_name,
    o.brand_owner_name,
    CAST(NULL AS STRING) AS segment_name,
    CAST(NULL AS BOOL) AS name_missing,
    ---------- Monthly orders and customers ---------------
    os.total_orders as total_orders_for_the_month, 
    os.total_customers as total_customers_for_month,
    ---------- Total Net Sales (net amount paid by customer) for all the orders that supplier was a part of ------------
    ROUND((olg.amt_total_price_paid_net_eur),2) as Total_Net_Sales_EUR_order,
    ROUND((olg.amt_total_price_paid_net_lc),2) as Total_Net_Sales_LC_order,
  --------- Commercial Metrics------------------------
    COUNT(DISTINCT o.sku_id) as skus_sold,
    ------------ EUR ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_eur),0), 2) AS Net_Sales_EUR,
    ROUND(IFNULL(SUM(o.COGS_EUR),0)) AS COGS_EUR,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_eur > 0 THEN o.total_price_paid_net_eur END),0),2) AS Net_Sales_from_promo_eur,
    ROUND(IFNULL(SUM(o.total_supplier_funding_eur),0),2) AS total_supplier_funding_eur,
    ROUND((SUM(o.total_price_paid_net_eur) - SUM(o.COGS_EUR)), 2) AS front_margin_eur,
    ROUND(IFNULL(SUM(o.total_discount_eur),0)) AS total_discount_eur,
    ------------ LC ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_lc),0), 2) AS Net_Sales_LC,
    ROUND(IFNULL(SUM(o.COGS_LC),0)) AS COGS_LC,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_lc > 0 THEN o.total_price_paid_net_lc END),0),2) AS Net_Sales_from_promo_lc,
    ROUND(IFNULL(SUM(o.total_supplier_funding_lc),0),2) AS total_supplier_funding_lc,
    ROUND((SUM(o.total_price_paid_net_lc) - SUM(o.COGS_lc)), 2) AS front_margin_lc,
    ROUND(IFNULL(SUM(o.total_discount_lc),0)) AS total_discount_lc,
    ------- Order level data for the supplier---------------
    count(distinct o.order_id) as count_of_orders,
    count(distinct o.analytical_customer_id) as count_of_customers,
    ROUND(SUM (o.amt_gbv_eur),2) as total_GBV,
    ROUND(SUM (o.fulfilled_quantity),2) as fulfilled_quantity,
    COUNT(DISTINCT warehouse_id) as warehouses_sold,
    ------- Back-margin data for the supplier ---------------
    ROUND((total_rebate_lc),3) as total_rebate_lc,
    ROUND((total_rebate_wo_dist_allowance_lc),3) as total_rebate_wo_dist_allowance_lc,
    ROUND((Net_Purchases),3) as Net_Purchases

    FROM

    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    INNER JOIN

    orders_month as os

    ON 

    o.month = os.order_month
    AND o.global_entity_id = os.global_entity_id


    INNER JOIN

    brand_owner_order_level_gpv_monthly as olg

    ON 

    olg.global_entity_id = o.global_entity_id
    AND CAST(olg.time_period AS DATE) = o.month
    AND LOWER(TRIM(olg.brand_owner_name)) = LOWER(TRIM(o.brand_owner_name))


    LEFT JOIN

    brandowner_back_margins_monthly as bm

    ON

   bm.global_entity_id = o.global_entity_id
    AND CAST(bm.month AS DATE) = o.month
    AND LOWER(TRIM(bm.brand_owner_name)) = LOWER(TRIM(o.brand_owner_name))

    LEFT JOIN

    brand_owner_net_purchases_monthly as np

    ON

    np.country_code = o.country_code
    AND LOWER(TRIM(np.brand_owner_name)) = LOWER(TRIM(o.brand_owner_name))
    AND np.month = o.month
      
      
    WHERE
    o.order_date between '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL),



  brand_owner_final_quarterly AS (  

    SELECT
    'supplier' as level,
    'brand_owner' as division_type,
    o.global_entity_id,
    o.country_code,
    o.country_name,
    os.region,
    'Quarterly' as time_granularity,
    o.quarter_year as time_period,
    CAST(NULL AS STRING) as supplier_id,
    CAST(NULL AS STRING) AS supplier_name,
    o.brand_owner_name,
    CAST(NULL AS STRING) AS segment_name,
    CAST(NULL AS BOOL) AS name_missing,
    ---------- Monthly orders and customers ---------------
    os.total_orders as total_orders_for_the_month, 
    os.total_customers as total_customers_for_month,
    ---------- Total Net Sales (net amount paid by customer) for all the orders that supplier was a part of ------------
    ROUND(olg.amt_total_price_paid_net_eur,2) as Total_Net_Sales_EUR_order,
    ROUND(olg.amt_total_price_paid_net_lc,2) as Total_Net_Sales_LC_order,
  --------- Commercial Metrics------------------------
    COUNT(DISTINCT o.sku_id) as skus_sold,
    ------------ EUR ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_eur),0), 2) AS Net_Sales_EUR,
    ROUND(IFNULL(SUM(o.COGS_EUR),0)) AS COGS_EUR,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_eur > 0 THEN o.total_price_paid_net_eur END),0),2) AS Net_Sales_from_promo_eur,
    ROUND(IFNULL(SUM(o.total_supplier_funding_eur),0),2) AS total_supplier_funding_eur,
    ROUND((SUM(o.total_price_paid_net_eur) - SUM(o.COGS_EUR)), 2) AS front_margin_eur,
    ROUND(IFNULL(SUM(o.total_discount_eur),0)) AS total_discount_eur,
    ------------ LC ----------------------------------------
    ROUND(IFNULL(SUM(o.total_price_paid_net_lc),0), 2) AS Net_Sales_LC,
    ROUND(IFNULL(SUM(o.COGS_LC),0)) AS COGS_LC,
    ROUND(IFNULL(SUM(CASE WHEN o.unit_discount_amount_lc > 0 THEN o.total_price_paid_net_lc END),0),2) AS Net_Sales_from_promo_lc,
    ROUND(IFNULL(SUM(o.total_supplier_funding_lc),0),2) AS total_supplier_funding_lc,
    ROUND((SUM(o.total_price_paid_net_lc) - SUM(o.COGS_lc)), 2) AS front_margin_lc,
    ROUND(IFNULL(SUM(o.total_discount_lc),0)) AS total_discount_lc,
    ------- Order level data for the supplier---------------
    count(distinct o.order_id) as count_of_orders,
    count(distinct o.analytical_customer_id) as count_of_customers,
    ROUND(SUM (o.amt_gbv_eur),2) as total_GBV,
    ROUND(SUM (o.fulfilled_quantity),2) as fulfilled_quantity,
    COUNT(DISTINCT warehouse_id) as warehouses_sold,
    ------- Back-margin data for the supplier ---------------
    ROUND((total_rebate_lc),3) as total_rebate_lc,
    ROUND((total_rebate_wo_dist_allowance_lc),3) as total_rebate_wo_dist_allowance_lc,
    ROUND((Net_Purchases),3) as Net_Purchases

    FROM

    `fulfillment-dwh-production.cl_dmart._srm_orders_suppliers` as o

    INNER JOIN

    orders_quarterly as os

    ON 

    o.quarter_year = os.quarter_year
    AND o.global_entity_id = os.global_entity_id


    INNER JOIN

    brand_owner_order_level_gpv_quarterly as olg

    ON 

    o.quarter_year = olg.time_period
    AND 
    o.global_entity_id = olg.global_entity_id
    AND 
    LOWER(TRIM(o.brand_owner_name)) = LOWER(TRIM(olg.brand_owner_name))


    LEFT JOIN

    brandowner_back_margins_quarterly as bm

    ON

    bm.global_entity_id = o.global_entity_id
    AND 
    bm.quarter_year = o.quarter_year
    AND 
    LOWER(TRIM(bm.brand_owner_name)) = LOWER(TRIM(o.brand_owner_name))

    LEFT JOIN

    brand_owner_net_purchases_quarterly as np

    ON

    np.country_code = o.country_code
    AND LOWER(TRIM(np.brand_owner_name)) = LOWER(TRIM(o.brand_owner_name))
    AND np.quarter_year = o.quarter_year
      
      
    WHERE
    o.order_date between '2023-7-1' AND CURRENT_DATE()
    AND o.global_entity_id IN ('TB_KW', 'TB_AE', 'TB_QA', 'PY_AR', 'FP_SG', 'PY_PE')

    GROUP BY ALL)

    ---------------------------------------------------------------------- FINAL QUERY THAT UNIONS THE QUERIES ----------------------------------------------------------------------

    SELECT
    *,
    CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING)), '-', 
        LOWER(CAST(supplier_name AS STRING))

    ) AS unique_code

    FROM
    division_supplier_final_monthly

    GROUP BY ALL

    UNION ALL

    SELECT
    *,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING)), '-', 
        LOWER(CAST(supplier_name AS STRING))

    ) AS unique_code
    FROM
    division_supplier_final_quarterly

    UNION ALL

    SELECT
    *,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING)), '-', 
        LOWER(CAST(supplier_name AS STRING))

    ) AS unique_code
    FROM
    principal_supplier_final_monthly

    UNION ALL

    SELECT
    *,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(supplier_id AS STRING)), '-', 
        LOWER(CAST(supplier_name AS STRING))

    ) AS unique_code
    FROM
    principal_supplier_final_quarterly

        UNION ALL

    SELECT
    *,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(brand_owner_name AS STRING))

    ) AS unique_code
    FROM
    brand_owner_final_monthly

            UNION ALL

    SELECT
    *,
        CONCAT(
        LOWER('supplier'), '-', 
        LOWER(global_entity_id), '-', 
        LOWER(country_code), '-', 
        LOWER(country_name), '-', 
        LOWER(region), '-', 
        LOWER(CAST(time_granularity AS STRING)), '-', 
        LOWER(CAST(time_period AS STRING)), '-', 
        LOWER(CAST(brand_owner_name AS STRING))

    ) AS unique_code
    FROM
    brand_owner_final_quarterly

    GROUP BY ALL
