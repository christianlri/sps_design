CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_days_payable_outstanding_rough` AS
WITH
  pim AS (
    SELECT
      pim.product_id,
      pim.brand_owner_name,
    FROM
      `{{ params.project_id }}.{{ params.dataset.cl }}.pim_product` AS pim
    WHERE
      TRUE
    GROUP BY
      ALL
  ),
  qc AS (
    SELECT
      qc.global_entity_id,
      qc.sku AS sku_id,
      qc.pim_product_id,
    FROM
      `{{ params.project_id }}.{{ params.dataset.cl }}.qc_catalog_products` AS qc
    WHERE
      TRUE
      AND REGEXP_CONTAINS(qc.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY
      ALL
  ),
  products AS (
    SELECT
      qc.global_entity_id,
      qc.sku_id,
      COALESCE(pim.brand_owner_name, 'Unbranded') AS brand_owner_name,
    FROM
      qc
      LEFT JOIN pim
      ON qc.pim_product_id = pim.product_id
  ),
  supplier_mapping AS (
    SELECT
      sm.global_entity_id,
      CAST(sm.supplier_id AS STRING) AS supplier_id,
      CAST(sm.principal_supplier_id AS STRING) AS principal_supplier_id,
      sm.division_type
    FROM
      `{{ params.project_id }}.{{ params.dataset.cl }}._srm_supplier_supplier_hierarchy` AS sm
    WHERE
      TRUE
      AND REGEXP_CONTAINS(sm.global_entity_id, {{ params.param_global_entity_id }})
    GROUP BY
      ALL
  ),
  account AS (
    SELECT
      a.global_entity_id,
      a.srm_supplierportalid__c AS supplier_id,
      a.srm_suppliertype__c AS supplier_type_id,
      a.srm_supplierstatus__c AS supplier_status,
      a.bank_payment_term__c AS bank_payment_term,
      CASE
          -- Fallback for specific, known terms that don't fit a pattern
          WHEN bank_payment_term__c = 'Pay immediately (Z000)' THEN 0
          WHEN bank_payment_term__c IN ('Pay immediately w/o deduction (0001)', '0001') THEN 1
          WHEN bank_payment_term__c = 'Vendor payment split 50-50 (DHK) (A020)' THEN 20
          WHEN bank_payment_term__c IN ('0002', '0018', '0019') THEN 7
          WHEN bank_payment_term__c IN ('0003', '0020', '0021', '0022') THEN 10
          WHEN bank_payment_term__c IN ('0004', 'Z114') THEN 14
          WHEN bank_payment_term__c IN ('0005', '0023', '0025', '0026', '0028', '0030', 'Z130') THEN 30
          WHEN bank_payment_term__c IN ('0006', '0033') THEN 45
          WHEN bank_payment_term__c IN ('0007', '0024', '0027', '0029', '0031', '0035') THEN 60
          WHEN bank_payment_term__c = '0008' THEN 90
          WHEN bank_payment_term__c = '0009' THEN 120
          WHEN bank_payment_term__c = '0010' THEN 150
          WHEN bank_payment_term__c = '0011' THEN 180
          WHEN bank_payment_term__c = '0012' THEN 210
          WHEN bank_payment_term__c = '0013' THEN 240
          WHEN bank_payment_term__c = '0014' THEN 270
          WHEN bank_payment_term__c = '0015' THEN 300
          WHEN bank_payment_term__c = '0016' THEN 330
          WHEN bank_payment_term__c = '0017' THEN 365
          WHEN bank_payment_term__c = '0032' THEN 21
          WHEN bank_payment_term__c = '0034' THEN 28
          WHEN bank_payment_term__c = 'Z115' THEN 15
          -- General rule for terms containing 'days' (e.g., '10 days net')
          WHEN bank_payment_term__c LIKE '%days%' THEN
            CAST(REGEXP_EXTRACT(bank_payment_term__c, r'(\d+)') AS INT64)
          WHEN bank_payment_term__c LIKE '%End Of Month%' THEN
            CAST(REGEXP_EXTRACT(bank_payment_term__c, r'(\d+) Days') AS INT64)
          WHEN bank_payment_term__c LIKE '%from Invoice Date%' THEN
            CAST(REGEXP_EXTRACT(bank_payment_term__c, r'(\d+) Days') AS INT64)
          -- General rule for Z-prefixed codes (e.g., 'Z001', 'Z015')
          WHEN bank_payment_term__c LIKE 'Z0__' THEN
            CAST(REGEXP_EXTRACT(bank_payment_term__c, r'Z0(\d\d)') AS INT64)
          -- Matches 'Days_##_End_Of_Month' format (e.g., Days_90_End_Of_Month)
          WHEN bank_payment_term__c LIKE 'Days_%_End_Of_Month' THEN
            CAST(REGEXP_EXTRACT(bank_payment_term__c, r'Days_(\d+)_End_Of_Month') AS INT64) 
          -- General rule for Z/D-prefixed codes followed by digits (e.g., Z130, ZD30, Z114)
          WHEN REGEXP_CONTAINS(bank_payment_term__c, r'^[A-Z]{1,2}\d+$') THEN
            CAST(REGEXP_EXTRACT(bank_payment_term__c, r'[A-Z]{1,2}(\d+)') AS INT64)
          ELSE
            NULL
      END AS payment_days,
    FROM
      `{{ params.project_id }}.{{ params.dataset.curated_data_shared_salesforce_srm }}.account` AS a
    WHERE
      TRUE
      AND REGEXP_CONTAINS(a.country_code, {{ params.param_country_code }})
      AND a.bank_payment_term__c IS NOT NULL
    GROUP BY
      ALL
  ),
  sku_stock_days_on_hand AS (
    SELECT
      sd.month,
      CAST(DATE_TRUNC(sd.month, MONTH) AS STRING) AS stock_days_month,
      CAST(CONCAT('Q', EXTRACT(QUARTER FROM sd.month), '-', EXTRACT(YEAR FROM sd.month)) AS STRING) AS stock_days_quarter,
      sd.global_entity_id,
      sd.sku AS sku_id,
      CAST(sd.supplier_id AS STRING) AS supplier_id,
      ANY_VALUE(sd.supplier_name) AS supplier_name,
      COALESCE(sd.level_one_category, 'Unknown') AS l1_master_category,
      COALESCE(sd.level_two_category, 'Unknown') AS l2_master_category,
      COALESCE(sd.level_three_category, 'Unknown') AS l3_master_category,
      DATE_DIFF(DATE_ADD(DATE(month), INTERVAL 1 MONTH), DATE(month), DAY) AS days_in_month,
      DATE_DIFF(
        DATE_ADD(DATE_TRUNC(DATE(month), QUARTER), INTERVAL 1 QUARTER),
        DATE_TRUNC(DATE(month), QUARTER),
        DAY
      ) AS days_in_quarter,
      SUM(sd.month_end_stock_value_eur) sku_month_end_stock_value_eur,
      SUM(sd.cogs_eur_monthy) AS sku_cogs_eur_monthy,
    FROM
      `{{ params.project_id }}.{{ params.dataset.cl }}.stock_days_on_hand` AS sd
    WHERE
      TRUE
      AND sd.month BETWEEN DATE_TRUNC(DATE_SUB(CURRENT_DATE(), INTERVAL 12 MONTH), MONTH) AND DATE_TRUNC(CURRENT_DATE(), MONTH)
      AND REGEXP_CONTAINS(sd.country_code, {{ params.param_country_code }})
    GROUP BY
      ALL
  ),
  preaggregated AS (
    SELECT
      sd.stock_days_month,
      sd.stock_days_quarter,
      sd.global_entity_id,
      sd.supplier_id,
      sd.supplier_name,
      p.brand_owner_name,
      sd.l1_master_category,
      sd.l2_master_category,
      sd.l3_master_category,
      sm.principal_supplier_id,
      sm.division_type,
      a.supplier_type_id,
      a.supplier_status,
      a.payment_days,
      sd.days_in_month,
      sd.days_in_quarter,
      sd.sku_month_end_stock_value_eur,
      sd.sku_cogs_eur_monthy,
    FROM
      sku_stock_days_on_hand AS sd
      LEFT JOIN account AS a
      ON sd.global_entity_id = a.global_entity_id
      AND sd.supplier_id = a.supplier_id
      LEFT JOIN supplier_mapping AS sm
      ON sd.global_entity_id = sm.global_entity_id
      AND sd.supplier_id = sm.supplier_id
      LEFT JOIN products AS p
      ON sd.sku_id = p.sku_id
      AND sd.global_entity_id = p.global_entity_id
    WHERE
      TRUE
  )
SELECT
  global_entity_id,
  CASE
    WHEN GROUPING(stock_days_month) = 0 THEN stock_days_month
    ELSE stock_days_quarter
  END AS time_period,
  COALESCE(principal_supplier_id, supplier_id, brand_owner_name) AS brand_sup,
  COALESCE(l3_master_category, l2_master_category, l1_master_category, principal_supplier_id, supplier_id, brand_owner_name) AS entity_key,
  CASE
    WHEN supplier_id IS NOT NULL AND l1_master_category IS NOT NULL THEN 'division'
    WHEN brand_owner_name IS NOT NULL AND l1_master_category IS NOT NULL THEN 'brand_owner'
    WHEN principal_supplier_id IS NOT NULL AND l1_master_category IS NOT NULL THEN 'principal'
    WHEN supplier_id IS NOT NULL THEN 'division'
    WHEN principal_supplier_id IS NOT NULL THEN 'principal'
    WHEN brand_owner_name IS NOT NULL THEN 'brand_owner'
    ELSE NULL
  END AS division_type,
  CASE
    WHEN l3_master_category IS NOT NULL THEN 'level_three'
    WHEN l2_master_category IS NOT NULL THEN 'level_two'
    WHEN l1_master_category IS NOT NULL THEN 'level_one'
    WHEN supplier_id IS NOT NULL OR principal_supplier_id IS NOT NULL OR brand_owner_name IS NOT NULL THEN 'supplier'
    ELSE NULL
  END AS supplier_level,
  CASE
    WHEN GROUPING(stock_days_month) = 0 THEN 'Monthly'
    ELSE 'Quarterly'
  END AS time_granularity,
  MAX(payment_days) AS payment_days,
  CASE
    WHEN GROUPING(stock_days_month) = 0 THEN SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_month)))
    ELSE SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_quarter) / 3))
  END AS doh,
  CASE
    WHEN GROUPING(stock_days_month) = 0 THEN SAFE_SUBTRACT(MAX(payment_days), SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_month))))
    ELSE SAFE_SUBTRACT(MAX(payment_days), SAFE_DIVIDE(SUM(sku_month_end_stock_value_eur), SAFE_DIVIDE(SUM(sku_cogs_eur_monthy), MAX(days_in_quarter) / 3)))
  END AS dpo,
FROM
  preaggregated
GROUP BY
  GROUPING SETS (
    -- Grouping by Category L1 (Monthly, Quarterly)
    (stock_days_month, global_entity_id, supplier_id, l1_master_category),
    (stock_days_quarter, global_entity_id, supplier_id, l1_master_category),
    (stock_days_month, global_entity_id, brand_owner_name, l1_master_category),
    (stock_days_quarter, global_entity_id, brand_owner_name, l1_master_category),
    (stock_days_month, global_entity_id, principal_supplier_id, l1_master_category),
    (stock_days_quarter, global_entity_id, principal_supplier_id, l1_master_category),
    -- Grouping by Category L2 (Monthly, Quarterly)
    (stock_days_month, global_entity_id, supplier_id, l2_master_category),
    (stock_days_quarter, global_entity_id, supplier_id, l2_master_category),
    (stock_days_month, global_entity_id, brand_owner_name, l2_master_category),
    (stock_days_quarter, global_entity_id, brand_owner_name, l2_master_category),
    (stock_days_month, global_entity_id, principal_supplier_id, l2_master_category),
    (stock_days_quarter, global_entity_id, principal_supplier_id, l2_master_category),
    -- Grouping by Category L3 (Monthly, Quarterly)
    (stock_days_month, global_entity_id, supplier_id, l3_master_category),
    (stock_days_quarter, global_entity_id, supplier_id, l3_master_category),
    (stock_days_month, global_entity_id, brand_owner_name, l3_master_category),
    (stock_days_quarter, global_entity_id, brand_owner_name, l3_master_category),
    (stock_days_month, global_entity_id, principal_supplier_id, l3_master_category),
    (stock_days_quarter, global_entity_id, principal_supplier_id, l3_master_category),
    -- Grouping by Supplier/Brand/Principal (Monthly, Quarterly)
    (stock_days_month, global_entity_id, supplier_id),
    (stock_days_quarter, global_entity_id, supplier_id),
    (stock_days_month, global_entity_id, brand_owner_name),
    (stock_days_quarter, global_entity_id, brand_owner_name),
    (stock_days_month, global_entity_id, principal_supplier_id),
    (stock_days_quarter, global_entity_id, principal_supplier_id)
  )
