CREATE OR REPLACE TABLE `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_supplier_score_overview_rough` AS

WITH final AS (
    SELECT * FROM `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_score_overview_bo_categoriesl1`
    UNION ALL
    SELECT * FROM `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_score_overview_bo_categoriesl3` 
    UNION ALL
    SELECT * FROM `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_score_overview_bo_categoriesl2` 
    UNION ALL
    SELECT * FROM `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_supplier_score_overview_bo_rough`
),
-- This CTE preps the data from the 'final' table to create clean join keys.
prepped_final AS (
    SELECT
        f.*,
        -- Create a single entity key for joining based on the supplier level
        CASE 
            WHEN f.supplier_level = 'supplier' THEN CAST(f.supplier_id AS STRING)
            WHEN f.supplier_level = 'level_one' THEN f.level_one
            WHEN f.supplier_level = 'level_two' THEN f.level_two
            WHEN f.supplier_level = 'level_three' THEN f.level_three
        END AS entity_join_key,
        -- Create a single brand/supplier key for joining on category levels
        CASE 
            WHEN f.division_type = 'brand_owner' THEN LOWER(TRIM(f.brand_owner_name))
            WHEN f.division_type IN ('division', 'principal') THEN CAST(f.supplier_id AS STRING)
        END AS brand_sup_join_key
    FROM final AS f
    WHERE TRUE
    AND REGEXP_CONTAINS(f.global_entity_id, {{ params.param_global_entity_id }})
)
-- Final Select
SELECT
    o.*, 
    s.Segmentation_LC, 
    s.Segmentation_EUR, 
    COALESCE(p.median_price_index, 0) AS median_price_index,
    COALESCE(dpo.payment_days, 0) AS payment_days,
    COALESCE(dpo.doh, 0) AS doh,
    COALESCE(dpo.dpo, 0) AS dpo,
FROM prepped_final AS o
-- Join to Supplier Segmentation
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_supplier_segmentation_rough` AS s
    ON o.global_entity_id = s.global_entity_id
    AND o.time_period = s.time_period
    AND o.division_type = s.division_type
    AND (
        -- If division_type is 'principal' or 'division', join on supplier_id and supplier_name
        (
            o.division_type IN ('principal', 'division') AND
            (
                s.supplier_id = o.supplier_id
                AND s.supplier_name = o.supplier_name
            )
        )
        -- Else, join on brand_owner_name
        OR
        (
            o.division_type = 'brand_owner' AND
            o.brand_owner_name = s.brand_owner_name
        )
    )
-- Left Join to Price Index using the new join keys
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_price_index_rough` AS p
    ON o.global_entity_id = p.global_entity_id
    AND o.time_period = p.time_period
    AND o.time_granularity = p.time_granularity
    AND o.division_type = p.division_type
    AND o.supplier_level = p.supplier_level
    AND o.entity_join_key = p.entity_key
    AND o.brand_sup_join_key = p.brand_sup
-- Left Join to Days Payable Outstanding using the new join keys
LEFT JOIN `{{ params.project_id }}.{{ params.dataset.rl }}._srm_supplier_scorecard_days_payable_outstanding_rough` AS dpo
    ON o.global_entity_id = dpo.global_entity_id
    AND o.time_period = dpo.time_period
    AND o.time_granularity = dpo.time_granularity
    AND o.division_type = dpo.division_type
    AND o.supplier_level = dpo.supplier_level
    AND o.entity_join_key = dpo.entity_key
    AND o.brand_sup_join_key = dpo.brand_sup
GROUP BY ALL
