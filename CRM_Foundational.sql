CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.previously_bundled` AS (

    SELECT  adobe_tracking_id
        ,MIN(report_date) AS first_bundled_date
        ,MAX(report_date) AS last_bundled_date
        ,ARRAY_AGG(bundling_partner ORDER BY report_date DESC)[OFFSET(0)] as last_bundled_partner
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
    WHERE bundling_partner != 'N/A'
    GROUP BY 1

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Unsubs` AS (

    SELECT  adobe_tracking_id
            ,MIN(event_date) AS first_unsub_date
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE event_name = 'Email Unsubscribes'
    GROUP BY  1

);