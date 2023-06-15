CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.upgrade_date_rank` AS (
    SELECT adobe_tracking_id
            , report_date
            , row_number() OVER(partition by adobe_tracking_id order by report_date ) as upgrade_row_number -- rank the number of times a user upgrade
    FROM       
            (
                SELECT adobe_tracking_id
                        , report_date
                        , paying_account_flag as paying_account_flag_today
                        , LAG(paying_account_flag,1) OVER ( partition by adobe_tracking_id order by report_date  ) as paying_account_flag_yestd -- paying flag yesterday
                FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
                ORDER BY 1,2
            )
    WHERE paying_account_flag_today = 'Paying' AND paying_account_flag_yestd = 'NonPaying'

);


CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.previously_bundled` AS (
    SELECT  adobe_tracking_id
        ,report_date
    FROM
    (
        SELECT  adobe_tracking_id
            ,report_date
            ,SUM(case WHEN bundling_partner != 'N/A' THEN 1 else 0 end) OVER(partition by adobe_tracking_id ORDER BY report_date ) AS cumulative_bundled_num -- rank the number of times a user upgrade
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
    )
    WHERE cumulative_bundled_num != 0 

);


CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Braze_Id_Adobe_Id_Map` AS (

       SELECT  adobe_tid AS aid
              ,braze_id  AS bid
       FROM
       (
              SELECT  distinct profileid
                     ,partnerorsystemid
                     ,externalprofilerid AS braze_id
              FROM `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
              WHERE Partnerorsystemid = 'braze' 
       ) AS braze_customer_mapping
       LEFT JOIN
       (
              SELECT  distinct profileid AS pid
                     ,externalprofilerid AS adobe_tid
              FROM `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping`
              WHERE Partnerorsystemid = 'trackingid' 
       ) AS adobe_id
       ON braze_customer_mapping.profileid = adobe_id.pid

);


CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Unsubs` AS (

       SELECT  adobe_tracking_id
              ,MIN(event_date) AS first_unsub_date
       FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
       WHERE event_name = 'Email Unsubscribes'
       GROUP BY  1

);