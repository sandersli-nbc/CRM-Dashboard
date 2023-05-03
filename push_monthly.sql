DECLARE report_start_date date DEFAULT @report_start_date;
DECLARE report_end_date date DEFAULT @report_end_date;

--===================================================================================================================================================
-- Establish Audience 
--===================================================================================================================================================

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Delivered_{report}` AS (
    
    -- everyone who have received push IN the month
    SELECT  b.aid                                                                                           AS adobe_tracking_id
            ,CASE WHEN android_count >= ios_count AND android_count >= other_count THEN 'Android'
                  WHEN ios_count >= android_count AND ios_count >= other_count THEN 'iOS'  ELSE 'Other' END AS push_received_primary_device
    FROM
    (
        SELECT  aid
            ,SUM(CASE WHEN platform = 'Android' THEN 1 ELSE 0 END)              AS android_count
            ,SUM(CASE WHEN platform = 'iOS' THEN 1 ELSE 0 END)                  AS ios_count
            ,SUM(CASE WHEN platform NOT IN ('Android','iOS') THEN 1 ELSE 0 END) AS other_count
        FROM
        (
            SELECT  COALESCE(canvasName,campaignName) AS canvas_campaign_name
                ,identity                          AS other_7
                ,platform
            FROM `nbcu-sdp-prod-003.sdp_persistent_views.BrazePushNotificationContactView`
            WHERE DATE(TIMESTAMP(eventTimestamp), 'America/New_York') BETWEEN report_start_date AND report_end_date
            GROUP BY  1,2,3
            HAVING (SUM(CASE WHEN eventName = 'Push Notification Sends' THEN 1 ELSE 0 END) >= 1) AND (SUM(CASE WHEN eventName = 'Push Notification Bounces' THEN 1 ELSE 0 END) = 0)
        ) a
        INNER JOIN `nbcu-sdp-sandbox-prod.sl_sandbox.Braze_Id_Adobe_Id_Map` map
        ON map.bid = a.other_7
        GROUP BY  1
    ) b
    -- exclude users put in holdout group
    LEFT JOIN
    (
        SELECT  DISTINCT TrackingId AS aid
        FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
        WHERE cohort = format_timestamp('%B%Y', DATETIME_TRUNC(report_start_date, QUARTER))
        AND Hold_Out_Type_Current = 'Owned Push Notification Holdout'
        AND DATE(TIMESTAMP(RegistrationDate), 'America/New_York') <= report_end_date 
    ) g
    ON g.aid = b.aid
    WHERE g.aid IS NULL

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Holdout_{report}` AS (

    SELECT  DISTINCT TrackingId AS adobe_tracking_id
           ,'Holdout'           AS push_received_primary_device
    FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
    WHERE cohort = format_timestamp('%B%Y', DATETIME_TRUNC(report_start_date, QUARTER)) -- get cohort name AS month of quarter start + year
    AND Hold_Out_Type_Current = 'Owned Push Notification Holdout'
    AND DATE(TIMESTAMP(RegistrationDate), 'America/New_York') <= report_end_date

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Channel_Qualifier_{report}` AS (

    -- webhook push opt-in canvas in Braze 
    SELECT DISTINCT adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE canvas_id = 'f4f21b32-e2ce-493f-a4dd-9132e45c65ff' --canvas_name = 'Push Optins' not displayed
    AND event_date BETWEEN report_end_date AND DATE_ADD(report_end_date, INTERVAL 2 DAY) --edited
    AND event_name = 'Webhook Sends'

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Measurement_Audience_{report}` AS (

    SELECT  DISTINCT delivered_and_holdout.adobe_tracking_id AS aid
        ,cohort
        ,delivered_and_holdout.push_received_primary_device
        ,user.account_type
        ,abandon_maa.primary_device
        ,user.account_tenure
        ,user.paid_tenure
        ,user.billing_platform
        ,user.bundling_partner
        ,user.billing_cycle_category
        ,user.offer
        ,user.churn_frequency
        ,ia.First_Viewed_Title AS intender_audience
        ,ia.genre
        ,ia.network
        ,CASE WHEN pb.adobe_tracking_id IS NOT NULL THEN 'Previously Bundled' ELSE 'Never Bundled' END AS previously_bundled
    FROM (
        SELECT  *
            ,'Targeted' AS cohort
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Delivered_{report}`
        UNION ALL
        SELECT  *
            ,'Holdout' AS cohort
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Holdout_{report}`
    ) delivered_and_holdout

    -- Include only those who received push in the current reporting period or are in holdout
    INNER JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Channel_Qualifier_{report}` qualified
    ON delivered_and_holdout.adobe_tracking_id = qualified.adobe_tracking_id

    -- take out all abandon MAAs
    INNER JOIN
        (
            SELECT  DISTINCT adobe_tracking_id
                ,CASE WHEN primary_device_name IN ('Android Mobile','Ios Mobile','Windows Phone') THEN 'Mobile'
                        WHEN primary_device_name IN ('Www','Amazon Fire Tablet') THEN 'Other'  ELSE 'Large Screen' END AS primary_device
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
            WHERE report_date = report_end_date
            AND date_of_last_view IS NOT NULL
        ) abandon_maa
    ON delivered_and_holdout.adobe_tracking_id = abandon_maa.adobe_tracking_id

    --add attribute: account_type at the end of the reporting period
    INNER JOIN
        (
            SELECT  DISTINCT adobe_tracking_id
                ,account_type
                ,account_tenure
                ,CASE WHEN paying_account_flag = 'Paying' THEN tenure_paid_lens ELSE NULL END AS paid_tenure
                ,billing_platform
                ,bundling_partner
                ,CASE WHEN billing_cycle = 'ANNUAL' THEN 'Annual'
                        WHEN billing_cycle = 'MONTHLY' THEN 'Monthly' END                    AS billing_cycle_category
                ,CASE WHEN voucher_partner IS NULL THEN 'Not On Offer'  ELSE 'On Offer' END  AS offer
                ,CASE WHEN previous_paid_churn_count = 0 THEN '0'
                        WHEN previous_paid_churn_count = 1 THEN '1'
                        WHEN previous_paid_churn_count = 2 THEN '2'  ELSE '3+' END           AS churn_frequency
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
            WHERE report_date = report_end_date 
        ) user
    ON delivered_and_holdout.adobe_tracking_id = user.adobe_tracking_id

    -- include intended audience
    LEFT JOIN  `nbcu-ds-int-nft-001.PeacockDataMartMarketingGold.NMA_INTENDER_AUDIENCE_ATTRIBUTES_FINAL` ia
    ON delivered_and_holdout.adobe_tracking_id = ia.adobe_tracking_id

    -- include previously_bundled
    LEFT JOIN (
        SELECT DISTINCT adobe_tracking_id
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.previously_bundled`
        WHERE report_date = report_end_date
    ) pb
    ON delivered_and_holdout.adobe_tracking_id = pb.adobe_tracking_id

);

--===================================================================================================================================================
-- Measure Metrics 
--===================================================================================================================================================

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Video_Viewing_{report}` AS (

    SELECT adobe_tracking_id
        ,COUNT (DISTINCT CASE WHEN VIDEO.num_views_started = 1 THEN video.adobe_tracking_id ELSE NULL END) AS Distinct_Content_Starts -- num_views_started is a flag
        ,SUM (VIDEO.num_views_started ) AS Total_Content_Starts
        ,SUM(VIDEO.num_seconds_played_no_ads)/3600 AS Viewing_Time
        ,COUNT(DISTINCT CASE WHEN VIDEO.num_views_started = 1 THEN session_id ELSE NULL END) AS Distinct_Viewing_Sessions 
        ,COUNT(DISTINCT(CASE 
                            WHEN (num_seconds_played_no_ads > CASE WHEN LOWER(consumption_type) = 'virtual channel' THEN 299 ELSE 0 END)
                            AND (num_views_started>0) 
                            THEN CASE 
                                        WHEN (LOWER(consumption_type) = "shortform") THEN "Shortform"
                                        WHEN LOWER(franchise) != 'other' THEN franchise 
                                        ELSE display_name
                                    END
                        END)
            ) AS Repertoire_Pavo_Method
    FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Measurement_Audience_{report}` a
    
    INNER JOIN  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` VIDEO
        ON VIDEO.adobe_tracking_id = a.aid
        AND adobe_date between report_start_date AND report_end_date
    GROUP BY 1

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Channel_Base_{report}` AS (

    WITH 
    Lapsed_Save_Base AS ( --'Lapsed_Users'
        SELECT  DISTINCT adobe_tracking_id
            ,date_of_last_view
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
        WHERE (report_date BETWEEN report_start_date AND report_end_date)
        AND (days_since_last_view BETWEEN 30 AND 90) -- this guarantees we are only getting people who have at least past the 'lapsing' phase IN the time period. 
    )
    , Lapsed_Save_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM Lapsed_Save_Base
    )
    , Lapsed_Save_Num AS (
        SELECT  DISTINCT a.adobe_tracking_id
        FROM Lapsed_Save_Base a
        INNER JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` VIDEO
            ON a.adobe_tracking_id = VIDEO.adobe_tracking_id 
            AND (VIDEO.adobe_date BETWEEN report_start_date AND report_end_date) 
            AND (VIDEO.adobe_date BETWEEN DATE_ADD(date_of_last_view, INTERVAL 30 day) AND DATE_ADD(date_of_last_view, INTERVAL 90 DAY)) 
            AND (VIDEO.num_views_started > 0)
    )
    , Lapsing_Save_Base AS ( --'Lapsing_Users'
        SELECT  DISTINCT adobe_tracking_id
            ,date_of_last_view
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
        WHERE (report_date BETWEEN report_start_date AND report_end_date)
        AND (days_since_last_view BETWEEN 15 AND 29) -- this guarantees we are only getting people who have at least past the 'lapsing' phase IN the time period. 
    )
    , Lapsing_Save_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM Lapsing_Save_Base
    )
    , Lapsing_Save_Num AS (
        SELECT  DISTINCT a.adobe_tracking_id
        FROM Lapsing_Save_Base a
        INNER JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` VIDEO
            ON a.adobe_tracking_id = VIDEO.adobe_tracking_id
            AND (adobe_date BETWEEN report_start_date AND report_end_date)
            AND (VIDEO.adobe_date BETWEEN DATE_ADD(date_of_last_view, INTERVAL 15 day) AND DATE_ADD(date_of_last_view, INTERVAL 29 day))
            AND (VIDEO.num_views_started > 0)
    )
    , Upgrade_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` USER
        WHERE (paying_account_flag = 'NonPaying')
        AND (USER.report_date BETWEEN report_start_date AND report_end_date )
    )
    , Upgrade_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM
        (
            SELECT  report_date
                ,adobe_tracking_id
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` USER
            WHERE (entitlement_change_flag IN ('Upgrade: Free to Premium' , 'Upgrade: Free to Premium+'))
            -- , 'Upgrade: Premium to Premium+'
            AND (paying_account_flag = 'Paying')
            AND (USER.report_date BETWEEN report_start_date AND report_end_date)
        )
    )
    , Cancel_Save_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'Paying')
        AND (auto_renew_flag = 'OFF')
        AND (report_date BETWEEN report_start_date AND report_end_date )
    )
    , Cancel_Save_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM
        (
            SELECT  adobe_tracking_id
                ,report_date
                ,auto_renew_flag                                                                      AS auto_renew_flag_today
                ,LEAD(auto_renew_flag,1) OVER ( PARTITION BY adobe_tracking_id ORDER BY report_date ) AS auto_renew_flag_next_day
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
            WHERE report_date BETWEEN report_start_date AND report_end_date
            ORDER BY 1, 2 
        )
        WHERE (auto_renew_flag_today = 'OFF')
        AND (auto_renew_flag_next_day = 'ON')
    )
    , Net_New_Upgrade_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
        WHERE (first_paying_date IS NULL)
        AND (report_date BETWEEN report_start_date AND report_end_date) 
    )
    , Net_New_Upgrade_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.upgrade_date_rank`
        WHERE (upgrade_row_number = 1)
        AND (report_date BETWEEN report_start_date AND report_end_date)
    )
    , Paid_Winbacks_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM
        (
            SELECT  adobe_tracking_id
                ,report_date
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
            WHERE paying_account_flag = 'NonPaying'
            AND report_date BETWEEN report_start_date AND report_end_date 
        )
        WHERE adobe_tracking_id NOT IN ( 
            SELECT DISTINCT adobe_tracking_id 
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
            WHERE (first_paying_date IS NULL)
            AND (report_date BETWEEN report_start_date AND report_end_date)
        )  
    )
    , Paid_Winbacks_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.upgrade_date_rank`
        WHERE (upgrade_row_number > 1)
        AND (report_date BETWEEN report_start_date AND report_end_date) 
    )
    , EOM_Paid_Churn_Denom AS  (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN`
        WHERE (base_date = report_end_date)
        AND (entitlement = 'Paid')
    )
    , EOM_Paid_Churn_Num AS  (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN`
        WHERE (base_date = report_end_date)
        AND (entitlement = 'Paid')
        AND (Churn_flag = 'Churn')
    )
    SELECT  report_start_date                                                               AS Report_Month
        ,a.aid
        ,a.cohort
        ,a.push_received_primary_device
        ,a.account_type
        ,a.primary_device
        ,a.account_tenure
        ,a.paid_tenure
        ,a.billing_platform
        ,a.bundling_partner
        ,a.billing_cycle_category
        ,a.offer
        ,a.churn_frequency
        ,a.previously_bundled
        ,a.intender_audience
        ,a.genre
        ,a.network

        ,CASE WHEN video.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END                 AS Viewer
        ,video.Viewing_Time
        ,video.Repertoire_Pavo_Method
        ,video.Distinct_Viewing_Sessions
        ,CASE WHEN Lapsed_Save_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END     AS Lapsed_Save_Denom
        ,CASE WHEN Lapsed_Save_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END       AS Lapsed_Save_Num
        ,CASE WHEN Lapsing_Save_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END    AS Lapsing_Save_Denom
        ,CASE WHEN Lapsing_Save_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END      AS Lapsing_Save_Num
        ,CASE WHEN Upgrade_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END         AS Upgrade_Denom
        ,CASE WHEN Upgrade_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END           AS Upgrade_Num
        ,CASE WHEN Net_New_Upgrade_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END AS Net_New_Upgrade_Denom
        ,CASE WHEN Net_New_Upgrade_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END   AS Net_New_Upgrade_Num
        ,CASE WHEN Paid_Winbacks_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END   AS Paid_Winbacks_Denom
        ,CASE WHEN Paid_Winbacks_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END     AS Paid_Winbacks_Num
        ,CASE WHEN Cancel_Save_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END     AS Cancel_Save_Denom
        ,CASE WHEN Cancel_Save_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END       AS Cancel_Save_Num
        ,CASE WHEN EOM_Paid_Churn_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END  AS EOM_Paid_Churn_Denom
        ,CASE WHEN EOM_Paid_Churn_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END    AS EOM_Paid_Churn_Num
    FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Measurement_Audience_{report}` a
    LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Push_Video_Viewing_{report}` video
    ON a.aid = video.adobe_tracking_id
    LEFT JOIN Lapsed_Save_Denom
    ON a.aid = Lapsed_Save_Denom.adobe_tracking_id
    LEFT JOIN Lapsed_Save_Num
    ON Lapsed_Save_Denom.adobe_tracking_id = Lapsed_Save_Num.adobe_tracking_id
    LEFT JOIN Lapsing_Save_Denom
    ON a.aid = Lapsing_Save_Denom.adobe_tracking_id
    LEFT JOIN Lapsing_Save_Num
    ON Lapsing_Save_Denom.adobe_tracking_id = Lapsing_Save_Num.adobe_tracking_id
    LEFT JOIN Upgrade_Denom
    ON a.aid = Upgrade_Denom.adobe_tracking_id
    LEFT JOIN Upgrade_Num
    ON Upgrade_Denom.adobe_tracking_id = Upgrade_Num.adobe_tracking_id
    LEFT JOIN Net_New_Upgrade_Denom
    ON a.aid = Net_New_Upgrade_Denom.adobe_tracking_id
    LEFT JOIN Net_New_Upgrade_Num
    ON Net_New_Upgrade_Denom.adobe_tracking_id = Net_New_Upgrade_Num.adobe_tracking_id
    LEFT JOIN Paid_Winbacks_Denom
    ON a.aid = Paid_Winbacks_Denom.adobe_tracking_id
    LEFT JOIN Paid_Winbacks_Num
    ON Paid_Winbacks_Num.adobe_tracking_id = Paid_Winbacks_Denom.adobe_tracking_id
    LEFT JOIN Cancel_Save_Denom
    ON a.aid = Cancel_Save_Denom.adobe_tracking_id
    LEFT JOIN Cancel_Save_Num
    ON Cancel_Save_Denom.adobe_tracking_id = Cancel_Save_Num.adobe_tracking_id
    LEFT JOIN EOM_Paid_Churn_Denom
    ON a.aid = EOM_Paid_Churn_Denom.adobe_tracking_id
    LEFT JOIN EOM_Paid_Churn_Num
    ON EOM_Paid_Churn_Denom.adobe_tracking_id = EOM_Paid_Churn_Num.adobe_tracking_id 

);