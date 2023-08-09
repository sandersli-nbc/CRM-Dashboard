DECLARE report_start_date DATE DEFAULT @report_start_date;
DECLARE report_end_date DATE DEFAULT @report_end_date;

--===================================================================================================================================================
-- Establish Audience 
--===================================================================================================================================================

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Delivered_{report}` AS (

    -- everyone who have received emails in the month
    SELECT  DISTINCT adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE event_name = 'Email Deliveries'
    AND event_date BETWEEN report_start_date AND report_end_date
    AND ((LOWER(campaign_name) NOT LIKE '%transactional%') AND (LOWER(campaign_name) NOT LIKE '%transcomm%')) -- Exclude transactional emails 

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Holdout_{report}` AS ( 
    
    SELECT  DISTINCT TrackingId AS adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
    WHERE cohort = format_timestamp('%B%Y', DATETIME_TRUNC(report_start_date, QUARTER)) -- get cohort name as month of quarter start + year
    AND Hold_Out_Type_Current = 'Owned Email Holdout'
    AND DATE(TIMESTAMP(RegistrationDate), 'America/New_York') <= report_end_date

);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Channel_Qualifier_{report}` AS (

    -- Engagement: Deliveries 4 months before start of the holdout period, defined as start of quarter
    -- Qualifier exists to enforce equivalent engaged audience among cohorts, using previous sends as a proxy
    -- Primarily for holdout channel, as targetable group would automatically qualify
    SELECT DISTINCT adobe_tracking_id 
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE event_name = 'Email Deliveries'
    AND event_date BETWEEN DATE_SUB(DATETIME_TRUNC(report_start_date, QUARTER), INTERVAL 4 MONTH) AND report_end_date
    AND ((LOWER(campaign_name) NOT LIKE '%transactional%') AND (LOWER(campaign_name) NOT LIKE '%transcomm%')) -- Exclude transactional emails

    UNION ALL

    -- New users joining after 4 months before start of the cohort period
    SELECT DISTINCT adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
    WHERE registration_date BETWEEN DATE_SUB(DATETIME_TRUNC(report_start_date, QUARTER), INTERVAL 4 MONTH) AND report_end_date
    
);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Measurement_Audience_{report}` AS (

    SELECT  DISTINCT delivered_and_holdout.adobe_tracking_id                                            AS aid
        ,cohort
        ,abandon_maa.primary_device
        ,user.account_tenure
        ,user.account_type
        ,user.active_viewer
        ,user.billing_cycle_category
        ,user.billing_platform
        ,user.bundling_partner
        ,user.churn_frequency
        ,user.offer
        ,user.paid_tenure
        ,user.paying_account_flag
        ,user_start.prev_30d_viewer
        ,user_start.prev_paying_account_flag
        ,ia.First_Viewed_Title                                                                          AS intender_audience
        ,ia.genre
        ,ia.network
        ,CASE WHEN pb.adobe_tracking_id IS NOT NULL THEN 'Previously Bundled'  ELSE 'Never Bundled' END AS previously_bundled
    FROM (

        SELECT  t.adobe_tracking_id
            ,'Targeted' AS cohort
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Delivered_{report}` t
        LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Holdout_{report}` h
        ON t.adobe_tracking_id = h.adobe_tracking_id
        WHERE h.adobe_tracking_id IS NULL

        UNION ALL

        SELECT  h.adobe_tracking_id
            ,'Holdout' AS cohort
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Holdout_{report}` h
        LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Delivered_{report}` t
        ON h.adobe_tracking_id = t.adobe_tracking_id
        WHERE t.adobe_tracking_id IS NULL

    ) delivered_and_holdout

    -- Include only those who received email in the current reporting period or are in holdout
    INNER JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Channel_Qualifier_{report}` qualified
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
                ,account_tenure
                ,account_type
                ,video_watched_trailing30                                                              AS active_viewer
                ,CASE WHEN billing_cycle = 'ANNUAL' THEN 'Annual'
                        WHEN billing_cycle = 'MONTHLY' THEN 'Monthly' END                              AS billing_cycle_category
                ,CASE WHEN paying_account_flag = 'Paying' THEN billing_platform  ELSE 'Non-Paying' END AS billing_platform
                ,bundling_partner
                ,CASE WHEN previous_paid_churn_count = 0 THEN '0'
                        WHEN previous_paid_churn_count = 1 THEN '1'
                        WHEN previous_paid_churn_count = 2 THEN '2'  ELSE '3+' END                     AS churn_frequency
                ,CASE WHEN voucher_partner IS NULL THEN 'Not On Offer'  ELSE 'On Offer' END            AS offer
                ,CASE WHEN paying_account_flag = 'Paying' THEN tenure_paid_lens  ELSE 'Non-Paying' END AS paid_tenure
                ,paying_account_flag
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
            WHERE report_date = report_end_date 
        ) user
    ON delivered_and_holdout.adobe_tracking_id = user.adobe_tracking_id

    --add attribute: video_watched_trailing30 before the start of the reporting period
    LEFT JOIN
    (
        SELECT  DISTINCT adobe_tracking_id
            ,video_watched_trailing30 AS prev_30d_viewer
            ,paying_account_flag      AS prev_paying_account_flag
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE report_date = DATE_SUB(report_start_date, INTERVAL 1 DAY) 
    ) user_start
    ON delivered_and_holdout.adobe_tracking_id = user_start.adobe_tracking_id

    -- include intended audience
    LEFT JOIN  `nbcu-ds-int-nft-001.PeacockDataMartMarketingGold.NMA_INTENDER_AUDIENCE_ATTRIBUTES_FINAL` ia
    ON delivered_and_holdout.adobe_tracking_id = ia.adobe_tracking_id

    -- include previously_bundled
    LEFT JOIN
        (
            SELECT  DISTINCT adobe_tracking_id
            FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.previously_bundled`
            WHERE report_date = report_end_date 
        ) pb
    ON delivered_and_holdout.adobe_tracking_id = pb.adobe_tracking_id

    LEFT JOIN
        (
            SELECT  DISTINCT adobe_tracking_id
            FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Unsubs`
            WHERE first_unsub_date <= report_end_date 
        ) unsubs
    ON delivered_and_holdout.adobe_tracking_id = unsubs.adobe_tracking_id

    WHERE unsubs.adobe_tracking_id IS NULL

);

--===================================================================================================================================================
-- Output Metrics 
--===================================================================================================================================================

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Channel_Base_{report}` AS (

    WITH 
    Lapsed_Save_Base AS (
        SELECT  DISTINCT adobe_tracking_id
            ,date_of_last_view
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
        WHERE (report_date BETWEEN report_start_date AND report_end_date)
        AND (days_since_last_view BETWEEN 30 AND 90)
    )
    , Lapsed_Save_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM Lapsed_Save_Base
    )
    , Lapsed_Save_Num AS (
        SELECT  DISTINCT a.adobe_tracking_id
        FROM Lapsed_Save_Base a
        INNER JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` video
        ON video.adobe_tracking_id = a.adobe_tracking_id
        AND (adobe_date BETWEEN DATE_ADD(date_of_last_view, INTERVAL 30 day) AND DATE_ADD(date_of_last_view, INTERVAL 90 day))
        AND (adobe_date BETWEEN report_start_date AND report_end_date)
        AND (num_views_started = 1)
        AND NOT(
            (COALESCE(stream_type,"NULL") = 'trailer') 
            AND (
                COALESCE(LOWER(vdo_initiate),"NULL") LIKE ('%auto%play%')
                --From 2023-03-01 onwards vdo_initiate=n/a will be considered as Auto play
                OR 
                (adobe_date >= '2023-03-01' AND COALESCE(LOWER(vdo_initiate),"NULL") = 'n/a')
            )
        )
    )
    , Lapsing_Save_Base AS (
        SELECT  DISTINCT adobe_tracking_id
            ,date_of_last_view
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
        WHERE (report_date BETWEEN report_start_date AND report_end_date)
        AND (days_since_last_view BETWEEN 15 AND 29)
    )
    , Lapsing_Save_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM Lapsing_Save_Base
    )
    , Lapsing_Save_Num AS (
        SELECT  DISTINCT a.adobe_tracking_id
        FROM Lapsing_Save_Base a
        INNER JOIN  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` video
        ON video.adobe_tracking_id = a.adobe_tracking_id
        AND (adobe_date BETWEEN DATE_ADD(date_of_last_view, INTERVAL 15 day) AND DATE_ADD(date_of_last_view, INTERVAL 29 day))
        AND (adobe_date BETWEEN report_start_date AND report_end_date)
        AND (num_views_started = 1)
        AND NOT(
            (COALESCE(stream_type,"NULL") = 'trailer') 
            AND (
                COALESCE(LOWER(vdo_initiate),"NULL") LIKE ('%auto%play%')
                --From 2023-03-01 onwards vdo_initiate=n/a will be considered as Auto play
                OR 
                (adobe_date >= '2023-03-01' AND COALESCE(LOWER(vdo_initiate),"NULL") = 'n/a')
            )
        )
    )
    , Free_To_Paid_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` USER
        WHERE (paying_account_flag = 'NonPaying')
        AND (USER.report_date BETWEEN report_start_date AND report_end_date )
    )
    , Free_To_Paid_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM
        (
            SELECT  report_date
                ,adobe_tracking_id
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` USER
            WHERE (entitlement_change_flag IN ('Upgrade: Free to Premium' , 'Upgrade: Free to Premium+'))
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
    SELECT  report_start_date                                                             AS Report_Month
        ,a.aid
        ,a.cohort

        -- Filters
        ,a.primary_device
        ,a.account_tenure
        ,a.account_type
        ,a.active_viewer
        ,a.billing_cycle_category
        ,a.billing_platform
        ,a.bundling_partner
        ,a.churn_frequency
        ,a.offer
        ,a.paid_tenure
        ,a.paying_account_flag
        ,a.prev_30d_viewer
        ,a.prev_paying_account_flag
        ,a.intender_audience
        ,a.genre
        ,a.network
        ,a.previously_bundled

        -- Metrics
        ,CASE WHEN video.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END                 AS Viewer
        ,video.Viewing_Time
        ,video.Repertoire_Pavo_Method
        ,video.Distinct_Viewing_Sessions
        ,CASE WHEN Lapsed_Save_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END     AS Lapsed_Save_Denom
        ,CASE WHEN Lapsed_Save_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END       AS Lapsed_Save_Num
        ,CASE WHEN Lapsing_Save_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END    AS Lapsing_Save_Denom
        ,CASE WHEN Lapsing_Save_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END      AS Lapsing_Save_Num
        ,CASE WHEN Free_To_Paid_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END    AS Free_To_Paid_Denom
        ,CASE WHEN Free_To_Paid_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END      AS Free_To_Paid_Num
        ,CASE WHEN Net_New_Upgrade_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END AS Net_New_Upgrade_Denom
        ,CASE WHEN Net_New_Upgrade_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END   AS Net_New_Upgrade_Num
        ,CASE WHEN Paid_Winbacks_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END   AS Paid_Winbacks_Denom
        ,CASE WHEN Paid_Winbacks_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END     AS Paid_Winbacks_Num
        ,CASE WHEN Cancel_Save_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END     AS Cancel_Save_Denom
        ,CASE WHEN Cancel_Save_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END       AS Cancel_Save_Num
        ,CASE WHEN EOM_Paid_Churn_Denom.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END  AS EOM_Paid_Churn_Denom
        ,CASE WHEN EOM_Paid_Churn_Num.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END    AS EOM_Paid_Churn_Num
    FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Measurement_Audience_{report}` a
    LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Video_Viewing_{report}` video
    ON a.aid = video.adobe_tracking_id
    LEFT JOIN Lapsed_Save_Denom
    ON a.aid = Lapsed_Save_Denom.adobe_tracking_id
    LEFT JOIN Lapsed_Save_Num
    ON Lapsed_Save_Denom.adobe_tracking_id = Lapsed_Save_Num.adobe_tracking_id
    LEFT JOIN Lapsing_Save_Denom
    ON a.aid = Lapsing_Save_Denom.adobe_tracking_id
    LEFT JOIN Lapsing_Save_Num
    ON Lapsing_Save_Denom.adobe_tracking_id = Lapsing_Save_Num.adobe_tracking_id
    LEFT JOIN Free_To_Paid_Denom
    ON a.aid = Free_To_Paid_Denom.adobe_tracking_id
    LEFT JOIN Free_To_Paid_Num
    ON Free_To_Paid_Denom.adobe_tracking_id = Free_To_Paid_Num.adobe_tracking_id
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