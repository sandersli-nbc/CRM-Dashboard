DECLARE report_start_date DATE DEFAULT @report_start_date;
DECLARE report_end_date DATE DEFAULT @report_end_date;

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.CRM_Contribution_Platform_Audience_{report}` AS (
WITH users_end AS (
        SELECT  DISTINCT adobe_tracking_id
                ,account_tenure
                ,account_type
                ,video_watched_trailing30                                                              AS active_viewer
                ,CASE WHEN billing_cycle = 'ANNUAL' THEN 'Annual'
                      WHEN billing_cycle = 'MONTHLY' THEN 'Monthly' 
                 END                                                                                   AS billing_cycle_category
                ,CASE WHEN paying_account_flag = 'Paying' THEN billing_platform  ELSE 'Non-Paying' END AS billing_platform
                ,bundling_partner
                ,CASE WHEN previous_paid_churn_count = 0 THEN '0'
                      WHEN previous_paid_churn_count = 1 THEN '1'
                      WHEN previous_paid_churn_count = 2 THEN '2'  
                      ELSE '3+' 
                 END                                                                                   AS churn_frequency
                 ,entitlement
                ,CASE WHEN LOWER(Bundling_partner) LIKE '%wholesale%' THEN 'Wholesale'
                    WHEN Bundling_partner = "INSTACART-US" THEN 'Wholesale'
                    WHEN Bundling_partner = 'N/A' THEN 'Full Price'
                    ELSE 'Limited Time Offer' 
                 END                                                                                   AS offer_type
                ,CASE WHEN last_paid_date IS NULL THEN 'N/A'
                      WHEN DATE_DIFF(report_date, last_paid_date, DAY) <= 90 THEN '0-90 days'
                      ELSE '91+ days'
                 END                                                                                   AS last_paid_tenure
                ,paying_account_flag
                ,monthly_active_account
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE report_date = report_end_date
    ), users_start AS (
        SELECT DISTINCT adobe_tracking_id
            ,video_watched_trailing30 AS prev_30d_viewer
            ,paying_account_flag      AS prev_paying_account_flag
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE report_date = DATE_SUB(report_start_date, INTERVAL 1 DAY) 
    ), pd AS (
      SELECT  DISTINCT adobe_tracking_id
            ,CASE WHEN primary_device_name IN ('Android Mobile','Ios Mobile','Windows Phone') THEN 'Mobile'
                    WHEN primary_device_name IN ('Www','Amazon Fire Tablet') THEN 'Other'  ELSE 'Large Screen' END AS primary_device
      FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
      WHERE report_date = report_end_date
    ), pb AS(
            SELECT  DISTINCT adobe_tracking_id
            FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.previously_bundled`
            WHERE first_bundled_date <= report_end_date 
    ), ia AS (
        SELECT *
        FROM `nbcu-ds-int-nft-001.PeacockDataMartMarketingGold.NMA_INTENDER_AUDIENCE_ATTRIBUTES_FINAL`
    ), inflows AS (
        SELECT adobe_tracking_id, MAX(gross_add) as gross_add, MAX(win_back) as win_back
        FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.STAGING_PAID_COHORT_ACCOUNT_FLOW_LOGIC`
        WHERE report_date between report_start_date and report_end_date
        AND paying_account_flag = 'Paying'
        GROUP BY 1
    )
    SELECT DISTINCT DATE_TRUNC(report_start_date,MONTH) AS Report_Month
        ,users_end.adobe_tracking_id as aid
        -- Filters
        ,Account_Type
        ,Active_Viewer
        ,Primary_Device
        ,Account_Tenure
        ,Billing_Platform
        ,Bundling_Partner
        ,Billing_Cycle_Category
        ,Churn_Frequency
        ,Entitlement
        ,Last_Paid_Tenure
        ,Offer_Type
        ,Paying_Account_Flag
        ,CASE WHEN pb.adobe_tracking_id IS NOT NULL THEN 'Previously Bundled'  ELSE 'Never Bundled' END AS previously_bundled
        ,Prev_30d_Viewer
        ,Prev_Paying_Account_Flag
        ,ia.First_Viewed_Title                                                                          AS intender_audience
        ,Genre
        ,Network
        ,Monthly_Active_Account
        -- Inflows
        ,Gross_add
        ,Win_back
        ,CASE WHEN Gross_add = 1 OR Win_back = 1 THEN 1 ELSE 0 END                                      AS Inflow
    FROM users_end
    LEFT JOIN inflows
    ON users_end.adobe_tracking_id = inflows.adobe_tracking_id
    LEFT JOIN pd
    ON users_end.adobe_tracking_id = pd.adobe_tracking_id
    LEFT JOIN users_start
    ON users_end.adobe_tracking_id = users_start.adobe_tracking_id
    LEFT JOIN pb
    ON users_end.adobe_tracking_id = pb.adobe_tracking_id
    LEFT JOIN ia
    ON users_end.adobe_tracking_id = ia.adobe_tracking_id
);

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.CRM_Platform_Metrics_{report}` AS (
    WITH video_viewing AS
    (
        SELECT  DISTINCT adobe_tracking_id
            ,Viewing_Time
            ,Repertoire_Pavo_Method
            ,Distinct_Viewing_Sessions
            ,Active_Days
        FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Video_Viewing_{report}`
    )
    , Lapsed_Save_Base AS (
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
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'NonPaying')
        AND (report_date BETWEEN report_start_date AND report_end_date )
    )
    , Free_To_Paid_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'Paying')
        AND (paying_account_change_flag = 'NonPaying to Paying') 
        AND (report_date BETWEEN report_start_date AND report_end_date)
    )
    , Net_New_Upgrade_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
        WHERE (paying_account_flag = 'NonPaying')
        AND (first_paying_date IS NULL)
        AND (report_date BETWEEN report_start_date AND report_end_date) 
    )
    , Net_New_Upgrade_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'Paying')
        AND (paying_account_change_flag = 'NonPaying to Paying')
        AND (first_paying_date = last_paid_date)
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
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'Paying')
        AND (paying_account_change_flag = 'NonPaying to Paying')
        AND (first_paying_date != last_paid_date)
        AND (report_date BETWEEN report_start_date AND report_end_date) 
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
        ,CASE WHEN email.aid IS NOT NULL THEN 1 ELSE 0 END                                AS email_audience_flag
        ,email.cohort                                                                     AS email_cohort

        -- Filters
        ,a.primary_device
        ,a.account_tenure
        ,a.account_type
        ,a.active_viewer
        ,a.billing_cycle_category
        ,a.billing_platform
        ,a.bundling_partner
        ,a.churn_frequency
        ,a.entitlement
        ,a.offer_type
        ,a.last_paid_tenure
        ,a.paying_account_flag
        ,a.prev_30d_viewer
        ,a.prev_paying_account_flag
        ,a.intender_audience
        ,a.genre
        ,a.network
        ,a.previously_bundled
        ,a.gross_add
        ,a.win_back
        ,a.inflow
        ,a.monthly_active_account

        -- Metrics
        ,CASE WHEN video.adobe_tracking_id IS NOT NULL THEN 1  ELSE 0 END                 AS Viewer
        ,video.Viewing_Time
        ,video.Repertoire_Pavo_Method
        ,video.Distinct_Viewing_Sessions
        ,video.Active_Days
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
    FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.CRM_Contribution_Platform_Audience_{report}` a
    LEFT JOIN video_viewing video
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
    -- Join with Email Audience
    LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Channel_Base_{report}` email
    ON a.aid = email.aid
);