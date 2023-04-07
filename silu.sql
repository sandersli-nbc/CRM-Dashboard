-- Run targetable for Q4: October
declare report_start_date date default '2022-10-01';
declare report_end_date date default '2022-10-01';
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Delivered_v4_Oct23` AS
   SELECT DISTINCT adobe_tracking_id
    FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE event_name = 'Email Deliveries'
    AND event_date BETWEEN report_start_date and report_end_date
    AND LOWER(campaign_name) NOT LIKE 'transactional%' -- Exclude transactional emails
    
-----------------------------------------------------------------------------------------
declare report_start_date date default '2022-10-01';
declare report_end_date date default '2022-10-01';
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Holdout_v4_Oct23` AS (
    SELECT  DISTINCT holdout.adobe_tracking_id
    FROM
    (
        SELECT  DISTINCT TrackingId AS adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
        WHERE cohort = format_timestamp('%B%Y', DATETIME_TRUNC(report_start_date, QUARTER)) -- get cohort name as month of quarter start + year
        AND Hold_Out_Type_Current = 'Owned Email Holdout'
        AND DATE(TIMESTAMP(RegistrationDate), 'America/New_York') <= report_end_date
    ) holdout
    -- Exclude those who are assigned to Email Holdout but actually received emails in holdout period
    LEFT JOIN (
        SELECT DISTINCT adobe_tracking_id
        FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
        WHERE event_name = 'Email Deliveries'
        AND event_date BETWEEN DATETIME_TRUNC(report_start_date, QUARTER) and report_end_date
        AND LOWER(campaign_name) NOT LIKE 'transactional%' -- Exclude transactional emails
    ) received
    ON holdout.adobe_tracking_id = received.adobe_tracking_id
    WHERE received.adobe_tracking_id IS NULL
)
-----------------------------------------------------------------------------------------
declare report_start_date date default '2022-10-01';
declare report_end_date date default '2022-10-01';
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Channel_Qualifier_v4_Oct23` AS
-- Engagement: Deliveries 4 months before start of the holdout period, defined as start of quarter
SELECT DISTINCT adobe_tracking_id AS aid
FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
WHERE event_name = 'Email Deliveries'
AND event_date BETWEEN DATE_SUB(DATETIME_TRUNC(report_start_date, QUARTER), INTERVAL 4 MONTH) AND report_end_date
AND lower(campaign_name) NOT LIKE 'transactional%' -- Exclude transactional emails
UNION ALL
-- New users joining after 4 months before start of the cohort period
SELECT DISTINCT adobe_tracking_id AS aid
FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
WHERE registration_date BETWEEN DATE_SUB(DATETIME_TRUNC(report_start_date, QUARTER), INTERVAL 4 MONTH) AND report_end_date
-----------------------------------------------------------------------------------------
declare report_start_date date default '2022-10-01';
declare report_end_date date default '2022-10-01';
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Measurement_Audience_v4_Oct23` AS
SELECT  distinct delivered_and_holdout.adobe_tracking_id AS aid
       ,cohort
       ,user.account_type
       ,user.account_tenure
FROM (
    SELECT *, 'Email_Targeted' as cohort from `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Delivered_v4_Oct23`
    UNION ALL
    SELECT *, 'Holdout' as cohort from `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Holdout_v4_Oct23`
) delivered_and_holdout
-- Include only those who received email in the current reporting period or are in holdout
INNER JOIN `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Channel_Qualifier_v4_Oct23`  qualified
ON delivered_and_holdout.adobe_tracking_id = qualified.aid
-- for after 2021/july, email channel only, take out all abandon MAAs
INNER JOIN
    (
        SELECT  DISTINCT adobe_tracking_id
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
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE report_date = report_end_date
    ) user
ON delivered_and_holdout.adobe_tracking_id = user.adobe_tracking_id
-- exclude unsubscribed
LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Email_Unsubs_v4_Jan23` email_unsubs
ON delivered_and_holdout.adobe_tracking_id = email_unsubs.adobe_tracking_id
WHERE email_unsubs.adobe_tracking_id IS NULL
-----------------------------------------------------------------------------------------
-- QA:
Holdout
377500
Email_Targeted
7016049
-----------------------------------------------------------------------------------------
-----------------------------------------------------------------------------------------
--------
--Churn Rate
declare report_start_date date default '2022-10-01';
declare report_end_date date default '2022-10-31';
WITH
EOM_Paid_Churn_Denom AS (
    SELECT  distinct adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN`
    WHERE base_date = report_end_date
    AND entitlement = 'Paid'
)
, EOM_Paid_Churn_Num AS (
    SELECT  distinct adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN`
    WHERE base_date = report_end_date
    AND entitlement = 'Paid'
    AND Churn_flag = 'Churn'
)
select cohort
      , count (distinct denom.adobe_tracking_id) as count_denom
      , count (distinct num.adobe_tracking_id) as count_num
FROM `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Measurement_Audience_v4_Oct23`  a
left join EOM_Paid_Churn_Denom denom
  on a.aid = denom. adobe_tracking_id
left join EOM_Paid_Churn_Num num
  on num.adobe_tracking_id = denom. adobe_tracking_id
group by 1