declare report_start_date date default '2022-10-01';
declare report_end_date date default '2022-10-01';

WITH 
email_holdout as (
    SELECT  distinct holdout.adobe_tracking_id
    FROM
    (
        SELECT  distinct TrackingId AS adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
        WHERE cohort = format_timestamp('%B%Y', DATETIME_TRUNC(report_start_date, QUARTER)) -- get cohort name as month of quarter start + year
        AND Hold_Out_Type_Current = 'Owned Email Holdout'
        AND DATE(TIMESTAMP(RegistrationDate), 'America/New_York') <= report_end_date
    ) holdout
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
, email_channel_qualifier as (
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
    WHERE registration_date between DATE_SUB(DATETIME_TRUNC(report_start_date, QUARTER), INTERVAL 4 MONTH) AND report_end_date
)
, email_unsubs as (
    SELECT DISTINCT adobe_tracking_id
    FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
    WHERE event_name = 'Email Unsubscribes' 
    AND event_date <= report_end_date
)

SELECT  distinct email_holdout.adobe_tracking_id AS aid
FROM email_holdout

-- Include only those who received email in the current reporting period or are in holdout
INNER JOIN email_channel_qualifier
ON email_holdout.adobe_tracking_id = email_channel_qualifier.aid

-- for after 2021/july, email channel only, take out all abandon MAAs
INNER JOIN
    (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
        WHERE report_date = report_end_date
        AND date_of_last_view IS NOT NULL
    ) abandon_maa
ON email_holdout.adobe_tracking_id = abandon_maa.adobe_tracking_id

-- exclude unsubscribed
LEFT JOIN email_unsubs
ON email_holdout.adobe_tracking_id = email_unsubs.adobe_tracking_id
WHERE email_unsubs.adobe_tracking_id IS NULL