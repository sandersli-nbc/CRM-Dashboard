%%bigquery --project nbcu-ds-sandbox-a-001 --params $params

create or replace table `nbcu-ds-sandbox-a-001.SLi_sandbox.Wes_Holdout_Test` AS

WITH 
email_holdout as (
    SELECT  distinct holdout.adobe_tracking_id
    FROM
    (
        SELECT  distinct TrackingId AS adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
        WHERE cohort = format_timestamp('%B%Y', DATETIME_TRUNC(@report_start_date, QUARTER)) -- get cohort name as month of quarter start + year
        AND Hold_Out_Type_Current = 'Owned Email Holdout'
        AND DATE(TIMESTAMP(RegistrationDate), 'America/New_York') <= @report_end_date
    ) holdout
    LEFT JOIN (
        SELECT DISTINCT adobe_tracking_id 
        FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
        WHERE event_name = 'Email Deliveries' 
        AND event_date BETWEEN DATETIME_TRUNC(@report_start_date, QUARTER) and @report_end_date -- CHANGED
        AND LOWER(campaign_name) NOT LIKE '%transactional%' -- Exclude transactional emails
    ) received
    ON holdout.adobe_tracking_id = received.adobe_tracking_id
    WHERE received.adobe_tracking_id IS NULL
)
, email_channel_qualifier as (
    -- Engagement: Deliveries 4 months before start of the holdout period, defined as start of quarter
    SELECT DISTINCT adobe_tracking_id AS aid 
    FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE event_name = 'Email Deliveries'
    AND event_date BETWEEN DATE_SUB(DATETIME_TRUNC(@report_start_date, QUARTER), INTERVAL 4 MONTH) AND @report_end_date
    AND lower(campaign_name) NOT LIKE 'transactional%' -- Exclude transactional emails

    UNION ALL

    -- New users joining after 4 months before start of the cohort period
    SELECT DISTINCT adobe_tracking_id AS aid
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
    WHERE registration_date between DATE_SUB(@report_start_date, INTERVAL 4 MONTH) and @report_end_date -- CHANGED
)
, email_unsubs as (
    SELECT DISTINCT adobe_tracking_id
    FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
    WHERE event_name = 'Email Unsubscribes' 
    AND event_date <= @report_end_date
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
        WHERE report_date = @report_end_date
        AND date_of_last_view IS NOT NULL
    ) abandon_maa
ON email_holdout.adobe_tracking_id = abandon_maa.adobe_tracking_id

-- exclude unsubscribed
LEFT JOIN email_unsubs
ON email_holdout.adobe_tracking_id = email_unsubs.adobe_tracking_id
WHERE email_unsubs.adobe_tracking_id IS NULL






%%bigquery --project nbcu-ds-sandbox-a-001 --params $params

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.churn_oct22_wes_edit_v2` AS

WITH EmailDeliveryStatus AS 
(
    SELECT  distinct @report_start_date Month_Year
        ,adobe_tracking_id aid
        ,event_name
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE date(eventTimestamp) BETWEEN @report_start_date AND @report_end_date
    AND event_name in('Email Deliveries')
    AND lower(campaign_name) not like '%transactional%' -- remove transactional emails 
    UNION ALL
    SELECT  distinct @report_start_date Month_Year
        ,adobe_tracking_id aid
        ,event_name -- identify if the last action a sub took was to unsubscribe
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
    WHERE date(eventTimestamp) < @report_end_date
    AND event_name in('Email Unsubscribes') 
)
SELECT  @report_start_date Month_Year
       ,l.aid
       ,l.billing_platform
       ,l.billing_cycle
       ,tenure_paid_lens
       ,CASE WHEN abandoned_ids is not null THEN "x"  ELSE null END abandoned_flag
       ,marketing_status.category
       ,CASE WHEN l.billing_platform = 'NBCU' THEN 'Direct'  ELSE 'IAP' END grouped_billing_platform
       ,CASE WHEN l.paying_account_flag != r.paying_account_flag THEN 'Downgrade'  ELSE 'No Change' END change_flag
       ,CASE WHEN l.voucher_partner is null THEN "Not On Voucher"  ELSE "On Voucher" END voucher_flag
       ,CASE WHEN ho.aid is not null THEN "Holdout"
             WHEN ho1.hid is not null THEN "Exclude"
             WHEN l.aid is not null THEN 'Targetable' END Audience
FROM
(
	SELECT  adobe_tracking_id aid
	       ,household_id hid
	       ,billing_platform
	       ,paying_account_flag
	       ,billing_cycle
	       ,tenure_paid_lens
	       ,voucher_partner
	FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
	WHERE report_date = @report_end_date
	AND paying_account_flag = 'Paying' 
) l

-- retrieve abandoned maas
LEFT JOIN
(
	SELECT  adobe_tracking_id abandoned_ids
	FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`
	WHERE report_date = @report_end_date
	AND date_of_last_view is null 
) a
ON l.aid = a.abandoned_ids

-- retrieve thier marketing status: where they are unsubscribed or received an email in the last 4 months
LEFT JOIN
(
	SELECT  aid
	       ,event_name category -- identify if the last action a sub took was to unsubscribe
	FROM EmailDeliveryStatus
	WHERE Month_Year = @report_start_date 
) marketing_status
ON l.aid = marketing_status.aid

-- retrieve status 1 month later
LEFT JOIN
(
	SELECT  adobe_tracking_id
	       ,billing_platform
	       ,paying_account_flag
	FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
	WHERE report_date = '2022-11-30' 
) r
ON l.aid = r.adobe_tracking_id

-- retrieve members of holdout group
LEFT JOIN `nbcu-ds-sandbox-a-001.SLi_sandbox.Wes_Holdout_Test` ho
ON l.aid = ho.aid

--retrieve the rest of the global hold out that doesnt fit holdout definition. exclude these users from the analysis
LEFT JOIN
(
	SELECT  HouseholdId hid
	FROM `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP`
	WHERE cohort = 'October2022'
	AND Hold_Out_Type_Current = 'Owned Email Holdout' -- Exclude those who are assigned to Email Holdout but actually received emails
	AND TrackingId not in(
	SELECT  aid
	FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Wes_Holdout_Test` )
) ho1
ON l.hid = ho1.hid