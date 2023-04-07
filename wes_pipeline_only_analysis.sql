%%bigquery --project nbcu-ds-sandbox-a-001 --params $params

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.churn_oct22_wes` AS

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
LEFT JOIN
(
	SELECT  aid
	FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Q42022_Holdout_Wes`
) ho
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
	FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.Q42022_Holdout_Wes` )
) ho1
ON l.hid = ho1.hid