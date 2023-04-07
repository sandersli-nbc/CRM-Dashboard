-------------------------------------------------------------------------------------------
-- create table to identify users "Marketing Status"
--    Whether or no a user received and email in the past month (Oct 1-31, Nov 1-30, Dec 1-31)
--    Whether or not the user has unsubscribed
-------------------------------------------------------------------------------------------
create or replace table `nbcu-ds-sandbox-a-001.wes_crm_sandbox.BillingPlatformAnalysis_EmailDeliveryStatus` AS

-------------------------------------------------------------------------------------------
-- add October data
-------------------------------------------------------------------------------------------

select 
  distinct '2022-10-01' Month_Year, 
  adobe_tracking_id aid, 
  event_name 
from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
where 
  date(eventTimestamp) between '2022-10-01' and '2022-10-31'
  and event_name in('Email Deliveries') 
  and lower(campaign_name) not like '%transactional%' -- remove transactional emails

union all

select 
  distinct '2022-10-01' Month_Year, 
  adobe_tracking_id aid, 
  event_name -- identify if the last action a sub took was to unsubscribe
from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE`
where 
  date(eventTimestamp) < '2022-10-31'
  and event_name in('Email Unsubscribes')
[6:59 AM]  -------------------------------------------------------------------------------------------
-- create table which contains users 
--    Billing Platform
--    If they downgraded in the specified month and other urser data information
--    Whether they are in the holdout or targetable group
-------------------------------------------------------------------------------------------
create or replace table `nbcu-ds-sandbox-a-001.wes_crm_sandbox.BillingPlatformAnalysis_UserData_Feb2023` as

-------------------------------------------------------------------------------------------
-- add October data
-------------------------------------------------------------------------------------------
select 
  '2022-10-01' Month_Year,
  l.aid,
  l.billing_platform,
  l.billing_cycle,
  tenure_paid_lens,
  case when abandoned_ids is not null then "x" else null end abandoned_flag,
  marketing_status.category,
  case when l.billing_platform = 'NBCU' then 'Direct' else 'IAP' end grouped_billing_platform,
  case when l.paying_account_flag != r.paying_account_flag then 'Downgrade' else 'No Change' end change_flag,
  case when l.voucher_partner is null then "Not On Voucher" else "On Voucher" end voucher_flag,
  case when ho.aid is not null then "Holdout" when ho1.hid is not null then "Exclude" when l.aid is not null then 'Targetable' end Audience
from(
  select 
    adobe_tracking_id aid,
    household_id hid,
    billing_platform,
    paying_account_flag,
    billing_cycle,
    tenure_paid_lens,
    voucher_partner
  from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
  where 
    report_date = '2022-10-31'
    and paying_account_flag = 'Paying'
) l

-- retrieve abandoned maas
left join(
  select adobe_tracking_id abandoned_ids
  from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES` 
  where 
    report_date = '2022-10-31'
    and date_of_last_view is null
) a
on l.aid = a.abandoned_ids

-- retrieve thier marketing status: where they are unsubscribed or received an email in the last 4 months
left join(
  select 
    aid, 
    event_name category -- identify if the last action a sub took was to unsubscribe
  from `nbcu-ds-sandbox-a-001.wes_crm_sandbox.BillingPlatformAnalysis_EmailDeliveryStatus`
  where Month_Year = '2022-10-01'
) marketing_status
on l.aid = marketing_status.aid

-- retrieve status 1 month later
left join(
  select 
    adobe_tracking_id,
    billing_platform,
    paying_account_flag
  from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
  where 
    report_date = '2022-11-30'
) r
on l.aid = r.adobe_tracking_id

-- retrieve members of holdout group
left join(
  select aid
  from `nbcu-ds-sandbox-a-001.wes_crm_sandbox.Q42022_Holdout`
) ho
on l.aid = ho.aid

--retrieve the rest of the global hold out that doesnt fit holdout definition. exclude these users from the analysis
left join(
  select HouseholdId hid
  from `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP` 
  where 
    cohort = 'October2022' 
    and Hold_Out_Type_Current = 'Owned Email Holdout' -- Exclude those who are assigned to Email Holdout but actually received emails
    and TrackingId not in(
      select aid
      from `nbcu-ds-sandbox-a-001.wes_crm_sandbox.Q42022_Holdout`
    )
) ho1
on l.hid = ho1.hid
[7:00 AM] -------------------------------------------------------------------------------------------
-- Broken out by grouped billing platform, summary of users and their actions
-------------------------------------------------------------------------------------------
select 
  u.Month_Year,
  audience,
  -- billing_platform,
  grouped_billing_platform,
  -- tenure_paid_lens,
  sum(count(distinct u.aid)) over(partition by u.Month_Year) total_paid_maas,
  count(distinct u.aid) paid_maas,
  count(distinct case when change_flag = 'Downgrade' then u.aid else null end) total_downgrades,
  sum(case when change_flag = 'No Change' then 1 else 0 end) total_no_change,
from `nbcu-ds-sandbox-a-001.wes_crm_sandbox.BillingPlatformAnalysis_UserData_Feb2023` 
where 
  Audience ='Targetable'
  and billing_cycle = 'MONTHLY'
  and abandoned_flag is null
  and category != 'Email Unsubscribes'
group by 1,2,3 --,4
order by 1,2,3 --,4