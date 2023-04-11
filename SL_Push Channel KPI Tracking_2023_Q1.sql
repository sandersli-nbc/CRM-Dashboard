--SL_Push Channel KPI Tracking_2023_Q1
-----------------------------------------------------------------------------------------


-----------------------------------------------------------------------------------------
-- Braze ID -> Adobe Tracking ID mapping View
-----------------------------------------------------------------------------------------

CREATE OR REPLACE view  `nbcu-sdp-sandbox-prod.sl_sandbox.Braze_Id_Adobe_Id_Map` AS
select adobe_tid as aid, braze_id as bid from 
( select distinct profileid, partnerorsystemid, externalprofilerid as braze_id from  `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping` 
    where Partnerorsystemid = 'braze'
) as braze_customer_mapping
left join 
(   select distinct profileid as pid, externalprofilerid as adobe_tid from  `nbcu-sdp-prod-003.sdp_persistent_views.CustomerKeysMapping` 
    where Partnerorsystemid = 'trackingid'
) as adobe_id
on braze_customer_mapping.profileid = adobe_id.pid;

-----------------------------------------------------------------------------------------



--push opt-in
select * -- distinct adobe_tracking_id as aid
from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
where canvas_id ='f4f21b32-e2ce-493f-a4dd-9132e45c65ff' --canvas_name = 'Push Optins'; not displayed on Oct 11 because of canvas metadata issue right now (should be fixed soon)
and event_date >= '2022-11-30'
and event_name = 'Webhook Sends'
limit 100 



-----------------------------------------------------------------------------------------
--  SL_Push Channel KPI Tracking_2023Q1
-----------------------------------------------------------------------------------------

 -- 2023 Jan
DECLARE report_start_date date DEFAULT '2023-01-01';
DECLARE report_end_date date DEFAULT '2023-01-31';


/*
 -- 2023 Feb
DECLARE report_start_date date DEFAULT '2023-02-01';
DECLARE report_end_date date DEFAULT '2023-02-28';
*/

/*
 -- 2023 Mar
DECLARE report_start_date date DEFAULT '2023-03-01';
DECLARE report_end_date date DEFAULT '2023-03-31';
*/



WITH 
Push_Targeted AS ( -- everyone who have received push in the month, and message not bounced 

    SELECT distinct aid 

    FROM (
                select canvas_campaign_name, other_7, sum(event_count) as event_flag

                from (

                    select distinct coalesce (canvasName,campaignName) as canvas_campaign_name
                            , identity as other_7
                            , case when eventName = 'Push Notification Sends' then 10
                                when eventName = 'Push Notification Bounces' then 1
                                else 0 end AS event_count

                    from `nbcu-sdp-prod-003.sdp_persistent_views.BrazePushNotificationContactView` 
                    where date(eventTimestamp) BETWEEN report_start_date AND report_end_date
                    )

                group by 1,2
                having sum(event_count) = 10
            ) a

    INNER JOIN `nbcu-sdp-sandbox-prod.sl_sandbox.Braze_Id_Adobe_Id_Map` map
    ON map.bid = a.other_7
) 

, push_channel_holdout AS ( 
        select distinct TrackingId as aid-- distinct Hold_Out_Type_Current --cohort, count(distinct TrackingId)
        from `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP` 
        where cohort = 'January2023'
        and Hold_Out_Type_Current = 'Owned Push Notification Holdout' -- 'Owned Email Holdout' 
    )

-----------------------------------------------------------------------------------------
-- Lauren's holdout: braze segment - not received wide send push in Q2 & Push Opted-in 
-----------------------------------------------------------------------------------------
-- webhook push opt-in canvas in Braze 


,Holdout AS ( 
    select distinct b.aid
    
    from (
          select distinct adobe_tracking_id 
          from `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
          where canvas_id ='f4f21b32-e2ce-493f-a4dd-9132e45c65ff' --canvas_name = 'Push Optins'; not displayed on Oct 11 because of canvas metadata issue right now (should be fixed soon)
          and event_date >= report_end_date -- edited from end of each quarter; '2022-12-31','2022-09-30'
          and event_name = 'Webhook Sends'
          group by 1           
              ) a

     INNER JOIN push_channel_holdout b
        ON a.adobe_tracking_id = b.aid 
)


,All_Cohorts AS (

     SELECT distinct a.aid
     , a.cohort
     , user.primary_device_name as primary_device

     FROM
        (SELECT *, 'Push_Targeted' as cohort from Push_Targeted
            UNION ALL
            SELECT *, 'Holdout' as cohort from Holdout
        ) a

    INNER JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES` user
          ON a.aid = user.adobe_tracking_id
          AND report_date = report_end_date
 
        -- for after 2021/july, exclude all abandon MAAs
        INNER JOIN 
        (SELECT DISTINCT adobe_tracking_id FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES` 
            WHERE report_date = report_end_date
            AND date_of_last_view IS NOT NULL 
        ) abandon_maa 
        ON a.aid = abandon_maa.adobe_tracking_id

)


, Video_Viewing AS (
    SELECT 
        adobe_tracking_id
        , COUNT (DISTINCT CASE WHEN VIDEO.num_views_started =1 THEN video.adobe_tracking_id ELSE NULL END) AS Distinct_Content_Starts
        , SUM (VIDEO.num_views_started ) AS Total_Content_Starts
        , SUM(VIDEO.num_seconds_played_no_ads)/3600 AS Viewing_Time
        , COUNT(DISTINCT session_id) AS Distinct_Viewing_Sessions 
        , COUNT(DISTINCT(CASE WHEN (num_seconds_played_no_ads > CASE WHEN lower(consumption_type) = 'virtual channel' THEN 299 ELSE 0 END)
                and (num_views_started>0) THEN CASE WHEN (lower(consumption_type) = "shortform") THEN "Shortform"
                                                    WHEN lower(franchise) != 'other' THEN franchise ELSE display_name
                                                    END
                end)) as Repertoire_Pavo_Method
                
    FROM All_Cohorts a

    INNER JOIN  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` VIDEO
        ON VIDEO.adobe_tracking_id = a.aid
        AND adobe_date between report_start_date AND report_end_date
    GROUP BY 1

    )



,Save_Denom AS ( --'Lapsing_Users'
        SELECT distinct adobe_tracking_id, date_of_last_view -- no need for max (days_since_last_view), can be saved multiple times, dedup later
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES` 
        WHERE report_date BETWEEN report_start_date AND report_end_date 
        AND days_since_last_view BETWEEN 15 AND 29 -- this guarantees we are only getting people who have at least past the 'lapsing' phase in the time period.
    )


,Save_Num as (
        SELECT distinct a.adobe_tracking_id      
        FROM Save_Denom a
        INNER JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO` VIDEO 
            ON a.adobe_tracking_id = VIDEO.adobe_tracking_id 
            AND adobe_date >= report_start_date AND adobe_date<= report_end_date
            AND VIDEO.adobe_date >= DATE_ADD(date_of_last_view, INTERVAL 15 day)
            AND VIDEO.adobe_date <= DATE_ADD(date_of_last_view, INTERVAL 29 day)
    )



,Winback_Denom AS ( --'Lapsed_Users'
        SELECT distinct adobe_tracking_id, date_of_last_view
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES` 
        WHERE report_date BETWEEN report_start_date AND report_end_date 
        AND days_since_last_view >=30 AND days_since_last_view <= 90-- this guarantees we are only getting people who have at least past the 'lapsing' phase in the time period.
    )

-- REVIEW: compare this code to email dash (Gold table)
,Winback_Num AS (
    SELECT distinct a.adobe_tracking_id

    FROM Winback_Denom a
    INNER JOIN `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`  VIDEO 
        ON a.adobe_tracking_id = VIDEO.adobe_tracking_id 
        AND adobe_date >= report_start_date AND adobe_date<= report_end_date
        AND VIDEO.adobe_date >= DATE_ADD(date_of_last_view, INTERVAL 30 day)
    )


, Upgrade_Denom AS (
-- upgrade metric 2.0: nonpaying to paying
    SELECT distinct adobe_tracking_id 
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`  USER
    WHERE paying_account_flag = 'NonPaying' 
    AND USER.report_date BETWEEN report_start_date and report_end_date
)


, Upgrade_Num AS (
-- find all users who have upgraded at least once in the month of
    SELECT  distinct adobe_tracking_id
    FROM
        (SELECT  
                report_date
             , adobe_tracking_id     
         FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`  USER
         WHERE entitlement_change_flag IN ('Upgrade: Free to Premium'
                                                    , 'Upgrade: Free to Premium+'
                                                    , 'Upgrade: Premium to Premium+') 
         AND paying_account_flag = 'Paying'                                            
            AND USER.report_date BETWEEN report_start_date and report_end_date
        ) 
)    

, Paid_Churn_Denom AS (
    SELECT distinct adobe_tracking_id
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
    WHERE paying_account_flag = 'Paying'
    AND auto_renew_flag = 'OFF'
    AND report_date BETWEEN report_start_date and report_end_date
)

, Paid_Churn_Num AS (

    select distinct adobe_tracking_id
    from (
            select adobe_tracking_id
                , report_date
                , auto_renew_flag as auto_renew_flag_today
                , LEAD(auto_renew_flag,1) OVER ( partition by adobe_tracking_id order by report_date ) as auto_renew_flag_next_day
            from  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`  
            where report_date BETWEEN report_start_date and report_end_date       
            order by 1,2
            )
    where auto_renew_flag_today = 'OFF' and auto_renew_flag_next_day = 'ON'
)

, New_Upgrade_Denom AS (

    select distinct adobe_tracking_id
    from `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_never`
    where report_date between report_start_date and report_end_date
)

, New_Upgrade_Num AS (
    
    select distinct adobe_tracking_id
    from `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_date_rank`
    where upgrade_row_number = 1
    and report_date between report_start_date and report_end_date
)


, Paid_Winbacks_Denom AS (

 SELECT distinct adobe_tracking_id
        FROM (
                SELECT adobe_tracking_id
                        , report_date
                FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
                WHERE paying_account_flag = 'NonPaying'  
                AND report_date between report_start_date and report_end_date
            )
        WHERE adobe_tracking_id NOT IN (select distinct adobe_tracking_id from `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_never` 
                                        where report_date between report_start_date and report_end_date )

)

, Paid_Winbacks_Num AS (

    select distinct adobe_tracking_id
    from `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_date_rank`
    where upgrade_row_number > 1
    and report_date between report_start_date and report_end_date
)

-- this metric uses silver_churn, which is based on data in PAVO dash Churn Trend
, EOM_Paid_Churn_Denom AS (
  select adobe_tracking_id
  from nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN
  where date = report_end_date
  and entitlement = 'Paid'
)


, EOM_Paid_Churn_Num AS (
  select adobe_tracking_id
  from nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN
  where date = report_end_date
  and entitlement = 'Paid'
  and Churn_flag = 'Churn'
)


, CTE_1 AS (
  SELECT 
    report_start_date AS Report_Month
    --, Account_Type
    
        , count(distinct case when cohort = 'Push_Targeted' then a.aid end ) as Distinct_Cohort_Size_Targeted
        , count(distinct case when cohort = 'Holdout' then a.aid end) as Distinct_Cohort_Size_Holdout

        , count(distinct case when cohort = 'Push_Targeted' then video.adobe_tracking_id end ) as Total_Returns_Targeted
        , count(distinct case when cohort = 'Holdout' then video.adobe_tracking_id end) as Total_Returns_Holdout

        , sum(case when cohort = 'Push_Targeted' then video.Viewing_Time end ) as Total_Usage_Targeted
        , sum(case when cohort = 'Holdout' then video.Viewing_Time end) as Total_Usage_Holdout

        , sum(case when cohort = 'Push_Targeted' then video.Repertoire_Pavo_Method end ) as Total_Repertoire_Targeted
        , sum(case when cohort = 'Holdout' then video.Repertoire_Pavo_Method end) as Total_Repertoire_Holdout

        , sum(case when cohort = 'Push_Targeted' then video.Distinct_Viewing_Sessions end ) as Total_Viewing_Sessions_Targeted
        , sum(case when cohort = 'Holdout' then video.Distinct_Viewing_Sessions end) as Total_Viewing_Sessions_Holdout


        , count(distinct case when cohort = 'Push_Targeted' then Winback_Denom.adobe_tracking_id end ) as Winback_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then Winback_Num.adobe_tracking_id end) as Winback_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Winback_Denom.adobe_tracking_id end ) as Winback_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Winback_Num.adobe_tracking_id end) as Winback_Num_Holdout

        , count(distinct case when cohort = 'Push_Targeted' then Save_Denom.adobe_tracking_id end ) as Save_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then Save_Num.adobe_tracking_id end) as Save_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Save_Denom.adobe_tracking_id end ) as Save_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Save_Num.adobe_tracking_id end) as Save_Num_Holdout

        , count(distinct case when cohort = 'Push_Targeted' then Upgrade_Denom.adobe_tracking_id end ) as Upgrades_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then Upgrade_Num.adobe_tracking_id end) as Upgrades_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Upgrade_Denom.adobe_tracking_id end ) as Upgrades_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Upgrade_Num.adobe_tracking_id end) as Upgrades_Num_Holdout



        , count(distinct case when cohort = 'Push_Targeted' then New_Upgrade_Denom.adobe_tracking_id end ) as Total_New_Upgrade_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then New_Upgrade_Num.adobe_tracking_id end) as Total_New_Upgrade_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then New_Upgrade_Denom.adobe_tracking_id end ) as Total_New_Upgrade_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then New_Upgrade_Num.adobe_tracking_id end) as Total_New_Upgrade_Num_Holdout

        , count(distinct case when cohort = 'Push_Targeted' then Paid_Winbacks_Denom.adobe_tracking_id end ) as Total_Paid_Winbacks_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then Paid_Winbacks_Num.adobe_tracking_id end) as Total_Paid_Winbacks_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Paid_Winbacks_Denom.adobe_tracking_id end ) as Total_Paid_Winbacks_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Paid_Winbacks_Num.adobe_tracking_id end) as Total_Paid_Winbacks_Num_Holdout

        , count(distinct case when cohort = 'Push_Targeted' then Paid_Churn_Denom.adobe_tracking_id end ) as Total_Paid_Churn_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then Paid_Churn_Num.adobe_tracking_id end) as Total_Paid_Churn_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Paid_Churn_Denom.adobe_tracking_id end ) as Total_Paid_Churn_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Paid_Churn_Num.adobe_tracking_id end) as Total_Paid_Churn_Num_Holdout

        , count(distinct case when cohort = 'Push_Targeted' then EOM_Paid_Churn_Denom.adobe_tracking_id end ) as EOM_Paid_Churn_Denom_Targeted
        , count(distinct case when cohort = 'Push_Targeted' then EOM_Paid_Churn_Num.adobe_tracking_id end) as EOM_Paid_Churn_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then EOM_Paid_Churn_Denom.adobe_tracking_id end ) as EOM_Paid_Churn_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then EOM_Paid_Churn_Num.adobe_tracking_id end) as EOM_Paid_Churn_Num_Holdout


  FROM All_Cohorts a
  LEFT JOIN Video_Viewing video
      ON a.aid = video.adobe_tracking_id

  LEFT JOIN Winback_Denom
      ON a.aid = Winback_Denom.adobe_tracking_id

  LEFT JOIN Winback_Num
      ON Winback_Denom.adobe_tracking_id = Winback_Num.adobe_tracking_id

  LEFT JOIN Save_Denom
      ON a.aid = Save_Denom.adobe_tracking_id

  LEFT JOIN Save_Num
      ON Save_Denom.adobe_tracking_id = Save_Num.adobe_tracking_id

  LEFT JOIN Upgrade_Denom
      ON a.aid = Upgrade_Denom.adobe_tracking_id

  LEFT JOIN Upgrade_Num 
      ON Upgrade_Denom.adobe_tracking_id = Upgrade_Num.adobe_tracking_id

  LEFT JOIN New_Upgrade_Denom
      on a.aid = New_Upgrade_Denom.adobe_tracking_id

  LEFT JOIN New_Upgrade_Num
      on New_Upgrade_Denom.adobe_tracking_id = New_Upgrade_Num.adobe_tracking_id

  LEFT JOIN Paid_Winbacks_Denom
      on a.aid = Paid_Winbacks_Denom.adobe_tracking_id

  LEFT JOIN Paid_Winbacks_Num
      on Paid_Winbacks_Num.adobe_tracking_id = Paid_Winbacks_Denom.adobe_tracking_id

  LEFT JOIN Paid_Churn_Denom
      on a.aid = Paid_Churn_Denom.adobe_tracking_id

  LEFT JOIN Paid_Churn_Num
      on Paid_Churn_Denom.adobe_tracking_id = Paid_Churn_Num.adobe_tracking_id
  
  LEFT JOIN EOM_Paid_Churn_Denom
      on a.aid = EOM_Paid_Churn_Denom.adobe_tracking_id

  LEFT JOIN EOM_Paid_Churn_Num
      on EOM_Paid_Churn_Denom.adobe_tracking_id = EOM_Paid_Churn_Num.adobe_tracking_id
  Group By 1
  )

, CTE_2 AS (
  SELECT Report_Month
  --, Primary_Device
  , Distinct_Cohort_Size_Targeted   as Push_Targeted
  , Distinct_Cohort_Size_Holdout   as Push_Holdout

  , safe_divide(Total_Returns_Targeted, Distinct_Cohort_Size_Targeted)  as Return_Rate_Engagers
  , safe_divide(Total_Returns_Holdout, Distinct_Cohort_Size_Holdout)  as Return_Rate_Holdout

  , safe_divide(Total_Usage_Targeted, Distinct_Cohort_Size_Targeted)  as Usage_Engagers
  , safe_divide(Total_Usage_Holdout, Distinct_Cohort_Size_Holdout)   as Usage_Holdout

  , safe_divide(Total_Repertoire_Targeted, Distinct_Cohort_Size_Targeted)  as Repertoire_Engagers
  , safe_divide(Total_Repertoire_Holdout,Distinct_Cohort_Size_Holdout)   as Repertoire_Holdout


  , safe_divide(Total_Viewing_Sessions_Targeted, Distinct_Cohort_Size_Targeted)  as Sessions_Engagers
  , safe_divide(Total_Viewing_Sessions_Holdout, Distinct_Cohort_Size_Holdout)   as Sessions_Holdout

  , Winback_Denom_Targeted 
  , safe_divide(Winback_Num_Targeted, Winback_Denom_Targeted)  as Winback_Rate_Engagers
  , safe_divide(Winback_Num_Holdout, Winback_Denom_Holdout) as Winback_Rate_Holdout

  , Save_Denom_Targeted 
  , safe_divide(Save_Num_Targeted, Save_Denom_Targeted)  as Save_Rate_Engagers
  , safe_divide(Save_Num_Holdout, Save_Denom_Holdout) as Save_Rate_Holdout

  , Upgrades_Denom_Targeted
  , safe_divide(Upgrades_Num_Targeted, Upgrades_Denom_Targeted)  as Upgrade_Rate_Engagers
  , safe_divide(Upgrades_Num_Holdout, Upgrades_Denom_Holdout) as Upgrade_Rate_Holdout

  , Total_New_Upgrade_Denom_Targeted
  , safe_divide(Total_New_Upgrade_Num_Targeted, Total_New_Upgrade_Denom_Targeted)  as New_Upgrade_Rate_Engagers
  , safe_divide(Total_New_Upgrade_Num_Holdout,  Total_New_Upgrade_Denom_Holdout) as New_Upgrade_Rate_Holdout

  , Total_Paid_Winbacks_Denom_Targeted
  , safe_divide(Total_Paid_Winbacks_Num_Targeted, Total_Paid_Winbacks_Denom_Targeted)  as Paid_Winback_Rate_Engagers 
  , safe_divide(Total_Paid_Winbacks_Num_Holdout,  Total_Paid_Winbacks_Denom_Holdout) as Paid_Winback_Rate_Holdout 

  , Total_Paid_Churn_Denom_Targeted
  , safe_divide(Total_Paid_Churn_Num_Targeted, Total_Paid_Churn_Denom_Targeted) as Paid_Churn_Save_Rate_Engagers
  , safe_divide(Total_Paid_Churn_Num_Holdout, Total_Paid_Churn_Denom_Holdout) as Paid_Churn_Save_Rate_Holdout

  , EOM_Paid_Churn_Denom_Targeted
  , safe_divide(EOM_Paid_Churn_Num_Targeted, EOM_Paid_Churn_Denom_Targeted) as Paid_Churn_Rate_Engagers
  , safe_divide(EOM_Paid_Churn_Num_Holdout, EOM_Paid_Churn_Denom_Holdout) as Paid_Churn_Rate_Holdout

  from CTE_1
)


SELECT Report_Month
--, Account_Type
, Push_Targeted
, Push_Holdout


, Return_Rate_Engagers                                                   as Return_Rate_Engagers
, Return_Rate_Holdout                                                    as Return_Rate_Holdout
, Return_Rate_Engagers - Return_Rate_Holdout                             as Return_Rate_Lift_PTS
, safe_divide(Return_Rate_Engagers, Return_Rate_Holdout) *100                        as Return_Rate_Lift_Index
, (Return_Rate_Engagers - Return_Rate_Holdout) * Push_Targeted          as Returns_Incrementals

, Usage_Engagers                                                         as Usage_Engagers
, Usage_Holdout                                                          as Usage_Holdout
, Usage_Engagers - Usage_Holdout                                         as Usage_Lift_PTS
, safe_divide(Usage_Engagers, Usage_Holdout) *100                                    as Usage_Lift_Index
, (Usage_Engagers - Usage_Holdout) * Push_Targeted                      as Usage_Incrementals


, Repertoire_Engagers                                                    as Repertoire_Engagers
, Repertoire_Holdout                                                     as Repertoire_Holdout
, Repertoire_Engagers - Repertoire_Holdout                               as Repertoire_Lift_PTS
, safe_divide(Repertoire_Engagers, Repertoire_Holdout) *100              as Repertoire_Lift_Index
, (Repertoire_Engagers - Repertoire_Holdout) * Push_Targeted as Repertoire_Incrementals


, Sessions_Engagers                                                      as Sessions_Engagers
, Sessions_Holdout                                                       as Sessions_Holdout
, Sessions_Engagers - Sessions_Holdout                                   as Sessions_Lift_PTS
, safe_divide( Sessions_Engagers, Sessions_Holdout) *100                 as Sessions_Lift_Index
, (Sessions_Engagers - Sessions_Holdout) * Push_Targeted as Sessions_Incrementals


, Winback_Rate_Engagers                                                   as Winback_Rate_Engagers
, Winback_Rate_Holdout                                                    as Winback_Rate_Holdout
, Winback_Rate_Engagers - Winback_Rate_Holdout                            as Winback_Rate_Lift_PTS
, safe_divide(Winback_Rate_Engagers, Winback_Rate_Holdout) *100                       as Winback_Rate_Lift_Index
, (Winback_Rate_Engagers - Winback_Rate_Holdout) * Winback_Denom_Targeted as Winback_Incrementals


, Save_Rate_Engagers                                                      as Save_Rate_Engagers
, Save_Rate_Holdout                                                       as Save_Rate_Holdout
, Save_Rate_Engagers - Save_Rate_Holdout                                  as Save_Rate_Lift_PTS
, safe_divide(Save_Rate_Engagers, Save_Rate_Holdout) *100                 as Save_Rate_Lift_Index
, (Save_Rate_Engagers - Save_Rate_Holdout) * Save_Denom_Targeted          as Save_Rate_Lift_Incrementals


, Upgrade_Rate_Engagers                                                    as Upgrade_Rate_Engagers
, Upgrade_Rate_Holdout                                                     as Upgrade_Rate_Holdout
, Upgrade_Rate_Engagers - Upgrade_Rate_Holdout                             as Upgrade_Rate_Lift_PTS
, safe_divide(Upgrade_Rate_Engagers, Upgrade_Rate_Holdout) *100                        as Upgrade_Rate_Lift_Index
, (Upgrade_Rate_Engagers - Upgrade_Rate_Holdout) * Upgrades_Denom_Targeted as Upgrade_Incrementals


, New_Upgrade_Rate_Engagers                                                as New_Upgrade_Rate_Engagers
, New_Upgrade_Rate_Holdout                                                 as New_Upgrade_Rate_Holdout
, New_Upgrade_Rate_Engagers - New_Upgrade_Rate_Holdout                     as New_Upgrade_Rate_Lift_PTS
, safe_divide(New_Upgrade_Rate_Engagers,New_Upgrade_Rate_Holdout) *100                as New_Upgrade_Rate_Lift_Index
, (New_Upgrade_Rate_Engagers - New_Upgrade_Rate_Holdout) * Total_New_Upgrade_Denom_Targeted as New_Upgrade_Incrementals

, Paid_Winback_Rate_Engagers                                               as Paid_Winback_Rate_Engagers
, Paid_Winback_Rate_Holdout                                                as Paid_Winback_Rate_Holdout
, Paid_Winback_Rate_Engagers - Paid_Winback_Rate_Holdout                   as Paid_Winback_Rate_Lift_PTS
, safe_divide(Paid_Winback_Rate_Engagers, Paid_Winback_Rate_Holdout) *100              as Paid_Winback_Rate_Lift_Index
, (Paid_Winback_Rate_Engagers - Paid_Winback_Rate_Holdout) * Total_Paid_Winbacks_Denom_Targeted as Paid_Winback_Rate_Lift_Incrementals

, Paid_Churn_Save_Rate_Engagers                                             as Paid_Churn_Save_Rate_Engagers
, Paid_Churn_Save_Rate_Holdout                                       as Paid_Churn_Save_Rate_Holdout
, Paid_Churn_Save_Rate_Engagers - Paid_Churn_Save_Rate_Holdout       as Paid_Churn_Save_Rate_Lift_PTS
, safe_divide(Paid_Churn_Save_Rate_Engagers,Paid_Churn_Save_Rate_Holdout) *100  as Paid_Churn_Save_Rate_Lift_Index
, (Paid_Churn_Save_Rate_Engagers - Paid_Churn_Save_Rate_Holdout) * Total_Paid_Churn_Denom_Targeted as Paid_Churn_Save_Rate_Incrementals


, Paid_Churn_Rate_Engagers                                                  as EOM_Paid_Churn_Rate_Engagers
, Paid_Churn_Rate_Holdout                                                   as EOM_Paid_Churn_Rate_Holdout
, Paid_Churn_Rate_Engagers - Paid_Churn_Rate_Holdout                        as EOM_Paid_Churn_Rate_Lift_PTS
, safe_divide(Paid_Churn_Rate_Engagers, Paid_Churn_Rate_Holdout) *100                   as EOM_Paid_Churn_Rate_Lift_Index
, (Paid_Churn_Rate_Engagers - Paid_Churn_Rate_Holdout) * EOM_Paid_Churn_Denom_Targeted as EOM_Paid_Churn_Rate_Incrementals


FROM CTE_2
;
