--SL_Email Channel KPI Tracking_2023_Q1
-----------------------------------------------------------------------------------------

-- Modified Repertoire since Jan 2023



-- ADD and refresh two intermediate tables: 

-----------------------------------------------------------------------------------------
--  Upgrade table A: distinguish first time upgrade vs subsequent upgrades
-----------------------------------------------------------------------------------------

CREATE OR REPLACE table  `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_date_rank` AS

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
            --WHERE report_date between '2022-01-01' and '2022-07-10' -- don't add report_date since we need to distinguish upgrades from beginnning of time 
            ORDER BY 1,2    -- testing
        )
WHERE paying_account_flag_today = 'Paying' AND paying_account_flag_yestd = 'NonPaying'; 

-- select count (adobe_tracking_id) from  `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_date_rank` 


-----------------------------------------------------------------------------------------
--  Upgrade table B: Denom for never upgraded 
-----------------------------------------------------------------------------------------
-- select count (distinct adobe_tracking_id)  from `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_never`  where report_date between '2022-01-01' and '2022-01-10'

-- as of 2022-01-01,  62127044 users never upgraded 

CREATE OR REPLACE table  `nbcu-ds-sandbox-a-001.sl_sandbox.upgrade_never` AS

SELECT adobe_tracking_id
       , report_date

FROM (
        SELECT adobe_tracking_id
                , report_date
                , sum(paying_flag_numeric) OVER(partition by adobe_tracking_id order by report_date ) as cumulative_nonpaying_num -- rank the number of times a user upgrade
        FROM (
                SELECT adobe_tracking_id
                                    , report_date
                                    , case when paying_account_flag = 'Paying' then 1 else 0 end as paying_flag_numeric
                                    
                            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
            )
    )

WHERE cumulative_nonpaying_num = 0; 

-----------------------------------------------------------------------------------------
-- V3_With Other Opens
-----------------------------------------------------------------------------------------

/* changes made:
1) exclude Oct Privacy email openers from targetable and holdout
2) include only email engagers for both targetable and holdout
3) exclude unsubscribed from holdout (as of end of the month - Jan)
*/

-- unsubscribe table: exclude unsubscribed (as of beg of the month - Jan)
--11,168,573

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.sl_sandbox.Email_Unsubs` AS

SELECT DISTINCT adobe_tracking_id
          FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
          WHERE event_name = 'Email Unsubscribes' 
          AND event_date <= '2023-01-31' --report_end_date

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

Email_Targeted AS ( -- everyone who have received emails in the month, and opened at least one email in the past

    SELECT DISTINCT adobe_tracking_id 
            FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
            WHERE event_name = 'Email Deliveries' 
            AND event_date BETWEEN report_start_date and report_end_date
)


--HOLDOUT 2.0, for months after July 2021
-- In holout CTE, ID NOT in () stopped working so I modified the query with a left join 
,Holdout AS ( 

      select distinct holdout.adobe_tracking_id
      from (
        select distinct TrackingId as adobe_tracking_id -- distinct Hold_Out_Type_Current --cohort, count(distinct TrackingId)
        from `nbcu-ds-prod-001.PeacockDataMartMarketingGold.HOLDOUT_GROUP` 
        where cohort = 'January2023'  -- 'April2022' 
        and Hold_Out_Type_Current = 'Owned Email Holdout'
        -- Exclude those who are assigned to Email Holdout but actually received emails
      ) holdout
      left join 
        (SELECT DISTINCT adobe_tracking_id
          FROM  `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_MPARTICLE_BRAZE` 
          WHERE event_name = 'Email Deliveries' 
          AND event_date >= report_start_date
          AND event_date <= report_end_date 
        ) delivered-- Email Delivered ever 
      on holdout.adobe_tracking_id = delivered.adobe_tracking_id
      and delivered.adobe_tracking_id is null 

    )

,All_Cohorts AS (
    -- email targetable and holdout cohort, users signed up after 2020/8/11
     SELECT distinct a.adobe_tracking_id as aid
     , cohort

     FROM         -- Include email engagers only: users who have at least 1 'Other Opens' in entire user histoy; exclude Oct 2022 Privacy email
     (SELECT DISTINCT identity 
            FROM  `nbcu-sdp-prod-003.sdp_persistent_views.BrazeMarketingView` 
            WHERE eventName = 'Email Opens' and machineOpen is null
            AND date(eventTimestamp) <= report_end_date
            AND campaignName NOT IN ('TransactionalTermsOfUseEngaged20221028', 'TransactionalTermsOfUseNONEngaged20221028') -- Exclude email openers of Oct privacy email
        ) Email_Engagers --opened email at least once, using other open 

    INNER JOIN `nbcu-ds-sandbox-a-001.sl_sandbox.Braze_Id_Adobe_Id_Map` mapping
     ON mapping.bid = Email_Engagers.identity

    INNER JOIN
        (SELECT *, 'Email_Targeted' as cohort from Email_Targeted
            UNION ALL
            SELECT *, 'Holdout' as cohort from Holdout
        ) a

     ON a.adobe_tracking_id = mapping.aid

        --add attribute: account_type 
        /*
        INNER JOIN ( SELECT * FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` WHERE report_date = report_end_date ) user
        ON a.adobe_tracking_id = user.adobe_tracking_id
        */

        -- for after 2021/july, email channel only, take out all abandon MAAs
        INNER JOIN 
        (SELECT DISTINCT adobe_tracking_id FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES` 
            WHERE report_date = report_end_date
            AND date_of_last_view IS NOT NULL 
        ) abandon_maa 
        ON a.adobe_tracking_id = abandon_maa.adobe_tracking_id
 
)


, Video_Viewing AS (
    SELECT 
        adobe_tracking_id
        , COUNT (DISTINCT CASE WHEN VIDEO.num_views_started =1 THEN video.adobe_tracking_id ELSE NULL END) AS Distinct_Content_Starts
        , SUM (VIDEO.num_views_started ) AS Total_Content_Starts
        , SUM(VIDEO.num_seconds_played_no_ads)/3600 AS Viewing_Time
        , COUNT(DISTINCT session_id) AS Distinct_Viewing_Sessions 
        /*
        , COUNT(DISTINCT (CASE WHEN (num_views_started = 1 AND num_seconds_played_no_ads > 0) THEN 
                    CASE WHEN (VIDEO.consumption_type = "Shortform") THEN "Shortform" 
                    ELSE VIDEO.program END 
                ELSE NULL END)) as Repertoire_Pavo_Method -- /? TO MODIFY?? 
        */ -- Used in 2022 reporting 
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
--        , a.Account_Type
    
        , count(distinct case when cohort = 'Email_Targeted' then a.aid end ) as Distinct_Cohort_Size_Targeted
        , count(distinct case when cohort = 'Holdout' then a.aid end) as Distinct_Cohort_Size_Holdout

        , count(distinct case when cohort = 'Email_Targeted' then video.adobe_tracking_id end ) as Total_Returns_Targeted
        , count(distinct case when cohort = 'Holdout' then video.adobe_tracking_id end) as Total_Returns_Holdout

        , sum(case when cohort = 'Email_Targeted' then video.Viewing_Time end ) as Total_Usage_Targeted
        , sum(case when cohort = 'Holdout' then video.Viewing_Time end) as Total_Usage_Holdout

        , sum(case when cohort = 'Email_Targeted' then video.Repertoire_Pavo_Method end ) as Total_Repertoire_Targeted
        , sum(case when cohort = 'Holdout' then video.Repertoire_Pavo_Method end) as Total_Repertoire_Holdout

        , sum(case when cohort = 'Email_Targeted' then video.Distinct_Viewing_Sessions end ) as Total_Viewing_Sessions_Targeted
        , sum(case when cohort = 'Holdout' then video.Distinct_Viewing_Sessions end) as Total_Viewing_Sessions_Holdout


        , count(distinct case when cohort = 'Email_Targeted' then Winback_Denom.adobe_tracking_id end ) as Winback_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then Winback_Num.adobe_tracking_id end) as Winback_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Winback_Denom.adobe_tracking_id end ) as Winback_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Winback_Num.adobe_tracking_id end) as Winback_Num_Holdout

        , count(distinct case when cohort = 'Email_Targeted' then Save_Denom.adobe_tracking_id end ) as Save_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then Save_Num.adobe_tracking_id end) as Save_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Save_Denom.adobe_tracking_id end ) as Save_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Save_Num.adobe_tracking_id end) as Save_Num_Holdout

        , count(distinct case when cohort = 'Email_Targeted' then Upgrade_Denom.adobe_tracking_id end ) as Upgrades_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then Upgrade_Num.adobe_tracking_id end) as Upgrades_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Upgrade_Denom.adobe_tracking_id end ) as Upgrades_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Upgrade_Num.adobe_tracking_id end) as Upgrades_Num_Holdout



        , count(distinct case when cohort = 'Email_Targeted' then New_Upgrade_Denom.adobe_tracking_id end ) as Total_New_Upgrade_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then New_Upgrade_Num.adobe_tracking_id end) as Total_New_Upgrade_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then New_Upgrade_Denom.adobe_tracking_id end ) as Total_New_Upgrade_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then New_Upgrade_Num.adobe_tracking_id end) as Total_New_Upgrade_Num_Holdout

        , count(distinct case when cohort = 'Email_Targeted' then Paid_Winbacks_Denom.adobe_tracking_id end ) as Total_Paid_Winbacks_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then Paid_Winbacks_Num.adobe_tracking_id end) as Total_Paid_Winbacks_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Paid_Winbacks_Denom.adobe_tracking_id end ) as Total_Paid_Winbacks_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Paid_Winbacks_Num.adobe_tracking_id end) as Total_Paid_Winbacks_Num_Holdout

        , count(distinct case when cohort = 'Email_Targeted' then Paid_Churn_Denom.adobe_tracking_id end ) as Total_Paid_Churn_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then Paid_Churn_Num.adobe_tracking_id end) as Total_Paid_Churn_Num_Targeted
        , count(distinct case when cohort = 'Holdout' then Paid_Churn_Denom.adobe_tracking_id end ) as Total_Paid_Churn_Denom_Holdout
        , count(distinct case when cohort = 'Holdout' then Paid_Churn_Num.adobe_tracking_id end) as Total_Paid_Churn_Num_Holdout

        , count(distinct case when cohort = 'Email_Targeted' then EOM_Paid_Churn_Denom.adobe_tracking_id end ) as EOM_Paid_Churn_Denom_Targeted
        , count(distinct case when cohort = 'Email_Targeted' then EOM_Paid_Churn_Num.adobe_tracking_id end) as EOM_Paid_Churn_Num_Targeted
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

  Group By 1 --,2
  )

, CTE_2 AS (
  SELECT Report_Month
--  , Account_Type 
  , Distinct_Cohort_Size_Targeted   as Email_Engagers
  , Distinct_Cohort_Size_Holdout   as Email_Holdout


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
, Email_Engagers
, Email_Holdout


, Return_Rate_Engagers                                                   as Return_Rate_Engagers
, Return_Rate_Holdout                                                    as Return_Rate_Holdout
, Return_Rate_Engagers - Return_Rate_Holdout                             as Return_Rate_Lift_PTS
, safe_divide(Return_Rate_Engagers, Return_Rate_Holdout) *100                        as Return_Rate_Lift_Index
, (Return_Rate_Engagers - Return_Rate_Holdout) * Email_Engagers          as Returns_Incrementals

, Usage_Engagers                                                         as Usage_Engagers
, Usage_Holdout                                                          as Usage_Holdout
, Usage_Engagers - Usage_Holdout                                         as Usage_Lift_PTS
, safe_divide(Usage_Engagers, Usage_Holdout) *100                                    as Usage_Lift_Index
, (Usage_Engagers - Usage_Holdout) * Email_Engagers                      as Usage_Incrementals


, Repertoire_Engagers                                                    as Repertoire_Engagers
, Repertoire_Holdout                                                     as Repertoire_Holdout
, Repertoire_Engagers - Repertoire_Holdout                               as Repertoire_Lift_PTS
, safe_divide(Repertoire_Engagers, Repertoire_Holdout) *100                          as Repertoire_Lift_Index
, (Repertoire_Engagers - Repertoire_Holdout) * Email_Engagers as Repertoire_Incrementals


, Sessions_Engagers                                                      as Sessions_Engagers
, Sessions_Holdout                                                       as Sessions_Holdout
, Sessions_Engagers - Sessions_Holdout                                   as Sessions_Lift_PTS
, safe_divide(Sessions_Engagers, Sessions_Holdout) *100                              as Sessions_Lift_Index
, (Sessions_Engagers - Sessions_Holdout) * Email_Engagers as Sessions_Incrementals


, Winback_Rate_Engagers                                                   as Winback_Rate_Engagers
, Winback_Rate_Holdout                                                    as Winback_Rate_Holdout
, Winback_Rate_Engagers - Winback_Rate_Holdout                            as Winback_Rate_Lift_PTS
, safe_divide(Winback_Rate_Engagers, Winback_Rate_Holdout) *100                       as Winback_Rate_Lift_Index
, (Winback_Rate_Engagers - Winback_Rate_Holdout) * Winback_Denom_Targeted as Winback_Incrementals


, Save_Rate_Engagers                                                      as Save_Rate_Engagers
, Save_Rate_Holdout                                                       as Save_Rate_Holdout
, Save_Rate_Engagers - Save_Rate_Holdout                                  as Save_Rate_Lift_PTS
, safe_divide(Save_Rate_Engagers, Save_Rate_Holdout) *100                             as Save_Rate_Lift_Index
, (Save_Rate_Engagers - Save_Rate_Holdout) * Save_Denom_Targeted          as Save_Rate_Lift_Incrementals


, Upgrade_Rate_Engagers                                                    as Upgrade_Rate_Engagers
, Upgrade_Rate_Holdout                                                     as Upgrade_Rate_Holdout
, Upgrade_Rate_Engagers - Upgrade_Rate_Holdout                             as Upgrade_Rate_Lift_PTS
, safe_divide(Upgrade_Rate_Engagers, Upgrade_Rate_Holdout) *100                        as Upgrade_Rate_Lift_Index
, (Upgrade_Rate_Engagers - Upgrade_Rate_Holdout) * Upgrades_Denom_Targeted as Upgrade_Incrementals


, New_Upgrade_Rate_Engagers                                                as New_Upgrade_Rate_Engagers
, New_Upgrade_Rate_Holdout                                                 as New_Upgrade_Rate_Holdout
, New_Upgrade_Rate_Engagers - New_Upgrade_Rate_Holdout                     as New_Upgrade_Rate_Lift_PTS
, safe_divide(New_Upgrade_Rate_Engagers, New_Upgrade_Rate_Holdout) *100                as New_Upgrade_Rate_Lift_Index
, (New_Upgrade_Rate_Engagers - New_Upgrade_Rate_Holdout) * Total_New_Upgrade_Denom_Targeted as New_Upgrade_Incrementals

, Paid_Winback_Rate_Engagers                                               as Paid_Winback_Rate_Engagers
, Paid_Winback_Rate_Holdout                                                as Paid_Winback_Rate_Holdout
, Paid_Winback_Rate_Engagers - Paid_Winback_Rate_Holdout                   as Paid_Winback_Rate_Lift_PTS
, safe_divide(Paid_Winback_Rate_Engagers, Paid_Winback_Rate_Holdout) *100              as Paid_Winback_Rate_Lift_Index
, (Paid_Winback_Rate_Engagers - Paid_Winback_Rate_Holdout) * Total_Paid_Winbacks_Denom_Targeted as Paid_Winback_Rate_Lift_Incrementals

, Paid_Churn_Save_Rate_Engagers                                             as Paid_Churn_Save_Rate_Engagers
, Paid_Churn_Save_Rate_Holdout                                              as Paid_Churn_Save_Rate_Holdout
, Paid_Churn_Save_Rate_Engagers - Paid_Churn_Save_Rate_Holdout              as Paid_Churn_Save_Rate_Lift_PTS
, safe_divide(Paid_Churn_Save_Rate_Engagers, Paid_Churn_Save_Rate_Holdout) *100         as Paid_Churn_Save_Rate_Lift_Index
, (Paid_Churn_Save_Rate_Engagers - Paid_Churn_Save_Rate_Holdout) * Total_Paid_Churn_Denom_Targeted as Paid_Churn_Save_Rate_Incrementals

, Paid_Churn_Rate_Engagers                                                  as EOM_Paid_Churn_Rate_Engagers
, Paid_Churn_Rate_Holdout                                                   as EOM_Paid_Churn_Rate_Holdout
, Paid_Churn_Rate_Engagers - Paid_Churn_Rate_Holdout                        as EOM_Paid_Churn_Rate_Lift_PTS
, safe_divide(Paid_Churn_Rate_Engagers, Paid_Churn_Rate_Holdout) *100                   as EOM_Paid_Churn_Rate_Lift_Index
, (Paid_Churn_Rate_Engagers - Paid_Churn_Rate_Holdout) * EOM_Paid_Churn_Denom_Targeted as EOM_Paid_Churn_Rate_Incrementals


FROM CTE_2