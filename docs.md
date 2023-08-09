# KPIs

- `audience` table refers to group of users 
- `viewer` table defines viewership metrics of all users, defined in **Viewer Metrics**
- All divisions are `safe_divide`, but are excluded from logic for clarity
- All measures assume distinct adobe_tracking_id, but `DISTINCT` keyword is added for clarity

### Viewer Metrics
- Viewing table defined monthly
- Excludes ads, trailers, and autoplay (same logic as video_watched_trailing_30d in SILVER_USER) 

```
SELECT  adobe_tracking_id
    ,COUNT(DISTINCT CASE WHEN num_views_started = 1 THEN adobe_tracking_id ELSE NULL END) AS Distinct_Content_Starts -- num_views_started is a flag
    ,SUM(num_views_started)                                                               AS Total_Content_Starts
    ,SUM(num_seconds_played_no_ads)/3600                                                  AS Viewing_Time
    ,COUNT(DISTINCT CASE WHEN num_views_started = 1 THEN session_id ELSE NULL END)        AS Distinct_Viewing_Sessions
    ,COUNT(DISTINCT(CASE 
                        WHEN (num_seconds_played_no_ads > CASE 
                                                            WHEN LOWER(consumption_type) = 'virtual channel' THEN 299 
                                                            ELSE 0 
                                                            END
                                ) AND (num_views_started > 0) 
                        THEN CASE 
                                WHEN (LOWER(consumption_type) = "shortform") THEN "Shortform"
                                WHEN LOWER(franchise) != 'other' THEN franchise 
                                ELSE display_name
                                END
                    END)
        )                                                                                    AS Repertoire_Pavo_Method
FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
WHERE (adobe_date BETWEEN @report_start_date AND @report_end_date)
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
GROUP BY 1
```

### Return Rate
- Percentage of the cohort with a video watch activity sometime in the reporting period.
- Num: Users with video watch activity (Silver Video) 
- Denom: Cohort Size
- Dependencies: `Video_Viewing`

```
SELECT COUNT(DISTINCT v.adobe_tracking_id) / COUNT(DISTINCT a.adobe_tracking_id)
FROM audience a
LEFT JOIN viewing v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Usage
- Average viewing time of all users in the cohort
- Num: Sum of all user viewing time
- Denom: Cohort Size
- Dependencies: `Video_Viewing`

```
SELECT SUM(v.Viewing_Time) / COUNT(DISTINCT a.adobe_tracking_id)
FROM audience a
LEFT JOIN viewing v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Repertoire
- Average repertoire of all users in the cohort
- Num: Sum of all user repertoire
- Denom: Cohort Size
- Dependencies: `Video_Viewing`

```
SELECT SUM(v.Repertoire_Pavo_Method) / COUNT(DISTINCT a.adobe_tracking_id)
FROM audience a
LEFT JOIN viewing v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Viewing Sessions
- Average number of distinct viewing sessions of all users in the cohort
- Num: Sum of all distinct user viewing sessions
- Denom: Cohort Size
- Dependencies: `Video_Viewing`

```
SELECT SUM(v.Distinct_Viewing_Sessions) / COUNT(DISTINCT a.adobe_tracking_id)
FROM audience a
LEFT JOIN viewing v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Lapsed Save Rate
- Percentage of the cohort who were past lapsing (last watch date between 30 and 90 days) sometime during the reporting period that had watched something in the reporting period
- Num: Users who watched something in the current month who were past lapsing (last watch date is between 30 and 90 days)
- Denom: Users in the month who are past lapsing (last watch date is between 30 and 90 days)
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`, `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`

```
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
SELECT COUNT(DISTINCT Lapsed_Save_Num.adobe_tracking_id) / COUNT(DISTINCT Lapsed_Save_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN Lapsed_Save_Denom
    ON a.adobe_tracking_id = Lapsed_Save_Denom.adobe_tracking_id
LEFT JOIN Lapsed_Save_Num
    ON Lapsed_Save_Denom.adobe_tracking_id = Lapsed_Save_Num.adobe_tracking_id
```

### Lapsing Save Rate
- Percentage of the cohort who were past lapsing (last watch date between 15 and 29 days) sometime during the reporting period that had watched something in the reporting period
- Num: Users who watched something in the current month who were lapsing users (last watch date is betweeen 15 and 29 days)
- Denom: Users in the month who are lapsing users (last watch date is betweeen 15 and 29 days)
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_PRIMARY_DEVICES`, `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`

```
WITH 
    Lapsing_Save_Base AS (
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
SELECT COUNT(DISTINCT Lapsing_Save_Num.adobe_tracking_id) / COUNT(DISTINCT Lapsing_Save_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN Lapsing_Save_Denom
    ON a.adobe_tracking_id = Lapsing_Save_Denom.adobe_tracking_id
LEFT JOIN Lapsing_Save_Num
    ON Lapsing_Save_Denom.adobe_tracking_id = Lapsing_Save_Num.adobe_tracking_id
```

### Free-to-Paid Rate
- Percentage of users with a nonpaying status sometime in the reporting period that then possess a paying account status with an entitlement shift sometime in the reporting period
- Num: Users with an entitlement change flag and a paying status in the current month
- Denom: Users with a nonpaying status in the current month
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`

```
WITH 
    Free_To_Paid_Denom AS (
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
SELECT COUNT(DISTINCT Free_To_Paid_Num.adobe_tracking_id) / COUNT(DISTINCT Free_To_Paid_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN Free_To_Paid_Denom
    ON a.adobe_tracking_id = Free_To_Paid_Denom.adobe_tracking_id
LEFT JOIN Free_To_Paid_Num
    ON Free_To_Paid_Denom.adobe_tracking_id = Free_To_Paid_Num.adobe_tracking_id
```

### New Upgrade Rate
- Percentage of users who had never paid before who shifting to a paying status sometime in the reporting period
- Num: Users with their first change to paying status in the current month
- Denom: Users with no history of a paying status
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`, `upgrade_date_rank` (See note)

```
WITH 
    Net_New_Upgrade_Denom AS (
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
SELECT COUNT(DISTINCT Net_New_Upgrade_Num.adobe_tracking_id) / COUNT(DISTINCT Net_New_Upgrade_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN Net_New_Upgrade_Denom
    ON a.adobe_tracking_id = Net_New_Upgrade_Denom.adobe_tracking_id
LEFT JOIN Net_New_Upgrade_Num
    ON Net_New_Upgrade_Denom.adobe_tracking_id = Net_New_Upgrade_Num.adobe_tracking_id
```

### Paid Winback Rate
- Percentage of users who had paid before but with a nonpaying status in the current month that have no shifted to a paying status sometime in the reporting month
- Num: Users with a paying status in the current month who had a nonpaying status in the currrent month and previously been paying sometime in their history
- Denom: Users with a nonpaying status in the current month and previously been paying sometime in their history
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`, `upgrade_date_rank` (See note)
```
WITH 
    Paid_Winbacks_Denom AS (
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
SELECT COUNT(DISTINCT Paid_Winbacks_Num.adobe_tracking_id) / COUNT(DISTINCT Paid_Winbacks_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN Paid_Winbacks_Denom
    ON a.adobe_tracking_id = Paid_Winbacks_Denom.adobe_tracking_id
LEFT JOIN Paid_Winbacks_Num
    ON Paid_Winbacks_Denom.adobe_tracking_id = Paid_Winbacks_Num.adobe_tracking_id
```

### Cancellation Save Rate (prev. Paid Churn Save Rate)
- Percentage of users who turned off auto renew sometime in the reporting period who then turn it back on
- Num: Users with a paying account and auto renew turned on who had previously had a paying account and auto renew off in the current month
- Denom: Users with a paying account and auto renew off in the current month
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`

```
WITH 
    Cancel_Save_Denom AS (
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
SELECT COUNT(DISTINCT Cancel_Save_Num.adobe_tracking_id) / COUNT(DISTINCT Cancel_Save_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN Cancel_Save_Denom
    ON a.adobe_tracking_id = Cancel_Save_Denom.adobe_tracking_id
LEFT JOIN Cancel_Save_Num
    ON Cancel_Save_Denom.adobe_tracking_id = Cancel_Save_Num.adobe_tracking_id
```

### Paid Churn Rate
- Percentage of users who have churned sometime in the reporting period who had been paying 
- Num: Users with a paid flag and a churn flag at the end of the month
- Denom: User with a paid flag at the end of the month
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_CHURN`

```
WITH 
    EOM_Paid_Churn_Denom AS  (
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
SELECT COUNT(DISTINCT EOM_Paid_Churn_Num.adobe_tracking_id) / COUNT(DISTINCT EOM_Paid_Churn_Denom.adobe_tracking_id)
FROM audience a 
LEFT JOIN EOM_Paid_Churn_Denom
    ON a.adobe_tracking_id = EOM_Paid_Churn_Denom.adobe_tracking_id
LEFT JOIN EOM_Paid_Churn_Num
    ON EOM_Paid_Churn_Denom.adobe_tracking_id = EOM_Paid_Churn_Num.adobe_tracking_id
```

### Notes:
- `upgrade_date_rank` table defined as 

```
CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.upgrade_date_rank` AS (

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
                ORDER BY 1,2
            )
    WHERE paying_account_flag_today = 'Paying' AND paying_account_flag_yestd = 'NonPaying'

);
```