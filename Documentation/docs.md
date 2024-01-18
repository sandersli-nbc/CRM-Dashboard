# KPIs

- `{audience}` table refers to group of qualified users in email/push measurement
    - Email (`Silver_Email_Monthly.sql`)
        - Targeted must have have received an email in the given month (excludes transactional emails)
        - Holdout must be in email holdout, as well as have received an email 4 months before the start of the relevant holdout period or be a new user registered 4 months before the start of the relevant holdout period
        - In both targeted and holdout, users must have viewed something on Peacock before and must not be unsubscribed from email before the end of the measurement month to qualify for measurement.
    - Push (`Silver_Push_Monthly.sql`)
        - Targeted must have received a push notification in the given month
        - Holdout must be in push holdout
        - In both targeted and holdout, users must have viewed something on Peacock before and must have received a monthly webhook sent after the end of each month to qualify for measurement.
- `{viewing}` table defines viewership metrics of all users (`CRM_Viewing.sql`)
    - Includes any user in measurement audience who viewed content in the given month
    - Excludes ad views and autoplay trailers
- All divisions are `safe_divide`, but are excluded from logic for clarity
- All measures assume distinct adobe_tracking_id, but `DISTINCT` keyword is added for clarity
- Calculated between `report_start_date` and `report_end_date`, the start and end of the report month

### Return Rate
- Percentage of the audience with a video watch activity sometime in the reporting period.
- Num: Users with video watch activity
- Denom: Audience Size
- Dependencies: `Video_Viewing`

```
SELECT COUNT(DISTINCT v.adobe_tracking_id) / COUNT(DISTINCT a.adobe_tracking_id)
FROM {audience} a
LEFT JOIN {viewing} v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Usage
- Average viewing time of all users in the audience
- Num: Sum of all user viewing time
- Denom: Audience Size
- Dependencies: `Video_Viewing`

```
SELECT SUM(v.Viewing_Time) / COUNT(DISTINCT a.adobe_tracking_id)
FROM {audience} a
LEFT JOIN {viewing} v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Repertoire
- Average repertoire of all users in the audience
- Num: Sum of all user repertoire
- Denom: Audience Size
- Dependencies: `Video_Viewing`

```
SELECT SUM(v.Repertoire_Pavo_Method) / COUNT(DISTINCT a.adobe_tracking_id)
FROM {audience} a
LEFT JOIN {viewing} v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Viewing Sessions
- Average number of distinct viewing sessions of all users in the audience
- Num: Sum of all distinct user viewing sessions
- Denom: Audience Size
- Dependencies: `Video_Viewing`

```
SELECT SUM(v.Distinct_Viewing_Sessions) / COUNT(DISTINCT a.adobe_tracking_id)
FROM {audience} a
LEFT JOIN {viewing} v
ON a.adobe_tracking_id = v.adobe_tracking_id
```

### Active Days
- Average number of distinct viewing days of all users in the cohort in the reporting period
- Num: Distinct number of active days for each user over the month
- Denom: Audience Size
- Dependencies: `Video_Viewing`

```
WITH cte AS (
  SELECT
    DATE_TRUNC(v.adobe_date, MONTH) AS report_month,
    adobe_tracking_id,
    COUNT(DISTINCT v.adobe_date) AS active_days
  FROM {audience} a
  LEFT JOIN {viewing} v
  ON a.adobe_tracking_id = v.adobe_tracking_id
    AND v.adobe_date BETWEEN report_start_date
    AND report_end_date
  GROUP BY 1,2 
  )
SELECT SUM(active_days) / COUNT(DISTINCT a.adobe_tracking_id)
FROM cte
```

### Lapsed Save Rate
- Percentage of the audience who were past lapsing (last watch date between 30 and 90 days) sometime during the reporting period that had watched something in the reporting period
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
FROM {audience} a 
LEFT JOIN Lapsed_Save_Denom
    ON a.adobe_tracking_id = Lapsed_Save_Denom.adobe_tracking_id
LEFT JOIN Lapsed_Save_Num
    ON Lapsed_Save_Denom.adobe_tracking_id = Lapsed_Save_Num.adobe_tracking_id
```

### Lapsing Save Rate
- Percentage of the audience who were past lapsing (last watch date between 15 and 29 days) sometime during the reporting period that had watched something in the reporting period
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
FROM {audience} a 
LEFT JOIN Lapsing_Save_Denom
    ON a.adobe_tracking_id = Lapsing_Save_Denom.adobe_tracking_id
LEFT JOIN Lapsing_Save_Num
    ON Lapsing_Save_Denom.adobe_tracking_id = Lapsing_Save_Num.adobe_tracking_id
```

### Free-to-Paid Rate
- Percentage of users with a nonpaying status sometime in the reporting period that then possess a paying account status with an entitlement shift sometime in the reporting period
- Num: Users with a paying status in the current month
- Denom: Users with a nonpaying status in the current month
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`

```
WITH 
    Free_To_Paid_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'NonPaying')
        AND (report_date BETWEEN DATE_SUB(report_start_date, INTERVAL 1 DAY) AND report_end_date )
    )
    , Free_To_Paid_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'Paying')
        AND (paying_account_change_flag = 'NonPaying to Paying') 
        AND (report_date BETWEEN report_start_date AND report_end_date)
    )
SELECT COUNT(DISTINCT Free_To_Paid_Num.adobe_tracking_id) / COUNT(DISTINCT Free_To_Paid_Denom.adobe_tracking_id)
FROM {audience} a 
LEFT JOIN Free_To_Paid_Denom
    ON a.adobe_tracking_id = Free_To_Paid_Denom.adobe_tracking_id
LEFT JOIN Free_To_Paid_Num
    ON Free_To_Paid_Denom.adobe_tracking_id = Free_To_Paid_Num.adobe_tracking_id
```

### Net New Upgrade Rate
- Percentage of users who had never paid before who shifting to a paying status sometime in the reporting period
- Num: Users with their first change to paying status in the current month
- Denom: Users with no history of a paying status
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`

```
WITH 
    Net_New_Upgrade_Denom AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
        WHERE (paying_account_flag = 'NonPaying')
        AND (first_paying_date IS NULL)
        AND (report_date BETWEEN DATE_SUB(report_start_date, INTERVAL 1 DAY) AND report_end_date) 
    )
    , Net_New_Upgrade_Num AS (
        SELECT  DISTINCT adobe_tracking_id
        FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
        WHERE (paying_account_flag = 'Paying')
        AND (paying_account_change_flag = 'NonPaying to Paying')
        AND (first_paying_date = last_paid_date)
        AND (report_date BETWEEN report_start_date AND report_end_date) 
    )
SELECT COUNT(DISTINCT Net_New_Upgrade_Num.adobe_tracking_id) / COUNT(DISTINCT Net_New_Upgrade_Denom.adobe_tracking_id)
FROM {audience} a 
LEFT JOIN Net_New_Upgrade_Denom
    ON a.adobe_tracking_id = Net_New_Upgrade_Denom.adobe_tracking_id
LEFT JOIN Net_New_Upgrade_Num
    ON Net_New_Upgrade_Denom.adobe_tracking_id = Net_New_Upgrade_Num.adobe_tracking_id
```

### Paid Winback Rate
- Percentage of users who had paid before but with a nonpaying status in the current month that have no shifted to a paying status sometime in the reporting month
- Num: Users converting from Free-to-Paid and previously been paying sometime in their history
- Denom: Users with a nonpaying status in the current month and previously been paying sometime in their history
- Dependencies: `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER`
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
            AND report_date BETWEEN DATE_SUB(report_start_date, INTERVAL 1 DAY) AND report_end_date 
        )
        WHERE adobe_tracking_id NOT IN ( 
            SELECT DISTINCT adobe_tracking_id 
            FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_USER` 
            WHERE (first_paying_date IS NULL)
            AND (report_date BETWEEN DATE_SUB(report_start_date, INTERVAL 1 DAY) AND report_end_date)
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
SELECT COUNT(DISTINCT Paid_Winbacks_Num.adobe_tracking_id) / COUNT(DISTINCT Paid_Winbacks_Denom.adobe_tracking_id)
FROM {audience} a 
LEFT JOIN Paid_Winbacks_Denom
    ON a.adobe_tracking_id = Paid_Winbacks_Denom.adobe_tracking_id
LEFT JOIN Paid_Winbacks_Num
    ON Paid_Winbacks_Denom.adobe_tracking_id = Paid_Winbacks_Num.adobe_tracking_id
```

### Cancellation Save Rate
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
FROM {audience} a 
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
FROM {audience} a 
LEFT JOIN EOM_Paid_Churn_Denom
    ON a.adobe_tracking_id = EOM_Paid_Churn_Denom.adobe_tracking_id
LEFT JOIN EOM_Paid_Churn_Num
    ON EOM_Paid_Churn_Denom.adobe_tracking_id = EOM_Paid_Churn_Num.adobe_tracking_id
```