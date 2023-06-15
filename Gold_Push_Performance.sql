CREATE or replace TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.GOLD_PUSH_CHANNEL_PERFORMANCE` AS
SELECT  Result_Type

       ,Report_Month
       ,Cohort

       ,Account_Type
       ,Active_Viewer
       ,Primary_Device
       ,Account_Tenure
       ,Paid_Tenure
       ,Billing_Platform
       ,Bundling_Partner
       ,Billing_Cycle_Category
       ,Offer
       ,Churn_Frequency
       ,Previously_Bundled
       ,Intender_Audience
       ,Genre
       ,Network

       ,COUNT(DISTINCT aid)            AS Users
       ,SUM(Viewer)                    AS Viewers
       ,SUM(Viewing_Time)              AS Viewing_Time
       ,SUM(Repertoire_Pavo_Method)    AS Repertoire
       ,SUM(Distinct_Viewing_Sessions) AS Viewing_Sessions
       ,SUM(Lapsed_Save_Denom)         AS Lapsed_Save_Denom
       ,SUM(Lapsed_Save_Num)           AS Lapsed_Save_Num
       ,SUM(Lapsing_Save_Denom)        AS Lapsing_Save_Denom
       ,SUM(Lapsing_Save_Num)          AS Lapsing_Save_Num
       ,SUM(Free_To_Paid_Denom)        AS Free_To_Paid_Denom
       ,SUM(Free_To_Paid_Num)          AS Free_To_Paid_Num
       ,SUM(Net_New_Upgrade_Denom)     AS Net_New_Upgrade_Denom
       ,SUM(Net_New_Upgrade_Num)       AS Net_New_Upgrade_Num
       ,SUM(Paid_Winbacks_Denom)       AS Paid_Winbacks_Denom
       ,SUM(Paid_Winbacks_Num)         AS Paid_Winbacks_Num
       ,SUM(Cancel_Save_Denom)         AS Cancel_Save_Denom
       ,SUM(Cancel_Save_Num)           AS Cancel_Save_Num
       ,SUM(EOM_Paid_Churn_Denom)      AS EOM_Paid_Churn_Denom
       ,SUM(EOM_Paid_Churn_Num)        AS EOM_Paid_Churn_Num
FROM (
       SELECT  'Monthly' AS Result_Type
              ,*
       FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.SILVER_PUSH_CHANNEL_PERFORMANCE_MONTHLY`
       UNION ALL
       SELECT  'Quarterly' AS Result_Type
              ,*
       FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.SILVER_PUSH_CHANNEL_PERFORMANCE_QUARTERLY`
)
GROUP BY  Result_Type
         ,Report_Month
         ,Cohort
         ,Account_Type
         ,Active_Viewer
         ,Primary_Device
         ,Account_Tenure
         ,Paid_Tenure
         ,Billing_Platform
         ,Bundling_Partner
         ,Billing_Cycle_Category
         ,Offer
         ,Churn_Frequency
         ,Previously_Bundled
         ,Intender_Audience
         ,Genre
         ,Network      