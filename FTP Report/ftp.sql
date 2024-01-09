WITH CTE_1 AS
(
	SELECT  Report_Month
	       ,COUNT(distinct CASE WHEN cohort = 'Targeted' THEN aid END )     AS Distinct_Cohort_Size_Targeted
	       ,COUNT(distinct CASE WHEN cohort = 'Holdout' THEN aid END)       AS Distinct_Cohort_Size_Holdout
	       ,SUM(CASE WHEN cohort = 'Targeted' THEN Free_To_Paid_Denom END ) AS Free_To_Paid_Denom_Targeted
	       ,SUM(CASE WHEN cohort = 'Targeted' THEN Free_To_Paid_Num END)    AS Free_To_Paid_Num_Targeted
	       ,SUM(CASE WHEN cohort = 'Holdout' THEN Free_To_Paid_Denom END )  AS Free_To_Paid_Denom_Holdout
	       ,SUM(CASE WHEN cohort = 'Holdout' THEN Free_To_Paid_Num END)     AS Free_To_Paid_Num_Holdout
	FROM `nbcu-ds-sandbox-a-001.SLi_sandbox.SILVER_EMAIL_CHANNEL_PERFORMANCE_MONTHLY`
	GROUP BY  1
), CTE_2 AS
(
	SELECT  Report_Month
	       ,Distinct_Cohort_Size_Targeted                                      AS Total_Targeted
	       ,Distinct_Cohort_Size_Holdout                                       AS Total_Holdout
	       ,Free_To_Paid_Denom_Targeted
	       ,Free_To_Paid_Num_Targeted
	       ,safe_divide(Free_To_Paid_Num_Targeted,Free_To_Paid_Denom_Targeted) AS Free_To_Paid_Rate_Engagers
	       ,safe_divide(Free_To_Paid_Num_Holdout,Free_To_Paid_Denom_Holdout)   AS Free_To_Paid_Rate_Holdout
	FROM CTE_1
)
SELECT  Report_Month
       ,Total_Targeted
       ,Total_Holdout
       ,Free_To_Paid_Rate_Engagers                                                             AS Free_To_Paid_Rate_Engagers
       ,Free_To_Paid_Rate_Holdout                                                              AS Free_To_Paid_Rate_Holdout
       ,Free_To_Paid_Rate_Engagers - Free_To_Paid_Rate_Holdout                                 AS Free_To_Paid_Rate_Lift_PTS
       ,safe_divide(Free_To_Paid_Rate_Engagers,Free_To_Paid_Rate_Holdout) *100                 AS Free_To_Paid_Rate_Lift_Index
       ,(Free_To_Paid_Rate_Engagers - Free_To_Paid_Rate_Holdout) * Free_To_Paid_Denom_Targeted AS Free_To_Paid_Incrementals
FROM CTE_2
ORDER BY 1