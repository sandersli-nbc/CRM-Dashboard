DECLARE report_start_date DATE DEFAULT @report_start_date;
DECLARE report_end_date DATE DEFAULT @report_end_date;

--===================================================================================================================================================
-- Viewing Tables 
--===================================================================================================================================================

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Video_Viewing_{report}` AS (

    SELECT  adobe_tracking_id
        ,COUNT(DISTINCT CASE WHEN num_views_started = 1 THEN adobe_tracking_id ELSE NULL END) AS Distinct_Content_Starts -- num_views_started is a flag
        ,SUM(num_views_started)                                                               AS Total_Content_Starts
        ,SUM(num_seconds_played_no_ads)/3600                                                  AS Viewing_Time
        ,COUNT(DISTINCT CASE WHEN num_views_started = 1 THEN session_id ELSE NULL END)        AS Distinct_Viewing_Sessions
        ,COUNT(DISTINCT CASE WHEN ((num_views_started > 0) AND (num_seconds_played_no_ads > CASE WHEN (LOWER(consumption_type) = 'virtual channel') THEN 299 ELSE 0 END))
                             THEN CASE WHEN ((COALESCE(stream_type, 'NULL') = 'trailer') OR (LOWER(consumption_type) = 'shortform')) THEN 'Shortform'
                                         WHEN (LOWER(franchise) != 'other') THEN franchise 
                                         ELSE display_name 
                                 END
                        ELSE NULL
                        END
         ) AS Repertoire_Pavo_Method
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
    WHERE (adobe_date BETWEEN report_start_date AND report_end_date)
    AND (num_views_started = 1)
    AND NOT(
        (COALESCE(stream_type,"NULL") = 'trailer') 
        AND (
            (COALESCE(LOWER(vdo_initiate),"NULL") LIKE ('%auto%play%'))
            OR 
            (COALESCE(LOWER(vdo_initiate),"NULL") = 'n/a')
        )
    )
    GROUP BY 1

);