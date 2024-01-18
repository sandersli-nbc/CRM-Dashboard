DECLARE report_start_date DATE DEFAULT @report_start_date;
DECLARE report_end_date DATE DEFAULT @report_end_date;

--===================================================================================================================================================
-- Viewing Tables 
--===================================================================================================================================================

CREATE OR REPLACE TABLE `nbcu-ds-sandbox-a-001.SLi_sandbox.Video_Viewing_{report}` AS (

    SELECT  DATE_TRUNC(adobe_date, MONTH) AS Report_Month
        ,adobe_tracking_id
        ,COUNT(DISTINCT CASE WHEN num_views_started = 1 THEN display_name ELSE NULL END) AS Distinct_Content_Starts -- num_views_started is a flag
        -- Sunset num_views_started field starting Q4 2023
        ,SUM(num_views_started) AS Total_Content_Starts
        ,SUM(num_seconds_played_no_ads)/3600                                                  AS Viewing_Time
        ,COUNT(DISTINCT CASE WHEN num_views_started = 1 THEN session_id ELSE NULL END)        AS Distinct_Viewing_Sessions
        ,COUNT(DISTINCT CASE WHEN ((num_views_started = 1) 
                              AND (num_seconds_played_no_ads > CASE WHEN (LOWER(consumption_type) = 'virtual channel') 
                                                                    THEN 299 
                                                                    ELSE 0 
                                                               END
                                  )
                             )
                             THEN CASE WHEN ((COALESCE(stream_type, 'NULL') = 'trailer') 
                                         OR (consumption_type = 'Shortform')) THEN 'Shortform'
                                       WHEN (LOWER(franchise) != 'other') THEN franchise 
                                       ELSE display_name 
                                 END
                        ELSE NULL
                        END
         ) AS Repertoire_Pavo_Method
        ,COUNT(DISTINCT adobe_date)                                                             AS active_days
    FROM `nbcu-ds-prod-001.PeacockDataMartSilver.SILVER_VIDEO`
    WHERE (adobe_date BETWEEN report_start_date AND report_end_date)
    AND (num_views_started = 1)
    AND NOT(
        COALESCE(stream_type, "NULL") = 'trailer' 
        AND (
            COALESCE(lower(vdo_initiate), "NULL") like ('%auto%play%') 
            OR 
            (DATE(adobe_date) >= '2023-03-01' AND COALESCE(LOWER(vdo_initiate), "NULL") = 'n/a')
        )
    )
    /* -- future logic, manual trailers only 
    WHERE (adobe_date BETWEEN report_start_date AND report_end_date)
    AND (num_seconds_played_no_ads > 0)
    AND (num_views_started = 1)
    AND (
        -- Not a trailer
        (COALESCE(stream_type,"NULL") != 'trailer') 
        OR
        -- If a trailer, must be manual
        ((COALESCE(stream_type,"NULL") = 'trailer') AND (COALESCE(LOWER(vdo_initiate),"NULL") LIKE ('%manual%')))
    )
    */
    GROUP BY 1,2

);
