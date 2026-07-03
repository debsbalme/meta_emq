CREATE OR REPLACE TABLE `{bq_project}.{bq_dataset}.pixel_daily_event_agg` AS
SELECT 
  *,
  LAG(event_count_total) OVER (
    PARTITION BY pixel_id, event_name 
    ORDER BY reportingdate
  ) AS prev_day_event_volume,

  event_count_total - LAG(event_count_total) OVER (
    PARTITION BY pixel_id, event_name 
    ORDER BY reportingdate
  ) AS event_volume_daily_diff,

  SAFE_DIVIDE(
    event_count_total - LAG(event_count_total) OVER (
      PARTITION BY pixel_id, event_name 
      ORDER BY reportingdate
    ),
    LAG(event_count_total) OVER (
      PARTITION BY pixel_id, event_name 
      ORDER BY reportingdate
    )
  ) AS event_volume_pct_diff

FROM (
  SELECT 
    pixel_id,
    event_name,
    DATE(start_time) AS reportingdate,
    SUM(event_count_total) AS event_count_total,
    SUM(event_count_server) AS event_count_server,
    SUM(event_count_web) AS event_count_web
  FROM (
    SELECT DISTINCT * 
    FROM `{bq_project}.{bq_dataset}.pixel_event_volume`
  )
  GROUP BY 1,2,3

  UNION ALL

  SELECT 
    pixel_id,
    'TotalEvents' AS event_name,
    DATE(start_time) AS reportingdate,
    SUM(event_count_total) AS event_count_total,
    SUM(event_count_server) AS event_count_server,
    SUM(event_count_web) AS event_count_web
  FROM (
    SELECT DISTINCT * 
    FROM `{bq_project}.{bq_dataset}.pixel_event_volume`
  )
  GROUP BY 1,2,3
);

CREATE OR REPLACE TABLE `{bq_project}.{bq_dataset}.business_pixels_agg` AS
SELECT 
  pix.*,
  lkup.Lead_Agency,
  lkup.Category,
  lkup.Brand
FROM `{bq_project}.{bq_dataset}.business_pixels` pix
LEFT JOIN (
  SELECT 
    account_id, 
    Lead_Agency, 
    Category, 
    Brand 
  FROM `{lookup_project}.{lookup_dataset}.scoe_lookup`
  WHERE platform = 'Meta'
) lkup
ON pix.owner_ad_account_id = lkup.account_id;

CREATE OR REPLACE TABLE `{bq_project}.{bq_dataset}.pixel_daily_spend_agg` AS
SELECT 
  adsp.*,
  pxl.*
FROM `{lookup_project}.{lookup_dataset}.meta_signals_spend` adsp
INNER JOIN `{bq_project}.{bq_dataset}.business_pixels` pxl
ON CAST(SUBSTR(pxl.owner_ad_account_id, 5) AS INT64) = adsp.account_id;



CREATE OR REPLACE TABLE `{bq_project}.{bq_dataset}.meta_signals.pixel_stats_calc`
AS 
SELECT grp.*, 
 (ACX_CAPI_SCORE+ACX_COVERAGE_SCORE+ACX_EMQ_SCORE+ACX_FRESHNESS_SCORE) as ACX_MATURITY_SCORE
FROM
(SELECT
 dsq.* EXCEPT (data_added, event_name, pixel_id),
  evagg.*,
  CASE WHEN event_count_server > 0 then "CAPI CONNECTED" else "NO CAPI CONNECTED" end as ACX_CAPI_STATUS,
  CASE WHEN coverage_pct  is null then "NO COVERAGE" 
    WHEN coverage_pct > 0 and coverage_pct <=33 then  "LOW 0-33%"
    WHEN coverage_pct > 33 and coverage_pct <=66 then  "MID 33%-66%"
    WHEN  coverage_pct > 66 then  "HIGH 66%-100%"
   END AS ACX_COVERAGE_GROUP,
  CASE WHEN emq_composite_score is null then "NO EMQ"
    WHEN emq_composite_score > 0 and emq_composite_score <=4 then  "LOW 0-3"
    WHEN emq_composite_score > 4 and emq_composite_score <=7 then  "MID 4-6"
    WHEN  emq_composite_score > 7 then  "HIGH 7+"   
  END AS ACX_EMQ_GROUP,
  freshness_upload_frequency as ACX_FRESHNESS_GROUP,

  CASE WHEN event_count_server > 0 then 1 else 0 end as ACX_CAPI_SCORE,
  CASE WHEN coverage_pct  is null then 0
   WHEN coverage_pct =0 then 0
    WHEN coverage_pct > 0 and coverage_pct <=33 then  1
    WHEN coverage_pct > 33 and coverage_pct <=66 then  2
    WHEN  coverage_pct > 66 then 3
   END AS ACX_COVERAGE_SCORE,
  CASE WHEN emq_composite_score is null then  0
   WHEN emq_composite_score =0 then 0
    WHEN emq_composite_score > 0 and emq_composite_score <=4 then  1
    WHEN emq_composite_score > 4 and emq_composite_score <=7 then  2
    WHEN  emq_composite_score > 7 then  3
  END AS ACX_EMQ_SCORE,
 CASE WHEN freshness_upload_frequency is null then 0
  WHEN freshness_upload_frequency = "weekly" then 1
  WHEN freshness_upload_frequency = "daily" then 2
  WHEN freshness_upload_frequency = "hourly" then 3
  WHEN freshness_upload_frequency = "real_time" then 4
   END AS ACX_FRESHNESS_SCORE
  

 from `{lookup_project}.{lookup_dataset}.pixel_daily_event_agg` evagg
 LEFT OUTER JOIN 
  `{lookup_project}.{lookup_dataset}.pixel_dataset_quality` dsq
ON 
 evagg.pixel_id = dsq.pixel_id and
 evagg.reportingdate = CAST(dsq.data_added as DATE) and
  evagg.event_name = dsq.event_name) grp

