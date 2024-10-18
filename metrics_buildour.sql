-- GMV2 & SI T1W 
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
    ck.SLR_ID,
    ck.EU_B2C_C2C_FLAG,
    SUM(CK.gmv20_plan) AS GMV,
    SUM(CK.gmv20_sold_quantity) AS SI
FROM ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck ON ck.GMV_DT = CAL.CAL_DT
LEFT JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = ck.LEAF_CATEG_ID AND CAT.site_id = ck.lstg_site_id
WHERE
---    CAL.AGE_FOR_WEEK_ID >= -53 AND CAL.AGE_FOR_WEEK_ID <= -1 AND
    ck.SLR_CNTRY_ID = 3 AND
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2022 AND
	--CK.GMV_DT >= '2022-01-01' AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND
    CK.ISCORE = 1 -- 
GROUP BY
    1, 2, 3, 4
ORDER BY
	2 ASC,1 ASC, 3 ASC
	LIMIT 20;

-- GMV2 & SI T52W 
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
    ck.SLR_ID,
    ck.EU_B2C_C2C_FLAG,
    SUM(CK.gmv20_plan) AS GMV,
    SUM(CK.gmv20_sold_quantity) AS SI
FROM ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON DATEDIFF(WEEK, CAL.RETAIL_WK_END_DATE, CK.GMV_DT) >= 0 AND DATEDIFF(WEEK, CAL.RETAIL_WK_END_DATE, CK.GMV_DT) < 53 
--ON ck.GMV_DT = CAL.CAL_DT
LEFT JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = ck.LEAF_CATEG_ID AND CAT.site_id = ck.lstg_site_id
WHERE
---    CAL.AGE_FOR_WEEK_ID >= -53 AND CAL.AGE_FOR_WEEK_ID <= -1 AND
    ck.SLR_CNTRY_ID = 3 AND
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2022 AND
	--CK.GMV_DT >= '2022-01-01' AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND
    CK.ISCORE = 1 -- 
GROUP BY
    1, 2, 3, 4
ORDER BY
	2 ASC,1 ASC, 3 ASC
	LIMIT 20;
--- current date CAL.AGE_FOR_DT_ID = -1

--- T1W LL
--- Need to add UK & B2C filters
SELECT
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  I.SLR_ID,
  COUNT(1) AS LL
FROM
  ACCESS_VIEWS.DW_LSTG_ITEM I
  INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT BETWEEN I.AUCT_START_DT
  AND I.AUCT_END_DT
  AND CAL.AGE_FOR_DT_ID = -1
  INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
  AND CAT.SITE_ID = I.ITEM_SITE_ID
  AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  where 
  ---I.auct_end_dt >= '2022-01-01'
  CAL.RETAIL_WEEK >= 1
  AND CAL.RETAIL_YEAR >= 2022
  AND I.wacko_yn = 'N'
  group by 1,2,3
  ORDER BY 2 ASC, 1 ASC, 3
  limit 20;
  
  --- CBT GMC calculation
  
SELECT
ck.BYR_CNTRY_ID byrcntry,
ck.SLR_CNTRY_ID slrcntry,
ck.created_dt,
(CASE WHEN ck.BYR_CNTRY_ID <> ck.SLR_CNTRY_ID THEN 1 ELSE 0 END) CBT,
SUM(gmv_plan_usd) as gmv
FROM DW_CHECKOUT_TRANS ck
INNER JOIN DW_CATEGORY_GROUPINGS ON (DW_CATEGORY_GROUPINGS.LEAF_CATEG_ID=ck.LEAF_CATEG_ID AND DW_CATEGORY_GROUPINGS.SITE_ID=ck.SITE_ID)
WHERE
 ck.sale_type NOT IN (12,15)
AND ck.ck_wacko_yn ='N'
AND ck.SLR_CNTRY_ID = 3
AND NOT ((DW_CATEGORY_GROUPINGS.USER_DEFINED_FIELD1 IN (4)
 AND DW_CATEGORY_GROUPINGS.CATEG_LVL3_ID IN (91952,25249,67145,92035,97185,92080,92082,26247))
 OR DW_CATEGORY_GROUPINGS.USER_DEFINED_FIELD1 IN (5,7,41, 23))
GROUP BY 1,2,3,4
limit 20;
  
 select *
 from DW_CHECKOUT_TRANS ck
 limit 20
  
select *
FROM ACCESS_VIEWS.ebay_trans_rltd_event
limit 10

 --- defect metrics, needs to be filtered on B2C
select year(TRANS_DT), month(TRANS_DT), SLR_ID,   
sum(ESC_SNAD_FLAG) AS escal_SNAD_count,
sum(case when (OPEN_SNAD_FLAG + RTRN_SNAD_FLAG) > 0 then 1 else 0 end) as non_escal_SNAD_count,
sum(STOCKOUT_FLAG) as STOCKOUT_count,
sum(ESC_INR_FLAG) as escal_INR_count,
sum(OPEN_INR_FLAG) as non_escal_INR_count,
sum(LOW_DSR_IAD_FLAG) as low_IAD_DSR_count,
sum(BYR_TO_SLR_NN_FLAG) as NN_feedback_count,
sum(SNAD_MSG_FLAG) as Non_escal_SNAD_count,
sum(INR_MSG_FLAG) as Non_escal_INR_count
from ACCESS_VIEWS.ebay_trans_rltd_event
where SLR_CNTRY_ID = 3
and TRANS_DT >= '2022-01-01'
AND core_categ_ind = 1
AND ! auct_type_code IN(10, 12, 15)
AND ck_wacko_yn_ind = 'N'
AND rprtd_wacko_yn_ind = 'N'
group by 1, 2, 3
order by 3, 1, 2
limit 50

--- seller standard
SELECT
  USER_ID,
  LAST_EVAL_DT,
  Case
    when SPS_SLR_LEVEL_CD = 1 then 'ETRS'
    when SPS_SLR_LEVEL_CD = 2 then 'ASTD'
    when SPS_SLR_LEVEL_CD = 4 then 'BSTD'
  end as Seller_Rating
FROM
  PRS_RESTRICTED_V.SPS_LEVEL SPS
WHERE
  SPS_EVAL_TYPE_CD = 3 ---- trending Standard code
  AND sps_prgrm_id = 3
ORDER BY 1,2
 limit 100
--   QUALIFY ROW_NUMBER() OVER (
--     PARTITION BY USER_ID,
--     SPS_PRGRM_ID
--     ORDER BY LAST_EVAL_DT DESC
--   ) = 1;


SELECT *
FROM PRS_RESTRICTED_V.SPS_LEVEL
LIMIT 29

--- STORE ATTRIBUTE KPIS
--- USE BEG_DT & END_DT FOR FILTERING
SELECT YEAR(BEG_DT) AS YR,
	   MONTH(BEG_DT) AS MN,
	   SLR_ID, 
	   COUNT(CASE WHEN PROD_ID = 101 THEN 1 ELSE NULL END) AS STARTER_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 3 THEN 1 ELSE NULL END) AS BASIC_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 4 THEN 1 ELSE NULL END) AS FEATURE_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 5 THEN 1 ELSE NULL END) AS ANCHOR_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 102 THEN 1 ELSE NULL END) AS ENTERPRISE_STORE_COUNT
FROM ACCESS_VIEWS.DW_STORE_ATTR_HIST
GROUP BY 1,2,3
ORDER BY 3,1,2
LIMIT 100

--- AVERAGE START PRICE
--- LSTG_STATUS_ID TO FILTER ON LIVE LISTING ETC
SELECT SLR_ID,
	   AVG((START_PRICE_LSTG_CURNCY_AMT*CURNCY_PLAN_RATE)/QTY_AVAIL)
FROM PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT
WHERE SLR_CNTRY_ID = 3
AND ISCORE = 1
GROUP BY 1
ORDER BY 1 DESC
LIMIT 100

---- LIVE LISTING

SELECT
  year(cal.CAL_DT) yr,
  month(cal.CAL_DT) mon,
   I.SLR_ID,
  COUNT(1) AS LL
FROM
  ACCESS_VIEWS.DW_LSTG_ITEM I
  INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT BETWEEN I.AUCT_START_DT
  AND I.AUCT_END_DT
  AND CAL.AGE_FOR_DT_ID = -1
  INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
  AND CAT.SITE_ID = I.ITEM_SITE_ID
  AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  where I.auct_end_dt >= '2022-01-01'
  AND I.LSTG_STATUS_ID = 0 ---LIVE LISTING FILTER
  AND I.wacko_yn = 'N'
  group by 1, 2, 3
  limit 100;

select *
from P_ZETA_AUTOETL_V.EP_DRAFT_LSTG_CONV, SRCH_SRP_ITEM_FACT
limit 10

select *
from ACCESS_VIEWS.SLNG_SLR_HUB_SBSCRPTN
limit 30





