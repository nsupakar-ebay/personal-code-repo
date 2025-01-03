----US

---- T52_CAL 

DROP TABLE IF EXISTS CAL_52W;
CREATE TEMP TABLE CAL_52W AS
  SELECT
  retail_year
   ,retail_week
   ,rtl_week_beg_dt
   ,retail_wk_end_date
   ,date_sub(retail_wk_end_date,363) as retail_week_b4_52w_dt
   ,date_sub(retail_wk_end_date,90) as retail_week_b4_13w_dt
 FROM ACCESS_VIEWS.DW_CAL_DT
 WHERE age_for_rtl_week_iD<-1
 AND RETAIL_YEAR>=2022
 GROUP BY 1,2,3,4,5;	
 
 

 --- list of all active sellers
 
drop table if exists P_nishant_local_T.usr_cross_new_us; 
CREATE TABLE P_nishant_local_T.usr_cross_new_us AS
SELECT distinct CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.RTL_WEEK_BEG_DT,
	cal.AGE_FOR_RTL_WEEK_ID,
	cal.RETAIL_WK_END_DATE,
    ck.SLR_ID
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
cross JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
WHERE
    ck.SLR_CNTRY_ID = 1 AND 
    UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR') and
	CAL.RETAIL_YEAR >= 2022
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
	and ck.gmv_dt >='2021-11-01'
order by 5,2,1;

---CASE WHEN  UPPER(LSTG.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR') 
--- new GMV 
drop table if exists p_NISHANT_LOCAL_T.ALL_B2C_DAILY_us;
create table p_NISHANT_LOCAL_T.ALL_B2C_DAILY_us as
SELECT
	cal.retail_year,
	cal.retail_week,
	cal.RETAIL_WK_END_DATE,
	CK.GMV_DT,
	CK.SLR_ID,
	B.FIRST_SELL_DT,
	SUM(gmv20_plan) AS GMV,
	SUM(gmv20_sold_quantity) AS SI,
	SUM(CASE WHEN ck.BYR_CNTRY_ID <> CK.SLR_CNTRY_ID THEN CK.gmv20_plan ELSE 0 END) AS CBT_GMV,
    SUM(CASE WHEN ck.BYR_CNTRY_ID <> CK.SLR_CNTRY_ID THEN CK.gmv20_sold_quantity ELSE 0 END) AS CBT_SI
FROM PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT CK	
LEFT JOIN (
		SELECT
		CK.SLR_ID,
		MIN(GMV_DT) AS FIRST_SELL_DT
		FROM PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT CK
		INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = CK.GMV_DT
		WHERE 1=1
			AND age_for_rtl_week_id<0
			AND CK.SLR_CNTRY_ID = 1 --
			AND CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3)
			AND CK.ISCORE = 1 -- CORE ONLY 	
			AND CK.EU_B2C_C2C_FLAG =  'B2C'
			AND CK.CK_WACKO_YN = 'N'
		GROUP BY 1) B
	ON CK.SLR_ID=B.SLR_ID
INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = CK.GMV_DT
WHERE 1=1
    and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR') 
	AND age_for_rtl_week_id<0
	AND RETAIL_YEAR>=2020
	AND CK.AUCT_END_DT>='2016-01-01'
	AND CK.SLR_CNTRY_ID = 1 --DE
	AND CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3)
	AND CK.ISCORE = 1 -- CORE ONLY 	
    AND CK.EU_B2C_C2C_FLAG =  'B2C'
	AND CK.CK_WACKO_YN = 'N'
GROUP BY 1,2,3,4,5,6;

--- new t52w gmv
DROP TABLE IF EXISTS P_nishant_local_t.GMV_T52_METRICS_us;
CREATE TABLE P_nishant_local_t.GMV_T52_METRICS_us
SELECT
	A.retail_year,
	A.retail_week,
	A.RETAIL_WK_END_DATE,
	A.SLR_ID,
	SUM(A.GMV) AS T52W_GMV,
	SUM(A.SI) AS T52W_SI,
	Sum(a.CBT_GMV) as T52W_CBT_GMV,
	SUM(A.CBT_SI) AS T52W_CBT_SI
FROM
(SELECT
	B.retail_year,
	B.retail_week,
	B.RETAIL_WK_END_DATE,
	SLR_ID,
	 SUM(coalesce(gmv,0)) AS GMV,
    SUM(coalesce(si,0)) AS SI,
	SUM(CBT_GMV) AS CBT_GMV,
    SUM(CBT_SI) AS CBT_SI
FROM p_NISHANT_LOCAL_T.ALL_B2C_DAILY_us A
INNER JOIN CAL_52W B on GMV_DT between  b.retail_week_b4_52w_dt and b.retail_wk_end_date
GROUP BY 1,2,3,4) A
where a.retail_year>=2021
GROUP BY 1,2,3,4;



--- new t1w gmv
DROP TABLE IF EXISTS P_nishant_local_t.GMV_T1_METRICS_us;
CREATE TABLE P_nishant_local_t.GMV_T1_METRICS_us
SELECT
	a.retail_year,
	a.retail_week,
	SLR_ID,
	 SUM(coalesce(gmv,0)) AS T1W_GMV,
    SUM(coalesce(si,0)) AS T1W_SI,
	SUM(CBT_GMV) AS T1W_CBT_GMV,
    SUM(CBT_SI) AS T1W_CBT_SI
FROM p_NISHANT_LOCAL_T.ALL_B2C_DAILY_us A
GROUP BY 1,2,3;

--- new t13w gmv
DROP TABLE IF EXISTS P_nishant_local_t.GMV_T13_METRICS_us;
CREATE TABLE P_nishant_local_t.GMV_T13_METRICS_us
SELECT
	A.retail_year,
	A.retail_week,
	A.RETAIL_WK_END_DATE,
	A.SLR_ID,
	SUM(A.GMV) AS T13W_GMV,
	SUM(A.SI) AS T13W_SI,
	Sum(a.CBT_GMV) as T13W_CBT_GMV,
	SUM(A.CBT_SI) AS T13W_CBT_SI
FROM
(SELECT
	B.retail_year,
	B.retail_week,
	B.RETAIL_WK_END_DATE,
	SLR_ID,
	 SUM(coalesce(gmv,0)) AS GMV,
    SUM(coalesce(si,0)) AS SI,
	SUM(CBT_GMV) AS CBT_GMV,
    SUM(CBT_SI) AS CBT_SI
FROM p_NISHANT_LOCAL_T.ALL_B2C_DAILY_us A
INNER JOIN CAL_52W B on GMV_DT between  b.retail_week_b4_13w_dt and b.retail_wk_end_date
GROUP BY 1,2,3,4) A
where a.retail_year>=2021
GROUP BY 1,2,3,4;


-- SELECT  sum(t52w_gmv), sum(t52w_si), count(distinct SLR_ID)
-- from P_nishant_local_t.GMV_T52_METRICS_de
-- where retail_week = 39
-- and retail_year = 2024
-- and t52w_gmv >0

-- t1w_ll
DROP TABLE IF EXISTS P_nishant_local_T.LL_T1_METRICS_us; 
create table P_nishant_local_T.LL_T1_METRICS_us as 
SELECT
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  cal.AGE_FOR_RTL_WEEK_ID,
  CAL.SLR_ID,
  COUNT(distinct I.ITEM_ID) AS T1W_LL
  ,avg(i.START_PRICE_USD) as t1w_avg_start_price,
  count(distinct case when I.RELIST_UP_FLAG = 1 THEN I.ITEM_ID else null end) as T1W_RELIST_LL,
  count(distinct case when FNL_YN_IND  = 1 THEN I.ITEM_ID else null end) as T1W_FNL_LL
FROM
  P_nishant_local_T.usr_cross_new_us CAL
  left JOIN ACCESS_VIEWS.DW_LSTG_ITEM I   ON
  CAL.RETAIL_WK_END_DATE >= I.AUCT_START_DT
  AND CAL.RTL_WEEK_BEG_DT <= I.AUCT_END_DT
  AND I.SLR_ID = CAL.SLR_ID
   INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
   AND CAT.SITE_ID = I.ITEM_SITE_ID
   AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  LEFT JOIN PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT FCT
  ON I.ITEM_ID = FCT.ITEM_ID
  where 
  i.SLR_CNTRY_ID = 1 and
  CAL.RETAIL_YEAR >=2022
  group by 1,2,3,4
  ORDER BY 2 asc, 1 ASC, 3; 
  
 --- t52w live listing
DROP TABLE IF EXISTS P_nishant_local_T.LL_T52_METRICS_us; 
create table P_nishant_local_T.LL_T52_METRICS_us as 
SELECT
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  cal.AGE_FOR_RTL_WEEK_ID,
  CAL.SLR_ID,
  COUNT(distinct I.ITEM_ID) AS T52W_LL
  ,avg(i.START_PRICE_USD) as t52w_avg_start_price,
  count(distinct case when I.RELIST_UP_FLAG = 1 THEN I.ITEM_ID else null end) as T52W_RELIST_LL,
  count(distinct case when FNL_YN_IND  = 1 THEN I.ITEM_ID else null end) as T52W_FNL_LL
FROM
  P_nishant_local_T.usr_cross_new_us CAL
  LEFT JOIN CAL_52W CAL2
  ON CAL2.RETAIL_WEEK = CAL.RETAIL_WEEK
  AND CAL.RETAIL_YEAR = CAL2.RETAIL_YEAR
  left JOIN ACCESS_VIEWS.DW_LSTG_ITEM I   ON
  CAL2.retail_wk_end_date >= I.AUCT_START_DT
  AND CAL2.retail_week_b4_52w_dt <= I.AUCT_END_DT
  AND I.SLR_ID = CAL.SLR_ID
    LEFT JOIN PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT FCT
  ON I.ITEM_ID = FCT.ITEM_ID
   INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
   AND CAT.SITE_ID = I.ITEM_SITE_ID
   AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  where 
  i.SLR_CNTRY_ID = 1 and
  CAL.RETAIL_YEAR >= 2022
  group by 1,2,3,4
  ORDER BY 2 asc, 1 ASC, 3;  

--- T1W defect metrics
DROP TABLE IF EXISTS P_nishant_local_T.T1W_DEFECT_METRICS_us;
CREATE TABLE P_nishant_local_T.T1W_DEFECT_METRICS_us AS
select cal.RETAIL_YEAR, CAL.RETAIL_WEEK,CAL.AGE_FOR_RTL_WEEK_ID, rle.SLR_ID,   
sum(COALESCE(rle.ESC_SNAD_FLAG,0)) AS T1W_escal_SNAD_count,
sum(case when (rle.OPEN_SNAD_FLAG + rle.RTRN_SNAD_FLAG) > 0 then 1 else 0 end) as T1W_non_escal_SNAD_count,
sum(COALESCE(rle.STOCKOUT_FLAG,0)) as T1W_STOCKOUT_count,
sum(COALESCE(rle.ESC_INR_FLAG,0)) as T1W_escal_INR_count,
sum(COALESCE(rle.OPEN_INR_FLAG,0)) as T1W_non_escal_INR_count,
sum(COALESCE(rle.LOW_DSR_IAD_FLAG,0)) as T1W_low_IAD_DSR_count,
sum(COALESCE(rle.BYR_TO_SLR_NN_FLAG,0)) as T1W_NN_feedback_count,
sum(COALESCE(rle.SNAD_MSG_FLAG,0)) as T1W_Non_escal_SNAD_MSG_count,
sum(COALESCE(rle.INR_MSG_FLAG,0)) as T1W_Non_escal_INR_MSG_count
from ACCESS_VIEWS.DW_CAL_DT CAL
left join ACCESS_VIEWS.ebay_trans_rltd_event rle 
on cal.cal_dt = rle.TRANS_DT
where rle.SLR_CNTRY_ID = 1
AND rle.core_categ_ind = 1
AND ! rle.auct_type_code IN(10, 12, 15)
AND rle.ck_wacko_yn_ind = 'N'
AND rle.rprtd_wacko_yn_ind = 'N'
AND CAL.RETAIL_YEAR >= 2021 
AND CAL.AGE_FOR_WEEK_ID <= -1 
and rle.slr_ID in (select distinct slr_id from P_nishant_local_T.usr_cross_new_us)
group by 1, 2, 3, 4
order by 3, 1, 2;

--- T52W defect metrics
--- RR
--- CHECKED FOR DUPLICATES: PASSED
--- NEED TO CHECK FOR ACCURACY
drop table if exists P_nishant_local_T.T52W_DEFECT_METRICS_us; 
CREATE TABLE P_nishant_local_T.T52W_DEFECT_METRICS_us AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(COALESCE(b.T1W_escal_SNAD_count,0)) AS T52W_escal_SNAD_count,
sum(COALESCE(b.T1W_non_escal_SNAD_count,0)) AS T52W_non_escal_SNAD_count,
sum(COALESCE(b.T1W_STOCKOUT_count,0)) AS T52W_STOCKOUT_count,
sum(COALESCE(b.T1W_escal_INR_count,0)) AS T52W_escal_INR_count,
sum(COALESCE(b.T1W_non_escal_INR_count,0)) AS T52W_non_escal_INR_count,
sum(COALESCE(b.T1W_low_IAD_DSR_count,0)) AS T52W_low_IAD_DSR_count,
sum(COALESCE(b.T1W_NN_feedback_count,0)) AS T52W_NN_feedback_count,
sum(COALESCE(b.T1W_Non_escal_SNAD_MSG_count,0)) AS T52W_Non_escal_SNAD_MSG_count,
sum(COALESCE(b.T1W_Non_escal_INR_MSG_count,0)) AS T52W_Non_escal_INR_MSG_count
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.T1W_DEFECT_METRICS_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
GROUP BY 1,2,3,4;


--- LATEST seller standard
drop table if exists P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS_us;
CREATE TABLE P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS_us AS
SELECT RETAIL_WEEK,
       RETAIL_YEAR,
	   USER_ID,
	   Case
    when SPS_SLR_LEVEL_CD = 1 then 'ETRS'
    when SPS_SLR_LEVEL_CD = 2 then 'ASTD'
	when SPS_SLR_LEVEL_CD = 3 then 'STANDARD'
    when SPS_SLR_LEVEL_CD = 4 then 'BSTD'
    end as LATEST_SELLER_RATING  	   
FROM (
SELECT
  cal.slr_id as user_id,
  SPS.LAST_EVAL_DT,
  SPS.SPS_SLR_LEVEL_CD,
  CAL.RETAIL_WK_END_DATE,
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  ROW_NUMBER() OVER(PARTITION BY cal.slr_id, cal.RETAIL_YEAR, cal.RETAIL_week ORDER BY SPS.sps_slr_level_sum_start_dt DESC) AS RNK
-- Case
--     when SPS_SLR_LEVEL_CD = 1 then 'ETRS'
--     when SPS_SLR_LEVEL_CD = 2 then 'ASTD'
--     when SPS_SLR_LEVEL_CD = 4 then 'BSTD'
--   end as Seller_Rating  
FROM P_nishant_local_T.usr_cross_new_us CAL
LEFT JOIN PRS_RESTRICTED_V.SPS_LEVEL_METRIC_SUM SPS
ON CAL.RETAIL_WK_END_DATE >= SPS.LAST_EVAL_DT
and cal.slr_id = sps.user_id
WHERE CAL.RETAIL_YEAR >=2022
AND SPS_EVAL_TYPE_CD = 1 ---- trending Standard code
and sps_prgrm_id = 3
ORDER BY 1,2,3
)
where rnk =1;



--- NUMBER OF STORES ACTIVE ON AT LEAST ONE DAY IN THE WEEKN
DROP TABLE IF EXISTS P_nishant_local_T.STORE_KPI_METRICS_us;
CREATE TABLE P_nishant_local_T.STORE_KPI_METRICS_us AS
SELECT CAL.RETAIL_YEAR,
	   CAL.RETAIL_WEEK,
	   CAL.AGE_FOR_RTL_WEEK_ID,
	   CAL.SLR_ID, 
	   COUNT(CASE WHEN PROD_ID = 101 THEN 1 ELSE NULL END) AS STARTER_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 3 THEN 1 ELSE NULL END) AS BASIC_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 4 THEN 1 ELSE NULL END) AS FEATURE_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 5 THEN 1 ELSE NULL END) AS ANCHOR_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 102 THEN 1 ELSE NULL END) AS ENTERPRISE_STORE_COUNT
FROM P_nishant_local_T.usr_cross_new_us CAL
LEFT JOIN ACCESS_VIEWS.DW_STORE_ATTR_HIST HIST ON 
CAL.RETAIL_WK_END_DATE >= HIST.BEG_DT
AND CAL.RTL_WEEK_BEG_DT <= HIST.END_DT
AND CAL.SLR_ID = HIST.SLR_ID
WHERE CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2022
AND CAL.AGE_FOR_RTL_WEEK_ID <= -1
GROUP BY 1,2,3,4
ORDER BY 3,1,2;

---T52 STORE COUNTS

DROP TABLE IF EXISTS P_nishant_local_T.T52_STORE_KPI_METRICS_us ;
CREATE TABLE P_nishant_local_T.T52_STORE_KPI_METRICS_us AS
select CAL.SLR_ID, CAL.retail_week, CAL.retail_year, CAL.AGE_FOR_RTL_WEEK_ID,
	   COUNT(CASE WHEN PROD_ID = 101 THEN 1 ELSE NULL END) AS T52W_STARTER_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 3 THEN 1 ELSE NULL END) AS T52W_BASIC_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 4 THEN 1 ELSE NULL END) AS T52W_FEATURE_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 5 THEN 1 ELSE NULL END) AS T52W_ANCHOR_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 102 THEN 1 ELSE NULL END) AS T52W_ENTERPRISE_STORE_COUNT
from P_nishant_local_T.usr_cross_new_us CAL
LEFT JOIN CAL_52W a
ON CAL.RETAIL_WEEK = A.RETAIL_WEEK
AND CAL.RETAIL_YEAR = A.RETAIL_YEAR
left JOIN ACCESS_VIEWS.DW_STORE_ATTR_HIST b
on CAL.slr_id = b.slr_id
AND A.retail_wk_end_date >= B.BEG_DT
AND A.retail_week_b4_52w_dt <= B.END_DT
GROUP BY 1,2,3,4;

---T1W NATIVE & SELLER HUB USAGE
-- DROP TABLE IF EXISTS P_nishant_local_T.T1W_NSH_USAGE_METRICS_de;
-- CREATE TABLE P_nishant_local_T.T1W_NSH_USAGE_METRICS_de AS
-- select CAL.RETAIL_YEAR, CAL.RETAIL_WEEK, CAL.AGE_FOR_RTL_WEEK_ID,
-- FCT.USER_ID as slr_id,
-- SUM(FCT.sh_cnt) AS T1W_SELLERH_CNT,
-- SUM(FCT.NATIVE_CNT) AS T1W_NATIVE_CNT
-- from ACCESS_VIEWS.DW_CAL_DT CAL
-- LEFT JOIN P_ZETA_AUTOETL_V.SLR_HUB_DLY_USG_FCT FCT
-- ON CAL.CAL_DT = FCT.CAL_DT
-- WHERE CAL.RETAIL_WEEK >=1
-- AND CAL.RETAIL_YEAR >=2021
-- AND CAL.AGE_FOR_DT_ID <= -1
-- GROUP BY 1,2,3,4;

---T52W NATIVE & SELLER HUB USAGE

drop table if exists P_nishant_local_T.T52W_NSH_USAGE_METRICS_us;
CREATE TABLE P_nishant_local_T.T52W_NSH_USAGE_METRICS_us AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(coalesce(b.T1W_SELLERH_CNT,0)) as T52W_SELLERH_CNT,
sum(coalesce(b.T1W_NATIVE_CNT,0)) as T52W_NATIVE_CNT
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.T1W_NSH_USAGE_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;


--- FEE T1W
dROP TABLE IF EXISTS P_nishant_local_T.T1W_FEE_us;
CREATE TABLE P_nishant_local_T.T1W_FEE_us AS 
SELECT
CAL.RETAIL_WEEK,
CAL.RETAIL_YEAR,
CAL.AGE_FOR_RTL_WEEK_ID
,a.SLR_ID
,SUM(CASE WHEN a.actn_code IN (504,505) THEN -1*a.AMT_USD END) as T1W_Variable_FVF -- NET
,SUM(CASE WHEN a.actn_code IN (508,509) THEN -1*a.AMT_USD END) as T1W_Fixed_FVF --- 
,SUM(CASE WHEN a.actn_code IN (1,24) THEN -1*a.AMT_USD END) as T1W_Insertion_Fee ---- MIGHT BE COUNTED IN OTHER FEES
,SUM(CASE WHEN a.actn_code IN (139, 140) THEN -1*a.AMT_USD END) as T1W_Subscription_Fee ---- only store subscription fees 139
,SUM(CASE WHEN a.actn_code IN (409,410,474,475,526,527) THEN -1*a.AMT_USD END) as T1W_PL_Fee
FROM ACCESS_VIEWS.DW_CAL_DT CAL 
LEFT JOIN ACCESS_VIEWS.DW_GEM2_CMN_RVNU_I a
ON CAL.CAL_DT = A.ACCT_TRANS_DT
AND a.slr_cntry_id = 1
INNER JOIN access_views.dw_usegm_hist b
ON a.slr_id=b.USER_ID
AND a.ACCT_TRANS_DT BETWEEN b.BEG_DATE AND b.END_DATE
INNER JOIN dw_category_groupings cat
ON a.LSTG_SITE_ID=cat.site_id AND a.leaf_categ_id=cat.leaf_categ_id
WHERE 
1=1
and cal.RETAIL_YEAR >= 2021
 and CAL.AGE_FOR_DT_ID <= -1
AND a.slr_cntry_id = 1
AND a.slr_id in (select distinct  slr_id from P_nishant_local_T.usr_cross_new_us) 
AND cat.sap_category_id NOT IN (23,5,7,41) -- ask about these exclusions
GROUP BY 1, 2, 3, 4; 

----- Fee table T52W

DROP TABLE IF EXISTS P_nishant_local_T.T52W_FEE_us;
CREATE TABLE P_nishant_local_T.T52W_FEE_us AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
SUM(coalesce(B.T1W_Variable_FVF,0)) AS T52W_Variable_FVF
,SUM(coalesce(B.T1W_Fixed_FVF,0)) AS T52W_Fixed_FVF
,SUM(coalesce(B.T1W_Insertion_Fee,0)) AS T52W_Insertion_Fee
,SUM(coalesce(B.T1W_Subscription_Fee,0)) AS T52W_Subscription_Fee
,SUM(coalesce(B.T1W_PL_Fee,0)) AS T52W_PL_Fee
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.T1W_FEE_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

---- t13 fee table

DROP TABLE IF EXISTS P_nishant_local_T.T13W_FEE_us;
CREATE TABLE P_nishant_local_T.T13W_FEE_us AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
SUM(coalesce(B.T1W_Variable_FVF,0)) AS T13W_Variable_FVF
,SUM(coalesce(B.T1W_Fixed_FVF,0)) AS T13W_Fixed_FVF
,SUM(coalesce(B.T1W_Insertion_Fee,0)) AS T13W_Insertion_Fee
,SUM(coalesce(B.T1W_Subscription_Fee,0)) AS T13W_Subscription_Fee
,SUM(coalesce(B.T1W_PL_Fee,0)) AS T13W_PL_Fee
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.T1W_FEE_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=13
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;



--- T1W LISTING ATTRIBUTES

DROP TABLE IF EXISTS P_nishant_local_T.T1W_LISTING_METRICS_us;
CREATE TABLE P_nishant_local_T.T1W_LISTING_METRICS_us AS
SELECT  CAL.RETAIL_WEEK,
	    CAL.RETAIL_YEAR,
		cal.AGE_FOR_RTL_WEEK_ID,
		FCT.SLR_ID,
	    AVG(LENGTH(FCT.AUCT_TITLE)) AS T1W_AVG_TITLE_LENGTH,
		AVG(LENGTH(FCT.SUBTITLE)) AS T1W_AVG_SUBTITLE_LENGTH,
		AVG(FCT.PHT_CNT) AS T1W_AVG_PHOTO_COUNT
FROM ACCESS_VIEWS.DW_CAL_DT CAL 
LEFT JOIN PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT FCT
ON CAL.RETAIL_WK_END_DATE >= FCT.AUCT_START_DT
AND CAL.RETAIL_WK_END_DATE <= FCT.AUCT_END_DT	
WHERE FCT.SLR_CNTRY_ID = 1
and  UPPER(fct.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
AND CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2021
AND FCT.ISCORE = 1
GROUP BY 1,2,3,4;

--- T52W LISTING ATTRIBUTES
--- NO DUPLICATES
--- NEED TO VALIDATE cross
---- NOT RUN, pls run again - need to split into parts
DROP TABLE IF EXISTS P_nishant_local_T.T52W_LISTING_METRICS_us;
CREATE TABLE P_nishant_local_T.T52W_LISTING_METRICS_us AS
SELECT  A.RETAIL_WEEK,
	    A.RETAIL_YEAR,
		A.SLR_ID,
	    AVG(LENGTH(b.AUCT_TITLE)) AS T52W_AVG_TITLE_LENGTH,
		AVG(LENGTH(b.SUBTITLE)) AS T52W_AVG_SUBTITLE_LENGTH,
		AVG(b.PHT_CNT) AS T52W_AVG_PHOTO_COUNT
FROM P_nishant_local_T.usr_cross_new_us A
left join cal_52w cal
on a.retail_year = cal.retail_year
and a.retail_week = cal.retail_week
LEFT JOIN PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT B
on a.slr_id = b.slr_id
and CAL.RETAIL_WK_END_DATE >= b.AUCT_START_DT
AND CAL.retail_week_b4_52w_dt <= b.AUCT_END_DT	
where a.retail_year >= 2022
GROUP BY 1,2,3
ORDER BY 1,3,2;


---OUTGOING T1W M2M COUNT

-- DROP TABLE IF EXISTS P_nishant_local_T.T1W_OUTB_METRICS;
-- CREATE TABLE P_nishant_local_T.T1W_OUTB_METRICS AS
-- SELECT CAL.RETAIL_WEEK,
-- 	   CAL.RETAIL_YEAR,
-- 	   CAL.RETAIL_WK_END_DATE,
-- 	   CAL.AGE_FOR_RTL_WEEK_ID,
-- 	   EM.SNDR_ID AS ID,
-- 	   COUNT(DISTINCT EM.EMAIL_TRACKING_ID) AS T1W_OUT_M2M_CNT
-- from ACCESS_VIEWS.DW_CAL_DT CAL 
-- left join prs_secure_v.dw_ue_email_tracking em
-- on cal.cal_dt = em.SRC_CRE_DT
-- WHERE RETAIL_WEEK>=1
-- AND RETAIL_YEAR >=2021
-- AND CAL.AGE_FOR_WEEK_ID <= -1 
-- GROUP BY 1,2,3,4,5;

----T52W OUTB_M2M
DROP TABLE IF EXISTS P_nishant_local_T.T52W_OUTB_METRICS_us;
CREATE TABLE P_nishant_local_T.T52W_OUTB_METRICS_us AS
SELECT A.RETAIL_WEEK,
	   A.RETAIL_YEAR,
	   A.AGE_FOR_RTL_WEEK_ID,
	   A.slr_ID,
	   SUM(COALESCE(B.T1W_OUT_M2M_CNT,0)) AS T52W_OUT_M2M_CNT 
from P_nishant_local_T.usr_cross_new_us A 
left join P_nishant_local_T.T1W_OUTB_METRICS b
on a.slr_id = b.id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;




---INCOMING M2M COUNT
-- DROP TABLE IF EXISTS P_nishant_local_T.T1W_INB_METRICS;
-- CREATE TABLE P_nishant_local_T.T1W_INB_METRICS AS
-- SELECT CAL.RETAIL_WEEK,
-- 	   CAL.RETAIL_YEAR,
-- 	   CAL.RETAIL_WK_END_DATE,
-- 	   CAL.AGE_FOR_RTL_WEEK_ID,
-- 	   EM.RCPNT_ID AS ID,
-- 	   COUNT(DISTINCT EM.EMAIL_TRACKING_ID) AS T1W_INB_M2M_CNT
-- from ACCESS_VIEWS.DW_CAL_DT CAL 
-- left join prs_secure_v.dw_ue_email_tracking em
-- on cal.cal_dt = em.SRC_CRE_DT
-- WHERE RETAIL_WEEK>=1
-- AND RETAIL_YEAR >=2021
-- GROUP BY 1,2,3,4,5;

----T52W INB_M2M
DROP TABLE IF EXISTS P_nishant_local_T.T52W_INB_METRICS_us;
CREATE TABLE P_nishant_local_T.T52W_INB_METRICS_us AS
SELECT A.RETAIL_WEEK,
	   A.RETAIL_YEAR,
	   A.AGE_FOR_RTL_WEEK_ID,
	   A.slr_ID ,
	   SUM(COALESCE(B.T1W_INB_M2M_CNT,0)) AS T52W_INB_M2M_CNT 
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.T1W_INB_METRICS b
on a.slr_id = b.id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;


--- GSP enabled t1w GMV
DROP TABLE IF EXISTS P_nishant_local_T.T1W_GSP_METRICS_us;
CREATE TABLE P_nishant_local_T.T1W_GSP_METRICS_us AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.SLR_ID,
    ck.EU_B2C_C2C_FLAG,
    SUM(coalesce(CK.gmv20_plan,0)) AS T1W_GSP_GMV
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
inner join ACCESS_VIEWS.DW_LSTG_ITEM lst
on lst.item_id = ck.item_id
and lst.GSP_ENABLED_FLAG=1
WHERE
    ck.SLR_CNTRY_ID = 1 AND 
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;


-- gsp enabled GMV _ t52
drop table if exists P_nishant_local_T.T52W_GSP_METRICS_us;
CREATE TABLE P_nishant_local_T.T52W_GSP_METRICS_us AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(COALESCE(b.T1W_GSP_GMV,0)) as T52W_GSP_GMV
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.T1W_GSP_METRICS_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;



----- PL penetration T52W METRICS
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS_us;
CREATE TABLE P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS_us AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID
, round(sum(b.T1W_PL_Net_Revenue),2) T52W_PL_Net_Revenue
	, round(sum(b.T1W_PL_Net_Revenue)*100/sum(b.T1W_PL_GMV),2) as T52W_PL_net_revenue_penetration_percnt
	, round(sum(b.T1W_PLS_enabled_GMV_pene_pct_num)*100/sum(b.PLS_enabled_GMV_pene_pct_deno),2) as T52W_PLS_enabled_GMV_penetration_percnt
	, round(sum(b.PLS_listing_adoption_pct_num)*100/sum(b.PLS_listing_adoption_pct_deno),2) as T52W_PLS_listing_adoption_percnt
	, round(sum(b.PLA_listing_adoption_num)*100/sum(b.PLS_listing_adoption_pct_deno),2) as T52W_PLA_listing_adoption_percnt
 from P_NISHANT_LOCAL_T.usr_cross_new_us a
left join P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

	
----t52w pl metrics
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T52W_pl_METRICS_us;
CREATE TABLE P_NISHANT_LOCAL_T.T52W_pl_METRICS_us AS
select a.SlR_ID as seller_id, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID
,   SUM(coalesce(b.T1W_pls_si,0)) T52W_pls_si,
    SUM(coalesce(b.T1W_pla_si,0)) T52W_pla_si,
    SUM(coalesce(b.T1W_sfa_si,0)) T52W_sfa_si,
    SUM(coalesce(b.T1W_pls_gmv,0)) T52W_pls_gmv,
	SUM(coalesce(b.T1W_pls_gmv_LC,0)) T52W_pls_gmv_LC,
    SUM(coalesce(b.T1W_pla_gmv,0)) T52W_pla_gmv,
	SUM(coalesce(b.T1W_pla_gmv_LC,0)) T52W_pla_gmv_LC,
    SUM(coalesce(b.T1W_sfa_gmv,0)) T52W_sfa_gmv,
	SUM(coalesce(b.T1W_sfa_gmv_LC,0)) T52W_sfa_gmv_LC,
	sum(coalesce(b.T1W_pls_ad_fee,0)) T52W_pls_ad_fee,
	sum(coalesce(b.T1W_pls_ad_fee_LC,0)) T52W_pls_ad_fee_LC,
	sum(coalesce(b.T1W_pla_ad_fee,0)) T52W_pla_ad_fee, 
	sum(coalesce(b.T1W_pla_ad_fee_LC,0)) T52W_pla_ad_fee_LC, 
    SUM(coalesce(b.T1W_sfa_ad_fee,0)) T52W_sfa_ad_fee,
	SUM(coalesce(b.T1W_sfa_ad_fee_LC,0)) T52W_sfa_ad_fee_LC,
	SUM(coalesce(b.T1W_total_ad_fee,0)) T52W_total_ad_fee,
	SUM(coalesce(b.T1W_total_ad_fee_LC,0)) T52W_total_ad_fee_LC
from P_NISHANT_LOCAL_T.usr_cross_new_us a
left join P_NISHANT_LOCAL_T.T1W_pl_METRICS b
on a.slr_id = b.seller_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;


--- seller type L2 & L3
drop table if exists P_NISHANT_LOCAL_T.latest_seller_type_us;
CREATE TABLE P_NISHANT_LOCAL_T.latest_seller_type_us AS
select cal_month,
	   cal_year,
	   user_id as slr_id,
	   slr_type_l2,
	   slr_type_l3
from DW_STORE_NEW_SELLER_SEGMENTS 
where user_id in 
(
select distinct SLR_ID
from PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
WHERE
    ck.SLR_CNTRY_ID = 1
	and ck.GMV_DT >= '2022-01-01'
--     and CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) 
--     and ck.CK_WACKO_YN = 'N'  
--     and CK.ISCORE = 1 
	and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
)
order by 3,2,1;


--- T1W PL METRICS 2
DROP TABLE IF EXISTS p_NISHANT_LOCAL_t.T1W_PL_METRICS_2_us;
CREATE TABLE p_NISHANT_LOCAL_t.T1W_PL_METRICS_2_us AS
select CAL.RETAIL_WEEK,
	   cal.retail_year,
	   cal.AGE_FOR_RTL_WEEK_ID,
	   FN.slr_id,
	   sum(PLS_NEt) T1W_PLS_NET_REV
	   ,SUM(PLS_gross) T1W_PLS_GROSS_REV
	   ,SUM(PLa_NEt) T1W_PLA_NET_REV
	   ,SUM(PLa_gross) T1W_PLA_GROSS_REV
	   ,SUM(PLS_GMV) AS T1W_PLS_GMV
	   ,SUM(PLS_SI) T1W_PLS_SI
	   ,SUM(PLA_GMV) T1W_PLA_GMV
	   ,SUM(PLA_SI) T1W_PLA_SI
	   ,SUM(PLS_NET) + SUM(PLA_NET) + SUM(PLX_NET) + SUM(OA_NET) AS T1W_PL_REV
	   , SUM(PLS_GMV) + SUM(PLA_GMV) + SUM(OA_GMV) AS T1W_PL_GMV
	   , --SUM(PLS_PLA_ELIG_LSTG) AS T1W_PL_eligible_listings,
    	SUM(PLS_enabled_lstg) AS T1W_PLS_Live_Listings,
    	SUM(pla_lstgs) AS T1W_PLA_Live_listings
	   , (SUM(PLS_GROSS) / NULLIF(SUM(PLS_GMV), 0)) AS T1W_pls_sold_adrate
from ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN P_NBD_T.SLRSEG_allTiers_final  FN
ON FN.CAL_DT = CAL.CAL_DT
where slr_cntry_id = 1
and fn.slr_ID in (select distinct slr_id from p_NISHANT_LOCAL_t.usr_cross_new_us)
and cal.RETAIL_YEAR >=2021
AND CAL.AGE_FOR_WEEK_ID <= -1
GROUP BY 1,2,3,4
order by 4,2,1;

---t52 PL metrics

-- drop table if exists p_NISHANT_LOCAL_t.T52W_PL_METRICS_2_de;
CREATE TABLE p_NISHANT_LOCAL_t.T52W_PL_METRICS_2_us AS
select a.RETAIL_WEEK,
	   a.retail_year,
	   a.AGE_FOR_RTL_WEEK_ID,
	   a.slr_id,
	   sum(b.T1W_PLS_NET_REV) T52W_PLS_NET_REV
	   ,SUM(b.T1W_PLS_GROSS_REV) T52W_PLS_GROSS_REV
	   ,SUM(b.T1W_PLA_NET_REV) T52W_PLA_NET_REV
	   ,SUM(b.T1W_PLA_GROSS_REV) T52W_PLA_GROSS_REV
	   ,SUM(b.T1W_PLS_GMV) T52W_PLS_GMV
	   ,SUM(b.T1W_PLS_SI) T52W_PLS_SI
	   ,SUM(b.T1W_PLA_GMV) T52W_PLA_GMV
	   ,SUM(b.T1W_PLA_SI) T52W_PLA_SI
	   ,SUM(b.T1W_PL_REV) T52W_PL_REV 
	   , SUM(b.T1W_PL_GMV) T52W_PL_GMV
	    --SUM(b.T1W_PL_eligible_listings) T52W_PL_eligible_listings,
    	,SUM(b.T1W_PLS_Live_Listings) T52W_PLS_Live_Listings,
    	SUM(b.T1W_PLA_Live_listings) T52W_PLA_Live_listings
	   , (SUM(b.T1W_PLA_GROSS_REV) / NULLIF(SUM(b.T1W_PLS_GMV), 0)) AS T52W_pls_sold_adrate
from p_NISHANT_LOCAL_t.usr_cross_new_us a
left join p_NISHANT_LOCAL_t.T1W_PL_METRICS_2_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
group by 1,2,3,4;

---- T1W_PURCHASES, GMB

-- DROP TABLE IF EXISTS P_nishant_local_T.GMB_T1_METRICS_de;
CREATE TABLE P_nishant_local_T.GMB_T1_METRICS_us AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.BYR_ID,
    ck.EU_B2C_C2C_FLAG,
    SUM(CK.gmv20_plan) AS T1W_GMB,
    SUM(CK.gmv20_sold_quantity) AS T1W_PURCHASES
FROM  ACCESS_VIEWS.DW_CAL_DT CAL 
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
WHERE
    ck.BYR_CNTRY_ID = 1 AND 
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and  UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;

--  SELECT -- retail_week,
-- -- 		retail_year,
-- -- 		slr_id,
-- 		count(*)
-- FROM  P_nishant_local_T.BYR_T52_METRICS
-- -- group by 1,2,3
-- -- order by 4 desc
-- LIMIT 100


-- GMB2 & PURCHASES T52W 
-- drop table if exists P_nishant_local_T.GMB_T52_METRICS_de;
CREATE TABLE P_nishant_local_T.GMB_T52_METRICS_us AS
select a.slr_ID , a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_GMB) as T52W_GMB,
sum(b.T1W_PURCHASES) as T52W_PURCHASES
from P_nishant_local_t.usr_cross_new_us a
left join P_nishant_local_T.GMB_T1_METRICS_us b
on a.slr_id = b.BYR_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;




---- T1W BYER COUNT distinct buyers
-- drop table if exists P_nishant_local_T.BYR_T1_METRICS;
CREATE TABLE P_nishant_local_T.BYR_T1_METRICS_us AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.slr_ID,
    count(distinct ck.BYR_ID) T1W_byr_cnt
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
WHERE
    ck.slr_CNTRY_ID =1  AND 
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
GROUP BY
    1, 2, 3, 4
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;
	

--- ctr view
-- drop table if exists P_nishant_local_T.AD_T52w_METRICS_us;
CREATE TABLE P_nishant_local_T.AD_T52w_METRICS_us as
select  
a.slr_id, 
a.RETAIL_WEEK,
a.RETAIL_YEAR,
a.AGE_FOR_RTL_WEEK_ID,

SUM(b.total_imprsns_curr) t52w_total_imprsns_curr,
SUM(b.pl_imps_curr) t52w_pl_imps_curr,
SUM(b.pla_imps_curr) t52w_pla_imps_curr,
SUM(b.organic_impressions_curr) t52w_organic_impressions_curr,

SUM(b.total_clicks_curr) t52w_total_clicks_curr,
SUM(b.pl_clicks_curr) t52w_pl_clicks_curr,
SUM(b.pla_clicks_curr) t52w_pla_clicks_curr,
	SUM(b.organic_clicks_curr) t52w_organic_clicks_curr
from P_nishant_local_t.usr_cross_new_us  a
left join P_nishant_local_T.AD_T1_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
and a.retail_year >=2022
group by 1,2,3,4;


----T52 budget usage
-- DROP TABLE IF EXISTS p_NISHANT_LOCAL_T.T52W_BUDGET_METRICS_us;
CREATE TABLE p_NISHANT_LOCAL_T.T52W_BUDGET_METRICS_us AS
select a.sLr_id,
a.RETAIL_WEEK,
a.RETAIL_YEAR,
a.AGE_FOR_RTL_WEEK_ID
,COALESCE(sum(b.T1W_ads_fee_usd),0) AS T52W_ads_fee_usd
,COALESCE(sum(b.T1W_budget_amt_usd),0) T52W_budget_amt_usd
,COALESCE(sum(b.T1W_ads_fee_usd),0)/COALESCE(sum(b.T1W_budget_amt_usd),0) as T52W_cmpgn_budget_usage
FROM P_nishant_local_t.usr_cross_new_us A
LEFT JOIN p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS B
on a.sLr_id = b.sELlEr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
and a.retail_year >=2022
group by 1,2,3,4;


--- seller address
-- DROP TABLE IF EXISTS  P_nishant_local_t.seller_address_us;
create table P_nishant_local_t.seller_address_us as
select user_id as seller_id, city, pstl_code as zip
from PRS_SECURE_V.DW_USERS adr
where user_id in 
(
select distinct slr_id as user_id
from P_nishant_local_T.usr_cross_new_us
);

----- gmv dominant category ( only add table 4 to join )
----- table 1: d cat base
--- need to rerun & remove duplicates
-- DROP TABLE IF EXISTS P_nishant_local_T.dl_base_us;
create table P_nishant_local_T.dl_base_us as
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.SLR_ID,
    ck.BSNS_VRTCL_NAME,               	
    SUM(CK.gmv20_plan) AS T1W_GMV
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
WHERE
    ck.SLR_CNTRY_ID = 1 AND 
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
	4, 2 ASC,1 ASC, 5;


----TABLE 4 DOMINANT t1w & t52 dominant vertical combined
-- DROP TABLE IF EXISTS P_nishant_local_t.dominant_vertical_gmv;
create table P_nishant_local_t.dominant_vertical_gmv_us as 
select cal.retail_week,
	   cal.retail_year,
	   cal.slr_id,
	   a.BSNS_VRTCL_NAME as t1w_dominant_vertical,
	   b.BSNS_VRTCL_NAME as t52w_dominant_vertical
	from P_nishant_local_T.usr_cross_new_us  cal
	left join
	(select RETAIL_WEEK,
	RETAIL_YEAR,
	AGE_FOR_RTL_WEEK_ID,
    SLR_ID,
	BSNS_VRTCL_NAME,
	T1W_GMV,
	rOW_NUMBER() over (PARTITION by RETAIL_WEEK,RETAIL_YEAR,AGE_FOR_RTL_WEEK_ID,SLR_ID order by t1w_gmv desc ) as rnk 
from P_nishant_local_T.dl_base_us
order by 4, 2, 1, 7) a
on a.retail_year = cal.retail_year
and a.retail_week = cal.retail_week
and a.slr_id = cal.slr_id
left join 
(
select RETAIL_WEEK,
	RETAIL_YEAR,
	AGE_FOR_RTL_WEEK_ID,
    SLR_ID,
	BSNS_VRTCL_NAME,
	t52w_gmv,
    rOW_NUMBER() over (PARTITION by RETAIL_WEEK,RETAIL_YEAR,AGE_FOR_RTL_WEEK_ID,SLR_ID order by t52w_gmv desc ) as rnk 
from (
select a.RETAIL_WEEK,
	a.RETAIL_YEAR,
	a.AGE_FOR_RTL_WEEK_ID,
    a.SLR_ID,
	B.BSNS_VRTCL_NAME,
	sum(b.T1W_GMV) as t52w_GMV
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.dl_base_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
group by 1,2,3,4,5)
order by 4, 2, 1, 7
) b 
on a.retail_week = b.retail_week
and a.retail_year = b.retail_year
and a.slr_id = b.slr_id
and b.rnk =1
where a.rnk = 1
and a.retail_year >=2022
order by 3,2,1;


----- new or retained buyers
-- DROP TABLE IF EXISTS NORB_TEMP;
CREATE temp TABLE NORB_TEMP_us
SELECT
	CAL.retail_year,
	CAL.retail_week,
	CAL.RETAIL_WK_END_DATE,
	A.GMV_DT,
	A.BYR_ID,
	case when b.BYR_ID is null then 'B2c_NORB'  else 'ACTIVE' end as BYR_TYPE
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT A
ON A.GMV_DT = CAL.CAL_DT
left join PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT B
on a.BYR_ID = b.BYR_ID AND b.GMV_DT BETWEEN date_sub(a.GMV_DT,365) AND date_sub(a.GMV_DT,1)
--left join p_eupricing_t.uk_new_sellers C	on A.SLR_ID=C.SELLER_ID WHERE C.SELLER_ID IS NULL -- excluding D2C
WHERE  CAL.AGE_FOR_WEEK_ID <= -1 
	and UPPER(a.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
	AND A.SLR_CNTRY_ID = 1 AND 
	CAL.RETAIL_YEAR >= 2022 AND
    A.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    A.CK_WACKO_YN = 'N' AND 
    A.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 ;

-

-- FINAL TABLE, USE THIS ( NORB or active buyers)
-- drop table if exists P_nishant_local_T.BYR_NORB_METRIC;
CREATE TABLE P_nishant_local_T.BYR_NORB_METRIC_us AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.slr_ID,
    count(DISTINCT CASE WHEN NORB.BYR_TYPE = 'ACTIVE' then ck.BYR_ID else null end) active_byr_cnt,
	count(DISTINCT CASE WHEN NORB.BYR_TYPE = 'B2c_NORB' then ck.BYR_ID else null end) NORB_byr_cnt
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
LEFT JOIN NORB_TEMP_us NORB
ON CK.BYR_ID = NORB.BYR_ID
AND CK.GMV_DT = NORB.GMV_DT
WHERE
	CK.SLR_ID IN (SELECT DISTINCT SLR_ID FROM p_NISHANT_LOCAL_T.usr_cross_new_us )
	AND 
    ck.slr_CNTRY_ID = 1 AND 
	CAL.RETAIL_YEAR >= 2022 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
GROUP BY
    1, 2, 3, 4
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;



--- fvf t1w
-- drop table if exists P_nishant_local_t.fvf_t1_de;
create table P_nishant_local_t.fvf_t1_us as
SELECT
cal.retail_week,
cal.retail_year,
cal.AGE_FOR_RTL_WEEK_ID,
l.slr_id
,SUM(case when actn_code_type_desc1 like 'Final%' and lower(actn_code_desc) not like '%credit%' then (-1*a.amt_blng_curncy) End) as t1w_gross_fvf_lc
,SUM(case when actn_code_type_desc1 like 'Final%' and lower(actn_code_desc) not like '%credit%' then (-1*a.amt_usd) End) as t1w_gross_fvf
,SUM(case when actn_code_type_desc1 like 'Final%' then (-1*a.amt_blng_curncy) End) as t1w_net_fvf_lc,
SUM(case when actn_code_type_desc1 like 'Final%' then (-1*a.amt_usd) End) as t1w_net_fvf
FROM  ACCESS_VIEWS.DW_LSTG_ITEM l
INNER JOIN ACCESS_VIEWS.DW_ACCOUNTS_ALL AS a
ON l.ITEM_ID = a.ITEM_ID AND l.SLR_ID = a.USER_ID
join ACCESS_VIEWS.DW_ACTION_CODES ac
on a.actn_code = ac.actn_code
inner join  ACCESS_VIEWS.DW_CAL_DT cal
on a.ACCT_TRANS_DT = cal.cal_dt
WHERE cal.retail_year >=2021
and AGE_FOR_RTL_WEEK_ID <=-1
-- and a.item_site_id in (3)
and trim(upper(a.wacko_yn)) = 'N'
and a.user_id in (select distinct slr_id from P_nishant_local_T.usr_cross_new_us)
group by 1,2,3,4;



---- t52 fvf
-- DROP TABLE IF EXISTS P_nishant_local_t.fvf_t52_de;
create table P_nishant_local_t.fvf_t52_us as
select base.retail_year, base.retail_week, base.slr_id
,SUM(b.t1w_gross_fvf_lc) t52w_gross_fvf_lc
,SUM(b.t1w_gross_fvf) t52w_gross_fvf
,SUM(b.t1w_net_fvf_lc) t52w_net_fvf_lc,
SUM(b.t1w_net_fvf) t1w_net_fvf 
from  P_nishant_local_T.usr_cross_new_us base
left join P_nishant_local_t.fvf_t1_us  b
on base.slr_id = b.slr_id
and (base.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (base.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
group by 1,2,3;

----- t13 fvf
-- DROP TABLE IF EXISTS P_nishant_local_t.fvf_t13_de;
create table P_nishant_local_t.fvf_t13_us as
select base.retail_year, base.retail_week, base.slr_id
,SUM(b.t1w_gross_fvf_lc) t13w_gross_fvf_lc
,SUM(b.t1w_gross_fvf) t13w_gross_fvf
,SUM(b.t1w_net_fvf_lc) t13w_net_fvf_lc,
SUM(b.t1w_net_fvf) t13w_net_fvf 
from  P_nishant_local_T.usr_cross_new_us base
left join P_nishant_local_t.fvf_t1_us  b
on base.slr_id = b.slr_id
and (base.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (base.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=13
group by 1,2,3;


---- t1w item condition GMV
-- DROP TABLE IF EXISTS  P_nishant_local_t.t1w_refurb_gmv_de;
create table P_nishant_local_t.t1w_refurb_gmv_us as
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.SLR_ID,
    SUM(case when CND.CNDTN_ROLLUP_ID =1 then coalesce(CK.gmv20_plan,0) else 0 end) AS T1W_new_item_GMV,
    SUM(case when CND.ITEM_CNDTN_ID =3 then coalesce(CK.gmv20_plan,0) else 0 end) AS T1W_used_item_GMV,
    SUM(case when CND.ITEM_CNDTN_ID =2 then coalesce(CK.gmv20_plan,0) else 0 end) AS T1W_refurb_item_GMV
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
left join ACCESS_VIEWS.lstg_item_cndtn cnd`
on ck.item_id = cnd.item_id
WHERE
    ck.SLR_CNTRY_ID = 1 AND
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
GROUP BY
    1, 2, 3, 4
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;

			
--- t52 item condition gmv
-- DROP TABLE IF EXISTS  P_nishant_local_t.t52w_refurb_gmv;
create table P_nishant_local_t.t52w_refurb_gmv_us as
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(COALESCE(b.T1W_new_item_GMV,0)) as T52W_new_item_GMV,
sum(COALESCE(b.T1W_used_item_GMV,0)) as T52W_used_item_GMV,
sum(COALESCE(b.T1W_refurb_item_GMV,0)) as T52W_refurb_item_GMVx
from P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_T.t1w_refurb_gmv_us b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;


--- COUPON SPEND METRICS T1W
-- drop table if exists P_nishant_local_t.t1w_coupon_metrics_de;
create table P_nishant_local_t.t1w_coupon_metrics_us as
select 
cal.RETAIL_WEEK,
cal.RETAIL_YEAR,
cal.AGE_FOR_RTL_WEEK_ID,
coup.SELLER_ID
,sum(coalesce(coup.spend_usd,0)) t1w_total_spend_usd ,
sum(coalesce(coup.spend_ebay_usd,0)) t1w_ebay_spend_usd,
sum(coalesce(coup.SPEND_SELLER_USD_APAC + coup.SPEND_SELLER_USD_EXCL_APAC,0)) t1w_seller_coupon_spend_usd
from ACCESS_VIEWS.DW_CAL_DT CAL
left join p_planning_v.open_coupon_transaction_level_final coup
on cal.cal_dt = coup.REDMD_DATE
where 1=1
and seller_id in (select distinct slr_id from P_nishant_local_T.usr_cross_new_us)
AND cal.retail_year >=2021
AND spend_seller_lc > 0
group by 1,2,3,4;

---t52w coupon SPEND
-- drop table if exists P_nishant_local_t.t52w_coupon_metrics_de;
create table P_nishant_local_t.t52w_coupon_metrics_us as
select 
a.RETAIL_WEEK,
a.RETAIL_YEAR,
a.AGE_FOR_RTL_WEEK_ID,
a.Slr_id seller_id,
sum(b.t1w_total_spend_usd) t52w_total_spend_usd ,
sum(b.t1w_ebay_spend_usd) t52w_ebay_spend_usd ,
sum(b.t1w_seller_coupon_spend_usd) t52w_seller_coupon_spend_usd
from  P_nishant_local_T.usr_cross_new_us a
left join P_nishant_local_t.t1w_coupon_metrics_us b
on a.slr_id = b.seller_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <=52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

------ linked accounts
-- DROP table if exists  P_NISHANT_LOCAL_T.CTRL_ACCT_DET_us ;
-- Create table P_NISHANT_LOCAL_T.CTRL_ACCT_DET_us as
-- SELECT
-- 	SL1.user_id as user_id,
-- 	XSID.incdata_id
-- from
-- (SELECT
-- A.SELLER_PARENT_UID as user_id
-- FROM
-- ACCESS_VIEWS.CHECKOUT_METRIC_ITEM_EXT A
-- INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = A.GMV_DT
-- LEFT JOIN ACCESS_VIEWS.DW_USEGM_HIST USEGM_HIST  ON USEGM_HIST.USER_ID = A.SELLER_ID AND USEGM_HIST.USEGM_GRP_ID  = 48    
--    AND A.GMV_DT BETWEEN USEGM_HIST.BEG_DATE AND USEGM_HIST.END_DATE
-- WHERE 1=1
-- AND A.GMV_DT >'2022-01-01' AND A.AUCT_END_DT >= '2022-01-01'
-- AND A.SELLER_COUNTRY_ID  in (1)
--     AND A.CK_WACKO_YN = 'N'
--     AND A.AUCT_TYPE_CODE NOT IN (10,12,15)
-- 	And A.core_item_cnt > 0
-- 	AND  (USEGM_HIST.USEGM_ID = 206 )) SL1 -- the base data set
		
-- LEFT JOIN incdata_v.INC_MASTER_CUSTOMER XSID
-- 	on SL1.user_id=XSID.incdata_srcid
-- group by 1,2
-- ;

-- -- DROP TABLE if exists p_NISHANT_LOCAL_T.TEST_ACCT_DET_de;
-- Create table p_NISHANT_LOCAL_T.TEST_ACCT_DET_us as
-- SELECT
-- 	SL1.user_id as user_id,
-- 	XSID.incdata_id
	
-- from
-- (SELECT
-- A.SELLER_PARENT_UID as user_id
-- FROM
-- ACCESS_VIEWS.CHECKOUT_METRIC_ITEM_EXT A
-- INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = A.GMV_DT
-- LEFT JOIN ACCESS_VIEWS.DW_USEGM_HIST USEGM_HIST  ON USEGM_HIST.USER_ID = A.SELLER_ID AND USEGM_HIST.USEGM_GRP_ID  = 48       AND A.GMV_DT BETWEEN USEGM_HIST.BEG_DATE AND USEGM_HIST.END_DATE
-- WHERE 1=1
-- AND A.GMV_DT BETWEEN '2022-01-01' AND CURRENT_DATE
-- AND A.AUCT_END_DT >= '2022-01-01'
-- AND A.SELLER_COUNTRY_ID  in (1)
--     AND A.CK_WACKO_YN = 'N'
--     AND A.AUCT_TYPE_CODE NOT IN (10,12,15)
-- 	And A.core_item_cnt > 0
-- 		AND  (USEGM_HIST.USEGM_ID <> 206 OR  USEGM_HIST.USEGM_ID IS NULL ) ) SL1 -- the data set in which we want to find linked accounts
-- LEFT JOIN incdata_v.INC_MASTER_CUSTOMER XSID
-- 	on SL1.user_id=XSID.incdata_srcid
-- group by 1,2
-- ;

-- -- DROP TABLE IF EXISTS  P_nishant_local_t.linked_accs_de;
-- create table P_nishant_local_t.linked_accs_de as
-- select USR_CTRL, USR_LINK,
-- max(XID_match_YN) as XID_match_YN
-- FROM
-- (
-- select A.user_Id as USR_CTRL,
-- 		B.user_Id as USR_LINK,
-- case when A.incdata_id=B.incdata_id then 'Y' else 'N' end AS XID_match_YN
	
-- from P_nishant_local_t.CTRL_ACCT_DET_de A
-- inner Join
-- P_nishant_local_t.TEST_ACCT_DET_de B on A.incdata_id=B.incdata_id
-- where A.user_Id<>B.user_Id
-- )f
-- group by 1,2
-- ;

-- -- Final Linked accounts table (use this)
-- -- drop table if exists P_nishant_local_t.linked_accs_final_de;
-- create table P_nishant_local_t.linked_accs_final_de as
-- select usr_ctrl, count(distinct usr_link) as linked_acc
-- from P_nishant_local_t.linked_accs_de
-- where XID_match_YN in ('Y')
-- and usr_ctrl in (select distinct slr_id from P_nishant_local_T.usr_cross_new_de)
-- group by 1
-- order by 2 desc;

--- NORS
-- drop table if exists P_nishant_local_t.ALL_B2C_DAILY_us;
create table P_nishant_local_t.ALL_B2C_DAILY_us as
SELECT
	cal.retail_year,
	cal.retail_week,
	cal.RETAIL_WK_END_DATE,
	CK.GMV_DT,
	CK.SLR_ID,
	B.FIRST_SELL_DT,
	SUM(gmv20_plan) AS GMV,
	SUM(gmv20_sold_quantity) AS SI
FROM PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT CK	
LEFT JOIN (
		SELECT
		CK.SLR_ID,
		MIN(GMV_DT) AS FIRST_SELL_DT
		FROM PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT CK
		INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = CK.GMV_DT
		WHERE 1=1
			AND age_for_rtl_week_id<-1
			AND CK.SLR_CNTRY_ID = 1
			AND CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3)
			AND CK.ISCORE = 1 -- CORE ONLY 	
			AND UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
			AND CK.CK_WACKO_YN = 'N'
		GROUP BY 1) B
	ON CK.SLR_ID=B.SLR_ID
INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = CK.GMV_DT
WHERE 1=1
	AND age_for_rtl_week_id<-1
	AND RETAIL_YEAR>=2020
	AND CK.AUCT_END_DT>='2016-01-01'
	AND CK.SLR_CNTRY_ID = 1 --de
	AND CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3)
	AND CK.ISCORE = 1 -- CORE ONLY 	
    AND UPPER(ck.CUST_SGMNTN_DESC) IN ('MERCHANT','LARGE MERCHANT','ENTREPRENEUR')
	AND CK.CK_WACKO_YN = 'N'
GROUP BY 1,2,3,4,5,6;

-- NORS OR ACTIVE SELLER
-- DROP TABLE IF EXISTS P_nishant_local_t.ALL_B2C_TXN_T365D_us;
CREATE  TABLE P_nishant_local_t.ALL_B2C_TXN_T365D_us
SELECT
	A.retail_year,
	A.retail_week,
	A.RETAIL_WK_END_DATE,
	A.SLR_ID,
	case when b.SLR_ID is null then 'B2C_NORS'  else 'ACTIVE' end as SELLER_TYPE
FROM P_nishant_local_T.usr_cross_new_us A
	left join P_nishant_local_t.ALL_B2C_DAILY_us B
	on a.SLR_ID = b.SLR_ID AND b.GMV_DT BETWEEN date_sub(a.RETAIL_WK_END_DATE,371) AND date_sub(a.RETAIL_WK_END_DATE,7)
GROUP BY 1,2,3,4,5;



--- final table
DROP TABLE IF EXISTS P_nishant_local_T.final_metrics_2_us;
create table P_nishant_local_T.final_metrics_2_us as
SELECT BASE.RETAIL_YEAR, BASE.RETAIL_WEEK,
	   base.RETAIL_WK_END_DATE, BASE.SLR_ID,
	   AI.CITY AS SELLER_addrress, AI.ZIP,
 	   nors.seller_type,
		W.SLR_TYPE_L2 LATEST_SELLER_TYPE2,
		W.SLR_TYPE_L3 LATEST_SELLER_TYPE3,
	   AJ.t1w_dominant_vertical,
	   AJ.t52w_dominant_vertical,
	--    AJ2.linked_acc,
	   A.T1W_GMV,
-- 	   A.T1W_GMV_LC,
	   GSP.T1W_gsp_GMV,
	   A.T1W_SI,
	   (A.T1W_GMV/A.T1W_SI) as T1W_ASP,
	   A.T1W_CBT_GMV, A.T1W_CBT_SI, (A.T1W_CBT_GMV/A.T1W_CBT_SI) AS T1W_CBT_ASP,
	   A.T1W_CBT_GMV/A.T1W_GMV T1W_CBT_PERC,
A3.T1W_new_item_GMV,
A3.T1W_used_item_GMV,
A3.T1W_refurb_item_GMV,
A4.T52W_new_item_GMV,
A4.T52W_used_item_GMV,
A4.T52W_refurb_item_GMV,
	   B.T52W_GMV
-- 	   B.T52W_GMV_LC
	   , GSP2.T52W_gsp_GMV, B.T52W_SI, (B.T52W_GMV/B.T52W_SI) AS T52W_ASP,
	   B.T52W_CBT_GMV, B.T52W_CBT_SI, (B.T52W_CBT_GMV/B.T52W_CBT_SI) AS T52W_CBT_ASP,
	   B.T52W_CBT_GMV/B.T52W_GMV T52W_CBT_PERC,
	   A2.T13W_GMV, A2.T13W_SI, (A2.T13W_GMV/A2.T13W_SI) AS T13W_ASP,
	   A2.T13W_CBT_GMV, A2.T13W_CBT_SI, (A2.T13W_CBT_GMV/A2.T13W_CBT_SI) AS T13W_CBT_ASP,
	    case when B.T52W_GMV <=100000 then '$0-100k'
 	   when B.T52W_GMV <=250000 and B.T52W_GMV >100000 then '$100k-250k'
	   when B.T52W_GMV >250000 then '>$250k' END
 	   as t52_gmv_bucket,
	   C.T1W_LL,
	   c.T1W_RELIST_LL,
	   c.T1W_FNL_LL,
	   c.t1w_avg_start_price,
	   D.T52W_LL,
	   d.T52W_RELIST_LL,
	   d.T52W_FNL_LL,
	   d.t52w_avg_start_price,
	   E.T1W_escal_SNAD_count, E.T1W_non_escal_SNAD_count, E.T1W_STOCKOUT_count,
       E.T1W_escal_INR_count,E.T1W_non_escal_INR_count,E.T1W_low_IAD_DSR_count,
       E.T1W_NN_feedback_count, E.T1W_Non_escal_SNAD_MSG_count,
       E.T1W_Non_escal_INR_MSG_count,
	   F.T52W_escal_SNAD_count, F.T52W_non_escal_SNAD_count, F.T52W_STOCKOUT_count,
       F.T52W_escal_INR_count, F.T52W_non_escal_INR_count, F.T52W_low_IAD_DSR_count,
       F.T52W_NN_feedback_count, F.T52W_Non_escal_SNAD_MSG_count,
       F.T52W_Non_escal_INR_MSG_count,
	   G.LATEST_SELLER_RATING,
	   H.STARTER_STORE_COUNT, H.BASIC_STORE_COUNT, H.FEATURE_STORE_COUNT, H.ANCHOR_STORE_COUNT, H.ENTERPRISE_STORE_COUNT,
	   H2.T52W_STARTER_STORE_COUNT, H2.T52W_BASIC_STORE_COUNT, H2.T52W_FEATURE_STORE_COUNT, H2.T52W_ANCHOR_STORE_COUNT, H2.T52W_ENTERPRISE_STORE_COUNT,
	   I.T1W_SELLERH_CNT,I.T1W_NATIVE_CNT,
	   J.T52W_SELLERH_CNT,J.T52W_NATIVE_CNT,
-- 	   K.T13W_Variable_FVF, -- NET
--        K.T13W_Fixed_FVF --- 
K5.t1w_total_spend_usd ,
K5.t1w_ebay_spend_usd,
K5.t1w_seller_coupon_spend_usd,
K6.t52w_total_spend_usd ,
K6.t52w_ebay_spend_usd,
K6.t52w_seller_coupon_spend_usd,
-- 		k2.t1w_gross_fvf_lc,
		k2.t1w_gross_fvf,
-- 		k2.t1w_net_fvf_lc,
		k2.t1w_net_fvf,
-- 		k3.t13w_gross_fvf_lc,
		k3.t13w_gross_fvf,
-- 		k3.t13w_net_fvf_lc,
		k3.t13w_net_fvf,
		k3.t13w_net_fvf/A2.T13W_GMV AS T13W_FVF_TAKE_RATE,
-- 		k4.t52w_gross_fvf_lc,
		k4.t52w_gross_fvf,
-- 		k4.t52w_net_fvf_lc,
		k4.t1w_net_fvf as t52w_net_fvf,
		K4.T1W_NET_FVF/B.T52W_GMV AS T52_fvf_TAKE_RATE
       ,K.T13W_Insertion_Fee ---- 
       ,K.T13W_Subscription_Fee ---- 
       ,K.T13W_PL_Fee
-- 	   L.T52W_Variable_FVF, -- NET
--        L.T52W_Fixed_FVF --- 
       ,L.T52W_Insertion_Fee ---- 
       ,L.T52W_Subscription_Fee ---- 
       ,L.T52W_PL_Fee,
	    M.T1W_AVG_TITLE_LENGTH,
		M.T1W_AVG_SUBTITLE_LENGTH,
		M.T1W_AVG_PHOTO_COUNT,
	   N.T52W_AVG_TITLE_LENGTH,
		N.T52W_AVG_SUBTITLE_LENGTH,
		N.T52W_AVG_PHOTO_COUNT,
		O.T1W_OUT_M2M_CNT,
		P.T52W_OUT_M2M_CNT,
		Q.T1W_INB_M2M_CNT,
		R.T52W_INB_M2M_CNT,
		---S.SLR_TYPE LATEST_SELLER_TYPE1,
		---T.T1W_PL_Net_Revenue
	---, T.T1W_PL_GMV
      T.T1W_PL_net_revenue_penetration_percnt
	, T.T1W_PLS_enabled_GMV_penetration_percnt
	, T.T1W_PLS_listing_adoption_percnt
	, T.T1W_PLA_listing_adoption_percnt,
	---U.T52W_PL_Net_Revenue
	 U.T52W_PL_net_revenue_penetration_percnt
	, U.T52W_PLS_enabled_GMV_penetration_percnt
	, U.T52W_PLS_listing_adoption_percnt
	, U.T52W_PLA_listing_adoption_percnt
	--V.T1W_pls_si,
    --V.T1W_pla_si,
    ,V.T1W_sfa_si
    ---V.T1W_pls_gmv,
    ----V.T1W_pla_gmv,
    ---V.T1W_sfa_gmv,
	,V.T1W_pls_ad_fee,
	V.T1W_pla_ad_fee, 
    V.T1W_sfa_ad_fee,
	V.T1W_total_ad_fee,
	V.T1W_pls_ad_fee_LC,
	V.T1W_pla_ad_fee_LC, 
    V.T1W_sfa_ad_fee_LC,
	V.T1W_total_ad_fee_LC,
	---V2.T52W_pls_si,
    ----V2.T52W_pla_si,
    V2.T52W_sfa_si,
--     V2.T52W_pls_gmv,
--     V2.T52W_pla_gmv,
--     V2.T52W_sfa_gmv,
	V2.T52W_pls_ad_fee,
	V2.T52W_pla_ad_fee, 
    V2.T52W_sfa_ad_fee,
	V2.T52W_total_ad_fee,
-- 	V2.T52W_pls_ad_fee_LC,
-- 	V2.T52W_pla_ad_fee_LC, 
--     V2.T52W_sfa_ad_fee_LC,
-- 	V2.T52W_total_ad_fee_LC,
	X.T1W_PLS_NET_REV
   ,X.T1W_PLS_GROSS_REV
   ,X.T1W_PLA_NET_REV
   ,X.T1W_PLA_GROSS_REV
   ,X.T1W_PLS_GMV
    ,X.T1W_PLS_SI
	   ,X.T1W_PLA_GMV
	   ,X.T1W_PLA_SI
-- 	   ,X.T1W_PL_REV
-- 	   , X.T1W_PL_GMV
	   --, X.T1W_PL_eligible_listings,
    	,X.T1W_PLS_Live_Listings,
    	X.T1W_PLA_Live_listings
	   , X.T1W_pls_sold_adrate,
	   X2.T52W_PLS_NET_REV
   ,X2.T52W_PLS_GROSS_REV
   ,X2.T52W_PLA_NET_REV
   ,X2.T52W_PLA_GROSS_REV
   ,X2.T52W_PLS_GMV
    ,X2.T52W_PLS_SI
	   ,X2.T52W_PLA_GMV
	   ,X2.T52W_PLA_SI,
-- 	   ,X2.T52W_PL_REV
-- 	   , X2.T52W_PL_GMV,
	   --, X2.T52W_PL_eligible_listings,
    	X2.T52W_PLS_Live_Listings,
    	X2.T52W_PLA_Live_listings
	   , X2.T52W_pls_sold_adrate,
	   X2.T52W_PLA_GMV/V2.T52W_pla_ad_fee t52w_pla_ROAS,
	   X2.T52W_PLS_GMV/V2.T52W_pls_ad_fee T52W_PLS_ROAS,
	   b.t52W_gmv/V2.T52W_total_ad_fee T52W_OVERALL_ROAS,
	    Y.T1W_GMB t1w_gmb_sellers_who_buy,
        Y.T1W_PURCHASES,
		Y2.T52W_GMB t52w_gmb_sellers_who_buy,
        Y2.T52W_PURCHASES,
		Z.T1W_byr_cnt,
		Z3.active_byr_cnt AS T1W_ACTV_BYR_CNT,
		Z3.NORB_BYR_CNT AS T1W_NORB_BYR_CNT,
--  		Z2.T52W_byr_cnt,
-- 		Z4.T1W_VIEW_COUNT,
-- 		Z5.T52W_VIEW_COUNT,
		AA.total_imprsns_curr T1W_TOTAL_imps,
		AA.total_clicks_CURR T1W_total_clicks,
		(AA.total_clicks_CURR/AA.total_imprsns_curr ) AS T1W_OVERALL_CTR,
		AA.pl_imps_curr T1W_pl_imps,
		AA.pl_clicks_curr T1W_pl_clicks,
		(AA.pl_clicks_curr /AA.pl_imps_curr ) AS T1W_PL_CTR,
		AA.organic_impressions_curr T1W_organic_impressions,
		AA.organic_clicks_curr T1W_organic_clicks,
		(AA.organic_clicks_curr /AA.organic_impressions_curr ) AS T1W_ORGANIC_CTR,
		AA.pla_imps_curr T1W_pla_imps,
		AA.pla_clicks_curr T1W_pla_clicks,
		(AA.pla_clicks_curr /AA.pla_imps_curr ) AS T1W_PLA_CTR,
		AB.t52w_organic_impressions_curr as t52w_organic_impressions,
		ab.t52w_organic_clicks_curr as t52w_organic_clicks,
		ab.t52w_organic_clicks_curr/AB.t52w_organic_impressions_curr as t52w_organic_ctr,
		AB.t52w_total_imprsns_curr,
		AB.t52w_total_clicks_curr,
	    (AB.t52w_total_clicks_curr /AB.t52w_total_imprsns_curr ) AS T52W_OVERALL_CTR,
		AB.t52w_pl_imps_curr,
		AB.t52w_pl_clicks_curr,
		(AB.t52w_pl_clicks_curr /AB.t52w_pl_imps_curr ) AS T52W_PL_CTR,
		AB.t52w_pla_imps_curr,
		AB.t52w_pla_clicks_curr,
		(AB.t52w_pla_clicks_curr /AB.t52w_pla_imps_curr ) AS T52W_PLA_CTR,
		AC.T1W_ads_fee_usd
		,AC.T1W_budget_amt_usd
		,AC.T1W_cmpgn_budget_usage,
		AD.T52W_ads_fee_usd
		,AD.T52W_budget_amt_usd
		,AD.T52W_cmpgn_budget_usage
-- 		,AE.T1W_FIN3_BI,
--       AE.T1W_PROM_3D_BI,
--       AE.T1W_ACTUAL_3D_BI,
--       AE.T1W_LATE_DELIVERY_BI,
-- 	  AE.T1W_LATE_DELIVERY_BI/AE.T1W_VALID_TRACKING_BI AS T1W_LDR,
--       AE.T1W_FREE_SHIPPING_TXNS,
--       AE.T1W_VALID_TRACKING_BI,
--       AE.T1W_TRACKING_UPLOAD_BI,
--       AE.T1W_HT_0_1D_TXNS,
-- 	  AE.T1W_INR_14D_TXNS,
-- 	  AE.T1W_ESC_INR_14D_TXNS
-- 	  ,AF.T52W_FIN3_BI,
--       AF.T52W_PROM_3D_BI,
--       AF.T52W_ACTUAL_3D_BI,
--       AF.T52W_LATE_DELIVERY_BI,
-- 	  AF.T52W_LATE_DELIVERY_BI/AF.T52W_VALID_TRACKING_BI AS T52W_LDR,
--       AF.T52W_FREE_SHIPPING_TXNS,
--       AF.T52W_VALID_TRACKING_BI,
--       AF.T52W_TRACKING_UPLOAD_BI,
--       AF.T52W_HT_0_1D_TXNS,
-- 	  AF.T52W_INR_14D_TXNS,
-- 	  AF.T52W_ESC_INR_14D_TXNS,
-- 	AG.T1W_LL_with_Free_in_3_pct
-- 	, AG.T1W_LL_1HD_pct
-- 	, AG.T1W_LL_next_day_pct
-- 	, AG.T1W_LL_tracked_services_pct
-- 	, AG.T1W_LL_0_3D_EDD_pct,
-- 	AH.LL_with_Free_in_3_pct AS T52W_LL_with_Free_in_3_pct
-- 	, AH.T52W_LL_1HD_pct
-- 	, AH.T52W_LL_next_day_pct
-- 	, AH.T52W_LL_tracked_services_pct
-- 	, AH.T1W_LL_0_3D_EDD_pct AS T52W_LL_0_3D_EDD_pct

FROM P_nishant_local_T.usr_cross_new_de BASE --- need to fix (fixed) changed base table
LEFT JOIN P_nishant_local_T.GMV_T1_METRICS_de A ----gtg
ON A.SLR_ID = BASE.SLR_ID
AND A.RETAIL_WEEK = BASE.RETAIL_WEEK
AND A.RETAIL_YEAR = BASE.RETAIL_YEAR
LEFT JOIN P_nishant_local_t.t1w_refurb_gmv_de A3
ON A3.SLR_ID = BASE.SLR_ID
AND A3.RETAIL_WEEK = BASE.RETAIL_WEEK
AND A3.RETAIL_YEAR = BASE.RETAIL_YEAR
LEFT JOIN P_nishant_local_t.t52w_refurb_gmv_de A4
ON A4.SLR_ID = BASE.SLR_ID
AND A4.RETAIL_WEEK = BASE.RETAIL_WEEK
AND A4.RETAIL_YEAR = BASE.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMV_T13_METRICS_de A2 ---gtg
ON A2.SLR_ID = BASE.SLR_ID
AND A2.RETAIL_WEEK = BASE.RETAIL_WEEK
AND A2.RETAIL_YEAR = BASE.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMV_T52_METRICS_de B ---gtg
ON BASE.SLR_ID = B.SLR_ID
AND BASE.RETAIL_WEEK = B.RETAIL_WEEK
AND BASE.RETAIL_YEAR = B.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LL_T1_METRICS_de C   -- need to fix (fixed)
ON BASE.SLR_ID = C.SLR_ID
AND BASE.RETAIL_WEEK = C.RETAIL_WEEK
AND BASE.RETAIL_YEAR = C.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LL_T52_METRICS_de D --- need to fix (fixed)
ON BASE.SLR_ID = D.SLR_ID 
AND BASE.RETAIL_WEEK = D.RETAIL_WEEK
AND BASE.RETAIL_YEAR = D.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_DEFECT_METRICS_de E --- gtg
ON BASE.SLR_ID = E.SLR_ID
AND BASE.RETAIL_WEEK = E.RETAIL_WEEK
AND BASE.RETAIL_YEAR = E.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_DEFECT_METRICS_de F --- gtg
ON BASE.SLR_ID = F.SLR_ID
AND BASE.RETAIL_WEEK = F.RETAIL_WEEK
AND BASE.RETAIL_YEAR = F.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS_de G -- gtg
ON BASE.SLR_ID = G.USER_ID
AND BASE.RETAIL_WEEK = G.RETAIL_WEEK
AND BASE.RETAIL_YEAR = G.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.STORE_KPI_METRICS_de H --- gtg
ON BASE.SLR_ID = H.SLR_ID
AND BASE.RETAIL_WEEK = H.RETAIL_WEEK
AND BASE.RETAIL_YEAR = H.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52_STORE_KPI_METRICS_de H2 --- gtg
ON BASE.SLR_ID = H2.SLR_ID
AND BASE.RETAIL_WEEK = H2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = H2.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_NSH_USAGE_METRICS I --- non de
ON BASE.SLR_ID = I.SLR_ID
AND BASE.RETAIL_WEEK = I.RETAIL_WEEK
AND BASE.RETAIL_YEAR = I.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_NSH_USAGE_METRICS_de J -- gtg
ON BASE.SLR_ID = J.SLR_ID
AND BASE.RETAIL_WEEK = J.RETAIL_WEEK
AND BASE.RETAIL_YEAR = J.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN  P_nishant_local_T.T13W_FEE_de K --- gtg
ON BASE.SLR_ID = K.SLR_ID
AND BASE.RETAIL_WEEK = K.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K.RETAIL_YEAR
left join P_nishant_local_t.fvf_t1_de k2
ON BASE.SLR_ID = K2.SLR_ID
AND BASE.RETAIL_WEEK = K2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K2.RETAIL_YEAR
left join P_nishant_local_t.fvf_t13_de k3
ON BASE.SLR_ID = K3.SLR_ID
AND BASE.RETAIL_WEEK = K3.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K3.RETAIL_YEAR
left join P_nishant_local_t.fvf_t52_de k4
ON BASE.SLR_ID = K4.SLR_ID
AND BASE.RETAIL_WEEK = K4.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K4.RETAIL_YEAR
left join P_nishant_local_t.t1w_coupon_metrics_de K5
ON BASE.SLR_ID = K5.SELLER_ID
AND BASE.RETAIL_WEEK = K5.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K5.RETAIL_YEAR
left join P_nishant_local_t.t52w_coupon_metrics_de K6
ON BASE.SLR_ID = K6.SELLER_ID
AND BASE.RETAIL_WEEK = K6.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K6.RETAIL_YEAR
LEFT JOIN  P_nishant_local_T.T52W_FEE_de L --- gtg
ON BASE.SLR_ID = L.SLR_ID
AND BASE.RETAIL_WEEK = L.RETAIL_WEEK
AND BASE.RETAIL_YEAR = L.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T1W_LISTING_METRICS_de M ---- gtg
ON BASE.SLR_ID = M.SLR_ID
AND BASE.RETAIL_WEEK = M.RETAIL_WEEK
AND BASE.RETAIL_YEAR = M.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T52W_LISTING_METRICS_de N --- gtg
ON BASE.SLR_ID = N.SLR_ID
AND BASE.RETAIL_WEEK = N.RETAIL_WEEK
AND BASE.RETAIL_YEAR = N.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T1W_OUTB_METRICS O ---- non de
ON BASE.SLR_ID = O.ID
AND BASE.RETAIL_WEEK = O.RETAIL_WEEK
AND BASE.RETAIL_YEAR = O.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T52W_OUTB_METRICS_de P -- gtg
ON BASE.SLR_ID = P.SLR_ID
AND BASE.RETAIL_WEEK = P.RETAIL_WEEK
AND BASE.RETAIL_YEAR = P.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T1W_INB_METRICS Q --- non de
ON BASE.SLR_ID = Q.ID
AND BASE.RETAIL_WEEK = Q.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Q.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T52W_INB_METRICS_de R --- gtg
ON BASE.SLR_ID = R.SLR_ID
AND BASE.RETAIL_WEEK = R.RETAIL_WEEK
AND BASE.RETAIL_YEAR = R.RETAIL_YEAR
-- LEFT JOIN p_NISHANT_LOCAL_T.new_or_reactivated_seller_de S ---gtg
-- ON BASE.SLR_ID = S.SLR_ID
-- AND BASE.RETAIL_WEEK = S.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = S.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS T ---non de
ON BASE.SLR_ID = T.SLR_ID
AND BASE.RETAIL_WEEK = T.RETAIL_WEEK
AND BASE.RETAIL_YEAR = T.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS_de U --- gtg
ON BASE.SLR_ID = U.SLR_ID
AND BASE.RETAIL_WEEK = U.RETAIL_WEEK
AND BASE.RETAIL_YEAR = U.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T1W_pl_METRICS V  ---non de
ON BASE.SLR_ID = V.SELLER_ID
AND BASE.RETAIL_WEEK = V.RETAIL_WEEK
AND BASE.RETAIL_YEAR = V.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T52W_pl_METRICS_de V2 ---gtg
ON BASE.SLR_ID = V2.SELLER_ID
AND BASE.RETAIL_WEEK = V2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = V2.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.latest_seller_type_de W ----gtg
ON BASE.SLR_ID = W.SLR_ID
AND MONTH(BASE.RETAIL_WK_END_DATE) = W.CAL_MONTH
AND BASE.RETAIL_YEAR = W.CAL_YEAR
LEFT JOIN p_NISHANT_LOCAL_t.T1W_PL_METRICS_2_de X ----gtg
ON BASE.SLR_ID = X.SLR_ID
AND BASE.RETAIL_WEEK = X.RETAIL_WEEK
AND BASE.RETAIL_YEAR = X.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_t.T52W_PL_METRICS_2_de X2 ---gtg
ON BASE.SLR_ID = X2.SLR_ID
AND BASE.RETAIL_WEEK = X2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = X2.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMB_T1_METRICS_de Y ---gtg
ON BASE.SLR_ID = Y.BYR_ID
AND BASE.RETAIL_WEEK = Y.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Y.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMB_T52_METRICS_de Y2 --gtg
ON BASE.SLR_ID = Y2.SLR_ID
AND BASE.RETAIL_WEEK = Y2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Y2.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.BYR_T1_METRICS_de Z ---gtg
ON BASE.SLR_ID = Z.SLR_ID
AND BASE.RETAIL_WEEK = Z.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Z.RETAIL_YEAR
-- LEFT JOIN P_nishant_local_T.BYR_T52_METRICS_de Z2 --- need to fix (fixed)
-- ON BASE.SLR_ID = Z2.SLR_ID
-- AND BASE.RETAIL_WEEK = Z2.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = Z2.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.BYR_NORB_METRIC_de Z3
ON BASE.SLR_ID = Z3.SLR_ID
AND BASE.RETAIL_WEEK = Z3.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Z3.RETAIL_YEAR
-- LEFT JOIN P_NISHANT_LOCAL_T.T1W_VIEW_CNT Z4
-- ON BASE.SLR_ID = Z4.SLR_ID
-- AND BASE.RETAIL_WEEK = Z4.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = Z4.RETAIL_YEAR
-- LEFT JOIN  P_NISHANT_LOCAL_T.T52W_VIEW_CNT Z5
-- ON BASE.SLR_ID = Z5.SLR_ID
-- AND BASE.RETAIL_WEEK = Z5.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = Z5.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.AD_T1_METRICS AA --- non de
ON BASE.SLR_ID = AA.SLR_ID
AND BASE.RETAIL_WEEK = AA.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AA.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.AD_T52w_METRICS_de AB ---gtg
ON BASE.SLR_ID = AB.SLR_ID
AND BASE.RETAIL_WEEK = AB.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AB.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS AC ---non de
ON BASE.SLR_ID = AC.SELLER_ID
AND BASE.RETAIL_WEEK = AC.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AC.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_T.T52W_BUDGET_METRICS_de AD --- need to fix (fixed)
ON BASE.SLR_ID = AD.SLR_ID
AND BASE.RETAIL_WEEK = AD.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AD.RETAIL_YEAR
-- LEFT JOIN P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS AE ---gtg
-- ON BASE.SLR_ID = AE.SlR_ID
-- AND BASE.RETAIL_WEEK = AE.TXN_RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = AE.TXN_RETAIL_YEAR
-- LEFT JOIN P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS AF --- gtg
-- ON BASE.SLR_ID = AF.SLR_ID
-- AND BASE.RETAIL_WEEK = AF.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = AF.RETAIL_YEAR
-- LEFT JOIN P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS_2 AG ---gtg
-- ON BASE.SLR_ID = AG.SLR_ID
-- AND BASE.RETAIL_WEEK = AG.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = AG.RETAIL_YEAR
-- LEFT JOIN P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS_2 AH ---gtg
-- ON BASE.SLR_ID = AH.SLR_ID
-- AND BASE.RETAIL_WEEK = AH.RETAIL_WEEK
-- AND BASE.RETAIL_YEAR = AH.RETAIL_YEAR
LEFT JOIN P_nishant_local_t.seller_address_de AI
ON BASE.SLR_ID = AI.SELLER_ID
LEFT JOIN P_nishant_local_t.dominant_vertical_gmv_de Aj
ON BASE.SLR_ID = Aj.SLR_ID
AND BASE.RETAIL_WEEK = AJ.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AJ.RETAIL_YEAR
LEFT JOIN P_nishant_local_t.linked_accs_final_de AJ2
ON BASE.SLR_ID = Aj2.USR_CTRL
LEFT JOIN P_nishant_local_t.t1w_gsp_metrics_de GSP
ON BASE.SLR_ID = GSP.SLR_ID
AND BASE.RETAIL_WEEK = GSP.RETAIL_WEEK
AND BASE.RETAIL_YEAR = GSP.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.t52w_gsp_metrics_de GSP2
ON BASE.SLR_ID = GSP2.SLR_ID
AND BASE.RETAIL_WEEK = GSP2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = GSP2.RETAIL_YEAR
left join P_nishant_local_t.ALL_B2C_TXN_T365D_de nors
ON BASE.SLR_ID =nors.SLR_ID 
AND BASE.RETAIL_WEEK =  nors.RETAIL_WEEK  
AND  BASE.RETAIL_YEAR = nors.RETAIL_YEAR 
;
--;

---- temp table to move only sellers with t52w_gmv>0
-- DROP TABLE IF EXISTS P_nishant_local_T.final_metrics_1_de;
create table P_nishant_local_T.final_metrics_1_de as 
select *
from P_nishant_local_T.final_metrics_2_de;

-- final table with only active sellers
DROP TABLE IF EXISTS P_nishant_local_T.final_metrics_2_de;
create table P_nishant_local_T.final_metrics_2_de as 
select *
from P_nishant_local_T.final_metrics_1_de
where t52w_gmv>0;

-- select sum(t52w_gmv)
-- from P_nishant_local_T.final_metrics_2
-- where retail_week = 39
-- and retail_year = 2024

-- drop table if exists P_nishant_local_T.final_metrics_2;
-- create table P_nishant_local_T.final_metrics_2 as 
-- select nors.seller_type, a.* 
-- from P_nishant_local_T.final_metrics_1 a 
-- left join P_nishant_local_t.ALL_B2C_TXN_T365D_uk nors
-- ON a.SLR_ID =nors.SLR_ID 
-- AND a.RETAIL_WEEK =  nors.RETAIL_WEEK  
-- AND  a.RETAIL_YEAR = nors.RETAIL_YEAR 
--  ;


-- left join P_nishant_local_t.ALL_B2C_TXN_T365D_uk nors
-- ON BASE.SLR_ID =nors.SLR_ID 
-- AND BASE.RETAIL_WEEK =  nors.RETAIL_WEEK  
-- AND  BASE.RETAIL_YEAR = nors.RETAIL_YEAR 


--- dropping tables
-- drop table if exists P_nishant_local_T.GMV_T1_METRICS;
-- drop table if exists  P_nishant_local_t.t1w_refurb_gmv;
-- drop table if exists  P_nishant_local_t.t52w_refurb_gmv;
-- drop table if exists  P_nishant_local_T.GMV_T13_METRICS;
-- drop table if exists  P_nishant_local_T.GMV_T52_METRICS;
-- drop table if exists  P_nishant_local_T.LL_T1_METRICS;   -- need to fix (fixed)
-- drop table if exists  P_nishant_local_T.T1W_DEFECT_METRICS; --- gtg
-- drop table if exists  P_nishant_local_T.T52W_DEFECT_METRICS; --- gtg
-- drop table if exists  P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS; -- gtg
-- drop table if exists  P_nishant_local_T.STORE_KPI_METRICS; --- gtg
-- drop table if exists  P_nishant_local_T.T1W_NSH_USAGE_METRICS; --- gtg
-- drop table if exists  P_nishant_local_T.T52W_NSH_USAGE_METRICS; -- gtg
-- drop table if exists  P_nishant_local_T.T13W_FEE; --- gtg
-- drop table if exists  P_nishant_local_t.fvf_t1; 
-- drop table if exists  P_nishant_local_t.fvf_t13; 
-- drop table if exists  P_nishant_local_t.fvf_t52;
-- drop table if exists P_nishant_local_t.t1w_coupon_metrics;
-- drop table if exists  P_nishant_local_t.t52w_coupon_metrics;
-- drop table if exists  P_nishant_local_T.T52W_FEE; --- gtg
-- drop table if exists  P_nishant_local_T.T1W_LISTING_METRICS; ---- gtg
-- drop table if exists  P_nishant_local_T.T1W_OUTB_METRICS; ---- gtg
-- drop table if exists  P_nishant_local_T.T52W_OUTB_METRICS; -- gtg
-- drop table if exists  P_nishant_local_T.T1W_INB_METRICS; --- gtg
-- drop table if exists P_nishant_local_T.T52W_INB_METRICS; --- gtg
-- drop table if exists  p_NISHANT_LOCAL_T.new_or_reactivated_seller; ---gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS; ---gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS; --- gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T1W_pl_METRICS;  ---gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T52W_pl_METRICS; ---gtg
-- drop table if exists  P_NISHANT_LOCAL_T.latest_seller_type; ----gtg
-- drop table if exists  p_NISHANT_LOCAL_t.T1W_PL_METRICS_2; ----gtg
-- drop table if exists  p_NISHANT_LOCAL_t.T52W_PL_METRICS_2; ---gtg
-- drop table if exists  P_nishant_local_T.GMB_T1_METRICS; ---gtg
-- drop table if exists  P_nishant_local_T.GMB_T52_METRICS; --gtg
-- drop table if exists  P_nishant_local_T.BYR_T1_METRICS; ---gtg
-- drop table if exists  P_nishant_local_T.BYR_T52_METRICS; --- need to fix (fixed)
-- drop table if exists  P_nishant_local_T.BYR_NORB_METRIC;
-- drop table if exists  P_nishant_local_T.AD_T1_METRICS; --- gtg
-- drop table if exists  P_nishant_local_T.AD_T52w_METRICS; ---gtg
-- drop table if exists  p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS; ---gtg
-- drop table if exists  p_NISHANT_LOCAL_T.T52W_BUDGET_METRICS;--- need to fix (fixed)
-- drop table if exists  P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS; ---gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS; --- gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS_2; ---gtg
-- drop table if exists  P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS_2; ---gtg
-- drop table if exists  P_nishant_local_t.seller_address;
-- drop table if exists  P_nishant_local_t.dominant_vertical_gmv;
-- drop table if exists  P_nishant_local_t.linked_accs_final;
-- drop table if exists  P_nishant_local_t.t1w_gsp_metrics;
-- drop table if exists  P_nishant_local_T.t52w_gsp_metrics;




-- yoy table
drop table if exists p_nishant_local_t.yoy_change_2_us;
create table p_nishant_local_t.yoy_change_2_us as
SELECT 
a.RETAIL_YEAR, a.RETAIL_WEEK,
a.RETAIL_WK_END_DATE, 
a.SLR_ID,
a.seller_type,
a.LATEST_SELLER_TYPE2,
a.LATEST_SELLER_TYPE3,
a.SELLER_addrress, 
a.ZIP,
a.t1w_dominant_vertical,
a.t52w_dominant_vertical,
(coalesce(a.T1W_GMV,0) - coalesce(b.T1W_GMV,0))/coalesce(b.T1W_GMV,0) as T1W_GMV_yoy,
-- (coalesce(a.T1W_GMV_LC,0) - coalesce(b.T1W_GMV_LC,0))/coalesce(b.T1W_GMV_LC,0) T1W_GMV_LC_yoy,
(coalesce(a.T1W_gsp_GMV,0) - coalesce(b.T1W_gsp_GMV,0))/coalesce(b.T1W_gsp_GMV,0) T1W_gsp_GMV_yoy,
(coalesce(a.T1W_SI,0) - coalesce(b.T1W_SI,0))/coalesce(b.T1W_SI,0) T1W_SI_yoy,
(coalesce(a.T1W_asp,0) - coalesce(b.T1W_asp,0))/coalesce(b.T1W_asp,0) T1W_asp_yoy,
(coalesce(a.T1W_CBT_GMV,0) - coalesce(b.T1W_CBT_GMV,0))/coalesce(b.T1W_CBT_GMV,0) T1W_CBT_GMV_yoy,
(coalesce(a.T1W_CBT_SI,0) - coalesce(b.T1W_CBT_SI,0))/coalesce(b.T1W_CBT_SI,0) T1W_CBT_SI_yoy,
(coalesce(a.T1W_CBT_ASP,0) - coalesce(b.T1W_CBT_ASP,0))/coalesce(b.T1W_CBT_ASP,0) T1W_CBT_ASP_yoy,
(coalesce(a.T1W_CBT_PERC,0) - coalesce(b.T1W_CBT_PERC,0))/coalesce(b.T1W_CBT_PERC,0) T1W_CBT_PERC_yoy,
(coalesce(a.T1W_new_item_GMV,0) - coalesce(b.T1W_new_item_GMV,0))/coalesce(b.T1W_new_item_GMV,0) T1W_new_item_GMV_yoy,
(coalesce(a.T1W_refurb_item_GMV,0) - coalesce(b.T1W_refurb_item_GMV,0))/coalesce(b.T1W_refurb_item_GMV,0) T1W_refurb_item_GMV_yoy,
(coalesce(a.T1W_used_item_GMV,0) - coalesce(b.T1W_used_item_GMV,0))/coalesce(b.T1W_used_item_GMV,0) T1W_used_item_GMV_yoy,
(coalesce(a.T52W_new_item_GMV,0) - coalesce(b.T52W_new_item_GMV,0))/coalesce(b.T52W_new_item_GMV,0)  T52W_new_item_GMV_yoy,
(coalesce(a.T52W_refurb_item_GMV,0) - coalesce(b.T52W_refurb_item_GMV,0))/coalesce(b.T52W_refurb_item_GMV,0)  T52W_refurb_item_GMV_yoy,
(coalesce(a.T52W_used_item_GMV,0) - coalesce(b.T52W_used_item_GMV,0))/coalesce(b.T52W_used_item_GMV,0) T52W_used_item_GMV_yoy,
(coalesce(a.T52W_GMV,0) - coalesce(b.T52W_GMV,0))/coalesce(b.T52W_GMV,0) T52W_GMV_yoy,
-- (coalesce(a.T52W_GMV_LC,0) - coalesce(b.T52W_GMV_LC,0))/coalesce(b.T52W_GMV_LC,0) T52W_GMV_LC_yoy,
(coalesce(a.T52W_gsp_GMV,0) - coalesce(b.T52W_gsp_GMV,0))/coalesce(b.T52W_gsp_GMV,0) T52W_gsp_GMV_yoy,
(coalesce(a.T52W_SI,0) - coalesce(b.T52W_SI,0))/coalesce(b.T52W_SI,0) T52W_SI_yoy,
(coalesce(a.T52W_ASP,0) - coalesce(b.T52W_ASP,0))/coalesce(b.T52W_ASP,0) T52W_ASP_yoy,
(coalesce(a.T52W_CBT_GMV,0) - coalesce(b.T52W_CBT_GMV,0))/coalesce(b.T52W_CBT_GMV,0) T52W_CBT_GMV_yoy,
(coalesce(a.T52W_CBT_SI,0) - coalesce(b.T52W_CBT_SI,0))/coalesce(b.T52W_CBT_SI,0) T52W_CBT_SI_yoy,
(coalesce(a.T52W_CBT_ASP,0) - coalesce(b.T52W_CBT_ASP,0))/coalesce(b.T52W_CBT_ASP,0) T52W_CBT_ASP_yoy,
(coalesce(a.T52W_CBT_PERC,0) - coalesce(b.T52W_CBT_PERC,0))/coalesce(b.T52W_CBT_PERC,0) T52W_CBT_PERC_yoy,
(coalesce(a.T13W_GMV,0) - coalesce(b.T13W_GMV,0))/coalesce(b.T13W_GMV,0) T13W_GMV_yoy,
(coalesce(a.T13W_SI,0) - coalesce(b.T13W_SI,0))/coalesce(b.T13W_SI,0) T13W_SI_yoy,
(coalesce(a.T13W_ASP,0) - coalesce(b.T13W_ASP,0))/coalesce(b.T13W_ASP,0) T13W_ASP_yoy,
(coalesce(a.T13W_CBT_GMV,0) - coalesce(b.T13W_CBT_GMV,0))/coalesce(b.T13W_CBT_GMV,0) T13W_CBT_GMV_yoy,
(coalesce(a.T13W_CBT_SI,0) - coalesce(b.T13W_CBT_SI,0))/coalesce(b.T13W_CBT_SI,0) T13W_CBT_SI_yoy,
(coalesce(a.T13W_CBT_ASP,0) - coalesce(b.T13W_CBT_ASP,0))/coalesce(b.T13W_CBT_ASP,0) T13W_CBT_ASP_yoy,
(coalesce(a.T1W_LL,0) - coalesce(b.T1W_LL,0))/coalesce(b.T1W_LL,0) T1W_LL_yoy, 
(coalesce(a.t1w_avg_start_price,0) - coalesce(b.t1w_avg_start_price,0))/coalesce(b.t52w_avg_start_price,0) t1w_avg_start_price_yoy,
(coalesce(a.T1W_RELIST_LL,0) - coalesce(b.T1W_RELIST_LL,0))/coalesce(b.T1W_RELIST_LL,0) T1W_RELIST_LL_yoy,
(coalesce(a.T1W_FNL_LL,0) - coalesce(b.T1W_FNL_LL,0))/coalesce(b.T1W_FNL_LL,0) T1W_FNL_LL_yoy,
(coalesce(a.T52W_LL,0) - coalesce(b.T52W_LL,0))/coalesce(b.T52W_LL,0) T52W_LL_yoy,
(coalesce(a.t52w_avg_start_price,0) - coalesce(b.t52w_avg_start_price,0))/coalesce(b.t52w_avg_start_price,0) t52w_avg_start_price_yoy,
(coalesce(a.T52W_RELIST_LL,0) - coalesce(b.T52W_RELIST_LL,0))/coalesce(b.T52W_RELIST_LL,0) T52W_RELIST_LL_yoy,
(coalesce(a.T52W_FNL_LL,0) - coalesce(b.T52W_FNL_LL,0))/coalesce(b.T52W_FNL_LL,0) T52W_FNL_LL_yoy,
(coalesce(a.T1W_escal_SNAD_count,0) - coalesce(b.T1W_escal_SNAD_count,0))/coalesce(b.T1W_escal_SNAD_count,0) T1W_escal_SNAD_count_yoy,
(coalesce(a.T1W_non_escal_SNAD_count,0) - coalesce(b.T1W_non_escal_SNAD_count,0))/coalesce(b.T1W_non_escal_SNAD_count,0) T1W_non_escal_SNAD_count_yoy,
(coalesce(a.T1W_STOCKOUT_count,0) - coalesce(b.T1W_STOCKOUT_count,0))/coalesce(b.T1W_STOCKOUT_count,0) T1W_STOCKOUT_count_yoy,
(coalesce(a.T1W_escal_INR_count,0) - coalesce(b.T1W_escal_INR_count,0))/coalesce(b.T1W_escal_INR_count,0) T1W_escal_INR_count_yoy,
(coalesce(a.T1W_non_escal_INR_count,0) - coalesce(b.T1W_non_escal_INR_count,0))/coalesce(b.T1W_non_escal_INR_count,0) T1W_non_escal_INR_count_yoy,
(coalesce(a.T1W_low_IAD_DSR_count,0) - coalesce(b.T1W_low_IAD_DSR_count,0))/coalesce(b.T1W_low_IAD_DSR_count,0) T1W_low_IAD_DSR_count_yoy,
(coalesce(a.T1W_NN_feedback_count,0) - coalesce(b.T1W_NN_feedback_count,0))/coalesce(b.T1W_NN_feedback_count,0) T1W_NN_feedback_count_yoy,
(coalesce(a.T1W_Non_escal_SNAD_MSG_count,0) - coalesce(b.T1W_Non_escal_SNAD_MSG_count,0))/coalesce(b.T1W_Non_escal_SNAD_MSG_count,0) T1W_Non_escal_SNAD_MSG_count_yoy,
(coalesce(a.T1W_Non_escal_INR_MSG_count,0) - coalesce(b.T1W_Non_escal_INR_MSG_count,0))/coalesce(b.T1W_Non_escal_INR_MSG_count,0) T1W_Non_escal_INR_MSG_count_yoy,
(coalesce(a.T52W_escal_SNAD_count,0) - coalesce(b.T52W_escal_SNAD_count,0))/coalesce(b.T52W_escal_SNAD_count,0) T52W_escal_SNAD_count_yoy,
(coalesce(a.T52W_non_escal_SNAD_count,0) - coalesce(b.T52W_non_escal_SNAD_count,0))/coalesce(b.T52W_non_escal_SNAD_count,0) T52W_non_escal_SNAD_count_yoy,
(coalesce(a.T52W_STOCKOUT_count,0) - coalesce(b.T52W_STOCKOUT_count,0))/coalesce(b.T52W_STOCKOUT_count,0) T52W_STOCKOUT_count_yoy,
(coalesce(a.T52W_escal_INR_count,0) - coalesce(b.T52W_escal_INR_count,0))/coalesce(b.T52W_escal_INR_count,0) T52W_escal_INR_count_yoy,
(coalesce(a.T52W_non_escal_INR_count,0) - coalesce(b.T52W_non_escal_INR_count,0))/coalesce(b.T52W_non_escal_INR_count,0) T52W_non_escal_INR_count_yoy,
(coalesce(a.T52W_low_IAD_DSR_count,0) - coalesce(b.T52W_low_IAD_DSR_count,0))/coalesce(b.T52W_low_IAD_DSR_count,0) T52W_low_IAD_DSR_count_yoy,
(coalesce(a.T52W_NN_feedback_count,0) - coalesce(b.T52W_NN_feedback_count,0))/coalesce(b.T52W_NN_feedback_count,0) T52W_NN_feedback_count_yoy,
(coalesce(a.T52W_Non_escal_SNAD_MSG_count,0) - coalesce(b.T52W_Non_escal_SNAD_MSG_count,0))/coalesce(b.T52W_Non_escal_SNAD_MSG_count,0) T52W_Non_escal_SNAD_MSG_count_yoy,
(coalesce(a.T52W_Non_escal_INR_MSG_count,0) - coalesce(b.T52W_Non_escal_INR_MSG_count,0))/coalesce(b.T52W_Non_escal_INR_MSG_count,0) T52W_Non_escal_INR_MSG_count_yoy,
(coalesce(a.STARTER_STORE_COUNT,0) - coalesce(b.STARTER_STORE_COUNT,0))/coalesce(b.STARTER_STORE_COUNT,0) STARTER_STORE_COUNT_yoy,
(coalesce(a.BASIC_STORE_COUNT,0) - coalesce(b.BASIC_STORE_COUNT,0))/coalesce(b.BASIC_STORE_COUNT,0) BASIC_STORE_COUNT_yoy,
(coalesce(a.FEATURE_STORE_COUNT,0) - coalesce(b.FEATURE_STORE_COUNT,0))/coalesce(b.FEATURE_STORE_COUNT,0) FEATURE_STORE_COUNT_yoy,
(coalesce(a.ANCHOR_STORE_COUNT,0) - coalesce(b.ANCHOR_STORE_COUNT,0))/coalesce(b.ANCHOR_STORE_COUNT,0) ANCHOR_STORE_COUNT_yoy,
(coalesce(a.ENTERPRISE_STORE_COUNT,0) - coalesce(b.ENTERPRISE_STORE_COUNT,0))/coalesce(b.ENTERPRISE_STORE_COUNT,0) ENTERPRISE_STORE_COUNT_yoy,
(coalesce(a.T52W_BASIC_STORE_COUNT,0) - coalesce(b.T52W_BASIC_STORE_COUNT,0))/coalesce(b.T52W_BASIC_STORE_COUNT,0) T52W_BASIC_STORE_COUNT_yoy,
(coalesce(a.T52W_STARTER_STORE_COUNT,0) - coalesce(b.T52W_STARTER_STORE_COUNT,0))/coalesce(b.T52W_STARTER_STORE_COUNT,0) T52W_STARTER_STORE_COUNT_yoy,
(coalesce(a.T52W_FEATURE_STORE_COUNT,0) - coalesce(b.T52W_FEATURE_STORE_COUNT,0))/coalesce(b.T52W_FEATURE_STORE_COUNT,0) T52W_FEATURE_STORE_COUNT_yoy,
(coalesce(a.T52W_ANCHOR_STORE_COUNT,0) - coalesce(b.T52W_ANCHOR_STORE_COUNT,0))/coalesce(b.T52W_ANCHOR_STORE_COUNT,0) T52W_ANCHOR_STORE_COUNT_yoy,
(coalesce(a.T52W_ENTERPRISE_STORE_COUNT,0) - coalesce(b.T52W_ENTERPRISE_STORE_COUNT,0))/coalesce(b.T52W_ENTERPRISE_STORE_COUNT,0) T52W_ENTERPRISE_STORE_COUNT_yoy,
(coalesce(a.T1W_SELLERH_CNT,0) - coalesce(b.T1W_SELLERH_CNT,0))/coalesce(b.T1W_SELLERH_CNT,0) T1W_SELLERH_CNT_yoy,
(coalesce(a.T1W_NATIVE_CNT,0) - coalesce(b.T1W_NATIVE_CNT,0))/coalesce(b.T1W_NATIVE_CNT,0) T1W_NATIVE_CNT_yoy,
(coalesce(a.T52W_SELLERH_CNT,0) - coalesce(b.T52W_SELLERH_CNT,0))/coalesce(b.T52W_SELLERH_CNT,0) T52W_SELLERH_CNT_yoy,
(coalesce(a.T52W_NATIVE_CNT,0) - coalesce(b.T52W_NATIVE_CNT,0))/coalesce(b.T52W_NATIVE_CNT,0) T52W_NATIVE_CNT_yoy,
(coalesce(a.t1w_total_spend_usd,0) - coalesce(b.t1w_total_spend_usd,0))/coalesce(b.t1w_total_spend_usd,0) t1w_total_spend_usd_yoy,
(coalesce(a.t1w_ebay_spend_usd,0) - coalesce(b.t1w_ebay_spend_usd,0))/coalesce(b.t1w_ebay_spend_usd,0) t1w_ebay_spend_usd_yoy,
(coalesce(a.t1w_seller_coupon_spend_usd, 0) - coalesce(b.t1w_seller_coupon_spend_usd, 0))/coalesce(b.t1w_seller_coupon_spend_usd, 0) t1w_seller_coupon_spend_usd_yoy,
(coalesce(a.t52w_total_spend_usd, 0) - coalesce(b.t52w_total_spend_usd, 0))/coalesce(b.t52w_total_spend_usd, 0) t52w_total_spend_usd_yoy,
(coalesce(a.t52w_ebay_spend_usd, 0) - coalesce(b.t52w_ebay_spend_usd, 0))/coalesce(b.t52w_ebay_spend_usd, 0) t52w_ebay_spend_usd_yoy,
(coalesce(a.t52w_seller_coupon_spend_usd, 0) - coalesce(b.t52w_seller_coupon_spend_usd, 0))/coalesce(b.t52w_seller_coupon_spend_usd, 0) t52w_seller_coupon_spend_usd_yoy,
-- (coalesce(a.t1w_gross_fvf_lc, 0) - coalesce(b.t1w_gross_fvf_lc, 0))/coalesce(b.t1w_gross_fvf_lc, 0) t1w_gross_fvf_lc_yoy,
(coalesce(a.t1w_gross_fvf, 0) - coalesce(b.t1w_gross_fvf, 0))/coalesce(b.t1w_gross_fvf, 0) t1w_gross_fvf_yoy,
-- (coalesce(a.t1w_net_fvf_lc, 0) - coalesce(b.t1w_net_fvf_lc, 0))/coalesce(b.t1w_net_fvf_lc, 0) t1w_net_fvf_lc_yoy,
(coalesce(a.t1w_net_fvf, 0) - coalesce(b.t1w_net_fvf, 0))/coalesce(b.t1w_net_fvf, 0) t1w_net_fvf_yoy,
-- (coalesce(a.t13w_gross_fvf_lc, 0) - coalesce(b.t13w_gross_fvf_lc, 0))/coalesce(b.t13w_gross_fvf_lc, 0) t13w_gross_fvf_lc_yoy,
(coalesce(a.t13w_gross_fvf, 0) - coalesce(b.t13w_gross_fvf, 0))/coalesce(b.t13w_gross_fvf, 0) t13w_gross_fvf_yoy,
-- (coalesce(a.t13w_net_fvf_lc, 0) - coalesce(b.t13w_net_fvf_lc, 0))/coalesce(b.t13w_net_fvf_lc, 0) t13w_net_fvf_lc_yoy,
(coalesce(a.t13w_net_fvf, 0) - coalesce(b.t13w_net_fvf, 0))/coalesce(b.t13w_net_fvf, 0) t13w_net_fvf_yoy,
(coalesce(a.T13W_FVF_TAKE_RATE, 0) - coalesce(b.T13W_FVF_TAKE_RATE, 0))/coalesce(b.T13W_FVF_TAKE_RATE, 0) T13W_FVF_TAKE_RATE_yoy,
-- (coalesce(a.t52w_gross_fvf_lc, 0) - coalesce(b.t52w_gross_fvf_lc, 0))/coalesce(b.t52w_gross_fvf_lc, 0) t52w_gross_fvf_lc_yoy,
(coalesce(a.t52w_gross_fvf, 0) - coalesce(b.t52w_gross_fvf, 0))/coalesce(b.t52w_gross_fvf, 0) t52w_gross_fvf_yoy,
-- (coalesce(a.t52w_net_fvf_lc, 0) - coalesce(b.t52w_net_fvf_lc, 0))/coalesce(b.t52w_net_fvf_lc, 0) t52w_net_fvf_lc_yoy,
(coalesce(a.t52w_net_fvf, 0) - coalesce(b.t52w_net_fvf, 0))/coalesce(b.t52w_net_fvf, 0) t52w_net_fvf_yoy,
(coalesce(a.T52_fvf_TAKE_RATE, 0) - coalesce(b.T52_fvf_TAKE_RATE, 0))/coalesce(b.T52_fvf_TAKE_RATE, 0) T52_fvf_TAKE_RATE_yoy,
(coalesce(a.T13W_Insertion_Fee, 0) - coalesce(b.T13W_Insertion_Fee, 0))/coalesce(b.T13W_Insertion_Fee, 0) T13W_Insertion_Fee_yoy,
(coalesce(a.T13W_Subscription_Fee, 0) - coalesce(b.T13W_Subscription_Fee, 0))/coalesce(b.T13W_Subscription_Fee, 0) T13W_Subscription_Fee_yoy,
(coalesce(a.T13W_PL_Fee, 0) - coalesce(b.T13W_PL_Fee, 0))/coalesce(b.T13W_PL_Fee, 0) T13W_PL_Fee_yoy,
-- (coalesce(a.T52W_Variable_FVF, 0) - coalesce(b.T52W_Variable_FVF, 0))/coalesce(b.T52W_Variable_FVF, 0) T52W_Variable_FVF_yoy,
-- (coalesce(a.T52W_Fixed_FVF, 0) - coalesce(b.T52W_Fixed_FVF, 0))/coalesce(b.T52W_Fixed_FVF, 0) T52W_Fixed_FVF_yoy,
(coalesce(a.T52W_Insertion_Fee, 0) - coalesce(b.T52W_Insertion_Fee, 0))/coalesce(b.T52W_Insertion_Fee, 0) T52W_Insertion_Fee_yoy,
(coalesce(a.T52W_Subscription_Fee, 0) - coalesce(b.T52W_Subscription_Fee, 0))/coalesce(b.T52W_Subscription_Fee, 0) T52W_Subscription_Fee_yoy,
(coalesce(a.T52W_PL_Fee, 0) - coalesce(b.T52W_PL_Fee, 0))/coalesce(b.T52W_PL_Fee, 0) T52W_PL_Fee_yoy,
(coalesce(a.T1W_AVG_TITLE_LENGTH, 0) - coalesce(b.T1W_AVG_TITLE_LENGTH, 0))/coalesce(b.T1W_AVG_TITLE_LENGTH, 0) T1W_AVG_TITLE_LENGTH_yoy,
(coalesce(a.T1W_AVG_SUBTITLE_LENGTH, 0) - coalesce(b.T1W_AVG_SUBTITLE_LENGTH, 0))/coalesce(b.T1W_AVG_SUBTITLE_LENGTH, 0) T1W_AVG_SUBTITLE_LENGTH_yoy,
(coalesce(a.T1W_AVG_PHOTO_COUNT, 0) - coalesce(b.T1W_AVG_PHOTO_COUNT, 0))/coalesce(b.T1W_AVG_PHOTO_COUNT, 0) T1W_AVG_PHOTO_COUNT_yoy,
(coalesce(a.T52W_AVG_TITLE_LENGTH, 0) - coalesce(b.T52W_AVG_TITLE_LENGTH, 0))/coalesce(b.T52W_AVG_TITLE_LENGTH, 0) T52W_AVG_TITLE_LENGTH_yoy,
(coalesce(a.T52W_AVG_SUBTITLE_LENGTH, 0) - coalesce(b.T52W_AVG_SUBTITLE_LENGTH, 0))/coalesce(b.T52W_AVG_SUBTITLE_LENGTH, 0) T52W_AVG_SUBTITLE_LENGTH_yoy,
(coalesce(a.T52W_AVG_PHOTO_COUNT, 0) - coalesce(b.T52W_AVG_PHOTO_COUNT, 0))/coalesce(b.T52W_AVG_PHOTO_COUNT, 0) T52W_AVG_PHOTO_COUNT_yoy,
(coalesce(a.T1W_OUT_M2M_CNT, 0) - coalesce(b.T1W_OUT_M2M_CNT, 0))/coalesce(b.T1W_OUT_M2M_CNT, 0) T1W_OUT_M2M_CNT_yoy,
(coalesce(a.T52W_OUT_M2M_CNT, 0) - coalesce(b.T52W_OUT_M2M_CNT, 0))/coalesce(b.T52W_OUT_M2M_CNT, 0) T52W_OUT_M2M_CNT_yoy,
(coalesce(a.T1W_INB_M2M_CNT, 0) - coalesce(b.T1W_INB_M2M_CNT, 0))/coalesce(b.T1W_INB_M2M_CNT, 0) T1W_INB_M2M_CNT_yoy,
(coalesce(a.T52W_INB_M2M_CNT, 0) - coalesce(b.T52W_INB_M2M_CNT, 0))/coalesce(b.T52W_INB_M2M_CNT, 0) T52W_INB_M2M_CNT_yoy,
(coalesce(a.T1W_PL_net_revenue_penetration_percnt, 0) - coalesce(b.T1W_PL_net_revenue_penetration_percnt, 0))/coalesce(b.T1W_PL_net_revenue_penetration_percnt, 0) T1W_PL_net_revenue_penetration_percnt_yoy,
(coalesce(a.T1W_PLS_enabled_GMV_penetration_percnt, 0) - coalesce(b.T1W_PLS_enabled_GMV_penetration_percnt, 0))/coalesce(b.T1W_PLS_enabled_GMV_penetration_percnt, 0) T1W_PLS_enabled_GMV_penetration_percnt_yoy,
(coalesce(a.T1W_PLS_listing_adoption_percnt, 0) - coalesce(b.T1W_PLS_listing_adoption_percnt, 0))/coalesce(b.T1W_PLS_listing_adoption_percnt, 0) T1W_PLS_listing_adoption_percnt_yoy,
(coalesce(a.T1W_PLA_listing_adoption_percnt, 0) - coalesce(b.T1W_PLA_listing_adoption_percnt, 0))/coalesce(b.T1W_PLA_listing_adoption_percnt, 0) T1W_PLA_listing_adoption_percnt_yoy,
(coalesce(a.T52W_PL_net_revenue_penetration_percnt, 0) - coalesce(b.T52W_PL_net_revenue_penetration_percnt, 0))/coalesce(b.T52W_PL_net_revenue_penetration_percnt, 0) T52W_PL_net_revenue_penetration_percnt_yoy,
(coalesce(a.T52W_PLS_enabled_GMV_penetration_percnt, 0) - coalesce(b.T52W_PLS_enabled_GMV_penetration_percnt, 0))/coalesce(b.T52W_PLS_enabled_GMV_penetration_percnt, 0) T52W_PLS_enabled_GMV_penetration_percnt_yoy,
(coalesce(a.T52W_PLS_listing_adoption_percnt, 0) - coalesce(b.T52W_PLS_listing_adoption_percnt, 0))/coalesce(b.T52W_PLS_listing_adoption_percnt, 0) T52W_PLS_listing_adoption_percnt_yoy,
(coalesce(a.T52W_PLA_listing_adoption_percnt, 0) - coalesce(b.T52W_PLA_listing_adoption_percnt, 0))/coalesce(b.T52W_PLA_listing_adoption_percnt, 0) T52W_PLA_listing_adoption_percnt_yoy,
(coalesce(a.T1W_sfa_si, 0) - coalesce(b.T1W_sfa_si, 0))/coalesce(b.T1W_sfa_si, 0) T1W_sfa_si_yoy,
(coalesce(a.T1W_pls_ad_fee, 0) - coalesce(b.T1W_pls_ad_fee, 0))/coalesce(b.T1W_pls_ad_fee, 0) T1W_pls_ad_fee_yoy,
(coalesce(a.T1W_pla_ad_fee, 0) - coalesce(b.T1W_pla_ad_fee, 0))/coalesce(b.T1W_pla_ad_fee, 0) T1W_pla_ad_fee_yoy,
(coalesce(a.T1W_sfa_ad_fee, 0) - coalesce(b.T1W_sfa_ad_fee, 0))/coalesce(b.T1W_sfa_ad_fee, 0) T1W_sfa_ad_fee_yoy,
(coalesce(a.T1W_total_ad_fee, 0) - coalesce(b.T1W_total_ad_fee, 0))/coalesce(b.T1W_total_ad_fee, 0) T1W_total_ad_fee_yoy,
-- (coalesce(a.T1W_pls_ad_fee_LC, 0) - coalesce(b.T1W_pls_ad_fee_LC, 0))/coalesce(b.T1W_pls_ad_fee_LC, 0) T1W_pls_ad_fee_LC_yoy,
-- (coalesce(a.T1W_pla_ad_fee_LC, 0) - coalesce(b.T1W_pla_ad_fee_LC, 0))/coalesce(b.T1W_pla_ad_fee_LC, 0) T1W_pla_ad_fee_LC_yoy,
-- (coalesce(a.T1W_sfa_ad_fee_LC, 0) - coalesce(b.T1W_sfa_ad_fee_LC, 0))/coalesce(b.T1W_sfa_ad_fee_LC, 0) T1W_sfa_ad_fee_LC_yoy,
-- (coalesce(a.T1W_total_ad_fee_LC, 0) - coalesce(b.T1W_total_ad_fee_LC, 0))/coalesce(b.T1W_total_ad_fee_LC, 0) T1W_total_ad_fee_LC_yoy,
(coalesce(a.T52W_sfa_si, 0) - coalesce(b.T52W_sfa_si, 0))/coalesce(b.T52W_sfa_si, 0) T52W_sfa_si_yoy,
(coalesce(a.T52W_pls_ad_fee, 0) - coalesce(b.T52W_pls_ad_fee, 0))/coalesce(b.T52W_pls_ad_fee, 0) T52W_pls_ad_fee_yoy,
(coalesce(a.T52W_pla_ad_fee, 0) - coalesce(b.T52W_pla_ad_fee, 0))/coalesce(b.T52W_pla_ad_fee, 0) T52W_pla_ad_fee_yoy,
(coalesce(a.T52W_sfa_ad_fee, 0) - coalesce(b.T52W_sfa_ad_fee, 0))/coalesce(b.T52W_sfa_ad_fee, 0) T52W_sfa_ad_fee_yoy,
(coalesce(a.T52W_total_ad_fee, 0) - coalesce(b.T52W_total_ad_fee, 0))/coalesce(b.T52W_total_ad_fee, 0) T52W_total_ad_fee_yoy,
-- (coalesce(a.T52W_pls_ad_fee_LC, 0) - coalesce(b.T52W_pls_ad_fee_LC, 0))/coalesce(b.T52W_pls_ad_fee_LC, 0) T52W_pls_ad_fee_LC_yoy,
-- (coalesce(a.T52W_pla_ad_fee_LC, 0) - coalesce(b.T52W_pla_ad_fee_LC, 0))/coalesce(b.T52W_pla_ad_fee_LC, 0) T52W_pla_ad_fee_LC_yoy,
-- (coalesce(a.T52W_sfa_ad_fee_LC, 0) - coalesce(b.T52W_sfa_ad_fee_LC, 0))/coalesce(b.T52W_sfa_ad_fee_LC, 0) T52W_sfa_ad_fee_LC_yoy,
-- (coalesce(a.T52W_total_ad_fee_LC, 0) - coalesce(b.T52W_total_ad_fee_LC, 0))/coalesce(b.T52W_total_ad_fee_LC, 0) T52W_total_ad_fee_LC_yoy,
(coalesce(a.T1W_PLS_NET_REV, 0) - coalesce(b.T1W_PLS_NET_REV, 0))/coalesce(b.T1W_PLS_NET_REV, 0) T1W_PLS_NET_REV_yoy,
(coalesce(a.T1W_PLS_GROSS_REV, 0) - coalesce(b.T1W_PLS_GROSS_REV, 0))/coalesce(b.T1W_PLS_GROSS_REV, 0) T1W_PLS_GROSS_REV_yoy,
(coalesce(a.T1W_PLA_NET_REV, 0) - coalesce(b.T1W_PLA_NET_REV, 0))/coalesce(b.T1W_PLA_NET_REV, 0) T1W_PLA_NET_REV_yoy,
(coalesce(a.T1W_PLA_GROSS_REV, 0) - coalesce(b.T1W_PLA_GROSS_REV, 0))/coalesce(b.T1W_PLA_GROSS_REV, 0) T1W_PLA_GROSS_REV_yoy,
(coalesce(a.T1W_PLS_GMV, 0) - coalesce(b.T1W_PLS_GMV, 0))/coalesce(b.T1W_PLS_GMV, 0) T1W_PLS_GMV_yoy,
(coalesce(a.T1W_PLS_SI, 0) - coalesce(b.T1W_PLS_SI, 0))/coalesce(b.T1W_PLS_SI, 0) T1W_PLS_SI_yoy,
(coalesce(a.T1W_PLA_GMV, 0) - coalesce(b.T1W_PLA_GMV, 0))/coalesce(b.T1W_PLA_GMV, 0) T1W_PLA_GMV_yoy,
(coalesce(a.T1W_PLA_SI, 0) - coalesce(b.T1W_PLA_SI, 0))/coalesce(b.T1W_PLA_SI, 0) T1W_PLA_SI_yoy,
-- (coalesce(a.T1W_PL_REV, 0) - coalesce(b.T1W_PL_REV, 0))/coalesce(b.T1W_PL_REV, 0) T1W_PL_REV_yoy,
-- (coalesce(a.T1W_PL_GMV, 0) - coalesce(b.T1W_PL_GMV, 0))/coalesce(b.T1W_PL_GMV, 0) T1W_PL_GMV_yoy,
(coalesce(a.T1W_PLS_Live_Listings, 0) - coalesce(b.T1W_PLS_Live_Listings, 0))/coalesce(b.T1W_PLS_Live_Listings, 0) T1W_PLS_Live_Listings_yoy,
(coalesce(a.T1W_PLA_Live_listings, 0) - coalesce(b.T1W_PLA_Live_listings, 0))/coalesce(b.T1W_PLA_Live_listings, 0) T1W_PLA_Live_listings_yoy,
(coalesce(a.T1W_pls_sold_adrate, 0) - coalesce(b.T1W_pls_sold_adrate, 0))/coalesce(b.T1W_pls_sold_adrate, 0) T1W_pls_sold_adrate_yoy,
(coalesce(a.T52W_PLS_NET_REV, 0) - coalesce(b.T52W_PLS_NET_REV, 0))/coalesce(b.T52W_PLS_NET_REV, 0) T52W_PLS_NET_REV_yoy,
(coalesce(a.T52W_PLS_GROSS_REV, 0) - coalesce(b.T52W_PLS_GROSS_REV, 0))/coalesce(b.T52W_PLS_GROSS_REV, 0) T52W_PLS_GROSS_REV_yoy,
(coalesce(a.T52W_PLA_NET_REV, 0) - coalesce(b.T52W_PLA_NET_REV, 0))/coalesce(b.T52W_PLA_NET_REV, 0) T52W_PLA_NET_REV_yoy,
(coalesce(a.T52W_PLA_GROSS_REV, 0) - coalesce(b.T52W_PLA_GROSS_REV, 0))/coalesce(b.T52W_PLA_GROSS_REV, 0) T52W_PLA_GROSS_REV_yoy,
(coalesce(a.T52W_PLS_GMV, 0) - coalesce(b.T52W_PLS_GMV, 0))/coalesce(b.T52W_PLS_GMV, 0) T52W_PLS_GMV_yoy,
(coalesce(a.T52W_PLS_SI, 0) - coalesce(b.T52W_PLS_SI, 0))/coalesce(b.T52W_PLS_SI, 0) T52W_PLS_SI_yoy,
(coalesce(a.T52W_PLA_GMV, 0) - coalesce(b.T52W_PLA_GMV, 0))/coalesce(b.T52W_PLA_GMV, 0) T52W_PLA_GMV_yoy,
(coalesce(a.T52W_PLA_SI, 0) - coalesce(b.T52W_PLA_SI, 0))/coalesce(b.T52W_PLA_SI, 0) T52W_PLA_SI_yoy,
-- (coalesce(a.T52W_PL_REV, 0) - coalesce(b.T52W_PL_REV, 0))/coalesce(b.T52W_PL_REV, 0) T52W_PL_REV_yoy,
-- (coalesce(a.T52W_PL_GMV, 0) - coalesce(b.T52W_PL_GMV, 0))/coalesce(b.T52W_PL_GMV, 0) T52W_PL_GMV_yoy,
(coalesce(a.T52W_PLS_Live_Listings, 0) - coalesce(b.T52W_PLS_Live_Listings, 0))/coalesce(b.T52W_PLS_Live_Listings, 0) T52W_PLS_Live_Listings_yoy,
(coalesce(a.T52W_PLA_Live_listings, 0) - coalesce(b.T52W_PLA_Live_listings, 0))/coalesce(b.T52W_PLA_Live_listings, 0) T52W_PLA_Live_listings_yoy,
(coalesce(a.T52W_pls_sold_adrate, 0) - coalesce(b.T52W_pls_sold_adrate, 0))/coalesce(b.T52W_pls_sold_adrate, 0) T52W_pls_sold_adrate_yoy,
(coalesce(a.t52w_pla_ROAS, 0) - coalesce(b.t52w_pla_ROAS, 0))/coalesce(b.t52w_pla_ROAS, 0) t52w_pla_ROAS_yoy,
(coalesce(a.T52W_PLS_ROAS, 0) - coalesce(b.T52W_PLS_ROAS, 0))/coalesce(b.T52W_PLS_ROAS, 0) T52W_PLS_ROAS_yoy,
(coalesce(a.T52W_OVERALL_ROAS, 0) - coalesce(b.T52W_OVERALL_ROAS, 0))/coalesce(b.T52W_OVERALL_ROAS, 0) T52W_OVERALL_ROAS_yoy,
(coalesce(a.t1w_gmb_sellers_who_buy, 0) - coalesce(b.t1w_gmb_sellers_who_buy, 0))/coalesce(b.t1w_gmb_sellers_who_buy, 0) t1w_gmb_sellers_who_buy_yoy,
(coalesce(a.T1W_PURCHASES, 0) - coalesce(b.T1W_PURCHASES, 0))/coalesce(b.T1W_PURCHASES, 0) T1W_PURCHASES_yoy,
(coalesce(a.t52w_gmb_sellers_who_buy, 0) - coalesce(b.t52w_gmb_sellers_who_buy, 0))/coalesce(b.t52w_gmb_sellers_who_buy, 0) T52W_GMB_sellers_who_buy_yoy,
(coalesce(a.T52W_PURCHASES, 0) - coalesce(b.T52W_PURCHASES, 0))/coalesce(b.T52W_PURCHASES, 0) T52W_PURCHASES_yoy,
(coalesce(a.T1W_byr_cnt, 0) - coalesce(b.T1W_byr_cnt, 0))/coalesce(b.T1W_byr_cnt, 0) T1W_byr_cnt_yoy,
(coalesce(a.T1W_ACTV_BYR_CNT, 0) - coalesce(b.T1W_ACTV_BYR_CNT, 0))/coalesce(b.T1W_ACTV_BYR_CNT, 0) T1W_ACTV_BYR_CNT_yoy,
(coalesce(a.T1W_NORB_BYR_CNT, 0) - coalesce(b.T1W_NORB_BYR_CNT, 0))/coalesce(b.T1W_NORB_BYR_CNT, 0) T1W_NORB_BYR_CNT_yoy,
-- (coalesce(a.T52W_byr_cnt, 0) - coalesce(b.T52W_byr_cnt, 0))/coalesce(b.T52W_byr_cnt, 0) T52W_byr_cnt_yoy,
-- (coalesce(a.T1W_VIEW_COUNT, 0) - coalesce(b.T1W_VIEW_COUNT, 0))/coalesce(b.T1W_VIEW_COUNT, 0) T1W_VIEW_COUNT_yoy,
-- (coalesce(a.T52W_VIEW_COUNT, 0) - coalesce(b.T52W_VIEW_COUNT, 0))/coalesce(b.T52W_VIEW_COUNT, 0) T52W_VIEW_COUNT_yoy,
(coalesce(a.T1W_TOTAL_imps, 0) - coalesce(b.T1W_TOTAL_imps, 0))/coalesce(b.T1W_TOTAL_imps, 0) T1W_TOTAL_imps_yoy,
(coalesce(a.T1W_total_clicks, 0) - coalesce(b.T1W_total_clicks, 0))/coalesce(b.T1W_total_clicks, 0) T1W_total_clicks_yoy,
(coalesce(a.T1W_OVERALL_CTR, 0) - coalesce(b.T1W_OVERALL_CTR, 0))/coalesce(b.T1W_OVERALL_CTR, 0) T1W_OVERALL_CTR_yoy,
(coalesce(a.T1W_pl_imps, 0) - coalesce(b.T1W_pl_imps, 0))/coalesce(b.T1W_pl_imps, 0) T1W_pl_imps_yoy,
(coalesce(a.T1W_pl_clicks, 0) - coalesce(b.T1W_pl_clicks, 0))/coalesce(b.T1W_pl_clicks, 0) T1W_pl_clicks_yoy,
(coalesce(a.T1W_PL_CTR, 0) - coalesce(b.T1W_PL_CTR, 0))/coalesce(b.T1W_PL_CTR, 0) T1W_PL_CTR_yoy,
(coalesce(a.T1W_organic_impressions, 0) - coalesce(b.T1W_organic_impressions, 0))/coalesce(b.T1W_organic_impressions, 0) T1W_organic_impressions_yoy,
(coalesce(a.T1W_organic_clicks, 0) - coalesce(b.T1W_organic_clicks, 0))/coalesce(b.T1W_organic_clicks, 0) T1W_organic_clicks_yoy,
(coalesce(a.T1W_ORGANIC_CTR, 0) - coalesce(b.T1W_ORGANIC_CTR, 0))/coalesce(b.T1W_ORGANIC_CTR, 0) T1W_ORGANIC_CTR_yoy,
(coalesce(a.t52w_organic_impressions, 0) - coalesce(b.t52w_organic_impressions, 0))/coalesce(b.t52w_organic_impressions, 0) t52w_organic_impressions_yoy,
(coalesce(a.t52w_organic_clicks, 0) - coalesce(b.t52w_organic_clicks, 0))/coalesce(b.t52w_organic_clicks, 0) t52w_organic_clicks_yoy,
(coalesce(a.t52w_organic_ctr, 0) - coalesce(b.t52w_organic_ctr, 0))/coalesce(b.t52w_organic_ctr, 0) t52w_organic_ctr_yoy,
(coalesce(a.T1W_pla_imps, 0) - coalesce(b.T1W_pla_imps, 0))/coalesce(b.T1W_pla_imps, 0) T1W_pla_imps_yoy,
(coalesce(a.T1W_pla_clicks, 0) - coalesce(b.T1W_pla_clicks, 0))/coalesce(b.T1W_pla_clicks, 0) T1W_pla_clicks_yoy,
(coalesce(a.T1W_PLA_CTR, 0) - coalesce(b.T1W_PLA_CTR, 0))/coalesce(b.T1W_PLA_CTR, 0) T1W_PLA_CTR_yoy,
(coalesce(a.t52w_total_imprsns_curr, 0) - coalesce(b.t52w_total_imprsns_curr, 0))/coalesce(b.t52w_total_imprsns_curr, 0) t52w_total_imprsns_curr_yoy,
(coalesce(a.t52w_total_clicks_curr, 0) - coalesce(b.t52w_total_clicks_curr, 0))/coalesce(b.t52w_total_clicks_curr, 0) t52w_total_clicks_curr_yoy,
(coalesce(a.T52W_OVERALL_CTR, 0) - coalesce(b.T52W_OVERALL_CTR, 0))/coalesce(b.T52W_OVERALL_CTR, 0) T52W_OVERALL_CTR_yoy,
(coalesce(a.t52w_pl_imps_curr, 0) - coalesce(b.t52w_pl_imps_curr, 0))/coalesce(b.t52w_pl_imps_curr, 0) t52w_pl_imps_curr_yoy,
(coalesce(a.t52w_pl_clicks_curr, 0) - coalesce(b.t52w_pl_clicks_curr, 0))/coalesce(b.t52w_pl_clicks_curr, 0) t52w_pl_clicks_curr_yoy,
(coalesce(a.T52W_PL_CTR, 0) - coalesce(b.T52W_PL_CTR, 0))/coalesce(b.T52W_PL_CTR, 0) T52W_PL_CTR_yoy,
(coalesce(a.t52w_pla_imps_curr, 0) - coalesce(b.t52w_pla_imps_curr, 0))/coalesce(b.t52w_pla_imps_curr, 0) t52w_pla_imps_curr_yoy,
(coalesce(a.t52w_pla_clicks_curr, 0) - coalesce(b.t52w_pla_clicks_curr, 0))/coalesce(b.t52w_pla_clicks_curr, 0) t52w_pla_clicks_curr_yoy,
(coalesce(a.T52W_PLA_CTR, 0) - coalesce(b.T52W_PLA_CTR, 0))/coalesce(b.T52W_PLA_CTR, 0) T52W_PLA_CTR_yoy,
(coalesce(a.T1W_ads_fee_usd, 0) - coalesce(b.T1W_ads_fee_usd, 0))/coalesce(b.T1W_ads_fee_usd, 0) T1W_ads_fee_usd_yoy,
(coalesce(a.T1W_budget_amt_usd, 0) - coalesce(b.T1W_budget_amt_usd, 0))/coalesce(b.T1W_budget_amt_usd, 0) T1W_budget_amt_usd_yoy,
(coalesce(a.T1W_cmpgn_budget_usage, 0) - coalesce(b.T1W_cmpgn_budget_usage, 0))/coalesce(b.T1W_cmpgn_budget_usage, 0) T1W_cmpgn_budget_usage_yoy,
(coalesce(a.T52W_ads_fee_usd, 0) - coalesce(b.T52W_ads_fee_usd, 0))/coalesce(b.T52W_ads_fee_usd, 0) T52W_ads_fee_usd_yoy,
(coalesce(a.T52W_budget_amt_usd,0) - coalesce(b.T52W_budget_amt_usd,0))/coalesce(b.T52W_budget_amt_usd,0) T52W_budget_amt_usd_yoy,
(coalesce(a.T52W_cmpgn_budget_usage,0) - coalesce(b.T52W_cmpgn_budget_usage,0))/coalesce(b.T52W_cmpgn_budget_usage,0) T52W_cmpgn_budget_usage_yoy
-- ,
-- (coalesce(a.T1W_FIN3_BI,0) - coalesce(b.T1W_FIN3_BI,0))/coalesce(b.T1W_FIN3_BI,0) T1W_FIN3_BI_yoy,
-- (coalesce(a.T1W_PROM_3D_BI,0) - coalesce(b.T1W_PROM_3D_BI,0))/coalesce(b.T1W_PROM_3D_BI,0) T1W_PROM_3D_BI_yoy,
-- (coalesce(a.T1W_ACTUAL_3D_BI,0) - coalesce(b.T1W_ACTUAL_3D_BI,0))/coalesce(b.T1W_ACTUAL_3D_BI,0) T1W_ACTUAL_3D_BI_yoy,
-- (coalesce(a.T1W_LATE_DELIVERY_BI,0) - coalesce(b.T1W_LATE_DELIVERY_BI,0))/coalesce(b.T1W_LATE_DELIVERY_BI,0) T1W_LATE_DELIVERY_BI_yoy,
-- (coalesce(a.T1W_LDR,0) - coalesce(b.T1W_LDR,0))/coalesce(b.T1W_LDR,0) T1W_LDR_yoy,
-- (coalesce(a.T52W_LDR,0) - coalesce(b.T52W_LDR,0))/coalesce(b.T52W_LDR,0) T52W_LDR_yoy,
-- (coalesce(a.T1W_FREE_SHIPPING_TXNS,0) - coalesce(b.T1W_FREE_SHIPPING_TXNS,0))/coalesce(b.T1W_FREE_SHIPPING_TXNS,0) T1W_FREE_SHIPPING_TXNS_yoy,
-- (coalesce(a.T1W_VALID_TRACKING_BI,0) - coalesce(b.T1W_VALID_TRACKING_BI,0))/coalesce(b.T1W_VALID_TRACKING_BI,0) T1W_VALID_TRACKING_BI_yoy,
-- (coalesce(a.T1W_TRACKING_UPLOAD_BI,0) - coalesce(b.T1W_TRACKING_UPLOAD_BI,0))/coalesce(b.T1W_TRACKING_UPLOAD_BI,0) T1W_TRACKING_UPLOAD_BI_yoy,
-- (coalesce(a.T1W_HT_0_1D_TXNS,0) - coalesce(b.T1W_HT_0_1D_TXNS,0))/coalesce(b.T1W_HT_0_1D_TXNS,0) T1W_HT_0_1D_TXNS_yoy,
-- (coalesce(a.T1W_INR_14D_TXNS,0) - coalesce(b.T1W_INR_14D_TXNS,0))/coalesce(b.T1W_INR_14D_TXNS,0) T1W_INR_14D_TXNS_yoy,
-- (coalesce(a.T1W_ESC_INR_14D_TXNS,0) - coalesce(b.T1W_ESC_INR_14D_TXNS,0))/coalesce(b.T1W_ESC_INR_14D_TXNS,0) T1W_ESC_INR_14D_TXNS_yoy,
-- (coalesce(a.T52W_FIN3_BI,0) - coalesce(b.T52W_FIN3_BI,0))/coalesce(b.T52W_FIN3_BI,0) T52W_FIN3_BI_yoy,
-- (coalesce(a.T52W_PROM_3D_BI,0) - coalesce(b.T52W_PROM_3D_BI,0))/coalesce(b.T52W_PROM_3D_BI,0) T52W_PROM_3D_BI_yoy,
-- (coalesce(a.T52W_ACTUAL_3D_BI,0) - coalesce(b.T52W_ACTUAL_3D_BI,0))/coalesce(b.T52W_ACTUAL_3D_BI,0) T52W_ACTUAL_3D_BI_yoy,
-- (coalesce(a.T52W_LATE_DELIVERY_BI,0) - coalesce(b.T52W_LATE_DELIVERY_BI,0))/coalesce(b.T52W_LATE_DELIVERY_BI,0) T52W_LATE_DELIVERY_BI_yoy,
-- (coalesce(a.T52W_FREE_SHIPPING_TXNS,0) - coalesce(b.T52W_FREE_SHIPPING_TXNS,0))/coalesce(b.T52W_FREE_SHIPPING_TXNS,0) T52W_FREE_SHIPPING_TXNS_yoy,
-- (coalesce(a.T52W_VALID_TRACKING_BI,0) - coalesce(b.T52W_VALID_TRACKING_BI,0))/coalesce(b.T52W_VALID_TRACKING_BI,0) T52W_VALID_TRACKING_BI_yoy,
-- (coalesce(a.T52W_TRACKING_UPLOAD_BI,0) - coalesce(b.T52W_TRACKING_UPLOAD_BI,0))/coalesce(b.T52W_TRACKING_UPLOAD_BI,0) T52W_TRACKING_UPLOAD_BI_yoy,
-- (coalesce(a.T52W_HT_0_1D_TXNS,0) - coalesce(b.T52W_HT_0_1D_TXNS,0))/coalesce(b.T52W_HT_0_1D_TXNS,0) T52W_HT_0_1D_TXNS_yoy,
-- (coalesce(a.T52W_INR_14D_TXNS,0) - coalesce(b.T52W_INR_14D_TXNS,0))/coalesce(b.T52W_INR_14D_TXNS,0) T52W_INR_14D_TXNS_yoy,
-- (coalesce(a.T52W_ESC_INR_14D_TXNS,0) - coalesce(b.T52W_ESC_INR_14D_TXNS,0))/coalesce(b.T52W_ESC_INR_14D_TXNS,0) T52W_ESC_INR_14D_TXNS_yoy,
-- (coalesce(a.T1W_LL_with_Free_in_3_pct,0) - coalesce(b.T1W_LL_with_Free_in_3_pct,0))/coalesce(b.T1W_LL_with_Free_in_3_pct,0) T1W_LL_with_Free_in_3_pct_yoy,
-- (coalesce(a.T1W_LL_1HD_pct,0) - coalesce(b.T1W_LL_1HD_pct,0))/coalesce(b.T1W_LL_1HD_pct,0) T1W_LL_1HD_pct_yoy,
-- (coalesce(a.T1W_LL_next_day_pct,0) - coalesce(b.T1W_LL_next_day_pct,0))/coalesce(b.T1W_LL_next_day_pct,0) T1W_LL_next_day_pct_yoy,
-- (coalesce(a.T1W_LL_tracked_services_pct,0) - coalesce(b.T1W_LL_tracked_services_pct,0))/coalesce(b.T1W_LL_tracked_services_pct,0) T1W_LL_tracked_services_pct_yoy,
-- (coalesce(a.T1W_LL_0_3D_EDD_pct,0) - coalesce(b.T1W_LL_0_3D_EDD_pct,0))/coalesce(b.T1W_LL_0_3D_EDD_pct,0) T1W_LL_0_3D_EDD_pct_yoy,
-- (coalesce(a.T52W_LL_with_Free_in_3_pct,0) - coalesce(b.T52W_LL_with_Free_in_3_pct,0))/coalesce(b.T52W_LL_with_Free_in_3_pct,0) T52W_LL_with_Free_in_3_pct_yoy,
-- (coalesce(a.T52W_LL_1HD_pct,0) - coalesce(b.T52W_LL_1HD_pct,0))/coalesce(b.T52W_LL_1HD_pct,0) T52W_LL_1HD_pct_yoy,
-- (coalesce(a.T52W_LL_next_day_pct,0) - coalesce(b.T52W_LL_next_day_pct,0))/coalesce(b.T52W_LL_next_day_pct,0) T52W_LL_next_day_pct_yoy,
-- (coalesce(a.T52W_LL_tracked_services_pct,0) - coalesce(b.T52W_LL_tracked_services_pct,0))/coalesce(b.T52W_LL_tracked_services_pct,0) T52W_LL_tracked_services_pct_yoy,
-- (coalesce(a.T52W_LL_0_3D_EDD_pct,0) - coalesce(b.T52W_LL_0_3D_EDD_pct,0))/coalesce(b.T52W_LL_0_3D_EDD_pct,0) T52W_LL_0_3D_EDD_pct_yoy
from P_nishant_local_T.final_metrics_2_us a 
left join P_nishant_local_T.final_metrics_2_us b 
on a.retail_week = b.retail_week	
and a.slr_id = b.slr_id
and a.retail_year = b.retail_year + 1 
where a.retail_year >=2023;


