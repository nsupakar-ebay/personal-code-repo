-- GMV2 & SI T1W & CBT GMV
--- RUN & READY
DROP TABLE IF EXISTS P_nishant_local_T.GMV_T1_METRICS;
CREATE TABLE P_nishant_local_T.GMV_T1_METRICS AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.SLR_ID,
    ck.EU_B2C_C2C_FLAG,
    SUM(CK.gmv20_plan) AS T1W_GMV,
    SUM(CK.gmv20_sold_quantity) AS T1W_SI,
	SUM(CASE WHEN ck.BYR_CNTRY_ID <> CK.SLR_CNTRY_ID THEN CK.gmv20_plan ELSE 0 END) AS T1W_CBT_GMV,
    SUM(CASE WHEN ck.BYR_CNTRY_ID <> CK.SLR_CNTRY_ID THEN CK.gmv20_sold_quantity ELSE 0 END) AS T1W_CBT_SI
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
WHERE
    ck.SLR_CNTRY_ID = 3 AND 
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;



-- GMV2 & SI T52W 
--- RUN & READY
drop table if exists P_nishant_local_T.GMV_T52_METRICS;
CREATE TABLE P_nishant_local_T.GMV_T52_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_GMV) as T52W_GMV,
sum(b.T1W_SI) as T52W_SI,
sum(b.T1W_CBT_GMV) as T52W_CBT_GMV,
sum(b.T1W_CBT_SI) as T52W_CBT_SI
from P_nishant_local_T.GMV_T1_METRICS a
left join P_nishant_local_T.GMV_T1_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

--- t13 gmv
CREATE TABLE P_nishant_local_T.GMV_T13_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_GMV) as T13W_GMV,
sum(b.T1W_SI) as T13W_SI,
sum(b.T1W_CBT_GMV) as T13W_CBT_GMV,
sum(b.T1W_CBT_SI) as t13W_CBT_SI
from P_nishant_local_T.GMV_T1_METRICS a
left join P_nishant_local_T.GMV_T1_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <13
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

select *
from P_nishant_local_T.LL_T52_METRICS
limit 100
--- T1W LL
--- RUN & READY
-- need to rerun
-- CHECK COUNY
DROP TABLE IF EXISTS P_nishant_local_T.LL_T1_METRICS; 
create table P_nishant_local_T.LL_T1_METRICS as 
SELECT
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  cal.AGE_FOR_RTL_WEEK_ID,
  I.SLR_ID,
  COUNT(distinct I.ITEM_ID) AS T1W_LL,
 ---- count(distinct case when I.RELIST_UP_FLAG = 1 THEN I.ITEM_ID else null end) as T1W_RELIST_LL
  ---,count(distinct case when FNL_TYPE_CD IN (2,3,4) THEN I.ITEM_ID else null end) as T1W_FNL_LL
FROM
  ACCESS_VIEWS.DW_CAL_DT CAL
  left JOIN ACCESS_VIEWS.DW_LSTG_ITEM I   ON
  CAL.CAL_DT >= I.AUCT_START_DT
  AND CAL.CAL_DT <= I.AUCT_END_DT
   INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
   AND CAT.SITE_ID = I.ITEM_SITE_ID
   AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  ---LEFT JOIN PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT FCT
  ---ON I.ITEM_ID = FCT.ITEM_ID
  where 
  I.wacko_yn = 'N'
  and CAL.AGE_FOR_DT_ID <= -1
  and CAL.RETAIL_YEAR >= 2021
  group by 1,2,3,4
  ORDER BY 2 asc, 1 ASC, 3;  
  
  

--T52W LL
--- RUN & READY
drop table if exists P_nishant_local_T.LL_T52_METRICS; 
CREATE TABLE P_nishant_local_T.LL_T52_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_LL) as T52W_LL
from P_nishant_local_T.LL_T1_METRICS a
left join P_nishant_local_T.LL_T1_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;
 

--- T1W defect metrics
--- RUN & READY
DROP TABLE IF EXISTS P_nishant_local_T.T1W_DEFECT_METRICS;
CREATE TABLE P_nishant_local_T.T1W_DEFECT_METRICS AS
select cal.RETAIL_YEAR, CAL.RETAIL_WEEK,CAL.AGE_FOR_RTL_WEEK_ID, rle.SLR_ID,   
sum(rle.ESC_SNAD_FLAG) AS T1W_escal_SNAD_count,
sum(case when (rle.OPEN_SNAD_FLAG + rle.RTRN_SNAD_FLAG) > 0 then 1 else 0 end) as T1W_non_escal_SNAD_count,
sum(rle.STOCKOUT_FLAG) as T1W_STOCKOUT_count,
sum(rle.ESC_INR_FLAG) as T1W_escal_INR_count,
sum(rle.OPEN_INR_FLAG) as T1W_non_escal_INR_count,
sum(rle.LOW_DSR_IAD_FLAG) as T1W_low_IAD_DSR_count,
sum(rle.BYR_TO_SLR_NN_FLAG) as T1W_NN_feedback_count,
sum(rle.SNAD_MSG_FLAG) as T1W_Non_escal_SNAD_MSG_count,
sum(rle.INR_MSG_FLAG) as T1W_Non_escal_INR_MSG_count
from ACCESS_VIEWS.DW_CAL_DT CAL
left join ACCESS_VIEWS.ebay_trans_rltd_event rle 
on cal.cal_dt = rle.TRANS_DT
where rle.SLR_CNTRY_ID = 3
AND rle.core_categ_ind = 1
AND ! rle.auct_type_code IN(10, 12, 15)
AND rle.ck_wacko_yn_ind = 'N'
AND rle.rprtd_wacko_yn_ind = 'N'
AND CAL.RETAIL_WEEK >= 1 AND
CAL.RETAIL_YEAR >= 2021 
AND CAL.AGE_FOR_WEEK_ID <= -1 
group by 1, 2, 3, 4
order by 3, 1, 2;

--- T52W defect metrics
--- RUN & READY
CREATE TABLE P_nishant_local_T.T52W_DEFECT_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_escal_SNAD_count) AS T52W_escal_SNAD_count,
sum(b.T1W_non_escal_SNAD_count) AS T52W_non_escal_SNAD_count,
sum(b.T1W_STOCKOUT_count) AS T52W_STOCKOUT_count,
sum(b.T1W_escal_INR_count) AS T52W_escal_INR_count,
sum(b.T1W_non_escal_INR_count) AS T52W_non_escal_INR_count,
sum(b.T1W_low_IAD_DSR_count) AS T52W_low_IAD_DSR_count,
sum(b.T1W_NN_feedback_count) AS T52W_NN_feedback_count,
sum(b.T1W_Non_escal_SNAD_MSG_count) AS T52W_Non_escal_SNAD_MSG_count,
sum(b.T1W_Non_escal_INR_MSG_count) AS T52W_Non_escal_INR_MSG_count
from P_nishant_local_T.T1W_DEFECT_METRICS a
left join P_nishant_local_T.T1W_DEFECT_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4;

--- RUN & READY
--- LATEST seller standard
drop table if exists P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS;
CREATE TABLE P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS AS
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
  SPS.USER_ID,
  SPS.LAST_EVAL_DT,
  SPS.SPS_SLR_LEVEL_CD,
  CAL.RETAIL_WK_END_DATE,
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  RANK() OVER(PARTITION BY SPS.USER_ID, cal.RETAIL_YEAR, RETAIL_YEAR ORDER BY SPS.LAST_EVAL_DT DESC) AS RNK
-- Case
--     when SPS_SLR_LEVEL_CD = 1 then 'ETRS'
--     when SPS_SLR_LEVEL_CD = 2 then 'ASTD'
--     when SPS_SLR_LEVEL_CD = 4 then 'BSTD'
--   end as Seller_Rating  
FROM ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN PRS_RESTRICTED_V.SPS_LEVEL SPS
ON CAL.RETAIL_WK_END_DATE >= SPS.LAST_EVAL_DT
WHERE CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2022
AND CAL.AGE_FOR_DT_ID <= -1
AND SPS_EVAL_TYPE_CD = 3 ---- trending Standard code
AND sps_prgrm_id = 3
ORDER BY 1,2,3
)
where rnk =1



--- NUMBER OF STORES ACTIVE ON AT LEAST ONE DAY IN THE WEEKN
--- RUN & READY
---- ADD t52w
DROP TABLE IF EXISTS P_nishant_local_T.STORE_KPI_METRICS;
CREATE TABLE P_nishant_local_T.STORE_KPI_METRICS AS
SELECT CAL.RETAIL_YEAR,
	   CAL.RETAIL_WEEK,
	   CAL.AGE_FOR_RTL_WEEK_ID,
	   SLR_ID, 
	   COUNT(CASE WHEN PROD_ID = 101 THEN 1 ELSE NULL END) AS STARTER_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 3 THEN 1 ELSE NULL END) AS BASIC_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 4 THEN 1 ELSE NULL END) AS FEATURE_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 5 THEN 1 ELSE NULL END) AS ANCHOR_STORE_COUNT,
	   COUNT(CASE WHEN PROD_ID = 102 THEN 1 ELSE NULL END) AS ENTERPRISE_STORE_COUNT
FROM ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN ACCESS_VIEWS.DW_STORE_ATTR_HIST HIST ON 
CAL.CAL_DT >= HIST.BEG_DT
AND CAL.CAL_DT <= HIST.END_DT
WHERE CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2022
AND CAL.AGE_FOR_DT_ID <= -1
GROUP BY 1,2,3,4
ORDER BY 3,1,2;

---T52 STORE COUNTS
CREATE TABLE P_nishant_local_T.T52_STORE_KPI_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
SUM(B.STARTER_STORE_COUNT) T52W_STARTER_STORE_COUNT,
	   SUM(B.BASIC_STORE_COUNT) T52W_BASIC_STORE_COUNT,
	   SUM(B.FEATURE_STORE_COUNT)  T52W_FEATURE_STORE_COUNT,
	   SUM(B.ANCHOR_STORE_COUNT) T52W_ANCHOR_STORE_COUNT,
	   SUM(B.ENTERPRISE_STORE_COUNT) T52_ENTERPRISE_STORE_COUNT
from P_nishant_local_T.STORE_KPI_METRICS a
left JOIN P_nishant_local_T.STORE_KPI_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4;

SELECT *
FROM P_nishant_local_T.T52_STORE_KPI_METRICS
LIMIT 20

---T1W NATIVE & SELLER HUB USAGE
--- RUN & READY
DROP TABLE IF EXISTS P_nishant_local_T.T1W_NSH_USAGE_METRICS;
CREATE TABLE P_nishant_local_T.T1W_NSH_USAGE_METRICS AS
select CAL.RETAIL_YEAR, CAL.RETAIL_WEEK, CAL.AGE_FOR_RTL_WEEK_ID,
FCT.USER_ID as slr_id,
SUM(FCT.sh_cnt) AS T1W_SELLERH_CNT,
SUM(FCT.NATIVE_CNT) AS T1W_NATIVE_CNT
from ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN P_ZETA_AUTOETL_V.SLR_HUB_DLY_USG_FCT FCT
ON CAL.CAL_DT = FCT.CAL_DT
WHERE CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2021
AND CAL.AGE_FOR_DT_ID <= -1
GROUP BY 1,2,3,4;

---T52W NATIVE & SELLER HUB USAGE
--- RUN & READY
CREATE TABLE P_nishant_local_T.T52W_NSH_USAGE_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_SELLERH_CNT) as T52W_SELLERH_CNT,
sum(b.T1W_NATIVE_CNT) as T52W_NATIVE_CNT
from P_nishant_local_T.T1W_NSH_USAGE_METRICS a
left join P_nishant_local_T.T1W_NSH_USAGE_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

-- --- AVERAGE START PRICE
-- ---- NOT RUN
-- --- LSTG_STATUS_ID TO FILTER ON LIVE LISTING ETC
-- SELECT SLR_ID,
-- 	   AVG((START_PRICE_LSTG_CURNCY_AMT*CURNCY_PLAN_RATE)/QTY_AVAIL)
-- FROM PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT
-- WHERE SLR_CNTRY_ID = 3
-- AND ISCORE = 1
-- GROUP BY 1
-- ORDER BY 1 DESC
-- LIMIT 100


drop table if exists P_nishant_local_T.T52W_FVF_METRICS;
drop table if exists P_nishant_local_T.T1W_FVF_METRICS;
DROP TABLE IF EXISTS P_nishant_local_T.final_metrics;
---T1W FVF
--- RUN & READY
---- might not add
-- DROP TABLE IF EXISTS P_nishant_local_T.T1W_FVF_METRICS;
-- CREATE TABLE P_nishant_local_T.T1W_FVF_METRICS AS
-- select CAL.RETAIL_YEAR, CAL.RETAIL_WEEK, CAL.AGE_FOR_RTL_WEEK_ID,
-- 		REV.SLR_ID,
-- 		sum(REV.AMT_USD) as T1W_FVF_USD_AMT	
-- from ACCESS_VIEWS.DW_CAL_DT CAL
-- LEFT JOIN ACCESS_VIEWS.DW_GEM2_CMN_RVNU_I REV
-- ON CAL.CAL_DT = REV.ACCT_TRANS_DT 
-- WHERE CAL.RETAIL_WEEK >=1
-- AND CAL.RETAIL_YEAR >=2021
-- AND CAL.AGE_FOR_DT_ID <= -1
-- AND REV.SLR_CNTRY_ID = 3
-- and REV.actn_code in (select ACTN_CODE
-- 					from dw_action_codes
-- 					where UPPER(ACTN_CODE_TYPE_DESC1) = 'FINAL VALUE FEE'
-- 					GROUP BY 1)
-- GROUP BY 1,2,3, 4;


-- ---T52W FVF
-- --- RUN & READY
-- --- might  not add to final table
-- CREATE TABLE P_nishant_local_T.T52W_FVF_METRICS AS
-- select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
-- sum(b.T1W_FVF_USD_AMT) as T52W_FVF_USD_AMT
-- from P_nishant_local_T.T1W_FVF_METRICS a
-- left join P_nishant_local_T.T1W_FVF_METRICS b
-- on a.slr_id = b.slr_id
-- and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
-- and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
-- where a.retail_year >= 2022
-- GROUP BY 1,2,3,4
-- ORDER BY 1,3,2;

---CSS segmentationADD
---- not run
-- select 		cal.retail_year,
-- 			cal.retail_week,
-- 			u.user_id AS SLR_ID,
-- 			case WHEN cs.cust_sgmntn_cd IN (1, 7,13,19,25,31) THEN 'Large merchant'
--             WHEN cs.cust_sgmntn_cd IN (2, 8,14,20,26,32) THEN 'Merchants'
--             WHEN cs.cust_sgmntn_cd IN (3, 9,15,21,27,33) THEN 'Entrepreneur'
--             WHEN cs.cust_sgmntn_cd IN (4,10,16,22,28,34) THEN 'Regulars'
--             WHEN cs.cust_sgmntn_cd IN (5,11,17,23,29,35) THEN 'Occasional'
--             WHEN cs.cust_sgmntn_cd IN (6,12,18,24,30,36) THEN 'Lapsed'
--        ELSE 'Never' END AS LATEST_CSS_segment                   -- Nevers are buyers
-- from ACCESS_VIEWS.DW_CAL_DT CAL 
-- left join ACCESS_VIEWS.DW_USERS U     
-- left join prs_restricted_v.DNA_CUST_SELLER_SGMNTN_HIST cs
--     ON  u.user_id = cs.slr_id
--     AND cs.cust_sgmntn_grp_cd BETWEEN 36 AND 41            -- only Customer Seller Segmentation (CSS) data
--     AND current_date BETWEEN cs.cust_slr_sgmntn_beg_dt and cs.cust_slr_sgmntn_end_dt  -- change the current date 
-- where U.user_cntry_id =3                               -- seller domicility || 3-UK, 77-DE, 71-FR, IT-101, ES-186 rest look up in wiki
-- and U.user_dsgntn_id = 2                                  -- BUSINESS SELLER, NEED TO CONFIRM
-- group by 1,2;



--- final metric table
--- ADD % EXPORT TRANS METRIC
DROP TABLE IF EXISTS P_nishant_local_T.final_metrics;
create table P_nishant_local_T.final_metrics as
SELECT A.RETAIL_YEAR, A.RETAIL_WEEK, A.SLR_ID,
	   A.T1W_GMV, A.T1W_SI, (A.T1W_GMV/A.T1W_SI) as T1W_ASP,
	   A.T1W_CBT_GMV, A.T1W_CBT_SI, (A.T1W_CBT_GMV/A.T1W_CBT_SI) AS T1W_CBT_ASP,
	   B.T52W_GMV, B.T52W_SI, (B.T52W_GMV/B.T52W_SI) AS T52W_ASP,
	   B.T52W_CBT_GMV, B.T52W_CBT_SI, (B.T52W_CBT_GMV/B.T52W_CBT_SI) AS T52W_CBT_ASP,
	   C.T1W_LL,
	   D.T52W_LL,
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
	   I.T1W_SELLERH_CNT,I.T1W_NATIVE_CNT,
	   J.T52W_SELLERH_CNT,J.T52W_NATIVE_CNT,
	   K.T1W_FVF_USD_AMT,
	   L.T52W_FVF_USD_AMT
FROM P_nishant_local_T.GMV_T1_METRICS A
LEFT JOIN P_nishant_local_T.GMV_T52_METRICS B
ON A.SLR_ID = B.SLR_ID
AND A.RETAIL_WEEK = B.RETAIL_WEEK
AND A.RETAIL_YEAR = B.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LL_T1_METRICS C
ON A.SLR_ID = C.SLR_ID
AND A.RETAIL_WEEK = C.RETAIL_WEEK
AND A.RETAIL_YEAR = C.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LL_T52_METRICS D
ON A.SLR_ID = D.SLR_ID
AND A.RETAIL_WEEK = D.RETAIL_WEEK
AND A.RETAIL_YEAR = D.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_DEFECT_METRICS E
ON A.SLR_ID = E.SLR_ID
AND A.RETAIL_WEEK = E.RETAIL_WEEK
AND A.RETAIL_YEAR = E.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_DEFECT_METRICS F
ON A.SLR_ID = F.SLR_ID
AND A.RETAIL_WEEK = F.RETAIL_WEEK
AND A.RETAIL_YEAR = F.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS G
ON A.SLR_ID = G.USER_ID
AND A.RETAIL_WEEK = G.RETAIL_WEEK
AND A.RETAIL_YEAR = G.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.STORE_KPI_METRICS H
ON A.SLR_ID = H.SLR_ID
AND A.RETAIL_WEEK = H.RETAIL_WEEK
AND A.RETAIL_YEAR = H.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_NSH_USAGE_METRICS I
ON A.SLR_ID = I.SLR_ID
AND A.RETAIL_WEEK = I.RETAIL_WEEK
AND A.RETAIL_YEAR = I.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_NSH_USAGE_METRICS J
ON A.SLR_ID = J.SLR_ID
AND A.RETAIL_WEEK = J.RETAIL_WEEK
AND A.RETAIL_YEAR = J.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_FVF_METRICS K
ON A.SLR_ID = K.SLR_ID
AND A.RETAIL_WEEK = K.RETAIL_WEEK
AND A.RETAIL_YEAR = K.RETAIL_YEAR
and a.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_FVF_METRICS L
ON A.SLR_ID = L.SLR_ID
AND A.RETAIL_WEEK = L.RETAIL_WEEK
AND A.RETAIL_YEAR = L.RETAIL_YEAR
and a.retail_year >= 2022
WHERE A.RETAIL_YEAR >=2022 ;

select *
from P_nishant_local_T.final_metrics
limit 100

----- Fee table T1W
---- need to be run again
--- not joined on previous table
--- check with KM & check with , might have to change to quarterly check with finance team
--- feature fees, store fee
cheeck if rev bucket id is same for actn code for insertion fees
--- Questions open
----- 1) Is the table correct?
----- 2) do the actn codes look correct?
----- 3) Are the exclusions correct?
----- 4) Are fees available only on a quarterly basis?
----- fees are available with a lag of 14 days, the adjustment factor is generally 2% of fee cost


dROP TABLE IF EXISTS P_nishant_local_T.T1W_FEE;
CREATE TABLE P_nishant_local_T.T1W_FEE AS 
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
AND a.slr_cntry_id = 3
INNER JOIN access_views.dw_usegm_hist b
ON a.slr_id=b.USER_ID
AND a.ACCT_TRANS_DT BETWEEN b.BEG_DATE AND b.END_DATE
AND b.USEGM_GRP_ID=48 -- c2c b2c segmentation code (will contain c2c and b2c)
INNER JOIN dw_category_groupings cat
ON a.LSTG_SITE_ID=cat.site_id AND a.leaf_categ_id=cat.leaf_categ_id
WHERE 
1=1
and cal.RETAIL_YEAR >= 2021
 and CAL.AGE_FOR_DT_ID <= -1
AND a.slr_cntry_id = 3
AND b.USEGM_ID=206 -- B2C 
AND cat.sap_category_id NOT IN (23,5,7,41) -- ask about these exclusions
GROUP BY 1, 2, 3, 4; 

select *
from P_nishant_local_T.T52W_FEE
limit 100

----- Fee table T52W
---- only add quarterly fee
DROP TABLE IF EXISTS P_nishant_local_T.T52W_FEE;
CREATE TABLE P_nishant_local_T.T52W_FEE AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
SUM(B.T1W_Variable_FVF) AS T52W_Variable_FVF
,SUM(B.T1W_Fixed_FVF) AS T52W_Fixed_FVF
,SUM(B.T1W_Insertion_Fee) AS T52W_Insertion_Fee
,SUM(B.T1W_Subscription_Fee) AS T52W_Subscription_Fee
,SUM(B.T1W_PL_Fee) AS T52W_PL_Fee
from P_nishant_local_T.T1W_FEE a
left join P_nishant_local_T.T1W_FEE b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

---- t13 fee table
DROP TABLE IF EXISTS P_nishant_local_T.T13W_FEE;
CREATE TABLE P_nishant_local_T.T13W_FEE AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
SUM(B.T1W_Variable_FVF) AS T13W_Variable_FVF
,SUM(B.T1W_Fixed_FVF) AS T13W_Fixed_FVF
,SUM(B.T1W_Insertion_Fee) AS T13W_Insertion_Fee
,SUM(B.T1W_Subscription_Fee) AS T13W_Subscription_Fee
,SUM(B.T1W_PL_Fee) AS T13W_PL_Fee
from P_nishant_local_T.T1W_FEE a
left join P_nishant_local_T.T1W_FEE b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <13
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;







SELECT *
FROM P_nishant_local_T.T13W_FEE
LIMIT 100

---- AVERAGE START PRICE & LIVE LISTING
--- NEED TO ADD TO FINAL TABLE AGAIN
-- DROP TABLE IF EXISTS P_nishant_local_T.ASP_T1_METRICS; 
-- create table P_nishant_local_T.ASP_T1_METRICS as 
-- SELECT
--   CAL.RETAIL_WEEK,
--   CAL.RETAIL_YEAR,
--   cal.AGE_FOR_RTL_WEEK_ID,
--   I.SLR_ID,
--   COUNT(1) AS T1W_LL,
--   SUM(I.START_PRICE_USD) AS T1W_SUM_LL
-- FROM
--   ACCESS_VIEWS.DW_CAL_DT CAL
--   left JOIN ACCESS_VIEWS.DW_LSTG_ITEM I   ON
--   CAL.CAL_DT >= I.AUCT_START_DT
--   AND CAL.CAL_DT <= I.AUCT_END_DT
--    INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
--    AND CAT.SITE_ID = I.ITEM_SITE_ID
--    AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
--   where 
--   I.wacko_yn = 'N'
--   and CAL.AGE_FOR_DT_ID <= -1
--   and i.LSTG_STATUS_ID = 0
--   and CAL.RETAIL_YEAR >= 2021
--   group by 1,2,3,4
--   ORDER BY 2 asc, 1 ASC, 3;  

---T1W PL METRICS
--- HAVE NOT ADDED TO MAIN TABLE
---- DOnt add to final table
-- DROP TABLE IF EXISTS P_nishant_local_T.T1W_PL_METRICS; 
-- create table P_nishant_local_T.T1W_PL_METRICS
-- SELECT
-- RETAIL_WEEK,
-- RETAIL_YEAR,
-- cal.AGE_FOR_RTL_WEEK_ID,
-- SLR_ID,
-- Sum(CASE WHEN GEM.ACTN_CODE in (409,410) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT ) ELSE 0 END ) T1W_PLS_NET,
-- Sum(CASE WHEN GEM.ACTN_CODE = 409 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PLS_GROSS,
-- Sum(CASE WHEN GEM.ACTN_CODE in (474,475) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END ) T1W_PLA_NET,
-- Sum(CASE WHEN GEM.ACTN_CODE = 474 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PLA_GROSS,
-- Sum(CASE WHEN GEM.ACTN_CODE in (564,565) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END ) T1W_SFA_NET,
-- Sum(CASE WHEN GEM.ACTN_CODE = 564 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_SFA_GROSS,
-- Sum(CASE WHEN GEM.ACTN_CODE in (526,527) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END ) T1W_PLX_NET,
-- Sum(CASE WHEN GEM.ACTN_CODE = 526 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PLX_GROSS,
-- Sum(CASE WHEN GEM.ACTN_CODE in (568,569) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PD_NET,
-- Sum(CASE WHEN GEM.ACTN_CODE = 568 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PD_GROSS
-- FROM (SELECT ACCT_TRANS_DT, SLR_ID, ACTN_CODE, LEAF_CATEG_ID, LSTG_SITE_ID, SLR_CNTRY_ID, BYR_CNTRY_ID, TRXN_CURNCY_CD
--            ,Sum(Cast ( -1* TRXN_CURNCY_AMT AS FLOAT)) TRXN_CURNCY_AMT, Sum(Cast(-1 * AMT_USD AS FLOAT )) AMT_USD, Sum(Cast(-1 * AMT_M_USD AS FLOAT)) AMT_M_USD
-- FROM ACCESS_VIEWS.DW_GEM2_CMN_RVNU_I RVNU
-- WHERE ACCT_TRANS_DT between '2019-01-01' and CURRENT_DATE - 1
-- AND LSTG_TYPE_CODE NOT IN (10,15)
-- AND ADJ_TYPE_ID NOT IN (-1,-7,5)
-- AND LSTG_SITE_ID <> 223
-- AND ACCT_TRANS_ID <> 3178044411711
-- AND ACTN_CODE IN (409,410,474,475,526,527,564,565,568,569)
-- GROUP BY 1,2,3,4,5,6,7,8
--         UNION ALL
--         /* WASH DATA OF BAD TRANSACTIONS */
-- SELECT ACCT_TRANS_DT, SLR_ID, ACTN_CODE, LEAF_CATEG_ID, LSTG_SITE_ID, SLR_CNTRY_ID, BYR_CNTRY_ID, TRXN_CURNCY_CD
--            ,Sum(Cast ( TRXN_CURNCY_AMT AS FLOAT)) TRXN_CURNCY_AMT, Sum( Cast( AMT_USD AS FLOAT ) ) AMT_USD, Sum( Cast(AMT_M_USD AS FLOAT ) ) AMT_M_USD
-- FROM ACCESS_VIEWS.DW_GEM2_CMN_ADJ_RVNU_I
-- WHERE ACCT_TRANS_DT between '2019-01-01' and CURRENT_DATE - 1
-- AND LSTG_TYPE_CODE NOT IN(10,15)
-- AND LSTG_SITE_ID <> 223
-- AND ISWACKO_YN_ID = 1
-- AND ACCT_TRANS_ID<>3178044411711
-- AND ACTN_CODE IN (409,410,474,475,526,527,564,565,568,569)
-- GROUP BY 1,2,3,4,5,6,7,8
--     )GEM
--     INNER JOIN ACCESS_VIEWS.SSA_CURNCY_PLAN_RATE_DIM AS BPR  ON GEM. TRXN_CURNCY_CD = BPR. CURNCY_ID 
--     INNER JOIN ACCESS_VIEWS.DW_ACCT_ACTN_CODE_LKP AS B ON GEM.ACTN_CODE = B.ACTN_CODE AND B.REV_BKT_ID BETWEEN 29 AND 36
--     INNER JOIN ACCESS_VIEWS.DW_ACCT_LSTG_REV_BKT_LKP AS R ON R.REV_BKT_ID = B.REV_BKT_ID AND Upper(R.REV_GRP_CODE) = 'GEM'
--     INNER JOIN ACCESS_VIEWS.DW_ACTION_CODES AS ACTN_CODE ON ACTN_CODE.ACTN_CODE = GEM.ACTN_CODE
--     INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON GEM.ACCT_TRANS_DT = CAL.CAL_DT
-- 	where 
-- 	gem.slr_cntry_id = 3
-- 	and cal.RETAIL_WEEK >=1
-- 	and cal.RETAIL_YEAR >=2021
--     GROUP BY 1,2,3,4;

---T52W PL METRICS
--- HAVE NOT ADDED TO MAIN TABLE
---- DOnt add to final table
-- DROP TABLE IF EXISTS P_nishant_local_T.T52W_PL_METRICS;
-- CREATE TABLE P_nishant_local_T.T52W_PL_METRICS AS
-- select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
-- Sum(B.T1W_PLS_NET) AS T52W_PLS_NET,
-- Sum(B.T1W_PLS_GROSS) AS T52W_PLS_GROSS,
-- Sum(B.T1W_PLA_NET) AS T52W_PLA_NET,
-- Sum(B.T1W_PLA_GROSS) AS T52W_PLA_GROSS,
-- Sum(B.T1W_SFA_NET) AS T52W_SFA_NET,
-- Sum(B.T1W_SFA_GROSS) AS T52W_SFA_GROSS,
-- Sum(B.T1W_PLX_NET) AS T52W_PLX_NET,
-- Sum(B.T1W_PLX_GROSS) AS T52W_PLX_GROSS,
-- Sum(B.T1W_PD_NET) AS T52W_PD_NET,
-- Sum(B.T1W_PD_GROSS) AS T52W_PD_GROSS
-- from P_nishant_local_T.T1W_PL_METRICS a
-- left join P_nishant_local_T.T1W_PL_METRICS b
-- on a.slr_id = b.slr_id
-- and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
-- and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
-- where a.retail_year >= 2022
-- GROUP BY 1,2,3,4
-- ORDER BY 1,3,2;


SELECT *
FROM P_nishant_local_T.T52W_PL_METRICS
LIMIT 100

--- T1W LISTING ATTRIBUTES
--- NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T1W_LISTING_METRICS;
CREATE TABLE P_nishant_local_T.T1W_LISTING_METRICS AS
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
WHERE FCT.SLR_CNTRY_ID =3
AND CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2021
AND FCT.ISCORE = 1
GROUP BY 1,2,3,4;

select *
from P_nishant_local_T.T52W_LISTING_METRICS
limit 100
--- T52W LISTING ATTRIBUTES
--- NOT ADDED TO FINAL TABLE
--- RUN
DROP TABLE IF EXISTS P_nishant_local_T.T52W_LISTING_METRICS;
CREATE TABLE P_nishant_local_T.T52W_LISTING_METRICS AS
SELECT  A.RETAIL_WEEK,
	    A.RETAIL_YEAR,
		A.SLR_ID,
	    AVG(B.T1W_AVG_TITLE_LENGTH) AS T52W_AVG_TITLE_LENGTH,
		AVG(B.T1W_AVG_SUBTITLE_LENGTH) AS T52W_AVG_SUBTITLE_LENGTH,
		AVG(B.T1W_AVG_PHOTO_COUNT) AS T52W_AVG_PHOTO_COUNT
FROM P_nishant_local_T.T1W_LISTING_METRICS A
LEFT JOIN P_nishant_local_T.T1W_LISTING_METRICS B
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3
ORDER BY 1,3,2;

select *
from P_nishant_local_T.T52W_LISTING_METRICS
limit 10

---OUTGOING T1W M2M COUNT
--- NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T1W_OUTB_METRICS;
CREATE TABLE P_nishant_local_T.T1W_OUTB_METRICS AS
SELECT CAL.RETAIL_WEEK,
	   CAL.RETAIL_YEAR,
	   CAL.RETAIL_WK_END_DATE,
	   CAL.AGE_FOR_RTL_WEEK_ID,
	   EM.SNDR_ID AS ID,
	   COUNT(DISTINCT EM.EMAIL_TRACKING_ID) AS T1W_OUT_M2M_CNT
from ACCESS_VIEWS.DW_CAL_DT CAL 
left join prs_secure_v.dw_ue_email_tracking em
on cal.cal_dt = em.SRC_CRE_DT
WHERE RETAIL_WEEK>=1
AND RETAIL_YEAR >=2021
AND CAL.AGE_FOR_WEEK_ID <= -1 
GROUP BY 1,2,3,4,5;

----T52W OUTB_M2M
DROP TABLE IF EXISTS P_nishant_local_T.T52W_OUTB_METRICS;
CREATE TABLE P_nishant_local_T.T52W_OUTB_METRICS AS
SELECT A.RETAIL_WEEK,
	   A.RETAIL_YEAR,
	   A.AGE_FOR_RTL_WEEK_ID,
	   A.ID,
	   SUM(B.T1W_OUT_M2M_CNT) AS T52W_OUT_M2M_CNT 
from P_nishant_local_T.T1W_OUTB_METRICS a
left join P_nishant_local_T.T1W_OUTB_METRICS b
on a.id = b.id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;




---INCOMING M2M COUNT
--- NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T1W_INB_METRICS;
CREATE TABLE P_nishant_local_T.T1W_INB_METRICS AS
SELECT CAL.RETAIL_WEEK,
	   CAL.RETAIL_YEAR,
	   CAL.RETAIL_WK_END_DATE,
	   CAL.AGE_FOR_RTL_WEEK_ID,
	   EM.RCPNT_ID AS ID,
	   COUNT(DISTINCT EM.EMAIL_TRACKING_ID) AS T1W_INB_M2M_CNT
from ACCESS_VIEWS.DW_CAL_DT CAL 
left join prs_secure_v.dw_ue_email_tracking em
on cal.cal_dt = em.SRC_CRE_DT
WHERE RETAIL_WEEK>=1
AND RETAIL_YEAR >=2021
GROUP BY 1,2,3,4,5;

----T52W INB_M2M
DROP TABLE IF EXISTS P_nishant_local_T.T52W_INB_METRICS;
CREATE TABLE P_nishant_local_T.T52W_INB_METRICS AS
SELECT A.RETAIL_WEEK,
	   A.RETAIL_YEAR,
	   A.AGE_FOR_RTL_WEEK_ID,
	   A.ID,
	   SUM(B.T1W_INB_M2M_CNT) AS T52W_INB_M2M_CNT 
from P_nishant_local_T.T1W_INB_METRICS a
left join P_nishant_local_T.T1W_INB_METRICS b
on a.id = b.id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

--- GSP GMV
--- NOT RUN, DO CONFIRM
-- DOES NOT LOOK CORRECT
CREATE TABLE p_NISHANT_LOCAL_T.T1W_GSP_METRICS AS 
SELECT 
  cal.retail_year
  ,cal.retail_week 
  ,CAL.AGE_FOR_RTL_WEEK_ID,
  sum(cast(xo.GMV_PLAN_USD as DECIMAL(18,4))) as T1W_GSP_GMV
FROM
  access_views.dw_checkout_trans as xo
inner join
  access_views.dw_category_groupings as cat
on  
  cat.site_id=xo.site_id and cat.leaf_categ_id=xo.leaf_categ_id
inner join ( 
  SELECT  
    cal_dt
    ,retail_year
    ,retail_wk_end_date
    ,retail_week
	, AGE_FOR_RTL_WEEK_ID
  FROM
    access_views.dw_cal_dt 
  WHERE
    cal_dt<=current_date 
  ) as cal
on  xo.GMV_DT=cal.cal_dt and cal.retail_wk_end_date>='2018-01-01'
inner join ( 
  SELECT  
    curncy_id
    ,curncy_plan_rate 
  FROM 
    access_views.ssa_curncy_plan_rate_dim 
  WHERE 
    curncy_id in(1,3) 
  ) as lpr
on 
  lpr.curncy_id=xo.lstg_curncy_id
GROUP BY 1,2,3

SELECT *
FROM  p_NISHANT_LOCAL_T.T1W_GSP_METRICS
LIMIT 30

---- LDR metrics
--- NOT RUN
-- select cal.RETAIL_YEAR, CAL.RETAIL_WEEK,CAL.AGE_FOR_RTL_WEEK_ID, rle.SLR_ID,   
-- sum(rle.LATE_DLVRY_FLAG) AS T1W_LD_count
-- from ACCESS_VIEWS.DW_CAL_DT CAL
-- left join ACCESS_VIEWS.ebay_trans_rltd_event rle 
-- on cal.cal_dt = rle.TRANS_DT
-- where rle.SLR_CNTRY_ID = 3
-- AND rle.core_categ_ind = 1
-- AND ! rle.auct_type_code IN(10, 12, 15)
-- AND rle.ck_wacko_yn_ind = 'N'
-- AND rle.rprtd_wacko_yn_ind = 'N'
-- AND CAL.RETAIL_WEEK >= 1 AND
-- CAL.RETAIL_YEAR >= 2021 
-- AND CAL.AGE_FOR_WEEK_ID <= -1 
-- group by 1, 2, 3, 4
-- order by 3, 1, 2
-- limit 100;


---- new or retained seller
----RUN BUT NOT JOINED ON FINAL table
CREATE TABLE p_NISHANT_LOCAL_T.new_or_reactivated_seller as
select RETAIL_WEEK, RETAIL_YEAR, AGE_FOR_RTL_WEEK_ID, SLR_ID, NORS_DT,
case when SLR_TYPE_CD =1 then 'New' 
     when SLR_TYPE_CD = 2 then 'Reactivated'
	 else null end as slr_type
from(
select cal.RETAIL_WEEK, cal.RETAIL_YEAR, cal.AGE_FOR_RTL_WEEK_ID,
nors.USER_ID as slr_id, NORS_DT,SLR_TYPE_CD,
row_number() OVER ( PARTITION by USER_ID, cal.RETAIL_WEEK, cal.RETAIL_YEAR order by NORS_DT desc) as rnk
from  ACCESS_VIEWS.DW_CAL_DT CAL
left join ACCESS_VIEWS.SLR_NORS_GMV2_HIST nors
on nors.NORS_DT <= cal.RETAIL_WK_END_DATE
and nors.CNTRY_ID = 3
and cal.CAL_DATE >= '2022-01-01'
where nors.CNTRY_ID = 3
and cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2022
AND CAL.AGE_FOR_WEEK_ID <= -1 
and nors.USER_ID in (
select distinct SLR_ID
from PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
WHERE
    ck.SLR_CNTRY_ID = 3 
	and ck.GMV_DT >= '2022-01-01'
    and CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) 
    and ck.CK_WACKO_YN = 'N'  
    and CK.ISCORE = 1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
)
order by USER_ID,  RETAIL_YEAR, RETAIL_WEEK
)
where rnk =1

select *
from p_NISHANT_LOCAL_T.new_or_reactivated_seller
limit 100

----Run BUT NOT added to final table
----- PL T1W METRICS
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS;
CREATE TABLE P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS AS
select cal.RETAIL_YEAR
     	,cal.RETAIL_WEEK,
		cal.AGE_FOR_RTL_WEEK_ID
	, pl.slr_id
	, round(sum(pl.PLS_NET+pl.PLA_NET+pl.PLX_NET),2) as T1W_PL_Net_Revenue
	, sum(pl.PLS_NET+pl.PLA_NET+pl.PLX_NET) T1W_PL_NET_REVENUE_DENO
	, sum(pl.gmv) T1W_PL_GMV
	, round(sum(pl.PLS_NET+pl.PLA_NET+pl.PLX_NET)*100/sum(pl.gmv),2) as T1W_PL_net_revenue_penetration_percnt
	, round(sum(pl.PLS_Enabled_GMV),2) T1W_PLS_enabled_GMV_pene_pct_num, round(sum(pl.PLS_ELIG_GMV_USD),2) PLS_enabled_GMV_pene_pct_deno
	, round(sum(pl.PLS_Enabled_GMV)*100/sum(pl.PLS_ELIG_GMV_USD),2) as T1W_PLS_enabled_GMV_penetration_percnt
	, round(sum(pl.PLS_enabled_lstg),2) PLS_listing_adoption_pct_num, round(sum(pl.PLS_PLA_ELIG_LSTG),2) PLS_listing_adoption_pct_deno
	, round(sum(pl.PLS_enabled_lstg)*100/sum(pl.PLS_PLA_ELIG_LSTG),2) as T1W_PLS_listing_adoption_percnt
	, round(sum(pl.PLA_lstgs)*100/sum(pl.PLS_PLA_ELIG_LSTG),2) as T1W_PLA_listing_adoption_percnt
	, sum(pl.PLA_lstgs) PLA_listing_adoption_num
	, sum(pl.PLS_PLA_ELIG_LSTG) PLA_listing_adoption_deno
from  ACCESS_VIEWS.DW_CAL_DT CAL 
LEFT JOIN P_NBD_T.SLRSEG_allTiers_final pl
on PL.CAL_DT=cal.CAL_DT 
AND CAL.RETAIL_YEAR>=2021
AND CAL.RETAIL_WEEK>=1
WHERE CAL.RETAIL_YEAR>=2021
AND CAL.RETAIL_WEEK>=1
AND CAL.AGE_FOR_WEEK_ID <= -1 
group by 1,2,3,4

---- Run but not added to final table
----- PL T52W METRICS
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS;
CREATE TABLE P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID
, round(sum(b.T1W_PL_Net_Revenue),2) T52W_PL_Net_Revenue
	, round(sum(b.T1W_PL_Net_Revenue)*100/sum(b.T1W_PL_GMV),2) as T52W_PL_net_revenue_penetration_percnt
	, round(sum(b.T1W_PLS_enabled_GMV_pene_pct_num)*100/sum(b.PLS_enabled_GMV_pene_pct_deno),2) as T52W_PLS_enabled_GMV_penetration_percnt
	, round(sum(b.PLS_listing_adoption_pct_num)*100/sum(b.PLS_listing_adoption_pct_deno),2) as T52W_PLS_listing_adoption_percnt
	, round(sum(b.PLA_listing_adoption_num)*100/sum(b.PLA_listing_adoption_deno),2) as T52W_PLA_listing_adoption_percnt
from P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS a
left join P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

select *
from P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS
limit 10

----- T1W_VIEW COUNTS
--- RUN BUT NOT ADDED TO FINAL TABLE
--- NOT ADDING TO FINAL TABLE
-- CREATE TABLE P_NISHANT_LOCAL_T.T1W_VIEW_CNT AS
-- select cal.RETAIL_WEEK, cal.RETAIL_YEAR, cal.AGE_FOR_RTL_WEEK_ID,
-- VI.USER_ID as slr_id, 
-- SUM(VI_CNT) AS T1W_VIEW_COUNT
-- from  ACCESS_VIEWS.DW_CAL_DT CAL
-- left join ACCESS_VIEWS.USER_BRWS_SRCH_SD VI
-- on VI.CAL_DT = cal.CAL_DT
-- and cal.RETAIL_WEEK >= 1
-- AND CAL.RETAIL_YEAR >=2021
-- where 
-- cal.RETAIL_WEEK >=1
-- and cal.RETAIL_YEAR >=2021
-- AND CAL.AGE_FOR_WEEK_ID <= -1 
-- and VI.USER_ID in (
-- select distinct SLR_ID
-- from PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
-- WHERE
--     ck.SLR_CNTRY_ID = 3 
-- 	and ck.GMV_DT >= '2022-01-01'
--     and CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) 
--     and ck.CK_WACKO_YN = 'N'  
--     and CK.ISCORE = 1 
-- 	and ck.EU_B2C_C2C_FLAG = 'B2C'
-- )
-- GROUP BY 1,2,3,4
-- order by USER_ID,  RETAIL_YEAR, RETAIL_WEEK

-- ----- T52W_VIEW COUNTS
-- --- RUN BUT NOT ADDED TO FINAL TABLE
-- DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T52W_VIEW_CNT;
-- CREATE TABLE P_NISHANT_LOCAL_T.T52W_VIEW_CNT AS
-- select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
-- sum(b.T1W_VIEW_COUNT) as T52W_VIEW_COUNT
-- from P_NISHANT_LOCAL_T.T1W_VIEW_CNT a
-- left join P_NISHANT_LOCAL_T.T1W_VIEW_CNT b
-- on a.slr_id = b.slr_id
-- and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
-- and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
-- where a.retail_year >= 2022
-- GROUP BY 1,2,3,4
-- ORDER BY 1,3,2;

----T1W PLS ad rate
---not run
-- CREATE TABLE p_NISHANT_LOCAL_T.T1W_AD_RATE AS 
-- select
-- cal.RETAIL_WEEK,
-- cal.RETAIL_YEAR,
-- cal.AGE_FOR_RTL_WEEK_ID
-- ,slr_id 
-- ,avg(LSTG_BID_PCT)/100 as T1W_pls_adrate
-- ,avg(SOLD_BID_PCT)/100 as T1W_pls_sold_adrate
-- from ACCESS_VIEWS.DW_CAL_DT CAL
-- left join access_views.pl_item_mtrc_sd perf_metric
-- on perf_metric.CAL_DT = cal.CAL_DT
-- and cal.RETAIL_WEEK >=1
-- and cal.RETAIL_YEAR >=2021
-- where cal.RETAIL_WEEK >=1
-- and cal.RETAIL_YEAR >=2021
-- AND CAL.AGE_FOR_WEEK_ID <= -1 
-- and SLR_ID in (
-- select distinct SLR_ID
-- from PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
-- WHERE
--     ck.SLR_CNTRY_ID = 3 
-- 	and ck.GMV_DT >= '2022-01-01'
--     and CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) 
--     and ck.CK_WACKO_YN = 'N'  
--     and CK.ISCORE = 1 
-- 	and ck.EU_B2C_C2C_FLAG = 'B2C'
-- )

-- group by 1,2,3,4
-- limit 100;  

-----sfa PLA PLS ad rate, gmv, si
----- need to add transaction date
---  run  NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T1W_pl_METRICS;
CREATE TABLE P_NISHANT_LOCAL_T.T1W_pl_METRICS AS
SELECT
	cal.RETAIL_WEEK,
	cal.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    k.seller_id,
	SUM(CASE WHEN k.event_type_txt = 'PLS' THEN k.item_sold_qty ELSE 0 END) AS T1W_pls_si,
    SUM(CASE WHEN k.event_type_txt = 'PLPS' AND K.trans_id IS NULL THEN k.item_sold_qty ELSE 0 END) AS T1W_pla_si,
    SUM(CASE WHEN k.event_type_txt = 'PLPS' THEN k.item_sold_qty ELSE 0 END) AS T1W_pla_si_notdeduped,
    SUM(CASE WHEN k.event_type_txt = 'SFAS' THEN k.item_sold_qty ELSE 0 END) AS T1W_sfa_si,
    SUM(CASE WHEN k.event_type_txt = 'PLS' THEN k.sale_usd_amt ELSE 0 END) AS T1W_pls_gmv,
    SUM(CASE WHEN k.event_type_txt = 'PLPS' AND K.trans_id IS NULL THEN k.sale_usd_amt	 ELSE 0 END) AS T1W_pla_gmv,
    SUM(CASE WHEN k.event_type_txt = 'PLPS' THEN k.sale_usd_amt	 ELSE 0 END) AS T1W_pla_gmv_notdeduped,
    SUM(CASE WHEN k.event_type_txt = 'SFAS' THEN k.sale_usd_amt	 ELSE 0 END) AS T1W_sfa_gmv,
	sum(case when k.event_type_txt= 'PLS' then k.ad_fee_usd_amt else 0 end) as T1W_pls_ad_fee,
	sum(case when k.event_type_txt= 'PLPCPC' then k.ad_fee_usd_amt else 0 end) as T1W_pla_ad_fee, 
    SUM(CASE WHEN k.event_type_txt = 'SFACPC' THEN k.ad_fee_usd_amt ELSE 0 END) AS T1W_sfa_ad_fee,
	SUM(CASE WHEN k.event_type_txt = 'SFACPC' OR k.event_type_txt= 'PLPCPC' OR k.event_type_txt= 'PLS'
	THEN k.ad_fee_usd_amt ELSE 0 END) AS T1W_total_ad_fee
FROM ACCESS_VIEWS.DW_CAL_DT CAL
left join ACCESS_VIEWS.PL_ORG_ADS_SALES_FACT k
on FROM_UNIXTIME(K.EVENT_TM / 1000, 'yyyy-MM-dd')= cal.CAL_DT
and cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2021
where cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2021
AND CAL.AGE_FOR_WEEK_ID <= -1 
AND k.event_type_txt IN ('SFAS', 'SFACPC', 'PLS','PLPCPC')
GROUP BY 1,2,3,4;

	SELECT *
	FROM  P_NISHANT_LOCAL_T.T1W_pl_METRICS
	LIMIT 100
	
----t52w pl metrics
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T52W_pl_METRICS;
CREATE TABLE P_NISHANT_LOCAL_T.T52W_pl_METRICS AS
select a.SeLleR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID
,   SUM(b.T1W_pls_si) T52W_pls_si,
    SUM(b.T1W_pla_si) T52W_pla_si,
    SUM(b.T1W_sfa_si) T52W_sfa_si,
    SUM(b.T1W_pls_gmv) T52W_pls_gmv,
    SUM(b.T1W_pla_gmv) T52W_pla_gmv,
    SUM(b.T1W_sfa_gmv) T52W_sfa_gmv,
	sum(b.T1W_pls_ad_fee) T52W_pls_ad_fee,
	sum(b.T1W_pla_ad_fee) T52W_pla_ad_fee, 
    SUM(b.T1W_sfa_ad_fee) T52W_sfa_ad_fee,
	SUM(b.T1W_total_ad_fee) T52W_total_ad_fee
from P_NISHANT_LOCAL_T.T1W_pl_METRICS a
left join P_NISHANT_LOCAL_T.T1W_pl_METRICS b
on a.seller_id = b.seller_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

select *
from P_NISHANT_LOCAL_T.T52W_pl_METRICS
limit 100


--- T1W
---impressions, clicks & CTR
-- not run
select cal.retail_week,
	   cal.retail_year,
	   slr_id,
sum(total_imp_cnt) as total_imps,
sum(total_click_cnt) as total_click_cnt,
sum(ebay_click_cnt) as on_ebay_click_cnt,
sum(off_ebay_click_cnt) as off_ebay_click_cnt,
sum(ebay_click_cnt)/sum(total_imp_cnt) as ovl_conversion,
sum(srp_top20_slot_imp_cnt)+sum(srp_rest_slot_imp_cnt) as srp_imp_cnt,
sum(srp_top20_slot_imp_cnt) as srp_top20_slot_imp_cnt,
sum(srp_rest_slot_imp_cnt) as srp_rest_slot_imp_cnt,
sum(merch_imp_cnt) as merch_imp_cnt,
sum(case when event_type_cd='PL' then total_imp_cnt else 0 end) as pl_imps,
sum(case when event_type_cd='PL' then total_click_cnt else 0 end) as pl_clicks,
sum(case when event_type_cd='PL' then ebay_click_cnt else 0 end) as pl_on_ebay_clicks,
sum(case when event_type_cd='PL' then off_ebay_click_cnt else 0 end) as pl_off_ebay_clicks,
sum(case when event_type_cd='PL' then ebay_click_cnt else 0 end)/sum(case when event_type_cd='PL' then total_imp_cnt else 0 end) as pl_conversion,
sum(case when event_type_cd='PL' then srp_top20_slot_imp_cnt+srp_rest_slot_imp_cnt else 0 end) as pl_srp_imp_cnt,
sum(case when event_type_cd='PL' then srp_top20_slot_imp_cnt else 0 end) as pl_srp_top20_slot_imp_cnt,
sum(case when event_type_cd='PL' then srp_rest_slot_imp_cnt else 0 end) as pl_srp_rest_slot_imp_cnt,
sum(case when event_type_cd='PL' then merch_imp_cnt else 0 end) as pl_merch_imp_cnt,
sum(case when event_type_cd='PLP' then total_imp_cnt else 0 end) as pla_imps,
sum(case when event_type_cd='PLP' then total_click_cnt else 0 end) as pla_clicks,
sum(case when event_type_cd='PLP' then ebay_click_cnt else 0 end) as pla_on_ebay_clicks,
sum(case when event_type_cd='PLP' then off_ebay_click_cnt else 0 end) as pla_off_ebay_clicks,
SUM(CASE WHEN event_type_cd='Organic' THEN total_imp_cnt ELSE 0 END) AS organic_impressions,
SUM(CASE WHEN event_type_cd='Organic' THEN total_click_cnt ELSE 0 END) AS organic_clicks,
sum(case when event_type_cd='PLP' then ebay_click_cnt else 0 end)/sum(case when event_type_cd='PLP' then total_imp_cnt else 0 end) as pla_conversion,
sum(case when event_type_cd='PLP' then srp_top20_slot_imp_cnt+srp_rest_slot_imp_cnt else 0 end) as pla_srp_imp_cnt,
sum(case when event_type_cd='PLP' then merch_imp_cnt else 0 end) as pla_merch_imp_cnt,
sum(case when event_type_cd='SFA' then total_click_cnt else 0 end) as sfa_clicks

from ACCESS_VIEWS.DW_CAL_DT cal
left join (
select
bpe.slr_id,
from_unixtime(unix_timestamp(dt,'yyyyMMdd'),'yyyy-MM-dd') as dt,
bpe.event_type_cd,
bpe.item_id,
bpe.total_imp_cnt,
bpe.srp_top20_slot_imp_cnt,
bpe.srp_rest_slot_imp_cnt,
bpe.merch_imp_cnt,
bpe.total_click_cnt,
bpe.ebay_click_cnt,
bpe.off_ebay_click_cnt
from bpe_v.pl_org_ads_imp_click_sd bpe 
where from_unixtime(unix_timestamp(dt,'yyyyMMdd'),'yyyy-MM-dd') between "2020-09-01" and CURRENT_DATE
)	  base
on base.dt = cal.cal_dt
where cal.retail_year >=2021
and CAL.AGE_FOR_WEEK_ID <= -1 
group by 1,2,3
limit 100;



---- ODR, SAF & lsr rate

select  
  'trust_metrics' as insights_type,
  user_id as slr_id,
  
  CASE WHEN sps_prgrm_id=2 THEN 'US'
    WHEN sps_prgrm_id=3 THEN 'UK'
    WHEN sps_prgrm_id=4 THEN 'DE'
    WHEN sps_prgrm_id=1 THEN 'Global'
  END as program_site,
  
  CASE WHEN sps_slr_level_cd=1 THEN 'eTRS'
    WHEN sps_slr_level_cd=2 THEN 'Above Standard'
    WHEN sps_slr_level_cd=4 THEN 'Below Standard'
    ELSE 'na'
  END as slr_std,
  
  sps_slr_level_sum_start_dt,
  sps_slr_level_sum_end_dt,
  
  eval_mnth_beg_dt,
  eval_mnth_end_dt,
  
  last_eval_dt,
  
  CASE WHEN trans_3m_cnt >= 400 THEN trans_3m_cnt 
    ELSE trans_12m_cnt
  END as transaction_cnt,
  
  CAST(gmv_iso_amt AS DECIMAL(32,8)) * COALESCE(pln.curncy_plan_rate, 1) AS gmv_iso_amt,
  
  CASE WHEN (sps_slr_level_cd=1 AND trans_3m_cnt>=400 AND unq_byr_dfct_cnt<=3) THEN (CAST(slr_init_cncl_trans_3m_cnt+infrd_slr_cncl_trans_3m_cnt+ebay_close_inr_saf_3m_cnt+ebay_close_snad_saf_3m_cnt+pp_close_inr_saf_3m_cnt+pp_close_snad_saf_3m_cnt AS DECIMAL(18,5)) / CAST(trans_3m_cnt AS DECIMAL(18,5)))
    WHEN (sps_slr_level_cd=1 AND trans_12m_cnt<>0 AND unq_byr_dfct_cnt<=3) THEN (CAST(slr_init_cncl_trans_12m_cnt+infrd_slr_cncl_trans_12m_cnt+ebay_close_inr_saf_12m_cnt+ebay_close_snad_saf_12m_cnt+pp_close_inr_saf_12m_cnt+pp_close_snad_saf_12m_cnt AS DECIMAL(18,5)) / CAST(trans_12m_cnt AS DECIMAL(18,5)))
    WHEN (sps_slr_level_cd=4 AND trans_3m_cnt>=400 AND unq_byr_dfct_cnt>=5) THEN (CAST(slr_init_cncl_trans_3m_cnt+infrd_slr_cncl_trans_3m_cnt+ebay_close_inr_saf_3m_cnt+ebay_close_snad_saf_3m_cnt+pp_close_inr_saf_3m_cnt+pp_close_snad_saf_3m_cnt AS DECIMAL(18,5)) / CAST(trans_3m_cnt AS DECIMAL(18,5)))
    WHEN (sps_slr_level_cd=4 AND trans_12m_cnt<>0 AND unq_byr_dfct_cnt>=5) THEN (CAST(slr_init_cncl_trans_12m_cnt+infrd_slr_cncl_trans_12m_cnt+ebay_close_inr_saf_12m_cnt+ebay_close_snad_saf_12m_cnt+pp_close_inr_saf_12m_cnt+pp_close_snad_saf_12m_cnt AS DECIMAL(18,5)) / CAST(trans_12m_cnt AS DECIMAL(18,5)))
    WHEN trans_3m_cnt>=400 THEN (CAST(slr_init_cncl_trans_3m_cnt+infrd_slr_cncl_trans_3m_cnt+ebay_close_inr_saf_3m_cnt+ebay_close_snad_saf_3m_cnt+pp_close_inr_saf_3m_cnt+pp_close_snad_saf_3m_cnt AS DECIMAL(18,5)) / CAST(trans_3m_cnt AS DECIMAL(18,5)))
    WHEN trans_12m_cnt<>0 THEN (CAST(slr_init_cncl_trans_12m_cnt+infrd_slr_cncl_trans_12m_cnt+ebay_close_inr_saf_12m_cnt+ebay_close_snad_saf_12m_cnt+pp_close_inr_saf_12m_cnt+pp_close_snad_saf_12m_cnt AS DECIMAL(18,5)) / CAST(trans_12m_cnt AS DECIMAL(18,5)))
    ELSE 0
  END as odr_rate,
  
  
  CASE WHEN trans_3m_cnt>=400 THEN (CAST(ebay_close_inr_saf_3m_cnt+ebay_close_snad_saf_3m_cnt+pp_close_inr_saf_3m_cnt+pp_close_snad_saf_3m_cnt AS DECIMAL(18,5)) / CAST(trans_3m_cnt AS DECIMAL(18,5)))
    WHEN trans_12m_cnt<>0 THEN (CAST(ebay_close_inr_saf_12m_cnt+ebay_close_snad_saf_12m_cnt+pp_close_inr_saf_12m_cnt+pp_close_snad_saf_12m_cnt AS DECIMAL(18,5)) / CAST(trans_12m_cnt AS DECIMAL(18,5)))
    ELSE 0
  END as saf_rate,
  
  
  CASE WHEN dlvry_miss_elgbl_cnt <> 0 THEN CAST(CAST(dlvry_miss_cnt AS DECIMAL(18,5)) / CAST(dlvry_miss_elgbl_cnt AS DECIMAL(18,5)) AS DECIMAL(18,5))
    ELSE 0
  END as lsr_rate,
  
    CASE WHEN trans_3m_cnt <> 0 THEN CAST(CAST(VALID_SLA_TRK_UPLD_TRANS_CNT AS DECIMAL(18,5)) / CAST(trans_3m_cnt AS DECIMAL(18,5)) AS DECIMAL(18,5))
    ELSE 0
    END as tracking_uploaded_validated_rate

  
FROM PRS_RESTRICTED_V.sps_level_metric_sum as s

LEFT JOIN access_views.dw_currencies as dwcurr ON dwcurr.iso_code = s.gmv_iso_crncy_cd
LEFT JOIN ACCESS_VIEWS.ssa_curncy_plan_rate_dim as pln ON pln.curncy_id = dwcurr.curncy_id 

WHERE
  s.sps_eval_type_cd = 1 
  AND s.LAST_EVAL_DT >= TRUNC(ADD_MONTHS(TRUNC(CURRENT_DATE, 'MM'), -1), 'MM')
    AND s.USER_ID in (18116485)
  AND s.SPS_PRGRM_ID = CASE
    WHEN "DE" = 'US' THEN 2
    WHEN "DE" = 'UK' THEN 3
    WHEN "DE" = 'DE' THEN 4
    ELSE 1 END

QUALIFY ROW_NUMBER () OVER (PARTITION BY USER_ID, program_site, YEAR(sps_slr_level_sum_start_dt), MONTH(sps_slr_level_sum_start_dt) ORDER BY EVAL_MNTH_END_DT, LAST_EVAL_DT DESC ) = 1
limit 1;

---- number of new & returning buyers
---- not run or joined
SELECT flag,
case when orders = 1 then 'New Buyers' else 'Returning Buyers' end as BuyerType,
sum(orders) as transaction,
COUNT(DISTINCT a.buyer_id) AS Buyers,
SUM(a.PURCHASES) AS Purchases,
SUM(a.GMV_LC) AS GMV
FROM          
(SELECT user_slctd_id as flag, BUYER_ID,Count(DISTINCT TRANSACTION_ID) as orders,
SUM(DW_CHECKOUT_TRANS.CORE_ITEM_CNT) PURCHASES,              
sum(case when total_amount > 0 then (GMV_LC_AMT ) end ) AS GMV_LC              
FROM DW_CHECKOUT_TRANS
INNER JOIN ACCESS_VIEWS.ssa_curncy_plan_rate_dim AS lpr ---plan rate table
ON lpr.curncy_id=dw_checkout_trans.lstg_curncy_id 
  
INNER JOIN ${initials}_vg_all_items_base A
ON DW_CHECKOUT_TRANS.ITEM_ID = A.ITEM_ID
  
WHERE DW_CHECKOUT_TRANS.gmv_dt BETWEEN  '${start_date}' AND '${end_date}'   --- input start date and end date of analysis in variables section of zeta
AND DW_CHECKOUT_TRANS.AUCT_END_DT >=date_sub('${start_date}',7)
AND DW_CHECKOUT_TRANS.SITE_ID IN (${SITE_ID})                               --- input site_id in variables section of zeta
AND DW_CHECKOUT_TRANS.SALE_TYPE IN (1,2,7,8,9,13)      
and PAID_IND = 1
and CHECKOUT_STATUS = 2 
AND DW_CHECKOUT_TRANS.CK_WACKO_YN = 'N'      
GROUP BY 1,2)a
group by 1,2;

---latest address
--- not working
select user_id,
	   case when ADDRESS_TYPE = 1 then city_txt else null end as shipping_city,
	   case when ADDRESS_TYPE = 1 then zip else null end as shipping_zip,
	   case when ADDRESS_TYPE = 2 then city_txt else null end as seller_city,
	   case when ADDRESS_TYPE = 2 then zip else null end as seller_zip
from ACCESS_VIEWS.DW_USER_ADDRESSES
where ADDRESS_TYPE in (1, 2)
and CNTRY_ID = 3
order by 1
limit 30

select *
from ACCESS_VIEWS.DW_USER_ADDRESSES
limit 100

--- seller type L2 & L3
--- run but added to final table
CREATE TABLE P_NISHANT_LOCAL_T.latest_seller_type AS
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
    ck.SLR_CNTRY_ID = 3 
	and ck.GMV_DT >= '2022-01-01'
--     and CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) 
--     and ck.CK_WACKO_YN = 'N'  
--     and CK.ISCORE = 1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
)
order by 3,2,1

--latest CSS segmentation
-- not run or joined
-- select 		cal.retail_year,
-- 			cal.retail_week,
-- 			cs.SLR_ID AS SLR_ID,
-- 			case WHEN cs.cust_sgmntn_cd IN (1, 7,13,19,25,31) THEN 'Large merchant'
--             WHEN cs.cust_sgmntn_cd IN (2, 8,14,20,26,32) THEN 'Merchants'
--             WHEN cs.cust_sgmntn_cd IN (3, 9,15,21,27,33) THEN 'Entrepreneur'
--             WHEN cs.cust_sgmntn_cd IN (4,10,16,22,28,34) THEN 'Regulars'
--             WHEN cs.cust_sgmntn_cd IN (5,11,17,23,29,35) THEN 'Occasional'
--             WHEN cs.cust_sgmntn_cd IN (6,12,18,24,30,36) THEN 'Lapsed'
--        ELSE 'Never' END AS LATEST_CSS_segment                   -- Nevers are buyers
-- from ACCESS_VIEWS.DW_CAL_DT CAL 
-- left join prs_restricted_v.DNA_CUST_SELLER_SGMNTN_HIST cs
-- on cs.cust_sgmntn_grp_cd BETWEEN 36 AND 41            -- only Customer Seller Segmentation (CSS) data
-- AND cal.CAL_DT >= cs.cust_slr_sgmntn_beg_dt and
-- cal.CAL_DT <= cs.cust_slr_sgmntn_end_dt  -- change the current date 
-- and cal.RETAIL_WEEK>=1
-- and cal.RETAIL_YEAR >=2022
-- left join ACCESS_VIEWS.DW_USERS U 
-- ON  u.user_id = cs.slr_id
-- where                                 -- BUSINESS SELLER, NEED TO CONFIRM
-- cal.RETAIL_WEEK>=1
-- and cal.RETAIL_YEAR >=2022
-- and  U.user_cntry_id =3                               -- seller domicility || 3-UK, 77-DE, 71-FR, IT-101, ES-186 rest look up in wiki
-- and U.user_dsgntn_id = 2 
-- LIMIT 100


select *
from P_nishant_local_T.final_metrics---
where slr_id = 125650291
order by RETAIL_YEAR, RETAIL_WEEK
limit 200


---- cross table creation
--- run and ready dont use
drop table if exists P_nishant_local_T.usr_cross; 
CREATE TABLE P_nishant_local_T.usr_cross AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.RTL_MONTH_BEG_DT,
	cal.AGE_FOR_RTL_WEEK_ID,
	cal.RETAIL_WK_END_DATE,
    ck.SLR_ID
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
cross JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
WHERE
    ck.SLR_CNTRY_ID = 3 AND 
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2022
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
	and ck.gmv_dt >='2021-11-01'
order by 5,2,1
---dont use
CREATE TABLE P_nishant_local_T.usr_cross_dedup AS
select distinct RETAIL_WEEK,
	   RETAIL_YEAR,
	   AGE_FOR_RTL_WEEK_ID,
	   RETAIL_WK_END_DATE,
	   RTL_MONTH_BEG_DT,
       SLR_ID
from P_nishant_local_T.usr_cross
order by 5,2,1;

--- use this cross table

drop table if exists P_nishant_local_T.usr_cross_new; 
CREATE TABLE P_nishant_local_T.usr_cross_new AS
SELECT distinct CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.RTL_MONTH_BEG_DT,
	cal.AGE_FOR_RTL_WEEK_ID,
	cal.RETAIL_WK_END_DATE,
    ck.SLR_ID
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
cross JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
WHERE
    ck.SLR_CNTRY_ID = 3 AND 
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2022
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
	and ck.gmv_dt >='2021-11-01'
order by 5,2,1




select RETAIL_WEEK,
	   RETAIL_YEAR,
	   AGE_FOR_RTL_WEEK_ID,
	   RETAIL_WK_END_DATE,
       SLR_ID, count(*) as cn
from P_nishant_local_T.usr_cross_dedup
group by 1,2,3,4,5
order by cn desc
limit 100




--- T1W PL METRICS 2
DROP TABLE IF EXISTS p_NISHANT_LOCAL_t.T1W_PL_METRICS_2;
CREATE TABLE p_NISHANT_LOCAL_t.T1W_PL_METRICS_2 AS
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
	   , SUM(PLS_PLA_ELIG_LSTG) AS T1W_PL_eligible_listings,
    	SUM(PLS_enabled_lstg) AS T1W_PLS_Live_Listings,
    	SUM(pla_lstgs) AS T1W_PLA_Live_listings
	   , (SUM(PLS_GROSS) / NULLIF(SUM(PLS_GMV), 0)) AS T1W_pls_sold_adrate
from ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN P_NBD_T.SLRSEG_allTiers_final  FN
ON FN.CAL_DT = CAL.CAL_DT
where slr_cntry_id = 3 
and cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2021
AND CAL.AGE_FOR_WEEK_ID <= -1
GROUP BY 1,2,3,4
order by 4,2,1;
LIMIT 100

---t52 PL metrics
--- not run
CREATE TABLE p_NISHANT_LOCAL_t.T52W_PL_METRICS_2 AS
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
	   , SUM(b.T1W_PL_eligible_listings) T52W_PL_eligible_listings,
    	SUM(b.T1W_PLS_Live_Listings) T52W_PLS_Live_Listings,
    	SUM(b.T1W_PLA_Live_listings) T52W_PLA_Live_listings
	   , (SUM(b.T1W_PLA_GROSS_REV) / NULLIF(SUM(b.T1W_PLS_GMV), 0)) AS T52W_pls_sold_adrate
from p_NISHANT_LOCAL_t.T1W_PL_METRICS_2 a
left join p_NISHANT_LOCAL_t.T1W_PL_METRICS_2 b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
group by 1,2,3,4


select *
from p_NISHANT_LOCAL_t.T52W_PL_METRICS_2
where T52W_pls_sold_adrate <> 0
limit 100


---- T1W_PURCHASES, GMB
DROP TABLE IF EXISTS P_nishant_local_T.GMB_T1_METRICS;
CREATE TABLE P_nishant_local_T.GMB_T1_METRICS AS
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
    ck.BYR_CNTRY_ID = 3 AND 
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;

SELECT *
FROM  P_nishant_local_T.GMB_T52_METRICS
LIMIT 100


-- GMB2 & PURCHASES T52W 
--- RUN & READY
drop table if exists P_nishant_local_T.GMB_T52_METRICS;
CREATE TABLE P_nishant_local_T.GMB_T52_METRICS AS
select a.BYR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_GMB) as T52W_GMB,
sum(b.T1W_PURCHASES) as T52W_PURCHASES
from P_nishant_local_T.GMB_T1_METRICS a
left join P_nishant_local_T.GMB_T1_METRICS b
on a.BYR_id = b.BYR_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;




---- T1W BYER COUNT distinct buyers
drop table if exists P_nishant_local_T.BYR_T1_METRICS;
CREATE TABLE P_nishant_local_T.BYR_T1_METRICS AS
SELECT CAL.RETAIL_WEEK,
	CAL.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    ck.slr_ID,
    count(distinct ck.BYR_ID) T1W_byr_cnt
FROM  ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
ON ck.GMV_DT = CAL.CAL_DT
WHERE
    ck.slr_CNTRY_ID = 3 AND 
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
GROUP BY
    1, 2, 3, 4
ORDER BY
	4, 2 ASC,1 ASC, 3 ASC;
	
---t52 buyer cnt
--- add logic
drop table if exists P_nishant_local_T.BYR_T52_METRICS;
CREATE TABLE P_nishant_local_T.BYR_T52_METRICS AS
-- SELECT CAL.RETAIL_WEEK,
-- 	CAL.RETAIL_YEAR,
-- 	cal.AGE_FOR_RTL_WEEK_ID,
--     ck.slr_ID,
--     count(DISTINCT ck2.byr_id) as t52w_byr_cnt
-- FROM  ACCESS_VIEWS.DW_CAL_DT CAL
-- LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
-- ON ck.GMV_DT = CAL.CAL_DT
-- left join (
-- SELECT CAL.RETAIL_WEEK,
-- 	CAL.RETAIL_YEAR,
-- 	cal.AGE_FOR_RTL_WEEK_ID,
--     ck.slr_ID,
--     ck.BYR_ID
-- FROM  ACCESS_VIEWS.DW_CAL_DT CAL
-- LEFT JOIN  PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
-- ON ck.GMV_DT = CAL.CAL_DT
-- WHERE
--     ck.slr_CNTRY_ID = 3 AND 
-- 	CAL.RETAIL_YEAR >= 2021 AND
--     CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
--     ck.CK_WACKO_YN = 'N' AND 
--     CK.ISCORE = 1 
-- 	AND CAL.AGE_FOR_WEEK_ID <= -1 
-- 	and ck.EU_B2C_C2C_FLAG = 'B2C'
-- ORDER BY
-- 	4, 2 ASC,1 ASC, 3 ASC
-- ) ck2
-- on ck.slr_id = ck2.slr_id
-- and (cal.AGE_FOR_RTL_WEEK_ID -ck2.AGE_FOR_RTL_WEEK_ID) >=0
-- and (cal.AGE_FOR_RTL_WEEK_ID -ck2.AGE_FOR_RTL_WEEK_ID) <52
-- WHERE
--     ck.slr_CNTRY_ID = 3 AND 
-- 	CAL.RETAIL_YEAR >= 2022 AND
--     CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
--     ck.CK_WACKO_YN = 'N' AND 
--     CK.ISCORE = 1 
-- 	AND CAL.AGE_FOR_WEEK_ID <= -1 
-- 	and ck.EU_B2C_C2C_FLAG = 'B2C'
-- GROUP BY
--     1, 2, 3, 4
-- ORDER BY
-- 	4, 2 ASC,1 ASC, 3 ASC;


CREATE TABLE P_nishant_local_T.BYR_T52_METRICS AS
select a.slr_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_byr_cnt) as T52W_byr_cnt 
from P_nishant_local_T.BYR_T1_METRICS a
left join P_nishant_local_T.BYR_T1_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;




	
	

SELECT *
FROM P_nishant_local_T.BYR_T52_METRICS
LIMIT 100


------T1W CLICK IMPRESSION, CTR, CVR ETC
drop table if exists P_nishant_local_T.AD_T1_METRICS;
CREATE TABLE P_nishant_local_T.AD_T1_METRICS
select  
slr_id, 
RETAIL_WEEK,
RETAIL_YEAR,
AGE_FOR_RTL_WEEK_ID,

SUM(total_imp_cnt) AS total_imprsns_curr,
SUM(CASE WHEN event_type_cd = 'PL' THEN total_imp_cnt ELSE 0 END) AS pl_imps_curr,
SUM(CASE WHEN event_type_cd = 'PLP' THEN total_imp_cnt ELSE 0 END) AS pla_imps_curr,
SUM(CASE WHEN event_type_cd = 'Organic' THEN total_imp_cnt ELSE 0 END) AS organic_impressions_curr,

SUM(total_click_cnt) AS total_clicks_curr,
SUM(CASE WHEN event_type_cd = 'PL' THEN total_click_cnt ELSE 0 END) AS pl_clicks_curr,
SUM(CASE WHEN event_type_cd = 'PLP' THEN total_click_cnt ELSE 0 END) AS pla_clicks_curr,
SUM(CASE WHEN event_type_cd = 'Organic' THEN total_click_cnt ELSE 0 END) AS organic_clicks_curr

from (
select
cal.cal_dt,
cal.retail_week,
cal.retail_year,
cal.AGE_FOR_RTL_WEEK_ID,
bpe.slr_id,
bpe.event_type_cd,
bpe.item_id,
bpe.total_imp_cnt,
bpe.srp_top20_slot_imp_cnt,
bpe.srp_rest_slot_imp_cnt,
bpe.merch_imp_cnt,
bpe.total_click_cnt,
bpe.ebay_click_cnt,
bpe.off_ebay_click_cnt
from bpe_v.pl_org_ads_imp_click_sd bpe 
---inner join p_ads_vi_an_t.Ads_Flagship_akugupta_240930181454_item_target_table items 
---on items.item_id=bpe.item_id
inner join ACCESS_VIEWS.dw_cal_dt cal 
on bpe.event_dt=cal.cal_dt
where 
cal.RETAIL_YEAR >=2021
and CAL.AGE_FOR_WEEK_ID <= -1 
---group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15
) base
group by 1,2,3,4

select *
from P_nishant_local_T.AD_T1_METRICS
limit 100

--- ctr view
drop table if exists P_nishant_local_T.AD_T52w_METRICS;
CREATE TABLE P_nishant_local_T.AD_T52w_METRICS as
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
from P_nishant_local_T.AD_T1_METRICS a
left join P_nishant_local_T.AD_T1_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
and a.retail_year >=2022
group by 1,2,3,4;


--- T1W DAILY BUDGET & USAGE
--- RUN NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS;
CREATE TABLE p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS AS
select seller_id,
CAL.RETAIL_WEEK,
CAL.RETAIL_YEAR,
CAL.AGE_FOR_RTL_WEEK_ID
,COALESCE(sum(ads_fee_usd), 0) as T1W_ads_fee_usd
,COALESCE(sum(daily_budget_amt_usd),0) as T1W_budget_amt_usd
,COALESCE(sum(ads_fee_usd), 0)/COALESCE(sum(daily_budget_amt_usd),0) as T1W_cmpgn_budget_usage
from ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN
(
	select 
	cmpgn_budget.slr_id as seller_id,
	cmpgn_budget.cal_dt, 
	COALESCE(sum(ads_fee_usd), 0) as ads_fee_usd,
	COALESCE(sum(cmpgn_budget.daily_budget_amt_usd),0) as daily_budget_amt_usd

	from
	(
		select 
		slr_id,
		ads_cmpgn_id,
		cal_dt,
		max(daily_budget_amt_usd) as daily_budget_amt_usd
		from 
		p_liszt_v.plcpc_at_cpc_active
		where cal_dt >='2021-01-01'	
		group by 1,2,3
	) as cmpgn_budget
	left join (
		select 
		slr_id,
		cal_dt,
		ads_cmpgn_id,
		cast(sum(ads_fee_usd) as decimal(20,2)) ads_fee_usd
		from 
		p_liszt_v.plcpc_at_ads_perf
		where cal_dt >='2021-01-01'
		group by 1,2,3
	) as cmpgn_fee on cmpgn_budget.slr_id = cmpgn_fee.slr_id and cmpgn_budget.ads_cmpgn_id = cmpgn_fee.ads_cmpgn_id and cmpgn_budget.cal_dt = cmpgn_fee.cal_dt
	group by 1,2
) cmpgn 
ON CMPGN.CAL_DT = CAL.CAL_DT
WHERE
CAL.RETAIL_YEAR >=2021
and CAL.AGE_FOR_DT_ID <= -1
group by 1,2,3,4;

----T52
CREATE TABLE p_NISHANT_LOCAL_T.T52W_BUDGET_METRICS AS
select a.seller_id,
a.RETAIL_WEEK,
a.RETAIL_YEAR,
a.AGE_FOR_RTL_WEEK_ID
,COALESCE(sum(b.T1W_ads_fee_usd),0) AS T52W_ads_fee_usd
,COALESCE(sum(b.T1W_budget_amt_usd),0) T52W_budget_amt_usd
,COALESCE(sum(b.T1W_ads_fee_usd),0)/COALESCE(sum(b.T1W_budget_amt_usd),0) as T52W_cmpgn_budget_usage
FROM p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS A
LEFT JOIN p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS B
on a.sElLEr_id = b.sELlEr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
and a.retail_year >=2022
group by 1,2,3,4;


----T1W RETAIL STANDARD PRE TRANS
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS;
CREATE TABLE P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS AS
SELECT 
slr_id,
TXN_RETAIL_YEAR,TXN_RETAIL_WEEK,TXN_RW_END_DATE,txn_week_age,
SLR_COUNTRY,B2C_C2C_FLAG,
      sum(CASE WHEN FIN3_FLAG =1 then BI ELSE 0 END) as T1W_FIN3_BI,
      sum(CASE WHEN PROM_3D_FLAG =1 then BI ELSE 0 END) as T1W_PROM_3D_BI,
      sum(CASE WHEN ACTUAL_3D_FLAG =1 then BI ELSE 0 END) as T1W_ACTUAL_3D_BI,
      sum(CASE WHEN LATE_DELIVERY_FLAG =1 then BI ELSE 0 END) as T1W_LATE_DELIVERY_BI,
      sum(CASE WHEN FREE_SHIPPING_FLAG =1 then TXNS ELSE 0 END) as T1W_FREE_SHIPPING_TXNS,
      sum(CASE WHEN VALID_TRACKING_FLAG =1 then BI ELSE 0 END) as T1W_VALID_TRACKING_BI,
      sum(CASE WHEN TRACKING_UPLOAD_FLAG =1 then BI	 ELSE 0 END) as T1W_TRACKING_UPLOAD_BI,
      sum(CASE WHEN HT_1D_FLAG =1 then TXNS ELSE 0 END) as T1W_HT_0_1D_TXNS,
	  sum(CASE WHEN INR_14D_NR_FLAG =1 then Txns ELSE 0 END) as T1W_INR_14D_TXNS,
	  sum(CASE WHEN ESC_INR_14D_NR_FLAG =1 then Txns ELSE 0 END) as T1W_ESC_INR_14D_TXNS
FROM 
(
SELECT
	txn.slr_id,
    CAL.RETAIL_YEAR as TXN_RETAIL_YEAR , 
	CAL.RETAIL_WEEK AS TXN_RETAIL_WEEK,
	CAL.RETAIL_WK_END_DATE AS TXN_RW_END_DATE,
	CAL.AGE_FOR_RTL_WEEK_ID as txn_week_age,cal.AGE_FOR_QTR_ID as txn_qtr_age,
	CALD.RETAIL_YEAR as EDD_RETAIL_YEAR , CALD.RETAIL_WEEK AS EDD_RETAIL_WEEK,CALD.RETAIL_WK_END_DATE AS EDD_RW_END_DATE,cald.AGE_FOR_RTL_WEEK_ID as edd_week_age,
	--CASE WHEN BYR_CNTRY_ID = SLR_CNTRY_ID THEN 'DOMESTIC' ELSE 'IMPORTS' END AS CBT,
	CASE WHEN SLR_CNTRY_ID = 3 THEN 'UK' ELSE 'NON-UK' END AS SLR_COUNTRY,
	CASE WHEN ITEM_CNTRY_ID = 3 THEN 'UK' ELSE 'NON-UK' END AS ITEM_LOCATION,
	B2C_C2C_FLAG,
	VERTICAL,
	PERFECT_DELIVERY_FLAG,
	ASP_TRANCHE,
-- 	PERFECT_DELIVERY_FLAG_MIN,
	CASE WHEN FREE_SHIPPING_FLAG = 1 AND EDD_DAYS <= 3 THEN 1 ELSE 0 END AS FIN3_FLAG,
	CASE WHEN FREE_SHIPPING_FLAG = 1 AND EDD_DAYS_MIN <= 3 THEN 1 ELSE 0 END AS FIN3_FLAG_MIN,
	CASE WHEN EDD_DAYS <= 3 THEN 1 ELSE 0 END AS PROM_3D_FLAG,
	CASE WHEN EDD_DAYS <= 5 THEN 1 ELSE 0 END AS PROM_5D_FLAG,
	CASE WHEN EDD_DAYS_MIN <= 3 THEN 1 ELSE 0 END AS PROM_3D_FLAG_MIN,
	CASE WHEN DLVRY_DAYS <= 3 THEN 1 ELSE 0 END AS ACTUAL_3D_FLAG,
	LATE_DELIVERY_FLAG,
	FREE_SHIPPING_FLAG,
	VALID_TRACKING_FLAG,
	case when DLVRY_DAYS is not null then 1 else 0 end as  valid_tracking_flag_1,
	TRACKING_UPLOAD_FLAG,
	CASE WHEN SPCFD_HNDL_DAYS <= 1 THEN 1 ELSE 0 END AS HT_1D_FLAG,
	INR_14D_NR_FLAG,
	ESC_INR_14D_NR_FLAG,
	CLAIM_FLAG,
	RISK_FLAG,
	CLAIM_OPEN_EDD_MATURITY_DAYS,
	LOW_DSR_ST_FLAG,
	LOW_DSR_SC_FLAG,
	CASE WHEN LATE_DELIVERY_FLAG = 1 OR (CLAIM_FLAG = 'INR' AND CLAIM_OPEN_EDD_MATURITY_DAYS <= 28 AND RISK_FLAG = 0) 
	OR LOW_DSR_ST_FLAG = 1 OR LOW_DSR_SC_FLAG = 1  THEN 1 ELSE 0 END AS SOE_FLAG,
	SUM(GMV) AS GMV, 
	SUM(SOLD_ITEMS) AS BI, 
-- 	SUM(GMV)/SUM(SOLD_ITEMS) as ASP,
	SUM(TXNS) AS TXNS, 
	CASE WHEN VALID_TRACKING_FLAG =1 THEN SUM(TXNS) else 0 END AS TRACKED_TXNS,
	CASE WHEN VALID_TRACKING_FLAG =1 THEN sum(SOLD_ITEMS) ELSE 0 END AS TRACKED_BI,
	SUM(EDD_DAYS) AS EST_DAYS,
	 SUM(DLVRY_DAYS) AS DEL_DAYS
	,count(EDD_DAYS) as txns_1
	,count(DLVRY_DAYS) as tracked_txns_1
	,sum(case when EDD_DAYS<= 3 then EDD_DAYS else 0 end ) EST_DAYS_3d
	,sum(case when EDD_DAYS_MIN<= 3 then EDD_DAYS else 0 end ) EST_DAYS_3d_min
	,sum(case when EDD_DAYS <= 3 then DLVRY_DAYS else 0 end) DEL_DAYS_3d
	,sum(case when EDD_DAYS_MIN <= 3 then DLVRY_DAYS else 0 end) DEL_DAYS_3d_min
	,count(case when EDD_DAYS <= 3 then EDD_DAYS else NULL end) as txns_3d
	,count(case when EDD_DAYS_MIN <= 3 then EDD_DAYS else NULL end) as txns_3d_min
	,count(case when EDD_DAYS <= 3 then DLVRY_DAYS else NULL end) as tracked_txns_3d
	,count(case when EDD_DAYS_MIN <= 3 then DLVRY_DAYS else NULL end) as tracked_txns_3d_min
--count( case when  EDD_RW_END_DATE between  date_sub(EDD_RW_END_DATE,92) and EDD_RW_END_DATE then TXN.BYR_ID end) AS AB,
--count( distinct case when LATE_DELIVERY_FLAG = 1 OR (CLAIM_FLAG = 'INR' AND CLAIM_OPEN_EDD_MATURITY_DAYS <= 28 AND RISK_FLAG = 0) 
--	OR LOW_DSR_ST_FLAG = 1 OR LOW_DSR_SC_FLAG = 1 AND  EDD_RW_END_DATE between  date_sub(EDD_RW_END_DATE,92) and EDD_RW_END_DATE then TXN.BYR_ID end) AS AB_WITH_SOE

FROM P_UKANALYTICS_T.SHIPPING_FUNNEL_TXN_BASE TXN
INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = TXN.TRANS_DT
left JOIN ACCESS_VIEWS.DW_CAL_DT CALD ON CALD.CAL_DT = TXN.EDD_DT
-- LEFT JOIN P_AWANG_OPS_T.UK_MANAGED_SELLERS_ALL MGD ON MGD.SELLER_ID = TXN.SLR_ID AND MGD.FOCUSED_FLAG IN ('Focus','focus')

WHERE 1=1
	AND BYR_CNTRY_ID = 3
	AND TRANS_SITE_ID = 3
	AND CAL.RETAIL_YEAR >= 2019
	AND CAL.AGE_FOR_RTL_WEEK_ID <= -1
GROUP BY 
	txn.slr_id,
	CAL.RETAIL_YEAR ,CAL.RETAIL_WEEK,CAL.RETAIL_WK_END_DATE,CAL.AGE_FOR_RTL_WEEK_ID,cal.AGE_FOR_QTR_ID,
		CALD.RETAIL_YEAR ,CALD.RETAIL_WEEK ,CALD.RETAIL_WK_END_DATE,cald.AGE_FOR_RTL_WEEK_ID,
		SLR_CNTRY_ID,ITEM_CNTRY_ID,
         B2C_C2C_FLAG,VERTICAL,PERFECT_DELIVERY_FLAG,FREE_SHIPPING_FLAG,EDD_DAYS,EDD_DAYS_MIN,DLVRY_DAYS,
		 LATE_DELIVERY_FLAG,CLAIM_FLAG,RISK_FLAG,ASP_TRANCHE,
	CLAIM_OPEN_EDD_MATURITY_DAYS,
	LOW_DSR_ST_FLAG,
	LOW_DSR_SC_FLAG,FREE_SHIPPING_FLAG,VALID_TRACKING_FLAG,valid_tracking_flag_1,TRACKING_UPLOAD_FLAG,SPCFD_HNDL_DAYS,
		 INR_14D_NR_FLAG,ESC_INR_14D_NR_FLAG,SOE_FLAG) a
where TXN_RETAIL_YEAR >= 202
	and B2C_C2C_FLAG = 'B2C'
GROUP BY 
	1,2,3,4,5,6,7;

SELECT *
FROM P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS
LIMIT 100

---- T52 SELLER STANDARDS
CREATE TABLE P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS AS
SELECT 
A.slr_id,
A.TXN_RETAIL_YEAR,A.TXN_RETAIL_WEEK,A.TXN_RW_END_DATE,A.txn_week_age,
sum(B.T1W_FIN3_BI) AS T52W_FIN3_BI,
      sum(B.T1W_PROM_3D_BI) T52W_PROM_3D_BI,
      sum(B.T1W_ACTUAL_3D_BI) T52W_ACTUAL_3D_BI,
      sum(B.T1W_LATE_DELIVERY_BI) T52W_LATE_DELIVERY_BI,
      sum(B.T1W_FREE_SHIPPING_TXNS) T52W_FREE_SHIPPING_TXNS,
      sum(B.T1W_VALID_TRACKING_BI) T52W_VALID_TRACKING_BI,
      sum(B.T1W_TRACKING_UPLOAD_BI) T52W_TRACKING_UPLOAD_BI,
      sum(B.T1W_HT_0_1D_TXNS) T52W_HT_0_1D_TXNS,
	  sum(B.T1W_INR_14D_TXNS) T52W_INR_14D_TXNS,
	  sum(B.T1W_ESC_INR_14D_TXNS) T52W_ESC_INR_14D_TXNS

from P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS a
left join P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS b
on a.SLR_id = b.SLR_id
and (a.txn_week_age -b.txn_week_age) >=0
and (a.txn_week_age -b.txn_week_age) <52
where a.TXN_RETAIL_YEAR >= 2022
GROUP BY 1,2,3,4,5
ORDER BY 1,3,2;


--- RETAIL STANDARDS 2
CREATE TABLE P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS_2 AS
Select a.SLR_ID
	, a.RETAIL_YEAR
	, a.RETAIL_WEEK
	, a.retail_wk_end_date
	, A.AGE_FOR_RTL_WEEK_ID
	, SUM(FIN3_FLAG) as T1W_LL_with_Free_in_3_Num
	, SUM(FIN3_FLAG)*100/SUM(TOTAL_LL) as T1W_LL_with_Free_in_3_pct
	, SUM(HT_1D_FLAG) as T1W_LL_1HD_Num
	, SUM(HT_1D_FLAG)*100/SUM(TOTAL_LL) as T1W_LL_1HD_pct
	, SUM(NEXT_DAY_SHIP_FLAG) as T1W_LL_next_day_Num
	, SUM(NEXT_DAY_SHIP_FLAG)*100/SUM(TOTAL_LL) as T1W_LL_next_day_pct
	, SUM(TRACKED_SERVICE_FLAG) as T1W_LL_tracked_services_Num
	, SUM(TRACKED_SERVICE_FLAG)*100/SUM(TOTAL_LL) as T1W_LL_tracked_services_pct
	, SUM(EDD_3D_FLAG) as T1W_LL_0_3D_EDD_Num
	, SUM(EDD_3D_FLAG)*100 / SUM(TOTAL_LL) as T1W_LL_0_3D_EDD_pct
	, SUM(TOTAL_LL) as T1W_LL_Retail_Std_Deno
from
(SELECT CAL.RETAIL_YEAR
	, CAL.RETAIL_WEEK
	, CAT.BSNS_VRTCL_NAME
 	, cal.retail_wk_end_date
	, CAL.AGE_FOR_RTL_WEEK_ID
	, LL.SLR_ID
	, COUNT(DISTINCT CASE WHEN EDD_3D_FLAG = 1 then ll.ITEM_ID END) as EDD_3D_FLAG
	, COUNT(DISTINCT CASE WHEN FIN3_FLAG = 1 then ll.ITEM_ID else 0 END) as FIN3_FLAG
	, COUNT(DISTINCT CASE WHEN TRACKED_SERVICE_FLAG = 1 then ll.ITEM_ID END) as TRACKED_SERVICE_FLAG
	, COUNT(DISTINCT CASE WHEN HT_1D_FLAG = 1 then ll.ITEM_ID END) as HT_1D_FLAG
	, COUNT(DISTINCT CASE WHEN NEXT_DAY_SHIP_FLAG = 1 then ll.ITEM_ID else 0 END) as NEXT_DAY_SHIP_FLAG
	, COUNT(DISTINCT ll.ITEM_ID) AS TOTAL_LL
FROM P_UKANALYTICS_T.SHIPPING_FUNNEL_LL_BASE_curr LL
INNER JOIN ACCESS_VIEWS.DW_LSTG_ITEM LI on LL.item_ID = LI.ITEM_ID and LI.ITEM_SITE_ID =3 --and LI.AUCT_TYPE_CODE NOT IN (12,15)
INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.SITE_ID = LI.ITEM_SITE_ID AND CAT.LEAF_CATEG_ID = LI.LEAF_CATEG_ID
INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON CAL.CAL_DT = LL.LL_DATE
where CAT.SAP_CATEGORY_ID NOT IN (5,7,41,23) -- CORE CATEGORIES ONLY
 and cal.RETAIL_YEAR >= 2021
	  AND LI.ITEM_SITE_ID = 3
	  AND CAL.AGE_FOR_RTL_WEEK_ID <= -1
GROUP BY 1,2,3,4,5,6 ) a
	--INNER JOIN P_masandhu_T.opens_slrs b on b.slr_id=a.slr_id
-- 	join Slrs b on a.slr_id=b.slr_id
GROUP BY 1,2,3,4,5;

SELECT *
FROM P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS_2
LIMIT 10

---- T52 SELLER STANDARDS
DROP TABLE IF EXISTS P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS_2;
CREATE TABLE P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS_2 AS
SELECT 
	  a.SLR_ID
	, a.RETAIL_YEAR
	, a.RETAIL_WEEK
	, SUM(B.T1W_LL_Retail_Std_Deno) T52W_LL_Retail_Std_Deno
	, SUM(B.T1W_LL_with_Free_in_3_Num) T52W_LL_with_Free_in_3_Num
	, SUM(B.T1W_LL_with_Free_in_3_Num)*100/SUM(B.T1W_LL_Retail_Std_Deno) as LL_with_Free_in_3_pct
	, SUM(B.T1W_LL_1HD_Num) T52W_LL_1HD_Num
	, SUM(B.T1W_LL_1HD_Num)*100/SUM(B.T1W_LL_Retail_Std_Deno) as T52W_LL_1HD_pct
	, SUM(B.T1W_LL_next_day_Num) T52W_LL_next_day_Num
	, SUM(B.T1W_LL_next_day_Num)*100/SUM(B.T1W_LL_Retail_Std_Deno) as T52W_LL_next_day_pct
	, SUM(B.T1W_LL_tracked_services_Num) T52W_LL_tracked_services_Num
	, SUM(B.T1W_LL_tracked_services_Num)*100/SUM(B.T1W_LL_Retail_Std_Deno) as T52W_LL_tracked_services_pct
	, SUM(B.T1W_LL_0_3D_EDD_Num) T52W_LL_0_3D_EDD_Num
	, SUM(B.T1W_LL_0_3D_EDD_Num)*100 / SUM(B.T1W_LL_Retail_Std_Deno) as T1W_LL_0_3D_EDD_pct
from P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS_2 a
left join P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS_2 b
on a.SLR_id = b.SLR_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.RETAIL_YEAR >= 2022
GROUP BY 1,2,3
ORDER BY 1,3,2;

------------------ new metrics
--- seller address
create table P_nishant_local_t.seller_address as
select user_id as seller_id, city, pstl_code as zip
from PRS_SECURE_V.DW_USERS adr
where user_id in 
(
select distinct slr_id as user_id
from P_nishant_local_T.usr_cross_new
)

----- gmv dominant category ( only add table 4 to join )
----- table 1: d cat base
create table P_nishant_local_T.dl_base as
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
    ck.SLR_CNTRY_ID = 3 AND 
	CAL.RETAIL_WEEK >= 1 AND
	CAL.RETAIL_YEAR >= 2021 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND 
    CK.ISCORE = 1 
	AND CAL.AGE_FOR_WEEK_ID <= -1 
	and ck.EU_B2C_C2C_FLAG = 'B2C'
GROUP BY
    1, 2, 3, 4, 5
ORDER BY
	4, 2 ASC,1 ASC, 5

select *
from P_nishant_local_T.dl_base
limit 100


-----TABLE 2: t1w_ domninant
select RETAIL_WEEK,
	RETAIL_YEAR,
	AGE_FOR_RTL_WEEK_ID,
    SLR_ID,
	BSNS_VRTCL_NAME,
	T1W_GMV,
	rank() over (PARTITION by RETAIL_WEEK,RETAIL_YEAR,AGE_FOR_RTL_WEEK_ID,SLR_ID order by t1w_gmv desc ) as rnk 
from P_nishant_local_T.dl_base
order by 4, 2, 1, 7

---TABLE 3: t52w dominant
select RETAIL_WEEK,
	RETAIL_YEAR,
	AGE_FOR_RTL_WEEK_ID,
    SLR_ID,
	BSNS_VRTCL_NAME,
	t52w_gmv,
    rank() over (PARTITION by RETAIL_WEEK,RETAIL_YEAR,AGE_FOR_RTL_WEEK_ID,SLR_ID order by t52w_gmv desc ) as rnk 
from (
select a.RETAIL_WEEK,
	a.RETAIL_YEAR,
	a.AGE_FOR_RTL_WEEK_ID,
    a.SLR_ID,
	a.BSNS_VRTCL_NAME,
	sum(b.T1W_GMV) as t52w_GMV
from P_nishant_local_T.dl_base a
left join P_nishant_local_T.dl_base b
on a.slr_id = b.slr_id
and a.BSNS_VRTCL_NAME = b.BSNS_VRTCL_NAME
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
group by 1,2,3,4,5)
order by 4, 2, 1, 7

----TABLE 4 DOMINANT t1w & t52 dominant vertical combined
create table P_nishant_local_t.dominant_vertical_gmv as 
select a.retail_week,
	   a.retail_year,
	   a.slr_id,
	   a.BSNS_VRTCL_NAME as t1w_dominant_vertical,
	   b.BSNS_VRTCL_NAME as t52w_dominant_vertical
	from (select RETAIL_WEEK,
	RETAIL_YEAR,
	AGE_FOR_RTL_WEEK_ID,
    SLR_ID,
	BSNS_VRTCL_NAME,
	T1W_GMV,
	rank() over (PARTITION by RETAIL_WEEK,RETAIL_YEAR,AGE_FOR_RTL_WEEK_ID,SLR_ID order by t1w_gmv desc ) as rnk 
from P_nishant_local_T.dl_base
order by 4, 2, 1, 7) a
left join 
(
select RETAIL_WEEK,
	RETAIL_YEAR,
	AGE_FOR_RTL_WEEK_ID,
    SLR_ID,
	BSNS_VRTCL_NAME,
	t52w_gmv,
    rank() over (PARTITION by RETAIL_WEEK,RETAIL_YEAR,AGE_FOR_RTL_WEEK_ID,SLR_ID order by t52w_gmv desc ) as rnk 
from (
select a.RETAIL_WEEK,
	a.RETAIL_YEAR,
	a.AGE_FOR_RTL_WEEK_ID,
    a.SLR_ID,
	a.BSNS_VRTCL_NAME,
	sum(b.T1W_GMV) as t52w_GMV
from P_nishant_local_T.dl_base a
left join P_nishant_local_T.dl_base b
on a.slr_id = b.slr_id
and a.BSNS_VRTCL_NAME = b.BSNS_VRTCL_NAME
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
group by 1,2,3,4,5)
order by 4, 2, 1, 7
) b 
on a.retail_week = b.retail_week
and a.retail_year = b.retail_year
and a.slr_id = b.slr_id
and b.rnk =1
where a.rnk = 1
and a.retail_year >=2022
order by 3,2,1




--- FINAL TABLE JOIN 

SELECT *
FROM P_nishant_local_T.usr_cross_dedup
LIMIT 10




DROP TABLE IF EXISTS P_nishant_local_T.final_metrics_2;
create table P_nishant_local_T.final_metrics_2 as
SELECT BASE.RETAIL_YEAR, BASE.RETAIL_WEEK, BASE.SLR_ID,
	   AI.CITY AS SELLER_addrress, AI.ZIP,
	   AJ.t1w_dominant_vertical,
	   AJ.t52w_dominant_vertical,
	   A.T1W_GMV, A.T1W_SI, (A.T1W_GMV/A.T1W_SI) as T1W_ASP,
	   A.T1W_CBT_GMV, A.T1W_CBT_SI, (A.T1W_CBT_GMV/A.T1W_CBT_SI) AS T1W_CBT_ASP,
	   B.T52W_GMV, B.T52W_SI, (B.T52W_GMV/B.T52W_SI) AS T52W_ASP,
	   B.T52W_CBT_GMV, B.T52W_CBT_SI, (B.T52W_CBT_GMV/B.T52W_CBT_SI) AS T52W_CBT_ASP,
	   A2.T13W_GMV, A2.T13W_SI, (A2.T13W_GMV/A2.T13W_SI) AS T13W_ASP,
	   A2.T13W_CBT_GMV, A2.T13W_CBT_SI, (A2.T13W_CBT_GMV/A2.T13W_CBT_SI) AS T13W_CBT_ASP,
	    case when B.T52W_GMV <=100000 then '$0-100k'
 	   when B.T52W_GMV <=250000 and B.T52W_GMV >100000 then '$100k-250k'
	   when B.T52W_GMV >250000 then '>$250k' END
 	   as t52_gmv_bucket,
	   C.T1W_LL,
	   D.T52W_LL,
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
	   I.T1W_SELLERH_CNT,I.T1W_NATIVE_CNT,
	   J.T52W_SELLERH_CNT,J.T52W_NATIVE_CNT,
	   K.T13W_Variable_FVF, -- NET
       K.T13W_Fixed_FVF --- 
       ,K.T13W_Insertion_Fee ---- 
       ,K.T13W_Subscription_Fee ---- 
       ,K.T13W_PL_Fee,
	   L.T52W_Variable_FVF, -- NET
       L.T52W_Fixed_FVF --- 
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
		S.SLR_TYPE LATEST_SELLER_TYPE1,
		W.SLR_TYPE_L2 LATEST_SELLER_TYPE2,
		W.SLR_TYPE_L3 LATEST_SELLER_TYPE3,
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
	, U.T52W_PLA_listing_adoption_percnt,
	--V.T1W_pls_si,
    --V.T1W_pla_si,
    --V.T1W_sfa_si,
    ---V.T1W_pls_gmv,
    ----V.T1W_pla_gmv,
    ---V.T1W_sfa_gmv,
	V.T1W_pls_ad_fee,
	V.T1W_pla_ad_fee, 
    V.T1W_sfa_ad_fee,
	V.T1W_total_ad_fee,
	---V2.T52W_pls_si,
    ----V2.T52W_pla_si,
    ---V2.T52W_sfa_si,
--     V2.T52W_pls_gmv,
--     V2.T52W_pla_gmv,
--     V2.T52W_sfa_gmv,
	V2.T52W_pls_ad_fee,
	V2.T52W_pla_ad_fee, 
    V2.T52W_sfa_ad_fee,
	V2.T52W_total_ad_fee,
	X.T1W_PLS_NET_REV
   ,X.T1W_PLS_GROSS_REV
   ,X.T1W_PLA_NET_REV
   ,X.T1W_PLA_GROSS_REV
   ,X.T1W_PLS_GMV
    ,X.T1W_PLS_SI
	   ,X.T1W_PLA_GMV
	   ,X.T1W_PLA_SI
	   ,X.T1W_PL_REV
	   , X.T1W_PL_GMV
	   , X.T1W_PL_eligible_listings,
    	X.T1W_PLS_Live_Listings,
    	X.T1W_PLA_Live_listings
	   , X.T1W_pls_sold_adrate,
	   X2.T52W_PLS_NET_REV
   ,X2.T52W_PLS_GROSS_REV
   ,X2.T52W_PLA_NET_REV
   ,X2.T52W_PLA_GROSS_REV
   ,X2.T52W_PLS_GMV
    ,X2.T52W_PLS_SI
	   ,X2.T52W_PLA_GMV
	   ,X2.T52W_PLA_SI
	   ,X2.T52W_PL_REV
	   , X2.T52W_PL_GMV
	   , X2.T52W_PL_eligible_listings,
    	X2.T52W_PLS_Live_Listings,
    	X2.T52W_PLA_Live_listings
	   , X2.T52W_pls_sold_adrate,
	    Y.T1W_GMB,
        Y.T1W_PURCHASES,
		Y2.T52W_GMB,
        Y2.T52W_PURCHASES,
		Z.T1W_byr_cnt,
		Z2.T52W_byr_cnt,
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
		,AE.T1W_FIN3_BI,
      AE.T1W_PROM_3D_BI,
      AE.T1W_ACTUAL_3D_BI,
      AE.T1W_LATE_DELIVERY_BI,
      AE.T1W_FREE_SHIPPING_TXNS,
      AE.T1W_VALID_TRACKING_BI,
      AE.T1W_TRACKING_UPLOAD_BI,
      AE.T1W_HT_0_1D_TXNS,
	  AE.T1W_INR_14D_TXNS,
	  AE.T1W_ESC_INR_14D_TXNS
	  ,AF.T52W_FIN3_BI,
      AF.T52W_PROM_3D_BI,
      AF.T52W_ACTUAL_3D_BI,
      AF.T52W_LATE_DELIVERY_BI,
      AF.T52W_FREE_SHIPPING_TXNS,
      AF.T52W_VALID_TRACKING_BI,
      AF.T52W_TRACKING_UPLOAD_BI,
      AF.T52W_HT_0_1D_TXNS,
	  AF.T52W_INR_14D_TXNS,
	  AF.T52W_ESC_INR_14D_TXNS,
	AG.T1W_LL_with_Free_in_3_pct
	, AG.T1W_LL_1HD_pct
	, AG.T1W_LL_next_day_pct
	, AG.T1W_LL_tracked_services_pct
	, AG.T1W_LL_0_3D_EDD_pct,
	AH.LL_with_Free_in_3_pct AS T52W_LL_with_Free_in_3_pct
	, AH.T52W_LL_1HD_pct
	, AH.T52W_LL_next_day_pct
	, AH.T52W_LL_tracked_services_pct
	, AH.T1W_LL_0_3D_EDD_pct AS T52W_LL_0_3D_EDD_pct

FROM P_nishant_local_T.usr_cross_new BASE --- need to fix (fixed) changed base table
LEFT JOIN P_nishant_local_T.GMV_T1_METRICS A ----gtg
ON A.SLR_ID = BASE.SLR_ID
AND A.RETAIL_WEEK = BASE.RETAIL_WEEK
AND A.RETAIL_YEAR = BASE.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMV_T13_METRICS A2 ---gtg
ON A2.SLR_ID = BASE.SLR_ID
AND A2.RETAIL_WEEK = BASE.RETAIL_WEEK
AND A2.RETAIL_YEAR = BASE.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMV_T52_METRICS B ---gtg
ON BASE.SLR_ID = B.SLR_ID
AND BASE.RETAIL_WEEK = B.RETAIL_WEEK
AND BASE.RETAIL_YEAR = B.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LL_T1_METRICS C   -- need to fix (fixed)
ON BASE.SLR_ID = C.SLR_ID
AND BASE.RETAIL_WEEK = C.RETAIL_WEEK
AND BASE.RETAIL_YEAR = C.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LL_T52_METRICS D --- need to fix (fixed)
ON BASE.SLR_ID = D.SLR_ID
AND BASE.RETAIL_WEEK = D.RETAIL_WEEK
AND BASE.RETAIL_YEAR = D.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_DEFECT_METRICS E --- gtg
ON BASE.SLR_ID = E.SLR_ID
AND BASE.RETAIL_WEEK = E.RETAIL_WEEK
AND BASE.RETAIL_YEAR = E.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_DEFECT_METRICS F --- gtg
ON BASE.SLR_ID = F.SLR_ID
AND BASE.RETAIL_WEEK = F.RETAIL_WEEK
AND BASE.RETAIL_YEAR = F.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.LATEST_SELLER_STANDARD_METRICS G -- gtg
ON BASE.SLR_ID = G.USER_ID
AND BASE.RETAIL_WEEK = G.RETAIL_WEEK
AND BASE.RETAIL_YEAR = G.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.STORE_KPI_METRICS H --- gtg
ON BASE.SLR_ID = H.SLR_ID
AND BASE.RETAIL_WEEK = H.RETAIL_WEEK
AND BASE.RETAIL_YEAR = H.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T1W_NSH_USAGE_METRICS I --- gtg
ON BASE.SLR_ID = I.SLR_ID
AND BASE.RETAIL_WEEK = I.RETAIL_WEEK
AND BASE.RETAIL_YEAR = I.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN P_nishant_local_T.T52W_NSH_USAGE_METRICS J -- gtg
ON BASE.SLR_ID = J.SLR_ID
AND BASE.RETAIL_WEEK = J.RETAIL_WEEK
AND BASE.RETAIL_YEAR = J.RETAIL_YEAR
and BASE.retail_year >= 2022
LEFT JOIN  P_nishant_local_T.T13W_FEE K --- gtg
ON BASE.SLR_ID = K.SLR_ID
AND BASE.RETAIL_WEEK = K.RETAIL_WEEK
AND BASE.RETAIL_YEAR = K.RETAIL_YEAR
LEFT JOIN  P_nishant_local_T.T52W_FEE L --- gtg
ON BASE.SLR_ID = L.SLR_ID
AND BASE.RETAIL_WEEK = L.RETAIL_WEEK
AND BASE.RETAIL_YEAR = L.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T1W_LISTING_METRICS M ---- gtg
ON BASE.SLR_ID = M.SLR_ID
AND BASE.RETAIL_WEEK = M.RETAIL_WEEK
AND BASE.RETAIL_YEAR = M.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T52W_LISTING_METRICS N --- gtg
ON BASE.SLR_ID = N.SLR_ID
AND BASE.RETAIL_WEEK = N.RETAIL_WEEK
AND BASE.RETAIL_YEAR = N.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T1W_OUTB_METRICS O ---- gtg
ON BASE.SLR_ID = O.ID
AND BASE.RETAIL_WEEK = O.RETAIL_WEEK
AND BASE.RETAIL_YEAR = O.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T52W_OUTB_METRICS P -- gtg
ON BASE.SLR_ID = P.ID
AND BASE.RETAIL_WEEK = P.RETAIL_WEEK
AND BASE.RETAIL_YEAR = P.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T1W_INB_METRICS Q --- gtg
ON BASE.SLR_ID = Q.ID
AND BASE.RETAIL_WEEK = Q.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Q.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.T52W_INB_METRICS R --- gtg
ON BASE.SLR_ID = R.ID
AND BASE.RETAIL_WEEK = R.RETAIL_WEEK
AND BASE.RETAIL_YEAR = R.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_T.new_or_reactivated_seller S ---gtg
ON BASE.SLR_ID = S.SLR_ID
AND BASE.RETAIL_WEEK = S.RETAIL_WEEK
AND BASE.RETAIL_YEAR = S.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS T ---gtg
ON BASE.SLR_ID = T.SLR_ID
AND BASE.RETAIL_WEEK = T.RETAIL_WEEK
AND BASE.RETAIL_YEAR = T.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T52W_PL_PEN_METRICS U --- gtg
ON BASE.SLR_ID = U.SLR_ID
AND BASE.RETAIL_WEEK = U.RETAIL_WEEK
AND BASE.RETAIL_YEAR = U.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T1W_pl_METRICS V  ---gtg
ON BASE.SLR_ID = V.SELLER_ID
AND BASE.RETAIL_WEEK = V.RETAIL_WEEK
AND BASE.RETAIL_YEAR = V.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T52W_pl_METRICS V2 ---gtg
ON BASE.SLR_ID = V2.SELLER_ID
AND BASE.RETAIL_WEEK = V2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = V2.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.latest_seller_type W ----gtg
ON BASE.SLR_ID = W.SLR_ID
AND MONTH(BASE.RETAIL_WK_END_DATE) = W.CAL_MONTH
AND BASE.RETAIL_YEAR = W.CAL_YEAR
LEFT JOIN p_NISHANT_LOCAL_t.T1W_PL_METRICS_2 X ----gtg
ON BASE.SLR_ID = X.SLR_ID
AND BASE.RETAIL_WEEK = X.RETAIL_WEEK
AND BASE.RETAIL_YEAR = X.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_t.T52W_PL_METRICS_2 X2 ---gtg
ON BASE.SLR_ID = X2.SLR_ID
AND BASE.RETAIL_WEEK = X2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = X2.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMB_T1_METRICS Y ---gtg
ON BASE.SLR_ID = Y.BYR_ID
AND BASE.RETAIL_WEEK = Y.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Y.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.GMB_T52_METRICS Y2 --gtg
ON BASE.SLR_ID = Y2.BYR_ID
AND BASE.RETAIL_WEEK = Y2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Y2.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.BYR_T1_METRICS Z ---gtg
ON BASE.SLR_ID = Z.SLR_ID
AND BASE.RETAIL_WEEK = Z.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Z.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.BYR_T52_METRICS Z2 --- need to fix (fixed)
ON BASE.SLR_ID = Z2.SLR_ID
AND BASE.RETAIL_WEEK = Z2.RETAIL_WEEK
AND BASE.RETAIL_YEAR = Z2.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.AD_T1_METRICS AA --- gtg
ON BASE.SLR_ID = AA.SLR_ID
AND BASE.RETAIL_WEEK = AA.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AA.RETAIL_YEAR
LEFT JOIN P_nishant_local_T.AD_T52w_METRICS AB ---gtg
ON BASE.SLR_ID = AB.SLR_ID
AND BASE.RETAIL_WEEK = AB.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AB.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_T.T1W_BUDGET_METRICS AC ---gtg
ON BASE.SLR_ID = AC.SELLER_ID
AND BASE.RETAIL_WEEK = AC.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AC.RETAIL_YEAR
LEFT JOIN p_NISHANT_LOCAL_T.T52W_BUDGET_METRICS AD --- need to fix (fixed)
ON BASE.SLR_ID = AD.SELLER_ID
AND BASE.RETAIL_WEEK = AD.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AD.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS AE ---gtg
ON BASE.SLR_ID = AE.SlR_ID
AND BASE.RETAIL_WEEK = AE.TXN_RETAIL_WEEK
AND BASE.RETAIL_YEAR = AE.TXN_RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS AF --- gtg
ON BASE.SLR_ID = AF.SLR_ID
AND BASE.RETAIL_WEEK = AF.TXN_RETAIL_WEEK
AND BASE.RETAIL_YEAR = AF.TXN_RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T1_RETAIL_STANDARDS_2 AG ---gtg
ON BASE.SLR_ID = AG.SLR_ID
AND BASE.RETAIL_WEEK = AG.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AG.RETAIL_YEAR
LEFT JOIN P_NISHANT_LOCAL_T.T52_RETAIL_STANDARDS_2 AH ---gtg
ON BASE.SLR_ID = AH.SLR_ID
AND BASE.RETAIL_WEEK = AH.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AH.RETAIL_YEAR
LEFT JOIN P_nishant_local_t.seller_address AI
ON BASE.SLR_ID = AI.SELLER_ID
LEFT JOIN P_nishant_local_t.dominant_vertical_gmv Aj
ON BASE.SLR_ID = Aj.SLR_ID
AND BASE.RETAIL_WEEK = AJ.RETAIL_WEEK
AND BASE.RETAIL_YEAR = AJ.RETAIL_YEAR
--;

SELECT COUNT(*)
FROM P_nishant_local_t.seller_address


CREATE TABLE P_nishant_local_T.final_metrics_3 AS
SELECT *, case when T52W_GMV <=100000 then '$0-100k'
   			   when T52W_GMV <=250000 and T52W_GMV >100000 then '$100k-250k'
 	   		   when T52W_GMV >250000 then '>$250k' ELSE NULL END
  	   as t52_gmv_bucket
	   
FROM P_nishant_local_T.final_metrics_2


SELECT *
FROM P_nishant_local_T.final_metrics_2
LIMIT 500


