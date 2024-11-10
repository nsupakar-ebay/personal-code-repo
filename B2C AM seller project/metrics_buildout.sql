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

--- T1W LL
--- RUN & READY
DROP TABLE IF EXISTS P_nishant_local_T.LL_T1_METRICS; 
create table P_nishant_local_T.LL_T1_METRICS as 
SELECT
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  cal.AGE_FOR_RTL_WEEK_ID,
  I.SLR_ID,
  COUNT(1) AS T1W_LL
FROM
  ACCESS_VIEWS.DW_CAL_DT CAL
  left JOIN ACCESS_VIEWS.DW_LSTG_ITEM I   ON
  CAL.CAL_DT >= I.AUCT_START_DT
  AND CAL.CAL_DT <= I.AUCT_END_DT
   INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
   AND CAT.SITE_ID = I.ITEM_SITE_ID
   AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  where 
  I.wacko_yn = 'N'
  and CAL.AGE_FOR_DT_ID <= -1
  and i.LSTG_STATUS_ID = 0
  and CAL.RETAIL_YEAR >= 2021
  group by 1,2,3,4
  ORDER BY 2 asc, 1 ASC, 3;  
  

--T52W LL
--- RUN & READY
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
CREATE TABLE P_nishant_local_T.STORE_KPI_METRICS AS
SELECT CAL.RETAIL_YEAR,
	   CAL.RETAIL_WEEK,
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
GROUP BY 1,2,3
ORDER BY 3,1,2;



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

---T1W FVF
--- RUN & READY
DROP TABLE IF EXISTS P_nishant_local_T.T1W_FVF_METRICS;
CREATE TABLE P_nishant_local_T.T1W_FVF_METRICS AS
select CAL.RETAIL_YEAR, CAL.RETAIL_WEEK, CAL.AGE_FOR_RTL_WEEK_ID,
		REV.SLR_ID,
		sum(REV.AMT_USD) as T1W_FVF_USD_AMT	
from ACCESS_VIEWS.DW_CAL_DT CAL
LEFT JOIN ACCESS_VIEWS.DW_GEM2_CMN_RVNU_I REV
ON CAL.CAL_DT = REV.ACCT_TRANS_DT 
WHERE CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2021
AND CAL.AGE_FOR_DT_ID <= -1
AND REV.SLR_CNTRY_ID = 3
and REV.actn_code in (select ACTN_CODE
					from dw_action_codes
					where UPPER(ACTN_CODE_TYPE_DESC1) = 'FINAL VALUE FEE'
					GROUP BY 1)
GROUP BY 1,2,3, 4;


---T52W FVF
--- RUN & READY
CREATE TABLE P_nishant_local_T.T52W_FVF_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
sum(b.T1W_FVF_USD_AMT) as T52W_FVF_USD_AMT
from P_nishant_local_T.T1W_FVF_METRICS a
left join P_nishant_local_T.T1W_FVF_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;

---CSS segmentationADD
---- not run
select 		cal.retail_year,
			cal.retail_week,
			u.user_id AS SLR_ID,
			case WHEN cs.cust_sgmntn_cd IN (1, 7,13,19,25,31) THEN 'Large merchant'
            WHEN cs.cust_sgmntn_cd IN (2, 8,14,20,26,32) THEN 'Merchants'
            WHEN cs.cust_sgmntn_cd IN (3, 9,15,21,27,33) THEN 'Entrepreneur'
            WHEN cs.cust_sgmntn_cd IN (4,10,16,22,28,34) THEN 'Regulars'
            WHEN cs.cust_sgmntn_cd IN (5,11,17,23,29,35) THEN 'Occasional'
            WHEN cs.cust_sgmntn_cd IN (6,12,18,24,30,36) THEN 'Lapsed'
       ELSE 'Never' END AS LATEST_CSS_segment                   -- Nevers are buyers
from ACCESS_VIEWS.DW_CAL_DT CAL 
left join ACCESS_VIEWS.DW_USERS U     
left join prs_restricted_v.DNA_CUST_SELLER_SGMNTN_HIST cs
    ON  u.user_id = cs.slr_id
    AND cs.cust_sgmntn_grp_cd BETWEEN 36 AND 41            -- only Customer Seller Segmentation (CSS) data
    AND current_date BETWEEN cs.cust_slr_sgmntn_beg_dt and cs.cust_slr_sgmntn_end_dt  -- change the current date 
where U.user_cntry_id =3                               -- seller domicility || 3-UK, 77-DE, 71-FR, IT-101, ES-186 rest look up in wiki
and U.user_dsgntn_id = 2                                  -- BUSINESS SELLER, NEED TO CONFIRM
group by 1,2;



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
--- not joined on previous table
--- check with KM & check with , might have to change to quarterly check with finance team
--- feature fees, store fee
dROP TABLE IF EXISTS P_nishant_local_T.T1W_FEE;
CREATE TABLE P_nishant_local_T.T1W_FEE AS 
SELECT
CAL.RETAIL_WEEK,
CAL.RETAIL_YEAR,
CAL.AGE_FOR_RTL_WEEK_ID
,a.SLR_ID
,SUM(CASE WHEN a.actn_code IN (504,505) THEN -1*a.AMT_BC END) as T1W_Variable_FVF
,SUM(CASE WHEN a.actn_code IN (508,509) THEN -1*a.AMT_BC END) as T1W_Fixed_FVF
,SUM(CASE WHEN a.actn_code IN (1,24) THEN -1*a.AMT_BC END) as T1W_Insertion_Fee
,SUM(CASE WHEN a.actn_code IN (130, 131, 132, 133, 139, 140, 162, 163, 164, 165) THEN -1*a.AMT_BC END) as T1W_Subscription_Fee
,SUM(CASE WHEN a.actn_code IN (409,410,474,475,526,527) THEN -1*a.AMT_BC END) as T1W_PL_Fee
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
cal.RETAIL_WEEK >= 1
and cal.RETAIL_YEAR >= 2021
AND a.slr_cntry_id = 3
AND b.USEGM_ID=206 -- B2C 
AND cat.sap_category_id NOT IN (23,5,7,41)
GROUP BY 1, 2, 3, 4; 


----- Fee table T52W
--- not joined on FINAL table
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

SELECT *
FROM P_nishant_local_T.T52W_FEE
WHERE T52W_SUBSCRIPTION_FEE >0
LIMIT 100

---- AVERAGE START PRICE & LIVE LISTING
--- NEED TO ADD TO FINAL TABLE AGAIN
DROP TABLE IF EXISTS P_nishant_local_T.ASP_T1_METRICS; 
create table P_nishant_local_T.ASP_T1_METRICS as 
SELECT
  CAL.RETAIL_WEEK,
  CAL.RETAIL_YEAR,
  cal.AGE_FOR_RTL_WEEK_ID,
  I.SLR_ID,
  COUNT(1) AS T1W_LL,
  SUM(I.START_PRICE_USD) AS T1W_SUM_LL
FROM
  ACCESS_VIEWS.DW_CAL_DT CAL
  left JOIN ACCESS_VIEWS.DW_LSTG_ITEM I   ON
  CAL.CAL_DT >= I.AUCT_START_DT
  AND CAL.CAL_DT <= I.AUCT_END_DT
   INNER JOIN ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = I.LEAF_CATEG_ID
   AND CAT.SITE_ID = I.ITEM_SITE_ID
   AND cat.SAP_CATEGORY_ID NOT IN (5, 7, 41, 23, -999) --CORE ONLY
  where 
  I.wacko_yn = 'N'
  and CAL.AGE_FOR_DT_ID <= -1
  and i.LSTG_STATUS_ID = 0
  and CAL.RETAIL_YEAR >= 2021
  group by 1,2,3,4
  ORDER BY 2 asc, 1 ASC, 3;  

---T1W PL METRICS
--- HAVE NOT ADDED TO MAIN TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T1W_PL_METRICS; 
create table P_nishant_local_T.T1W_PL_METRICS
SELECT
RETAIL_WEEK,
RETAIL_YEAR,
cal.AGE_FOR_RTL_WEEK_ID,
SLR_ID,
Sum(CASE WHEN GEM.ACTN_CODE in (409,410) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT ) ELSE 0 END ) T1W_PLS_NET,
Sum(CASE WHEN GEM.ACTN_CODE = 409 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PLS_GROSS,
Sum(CASE WHEN GEM.ACTN_CODE in (474,475) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END ) T1W_PLA_NET,
Sum(CASE WHEN GEM.ACTN_CODE = 474 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PLA_GROSS,
Sum(CASE WHEN GEM.ACTN_CODE in (564,565) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END ) T1W_SFA_NET,
Sum(CASE WHEN GEM.ACTN_CODE = 564 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_SFA_GROSS,
Sum(CASE WHEN GEM.ACTN_CODE in (526,527) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END ) T1W_PLX_NET,
Sum(CASE WHEN GEM.ACTN_CODE = 526 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PLX_GROSS,
Sum(CASE WHEN GEM.ACTN_CODE in (568,569) THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PD_NET,
Sum(CASE WHEN GEM.ACTN_CODE = 568 THEN Cast (TRXN_CURNCY_AMT * BPR.CURNCY_PLAN_RATE AS FLOAT) ELSE 0 END) AS T1W_PD_GROSS
FROM (SELECT ACCT_TRANS_DT, SLR_ID, ACTN_CODE, LEAF_CATEG_ID, LSTG_SITE_ID, SLR_CNTRY_ID, BYR_CNTRY_ID, TRXN_CURNCY_CD
           ,Sum(Cast ( -1* TRXN_CURNCY_AMT AS FLOAT)) TRXN_CURNCY_AMT, Sum(Cast(-1 * AMT_USD AS FLOAT )) AMT_USD, Sum(Cast(-1 * AMT_M_USD AS FLOAT)) AMT_M_USD
FROM ACCESS_VIEWS.DW_GEM2_CMN_RVNU_I RVNU
WHERE ACCT_TRANS_DT between '2019-01-01' and CURRENT_DATE - 1
AND LSTG_TYPE_CODE NOT IN (10,15)
AND ADJ_TYPE_ID NOT IN (-1,-7,5)
AND LSTG_SITE_ID <> 223
AND ACCT_TRANS_ID <> 3178044411711
AND ACTN_CODE IN (409,410,474,475,526,527,564,565,568,569)
GROUP BY 1,2,3,4,5,6,7,8
        UNION ALL
        /* WASH DATA OF BAD TRANSACTIONS */
SELECT ACCT_TRANS_DT, SLR_ID, ACTN_CODE, LEAF_CATEG_ID, LSTG_SITE_ID, SLR_CNTRY_ID, BYR_CNTRY_ID, TRXN_CURNCY_CD
           ,Sum(Cast ( TRXN_CURNCY_AMT AS FLOAT)) TRXN_CURNCY_AMT, Sum( Cast( AMT_USD AS FLOAT ) ) AMT_USD, Sum( Cast(AMT_M_USD AS FLOAT ) ) AMT_M_USD
FROM ACCESS_VIEWS.DW_GEM2_CMN_ADJ_RVNU_I
WHERE ACCT_TRANS_DT between '2019-01-01' and CURRENT_DATE - 1
AND LSTG_TYPE_CODE NOT IN(10,15)
AND LSTG_SITE_ID <> 223
AND ISWACKO_YN_ID = 1
AND ACCT_TRANS_ID<>3178044411711
AND ACTN_CODE IN (409,410,474,475,526,527,564,565,568,569)
GROUP BY 1,2,3,4,5,6,7,8
    )GEM
    INNER JOIN ACCESS_VIEWS.SSA_CURNCY_PLAN_RATE_DIM AS BPR  ON GEM. TRXN_CURNCY_CD = BPR. CURNCY_ID 
    INNER JOIN ACCESS_VIEWS.DW_ACCT_ACTN_CODE_LKP AS B ON GEM.ACTN_CODE = B.ACTN_CODE AND B.REV_BKT_ID BETWEEN 29 AND 36
    INNER JOIN ACCESS_VIEWS.DW_ACCT_LSTG_REV_BKT_LKP AS R ON R.REV_BKT_ID = B.REV_BKT_ID AND Upper(R.REV_GRP_CODE) = 'GEM'
    INNER JOIN ACCESS_VIEWS.DW_ACTION_CODES AS ACTN_CODE ON ACTN_CODE.ACTN_CODE = GEM.ACTN_CODE
    INNER JOIN ACCESS_VIEWS.DW_CAL_DT CAL ON GEM.ACCT_TRANS_DT = CAL.CAL_DT
	where 
	gem.slr_cntry_id = 3
	and cal.RETAIL_WEEK >=1
	and cal.RETAIL_YEAR >=2021
    GROUP BY 1,2,3,4;

---T52W PL METRICS
--- HAVE NOT ADDED TO MAIN TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T52W_PL_METRICS;
CREATE TABLE P_nishant_local_T.T52W_PL_METRICS AS
select a.SLR_ID, a.retail_week, a.retail_year, a.AGE_FOR_RTL_WEEK_ID,
Sum(B.T1W_PLS_NET) AS T52W_PLS_NET,
Sum(B.T1W_PLS_GROSS) AS T52W_PLS_GROSS,
Sum(B.T1W_PLA_NET) AS T52W_PLA_NET,
Sum(B.T1W_PLA_GROSS) AS T52W_PLA_GROSS,
Sum(B.T1W_SFA_NET) AS T52W_SFA_NET,
Sum(B.T1W_SFA_GROSS) AS T52W_SFA_GROSS,
Sum(B.T1W_PLX_NET) AS T52W_PLX_NET,
Sum(B.T1W_PLX_GROSS) AS T52W_PLX_GROSS,
Sum(B.T1W_PD_NET) AS T52W_PD_NET,
Sum(B.T1W_PD_GROSS) AS T52W_PD_GROSS
from P_nishant_local_T.T1W_PL_METRICS a
left join P_nishant_local_T.T1W_PL_METRICS b
on a.slr_id = b.slr_id
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) >=0
and (a.AGE_FOR_RTL_WEEK_ID -b.AGE_FOR_RTL_WEEK_ID) <52
where a.retail_year >= 2022
GROUP BY 1,2,3,4
ORDER BY 1,3,2;


SELECT *
FROM P_nishant_local_T.T52W_PL_METRICS
LIMIT 100

--- LISTING ATTRIBUTES
--- NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T52W_LISTING_METRICS;
CREATE TABLE P_nishant_local_T.T52W_LISTING_METRICS AS
SELECT  CAL.RETAIL_WEEK,
	    CAL.RETAIL_YEAR,
		FCT.SLR_ID,
	    AVG(LENGTH(FCT.AUCT_TITLE)) AS T1W_AVG_TITLE_LENGTH,
		AVG(LENGTH(FCT.SUBTITLE)) AS T1W_AVG_SUBTITLE_LENGTH,
		AVG(FCT.PHT_CNT) AS T1W_PHOTO_COUNT
FROM ACCESS_VIEWS.DW_CAL_DT CAL 
LEFT JOIN PRS_RESTRICTED_V.SLNG_LSTG_SUPER_FACT FCT
ON CAL.RETAIL_WK_END_DATE >= FCT.AUCT_START_DT
AND CAL.RETAIL_WK_END_DATE <= FCT.AUCT_END_DT	
WHERE FCT.SLR_CNTRY_ID =3
AND CAL.RETAIL_WEEK >=1
AND CAL.RETAIL_YEAR >=2021
AND FCT.ISCORE = 1
GROUP BY 1,2,3;


SELECT *
FROM P_nishant_local_T.T52W_LISTING_METRICS
LIMIT 100



---PL revenue penetration

---OUTGOING M2M COUNT
--- NOT ADDED TO FINAL TABLE
DROP TABLE IF EXISTS P_nishant_local_T.T1W_OUTB_METRICS;
CREATE TABLE P_nishant_local_T.T1W_OUTB_METRICS AS
SELECT CAL.RETAIL_WEEK,
	   CAL.RETAIL_YEAR,
	   CAL.RETAIL_WK_END_DATE,
	   EM.SNDR_ID AS ID,
	   COUNT(DISTINCT EM.EMAIL_TRACKING_ID) AS T1W_OUT_M2M_CNT
from ACCESS_VIEWS.DW_CAL_DT CAL 
left join prs_secure_v.dw_ue_email_tracking em
on cal.cal_dt = em.SRC_CRE_DT
WHERE RETAIL_WEEK>=1
AND RETAIL_YEAR >=2021
GROUP BY 1,2,3,4;

---INCOMING M2M COUNT
--- NOT ADDED TO FINAL TABLE
---NOT RUN
DROP TABLE IF EXISTS P_nishant_local_T.T1W_INB_METRICS;
CREATE TABLE P_nishant_local_T.T1W_INB_METRICS AS
SELECT CAL.RETAIL_WEEK,
	   CAL.RETAIL_YEAR,
	   CAL.RETAIL_WK_END_DATE,
	   EM.RCPNT_ID AS ID,
	   COUNT(DISTINCT EM.EMAIL_TRACKING_ID) AS T1W_INB_M2M_CNT
from ACCESS_VIEWS.DW_CAL_DT CAL 
left join prs_secure_v.dw_ue_email_tracking em
on cal.cal_dt = em.SRC_CRE_DT
WHERE RETAIL_WEEK>=1
AND RETAIL_YEAR >=2021
GROUP BY 1,2,3,4;


select *
from P_nishant_local_T.T1W_INB_METRICS 
limit 100

--- GSP GMV
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
--- build out table
select cal.RETAIL_YEAR, CAL.RETAIL_WEEK,CAL.AGE_FOR_RTL_WEEK_ID, rle.SLR_ID,   
sum(rle.LATE_DLVRY_FLAG) AS T1W_LD_count
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
order by 3, 1, 2
limit 100;


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

----NOT run or added to final table
----- PL T1W METRICS
CREATE TABLE P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS AS
select cal.RETAIL_YEAR
     	,cal.RETAIL_WEEK,
		cal.AGE_FOR_RTL_WEEK_ID
	, pl.slr_id
	, round(sum(pl.PLS_NET+pl.PLA_NET+pl.PLX_NET),2) as T1W_PL_Net_Revenue
	, sum(pl.PLS_NET+pl.PLA_NET+pl.PLX_NET) PL_net_rev_pene_pct_num
	, sum(pl.gmv) PL_net_rev_pene_pct_deno
	, round(sum(pl.PLS_NET+pl.PLA_NET+pl.PLX_NET)*100/sum(pl.gmv),2) as T1W_PL_net_revenue_penetration_percnt
	, round(sum(pl.PLS_Enabled_GMV),2) PLS_enabled_GMV_pene_pct_num, round(sum(pl.PLS_ELIG_GMV_USD),2) PLS_enabled_GMV_pene_pct_deno
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
group by 1,2,3,4

SELECT *
FROM P_NISHANT_LOCAL_T.T1W_PL_PEN_METRICS
LIMIT 100

----- T1W_VIEW COUNTS
--- RUN BUT NOT ADDED TO FINAL TABLE
CREATE TABLE P_NISHANT_LOCAL_T.T1W_VIEW_CNT AS
select cal.RETAIL_WEEK, cal.RETAIL_YEAR, cal.AGE_FOR_RTL_WEEK_ID,
VI.USER_ID as slr_id, 
SUM(VI_CNT) AS T1W_VIEW_COUNT
from  ACCESS_VIEWS.DW_CAL_DT CAL
left join ACCESS_VIEWS.USER_BRWS_SRCH_SD VI
on VI.CAL_DT = cal.CAL_DT
and cal.RETAIL_WEEK >= 1
AND CAL.RETAIL_YEAR >=2021
where 
cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2021
AND CAL.AGE_FOR_WEEK_ID <= -1 
and VI.USER_ID in (
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
GROUP BY 1,2,3,4
order by USER_ID,  RETAIL_YEAR, RETAIL_WEEK


select *
from P_NISHANT_LOCAL_T.T1W_VIEW_CNT
limit 100

----PLS ad rate
---not run
select
cal.RETAIL_WEEK,
cal.RETAIL_YEAR,
cal.AGE_FOR_RTL_WEEK_ID
,slr_id 
,avg(LSTG_BID_PCT)/100 as pls_adrate
,avg(SOLD_BID_PCT)/100 as pls_sold_adrate
from ACCESS_VIEWS.DW_CAL_DT CAL
left join access_views.pl_item_mtrc_sd perf_metric
on perf_metric.CAL_DT = cal.CAL_DT
and cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2022
where cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2022
and SLR_ID in (
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

group by 1,2,3,4
limit 100;  

-----sfa ad rate, gmv, si
----- need to add transaction date
--- not run 
SELECT
	cal.RETAIL_WEEK,
	cal.RETAIL_YEAR,
	cal.AGE_FOR_RTL_WEEK_ID,
    k.seller_id,
    SUM(CASE WHEN k.event_type_txt = 'SFAS' THEN k.item_sold_qty ELSE 0 END) AS sfa_si,    
    SUM(CASE WHEN k.event_type_txt = 'SFAS' THEN k.sale_slr_blng_curncy_amt ELSE 0 END) AS sfa_gmv,   
    SUM(CASE WHEN k.event_type_txt = 'SFACPC' THEN k.ad_fee_slr_blng_curncy_amt ELSE 0 END) AS sfa_ad_fee
FROM ACCESS_VIEWS.DW_CAL_DT CAL
left join ACCESS_VIEWS.PL_ORG_ADS_SALES_FACT k
on k.CAL_DT = cal.CAL_DT
and cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2022
where cal.RETAIL_WEEK >=1
and cal.RETAIL_YEAR >=2022
AND k.event_type_txt IN ('SFAS', 'SFACPC')
GROUP BY 1,2,3,4
limit 100;


