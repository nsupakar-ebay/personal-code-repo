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






