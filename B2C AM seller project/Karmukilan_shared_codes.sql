====UK====


DROP TABLE IF EXISTS UK_Slr_Sgmnt;

CREATE TEMPORARY TABLE UK_Slr_Sgmnt AS
SELECT 
    SLR_ID,
    CUST_SGMNTN_DESC,
    CUST_SLR_SGMNTN_BEG_DT,
    CUST_SLR_SGMNTN_END_DT,
    CUST_SGMNTN_GRP_CD
FROM (
    SELECT 
        HIST.SLR_ID,
        LKP.CUST_SGMNTN_DESC,
        HIST.CUST_SLR_SGMNTN_BEG_DT,
        HIST.CUST_SLR_SGMNTN_END_DT,
        HIST.CUST_SGMNTN_GRP_CD,
        ROW_NUMBER() OVER (PARTITION BY HIST.SLR_ID ORDER BY HIST.CUST_SLR_SGMNTN_BEG_DT DESC) AS rn
    FROM PRS_RESTRICTED_V.DNA_CUST_SELLER_SGMNTN_HIST HIST
    LEFT JOIN PRS_RESTRICTED_V.CUST_SGMNTN LKP ON HIST.CUST_SGMNTN_CD = LKP.CUST_SGMNTN_CD
) AS subquery
WHERE rn = 1 AND cust_sgmntn_grp_cd = 36 AND CURRENT_DATE BETWEEN CUST_SLR_SGMNTN_BEG_DT AND CUST_SLR_SGMNTN_END_DT;

DROP TABLE IF EXISTS temp_fv_al_2; 
CREATE TEMPORARY TABLE temp_fv_al_2 AS 
SELECT DISTINCT CK_TRANS_ID, LSTG_ID, CK_DATE 
    FROM ACCESS_VIEWS.FOCUSED_VERT_TXN fv 
    JOIN ACCESS_VIEWS.fcsd_ctgry_hrchy_lkp lkp 
    ON lkp.fc_name = fv.focused_vertical_lvl1 
    AND lkp.fc_cntry = fv.glbl_rprt_geo_name  
    WHERE ROADMAP_STS_CD = 1;

DROP TABLE IF EXISTS temp_fv_al; 
CREATE TEMPORARY TABLE temp_fv_al AS 
SELECT 
        temp_fv_al_2.*, 
        b.AGE_FOR_WEEK_ID, 
        b.CAL_DT, 
        b.WEEK_NUM_DESC,
        ROW_NUMBER() OVER (ORDER BY b.CAL_DT) AS rank
    FROM temp_fv_al_2
    LEFT JOIN ACCESS_VIEWS.DW_CAL_DT b 
    ON temp_fv_al_2.ck_date = b.CAL_DT
    WHERE b.AGE_FOR_WEEK_ID >= -53 AND b.AGE_FOR_WEEK_ID <= -1;
	
	DROP TABLE IF EXISTS ranked_weeks_UK;
CREATE TEMPORARY TABLE ranked_weeks_UK AS 
( 
SELECT 
        WEEK_NUM_DESC,
        MIN(rank) AS min_rank,
        MAX(rank) AS max_rank
    FROM temp_fv_al
    GROUP BY WEEK_NUM_DESC
);



DROP TABLE IF EXISTS quater;
CREATE TEMPORARY TABLE quater AS 
  SELECT 
        (SELECT WEEK_NUM_DESC FROM ranked_weeks_UK WHERE min_rank = (SELECT MIN(min_rank) FROM ranked_weeks_UK)) AS min_week_desc,
        (SELECT WEEK_NUM_DESC FROM ranked_weeks_UK WHERE max_rank = (SELECT MAX(max_rank) FROM ranked_weeks_UK)) AS max_week_desc
;

-- Drop and create abc_uk_1
DROP TABLE IF EXISTS abc_uk_1;
CREATE TEMPORARY TABLE abc_uk_1 AS 
SELECT
    CONCAT(qr.min_week_desc, ' to ', qr.max_week_desc) AS Year,
    ck.SLR_ID,
    ck.SLR_CNTRY_ID,
    usr.user_slctd_id AS seller_name,
    cat.BSNS_VRTCL_NAME AS vertical,
    cat.META_CATEG_ID,
    cat.META_CATEG_NAME,
    cat.CATEG_LVL2_ID,
    cat.CATEG_LVL2_NAME,
    cat.CATEG_LVL3_ID,
    cat.CATEG_LVL3_NAME,
    CASE 
        WHEN UPPER(c.cust_sgmntn_desc) IN ('MERCHANT', 'LARGE MERCHANT', 'ENTREPRENEUR') THEN 'B2C' 
        ELSE 'C2C' 
    END AS Slr_Type,
	ck.EU_B2C_C2C_FLAG,
    SUM(CK.gmv20_plan) AS GMV,
    SUM(CK.gmv20_sold_quantity) AS SI
FROM
    PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ck
LEFT JOIN
    ACCESS_VIEWS.DW_CAL_DT CAL ON ck.GMV_DT = CAL.CAL_DT
LEFT JOIN
    (SELECT DISTINCT user_id, user_slctd_id FROM dw_users) usr ON usr.user_id = ck.SLR_ID
LEFT JOIN
    ACCESS_VIEWS.DW_CATEGORY_GROUPINGS CAT ON CAT.LEAF_CATEG_ID = ck.LEAF_CATEG_ID AND CAT.site_id = ck.lstg_site_id
LEFT JOIN
    UK_Slr_Sgmnt c ON ck.slr_id = c.slr_id
CROSS JOIN
    quater qr
WHERE
    CAL.AGE_FOR_WEEK_ID >= -53 AND CAL.AGE_FOR_WEEK_ID <= -1 AND
    ck.SLR_CNTRY_ID = 3 AND
    CK.LSTG_SITE_ID NOT IN (223, -1, -2, -3) AND
    ck.CK_WACKO_YN = 'N' AND
    CK.ISCORE = 1
GROUP BY
    1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12,13;
drop table if exists t2;
create temp table t2
select a.* , cal.QTR_BEG_DT, cal.QTR_END_DT
from P_UKANALYTICS_T.DE_pt_optins a
	Join DW_CAL_DT cal on concat("Q",right(cal.QTR_ID,1))=A.cohort_qtr and cal.YEAR_ID=a.cohort_year AND CAL.AGE_FOR_QTR_ID IN (-4,-3,-2,-1);
	
drop table if exists UK_Optin_data;
create temp table UK_Optin_data
SELECT DISTINCT *
FROM P_UKANALYTICS_T.UK_Optins_Details
WHERE 
    (Year = EXTRACT(YEAR FROM CURRENT_DATE) AND Qtr IN (
        'Q' || CAST(EXTRACT(QUARTER FROM CURRENT_DATE) AS VARCHAR(2)), 
        'Q' || CAST(EXTRACT(QUARTER FROM CURRENT_DATE) - 1 AS VARCHAR(2)), 
        'Q' || CAST(EXTRACT(QUARTER FROM CURRENT_DATE) - 2 AS VARCHAR(2)), 
        'Q' || CAST(EXTRACT(QUARTER FROM CURRENT_DATE) - 3 AS VARCHAR(2))
    ))
    OR
    (Year = EXTRACT(YEAR FROM CURRENT_DATE) - 1 AND Qtr IN (
        'Q4', 
        'Q3', 
        'Q2', 
        'Q1'
    ))
ORDER BY Year DESC, Qtr DESC
limit 4;
	
	
drop table if exists t2;
create temp table t2
select a.* , cal.QTR_BEG_DT, cal.QTR_END_DT
from UK_Optin_data a
	Join DW_CAL_DT cal on concat("Q",right(cal.QTR_ID,1))=A.qtr and cal.YEAR_ID=a.year AND CAL.AGE_FOR_QTR_ID IN (-4,-3,-2,-1);

drop table if exists P_CSI_TBS_T.UK_PT_Slr_Dtl_for_CY;
create table P_CSI_TBS_T.UK_PT_Slr_Dtl_for_CY as --uk_mosh_seller_wise_data_no_rank as 
(select a.*, 
	case when b.slr_id is NULL then "No" else "Yes" end Ex_PT_Slr_Flag		
from abc_uk_1 a 
	Left Join t2 b on b.slr_id=a.slr_id
where a.EU_B2C_C2C_FLAG =  'B2C' );


drop table if exists p_L4_cat_Data; 
create temp table p_L4_cat_Data as 
SELECT A.*,
	B.vertical Primary_vertical, B.META_CATEG_ID Primary_META_CATEG_ID, B.META_CATEG_NAME Primary_META_CATEG_NAME, B.CATEG_LVL2_ID Primary_CATEG_LVL2_ID, B.CATEG_LVL2_NAME Primary_CATEG_LVL2_NAME,
	B.CATEG_LVL3_ID Primary_CATEG_LVL3_ID, B.CATEG_LVL3_NAME Primary_CATEG_LVL3_NAME, B.Primary_L3_GMV
FROM (select Year, SLR_ID, SLR_CNTRY_ID, seller_name, Ex_PT_Slr_Flag, sum(GMV) Total_GMV from P_CSI_TBS_T.UK_PT_Slr_Dtl_for_CY GROUP BY 1,2,3,4,5) A
	Left Join (select Year, SLR_ID, SLR_CNTRY_ID, seller_name, vertical, META_CATEG_ID, META_CATEG_NAME, CATEG_LVL2_ID, CATEG_LVL2_NAME, CATEG_LVL3_ID, CATEG_LVL3_NAME, --CATEG_LVL4_ID, CATEG_LVL4_NAME, 
						sum(GMV) Primary_L3_GMV
				from (select *, rank() over (partition by slr_id order by GMV desc) gmv_rnk from P_CSI_TBS_T.UK_PT_Slr_Dtl_for_CY) 
				where gmv_rnk=1
				group by 1,2,3,4,5,6,7,8,9,10,11) B on A.Year=B.Year AND A.SLR_ID=B.SLR_ID AND A.SLR_CNTRY_ID=B.SLR_CNTRY_ID;



-- Filter for 1.In Category Annual GMV of $50K and 2.Category has atleast 200 or more such sellers
 --fileds In Primary Category Annual GMV of $50K 
 --check number of sellers per primary category min is 200 per pri mary category
drop table if exists p_L4_cat_pt_filters; 
create temp table p_L4_cat_pt_filters as 
select * 
from (select Primary_CATEG_LVL3_ID, Primary_CATEG_LVL3_NAME, count(distinct slr_id) slr_cnt
		from p_L4_cat_Data 
		--where Total_GMV>=100000 and Total_GMV<500000 and Primary_L3_GMV>=40000 ---and Primary_L3_GMV>=50000
		group by 1,2); 
--where slr_cnt>=200;

drop table if exists Next_PT_Target_slrs_uk; 
create temp table Next_PT_Target_slrs_uk as 
select a.* 
from p_L4_cat_Data a 
	join p_L4_cat_pt_filters b on a.Primary_CATEG_LVL3_ID=b.Primary_CATEG_LVL3_ID;

--Find the latest Seller Satndard -
DROP TABLE IF EXISTS Latest_Slr_std_UK;
CREATE TEMP TABLE Latest_Slr_std_UK
SELECT 
    slr_id,
    CASE 
        WHEN SPS_SLR_LEVEL_CD = 1 THEN 'Top Rated'
        WHEN SPS_SLR_LEVEL_CD = 2 THEN 'Above Standard'
        WHEN SPS_SLR_LEVEL_CD = 4 THEN 'Below Standard'
        ELSE 'Not assigned'
    END AS sps_slr_lvl_desc
FROM (
    SELECT 
        sps.USER_ID AS slr_id,
        sps.SPS_SLR_LEVEL_CD,
		sps.SPS_SLR_LEVEL_SUM_START_DT,
		sps.SPS_SLR_LEVEL_SUM_END_DT,
		sps.SPS_SLR_LEVEL_CD,
		sps.SPS_PRGRM_ID,
        ROW_NUMBER() OVER (PARTITION BY sps.USER_ID ORDER BY sps.SPS_SLR_LEVEL_SUM_START_DT DESC) AS rn
    FROM PRS_RESTRICTED_V.SPS_LEVEL_METRIC_SUM sps
    JOIN (SELECT DISTINCT slr_id FROM Next_PT_Target_slrs_uk) a ON a.slr_id = sps.USER_ID 
    WHERE sps.SPS_PRGRM_ID = 3  -- UK
      AND CURRENT_DATE BETWEEN sps.SPS_SLR_LEVEL_SUM_START_DT AND sps.SPS_SLR_LEVEL_SUM_END_DT
) AS recent_levels
WHERE rn = 1;

DROP TABLE IF EXISTS Latest_Predicted_Slr_std_UK;
CREATE TEMP TABLE Latest_Predicted_Slr_std_UK
select slr_id,
	case when SPS_SLR_LEVEL_CD=1 then 'Top Rated' when SPS_SLR_LEVEL_CD=2 then 'Above Standard' when SPS_SLR_LEVEL_CD=4 then 'Below Standard' else 'Not assigned' end as sps_slr_lvl_desc
from (select distinct sps.USER_ID slr_id,sps.SPS_SLR_LEVEL_CD SPS_SLR_LEVEL_CD, LAST_EVAL_DT
		from PRS_RESTRICTED_V.sps_level sps
			join (select distinct slr_id from Next_PT_Target_slrs_uk) a on a.slr_id=sps.USER_ID 
		where sps.SPS_PRGRM_ID=3  --US 
		   and sps.SPS_EVAL_TYPE_CD=3 
		 Qualify ROW_NUMBER() over (PARTITION by slr_id order by LAST_EVAL_DT desc)=1 ); -- on the date of data pull
		  

--Find the Marketin_optin or not flag - Latest
DROP TABLE IF EXISTS Marketing_Opt_in_UK;
CREATE TEMP TABLE Marketing_Opt_in_UK
Select b.USER_ID Slr_Id, b.WANT_TELEMARKETING,  b.EMAIL_MAST_PREF_YN, case when b.EMAIL_MAST_PREF_YN = 'Y' AND b.WANT_TELEMARKETING = 'Y' then "Opt-in" else "Opt-out" end Marketing_Opt_in_UK_Flg 
from DW_MDM_USER_A b 
	join (select distinct slr_id from Next_PT_Target_slrs_uk) a on a.slr_id=b.user_id;


drop table if exists P_CSI_TBS_T.UK_PT_Target_Slr_CY; 
create table P_CSI_TBS_T.UK_PT_Target_Slr_CY as 
select a.*, b.sps_slr_lvl_desc Seller_Standard, c.sps_slr_lvl_desc Predicted_Seller_Standard, d.WANT_TELEMARKETING, d.EMAIL_MAST_PREF_YN, d.Marketing_Opt_in_UK_Flg
from Next_PT_Target_slrs_uk a
	left join Latest_Slr_std_UK b on a.slr_id=b.slr_id
	left join Latest_Predicted_Slr_std_UK c on a.slr_id=c.slr_id 
	left join Marketing_Opt_in_UK d on a.slr_id=d.slr_id;


drop table if exists P_CSI_TBS_T.UK_PT_Target_Slr_CY_fin; 
create table P_CSI_TBS_T.UK_PT_Target_Slr_CY_fin as 
select * from  P_CSI_TBS_T.UK_PT_Target_Slr_CY
where  Total_GMV > 100000;

drop table if exists P_CSI_TBS_T.UK_PT_Target_Slr_CY_r; 
create table P_CSI_TBS_T.UK_PT_Target_Slr_CY_r as 
SELECT a.*,
CASE
	WHEN total_gmv < 100000 THEN ">100k"
    WHEN total_gmv >= 100000 AND total_gmv <= 250000 THEN "100k - 250k"
    WHEN total_gmv >= 250000 AND total_gmv <= 500000 THEN "250k - 500k"
    WHEN total_gmv >= 500000 AND total_gmv <= 750000 THEN "500k - 750k"
    WHEN total_gmv >= 750000 AND total_gmv <= 1000000 THEN "750k - 1M"
    ELSE ">1M"
END AS GMV_Range 
FROM P_CSI_TBS_T.UK_PT_Target_Slr_CY_fin a;


DROP TABLE IF EXISTS DatUK;
CREATE TEMPORARY TABLE DatUK AS
SELECT distinct *
FROM P_CSI_TBS_T.UK_PT_Target_Slr_CY_r;


DROP TABLE IF EXISTS UK_Data;
CREATE TEMPORARY TABLE UK_Data AS
SELECT 
    retail_year, 
    retail_week, 
    RETAIL_WK_END_DATE, 
    SLR_TYPE, slr_id,
    SUM(T52W_GMV) AS Total_GMV 
FROM 
   P_CSI_TBS_T.all_T52W_GMV
-- Uncomment and adjust the WHERE clause as needed
-- WHERE retail_year = 2024 AND retail_week = 39 
GROUP BY 
    retail_year, 
    retail_week, 
    RETAIL_WK_END_DATE, 
    SLR_TYPE,slr_id
HAVING 
    SUM(T52W_GMV) >= 100000;
	
	

	
	--select slr_type, count(distinct SLR_ID) from DE_Data
	--group by 1
	

DROP TABLE IF EXISTS UK_RangeData;
CREATE TEMPORARY TABLE UK_RangeData AS
select a.*,b.AGE_FOR_WEEK_ID from UK_Data a
inner join DW_CAL_DT b
on a.RETAIL_WK_END_DATE = b.RETAIL_WK_END_DATE;
--where b.AGE_FOR_WEEK_ID = -1;
-- select slr_id,sum(Total_GMV) Total_GMV from DE_RangeData
-- group by 1
-- order by slr_id



DROP TABLE IF EXISTS UK_ReqData;
CREATE TEMPORARY TABLE UK_ReqData AS
select distinct slr_id,slr_type, Total_GMV from UK_RangeData
where AGE_FOR_WEEK_ID = -1
--group by 1,2
order by slr_id;





--select * FROM DE_ReqData
--where slr_id = '72300'

DROP TABLE IF EXISTS UK_FinalData;
CREATE TEMPORARY TABLE UK_FinalData AS
select a.slr_id as slr_id_de, a.total_gmv as Total_GMV_DE,a.slr_type,b.* from UK_ReqData a
inner join DatUK b
on a.slr_id = b.slr_id;


-- drop table if exists P_CSI_TBS_T.UK_PT_Target_Slr_CY_AMUK; 
-- create table P_CSI_TBS_T.UK_PT_Target_Slr_CY_AMUK as 
-- SELECT a.*,   case when b.slr_id is NULL then "No" else "Yes" end AM_Flag, case when b.slr_id is NULL then "Not Assigned" else b.year end Year_flag, sort_array(collect_set(b.year) OVER (PARTITION BY b.slr_id,b.REGION))) AS years_for_slr_id from UK_FinalData a
-- left join P_UKANALYTICS_T.AM_Details b 
-- on a.slr_id_de = b.slr_id;

drop table if exists P_CSI_TBS_T.DE_PT_Target_Slr_CY_AMUK; 
create table P_CSI_TBS_T.DE_PT_Target_Slr_CY_AMUK as 
SELECT a.*,   case when b.slr_id is NULL then "No" else "Yes" end AM_Flag, case when b.slr_id is NULL then "Not Assigned" else b.year end Year_flag,concat_ws(', ', sort_array(collect_set(b.year) OVER (PARTITION BY b.slr_id,b.REGION))) AS years_for_slr_id   from UK_FinalData a
left join P_UKANALYTICS_T.AM_Details_UK b 
on a.slr_id_de = b.slr_id;

-- select * from P_CSI_TBS_T.DE_PT_Target_Slr_CY_AMUK
-- limit 10
-- select DISTINCT slr_ID_de,Total_GMV_De,slr_type,Year,Slr_ID,Slr_cntry_id,Seller_name,Ex_PT_Slr_Flag,Total_GMV Primary_CATEG_LVL2_ID,Primary_CATEG_LVL2_NAME,Primary_CATEG_LVL3_ID,Primary_CATEG_LVL3_NAME,Primary_L3_GMV,Seller_Standard,Predicted_Seller_Standard,WANT_TELEMARKETING,EMAIL_MAST_PREF_YN,Marketing_Opt_in_UK_Flg,GMV_Range,
-- AM_Flag,years_for_slr_id,CASE when IS NULL then "Not Assigned" else years_for_slr_id end AMDETAILS
-- from P_CSI_TBS_T.DE_PT_Target_Slr_CY_AMUK
-- order by slr_ID_de

DROP TABLE IF EXISTS DE_PT_Target_Slr_CY_AMUK1;
CREATE TEMPORARY TABLE DE_PT_Target_Slr_CY_AMUK1 AS
SELECT DISTINCT 
 slr_id_de,Total_GMV_DE,slr_type,Year,SLR_ID,SLR_CNTRY_ID,seller_name,Ex_PT_Slr_Flag,Total_GMV,Primary_vertical,
Primary_META_CATEG_ID,Primary_META_CATEG_NAME,Primary_CATEG_LVL2_ID,Primary_CATEG_LVL2_NAME,Primary_CATEG_LVL3_ID,
Primary_CATEG_LVL3_NAME,Primary_L3_GMV,Seller_Standard,Predicted_Seller_Standard,WANT_TELEMARKETING,EMAIL_MAST_PREF_YN,
Marketing_Opt_in_UK_Flg,GMV_Range,AM_Flag,years_for_slr_id,
CASE 
        WHEN am_fLAG = "No" THEN "Not Assigned" 
        ELSE years_for_slr_id 
    END AS AMDETAILS
FROM 
    P_CSI_TBS_T.DE_PT_Target_Slr_CY_AMUK
ORDER BY 
    slr_ID_de;

-- select * from DE_PT_Target_Slr_CY_AMUK1
-- --where AM_flag='Yes'
-- order by slr_id_de




drop table if exists P_CSI_TBS_T.UK_DataRangerr; 
create table P_CSI_TBS_T.UK_DataRangerr as 
SELECT a.*,
CASE
	WHEN Total_GMV_DE < 100000 THEN "<100k"
    WHEN Total_GMV_DE >= 100000 AND Total_GMV_DE <= 250000 THEN "100k - 250k"
    WHEN Total_GMV_DE >= 250000 AND Total_GMV_DE <= 500000 THEN "250k - 500k"
    WHEN Total_GMV_DE >= 500000 AND Total_GMV_DE <= 750000 THEN "500k - 750k"
    WHEN Total_GMV_DE >= 750000 AND Total_GMV_DE <= 1000000 THEN "750k - 1M"
    ELSE ">1M"
END AS GMV_RangeDe 
FROM DE_PT_Target_Slr_CY_AMUK1 a;

--  select * from P_CSI_TBS_T.UK_DataRangerr
--  order by slr_id
-- -- -- where AM_flag='Yes'
-- -- order by slr_id_de

-- select slr_type, count(distinct slr_id_de) from P_CSI_TBS_T.UK_DataRangerr
-- -- group by 1

-- select * from P_CSI_TBS_T.UK_DataRangerr



-- select * from P_CSI_TBS_T.UK_PT_Target_Slr_CY_r

reference




M, Karmukilan(AWF)
​
Supakar, Nishant
​
Hi Nishant,

Check the attached file.