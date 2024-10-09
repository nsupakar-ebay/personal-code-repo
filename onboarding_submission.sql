---- Fees charged by eBay 
select 
	distinct fee_code,
    FEE_TYPE
from ACCESS_VIEWS.DW_EAX_ALL_FEE_DTL
where FEE_CODE is not NULL
limit 10

--- Average selling price
SELECT SUM(GMV_PLAN_USD)/SUM(CORE_ITEM_CNT) AS PLAN_ASP /* ASP, using yearly exchange rate for USD */
FROM ACCESS_VIEWS.DW_CHECKOUT_TRANS
WHERE GMV_DT BETWEEN '2022-12-01' AND '2022-12-31'
	  and SLR_CNTRY_ID is not null

---- Average buying price
SELECT SUM(GMV_PLAN_USD)/SUM(CORE_ITEM_CNT) AS PLAN_ABP /* ABP, using yearly exchange rate for USD */
FROM ACCESS_VIEWS.DW_CHECKOUT_TRANS
WHERE GMV_DT BETWEEN '2022-12-01' AND '2022-12-31'
	  and BYR_CNTRY_ID is not null

---10 Indian sellers who sold more than $300 worth of items in Jewelry vertical whose username end with 'US' in the past 3 months
SELECT SELLER_ID,USR.USER_NAME,
	   SUM(FVT.GMV2_PLAN)
FROM ACCESS_VIEWS.FOCUSED_VERT_TXN FVT	
LEFT JOIN PRS_SECURE_V.DW_USERS USR ON FVT.SELLER_ID = USR.USER_ID	
WHERE FVT.FOCUSED_VERTICAL_LVL1 = 'Jewelry >=$300' AND	
	GMV_DT >= DATEADD(MONTH, -3, CURRENT_TIMESTAMP)
	AND  USR.USER_NAME like"%us"	
	and USER_CNTRY_ID = 95
	group by 1,2
	Having SUM(FVT.GMV2_PLAN)>300 ;

---top 10 best performing shipping service in US by GMV and Transactions
WITH RankedShippingMethods AS (	
    SELECT	sh.SHPMT_MTHD,	
        	SUM(tr.gmv_plan_usd) AS GMV_USD,	
        	COUNT(DISTINCT tr.TRANSACTION_ID) AS TRANSACTION_COUNT,	
        	DENSE_RANK() OVER (	
            ORDER BY	
                SUM(tr.gmv_plan_usd) DESC,	
                COUNT(DISTINCT tr.TRANSACTION_ID) DESC	
        ) AS rank	
    FROM	
        ACCESS_VIEWS.DW_CHECKOUT_TRANS tr	
    INNER JOIN	
        ACCESS_VIEWS.DW_SHPMT_MTHD_LKP sh	
        ON tr.shpmt_mthd_id = sh.SHPMT_MTHD_ID	
    WHERE	
        tr.rprtd_wacko_yn = 'N'	
    GROUP BY	
        sh.SHPMT_MTHD	
)	
SELECT	
    SHPMT_MTHD,	
    GMV_USD,	
    TRANSACTION_COUNT	
FROM	
    RankedShippingMethods	
WHERE	
    rank <= 10	
ORDER BY	
    GMV_USD DESC,	
    TRANSACTION_COUNT DESC;	
	
	
---  most used shipping service by each seller segment 	
WITH SegmentShippingUsage AS (
    SELECT ag.SLR_ID AS Slr_Id,
        CASE
            -- B2C segments
            WHEN css_seg.cust_sgmntn_cd IN (1, 7, 13, 19, 25, 31) THEN 'Large merchant'
            WHEN css_seg.cust_sgmntn_cd IN (2, 8, 14, 20, 26, 32) THEN 'Merchants'
            WHEN css_seg.cust_sgmntn_cd IN (3, 9, 15, 21, 27, 33) THEN 'Entrepreneur'
            -- C2C segments
            WHEN css_seg.cust_sgmntn_cd IN (4, 10, 16, 22, 28, 34) THEN 'Regulars'
            WHEN css_seg.cust_sgmntn_cd IN (5, 11, 17, 23, 29, 35) THEN 'Occasional'
            WHEN css_seg.cust_sgmntn_cd IN (6, 12, 18, 24, 30, 36) THEN 'Lapsed'
            ELSE 'NA'
        END AS slr_segm_css,
        sh.SHPMT_MTHD,
        COUNT(DISTINCT ag.CK_TRANS_ID) AS trans_count
    FROM
        PRS_RESTRICTED_V.SLNG_TRANS_SUPER_FACT ag
    LEFT JOIN PRS_RESTRICTED_V.DNA_CUST_SELLER_SGMNTN_HIST css_seg
        ON ag.SLR_ID = css_seg.slr_id
        AND css_seg.cust_sgmntn_grp_cd BETWEEN 36 AND 41
        AND CAST(ag.GMV_DT AS DATE) BETWEEN css_seg.CUST_SLR_SGMNTN_BEG_DT AND css_seg.CUST_SLR_SGMNTN_END_DT
    LEFT JOIN DW_SHPMT_MTHD_LKP sh
        ON ag.SHPMT_MTHD_ID = sh.SHPMT_MTHD_ID
    GROUP BY
        ag.SLR_ID, slr_segm_css, sh.SHPMT_MTHD
),
RankedShippingUsage AS (
    SELECT
        Slr_Id,
        slr_segm_css,
        SHPMT_MTHD,
        trans_count,
        RANK() OVER (PARTITION BY slr_segm_css ORDER BY trans_count DESC) AS rank
    FROM
        SegmentShippingUsage
)
SELECT
    Slr_Id,
    slr_segm_css,
    SHPMT_MTHD AS Most_Used_Shipping_Method,
    trans_count AS Usage_Count
FROM
    RankedShippingUsage
WHERE
    rank = 1
ORDER BY
    slr_segm_css;