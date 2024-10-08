--- GMV code

SELECT SELLER_ID,
	   year(GMV_DT) as yr,
	   month(GMV_DT) as mon,
	   SLR_CNTRY_ID,
  	   SUM(GMV_PLAN_USD) AS GMV2_PLAN_RATE_USD /* This is used in almost all internal metrics as the main GMV definition, using the one-a-year fixed plan rate for conversion. */
  		 
FROM 
ACCESS_VIEWS.DW_CHECKOUT_TRANS
group by 1,2,3,4
order by 2 asc, 3 asc