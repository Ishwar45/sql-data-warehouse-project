/*
********************************************************************************
--Re-run the quality check queries for silver layer to detect duplicates, unwanted spaces & invalid data
********************************************************************************
*/


/*
**********************************************
Quality check for silver.crm_cust_info
**********************************************
*/
--Check For NULLs and Duplicates in Primary Key
--Expectation: No Results
SELECT cst_id, count(*) from silver.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces 
--Expectation: No results
SELECT cst_key
FROM silver.crm_cust_info
where cst_key != TRIM(cst_key);

SELECT cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname 
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

/*
**********************************************
Quality check for silver.crm_cust_info
**********************************************
*/

select prd_id, count(*)
from silver.crm_prd_info
group by prd_id
having count(*) > 1 OR prd_id IS NULL;

--Checking if start date is less than end date
select *, LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt) - 1 as prd_end_dt_test
from silver.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R', 'AC-HE-HL-U509');

--checking if cost is Zero or negative
select *
from silver.crm_prd_info
where prd_cost < 0 OR prd_cost IS NULL;

--Checking order of dates
select *
from silver.crm_prd_info
where prd_start_dt > prd_end_dt;


/*
**********************************************
Quality check for silver.crm_cust_info
**********************************************
*/

--Check For NULLs and Duplicates in Primary Key
--Expectation: No Results
SELECT cst_id, count(*) from silver.crm_cust_info
group by cst_id
having count(*) > 1 OR cst_id IS NULL;

--Check for unwanted spaces 
--Expectation: No results
SELECT cst_key
FROM silver.crm_cust_info
where cst_key != TRIM(cst_key);

SELECT cst_firstname
from silver.crm_cust_info
where cst_firstname != TRIM(cst_firstname);

select cst_lastname 
from silver.crm_cust_info
where cst_lastname != TRIM(cst_lastname);

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info;

SELECT DISTINCT cst_marital_status
FROM silver.crm_cust_info;

/*
**********************************************
Quality check for silver.crm_sales_details
**********************************************
*/

--Check for invalid dates
select NULLIF(sls_order_dt, 0) as sls_order_dt from silver.crm_sales_details
where sls_order_dt <= 0 OR 
len(sls_order_dt) != 8 OR
sls_order_dt > 20500101 OR
sls_order_dt < 19000101;

select NULLIF(sls_ship_dt, 0) as sls_ship_dt from silver.crm_sales_details
where sls_ship_dt <= 0 OR 
len(sls_ship_dt) != 8 OR
sls_ship_dt > 20500101 OR
sls_ship_dt < 19000101;

select sls_ship_dt,sls_due_dt from silver.crm_sales_details
where sls_ship_dt < sls_due_dt;

select NULLIF(sls_due_dt, 0) as sls_due_dt from silver.crm_sales_details
where sls_due_dt <= 0 OR 
len(sls_due_dt) != 8 OR
sls_due_dt > 20500101 OR
sls_due_dt < 19000101;

--Checking for invalid order dates
select * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

--Checking Data consistency between : Sales, Quantity and Price
-- Sales = Quantity * Price
-- Negative, Zeros, NULLS are not allowed.
Select DISTINCT
sls_sales old_sls_sales,
sls_quantity,
sls_price old_sls_price,
CASE WHEN sls_sales IS NULL or sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
	 THEN sls_quantity * ABS(sls_price)
	 ELSE sls_sales
END AS sls_sales,

CASE WHEN sls_price IS NULL or sls_price <= 0
	 THEN sls_sales / NULLIF(sls_quantity, 0)
	 ELSE sls_price
END AS sls_price

from silver.crm_sales_details
where sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL 
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <= 0
ORDER BY sls_sales, sls_quantity, sls_price;


/*
**********************************************
Quality check for silver.erp_cust_az12
**********************************************
*/

--Identify out-of-range dates
select bdate from silver.erp_cust_az12
where bdate > GETDATE();

--Data standardization & Consistency
select gen from silver.erp_cust_az12
group by gen;

/*
**********************************************
Quality check for silver.erp_loc_a101
**********************************************
*/

select SUBSTRING(cid,7,LEN(cid)) from bronze.erp_loc_a101;
select cst_id from silver.crm_cust_info;

--Data standardization & Consistency
select distinct cntry from silver.erp_loc_a101;

/*
**********************************************
Quality check for silver.erp_px_cat_g1v2
**********************************************
*/

--Check for unwanted spaces
select * from silver.erp_px_cat_g1v2
where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance);

--Data standardization & Consistency
select distinct subcat 
from silver.erp_px_cat_g1v2;
