/*
===============================================================================
Gold Layer Views Creation (Business-Ready Analytics Layer)
===============================================================================
Script Purpose:
    This script creates dimensional and fact views in the 'gold' schema.
    The Gold layer represents the final, business-ready semantic layer
    designed for analytics, reporting, and dashboard consumption.

    The views consolidate, enrich, and standardise data from the Silver layer
    into a star-schema–like structure consisting of:
        - Customer Dimension
        - Product Dimension
        - Sales Fact

Views Created:
    1. gold.dim_customers
        - Provides a unified customer dimension by combining CRM and ERP data
        - CRM is treated as the master source for customer identity and gender
        - ERP attributes (birthdate, country) are used as enrichment
        - Surrogate key (customer_key) is generated using ROW_NUMBER()

    2. gold.dim_products
        - Provides the current-state product dimension
        - Enriches CRM product data with ERP category information
        - Filters out historical records using prd_end_dt IS NULL
        - Surrogate key (product_key) is generated using ROW_NUMBER()

    3. gold.fact_sales
        - Captures transactional sales data at order-line grain
        - Joins to customer and product dimensions using business keys
        - Designed for analytical queries such as revenue, volume, and trends

Design Considerations:
    - Views are used instead of tables to ensure freshness and reduce redundancy
    - Surrogate keys are generated at query time for dimensional modeling
    - LEFT JOINs preserve fact completeness even when dimension data is missing
    - Business rules prioritise CRM as the authoritative source where conflicts exist

Data Flow:
    Bronze (Raw) → Silver (Cleansed & Standardised) → Gold (Analytics Ready)

Usage Example:
    SELECT * FROM gold.fact_sales;
    SELECT * FROM gold.dim_customers;
    SELECT * FROM gold.dim_products;

===============================================================================
*/


CREATE VIEW gold.dim_customers AS
SELECT
ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marital_status,
	CASE 
		WHEN ci.cst_gndr !='N/A' 
			THEN ci.cst_gndr -- CRM is the Master for gender Information
		ELSE COALESCE(ca.gen, 'N/A')
	END AS gender ,
	ca.bdate AS birthdate,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca
ON  ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON	ci.cst_key = la.cid;

---------------------------------------------------------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------------------------------------------------------
CREATE VIEW gold.dim_products AS 
SELECT 
	ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key,
	pn.prd_id AS product_id,
	pn.prd_key AS product_number,
	pn.prd_nm AS product_name,
	pn.cat_id AS category_id,
	pc.cat AS category,
	pc.subcat AS subcategory,
	pc.maintenance,
	pn.prd_cost AS cost,
	pn.prd_line AS product_line,
	pn.prd_start_dt AS start_date

FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE prd_end_dt IS NULL ; -- Filter out all historical data


---------------------------------------------------------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------------------------------------------------------

CREATE VIEW gold.fact_sales AS 
SELECT 
	sd.sls_ord_num AS order_number,
	pr.product_key ,
	cu.customer_key,
	sd.sls_order_dt AS order_date,
	sd.sls_ship_dt AS shipping_date,
	sd.sls_due_dt AS due_date,
	sd.sls_sales AS sales_amount,
	sd.sls_quantity AS quantity,
	sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id
