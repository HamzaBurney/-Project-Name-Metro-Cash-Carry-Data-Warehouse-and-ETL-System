USE METRO_DATAWAREHOUSE;

-- Q1. Top Revenue-Generating Products on Weekdays and Weekends with Monthly Drill-Down
-- Find the top 5 products that generated the highest revenue, separated by weekday and weekend
-- sales, with results grouped by month for a specified year.
WITH SalesByType AS (
    SELECT  d.month_t, CASE WHEN d.weekend THEN 'Weekend' ELSE 'Weekday' END AS sale_type, p.product_id, p.product_name, SUM(s.sales_revenue) AS total_revenue
	FROM product p
    JOIN sales s ON p.product_id = s.product_id
    JOIN date d ON s.time_id = d.time_id 
    WHERE d.year_t = 2019
    GROUP BY d.month_t, sale_type, p.product_id, p.product_name
),
RankedSales AS (
    SELECT month_t, sale_type, product_id, product_name, total_revenue, ROW_NUMBER() OVER (PARTITION BY month_t, sale_type ORDER BY total_revenue DESC) AS `rank` FROM SalesByType
)
SELECT month_t, sale_type, product_id, product_name, total_revenue
FROM RankedSales WHERE `rank` <= 5 ORDER BY month_t, sale_type, `rank`;

-- Q2. Trend Analysis of Store Revenue Growth Rate Quarterly for 2017
-- Calculate the revenue growth rate for each store on a quarterly basis for 2017.

WITH QuarterlyRevenue AS (
    SELECT st.store_id, st.store_name, d.year_t, d.quarter_t, SUM(s.sales_revenue) AS total_revenue
    FROM store st
    JOIN sales s ON st.store_id = s.store_id
    JOIN date d ON s.time_id = d.time_id
    WHERE d.year_t = 2019
    GROUP BY st.store_id, st.store_name, d.year_t, d.quarter_t
),
RevenueWithLag AS (
    SELECT qr.store_id, qr.store_name, qr.quarter_t, qr.total_revenue,
        LAG(qr.total_revenue) OVER (PARTITION BY qr.store_id ORDER BY qr.quarter_t) AS prev_quarter_revenue
    FROM QuarterlyRevenue qr
)
SELECT store_id, store_name, quarter_t, total_revenue, prev_quarter_revenue,
    CASE  WHEN prev_quarter_revenue IS NOT NULL 
    THEN ROUND(((total_revenue - prev_quarter_revenue) / prev_quarter_revenue) * 100, 2)
	ELSE NULL
    END AS growth_rate
FROM RevenueWithLag
ORDER BY store_id, quarter_t;

-- Q3. Detailed Supplier Sales Contribution by Store and Product Name
-- For each store, show the total sales contribution of each supplier broken down by product name. The
-- output should group results by store, then supplier, and then product name under each supplier.

SELECT st.store_name, su.supplier_name, p.product_name, SUM(s.sales_revenue) AS total_sales
FROM store st
JOIN sales s ON st.store_id = s.store_id
JOIN supplier su ON s.supplier_id = su.supplier_id
JOIN product p ON s.product_id = p.product_id
GROUP BY st.store_name, su.supplier_name, p.product_name
ORDER BY st.store_name, su.supplier_name, p.product_name;


-- Q4. Seasonal Analysis of Product Sales Using Dynamic Drill-Down
-- Present total sales for each product, drilled down by seasonal periods (Spring, Summer, Fall,
-- Winter). This can help understand product performance across seasonal periods.

SELECT p.product_name,
    CASE 
        WHEN d.month_t IN (3, 4, 5) THEN 'Spring'
        WHEN d.month_t IN (6, 7, 8) THEN 'Summer'
        WHEN d.month_t IN (9, 10, 11) THEN 'Fall'
        WHEN d.month_t IN (12, 1, 2) THEN 'Winter'
    END AS season, SUM(s.sales_revenue) AS total_sales
FROM sales s
JOIN product p ON s.product_id = p.product_id
JOIN date d ON s.time_id = d.time_id
GROUP BY p.product_name, season
ORDER BY p.product_name, season;

-- Q5. Store-Wise and Supplier-Wise Monthly Revenue Volatility
-- Calculate the month-to-month revenue volatility for each store and supplier pair. Volatility can be
-- defined as the percentage change in revenue from one month to the next, helping identify stores
-- or suppliers with highly fluctuating sales.

WITH MonthlyRevenue AS (
    SELECT st.store_name, su.supplier_name, d.year_t, d.month_t, SUM(s.sales_revenue) AS total_revenue
    FROM store st
    JOIN sales s ON st.store_id = s.store_id
    JOIN supplier su ON s.supplier_id = su.supplier_id
    JOIN date d ON s.time_id = d.time_id
    GROUP BY st.store_name, su.supplier_name, d.year_t, d.month_t
),
RevenueWithLag AS (
    SELECT mr.store_name, mr.supplier_name, mr.year_t, mr.month_t, mr.total_revenue,
        LAG(mr.total_revenue) OVER (PARTITION BY mr.store_name, mr.supplier_name, mr.year_t ORDER BY mr.month_t) AS prev_month_revenue
    FROM MonthlyRevenue mr
)
SELECT store_name, supplier_name, year_t, month_t, total_revenue, prev_month_revenue,
    CASE 
        WHEN prev_month_revenue IS NOT NULL THEN 
            ROUND(((total_revenue - prev_month_revenue) / prev_month_revenue) * 100, 2)
        ELSE NULL
    END AS revenue_volatility
FROM RevenueWithLag
ORDER BY store_name, supplier_name, year_t, month_t;


-- Q6. Top 5 Products Purchased Together Across Multiple Orders (Product Affinity Analysis)
-- Identify the top 5 products frequently bought together within a set of orders (i.e., multiple
-- products purchased in the same transaction). This product affinity analysis could inform potential
-- product bundling strategies.

WITH ProductPairs AS (
    SELECT o1.product_id AS product_id_1, o2.product_id AS product_id_2, COUNT(*) AS pair_count
    FROM sales o1
    JOIN sales o2 ON o1.order_id = o2.order_id AND o1.product_id < o2.product_id
    GROUP BY o1.product_id, o2.product_id
),
ProductDetails AS (
    SELECT pp.product_id_1, p1.product_name AS product_name_1, pp.product_id_2, p2.product_name AS product_name_2, pp.pair_count
    FROM ProductPairs pp
    JOIN product p1 ON pp.product_id_1 = p1.product_id
    JOIN product p2 ON pp.product_id_2 = p2.product_id
)
SELECT product_name_1, product_name_2, pair_count
FROM ProductDetails
ORDER BY pair_count DESC
LIMIT 5;

-- Q7. Yearly Revenue Trends by Store, Supplier, and Product with ROLLUP
-- Use the ROLLUP operation to aggregate yearly revenue data by store, supplier, and product,
-- enabling a comprehensive overview from individual product-level details up to total revenue per
-- store. This query should provide an overview of cumulative and hierarchical sales figures.

SELECT st.store_name, su.supplier_name, p.product_name, SUM(s.sales_revenue) AS total_revenue, d.year_t
FROM store st
JOIN sales s ON st.store_id = s.store_id
JOIN supplier su ON s.supplier_id = su.supplier_id
JOIN product p ON s.product_id = p.product_id
JOIN date d ON s.time_id = d.time_id
WHERE d.year_t = 2019  
GROUP BY st.store_name, su.supplier_name, p.product_name, d.year_t
WITH ROLLUP ORDER BY st.store_name, su.supplier_name, p.product_name;


-- Q8. Revenue and Volume-Based Sales Analysis for Each Product for H1 and H2
-- For each product, calculate the total revenue and quantity sold in the first and second halves of
-- the year, along with yearly totals. This split-by-time-period analysis can reveal changes in product
-- popularity or demand over the year.

SELECT p.product_id, p.product_name,
    -- First Half (H1) Revenue and Quantity
	SUM(CASE WHEN d.half_of_year = 1 THEN s.sales_revenue ELSE 0 END) AS H1_revenue,
    SUM(CASE WHEN d.half_of_year = 1 THEN s.quantity ELSE 0 END) AS H1_quantity,
    -- Second Half (H2) Revenue and Quantity
    SUM(CASE WHEN d.half_of_year = 2 THEN s.sales_revenue ELSE 0 END) AS H2_revenue,
    SUM(CASE WHEN d.half_of_year = 2 THEN s.quantity ELSE 0 END) AS H2_quantity,
    -- Yearly Totals
    SUM(s.sales_revenue) AS total_revenue,
    SUM(s.quantity) AS total_quantity
FROM product p
JOIN sales s ON p.product_id = s.product_id
JOIN date d ON s.time_id = d.time_id
GROUP BY p.product_id, p.product_name
ORDER BY p.product_name;


-- Q9. Identify High Revenue Spikes in Product Sales and Highlight Outliers
-- Calculate daily average sales for each product and flag days where the sales exceed twice the daily
-- average by product as potential outliers or spikes. Explain any identified anomalies in the report,
-- as these may indicate unusual demand events.

WITH DailyProductSales AS (
    SELECT p.product_id, p.product_name, d.date_t, SUM(s.sales_revenue) AS daily_sales FROM product p
    JOIN sales s ON p.product_id = s.product_id
    JOIN date d ON s.time_id = d.time_id
    GROUP BY p.product_id, p.product_name, d.date_t
),
ProductDailyAverages AS (
    SELECT product_id, product_name, AVG(daily_sales) AS avg_daily_sales
    FROM DailyProductSales
    GROUP BY product_id, product_name
),
OutlierDetection AS (
    SELECT dps.product_id, dps.product_name, dps.date_t, dps.daily_sales, pda.avg_daily_sales,
        CASE WHEN dps.daily_sales > 2 * pda.avg_daily_sales THEN 'Outlier' ELSE 'Normal' 
        END AS sales_status
    FROM DailyProductSales dps
    JOIN ProductDailyAverages pda ON dps.product_id = pda.product_id
)
SELECT product_id, product_name, date_t, daily_sales, avg_daily_sales, sales_status
FROM OutlierDetection WHERE sales_status = 'Outlier' ORDER BY product_id, date_t;


-- Q10. Create a View STORE_QUARTERLY_SALES for Optimized Sales Analysis
-- Create a view named STORE_QUARTERLY_SALES that aggregates total quarterly sales by store,
-- ordered by store name. This view allows quick retrieval of store-specific trends across quarters,
-- significantly improving query performance for regular sales analysis.

CREATE VIEW STORE_QUARTERLY_SALES AS
SELECT st.store_id, st.store_name, d.quarter_t AS quarter, SUM(s.sales_revenue) AS total_quarterly_sales
FROM store st
JOIN sales s ON st.store_id = s.store_id
JOIN date d ON s.time_id = d.time_id
GROUP BY st.store_id, st.store_name, d.quarter_t
ORDER BY st.store_name, d.quarter_t;

