-- Data cleaning, checking column data types
/* 
SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'cust'; 

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'item';

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'orders';

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'pay'; 

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'prod';

SELECT COLUMN_NAME, DATA_TYPE 
FROM INFORMATION_SCHEMA.COLUMNS 
WHERE TABLE_NAME = 'seller';

-- Altering data types and assigning primary key constraint.
ALTER TABLE order
ALTER COLUMN order_id nvarchar(50) NOT NULL

ALTER TABLE order
ADD CONSTRAINT pk_ord PRIMARY KEY (order_id)

ALTER TABLE prod
ALTER COLUMN product_id nvarchar(50) NOT NULL

ALTER TABLE prod
ADD CONSTRAINT pk_pro PRIMARY KEY (product_id)
*/

-- Detecting duplicates 
/* 
WITH CTE AS (
    SELECT 
        order_id,
        order_item_id,
        product_id,
        seller_id,
        shipping_limit_date,
        price,
        freight_value,
        ROW_NUMBER() OVER(
            PARTITION BY 
                order_id,
                order_item_id,
                product_id,
                seller_id,
                shipping_limit_date,
                price,
                freight_value
            ORDER BY order_id
        ) AS row
    FROM item
)
SELECT * FROM CTE
WHERE row > 1
 */


-- State with highest purchase value
/* 
SELECT
    SUM(pay.payment_value) AS total_val,
    cust.customer_state
FROM
    cust
JOIN orders
    ON cust.customer_id = orders.customer_id
JOIN pay    
    ON orders.order_id = pay.order_id
GROUP BY 
    cust.customer_state
ORDER BY SUM(pay.payment_value) DESC
 */


-- Aggreagating top states with highest purchase value and it's proportion of the total.
/*
WITH CTE AS (
    SELECT
        cust.customer_state,
        SUM(pay.payment_value) AS total_payment,
        (SUM(pay.payment_value) / (SELECT SUM(payment_value) FROM pay)) * 100 AS perc 
        
        -- This line computes after GROUP BY has been performed. So it basically says GROUP the cust table on 'customer_state', SUM payment_value based on the grouping, then take the SUMMED value divided by the total value of the 'payment_value' column.
    FROM
        cust
        JOIN orders
            ON cust.customer_id = orders.customer_id
        JOIN pay
            ON orders.order_id = pay.order_id
    GROUP BY 
        cust.customer_state
),
cumu AS (
    SELECT
        customer_state,
        total_payment,
        perc,
        SUM(perc) OVER (ORDER BY total_payment DESC) AS cumulative_perc -- If we write this line in CTE, it will not recognise 'total_payment' because it is the same level, the alias has not been created yet. So we write it here in cumu
    FROM CTE
) 
SELECT
    customer_state,
    total_payment,
    perc,
    cumulative_perc 
FROM cumu
WHERE cumulative_perc <= 81
ORDER BY total_payment DESC;
-- Top 10 states contributing to 80% of the sales in three years are: Sao Paulo, Rio de Janeiro, Minas Gerais, Rio Grande do Sul, Paraná, Santa Catarina, Bahia, Goiás, Distrito Federal, Pernambuco
*/


-- Summary statistics of item table
/* 
WITH CTE AS (
    SELECT
        freight_value,
        PERCENTILE_CONT(0.5) WITHIN GROUP (
        ORDER BY freight_value
        ) OVER() AS median
FROM item
)
SELECT 
   COUNT(freight_value) AS count,
    SUM(freight_value) AS total_sum,
    AVG(freight_value) AS average,
    MIN(freight_value) AS minimum,
    MAX(freight_value) AS maximum,
    median
FROM CTE
GROUP BY median
 */


-- Time difference from order_purchase to order_approved.
/* 
WITH CTE AS (
SELECT 
    DATEDIFF(MINUTE, order_purchase_timestamp, order_approved_at) AS timediff,
    PERCENTILE_CONT(0.8) WITHIN GROUP (
            ORDER BY ((DATEDIFF(MINUTE, order_purchase_timestamp, order_approved_at)))
        ) OVER() AS median_time
FROM orders
)
SELECT 
    timediff / 1440 AS Days,
    (timediff % 1440) / 60 AS Hours,
    timediff % 60 AS Minutes,
    ROUND((median_time / 60), 2) AS median_hours
FROM CTE 
WHERE timediff <> 0
ORDER BY Days, Hours, Minutes

-- 80% or orders are approved within 1 day.
*/


-- Purchase date to carrier delivery date
/* 
WITH CTE AS (
SELECT 
    DATEDIFF(MINUTE, order_purchase_timestamp, order_delivered_carrier_date) AS timediff,
    PERCENTILE_CONT(0.8) WITHIN GROUP (
            ORDER BY ((DATEDIFF(MINUTE, order_purchase_timestamp, order_delivered_carrier_date)))
        ) OVER() AS median_minutes
FROM orders
)
SELECT
    timediff / 1440 AS Days,
    (timediff % 1440) / 60 AS Hours,
    timediff % 60 AS Minutes,
    ROUND((median_minutes / 60), 2) AS median_hours
FROM CTE 
WHERE timediff <> 0
ORDER BY Days, Hours, Minutes
-- We realise there are some rows of data where the time difference is negative, meaning that the deliverey date is earlier than order purchase date.
-- Although thre are only about 160 rows, we will drop these data to keep the dataset clean.


DELETE FROM orders
WHERE DATEDIFF(MINUTE, order_purchase_timestamp, order_delivered_carrier_date) < 0;

-- There are also data where the time difference from order to delivery is only a few minutes in between, which is not possible.
-- Let's perform an outlier calculation and treat anything above or below 1.5* IQR as an outlier.
WITH CTE AS (
    SELECT
        DATEDIFF(MINUTE, order_purchase_timestamp, order_delivered_carrier_date) AS timediff
    FROM orders
),
IQR AS (
    SELECT
        timediff,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY timediff) OVER () AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY timediff) OVER () AS q3
    FROM CTE
)
SELECT 
    timediff
FROM IQR
WHERE timediff < q1 - ((q3 - q1) * 1.5)
    OR timediff > q3 + ((q3 - q1) * 1.5)
-- These are the columns which we will drop from the dataset. 

WITH CTE AS (
    SELECT
        order_id,
        DATEDIFF(MINUTE, order_purchase_timestamp, order_delivered_carrier_date) AS timediff
    FROM orders
),
IQR AS (
    SELECT
        order_id,
        timediff,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY timediff) OVER () AS q1,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY timediff) OVER () AS q3
    FROM CTE
)
DELETE FROM orders
WHERE order_id IN (
        SELECT order_id
        FROM IQR
        WHERE timediff < q1 - ((q3 - q1) * 1.5)
            OR timediff > q3 + ((q3 - q1) * 1.5)
)
*/


--  Carrier delivery date vs estimated delivery date time differences
/* 
WITH CTE AS (
    SELECT
        DATEDIFF(MINUTE, order_delivered_carrier_date, order_estimated_delivery_date) AS timediff
    FROM orders
    WHERE order_status = 'delivered'
),
conversion AS (
    SELECT
        CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY (timediff)
                ) OVER()
            AS DECIMAL (18,4)
        ) AS median_minutes
    FROM CTE
)
SELECT
    median_minutes / 1440 AS Days,
    (median_minutes % 1440) / 60 AS Hours,
    median_minutes % 60 AS Minutes
FROM conversion 
GROUP BY median_minutes
ORDER BY Days, Hours, Minutes;
-- Median delivery time is around 3 weeks


SELECT 
    DISTINCT order_status,
    (SELECT COUNT(*)
    FROM orders
    WHERE order_delivered_carrier_date > order_estimated_delivery_date
        AND order_status = 'delivered'
    ) AS late_delivery,
    
    (SELECT COUNT(*)
    FROM orders
    WHERE order_delivered_carrier_date <= order_estimated_delivery_date
        AND order_status = 'delivered'
    ) AS on_time
FROM orders
WHERE order_status = 'delivered'
-- Almost all the orders are delivered within estimated time
 */


-- Number of days until customers delivery confirmation after carrier deliverey date
/* 
WITH CTE AS (
    SELECT
        DATEDIFF(MINUTE, order_delivered_carrier_date, order_delivered_customer_date) AS timediff
    FROM orders
    WHERE order_status = 'delivered'
),
median AS (
    SELECT
        CAST(PERCENTILE_CONT(0.5) WITHIN GROUP (
                ORDER BY (timediff)
                ) OVER()
            AS DECIMAL (18,4)
        ) AS median_minutes
    FROM CTE
)
SELECT
    median_minutes / 1440 AS Days,
    (median_minutes % 1440) / 60 AS Hours,
    median_minutes % 60 AS Minutes
FROM median 
GROUP BY median_minutes
ORDER BY Days, Hours, Minutes;
-- Customer usually confirms the delivery a week after actual delivery date.


WITH CTE AS (
    SELECT
        DATEDIFF(MINUTE, order_delivered_carrier_date, order_delivered_customer_date) AS timediff
    FROM orders
    WHERE order_status = 'delivered'
),
eighties AS (
    SELECT
        CAST(PERCENTILE_CONT(0.8) WITHIN GROUP (
                ORDER BY (timediff)
                ) OVER()
            AS DECIMAL (18,4)
        ) AS eighty_percentile
    FROM CTE
)
SELECT
    eighty_percentile / 1440 AS Days,
    (eighty_percentile % 1440) / 60 AS Hours,
    eighty_percentile % 60 AS Minutes
FROM eighties 
GROUP BY eighty_percentile
ORDER BY Days, Hours, Minutes;
-- 80% of customers confirms just before the 2 weeks timeframe.
*/


-- Identifying purchasing trend, is weekend sales higher?
/* 
WITH CTE AS (
        SELECT
        CASE
            WHEN DATEPART(weekday, order_purchase_timestamp) IN (1, 7) THEN 'Weekend'
            ELSE 'Weekday'
        END AS daytype
        FROM orders
)
SELECT 
    daytype,
    COUNT(*) AS total_count
FROM CTE
GROUP BY daytype;
-- Doens't seem like it


-- Identifying whether specific days has higher purchase than usual.
SELECT day,
    COUNT(*) AS day_count
FROM (
    SELECT
        DATENAME(weekday, order_purchase_timestamp) AS day
    FROM orders
) AS CTE
GROUP BY day
-- Not really either, but we can say that on Saturday and Sunday the orders are slighly lower than on weekdays.
 */


-- Identifying if there are shopping season on certain part of the year.
/*
WITH CTE AS (
        SELECT
        CASE
            WHEN DATEPART(month, order_purchase_timestamp) IN (1, 2, 3) THEN 'Q1'
            WHEN DATEPART(month, order_purchase_timestamp) IN (4, 5, 6) THEN 'Q2'
            WHEN DATEPART(month, order_purchase_timestamp) IN (7, 8, 9) THEN 'Q3'
            ELSE 'Q4'
        END AS quarter
        FROM orders
)
SELECT 
    quarter,
    COUNT(*) AS total_count
FROM CTE
GROUP BY quarter;
-- No, but we can definitely tell which month IS NOT online shopping season, that is during the holiday quarter Q4.


-- Identifying month by month shopping trend.
SELECT 
    month,
    COUNT(*) AS month_count
FROM (
    SELECT
        DATEPART(month, order_purchase_timestamp) AS month
    FROM orders
) AS CTE
GROUP BY month
ORDER BY month;
-- There is a trend where early of the year starts off slow, gradually pick up on March upon entering the spring season, peaks during summer season and a sharp fall when summer season ends. This trend continues till december before picking up again next year.
 */


-- Most common credit card instalment period
/* 
WITH CTE AS (
    SELECT
        payment_installments,
        COUNT(*) AS transaction_count,
        (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()) AS perc -- if we move *100 to a position outside of the division operator, perc and cumu perc will result in zero. Because when the division takes place it results in a number lesser than 1, and will round to 0 because of integer dtype. 0 * 100 is 0
    FROM pay
    GROUP BY 
        payment_installments
),
cumu AS (
    SELECT
        payment_installments,
        transaction_count,
        perc,
        SUM(perc) OVER (ORDER BY transaction_count DESC) AS cumulative_perc
    FROM CTE
) 
SELECT
    payment_installments,
    transaction_count,
    CAST(perc AS DECIMAL (18,2)) AS perc,
    CAST(cumulative_perc AS DECIMAL (18,2)) AS cumulative_perc
FROM cumu
ORDER BY transaction_count DESC;
-- Only a small fraction of buyers opt for instalments of more than a year for online purchase goods.
 */


-- Payment method distribution
/* 
WITH CTE AS (
    SELECT
        payment_type,
        COUNT(*) AS type_count,
        (COUNT(*) * 100.0 / SUM(COUNT(*)) OVER()) AS perc
    FROM pay
    GROUP BY 
        payment_type
)
SELECT
    payment_type,
    type_count,
    CAST(perc AS DECIMAL (18,2)) AS perc
FROM CTE
ORDER BY type_count DESC;
-- 90% of all purchases are paid via credit card or boleto. Boleto is a payment method offered by the central bank of Brazil for comsumers without credit card.
 */


-- Frequency distribution for each $50 bin values
/* 
SELECT
    dist,
    COUNT(*) AS count
FROM(
    SELECT
        CASE 
            WHEN payment_value >= 0 AND payment_value < 50 THEN '50 or below'
            WHEN payment_value >= 50 AND payment_value < 100 THEN '50 to 100'
            WHEN payment_value >= 100 AND payment_value < 150 THEN '100 to 150'
            WHEN payment_value >= 150 AND payment_value < 200 THEN '150 to 200'
            ELSE 'More than 200'
        END AS dist
    FROM pay
) AS CTE
GROUP BY dist
*/


-- Most sold product category
/* 
SELECT TOP 10
    COUNT(o.order_id) AS order_id_count,
    COUNT(i.product_id) AS product_id_count,
    p.product_category_name
FROM orders AS o
LEFT JOIN item AS i
    ON o.order_id = i.order_id
LEFT JOIN prod AS p
    ON i.product_id = p.product_id
GROUP BY p.product_category_name
ORDER BY order_id_count DESC;

-- The most brought products, in English, are
-- 1. Bedclothes, table dressing, bath towels
-- 2. Health and Beauty
-- 3. Sports and Leisure
-- 4. Decoration Furniture
-- 5. IT accessories
-- 6. Housewares
-- 7. Watches, Gifts
-- 8. Telephone products
-- 9. Garden tools
-- 10. Automotive products

-- Order by ascending gives us the category least bought, the 5 least popular categories are:
-- 1. Insurance and services
-- 2. Baby clothes
-- 3. PC games
-- 4. CDs and DVDs
-- 5. Cuisine
 */


-- Seller location
/* 
SELECT 
    seller_state,
    COUNT(*) AS state_count
FROM seller
GROUP BY seller_state
ORDER BY state_count DESC
-- At the state level, the state SP hosts the most number of sellers

SELECT 
    seller_city,
    seller_state,
    COUNT(*) AS city_count
FROM seller
GROUP BY seller_city, seller_state
ORDER BY city_count DESC
-- At the city level, Sao Paulo the largest city is where 694 sellers are located at. Curitiba and Rio de Janeiro hosts 124 and 93 sellers respectively, taking the second and third spot. 
 */


-- Sellers with top revenue
/*
SELECT 
    s.seller_id,
    SUM(i.price) AS total_revenue
FROM orders AS o
LEFT JOIN item AS i
    ON o.order_id = i.order_id
LEFT JOIN seller AS s
    ON i.seller_id = s.seller_id
GROUP BY s.seller_id
ORDER BY total_revenue DESC;


-- Most revenue generating category for top 10 sellers in 3 years
WITH CTE AS (
    SELECT 
        i.seller_id,
        p.product_category_name,
        SUM(i.price) AS cat_revenue,
        ROW_NUMBER() OVER (PARTITION BY i.seller_id ORDER BY SUM(i.price) DESC) AS rank
    FROM item AS i
    LEFT JOIN prod AS p
        ON i.product_id = p.product_id
    WHERE i.seller_id 
        IN (
            '4869f7a5dfa277a7dca6462dcf3b52b2',
            '53243585a1d6dc2643021fd1853d8905',
            '4a3ca9315b744ce9f8e9374361493884',
            'fa1c13f2614d7b5c4749cbc52fecda94',
            '7e93a43ef30c4f03f38b393420bc753a',
            'da8622b14eb17ae2831f4ac5b9dab84a',
            '7a67c85e85bb2ce8582c35f2203ad736',
            '955fee9216a65b617aa5c0531780ce60',
            '1025f0e2d44d7041d6cf58b6550e0bfa'
        )
    GROUP BY 
        i.seller_id,
        product_category_name
)
SELECT
    seller_id,
    product_category_name,
    cat_revenue
FROM CTE
WHERE rank = 1
ORDER BY cat_revenue DESC;

-- The most revenue generating products for the top 10 sellers are mainly watches, bedroom & washroom products and furnitures decorations.
 */


-- Sellers with top revenue for the year 2016, 2017 and 2018.
/* 
WITH CTE AS (
    SELECT 
        s.seller_id,
        SUM(i.price) AS total_revenue,
        YEAR(o.order_purchase_timestamp) AS year,
        ROW_NUMBER() OVER (PARTITION BY YEAR(o.order_purchase_timestamp) ORDER BY SUM(i.price) DESC) AS RANK
    FROM orders AS o
    LEFT JOIN item AS i
        ON o.order_id = i.order_id
    LEFT JOIN seller AS s
        ON i.seller_id = s.seller_id
    GROUP BY s.seller_id, YEAR(o.order_purchase_timestamp)
)
SELECT *
FROM CTE
WHERE RANK <= 5
ORDER BY year, total_revenue DESC;
 */


-- Percentage of total revenue generated by top 20% seller
/*
WITH CTE AS (
    SELECT 
        s.seller_id,
        SUM(i.price) AS total_revenue
    FROM orders AS o
    LEFT JOIN item AS i
        ON o.order_id = i.order_id
    LEFT JOIN seller AS s
        ON i.seller_id = s.seller_id
    GROUP BY s.seller_id
)
SELECT TOP 20 PERCENT
    seller_id,
    total_revenue,
    ROUND((total_revenue * 100.0) / SUM(total_revenue) OVER (), 2) AS percentage,
    ROUND(SUM(total_revenue) OVER (ORDER BY total_revenue DESC) * 100.0 / SUM(total_revenue) OVER (), 2) AS cumulative_percentage
FROM CTE
ORDER BY total_revenue DESC;
-- The top 20% seller generates about 82% of the total revenue across 3 years period.
 */


-- Top 3 seller by numbers of items sold and average price per item for 2016, 2017 and 2018 respectively
/*
WITH CTE AS (
    SELECT 
        s.seller_id,
        COUNT(o.order_id) AS item_count,
        YEAR(o.order_purchase_timestamp) AS year,
        ROW_NUMBER() OVER (PARTITION BY YEAR(o.order_purchase_timestamp) ORDER BY COUNT(o.order_id) DESC) AS RANK,
        AVG(i.price) AS average_price
    FROM orders AS o
    LEFT JOIN item AS i
        ON o.order_id = i.order_id
    LEFT JOIN seller AS s
        ON i.seller_id = s.seller_id
    GROUP BY s.seller_id, YEAR(o.order_purchase_timestamp)
)
SELECT *
FROM CTE
WHERE RANK <= 3
ORDER BY year, item_count DESC;
-- Average price per item seemed to fluctuate between $200 to as low as $57 across three years.
*/