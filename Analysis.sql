
sql_code = """
-- 1. Import the dataset and do EDA

-- 2. Datatype of columns in a table
SELECT column_name, data_type
FROM businesscase1sqlscaler.businesscasestudy1.INFORMATION_SCHEMA.COLUMNS
WHERE table_name = "orders";

-- 3. Time period of the dataset
SELECT
MIN(order_purchase_timestamp) AS STARTING_TIME,
MAX(order_purchase_timestamp) AS ENDING_TIME
FROM `businesscase1sqlscaler.businesscasestudy1.orders`;

-- 4. Cities and States of customers
SELECT
COUNT(DISTINCT(geolocation_city)) AS Cities,
COUNT(DISTINCT(geolocation_state)) AS States
FROM `businesscase1sqlscaler.businesscasestudy1.geolocation`;

SELECT
DISTINCT customer_city,
customer_state
FROM `businesscase1sqlscaler.businesscasestudy1.customers` AS Cus
JOIN `businesscase1sqlscaler.businesscasestudy1.orders` AS Ord
ON Cus.customer_id = Ord.customer_id;

-- 6. Growing trend and seasonality
SELECT
EXTRACT(year FROM order_purchase_timestamp) AS Year,
EXTRACT(month FROM order_purchase_timestamp) AS Months,
COUNT(*) AS Number_of_orders
FROM `businesscase1sqlscaler.businesscasestudy1.orders`
GROUP BY Year, Months
ORDER BY Year, Months;

SELECT
Month_code,
Total_sales
FROM (
  SELECT
  EXTRACT(MONTH FROM ord.order_purchase_timestamp) AS Month_code,
  ROUND(SUM(pay.payment_value), 2) AS Total_sales
  FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
  JOIN `businesscase1sqlscaler.businesscasestudy1.payments` AS pay
  ON ord.order_id = pay.order_id
  GROUP BY EXTRACT(MONTH FROM ord.order_purchase_timestamp)
)
ORDER BY Total_sales;

SELECT
EXTRACT(MONTH FROM order_purchase_timestamp) AS Month_code,
COUNT(*) AS Number_of_orders
FROM `businesscase1sqlscaler.businesscasestudy1.orders`
GROUP BY Month_code
ORDER BY Number_of_orders;

-- 7. Time of Day Analysis
SELECT
CASE
  WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 0 AND 6 THEN "Dawn"
  WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 7 AND 12 THEN "Morning"
  WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 13 AND 18 THEN "Afternoon"
  WHEN EXTRACT(HOUR FROM order_purchase_timestamp) BETWEEN 19 AND 23 THEN "Night"
END AS Time_zone,
COUNT(DISTINCT order_id) AS Number_of_orders
FROM `businesscase1sqlscaler.businesscasestudy1.orders`
GROUP BY Time_zone
ORDER BY Number_of_orders;

-- 9. Month on Month orders by states
SELECT
EXTRACT(MONTH FROM order_purchase_timestamp) AS Month_code,
customer_state,
COUNT(*) AS Number_of_orders
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ords
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS cust
ON ords.customer_id = cust.customer_id
GROUP BY customer_state, Month_code
ORDER BY customer_state;

-- 10. Distribution of customers
SELECT
COUNT(DISTINCT(customer_unique_id)) AS All_customers,
customer_state
FROM `businesscase1sqlscaler.businesscasestudy1.customers`
GROUP BY customer_state
ORDER BY All_customers;

-- 12. % increase in cost of orders from 2017 to 2018
SELECT
Order_value_2017,
Order_value_2018,
(((Order_value_2018 - Order_value_2017)/ Order_value_2017)* 100) AS percentage_increase_in_cost_of_orders
FROM (
  SELECT
  SUM(IF(EXTRACT(year FROM ord.order_purchase_timestamp) = 2017, pay.payment_value,0)) AS Order_value_2017,
  SUM(IF(EXTRACT(year FROM ord.order_purchase_timestamp) = 2018, pay.payment_value,0)) AS Order_value_2018
  FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
  INNER JOIN `businesscase1sqlscaler.businesscasestudy1.payments` AS pay
  ON ord.order_id = pay.order_id
  WHERE EXTRACT(month FROM ord.order_purchase_timestamp) BETWEEN 1 AND 8
);

-- 13. Mean & Sum of price and freight by state
SELECT
co.customer_state AS State,
SUM(ordit.price) AS Sum_price,
AVG(ordit.price) AS Mean_price,
SUM(ordit.freight_value) AS Sum_freight,
AVG(ordit.freight_value) AS Mean_freight
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.order_items` AS ordit
ON ord.order_id = ordit.order_id
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
GROUP BY co.customer_state
ORDER BY co.customer_state;

-- 15. Delivery and Estimated Delivery Times
SELECT
order_id,
TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS Delivery_time,
TIMESTAMP_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY) AS Estimated_delivery_time
FROM `businesscase1sqlscaler.businesscasestudy1.orders`
WHERE order_status = "delivered";

-- 16. Time to Delivery
SELECT
order_id,
DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY) AS time_to_delivery,
DATE_DIFF(order_delivered_customer_date, order_estimated_delivery_date, DAY) AS diff_estimated_delivery
FROM `businesscase1sqlscaler.businesscasestudy1.orders`;

-- 17. Group by state - mean values
SELECT
co.customer_state AS State,
ROUND(AVG(ordit.freight_value),2) AS Mean_freight_value,
ROUND(AVG(TIMESTAMP_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)),2) AS Mean_of_time_to_delivery,
ROUND(AVG(TIMESTAMP_DIFF(order_estimated_delivery_date, order_delivered_customer_date, DAY)),2) AS Mean_of_diff_estimated_delivery
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
JOIN `businesscase1sqlscaler.businesscasestudy1.order_items` AS ordit
ON ord.order_id = ordit.order_id
GROUP BY co.customer_state;

-- 19. Top 5 states by freight value
SELECT
co.customer_state,
ROUND(AVG(ordit.freight_value),2) AS Average_freight_value
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
JOIN `businesscase1sqlscaler.businesscasestudy1.order_items` AS ordit
ON ord.order_id = ordit.order_id
GROUP BY co.customer_state
ORDER BY Average_freight_value DESC
LIMIT 5;

SELECT
co.customer_state,
ROUND(AVG(ordit.freight_value),2) AS Average_freight_value
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
JOIN `businesscase1sqlscaler.businesscasestudy1.order_items` AS ordit
ON ord.order_id = ordit.order_id
GROUP BY co.customer_state
ORDER BY Average_freight_value ASC
LIMIT 5;

-- 20. Top 5 states by delivery time
SELECT
co.customer_state AS State,
ROUND(AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)),2) AS Mean_of_time_to_delivery
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
GROUP BY co.customer_state
ORDER BY Mean_of_time_to_delivery DESC
LIMIT 5;

SELECT
co.customer_state AS State,
ROUND(AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)),2) AS Mean_of_time_to_delivery
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
GROUP BY co.customer_state
ORDER BY Mean_of_time_to_delivery ASC
LIMIT 5;

-- 21. States with fast vs slow delivery
SELECT
geolocation_state,
ROUND(AVG(DATE_DIFF(order_delivered_customer_date, order_purchase_timestamp, DAY)),2) AS avg_Delivery_time
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
JOIN `businesscase1sqlscaler.businesscasestudy1.geolocation` AS geo
ON co.customer_zip_code_prefix = geo.geolocation_zip_code_prefix
WHERE order_status = "delivered"
GROUP BY geolocation_state
ORDER BY avg_Delivery_time ASC
LIMIT 5;

SELECT
geolocation_state,
ROUND(AVG(TIMESTAMP_DIFF(order_estimated_delivery_date, order_delivered_customer_date, DAY)),2) AS avg_estimated_Delivery_time
FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
JOIN `businesscase1sqlscaler.businesscasestudy1.customers` AS co
ON ord.customer_id = co.customer_id
JOIN `businesscase1sqlscaler.businesscasestudy1.geolocation` AS geo
ON co.customer_zip_code_prefix = geo.geolocation_zip_code_prefix
WHERE order_status = "delivered"
GROUP BY geolocation_state
ORDER BY avg_estimated_Delivery_time DESC
LIMIT 5;

-- 23. Month on Month count of orders for different payment types
SELECT
Month,
payment_type,
all_orders
FROM (
  SELECT COUNT(*) AS all_orders,
         EXTRACT(month FROM order_purchase_timestamp) AS Month,
         pay.payment_type
  FROM `businesscase1sqlscaler.businesscasestudy1.orders` AS ord
  JOIN `businesscase1sqlscaler.businesscasestudy1.payments` AS pay
  ON ord.order_id = pay.order_id
  GROUP BY Month, pay.payment_type
)
ORDER BY payment_type;

-- 24. Count of orders based on number of payment installments
SELECT
payment_installments,
COUNT(*) AS all_orders
FROM `businesscase1sqlscaler.businesscasestudy1.payments`
GROUP BY payment_installments;
"""
