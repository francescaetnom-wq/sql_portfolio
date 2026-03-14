--- subqueries
SELECT first_name, last_name
FROM bigquery-public-data.thelook_ecommerce.users
WHERE id IN (
  SELECT user_id
  FROM bigquery-public-data.thelook_ecommerce.orders
  WHERE status = 'Returned'
);

---- case when + subqueries
SELECT first_name, last_name,
CASE
WHEN age < 20 THEN 'Young'
    WHEN age BETWEEN 20 AND 35 THEN 'Adult'
    WHEN age BETWEEN 36 AND 55 THEN 'Senior'
    ELSE 'Old'
  END AS age_group
FROM bigquery-public-data.thelook_ecommerce.users
WHERE id IN (
  SELECT user_id
  FROM bigquery-public-data.thelook_ecommerce.orders
  WHERE status = 'Returned'
)
ORDER BY age DESC
LIMIT 1000;

---- subq + having and group by
SELECT country, AVG(total_orders) AS media_ordini
FROM (
    SELECT u.country, COUNT(o.order_id) AS total_orders
    FROM bigquery-public-data.thelook_ecommerce.users AS u
    LEFT JOIN bigquery-public-data.thelook_ecommerce.orders AS o ON u.id = o.user_id
    GROUP BY u.country
) AS ordini_per_paese
GROUP BY country
HAVING AVG(total_orders) > 3
ORDER BY media_ordini DESC;

---- case when + group by
SELECT 
CASE 
 WHEN num_of_item = 1 THEN 'Small'
 WHEN num_of_item BETWEEN 2 AND 3 THEN 'Medium'
 WHEN num_of_item >= 4 THEN 'Big'
END AS segment,
COUNT(*) AS total_orders, ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER(), 2) AS percentage
FROM bigquery-public-data.thelook_ecommerce.orders
GROUP BY segment
ORDER BY total_orders DESC;

---- subquery
SELECT country, first_name, last_name
FROM bigquery-public-data.thelook_ecommerce.users
WHERE id IN 
(SELECT user_id
FROM bigquery-public-data.thelook_ecommerce.orders
WHERE status = 'Cancelled')
ORDER BY country; 

---- count distinct + case when + group by
SELECT country,
COUNT(DISTINCT id) AS user_total,
COUNT(DISTINCT CASE WHEN gender= 'F' THEN id END) AS female_total,
COUNT(DISTINCT CASE WHEN gender= 'M' THEN id END) AS male_total
FROM bigquery-public-data.thelook_ecommerce.users
GROUP BY country
ORDER BY user_total DESC;

---- case when and group by
SELECT 
CASE WHEN age < 25 THEN 'Under 25'
WHEN age BETWEEN 25 AND 40 THEN 'Middle age'
WHEN age > 40 THEN 'Over 40'
END AS age_group,
COUNT(*) AS total_users
FROM bigquery-public-data.thelook_ecommerce.users
GROUP BY age_group
ORDER BY total_users DESC; 

---- basic functions
SELECT CONCAT(first_name,' ', last_name) AS full_name,
LOWER(email) AS email_lowered,
DATE_DIFF(CURRENT_DATE(), DATE(created_at), DAY) AS days_from_subs
FROM `bigquery-public-data.thelook_ecommerce.users`;


--- functions + count and group by
SELECT 
DATE_TRUNC(DATE(created_at),MONTH) AS month,
COUNT(order_id) AS num_order_per_month
FROM bigquery-public-data.thelook_ecommerce.orders
GROUP BY month ORDER BY month ASC;

---- date_sub
SELECT *
FROM `bigquery-public-data.thelook_ecommerce.orders`
WHERE DATE(created_at) >= DATE_SUB(CURRENT_DATE(), INTERVAL 2 YEAR);


---- extract + count + group by 
SELECT 
EXTRACT (DAYOFWEEK FROM created_at) AS day_of_week,
COUNT(order_id) AS total_orders
FROM bigquery-public-data.thelook_ecommerce.orders
GROUP BY day_of_week
ORDER BY total_orders DESC;

---- case when 
SELECT order_id, num_of_item, 
CASE WHEN num_of_item = 1 THEN 'Small'
WHEN num_of_item BETWEEN 2 AND 3 THEN 'Medium'
WHEN num_of_item >= 4 THEN 'Large'
END AS order_size
FROM bigquery-public-data.thelook_ecommerce.orders
LIMIT 50;

---- count + case when + group by 
 SELECT
    country,
    COUNT(*) AS total_utenti,
    COUNT(CASE WHEN age > 40 THEN id END) AS over_40,
    COUNT(CASE WHEN age < 25 THEN id END) AS under_25
FROM `bigquery-public-data.thelook_ecommerce.users`
GROUP BY country
ORDER BY total_utenti DESC
LIMIT 10;

----case when + count + group by
SELECT 
CASE WHEN status IN ('Complete', 'Shipped') THEN 'Positive'
WHEN status IN ('Cancelled', 'Returned' ) THEN 'Negative'
ELSE 'In progress'
END AS status_category,
COUNT(*) AS total_orders
FROM bigquery-public-data.thelook_ecommerce.orders
GROUP BY status_category
ORDER BY total_orders desc;

---- basic functions
SELECT CONCAT(first_name, ' ' , last_name) AS full_name,
country,
DATE_DIFF(CURRENT_DATE(),DATE(created_at),DAY) AS days_from_registration
FROM bigquery-public-data.thelook_ecommerce.users
WHERE EXTRACT(YEAR FROM created_at) = 2022
ORDER BY days_from_registration ASC;

---- basic functions
SELECT 
    DATE_TRUNC(DATE(created_at), MONTH) AS month,
    COUNT(*) AS total_registrations
    FROM bigquery-public-data.thelook_ecommerce.users
WHERE EXTRACT(YEAR FROM created_at) = 2022
GROUP BY month
ORDER BY month;

---- case when + count + group by
SELECT
    status,
    COUNT(*) AS total_orders,
    COUNT(CASE WHEN num_of_item = 1 THEN order_id END) AS single_item,
    COUNT(CASE WHEN num_of_item >= 4 THEN order_id END) AS bulk
FROM `bigquery-public-data.thelook_ecommerce.orders`
GROUP BY status
ORDER BY total_orders DESC;

----cte 
WITH order_status AS (
  SELECT user_id 
  FROM bigquery-public-data.thelook_ecommerce.orders 
  WHERE status = 'Returned'
)
SELECT first_name, country
FROM bigquery-public-data.thelook_ecommerce.users
WHERE id IN (SELECT user_id FROM order_status);

---- CTE + join and group by
WITH tot_order_per_user AS(
  SELECT user_id, COUNT(order_id) AS total_orders
  FROM `bigquery-public-data.thelook_ecommerce.orders`
  GROUP BY user_id
)
SELECT u.first_name, u.country, t.total_orders
FROM bigquery-public-data.thelook_ecommerce.users AS u
JOIN tot_order_per_user AS t
ON u.id = t.user_id
WHERE total_orders > 2
ORDER BY t.total_orders DESC;

----multiple cte + join 
WITH users_from_china AS(
  SELECT id 
  FROM bigquery-public-data.thelook_ecommerce.users
  WHERE country = 'China'
),
orders_complete AS(
  SELECT order_id, user_id
  FROM bigquery-public-data.thelook_ecommerce.orders
  WHERE status = 'Complete'
)
SELECT u.first_name, u.country, o.order_id
FROM bigquery-public-data.thelook_ecommerce.users AS u
INNER JOIN orders_complete AS o ON u.id = o.user_id
WHERE u.id IN (SELECT id FROM users_from_china)
LIMIT 50;

---- multiple cte + left join and group by 
WITH stats_per_country AS (
    SELECT
        u.country,
        COUNT(DISTINCT u.id)    AS total_users,
        COUNT(DISTINCT o.order_id) AS total_orders,
        AVG(o.num_of_item)      AS avg_items
    FROM `bigquery-public-data.thelook_ecommerce.users` AS u
    LEFT JOIN `bigquery-public-data.thelook_ecommerce.orders` AS o
        ON u.id = o.user_id
    GROUP BY u.country
),
global_avg AS (
    SELECT AVG(num_of_item) AS avg_globale
    FROM `bigquery-public-data.thelook_ecommerce.orders`
)
SELECT
    s.country,
    s.total_users,
    s.total_orders,
    s.avg_items
FROM stats_per_country AS s
WHERE s.avg_items > (SELECT avg_globale FROM global_avg)
ORDER BY s.total_orders DESC;

----WINDOW FUNCTION 1 ROW_NUBER
SELECT
    first_name,
    country,
    created_at,
    ROW_NUMBER() OVER(PARTITION BY country ORDER BY created_at) AS rn
FROM `bigquery-public-data.thelook_ecommerce.users`;

----WINDOW FUNCTION 2 AVG OVER()
SELECT order_id, user_id, num_of_item,
   ROUND(AVG(num_of_item) OVER(),2) AS global_avg
    FROM bigquery-public-data.thelook_ecommerce.orders
LIMIT 50;

----WINDOW FUNCTION 3 dense_rank

SELECT first_name, country, age, DENSE_RANK() OVER (PARTITION BY country ORDER BY age DESC) AS rank
FROM bigquery-public-data.thelook_ecommerce.users
ORDER BY country, rank
LIMIT 100;


---- cte + join + window function
WITH
  country_orders AS (
    SELECT u.country, COUNT(o.order_id) AS total_orders
    FROM `bigquery-public-data`.`thelook_ecommerce`.`orders` AS o
    INNER JOIN `bigquery-public-data`.`thelook_ecommerce`.`users` AS u
      ON o.user_id = u.id
    GROUP BY u.country
  )
SELECT
  country,
  total_orders,
  RANK() OVER (ORDER BY total_orders DESC) AS country_rank
FROM country_orders
ORDER BY country_rank
LIMIT 5;
