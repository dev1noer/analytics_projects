-- Selecting all columns and adding lag functions to get last year's data
SELECT *,
    LAG(sales_date) OVER(PARTITION BY sales_channel, week_day_id ORDER BY sales_date) AS prev_year_date,
    LAG(total_revenue) OVER(PARTITION BY sales_channel, week_day_id ORDER BY sales_date) AS prev_year_revenue,
    LAG(total_orders) OVER(PARTITION BY sales_channel, week_day_id ORDER BY sales_date) AS prev_year_orders,
    LAG(total_quantity) OVER(PARTITION BY sales_channel, week_day_id ORDER BY sales_date) AS prev_year_quantity
FROM (
    -- Main data selection
    SELECT
        sales_date,
        -- Assigning a channel name based on conditions
        CASE
            WHEN customer_id = 1001 THEN 'OnlineA'
            WHEN customer_id = 1002 THEN 'OnlineB'
            WHEN customer_id = 1003 THEN 'OnlineC'
            WHEN unit_id = 2001 AND economic_unit = 3001 AND payment_type = 4001 AND sales_date >= '2022-01-19' AND store_number = 5001 THEN 'OnlineC'
            ELSE 'Unknown'
        END AS sales_channel,
        -- Extracting day of the week
        EXTRACT(DOW FROM sales_date) AS day_of_week,
        -- Extracting week number
        EXTRACT(WEEK FROM sales_date) AS week_num,
        -- Creating a week-day identifier
        CAST(week_num AS VARCHAR(3)) || day_of_week AS week_day_id,
        -- Counting sale and return transactions
        COUNT(DISTINCT CASE WHEN transaction_type = 'sale' THEN transaction_id END) AS total_sales_orders,
        COUNT(DISTINCT CASE WHEN transaction_type = 'return' THEN transaction_id END) AS total_return_orders,
        -- Cleaning orders by subtracting returns
        total_sales_orders - total_return_orders AS net_orders,
        -- Total orders count
        COUNT(DISTINCT transaction_id) AS total_orders,
        -- Summing item quantities, excluding a specific item
        SUM(CASE WHEN item_id = 9001 THEN 0 ELSE quantity END) AS total_quantity,
        -- Summing revenue amount
        SUM(revenue) AS total_revenue
    FROM (
        -- Extracting transaction details from the last two years
        SELECT transaction_id, DATE_TRUNC('DAY', authorized_date) AS sales_date, item_id, unit_id, sales_type, customer_id, serial_number, store_number, SUM(revenue) AS revenue, SUM(quantity) AS quantity
        FROM sales.transaction_details
        WHERE country_id = 87
        AND authorized_date >= CURRENT_DATE - INTERVAL '26 MONTHS'
        GROUP BY 1,2,3,4,5,6,7,8
    ) a
    LEFT JOIN (
        -- Extracting business unit details
        SELECT transaction_id, economic_unit
        FROM sales.delivery_details
        WHERE country_id = 87
        AND authorized_date >= CURRENT_DATE - INTERVAL '26 MONTHS'
        GROUP BY 1,2
    ) b ON a.transaction_id = b.transaction_id
    LEFT JOIN (
        -- Extracting payment details
        SELECT transaction_id, payment_type
        FROM sales.payment_details
        WHERE currency_id = 40
        AND transaction_date >= CURRENT_DATE - INTERVAL '26 MONTHS'
        AND payment_type IN (1001,1002)
        GROUP BY 1,2
    ) c ON a.transaction_id = c.transaction_id
    LEFT JOIN (
        -- Extracting transaction type details
        SELECT transaction_id, transaction_type
        FROM sales.transaction_summary
        WHERE country_id = 87
        AND authorized_date >= CURRENT_DATE - INTERVAL '26 MONTHS'
        GROUP BY 1,2
    ) d ON a.transaction_id = d.transaction_id
    GROUP BY 1,2,3,4
) aa
-- Filtering data for the last 15 months
WHERE sales_date >= CURRENT_DATE - INTERVAL '15 MONTHS'
ORDER BY sales_date DESC;
