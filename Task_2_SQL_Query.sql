SELECT 
  cou.region,
  YEAR(ord.exec_date) AS H1_In_Year,
  SUM(ordItm.amount_total / exRate.rate) AS Sales_calculated_to_Usd,
  SUM(CASE WHEN ord.currency = 'EUR' THEN ordItm.amount_total ELSE 0.00 END) AS Sales_in_EUR,
  SUM(CASE WHEN ord.currency = 'GBP' THEN ordItm.amount_total ELSE 0.00 END) AS Sales_in_GBP,
  SUM(CASE WHEN ord.currency = 'USD' THEN ordItm.amount_total ELSE 0.00 END) AS Sales_in_USD,
  SUM(CASE WHEN ord.currency = 'EUR' THEN ordItm.amount_total * exRate.rate END)/SUM(CASE WHEN ord.currency = 'EUR' THEN ordItm.amount_total END) AS Moving_AVG_EUR,
  SUM(CASE WHEN ord.currency = 'GBP' THEN ordItm.amount_total * exRate.rate END)/SUM(CASE WHEN ord.currency = 'GBP' THEN ordItm.amount_total END) AS Moving_AVG_GBP

INTO dbsix.dbo.task_2_results

FROM bi.sales.Orders ord
JOIN bi.sales.OrderItems ordItm ON ord.id = ordItm.order_id
JOIN bi.sales.Customer cust ON ord.customer = cust.id
JOIN bi.sales.Country cou ON cust.country_id = cou.id
JOIN bi.sales.ExchangeRate exRate ON ord.exec_date = exRate.date AND ord.currency = exRate.currency
/*JOIN bi.sales.Product prod ON ordItm.product_id = prod.product_id*/

WHERE ord.is_paid = 1 -- Only paid orders, exclude pre-orders
  AND YEAR(ord.exec_date) IN (2018, 2019) -- Year 2018, 2019
  AND MONTH(ord.exec_date) BETWEEN 1 AND 6 -- H1
GROUP BY cou.region,YEAR(ord.exec_date)
ORDER BY 1
;
