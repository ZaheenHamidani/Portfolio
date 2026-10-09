-- Run after the full demo. All money values use integer CAD cents.
SELECT json_extract(payload, '$.business_date') AS business_date,
       COUNT(DISTINCT json_extract(payload, '$.order_id')) AS completed_orders,
       SUM(json_extract(payload, '$.quantity')) AS items_sold,
       SUM(json_extract(payload, '$.total_cents')) / 100.0 AS sales_cad
FROM orders
GROUP BY business_date
ORDER BY business_date;

SELECT json_extract(payload, '$.business_date') AS business_date,
       SUM(json_extract(payload, '$.paid_minutes')) / 60.0 AS paid_hours
FROM shifts
GROUP BY business_date
ORDER BY business_date;
