SELECT DISTINCT customer.*
FROM customer
JOIN accelerometer
ON customer.email = accelerometer.user