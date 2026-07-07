SELECT *
FROM step
JOIN accel
ON step.sensorReadingTime = accel.timestamp