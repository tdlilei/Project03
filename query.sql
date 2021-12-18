WITH t1 AS (
    SELECT name, high, ts, SUBSTRING(ts, 12, 2) AS hour  FROM "05"),
    t2 AS (SELECT name, SUBSTRING(ts, 12, 2) AS hour, MAX(high) AS max_high FROM "05" GROUP BY name, SUBSTRING(ts, 12, 2))

SELECT t1.name AS Ticker_Name, t1.hour AS Hour, t1.ts AS Datetime, t2.max_high AS Max_High
FROM t1
INNER JOIN t2
ON t1.name = t2.name AND t1.hour = t2.hour AND t1.high = t2.max_high
ORDER BY t1.name, t1.hour