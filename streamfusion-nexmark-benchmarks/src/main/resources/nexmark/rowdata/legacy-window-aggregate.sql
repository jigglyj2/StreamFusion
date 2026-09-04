SELECT bidder,
       COUNT(*) AS bid_count,
       SUM(price) AS spend,
       AVG(price) AS average_price,
       MIN(price) AS minimum_price,
       MAX(price) AS maximum_price,
       TUMBLE_START(`dateTime`, INTERVAL '10' SECOND) AS starttime,
       TUMBLE_END(`dateTime`, INTERVAL '10' SECOND) AS endtime
FROM bid
GROUP BY bidder, TUMBLE(`dateTime`, INTERVAL '10' SECOND)
