SELECT bidder,
       auction,
       price,
       `dateTime`,
       SUM(price) OVER (
         PARTITION BY auction
         ORDER BY order_time
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_spend
FROM (
  SELECT bidder,
         auction,
         price,
         `dateTime`,
         CAST(`dateTime` AS TIMESTAMP(3)) AS order_time
  FROM bid)
