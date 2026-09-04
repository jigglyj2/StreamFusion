SELECT bidder,
       auction,
       price,
       `dateTime`,
       SUM(price) OVER (
         PARTITION BY auction
         ORDER BY `dateTime`
         RANGE BETWEEN INTERVAL '10' SECOND PRECEDING AND CURRENT ROW) AS running_spend
FROM bid
