SELECT bidder,
       auction,
       price,
       `dateTime`,
       SUM(price) OVER (
         PARTITION BY auction
         ORDER BY price
         RANGE BETWEEN 1000000 PRECEDING AND CURRENT ROW) AS running_spend
FROM bid
