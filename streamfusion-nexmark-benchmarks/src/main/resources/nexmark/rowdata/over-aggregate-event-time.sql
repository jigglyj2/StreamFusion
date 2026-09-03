SELECT bidder,
       auction,
       price,
       `dateTime`,
       SUM(price) OVER (
         PARTITION BY auction
         ORDER BY `dateTime`
         ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_spend
FROM bid
