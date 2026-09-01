SELECT bidder,
       COUNT(*) AS bids,
       SUM(price) AS spend,
       MIN(price) AS minimum_price,
       MAX(price) AS maximum_price
FROM bid
GROUP BY bidder
