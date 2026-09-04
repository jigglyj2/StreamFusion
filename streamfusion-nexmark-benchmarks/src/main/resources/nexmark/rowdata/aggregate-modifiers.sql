SELECT bidder,
       COUNT(DISTINCT auction) AS distinct_auctions,
       COUNT(*) FILTER (WHERE price >= CAST(1000000 AS BIGINT)) AS expensive_bids,
       SUM(DISTINCT price) FILTER (WHERE price >= CAST(1000000 AS BIGINT)) AS distinct_expensive_spend,
       MIN(price) FILTER (WHERE price >= CAST(1000000 AS BIGINT)) AS minimum_expensive_price,
       MAX(price) FILTER (WHERE price >= CAST(1000000 AS BIGINT)) AS maximum_expensive_price
FROM bid
GROUP BY bidder
