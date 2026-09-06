SELECT bidder,
       spend,
       COUNT(*) AS grouped_rows
FROM (
    SELECT bidder,
           SUM(price) AS spend
    FROM bid
    GROUP BY bidder
) AS bidder_spend
GROUP BY bidder, spend
