SELECT bidder,
       channel,
       COUNT(*) AS bids,
       SUM(price) AS spend
FROM bid
GROUP BY GROUPING SETS ((bidder), (channel))
