SELECT bidder,
       COUNT(*) AS bid_count,
       SUM(price) AS spend,
       AVG(price) AS average_price,
       MIN(channel) AS minimum_channel,
       MAX(url) AS maximum_url,
       HOP_START(`dateTime`, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS starttime,
       HOP_END(`dateTime`, INTERVAL '2' SECOND, INTERVAL '10' SECOND) AS endtime
FROM bid
GROUP BY bidder, HOP(`dateTime`, INTERVAL '2' SECOND, INTERVAL '10' SECOND)
