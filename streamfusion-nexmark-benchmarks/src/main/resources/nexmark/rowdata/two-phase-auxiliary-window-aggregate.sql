SELECT bidder,
       spend,
       COUNT(*) AS grouped_rows,
       TUMBLE_START(`dateTime`, INTERVAL '10' SECOND) AS starttime,
       TUMBLE_END(`dateTime`, INTERVAL '10' SECOND) AS endtime
FROM (
  SELECT bidder, `dateTime`, SUM(price) AS spend
  FROM bid
  GROUP BY bidder, `dateTime`
) AS bidder_spend
GROUP BY bidder, spend, TUMBLE(`dateTime`, INTERVAL '10' SECOND)
