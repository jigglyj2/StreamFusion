SELECT bidder,
       COUNT(*) AS bid_count,
       MIN(channel) AS minimum_channel,
       MAX(url) AS maximum_url,
       TUMBLE_START(`dateTime`, INTERVAL '10' SECOND) AS starttime,
       TUMBLE_END(`dateTime`, INTERVAL '10' SECOND) AS endtime
FROM bid
GROUP BY bidder, TUMBLE(`dateTime`, INTERVAL '10' SECOND)
