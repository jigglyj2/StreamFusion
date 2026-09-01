SELECT bidder,
       COUNT(*) AS bid_count,
       window_start AS starttime,
       window_end AS endtime
FROM TABLE(
    SESSION(TABLE bid PARTITION BY bidder, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end
