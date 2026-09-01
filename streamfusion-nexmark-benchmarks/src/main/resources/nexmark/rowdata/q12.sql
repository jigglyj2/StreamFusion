SELECT bidder,
       COUNT(*) AS bid_count,
       window_start AS starttime,
       window_end AS endtime
FROM TABLE(
    TUMBLE(
        TABLE bid_with_proc_time,
        DESCRIPTOR(p_time),
        INTERVAL '10' SECOND))
GROUP BY bidder, window_start, window_end
