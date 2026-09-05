SELECT auction_bids.auction, auction_bids.num
FROM (
    SELECT auction,
           COUNT(*) AS num,
           window_start AS starttime,
           window_end AS endtime
    FROM TABLE(
        HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
    GROUP BY auction, window_start, window_end
) AS auction_bids
JOIN (
    SELECT MAX(count_bids.num) AS maxn,
           count_bids.starttime,
           count_bids.endtime
    FROM (
        SELECT COUNT(*) AS num,
               window_start AS starttime,
               window_end AS endtime
        FROM TABLE(
            HOP(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '2' SECOND, INTERVAL '10' SECOND))
        GROUP BY auction, window_start, window_end
    ) AS count_bids
    GROUP BY count_bids.starttime, count_bids.endtime
) AS max_bids
ON auction_bids.starttime = max_bids.starttime
AND auction_bids.endtime = max_bids.endtime
AND auction_bids.num >= max_bids.maxn
