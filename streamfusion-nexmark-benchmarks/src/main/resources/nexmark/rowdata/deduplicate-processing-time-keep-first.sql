SELECT auction, bidder, price, channel, url, `dateTime`, extra
FROM (
    SELECT bid_with_proc_time.*,
           ROW_NUMBER() OVER (
               PARTITION BY bidder, auction
               ORDER BY p_time ASC) AS rank_number
    FROM bid_with_proc_time
)
WHERE rank_number <= 1
