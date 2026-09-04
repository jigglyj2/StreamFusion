SELECT auction, bidder, price, channel, url, `dateTime`, extra
FROM (
    SELECT bid.*,
           ROW_NUMBER() OVER (
               PARTITION BY bidder, auction
               ORDER BY `dateTime` DESC) AS rank_number
    FROM bid
)
WHERE rank_number <= 1
