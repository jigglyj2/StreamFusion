SELECT auction, bidder, price, channel, url, `dateTime`, extra, rank_number
FROM (
    SELECT bid.*,
           ROW_NUMBER() OVER (
               PARTITION BY auction
               ORDER BY price DESC,
                        `dateTime` ASC,
                        bidder ASC,
                        channel ASC,
                        url ASC,
                        extra ASC) AS rank_number
    FROM bid
)
WHERE rank_number <= 10
