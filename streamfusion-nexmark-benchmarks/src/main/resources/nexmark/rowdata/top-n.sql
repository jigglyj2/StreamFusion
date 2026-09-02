SELECT auction, bidder, price, `dateTime`, extra, row_num
FROM (
    SELECT auction,
           bidder,
           price,
           `dateTime`,
           extra,
           ROW_NUMBER() OVER (
               PARTITION BY bidder
               ORDER BY price DESC, `dateTime` DESC, auction ASC) AS row_num
    FROM bid
)
WHERE row_num <= 10
