SELECT id, itemName, description, initialBid, reserve, `dateTime`, expires, seller, category,
       auction_extra, auction, bidder, price, bid_dateTime, bid_extra
FROM (
    SELECT a.id, a.itemName, a.description, a.initialBid, a.reserve, a.`dateTime`, a.expires,
           a.seller, a.category, a.extra AS auction_extra,
           b.auction, b.bidder, b.price, b.`dateTime` AS bid_dateTime, b.extra AS bid_extra,
           ROW_NUMBER() OVER (
               PARTITION BY a.id
               ORDER BY b.price DESC, b.`dateTime` ASC) AS row_num
    FROM auction AS a
    JOIN bid AS b
      ON a.id = b.auction
     AND b.`dateTime` BETWEEN a.`dateTime` AND a.expires
)
WHERE row_num <= 1
