SELECT b.auction,
       b.bidder,
       b.price,
       b.`dateTime` AS bid_time,
       a.seller,
       a.category
FROM bid AS b
LEFT JOIN versioned_auction FOR SYSTEM_TIME AS OF b.`dateTime` AS a
ON b.auction = a.id AND a.category >= 0
