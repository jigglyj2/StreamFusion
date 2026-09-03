SELECT B.auction,
       B.bidder,
       B.price,
       B.channel,
       B.url,
       B.`dateTime` AS bid_dateTime,
       B.extra AS bid_extra,
       A.itemName,
       A.description,
       A.initialBid,
       A.reserve,
       A.`dateTime` AS auction_dateTime,
       A.expires,
       A.seller,
       A.category,
       A.extra AS auction_extra
FROM bid AS B
JOIN auction AS A ON B.auction = A.id
WHERE A.category = 10
