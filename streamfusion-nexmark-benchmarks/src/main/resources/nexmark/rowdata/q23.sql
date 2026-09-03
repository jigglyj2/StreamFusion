SELECT B.bidder,
       B.price,
       B.channel,
       B.url,
       B.extra AS bid_extra,
       P.id AS person_id,
       P.name,
       P.emailAddress,
       P.creditCard,
       P.city,
       P.state,
       P.extra AS person_extra,
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
JOIN person AS P ON P.id = B.bidder
JOIN auction AS A ON A.seller = B.bidder
