SELECT bidder,
       price,
       channel,
       url,
       B.extra    AS bid_extra,

       P.id       AS person_id,
       name,
       emailAddress,
       creditCard,
       city,
       state,
       P.extra    AS person_extra,
       itemName,

       description,
       initialBid,
       reserve,
       A.`dateTime` AS auction_dateTime,
       expires,
       seller,
       category,
       A.extra    AS auction_extra
FROM bid B
         JOIN
     person P ON P.id = B.bidder
         JOIN
     auction A ON A.seller = B.bidder;
