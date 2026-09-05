SELECT l.auction,
       l.bidder,
       l.price,
       l.channel,
       l.url,
       l.`dateTime`,
       l.extra
FROM bid AS l
JOIN bid AS r
  ON l.auction = r.auction
 AND l.bidder = r.bidder
 AND l.price = r.price
 AND l.`dateTime` = r.`dateTime`
