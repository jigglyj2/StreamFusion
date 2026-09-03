SELECT a.id AS auction, b.bidder, b.price, b.`dateTime` AS bid_time
FROM auction AS a
JOIN bid AS b
  ON a.id = b.auction
 AND b.`dateTime` BETWEEN a.`dateTime` AND a.`dateTime` + INTERVAL '10' SECOND
