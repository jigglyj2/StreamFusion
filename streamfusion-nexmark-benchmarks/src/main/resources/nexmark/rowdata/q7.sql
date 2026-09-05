SELECT b.auction, b.bidder, b.price, b.`dateTime`, b.extra
FROM bid AS b
JOIN (
    SELECT MAX(price) AS maxprice, window_end AS `dateTime`
    FROM TABLE(
        TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
    GROUP BY window_start, window_end
) AS b1
ON b.price = b1.maxprice
WHERE b.`dateTime` BETWEEN b1.`dateTime` - INTERVAL '10' SECOND AND b1.`dateTime`
