SELECT auction, bidder, price, `dateTime`, extra
FROM bid
LIMIT 1000 OFFSET 50
