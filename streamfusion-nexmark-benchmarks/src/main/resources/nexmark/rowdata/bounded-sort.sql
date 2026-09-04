SELECT auction, bidder, price, `dateTime`, extra
FROM bid
ORDER BY price DESC, auction ASC, bidder ASC, `dateTime` ASC, extra ASC
