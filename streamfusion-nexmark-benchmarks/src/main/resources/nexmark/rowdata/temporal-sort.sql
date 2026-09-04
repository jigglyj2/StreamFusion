SELECT auction, bidder, price, `dateTime`, extra
FROM bid
ORDER BY `dateTime` ASC, price DESC, auction ASC, bidder ASC, extra ASC
