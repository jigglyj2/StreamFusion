SELECT auction, bidder, price
FROM bid
WHERE MOD(auction, 4) = 0
INTERSECT ALL
SELECT auction, bidder, price
FROM bid
WHERE MOD(auction, 6) = 0
