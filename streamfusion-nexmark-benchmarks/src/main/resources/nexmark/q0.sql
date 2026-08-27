-- Derived from github.com/nexmark/nexmark query q0 (Apache License 2.0).
INSERT INTO nexmark_output
SELECT auction, bidder, price, `dateTime`, extra
FROM bid
