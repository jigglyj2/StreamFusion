-- Derived from github.com/nexmark/nexmark query q1 (Apache License 2.0).
INSERT INTO nexmark_output
SELECT auction, bidder, CAST(0.908 * price AS BIGINT), `dateTime`, extra
FROM bid
