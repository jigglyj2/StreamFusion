SELECT auction, bidder, price, `dateTime`, extra, rank_number
FROM (
  SELECT auction, bidder, price, `dateTime`, extra,
    RANK() OVER (PARTITION BY bidder ORDER BY price DESC) AS rank_number
  FROM bid
)
WHERE rank_number BETWEEN 2 AND 10
