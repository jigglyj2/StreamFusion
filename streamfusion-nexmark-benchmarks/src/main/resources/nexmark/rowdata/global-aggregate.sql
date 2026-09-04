SELECT COUNT(*) AS bids,
       COUNT(price) AS prices,
       SUM(price) AS spend,
       MIN(bidder) AS minimum_bidder,
       MAX(auction) AS maximum_auction
FROM bid
