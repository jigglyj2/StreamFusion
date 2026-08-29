SELECT auction,
       bidder,
       price,
       channel,
       SPLIT_INDEX(url, '/', 3) AS dir1,
       SPLIT_INDEX(url, '/', 4) AS dir2,
       SPLIT_INDEX(url, '/', 5) AS dir3
FROM bid
