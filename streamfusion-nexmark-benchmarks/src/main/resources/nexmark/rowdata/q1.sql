SELECT auction,
       bidder,
       0.908 * price AS price,
       `dateTime`,
       extra
FROM bid
