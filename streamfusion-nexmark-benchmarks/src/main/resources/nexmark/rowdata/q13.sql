SELECT
    B.auction,
    B.bidder,
    B.price,
    B.`dateTime`,
    S.`value`
FROM (SELECT *, PROCTIME() as p_time FROM bid) B
JOIN side_input FOR SYSTEM_TIME AS OF B.p_time AS S
ON mod(B.auction, 10000) = S.key
