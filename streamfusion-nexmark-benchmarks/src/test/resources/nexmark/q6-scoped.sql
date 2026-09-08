-- Nexmark Q6 with aliases/filter scopes corrected; join, ranking and frame are unchanged.
-- Keep the upstream 10 PRECEDING boundary rather than silently changing the benchmark.
SELECT seller,
       AVG(price) OVER (
           PARTITION BY seller ORDER BY `dateTime`
           ROWS BETWEEN 10 PRECEDING AND CURRENT ROW) AS avg_price
FROM (
    SELECT a.id, a.seller, b.price, b.`dateTime`,
           ROW_NUMBER() OVER (
               PARTITION BY a.id, a.seller ORDER BY b.price DESC) AS rownum
    FROM auction AS a
    JOIN bid AS b
      ON a.id = b.auction
     AND b.`dateTime` BETWEEN a.`dateTime` AND a.expires
) AS winning
WHERE rownum <= 1
