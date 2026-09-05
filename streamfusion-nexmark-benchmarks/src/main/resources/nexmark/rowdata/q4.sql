SELECT category, AVG(final_price) AS final
FROM (
    SELECT a.id, a.category, MAX(b.price) AS final_price
    FROM auction AS a
    JOIN bid AS b
      ON a.id = b.auction
     AND b.`dateTime` BETWEEN a.`dateTime` AND a.expires
    GROUP BY a.id, a.category
)
GROUP BY category
