SELECT people.id, people.name, people.starttime
FROM (
    SELECT id, name, window_start AS starttime, window_end AS endtime
    FROM TABLE(TUMBLE(TABLE person, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
    GROUP BY id, name, window_start, window_end
) people
JOIN (
    SELECT seller, window_start AS starttime, window_end AS endtime
    FROM TABLE(TUMBLE(TABLE auction, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
    GROUP BY seller, window_start, window_end
) auctions
ON people.id = auctions.seller
AND people.starttime = auctions.starttime
AND people.endtime = auctions.endtime
