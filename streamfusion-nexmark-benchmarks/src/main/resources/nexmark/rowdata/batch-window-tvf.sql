SELECT auction,
       bidder,
       price,
       `dateTime`,
       window_start,
       window_end,
       window_time
FROM TABLE(
    TUMBLE(TABLE bid, DESCRIPTOR(`dateTime`), INTERVAL '10' SECOND))
