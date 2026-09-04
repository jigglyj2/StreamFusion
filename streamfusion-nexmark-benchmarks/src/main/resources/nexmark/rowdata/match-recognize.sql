SELECT bidder, first_auction, second_auction, third_auction
FROM bid_with_proc_time
MATCH_RECOGNIZE (
    PARTITION BY bidder
    ORDER BY p_time
    MEASURES A.auction AS first_auction,
             B.auction AS second_auction,
             C.auction AS third_auction
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B C)
    DEFINE A AS auction IS NOT NULL,
           B AS auction IS NOT NULL,
           C AS auction IS NOT NULL
)
