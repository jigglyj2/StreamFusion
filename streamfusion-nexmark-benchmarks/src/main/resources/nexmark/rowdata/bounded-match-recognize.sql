SELECT bidder, first_bidder, second_bidder, third_bidder
FROM bid_with_proc_time
MATCH_RECOGNIZE (
    PARTITION BY bidder
    ORDER BY p_time
    MEASURES A.bidder AS first_bidder,
             B.bidder AS second_bidder,
             C.bidder AS third_bidder
    ONE ROW PER MATCH
    AFTER MATCH SKIP PAST LAST ROW
    PATTERN (A B C)
    DEFINE A AS auction IS NOT NULL,
           B AS auction IS NOT NULL,
           C AS auction IS NOT NULL
)
