SELECT auction, expanded_value
FROM bid
CROSS JOIN UNNEST(ARRAY[auction, bidder, price]) AS expanded(expanded_value)
