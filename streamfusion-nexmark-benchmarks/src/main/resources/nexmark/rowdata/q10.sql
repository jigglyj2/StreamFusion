SELECT auction, bidder, price, `dateTime`, extra, DATE_FORMAT(`dateTime`, 'yyyy-MM-dd'), DATE_FORMAT(`dateTime`, 'HH:mm')
FROM bid;
