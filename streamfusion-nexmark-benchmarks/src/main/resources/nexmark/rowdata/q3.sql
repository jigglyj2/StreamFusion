SELECT p.name, p.city, p.state, a.id
FROM auction AS a
JOIN person AS p ON a.seller = p.id
WHERE a.category = 10
  AND (p.state = 'OR' OR p.state = 'ID' OR p.state = 'CA')
