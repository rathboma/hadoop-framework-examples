SELECT 
  productId, 
  count(distinct loc)
FROM
  transactions t
LEFT OUTER JOIN 
  users u on t.userId = u.id
GROUP BY productId;
