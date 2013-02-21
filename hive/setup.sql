CREATE EXTERNAL TABLE users(
  id INT, 
  email STRING, 
  language STRING, 
  loc STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
LOCATION '/data/users';


CREATE EXTERNAL TABLE transactions(
  id INT, 
  productId INT, 
  userId INT, 
  purchaseAmount INT, 
  itemDescription STRING
)
ROW FORMAT DELIMITED
  FIELDS TERMINATED BY '\t'
LOCATION '/data/transactions';