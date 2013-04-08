
USERS = load '$USERS' using PigStorage('\t') 
  as (id:int, email:chararray, language:chararray, location:chararray);

TRANSACTIONS = load '$TRANSACTIONS' using PigStorage('\t') 
  as (id:int, product:int, user:int, purchase_amount:double, description:chararray);

A = JOIN TRANSACTIONS by user LEFT OUTER, USERS by id;

B = GROUP A by product;

C = FOREACH B {
  LOCS = DISTINCT A.location;
  GENERATE group, COUNT(LOCS) as location_count;
};

STORE C INTO '$OUTPUT';
