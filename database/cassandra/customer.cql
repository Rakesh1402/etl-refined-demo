CREATE TABLE refined_zone.customer (
    customer_id text,
    customer_uid text,
    fname text,
    lname text,
    address text,
    zipcode_prefix text,
    city text,
    country text,
   PRIMARY KEY (customer_uid, zipcode_prefix));
