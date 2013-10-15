-- Create table with schema, table option and primary key as column constraint
CREATE TABLE calpont.PART_956
  (p_17c char(12) default '3',
   p_29 numeric(5,2) primary key,
   p_30 double default 3.14159,
   p_31 double precision default 123.456,
   p_32 date,
   p_33 blob,
   p_34 clob,
   p_35 number,
   check(fooby),
   check(dooby))
