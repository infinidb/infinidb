-- These are called out as "key scenarios" in the alter table use case

-- Rename table
alter table t1 rename t2;

-- Change a column's name and type
alter table t2 modify a tinyint not null, change b c char(20);

-- Add a new column
alter table t2 add d datetime;

-- Add indexes
alter table t2 add index (d), add index (a);

-- Remove a column
alter table t2 drop column c;
