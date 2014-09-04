create database if not exists oamtest;
use oamtest

-- Create a table with a tinyint.
create table if not exists tmp(c1 tinyint)engine=infinidb;

-- Insert
insert into tmp values (1);

-- Select
select * from tmp;

-- Trucate and drop table
truncate table tmp;
drop table tmp;

-- NEEDED for good status return
select "OK";
