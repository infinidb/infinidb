-- $Id: tpch16.sql 3647 2007-12-13 21:20:41Z rdempsey $
-- TPC-H/TPC-R Parts/Supplier Relationship Query (Q16)
-- Functional Query Definition
-- Approved February 1998

define 1 = Brand#45
define 2 = 'MEDIUM POLISHED'
define 3 = 49
define 4 = 14
define 5 = 23
define 6 = 45
define 7 = 19
define 8 = 3
define 9 = 36
define 10 = 9

select
	p_brand,
	p_type,
	p_size,
	count(distinct ps_suppkey) as supplier_cnt
from
	partsupp,
	part
where
	p_partkey = ps_partkey
	and p_brand <> '&1'
	and p_type not like '&2%'
	and p_size in (&3, &4, &5, &6, &7, &8, &9, &10)
	and ps_suppkey not in (
		select
			s_suppkey
		from
			supplier
		where
			s_comment like '%Customer%Complaints%'
	)
group by
	p_brand,
	p_type,
	p_size
order by
	supplier_cnt desc,
	p_brand,
	p_type,
	p_size;

