-- $Id: tpch15.sql 2657 2007-06-12 16:08:15Z rdempsey $
-- TPC-H/TPC-R Top Supplier Query (Q15)
-- Functional Query Definition
-- Approved February 1998

define 1 = 1996-01-01
define s = 1

create view revenue&s (supplier_no, total_revenue) as
	select
		l_suppkey,
		sum(l_extendedprice * (1 - l_discount))
	from
		lineitem
	where
		l_shipdate >= date '&1'
		and l_shipdate < date '&1' + interval '3' month
	group by
		l_suppkey;

select
	s_suppkey,
	s_name,
	s_address,
	s_phone,
	total_revenue
from
	supplier,
	revenue&s
where
	s_suppkey = supplier_no
	and total_revenue = (
		select
			max(total_revenue)
		from
			revenue&s
	)
order by
	s_suppkey;

drop view revenue&s;

