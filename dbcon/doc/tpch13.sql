-- $Id: tpch13.sql 2657 2007-06-12 16:08:15Z rdempsey $
-- TPC-H/TPC-R Customer Distribution Query (Q13)
-- Functional Query Definition
-- Approved February 1998

define 1 = special
define 2 = requests

select
	c_count,
	count(*) as custdist
from
	(
		select
			c_custkey c_custkey,
			count(o_orderkey) c_count
		from
			customer left outer join orders on
				c_custkey = o_custkey
				and o_comment not like '%&1%&2%'
		group by
			c_custkey
	) c_orders
group by
	c_count
order by
	custdist desc,
	c_count desc;

