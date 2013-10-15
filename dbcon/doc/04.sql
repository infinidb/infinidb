-- $ID$
-- TPC-H/TPC-R Order Priority Checking Query (Q4)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	o_orderpriority,
	count(*) as order_count
from
	orders
where
	o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month
	and exists (
		select
			*
		from
			lineitem
		where
			l_orderkey = o_orderkey
			and l_commitdate < l_receiptdate
	)
group by
	o_orderpriority
order by
	o_orderpriority;
:n -1

select
	o_orderpriority,
from
	orders
where
	o_orderdate >= date ':1'
	and o_orderdate < date ':1' + interval '3' month

GetColumnRowsByFTSCompare
        TCN = 1 (o_orderdate)
        SCN = 1
        BOP = AND
        Arg32 = date ':1', date ':1' + interval '3' month
        COP32 = GTE, LE
        RRI = 1
GetColumnRowsByOffset
        TCN = 2 (o_orderpriority)
        SCN = 1
        RSPwRID = PREV-0        
        