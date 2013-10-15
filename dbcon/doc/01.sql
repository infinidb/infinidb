-- $ID$
-- TPC-H/TPC-R Pricing Summary Report Query (Q1)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	sum(l_extendedprice) as sum_base_price,
	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
	avg(l_quantity) as avg_qty,
	avg(l_extendedprice) as avg_price,
	avg(l_discount) as avg_disc,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval ':1' day (3)
group by
	l_returnflag,
	l_linestatus
order by
	l_returnflag,
	l_linestatus;
:n -1

select
	l_returnflag,
	l_linestatus,
	sum(l_quantity) as sum_qty,
	avg(l_quantity) as avg_qty,
	count(*) as count_order
from
	lineitem
where
	l_shipdate <= date '1998-12-01' - interval '90' day

GetColumnRowsByFTSCompare
	TCN = 1 (lineitem.l_shipdate)
	SCN = 1
	BOP = nil
	Arg32 = 1998-09-01
	COP32 = LE
	RRI = 1
GetColunmRowsByOffset
	TCN = 2 (lineitem.l_returnflag)
	SCN = 1
	RSPwRID = PREV-0
GetColunmRowsByOffset
	TCN = 3 (lineitem.l_linestatus)
	SCN = 1
	RSPwRID = PREV-1
GetAggregateByOffset
	TCN = 4 (lineitem.l_quantity)
	SCN = 1
	RSPwRID = PREV-2
	AOP = SUM
GetAggregateByOffset
	TCN = 4 (lineitem.l_quantity)
	SCN = 1
	RSPwRID = PREV-3
	AOP = AVG
GetAggregateByOffset
	TCN = 0 (*)
	SCN = 1
	RSPwRID = PREV-4
	AOP = COUNT

