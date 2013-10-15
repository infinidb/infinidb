-- $ID$
-- TPC-H/TPC-R Promotion Effect Query (Q14)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	100.00 * sum(case
		when p_type like 'PROMO%'
			then l_extendedprice * (1 - l_discount)
		else 0
	end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from
	lineitem,
	part
where
	l_partkey = p_partkey
	and l_shipdate >= '1995-09-01'
	and l_shipdate < '1995-09-01' + interval '1' month;
:n -1


GetColumnRowsByFTSCompare
	TCN = 1 (lineitem.l_partkey)
	SCN = 1
	BOP = nil
	Arg32 = nil
	COP32 = nil
	RRI = 1
SendStackToArg32
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByIndexCompare
	TCN = 2 (part.p_partkey)
	SCN = 1
	BOP = OR
	Arg32 = PREV-0
	COP32 = EQ
	RRI = 1
FilterResultStacksByColumn (l_partkey = p_partkey)
	RSP1 = PREV-2
	RSP2 = PREV-0

-- Need a new macro that applies a filter to an RSP...
-- This is a BETWEEN filter
GetColumnRowsByFTSCompare
	TCN = 1 (lineitem.l_shipdate)
	SCN = 1
	BOP = AND
	Arg32 = '1995-09-01', '1995-10-01'
	COP32 = GE, LT
	RRI = 1

-- Not even sure where to go from here...we need to examine p_type
-- and we need to do math on l_extendedprice and l_discount
-- based on p_type
