-- $ID$
-- TPC-H/TPC-R Shipping Modes and Order Priority Query (Q12)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	l_shipmode,
	sum(case
		when o_orderpriority = '1-URGENT'
			or o_orderpriority = '2-HIGH'
			then 1
		else 0
	end) as high_line_count,
	sum(case
		when o_orderpriority <> '1-URGENT'
			and o_orderpriority <> '2-HIGH'
			then 1
		else 0
	end) as low_line_count
from
	orders,
	lineitem
where
	o_orderkey = l_orderkey
	and l_shipmode in (':1', ':2')
	and l_commitdate < l_receiptdate
	and l_shipdate < l_commitdate
	and l_receiptdate >= date ':3'
	and l_receiptdate < date ':3' + interval '1' year
group by
	l_shipmode
order by
	l_shipmode;
:n -1

GetColumnRowsByFTSCompare
	TCN = 1 (orders.o_orderkey)
	SCN = 1
	BOP = nil
	Arg32 = nil
	COP32 = nil
	RRI = 1
SendStackToArg32
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByIndexCompare
	TCN = 2 (lineitem.l_orderkey)
	SCN = 1
	BOP = OR
	Arg32 = PREV-0
	COP32 = EQ
	RRI = 1
FilterResultStacksByColumn (o_orderkey = l_orderkey)
	RSP1 = PREV-2
	RSP2 = PREV-0

// This is not right...we need a new macro that takes an RSP and a filter
GetColumnRowsByFTSCompare
	TCN = 3 (lineitem.l_shipmode)
	SCN = 1
	BOP = OR
	Arg32 = ':1', ':2'
	COP32 = EQ, EQ
	RRI = 1
FilterResultStacksByColumn ((o_orderkey = l_orderkey) and (l_shipmode in (':1', ':2')))
	RSP1 = PREV-1
	RSP2 = PREV-0

GetColumnRowsByFTSCompare
	TCN = 4 (lineitem.l_commitdate)
	SCN = 1
	BOP = nil
	Arg32 = nil
	COP32 = nil
	RRI = 1
SendStackToArg32
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByIndexCompare
	TCN = 5 (lineitem.l_recieptdate)
	SCN = 1
	BOP = OR
	Arg32 = PREV-0
	COP32 = LT
	RRI = 1
FilterResultStacksByColumn (l_commitdate < l_recieptdate)
	RSP1 = PREV-2
	RSP2 = PREV-0
FilterResultStacksByColumn ((o_orderkey = l_orderkey) and (l_shipmode in (':1', ':2')) and (l_commitdate < l_recieptdate))
	RSP1 = PREV-4
	RSP2 = PREV-0

// The rest of the filters are the same...
