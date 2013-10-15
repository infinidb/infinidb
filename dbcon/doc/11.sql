-- $ID$
-- TPC-H/TPC-R Important Stock Identification Query (Q11)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	ps_partkey,
	sum(ps_supplycost * ps_availqty) as value
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = ':1'
group by
	ps_partkey having
		sum(ps_supplycost * ps_availqty) > (
			select
				sum(ps_supplycost * ps_availqty) * :2
			from
				partsupp,
				supplier,
				nation
			where
				ps_suppkey = s_suppkey
				and s_nationkey = n_nationkey
				and n_name = ':1'
		)
order by
	value desc;
:n -1

select
	sum(ps_supplycost * ps_availqty) * :2
from
	partsupp,
	supplier,
	nation
where
	ps_suppkey = s_suppkey
	and s_nationkey = n_nationkey
	and n_name = ':1'

GetColumnRowsByFTSCompare
	TCN = 1 (partsupp.ps_suppkey)
	SCN = 1
	BOP = nil
	Arg32 = nil
	COP32 = nil
	RRI = 1
SendStackToArg32
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByIndexCompare
	TCN = 2 (supplier.s_suppkey)
	SCN = 1
	BOP = OR
	Arg32 = PREV-0
	COP32 = EQ
	RRI = 1
FilterResultStacksByColumn (ps_suppkey = s_suppkey)
	RSP1 = PREV-2
	RSP2 = PREV-0
GetColumnRowsByFTSCompare
	TCN = 3 (supplier.s_nationkey)
	SCN = 1
	BOP = nil
	Arg32 = nil
	COP32 = nil
	RRI = 1
SendStackToArg32
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByIndexCompare
	TCN = 4 (nation.n_nationkey)
	SCN = 1
	BOP = OR
	Arg32 = PREV-0
	COP32 = EQ
	RRI = 1
FilterResultStacksByColumn (s_nationkey = n_nationkey)
	RSP1 = PREV-2
	RSP2 = PREV-0
FilterResultStacksByRID (ps_suppkey = s_suppkey and s_nationkey = n_nationkey)
	RSP1 = PREV-4
	RSP2 = PREV-0
GetTokensByCompare (dtok(':1'))
	TCN = 5 (nation.n_name)
	BOP = nil
	Arg32 = ':1'
	COP32 = EQ
SendStackToArg32
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByIndexCompare (n_name = dtok(':1'))
	TCN = 5 (nation.n_name)
	SCN = 1
	BOP = nil
	Arg32 = PREV-0
	COP32 = EQ
	RRI = 1
FilterResultStacksByRID ((ps_suppkey = s_suppkey and s_nationkey = n_nationkey) and n_name = dtok(':1'))
	RSP1 = PREV-3
	RSP2 = PREV-0
GetColumnRowsByOffset
	TCN = 6 (supplier.ps_supplycost)
	SCN = 1
	RSPwRID = PREV-0
GetColumnRowsByOffset
	TCN = 7 (supplier.ps_availqty)
	SCN = 1
	RSPwRID = PREV-0
