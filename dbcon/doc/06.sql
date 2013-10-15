-- $ID$
-- TPC-H/TPC-R Forecasting Revenue Change Query (Q6)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	sum(l_extendedprice * l_discount) as revenue
from
	lineitem
where
	l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' year
	and l_discount between :2 - 0.01 and :2 + 0.01
	and l_quantity < :3;
:n -1

select
	l_extendedprice,
	l_discount
from
	lineitem
where
	l_shipdate >= date ':1'
	and l_shipdate < date ':1' + interval '1' year
	and l_discount between :2 - 0.01 and :2 + 0.01
	and l_quantity < :3;
	
GetColumnRowsByFTSCompare
        TCN = 1 (lineitem.l_shipdate)
        SCN = 1
        BOP = AND
        Arg32 = date ':1', date ':1' + interval '1' year
        COP32 = GTE, LE
SendStackToArg32
        RSP = PREV-0
??? filter l_discount from previous stack
??? filter l_quantity from previous stack
GetColumnRowsByOffset
        TCN = 10 (l_extendedprice)
        SCN = 1
        RSPwRID = PREV-0                  
GetColumnRowsByOffset
        TCN = 9 (l_discount)
        SCN = 1
        RSPwRID = PREV-0            	