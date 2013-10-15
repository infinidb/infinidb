-- $ID$
-- TPC-H/TPC-R Local Supplier Volume Query (Q5)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
	n_name,
	sum(l_extendedprice * (1 - l_discount)) as revenue
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	c_custkey = o_custkey
	and l_orderkey = o_orderkey
	and l_suppkey = s_suppkey
	and c_nationkey = s_nationkey
	and s_nationkey = n_nationkey
	and n_regionkey = r_regionkey
	and r_name = ':1'
	and o_orderdate >= date ':2'
	and o_orderdate < date ':2' + interval '1' year
group by
	n_name
order by
	revenue desc;
:n -1

select
	n_name,
from
	customer,
	orders,
	lineitem,
	supplier,
	nation,
	region
where
	r_name = ':1'
	and n_regionkey = r_regionkey
	and s_nationkey = n_nationkey
	and l_suppkey = s_suppkey
	and l_orderkey = o_orderkey
	and c_custkey = o_custkey
	and o_orderdate >= date ':2'
	and o_orderdate < date ':2' + interval '1' year
	
GetTokensByCompare
        DDN = ?
        BOP = EQ
        Arg32 = ':3'
        COP32 = AND
SendStackToArg32        
        RSP = PREV-0
GetColumnRowsByIndexCompare (r_name = ':1')
        TCN = 1 (region.r_name)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1        
GetColumnRowsByOffset 
        SCN = 1
        TCN = 2 (region.r_regionkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (n_regionkey = r_regionkey)
        TCN = 8 (nation.n_regionkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1        
FiterResultStacksByColumn
        RSP1 = PREV-2
        RSP2 = PREV-0   
GetColumnRowsByOffset 
        SCN = 1
        TCN = 9 (nation.n_nationkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (s_nationkey = n_nationkey)
        TCN = 15 (supplier.s_nationkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1        
FilterResultStacksByRID 
        RSP1 = PREV-3
        RSP2 = PREV-0               
GetColumnRowsByOffset 
        SCN = 1 
        TCN = 16 (supplier.s_suppkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (s_suppkey = l_suppkey)
        TCN = 20 (lineitem.l_suppkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
FilterResultStacksByRID 
        RSP1 = PREV-3
        RSP2 = PREV-0
GetColumnRowsByOffset 
        SCN = 1 
        TCN = 21 (lineitem.l_orderkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (l_orderkey = o_orderkey)
        TCN = 25 (orders.o_orderkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
FilterResultStacksByRID 
        RSP1 = PREV-3
        RSP2 = PREV-0	
GetColumnRowsByOffset 
        SCN = 1 
        TCN = 26 (orders.o_custkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (c_custkey = o_custkey)
        TCN = 30 (customer.c_custkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
FilterResultStacksByRID 
        RSP1 = PREV-3
        RSP2 = PREV-0	  
??? filter o_orderdate < '1995-01-01' from previous stack
??? filter l_shipdate > '1995-01-01' from previous stack
GetColumnRowsByOffset
        TCN = 10 (nation.n_name)
        SCN = 1
        RSPwRID = PREV-0                   