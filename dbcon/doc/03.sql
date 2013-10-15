-- $ID$
-- TPC-H/TPC-R Shipping Priority Query (Q3)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
        l_orderkey,
        sum(l_extendedprice * (1 - l_discount)) as revenue,
        o_orderdate,
        o_shippriority
from
        customer,
        orders,
        lineitem
where
        c_mktsegment = ':1'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < date ':2'
        and l_shipdate > date ':2'
group by
        l_orderkey,
        o_orderdate,
        o_shippriority
order by
        revenue desc,
        o_orderdate;
:n 10

select
        l_orderkey,
        o_orderdate
from    
        customer,
        orders,
        lineitem
where
        c_mktsegment =  'AUTOMOBILE'
        and c_custkey = o_custkey
        and l_orderkey = o_orderkey
        and o_orderdate < '1995-01-01'
        and l_shipdate > '1995-01-01'
        
GetTokensByCompare
        DDN = ?
        BOP = EQ
        Arg32 = 'AUTOMOBILE'
        COP32 = AND
SendStackToArg32        
        RSP = PREV-0
GetColumnRowsByIndexCompare (c_mktsegment =  'AUTOMOBILE')
        TCN = 1 (customer.c_mktsegment)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
GetColumnRowsByOffset 
        SCN = 1
        TCN = 2 (customer.c_custkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (c_custkey = o_custkey)
        TCN = 8 (orders.o_custkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
FiterResultStacksByColumn
        RSP1 = PREV-2
        RSP2 = PREV-0   
FilterResultStacksByRID 
        RSP1 = PREV-4
        RSP2 = PREV-0
GetColumnRowsByOffset
        SCN = 1
        TCN = 9 (order.o_orderkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0  
GetColumnRowsByIndexCompare (l_orderkey = o_orderkey)
        TCN = 15 (lineitem.l_orderkey)
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
        TCN = 10 (order.o_orderdate)
        RSPwRID = PREV-0       
SendStackToArg32        
        RSP = PREV-0
??? filter o_orderdate < '1995-01-01' from previous stack
??? filter l_shipdate > '1995-01-01' from previous stack
GetColumnRowsByOffset
        TCN = 15 (l_orderkey)
        SCN = 1
        RSPwRID = PREV-0    
GetColumnRowsByOffset
        TCN = 10 (o_orderdate)
        SCN = 1
        RSPwRID = PREV-0            
                                              
        
        
       
         