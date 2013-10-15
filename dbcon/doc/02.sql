-- $ID$
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998
:x
:o
select
        s_acctbal,
        s_name,
        n_name,
        p_partkey,
        p_mfgr,
        s_address,
        s_phone,
        s_comment
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        p_partkey = ps_partkey
        and s_suppkey = ps_suppkey
        and p_size = :1
        and p_type like '%:2'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = ':3'
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        p_partkey = ps_partkey
                        and s_suppkey = ps_suppkey
                        and s_nationkey = n_nationkey
                        and n_regionkey = r_regionkey
                        and r_name = ':3'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey;
:n 100

select
        s_acctbal,
        p_partkey       
from
        part,
        supplier,
        partsupp,
        nation,
        region
where
        r_name = ':3'
        and n_regionkey = r_regionkey
        and s_nationkey = n_nationkey
        and s_suppkey = ps_suppkey
        and p_partkey = ps_partkey
        and p_size = :1
        and ps_supplycost = (
                select
                        min(ps_supplycost)
                from
                        partsupp,
                        supplier,
                        nation,
                        region
                where
                        r_name = ':3'
                        and n_regionkey = r_regionkey
                        and s_nationkey = n_nationkey
                        and s_suppkey = ps_suppkey
                        and p_partkey = ps_partkey
        )

-- Sub SE
GetTokensByCompare
        DDN = ?
        BOP = EQ
        Arg32 = ':3'
        COP32 = AND
SendStackToArg32        
        RSP = PREV-0
GetColumnRowsByIndexCompare (r_name = ':3')
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
GetColumnRowsByIndexCompare (s_suppkey = ps_suppkey)
        TCN = 20 (partsupp.ps_suppkey)
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
        TCN = 21 (partsupp.ps_partkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (p_partkey = ps_partkey)
        TCN = 25 (part.p_partkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
FilterResultStacksByRID 
        RSP1 = PREV-3
        RSP2 = PREV-0        
GetAggregateByOffset
        TCN = 21 (partsupp.ps_supplycost)
        SCN = 1
        RSPwRID = PREV-0
        AOP = SUM
-- Parent SE         
GetColumnRowsByFTSCompare (ps_supplycose = subselect)
        TCN = 21 (partsupp.ps_supplycost)
        SCN = 1
        BOP = OR?
        Arg32 = PREV-0
GetTokensByCompare
        DDN = ?
        BOP = EQ
        Arg32 = ':3'
        COP32 = AND
SendStackToArg32        
        RSP = PREV-0
GetColumnRowsByIndexCompare (r_name = ':3')
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
GetColumnRowsByIndexCompare (s_suppkey = ps_suppkey)
        TCN = 20 (partsupp.ps_suppkey)
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
        TCN = 21 (partsupp.ps_partkey)
        RSPwRID = PREV-0
SendStackToArg32        
        RSP = PREV-0        
GetColumnRowsByIndexCompare (p_partkey = ps_partkey)
        TCN = 25 (part.p_partkey)
        SCN = 1
        BOP = OR
        Arg32 = PREV-0
        COP32 = EQ
        RRI = 1
FilterResultStacksByRID 
        RSP1 = PREV-3
        RSP2 = PREV-0                         
??? filter p_size=1 from previous stack        
FilterResultStacksByRID
        RSP1 = PREV-0
        RSP2 = PREV-20
GetColumnRowsByOffset
        TCN = 17 (supplier.s_acctbal)
        SCN = 1
        RSPwRID = PREV-0
GetColumnRowsByOffset
        TCN = 25 (part.p_partkey)
        SCN = 1
        RSPwRID = PREV-1        
