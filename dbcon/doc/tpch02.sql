-- $Id: tpch02.sql 3647 2007-12-13 21:20:41Z rdempsey $
-- TPC-H/TPC-R Minimum Cost Supplier Query (Q2)
-- Functional Query Definition
-- Approved February 1998

define 1 = 15
define 2 = BRASS
define 3 = EUROPE

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
        and p_size = &1
        and p_type like '%&2'
        and s_nationkey = n_nationkey
        and n_regionkey = r_regionkey
        and r_name = '&3'
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
                        and r_name = '&3'
        )
order by
        s_acctbal desc,
        n_name,
        s_name,
        p_partkey;

