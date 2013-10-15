-- $Id: q05-tpch06.sql 2657 2007-06-12 16:08:15Z rdempsey $
-- 5th query in 100 GB stream0 (tpch06).

select sysdate from dual;
SELECT 
	SUM(L_EXTENDEDPRICE*L_DISCOUNT) AS REVENUE
FROM 
	LINEITEM
WHERE 
	L_SHIPDATE >= date '1997-01-01' AND
	L_SHIPDATE < date '1997-01-01' + interval '1' year AND
	L_DISCOUNT BETWEEN 0.03 - 0.01 AND 0.03 + 0.01 AND
	L_QUANTITY < 25;

select sysdate from dual;
