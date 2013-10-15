-- $Id: q10-tpch13.sql 2657 2007-06-12 16:08:15Z rdempsey $
-- 10th query in 100 GB stream0 (tpch13).

SELECT 
	C_COUNT,
	COUNT(*) AS CUSTDIST
FROM 
	( 
		SELECT C_CUSTKEY,
		COUNT(O_ORDERKEY) C_COUNT
		FROM 
			CUSTOMER left outer join ORDERS on
				C_CUSTKEY = O_CUSTKEY AND
				O_COMMENT not like '%%express%%requests%%'
		GROUP BY 
			C_CUSTKEY
	) C_ORDERS
GROUP BY 
	C_COUNT
ORDER BY 
	CUSTDIST DESC,
	C_COUNT DESC;
