-- $Id: q12-tpch22.sql 2657 2007-06-12 16:08:15Z rdempsey $
-- 12th query in 100 GB stream0 (tpch22).

SELECT
        CNTRYCODE,
        COUNT(*) NUMCUST,
        SUM(C_ACCTBAL) TOTACCTBAL
FROM (
        SELECT
                SUBSTR(C_PHONE,1,2) CNTRYCODE,
                C_ACCTBAL
        FROM
                CUSTOMER
        WHERE
                SUBSTR(C_PHONE,1,2) IN
                        ('24', '28', '30', '18', '21', '23', '14')
                AND C_ACCTBAL > (
                        SELECT
                                AVG(C_ACCTBAL)
                        FROM
                                CUSTOMER
                        WHERE
                                C_ACCTBAL > 0.00
                                AND SUBSTR(C_PHONE,1,2) IN
                                        ('24', '28', '30', '18', '21', '23', '14')
                )
                AND NOT EXISTS (
                        SELECT *
                        FROM
                                ORDERS
                        WHERE
                                O_CUSTKEY = C_CUSTKEY
                )
) CUSTSALE
GROUP BY
        CNTRYCODE
ORDER BY
        CNTRYCODE;

