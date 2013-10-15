-- $Id: q16-tpch15.sql 2657 2007-06-12 16:08:15Z rdempsey $
-- 16th query in 100 GB stream0 (tpch15).

SELECT
        S_SUPPKEY,
        S_NAME,
        S_ADDRESS,
        S_PHONE,
        TOTAL_REVENUE
FROM
        SUPPLIER,
        REVENUE0
WHERE
        S_SUPPKEY = SUPPLIER_NO
        AND TOTAL_REVENUE = (
                SELECT
                        MAX(TOTAL_REVENUE)
                FROM
                        REVENUE0
        )
ORDER BY
        S_SUPPKEY;

