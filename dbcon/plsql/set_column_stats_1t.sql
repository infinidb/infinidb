-- $Id: set_column_stats_1t.sql 4240 2008-07-11 13:54:31Z rdempsey $
--
-- Install TPC-H stats into Oracle for 1TB
--

DECLARE
	distcnt NUMBER;
	density NUMBER;
	nullcnt NUMBER;
	srec dbms_stats.StatRec;
	avgclen NUMBER;
	numvals dbms_stats.numarray := dbms_stats.numarray();
	numrows NUMBER;
	numblks NUMBER;
	avgrlen NUMBER;
	datevals dbms_stats.datearray := dbms_stats.datearray();
	charvals dbms_stats.chararray := dbms_stats.chararray();
	vcharvals dbms_stats.chararray := dbms_stats.chararray();

BEGIN
	numvals.extend(2);
	datevals.extend(2);
	charvals.extend(2);
	vcharvals.extend(2);

	-- table values for LINEITEM
	numrows := 6000000000;
	numblks := 30000000;
	avgrlen := 127;
	dbms_stats.set_table_stats('S_TPCH1T', 'LINEITEM', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for PART
	numrows := 200000000;
	numblks := 1150000;
	avgrlen := 174;
	dbms_stats.set_table_stats('S_TPCH1T', 'PART', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for REGION
	numrows := 5;
	numblks := 4;
	avgrlen := 97;
	dbms_stats.set_table_stats('S_TPCH1T', 'REGION', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for NATION
	numrows := 25;
	numblks := 4;
	avgrlen := 102;
	dbms_stats.set_table_stats('S_TPCH1T', 'NATION', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for CUSTOMER
	numrows := 150000000;
	numblks := 1500000;
	avgrlen := 159;
	dbms_stats.set_table_stats('S_TPCH1T', 'CUSTOMER', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for ORDERS
	numrows := 1500000000;
	numblks := 9400000;
	avgrlen := 112;
	dbms_stats.set_table_stats('S_TPCH1T', 'ORDERS', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for SUPPLIER
	numrows := 10000000;
	numblks := 96000;
	avgrlen := 144;
	dbms_stats.set_table_stats('S_TPCH1T', 'SUPPLIER', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	-- table values for PARTSUPP
	numrows := 800000000;
	numblks := 7680000;
	avgrlen := 144;
	dbms_stats.set_table_stats('S_TPCH1T', 'PARTSUPP', numrows => numrows, numblks => numblks, avgrlen => avgrlen);

	srec.epc := 2;

	-- column-specific values for L_EXTENDEDPRICE
	numvals(1) := 900;
	numvals(2) := 105000;
	distcnt :=   10000000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_EXTENDEDPRICE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_QUANTITY
	numvals(1) := 1;
	numvals(2) := 50;
	distcnt := 50;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_QUANTITY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_SHIPDATE
	datevals(1) := '1992-01-01';
	datevals(2) := '1998-12-31';
	distcnt := 365 * 7;
	avgclen := 8;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, datevals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_SHIPDATE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_COMMITDATE
	datevals(1) := '1992-01-01';
	datevals(2) := '1998-12-31';
	distcnt := 365 * 7;
	avgclen := 8;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, datevals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_COMMITDATE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_RECEIPTDATE
	datevals(1) := '1992-01-01';
	datevals(2) := '1998-12-31';
	distcnt := 365 * 7;
	avgclen := 8;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, datevals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_RECEIPTDATE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_ORDERKEY
	numvals(1) := 1;
	numvals(2) := 6000000000;
	distcnt := 1500000000;
	avgclen := 7;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_ORDERKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_LINENUMBER
	numvals(1) := 1;
	numvals(2) := 7;
	distcnt := 7;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_LINENUMBER',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_PARTKEY
	numvals(1) := 1;
	numvals(2) := 200000000;
	distcnt :=  200000000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_PARTKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_SUPPKEY
	numvals(1) := 1;
	numvals(2) := 10000000;
	distcnt :=  10000000;
	avgclen := 5;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_SUPPKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_RETAILPRICE
	numvals(1) := 900;
	numvals(2) := 2100;
	distcnt :=   1000000;
	avgclen := 5;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_RETAILPRICE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_LINESTATUS
	charvals(1) := 'F';
	charvals(2) := 'O';
	distcnt := 2;
	avgclen := 1;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_LINESTATUS',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_RETURNFLAG
	charvals(1) := 'A';
	charvals(2) := 'R';
	distcnt := 3;
	avgclen := 1;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_RETURNFLAG',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_DISCOUNT
	numvals(1) := 0;
	numvals(2) := .1;
	distcnt :=  11;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_DISCOUNT',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_SHIPMODE
	charvals(1) := 'AIR';
	charvals(2) := 'TRUCK';
	distcnt := 7;
	avgclen := 4;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_SHIPMODE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for L_SHIPINSTRUCT
	charvals(1) := 'COLLECT COD';
	charvals(2) := 'TAKE BACK RETURN';
	distcnt := 4;
	avgclen := 12;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'LINEITEM', 'L_SHIPINSTRUCT',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_NAME
	vcharvals(1) := 'almond almond almond almond';
	vcharvals(2) := 'yellow yellow yellow yellow';
	distcnt := 200000000;
	avgclen := 35;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, vcharvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_NAME',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_TYPE
	vcharvals(1) := 'ECONOMY ANODIZED BRASS';
	vcharvals(2) := 'STANDARD POLISHED TIN';
	distcnt := 150;
	avgclen := 22;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, vcharvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_TYPE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_SIZE
	numvals(1) := 1;
	numvals(2) := 50;
	distcnt :=  50;
	avgclen := 2;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_SIZE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_BRAND
	vcharvals(1) := 'Brand#11';
	vcharvals(2) := 'Brand#55';
	distcnt := 25;
	avgclen := 8;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, vcharvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_BRAND',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_CONTAINER
	vcharvals(1) := 'JUMBO BAG';
	vcharvals(2) := 'WRAP PKG';
	distcnt := 40;
	avgclen := 8;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, vcharvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_CONTAINER',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for R_REGIONKEY
	numvals(1) := 0;
	numvals(2) := 4;
	distcnt :=  5;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'REGION', 'R_REGIONKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for N_NATIONKEY
	numvals(1) := 0;
	numvals(2) := 24;
	distcnt :=  25;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'NATION', 'N_NATIONKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for N_REGIONKEY
	numvals(1) := 0;
	numvals(2) := 4;
	distcnt :=  5;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'NATION', 'N_REGIONKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for C_CUSTKEY
	numvals(1) := 1;
	numvals(2) := 150000000;
	distcnt :=  150000000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'CUSTOMER', 'C_CUSTKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for C_NATIONKEY
	numvals(1) := 0;
	numvals(2) := 24;
	distcnt :=  25;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'CUSTOMER', 'C_NATIONKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for C_MKTSEGMENT
	vcharvals(1) := 'AUTOMOBILE';
	vcharvals(2) := 'MACHINERY';
	distcnt := 5;
	avgclen := 9;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, vcharvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'CUSTOMER', 'C_MKTSEGMENT',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for O_ORDERKEY
	numvals(1) := 1;
	numvals(2) := 6000000000;
	distcnt :=    1500000000;
	avgclen := 7;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'ORDERS', 'O_ORDERKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for O_CUSTKEY
	numvals(1) := 1;
	numvals(2) := 150000000;
	distcnt :=    100000000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'ORDERS', 'O_CUSTKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for O_ORDERDATE
	datevals(1) := '1992-01-01';
	datevals(2) := '1998-12-31';
	distcnt := 365 * 7;
	avgclen := 8;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, datevals);

	dbms_stats.set_column_stats('S_TPCH1T', 'ORDERS', 'O_ORDERDATE',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for O_ORDERSTATUS
	charvals(1) := 'F';
	charvals(2) := 'P';
	distcnt := 3;
	avgclen := 1;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'ORDERS', 'O_ORDERSTATUS',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for O_ORDERPRIORITY
	charvals(1) := '1-URGENT';
	charvals(2) := '5-LOW';
	distcnt := 5;
	avgclen := 7;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'ORDERS', 'O_ORDERPRIORITY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for O_COMMENT
	charvals(1) := 'AAAAAAAAAAAAAAAAAA';
	charvals(2) := 'zzzzzzzzzzzzzzzzzz';
	distcnt := 1500000000;
	avgclen := 75;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, charvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'ORDERS', 'O_COMMENT',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for S_SUPPKEY
	numvals(1) := 1;
	numvals(2) := 10000000;
	distcnt :=  10000000;
	avgclen := 5;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'SUPPLIER', 'S_SUPPKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for S_NATIONKEY
	numvals(1) := 0;
	numvals(2) := 24;
	distcnt :=  25;
	avgclen := 3;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'SUPPLIER', 'S_NATIONKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for PS_PARTKEY
	numvals(1) := 1;
	numvals(2) := 200000000;
	distcnt :=  200000000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PARTSUPP', 'PS_PARTKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for PS_SUPPKEY
	numvals(1) := 1;
	numvals(2) := 10000000;
	distcnt :=  10000000;
	avgclen := 5;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PARTSUPP', 'PS_SUPPKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for PS_AVAILQTY
	numvals(1) := 1;
	numvals(2) := 10000;
	distcnt :=  10000;
	avgclen := 5;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PARTSUPP', 'PS_AVAILQTY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for PS_SUPPLYCOST
	numvals(1) := 1;
	numvals(2) := 1000;
	distcnt :=  100000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PARTSUPP', 'PS_SUPPLYCOST',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

	-- column-specific values for P_PARTKEY
	numvals(1) := 1;
	numvals(2) := 200000000;
	distcnt :=  200000000;
	avgclen := 6;

	nullcnt := 0;
	density := 1 / distcnt;
	dbms_stats.prepare_column_values(srec, numvals);

	dbms_stats.set_column_stats('S_TPCH1T', 'PART', 'P_PARTKEY',
		distcnt => distcnt, density => density, nullcnt => nullcnt, srec => srec, avgclen => avgclen);

END;
/
exit
/
