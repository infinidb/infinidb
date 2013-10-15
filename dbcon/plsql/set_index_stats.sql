-- $Id: set_index_stats.sql 3166 2007-09-06 19:26:58Z rdempsey $
--
-- Install TPC-H index stats into Oracle for 1GB
--

DECLARE
	numrows NUMBER;
	numlblks NUMBER;
	numdist NUMBER;
	avglblk NUMBER;
	avgdblk NUMBER;
	clstfct NUMBER;
	indlevel NUMBER;
	-- guessq NUMBER;
	-- cachedblk NUMBER;
	-- cachehit NUMBER;

BEGIN
	-- O_ORDERKEY_IDX
	numrows := 1500000;
	numlblks := 3519;
	numdist := 1500000;
	avglblk := 1;
	avgdblk := 1;
	clstfct := 24065;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'O_ORDERKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- O_CUSTKEY_IDX
	numrows := 1500000;
	numlblks := 3332;
	numdist := 99996;
	avglblk := 1;
	avgdblk := 14;
	clstfct := 1499466;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'O_CUSTKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- C_CUSTKEY_IDX
	numrows := 150000;
	numlblks := 333;
	numdist := 150000;
	avglblk := 1;
	avgdblk := 1;
	clstfct := 3430;
	indlevel := 1;
	dbms_stats.set_index_stats('&1', 'C_CUSTKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- S_SUPPKEY_IDX
	numrows := 10000;
	numlblks := 21;
	numdist := 10000;
	avglblk := 1;
	avgdblk := 1;
	clstfct := 208;
	indlevel := 1;
	dbms_stats.set_index_stats('&1', 'S_SUPPKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- PS_PARTKEY_IDX
	numrows := 800000;
	numlblks := 1777;
	numdist := 200000;
	avglblk := 1;
	avgdblk := 1;
	clstfct := 16448;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'PS_PARTKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- PS_SUPPKEY_IDX
	numrows := 800000;
	numlblks := 1672;
	numdist := 10000;
	avglblk := 1;
	avgdblk := 80;
	clstfct := 800000;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'PS_SUPPKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- P_PARTKEY_IDX
	numrows := 200000;
	numlblks := 445;
	numdist := 200000;
	avglblk := 1;
	avgdblk := 1;
	clstfct := 3796;
	indlevel := 1;
	dbms_stats.set_index_stats('&1', 'P_PARTKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- L_ORDERKEY_IDX
	numrows := 5989741;
	numlblks := 14060;
	numdist := 1500000;
	avglblk := 1;
	avgdblk := 1;
	clstfct := 121199;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'L_ORDERKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- L_PARTKEY_IDX
	numrows := 6169354;
	numlblks := 13720;
	numdist := 198209;
	avglblk := 1;
	avgdblk := 31;
	clstfct := 6168482;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'L_PARTKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);

	-- L_SUPPKEY_IDX
	numrows := 6006538;
	numlblks := 12555;
	numdist := 10000;
	avglblk := 1;
	avgdblk := 599;
	clstfct := 5991028;
	indlevel := 2;
	dbms_stats.set_index_stats('&1', 'L_SUPPKEY_IDX', numrows => numrows, numlblks => numlblks, numdist => numdist,
		avglblk => avglblk, avgdblk => avgdblk, clstfct => clstfct, indlevel => indlevel);
END;
/
exit
/
