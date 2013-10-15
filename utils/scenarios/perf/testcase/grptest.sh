#!/bin/bash
#
#   This test should be executed on the UM module where mySQL front end is installed.
#   
#   grpNum=		group number 
#   testDB=		database to be used
#   numConcur		number of concurrent user
#   numRepeat		number of iterations to repeat the test
#   testType		D = disk run.  Flush disk catch before running a query
#			C = cache run.  Run each query twice, one disk and one cache
#			S = stream run. Flush disk cache one, then run all queries in the group without flush
#                    M = mixed run.  Use query groups 1 to 5 as one group.  all users will pick queries from the group.
#                                    Each query will be executed only once.
#   timeoutVal	Timeout value to abort the test.  Not yet implemented.
#   dbmsType		DBMS type, M for mySQL. 
#
   grpNum=1
   testDB=tpch100
   numConcur=2
   numRepeat=1
   testType=C
   timeoutVal=0
   dbmsType=M
#
   /root/genii/utils/scenarios/perf/test/pfSubmitGroupTest.sh $grpNum $testDB $numConcur $numRepeat $testType $timeoutVal $dbmsType