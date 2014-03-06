#!/usr/bin/expect
#
# $Id: buildTester.sh 421 2007-04-05 15:46:55Z dhill $
#
# Build, install, and test Calpont Builds
#
set EMAIL " "
set USERNAME root
set PASSWORD Calpont1
set BUILD " "
set SVN genii
set RELEASE " "
set SYSTEM noSystem
set USER " "
set DEBUG "-k"
set TEST(0) " "
set SERVER srvswdev11

#set SHARED "//cal6500/shared"
set SHARED "//calweb/shared"

spawn -noecho /bin/bash

for {set i 0} {$i<[llength $argv]} {incr i} {
	set arg($i) [lindex $argv $i]
}

set i 0
while true {
	if { $i == [llength $argv] } { break }
	if { $arg($i) == "-h" } {
		send_user "\n"
		send_user "'buildTester' executes a set of DVM test on a specifed system and\n"
		send_user "send the results via email or outputs results to a log file.\n"
		send_user "It has the option of installing a released\n"
		send_user "build or generating a new build when release of 'Latest' is specified\n"
		send_user "and installing that build on the specified system.\n"
		send_user "\n"
		send_user "Usage: buildTester -b 'build-type' -r 'release' -v 'svn-branch' -s 'system' -sv 'server' -u 'database-user' -p 'password' -d -e -t 'test-names'\n"
		send_user "			build-type 	  - optional: RHEL5 or FC6\n"
		send_user "			release    	  - Required when build-type is entered: Calpont release number or 'Latest'\n"
		send_user "			svn-branch    - SVN Branch built againest, default to genii\n"
		send_user "			system     	  - Target install/test system\n"
		send_user "			server     	  - Target query server hostname\n"
		send_user "			database-user - Database User, i.e. tpch1\n"
		send_user "			password 	  - system password, default to 'qcalpont!'\n"
		send_user "			-d			  - Debug mode, will output additional install and test data\n"
		send_user "			-e			  - email results\n"
		send_user "			test-names	  - Test name list seperated by spaces (sanity tpch)\n"
		exit
	}
	if { $arg($i) == "-b" } {
		incr i
		set BUILD $arg($i)
	} elseif { $arg($i) == "-r" } {
		incr i
		set RELEASE $arg($i)
	} elseif { $arg($i) == "-s" } {
		incr i
		set SYSTEM $arg($i)
	} elseif { $arg($i) == "-v" } {
		incr i
		set SVN $arg($i)
	} elseif { $arg($i) == "-u" } {
		incr i
		set USER $arg($i)
	} elseif { $arg($i) == "-p" } {
		incr i
		set PASSWORD $arg($i)
	} elseif { $arg($i) == "-t" } {
		incr i
		if { $i == [llength $argv] } {
			puts "no Test Name entered, enter ./buildTester.sh -h for additional info"; exit -1
		}
		set TESTNUMBER 0
		while true {
			set TEST($TESTNUMBER) $arg($i)
			incr i
			incr TESTNUMBER
			if { $i == [llength $argv] } {
				incr i -1
				break
			}
		}
	} elseif { $arg($i) == "-e" } {
		if { $SYSTEM == "qaftest2" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,dcathey@calpont.com,bpaul@calpont.com
		} elseif { $SYSTEM == "qaftest2" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,dcathey@calpont.com,bpaul@calpont.com
		} elseif { $SYSTEM == "caldev02" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,pleblanc@calpont.com,chao@calpont.com,xlou@calpont.com,dcathey@calpont.com
		} elseif { $SYSTEM == "srvswdev11" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,pleblanc@calpont.com,chao@calpont.com,xlou@calpont.com,dcathey@calpont.com
		} elseif { $SYSTEM == "devint2" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,pleblanc@calpont.com
		} elseif { $SYSTEM == "devint3" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,pleblanc@calpont.com
		} elseif { $SYSTEM == "ss2" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,pleblanc@calpont.com,chao@calpont.com,dcathey@calpont.com
		} elseif { $SYSTEM == "ss2ImportSSB100" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,dcathey@calpont.com,pleblanc@calpont.com
		} elseif { $SYSTEM == "demo01" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,pleblanc@calpont.com,chao@calpont.com
		} elseif { $SYSTEM == "qperfd01" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,bpaul@calpont.com,dcathey@calpont.com,pleblanc@calpont.com
		} elseif { $SYSTEM == "qperfd01" } {
			set EMAIL dhill@calpont.com,wweeks@calpont.com,bpaul@calpont.com,dcathey@calpont.com
		} elseif { $SYSTEM == "qcald02a" } {
				set EMAIL dhill@calpont.com,wweeks@calpont.com,mroberts@calpont.com
		} else {
				set EMAIL dhill@calpont.com,wweeks@calpont.com
		}
	} elseif { $arg($i) == "-d" } {
		set DEBUG " "
	} elseif { $arg($i) == "-sv" } {
		incr i
		set SERVER $arg($i)
	}
	incr i
}

if { $SYSTEM == "noSystem" } {
	set SYSTEM $BUILD
}

#cleanup
set RUN_LOG_FILE /tmp/$SYSTEM-$SVN.log
exec rm -f /tmp/test.log $RUN_LOG_FILE
# open log file
log_file $RUN_LOG_FILE

#get current date
exec date >/tmp/datesync.tmp
exec cat /tmp/datesync.tmp
set newtime [exec cat /tmp/datesync.tmp]
#
send_user "\n\n#####################################################################################################\n"
send_user     "   Automated Build Tester Report  $newtime\n"
send_user     "   BUILD=$BUILD RELEASE=$RELEASE SVN-BRANCH=$SVN SYSTEM=$SYSTEM\n"
send_user     "\n#####################################################################################################\n\n"
# build request
log_user 1
set timeout 30
if { $BUILD != " " } {
	set timeout 6060

	send_user "*******************   Generate Build $BUILD / $RELEASE  ****************************\n\n"

	log_user 0
	send "./autoBuilder -o $BUILD -r $RELEASE -s $SVN\n"
	log_user 1
	expect {
		-re "Calpont RPM Build successfully completed" { } abort
		-re "Build Failed" { send_user "\nBuild Failed\n" ; puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
	}
	#get current date
	exec date >/tmp/datesync.tmp
	exec cat /tmp/datesync.tmp
	set newtime [exec cat /tmp/datesync.tmp]

	send_user "\nBuild successfully completed  $newtime\n\n"
}
set timeout 30
#expect -re "#"
if { $DEBUG == " " } {
	log_user 1
} else {
	log_user 0
}
if { $SYSTEM != $BUILD } {
	set timeout 1500
	if { $RELEASE != " " } {
		# install request
		send_user "\n\n*******************   Install Build $RELEASE on $SYSTEM   *******************************\n\n"
		send "./autoInstaller -s $SYSTEM -r $RELEASE -n -d\n"
		expect {
			-re "Install Successfully completed"  { send_user "\nInstall successfully completed\n\n" } abort
			-re "Installation Failed" { send_user "\nInstallation Failed\n\n"
										puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
			timeout  { send_user "\nInstallation Failed - Timeout error\n\n"
										puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }

		}
	}
}
#get install RPM info
#log_user 1
#send_user "\n\n*******************   Calpont RPM Install Information  *******************************\n\n"
#exec ./remote_command_test.sh $SERVER $PASSWORD "rpm -iq calpont | grep Version -A 2" " " "No such file" 60 1
#exec ./remote_command.sh $SERVER $PASSWORD 'rpm -iq calpont' tools not 30 1
#expect {
#		-re " " { abort }
#		-re "FAILED" {
#			puts "\nCalpont RPM not installed\n"; 
#			exec echo "Test Failed, Calpont RPM not installed" >> $RUN_LOG_FILE
#			puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1
#		}
#}
set timeout 30

if { $TEST(0) != " " } {
	set timeout 600
	set t 0
	while true {
		if { $t == $TESTNUMBER } {
			break
		}
		set TESTCASE $TEST($t)
		incr t

		#get current date
		exec date >/tmp/datesync.tmp
		exec cat /tmp/datesync.tmp
		set newtime [exec cat /tmp/datesync.tmp]

		log_user 1
		send_user "\n\n*******************   Run Test $TESTCASE start at $newtime  **********************************\n\n"
		#log_user 0

		set TESTTYPE [exec expr substr $TESTCASE 1 5]
		if { $TESTTYPE == "mysql" } {
			set TESTCOMMAND [exec expr substr $TESTCASE 7 80]
		} else {
			# Oracle test case
			set TESTCOMMAND [exec expr substr $TESTCASE 8 80]
		}

		if { $TESTTYPE == "oracl" } {
			if { [catch { open "/usr/local/lib/python2.4/site-packages/dvm_app/$USER\_$SYSTEM\_brefd01.cfg" "r"} handle ] } {
				exec echo "\nTest Failed, DVM config file $USER\_$SYSTEM\_brefd01.cfg not found\n"; 
				exec echo "Test Failed, DVM config file $USER\_$SYSTEM\_brefd01.cfg not found" >> $RUN_LOG_FILE
				puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 
			}

			if { $TESTCOMMAND == "tpch" } {
				exec su - oracle -c "python /usr/local/lib/python2.4/site-packages/dvm_app/dvm.py -c /usr/local/lib/python2.4/site-packages/dvm_app/$USER\\_$SYSTEM\_brefd01.cfg -e1 -sS -rB $DEBUG -l2 -t Perf_Tpch > /tmp/test.log "
				expect {
					-re "Traceback" {
						exec echo "\nTest Failed, Database instance is down\n"; 
						exec echo "Test Failed, Database instance is down" >> $RUN_LOG_FILE
						puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
					-re "$"  { if { [catch { open "/tmp/test.log" "r"} handle ] } {
						exec echo "\nTest Failed, /tmp/test.log not found\n"; 
						exec echo "Test Failed, /tmp/test.log not found" >> $RUN_LOG_FILE
						puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
					}
				}
				exec cat /tmp/test.log  >> $RUN_LOG_FILE
			} else {
				if { $TESTCOMMAND == "dml" } {
					exec su - oracle -c "python /usr/local/lib/python2.4/site-packages/dvm_app/dvm.py -c /usr/local/lib/python2.4/site-packages/dvm_app/$USER\\_$SYSTEM\_brefd01.cfg -sS -rB -l2 -t Iter16_TableNameLengths $DEBUG  > /tmp/test.log "
					expect {
							-re "Traceback" {
								exec echo "\nTest Failed, Database instance is down\n"; 
								exec echo "Test Failed, Database instance is down" >> $RUN_LOG_FILE
								puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
							-re "$"  { if { [catch { open "/tmp/test.log" "r"} handle ] } {
								exec echo "\nTest Failed, /tmp/test.log not found\n"; 
								exec echo "Test Failed, /tmp/test.log not found" >> $RUN_LOG_FILE
								puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
							}
					}
					exec cat /tmp/test.log  >> $RUN_LOG_FILE

					exec su - oracle -c "python /usr/local/lib/python2.4/site-packages/dvm_app/dvm.py -c /usr/local/lib/python2.4/site-packages/dvm_app/$USER\\_$SYSTEM\_brefd01.cfg -sS -rB -l2 -t Iter16_Datatypes_Ext $DEBUG  > /tmp/test.log "
					expect {
							-re "Traceback" {
								exec echo "\nTest Failed, Database instance is down\n"; 
								exec echo "Test Failed, Database instance is down" >> $RUN_LOG_FILE
								puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
							-re "$"  { if { [catch { open "/tmp/test.log" "r"} handle ] } {
								exec echo "\nTest Failed, /tmp/test.log not found\n"; 
								exec echo "Test Failed, /tmp/test.log not found" >> $RUN_LOG_FILE
								puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
							}
					}
					exec cat /tmp/test.log  >> $RUN_LOG_FILE
				} else {
					# default to a test-command of a file name
					exec su - oracle -c "python /usr/local/lib/python2.4/site-packages/dvm_app/dvm.py -c /usr/local/lib/python2.4/site-packages/dvm_app/$USER\\_$SYSTEM\_brefd01.cfg -e2 -sS -rT $DEBUG -t /home/qa/bldqry/$TESTCOMMAND.txt -q /home/qa/bldqry > /tmp/test.log "
					expect {
							-re "Traceback" {
								exec echo "\nTest Failed, Database instance is down\n"; 
								exec echo "Test Failed, Database instance is down" >> $RUN_LOG_FILE
								puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
							-re "$"  { if { [catch { open "/tmp/test.log" "r"} handle ] } {
								exec echo "\nTest Failed, /tmp/test.log not found\n"; 
								exec echo "Test Failed, /tmp/test.log not found" >> $RUN_LOG_FILE
								puts [exec mail -s "$SYSTEM/$SVN Automated Build Tester Results" $EMAIL < $RUN_LOG_FILE] ; exit -1 }
							}
					}
					exec cat /tmp/test.log  >> $RUN_LOG_FILE
				}
			}
		} else { # run mysql test commands
			if { $TESTTYPE == "mysql" } {
				if { $TESTCOMMAND == "query" } {
					set timeout 72000
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "./nightly/runQueryTestAll $SVN" "runQueryTestAll completed" "No such file" $timeout
					expect {
							-re "FAILED"  { exec echo "\nFailed to run runQueryTestAll\n" >> $RUN_LOG_FILE} abort;
							-re "TIMEOUT"  { exec echo "\nTimeout on run runQueryTestAll\n" >> $RUN_LOG_FILE} abort;
							-re " "  { exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/nightly/srvswdev11/go.log"
										exec cat go.log >> $RUN_LOG_FILE
										exec rm -f go.log
							} abort;
					}
				} elseif { $TESTCOMMAND == "querySSB" } {
					set timeout 1200
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "./nightly/startQueryTesterSSB $SVN" "startQueryTester completed" "No such file" 1200
					expect {
						-re " "  { exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/queryTesterSSB.report"
							exec cat queryTesterSSB.report >> $RUN_LOG_FILE
							exec rm -f queryTesterSSB.report
						} abort;
						-re "FAILED"  { exec echo  "\nFailed to run queryTester\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo  "\nTimeout on run queryTester\n" >> $RUN_LOG_FILE} abort;
				}
				} elseif { $TESTCOMMAND == "concur" } {
					set timeout 600
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "/root/nightly/startConcurTester 32" "Success" "Failed" 600
					expect {
						-re " "  { exec echo "Concurrance Test Passed\n\n" >> $RUN_LOG_FILE} abort;
						-re "FAILED"  { exec echo  "\nConcurrance Test Failed\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo  "\nConcurrance Test Timeout\n" >> $RUN_LOG_FILE} abort;
						timeout  { exec echo  "\nConcurrance Test Timeout\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "continueousConcur" } {
					set timeout 30
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "/root/nightly/continueousConcurTester" "Started" "Failed" 30
					expect {
						-re " "  { exec echo "Continueous Concurrance Test Started \n\n" >> $RUN_LOG_FILE} abort;
						-re "FAILED"  { exec echo  "\nContinueous Concurrance Test Failed\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo  "\nContinueous Concurrance Test Timeout\n" >> $RUN_LOG_FILE} abort;
						timeout  { exec echo "\nContinueous Concurrance Test Timeout\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "queryCalpontOnly" } {
					set timeout 1200
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "./nightly/startQueryTesterCalpontOnly $SVN" "startQueryTester completed" "No such file" 1200
					expect {
						-re " "  { exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/queryTester_working_tpch1_calpontonly.report"
							exec cat queryTester_working_tpch1_calpontonly.report >> $RUN_LOG_FILE
							exec rm -f queryTester_working_tpch1_calpontonly.report} abort;
						-re "FAILED"  { exec echo "\nFailed to run queryTester\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo "\nTimeout on run queryTester\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "DBimport" } {
					set timeout 28800
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "./nightly/dbImport.sh $SVN" "dbImport completed" "No such file" $timeout
						expect {
							-re " "  { exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/nightly/qaftest2/go.log"
								exec cat go.log >> $RUN_LOG_FILE
								exec rm -f go.log} abort;
							-re "FAILED"  { exec echo "\nFailed to run dbImport\n" >> $RUN_LOG_FILE} abort;
							-re "TIMEOUT"  { exec echo "\nTimeout on run dbImport\n" >> $RUN_LOG_FILE} abort;
						}
				}
				if { $TESTCOMMAND == "ImportSSB100" } {
					set timeout 28800
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "./nightly/importSSB100.sh $SVN" "importSSB100 completed" "No such file" $timeout
					expect {
						-re " "  { 
							exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/nightly/srvalpha2/go.log"
							exec cat go.log >> $RUN_LOG_FILE
							exec rm -f go.log
						} abort;
						-re "FAILED" { 
							exec echo "\nFailed to run importSSB100\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo "\nTimeout on run importSSB100\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "dml" } {
					set timeout 60
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "./nightly/startQueryTesterDML $SVN" "startQueryTester completed" "No such file" 60
					expect {
						-re "FAILED"  { send_user "FAILED\n"
							exec echo "\nFailed to run queryTesterDML\n" >> $RUN_LOG_FILE};
						-re "TIMEOUT"  { send_user "TIMEOUT\n"
							exec echo "\nTimeout on run queryTesterDML\n" >> $RUN_LOG_FILE};
						-re " "  { send_user  "PASSED\n"
							exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/queryTesterDML.report"
							exec cat queryTesterDML.report >> $RUN_LOG_FILE
							exec rm -f queryTesterDML.report;
						};
						send_user "NOTHING\n"
					}
				} elseif { $TESTCOMMAND == "timings" } {
					set timeout 60
					log_user 1
					exec ./remote_command_test.sh $SERVER $PASSWORD "/root/nightly/updateQueries $SVN" "updateQueries completed" "No such file" 60 1
					set timeout 22000 
					exec ./remote_command_test.sh $SERVER $PASSWORD "/root/nightly/startTimingsTester $SVN" "startTimingsTester completed" "error" 22000 1
					expect {
						-re " "  {
							set REPORT demo
							exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/nightly/$REPORT/go.log"
							exec cat go.log >> $RUN_LOG_FILE} abort;
						-re "FAILED"  { exec echo "\nFailed to run timings\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo "\nTimeout on run timings\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "perfTests" } {
					set timeout 22000 
					exec ./remote_command_test.sh $SERVER $PASSWORD "/root/nightly/startPerfTests $SVN" "startPerfTests completed" "error" 22000 1
					expect {
						-re " "  {
							set REPORT perf
							exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/nightly/$REPORT/go.log"
							exec cat go.log >> $RUN_LOG_FILE} abort;
 				    	-re "FAILED"  { echo "\nFailed to run timings\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo "\nTimeout on run timings\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "dataWarehouse" } {
					set timeout 5000000 
					exec ./remote_command_test.sh $SERVER $PASSWORD "/root/nightly/startDataWarehouseTests $SVN" "startDataWarehouseTests completed" "error" 5000000 1
					expect {
						-re " "  {
							set REPORT dataWarehouse
							exec ./remote_scp_get.sh $SERVER $PASSWORD "/root/$SVN/mysql/queries/nightly/$REPORT/go.log"
							exec cat go.log >> $RUN_LOG_FILE} abort;
 				    	-re "FAILED"  { echo "\nFailed to run dataWarehouse\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo "\nTimeout on dataWarehouse\n" >> $RUN_LOG_FILE} abort;
					}
				} elseif { $TESTCOMMAND == "calbench" } {
					set timeout 22000
					log_user 1
					exec ./remote_command.sh $SERVER $PASSWORD "/home/calbench/AutomatedCalBench.sh " "EndAutomatedCalBench" "No such file" 22000 1
					expect {
						-re " "  { exec echo "\n\nAutomatedCalBench.sh successfully started\n" >> $RUN_LOG_FILE} abort;
						-re "FAILED"  { exec echo "\nFailed to run AutomatedCalBench.sh\n" >> $RUN_LOG_FILE} abort;
						-re "TIMEOUT"  { exec echo "\nTimeout on run AutomatedCalBench.sh\n" >> $RUN_LOG_FILE} abort;
					}
				}
			} else {
				send_user "Test Failed, Testcase $TESTCASE not supported\n"
				exec echo "Test Failed, Testcase $TESTCASE not supported" >> $RUN_LOG_FILE
			}
		}
		#get current date
		exec date >/tmp/datesync.tmp
		exec cat /tmp/datesync.tmp
		set newtime [exec cat /tmp/datesync.tmp]
		exec echo "\n\n*******************   Run Test $TESTCASE ended at $newtime  **********************************\n\n" >> $RUN_LOG_FILE
		sleep 10
	}
}
set timeout 30

#get current date
exec date >/tmp/datesync.tmp
exec cat /tmp/datesync.tmp
set newtime [exec cat /tmp/datesync.tmp]

exec echo "\nBuild Test Successfully Completed  $newtime\n\n" >> $RUN_LOG_FILE

#send report
log_user 0
if { $EMAIL != " " } {
	puts [exec mail -s "$SYSTEM/$SVN Automated Build/Test Results" $EMAIL < $RUN_LOG_FILE]
	send_user "email report sent\n"
} else {
	log_user 1
	send_user "Report located here: $RUN_LOG_FILE\n"
	exec cat $RUN_LOG_FILE
}

send_user "\nBuild Test Completed\n"

exit
