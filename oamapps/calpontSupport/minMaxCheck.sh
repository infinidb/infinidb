#!/bin/bash
#
# $Id: minMaxCheck.sh 1479  2011-07-20 09:53:32Z wweeks $
#

#
# This script resets the EM Min/Max values for the coluns defined in the cols array. 
#
# Usage:
#	./minMax.sh
#		runs against the columns defined in the cols array below.
#	./minMaxCheck.sh all 
#		run for all CP columns in the database (will take a long time against a large database).
#	./minMaxCheck.sh schemaname 
#		run for CP columns in tables in the schema.  If your schema is named all, you'll get all columns.
#	./minMaxCheck.sh schemaname tablename
#		run for CP columns against the given table.
#	./minMaxCheck.sh schemaname tablename columnname
#		run against the given column.
# 
# The script does the following:
# 1) Runs editem for the column.
# 2) Clears the min/max for the column with editem -c.
# 3) Counts the column so that the min/max get set again.
# 4) Runs editem for the column again.
# 5) Diffs the two editem runs and reports / saves the sdiff log files for the column if any of extents had the min/max changed.
#
# Notes:
# 1) An info.log entry will be logged at the end of the script if none of the columns checked had a bad extent map entry.
#    Example:
#       Mar 11 14:11:29 srvqaperf8 oamcpp[9872]: 29.091980 |0|0|0| I 08 CAL0000: min-max-monitor: okay
# 2) Two warning.log entries will be logged at the end of the script if one or min/max EM entries were corrected.
#    Example:
#       Mar 11 14:16:36 srvqaperf8 oamcpp[16364]: 36.231731 |0|0|0| W 08 CAL0000: min-max-monitor: some values were reset for oids  3004 3005
#       Mar 11 14:16:36 srvqaperf8 oamcpp[16365]: 36.263270 |0|0|0| W 08 CAL0000: min-max-monitor: log files are /tmp/idb_mm_mon.sdiff.*.15190
# 3) The script outputs the results of the selects in #3 above as it's going through the columns and will echo a line when it finds a col that was corrected.
#    Example:
#       **** Extent map min/max changed on the scan.  See results in /tmp/idb_mm_mon.sdiff.3039.15190


# Define the cols array.  Here's a sql statement that will list the date and datetime cols in the expected format.
#   idbmysql calpontsys -e "select concat(objectid, ':', \`schema\`, '.', tablename, '.', columnname) from syscolumn where datatype in (8, 11) and tablename not like 'temp%';" > www.txt
#
# NOTE:  The objectid will be looked up again when it's going through the columns in case the one in the array becomes stale.
#

if [ -z "$MYSQLCMD" ]; then
        MYSQLCMD="/usr/local/Calpont/mysql/bin/mysql --defaults-file=/usr/local/Calpont/mysql/my.cnf -u root"
fi

if [ -z "$INSTALLDIR" ]; then
        INSTALLDIR="/usr/local/Calpont"
fi

if [ -z "$PGMPATH" ]; then
	PGMPATH=$INSTALLDIR/bin
fi

cols=(
1339664:tpch1.orders.o_orderdate
1339718:tpch1.lineitem.l_shipdate
1339719:tpch1.lineitem.l_commitdate
1339720:tpch1.lineitem.l_receiptdate
1339759:dml.orders.o_orderdate
1339813:dml.lineitem.l_shipdate
1339814:dml.lineitem.l_commitdate
1339815:dml.lineitem.l_receiptdate
)

#
# If called with "all", run the script against all of the column types that use CP.
#
if [ $# -eq 1 ] && [ "$1" == "all" ]; then
	$MYSQLCMD --execute="select concat(objectid, ':', \`schema\`, '.', tablename, '.', columnname) from syscolumn where datatype not in (4, 10, 13) and not (datatype = 2 and columnlength > 8) and not (datatype = 12 and columnlength > 7);" calpontsys --skip-column-names > /tmp/idb_mm_mon.cols
	cols=( $( cat /tmp/idb_mm_mon.cols ) )
	rm -f /tmp/idb_mm_mon.cols

#
# Else if one parm passed, run against the columns in the given schema.
#
elif [ $# -eq 1 ]; then
	db=$1
        $MYSQLCMD --execute="select concat(objectid, ':', \`schema\`, '.', tablename, '.', columnname) from syscolumn where datatype not in (4, 10, 13) and not (datatype = 2 and columnlength > 8) and not (datatype = 12 and columnlength > 7) and \`schema\` = '$db';" calpontsys --skip-column-names > /tmp/idb_mm_mon.cols
	cols=( $( cat /tmp/idb_mm_mon.cols ) )
	rm -f /tmp/idb_mm_mon.cols

#
# Else if two parms passed, run the script against all the columns that use CP for that table.
#
elif [ $# -eq 2 ]; then
	db=$1
	tbl=$2
        $MYSQLCMD --execute="select concat(objectid, ':', \`schema\`, '.', tablename, '.', columnname) from syscolumn where datatype not in (4, 10, 13) and not (datatype = 2 and columnlength > 8) and not (datatype = 12 and columnlength > 7) and \`schema\` = '$db' and tablename = '$tbl';" calpontsys --skip-column-names > /tmp/idb_mm_mon.cols
        cols=( $( cat /tmp/idb_mm_mon.cols ) )
        rm -f /tmp/idb_mm_mon.cols

#
# Else if three parms passed, run the script against the column.
#
elif [ $# -eq 3 ]; then
	db=$1
	tbl=$2
	col=$3
        $MYSQLCMD --execute="select concat(objectid, ':', \`schema\`, '.', tablename, '.', columnname) from syscolumn where \`schema\` = '$db' and tablename = '$tbl' and columnname='$col';" calpontsys --skip-column-names > /tmp/idb_mm_mon.cols
        cols=( $( cat /tmp/idb_mm_mon.cols ) )
        rm -f /tmp/idb_mm_mon.cols
fi

i=0
j=0

if [ ${#cols[@]} -le 0 ]; then
        $PGMPATH/cplogger -w 0 "min-max-monitor: no qualifying columns" "$badoidlist"
        echo "min-max-monitor: no qualifying columns" "$badoidlist"
	exit 1
fi

badoidlist=
while [ $i -lt ${#cols[@]} ]; do
        let row=$i+1
        echo ""
        echo "Evaluating $row of  ${#cols[@]} at `date`.  Col is ${cols[$i]}."
        eval $(echo ${cols[$i]} | awk -F: '{printf "oid=%d\ntcn=%s\n", $1, $2}')
        eval $(echo $tcn | awk -F. '{printf "schema=%s\ntable=%s\ncolumn=%s\n", $1, $2, $3}')

	#
	# Look up the oid if the cols array is being used to keep from having to continually update the array if tables are dropped and recreated.
	#
	if [ $# -eq 0 ]; then
		$MYSQLCMD --execute="select concat(objectid, ':', \`schema\`, '.', tablename, '.', columnname) from syscolumn where \`schema\` = '$schema' and tablename='$table' and columnname='$column';" calpontsys --skip-column-names > /tmp/idb_mm_mon.cols
		results=`wc -l /tmp/idb_mm_mon.cols | awk '{print $1}'`
		if [ $results -eq 0 ]; then
			oid=0
		else
			oid=`cat /tmp/idb_mm_mon.cols`
		fi
	fi	

        $PGMPATH/editem -o$oid | awk '{print $1, $6, $8, $12}' >/tmp/idb_mm_mon.$oid.1.$$
        $PGMPATH/editem -c$oid
        $MYSQLCMD --execute="select count($column) from $table" $schema -vvv
        $PGMPATH/editem -o$oid | awk '{print $1, $6, $8, $12}' >/tmp/idb_mm_mon.$oid.2.$$
        sdiff /tmp/idb_mm_mon.$oid.1.$$ /tmp/idb_mm_mon.$oid.2.$$ --suppress-common-lines | grep -n -v invalid > /tmp/idb_mm_mon.sdiff.$oid.$$
        count=`wc -l /tmp/idb_mm_mon.sdiff.$oid.$$ | awk '{print $1}'`
        if [ $count -ne 0 ]; then
                badoidlist="$badoidlist $oid"
                ((j++))
                echo "**** Extent map min/max changed on the scan.  See results in /tmp/idb_mm_mon.sdiff.$oid.$$"
	else
		rm -f /tmp/idb_mm_mon.sdiff.$oid.$$ 
        fi
        rm -f /tmp/idb_mm_mon.$oid.*.$$
        ((i++))
done
echo ""
if [ $j -eq 0 ]; then
        $PGMPATH/cplogger -i 0 "min-max-monitor: okay"
        echo "min-max-monitor: okay"
        echo ""
	exit 0
else
        $PGMPATH/cplogger -w 0 "min-max-monitor: some values were reset for oids" "$badoidlist"
        $PGMPATH/cplogger -w 0 "min-max-monitor: log files are /tmp/idb_mm_mon.sdiff.*.$$"
        echo "min-max-monitor: some values were reset for oids" "$badoidlist"
        echo "min-max-monitor: log files are /tmp/idb_mm_mon.sdiff.*.$$"
        echo ""
	exit 1
fi


