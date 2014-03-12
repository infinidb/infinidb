#!/bin/bash

#
# Estimates the row count for a given table.  Uses number of extents * 8M for the estimate.
#

#
# Initialize variables.
#

if [ -z "$MYSQLCMD" ]; then
	INSTALLDIR="/usr/local/Calpont"
	MYSQLCNF=$INSTALLDIR/mysql/my.cnf
	MYSQLCMD="$INSTALLDIR/mysql/bin/mysql --defaults-file=$MYSQLCNF -u root"
fi

#
# Validate that there are two parameters - schema and table.
#
if [ $# -ne 2 ]; then
	echo ""
	echo "Reports the approximate row count for the given table."
	echo ""
	echo "Parameters:"
	echo "	Schema"
	echo "	Table"
fi
db=$1
table=$2

#
# Validate that the table exists.
#
sql="select count(*) from systable where \`schema\`='$db' and tablename='$table';"
count=`$MYSQLCMD calpontsys --skip-column-names -e "$sql;"`
if [ $count -le 0 ]; then
	echo ""
	echo "$db.$table does not exist in InfiniDB."
	echo ""
	exit 1
fi

#
# Grab the objectid and column width for a column in the table.
#
sql="select objectid from syscolumn where \`schema\`='$db' and tablename='$table' limit 1;" 
objectid=`$MYSQLCMD calpontsys --skip-column-names -e "$sql"`
sql="select columnlength from syscolumn where objectid=$objectid;"
colWidth=`$MYSQLCMD calpontsys --skip-column-names -e "$sql"`

#
# Use editem to count the extents.
#
extentCount=`/usr/local/Calpont/bin/editem -o $objectid | wc -l`
let extentCount-=2 # Take out the 2 extra rows for header and blank line at end.
let approximateRowCount=$extentCount*8192*1024;

echo ""
echo "Approximate row count for $db.$table is $approximateRowCount."
echo ""

exit 0
