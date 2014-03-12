#!/bin/bash

#
# Reports the max value from the extent map for the given column.
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
# Validate that there are three parameters - schema and table and columnname.
#
if [ $# -ne 3 ]; then
	echo ""
	echo "Reports the max value for the given column."
	echo ""
	echo "Parameters:"
	echo "	Schema"
	echo "	Table"
	echo "	Column"
	exit 1
fi
db=$1
table=$2
column=$3

#
# Validate that the column exists.
#
sql="select count(*) from syscolumn where \`schema\`='$db' and tablename='$table' and columnname='$column';"
count=`$MYSQLCMD calpontsys --skip-column-names -e "$sql;"`
if [ $count -le 0 ]; then
	echo ""
	echo "$db.$table.$column does not exist in InfiniDB."
	echo ""
	exit 1
fi

#
# Validate that the column type is one that this script supports.
# Supported Types:
# 6  int
# 8  date
# 9  bigint
# 11 datetime
sql="select datatype from syscolumn where \`schema\`='$db' and tablename='$table' and columnname='$column';"
dataType=`$MYSQLCMD calpontsys --skip-column-names -e "$sql"`
if [ $dataType -ne 6 ] && [ $dataType -ne 8 ] && [ $dataType -ne 9 ] && [ $dataType -ne 11 ]; then
	echo ""
	echo "The column data type must be an int, bigint, date, or datetime."
	echo ""
	exit 1
fi 

#
# Grab the objectid for the column.
#
sql="select objectid from syscolumn where \`schema\`='$db' and tablename='$table' and columnname='$column';" 
objectid=`$MYSQLCMD calpontsys --skip-column-names -e "$sql"`

#
# Set the editem specific parameter if the column is a date or datetime.
#
if [ $dataType -eq 8 ]; then
	parm="-t"
elif [ $dataType -eq 11 ]; then
	parm="-s"
fi

#
# Use the editem utility to get the min and max value.
#
/usr/local/Calpont/bin/editem -o $objectid $parm | grep max | awk -v dataType=$dataType '
	BEGIN {
		allValid=1;
		foundValidExtent=0;
	}
	{
		if(dataType == 11) {
			state=substr($14, 1, length($14)-1); # Datetime has date and time as two fields.
			thisMin=$6 " " substr($7, 1, length($7)-1);
			thisMax=$9 " " substr($10, 1, length($10)-1);
		}
		else {
			state=substr($12, 1, length($12)-1);
			thisMin=substr($6, 1, length($6)-1);
			thisMax=substr($8, 1, length($8)-1);
		}
		if(state == "valid") {
			if(!foundValidExtent) {
				min=thisMin;
				max=thisMax;
				foundValidExtent=1;
			}
			else {
				if(thisMin < min) {
					min=thisMin;
				}
				if(thisMax > max) {
					max=thisMax;
				}
			}
		} 
		else {
			allValid=0;
		}
	}
	END {
		if(foundValidExtent == 1) {
			print "";
			print "Min=" min;
			print "Max=" max;
			print "";
			if(allValid == 0) {
				print "Not all extents had min and max values set.  Answer is incomplete."
			}
		}
		else {
			print "";
			print "There were not any extents with valid min/max values.  Unable to provide answer.";
			print "";
		}
	}'

exit 0
