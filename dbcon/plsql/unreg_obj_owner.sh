#! /bin/sh
#
#/*******************************************************************************
#*  Script Name:    unreg_obj_owner.sh
#*  Date Created:   2007.10.12
#*  Author:         Jason Lowe
#*  Purpose:        Script to unregister a user for use with the Calpont Appliance.
#/******************************************************************************/
#
echo Connection identifier valid values:
echo "1.  tnsnames service name (ORACLE_SID),"
echo "2.  host/database name (EZCONNECT), or"
echo 3.  blank if connecting internally when the listener is down or database in restricted mode
echo Specify the connection identifier for your database:
read connid
calpontuser="CALPONT"
calpontuserpwd="calpont"
echo
echo "Specify the name of the user (object owner) you want to unregister with Calpont (e.g. CALUSER01)."
echo This is the user used for QUERY and DML operations.
echo This user or the associated shadow user will NOT be dropped, but connectivity to the Calpont Appliance will be removed.
echo Specify the name of the object owner user to unregister:
read caluser
echo
echo Specify the password for the object owner user:
read caluserpwd
echo

if [ "$connid" = "" ]; then
    calargs="$calpontuser/$calpontuserpwd"
else
    calargs="$calpontuser/$calpontuserpwd@$connid"
fi

sqlplus -S $calargs <<EOD
    SPOOL unreg_obj_owner.log
    SET VERIFY OFF
    --SET ECHO ON
    WHENEVER SQLERROR EXIT FAILURE;
    EXECUTE pkg_calpont.cal_register_object_owner('$caluser', '$caluserpwd', FALSE);
    SPOOL OFF
EOD
