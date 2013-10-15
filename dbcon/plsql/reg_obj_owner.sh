#! /bin/sh
#
#/*******************************************************************************
#*  Script Name:    reg_obj_owner.sh
#*  Date Created:   2007.10.12
#*  Author:         Jason Lowe
#*  Purpose:        Script to register a user for use with the Calpont Appliance.
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
echo "Specify the name of the existing user (object owner) you want to register with Calpont (e.g. CALUSER01)."
echo This user will be used for QUERY and DML operations.
echo "A shadow user (e.g. S_CALUSER01) will be created during registration to be used for DDL operations."
echo Specify the name of the existing object owner user to register:
read caluser
echo
echo Specify the password for the existing object owner user:
read caluserpwd
echo

if [ "$connid" = "" ]; then
    calargs="$calpontuser/$calpontuserpwd"
else
    calargs="$calpontuser/$calpontuserpwd@$connid"
fi

sqlplus -S $calargs <<EOD
    SPOOL reg_obj_owner.log
    SET VERIFY OFF
    --SET ECHO ON
    WHENEVER SQLERROR EXIT FAILURE;
    EXECUTE pkg_calpont.cal_register_object_owner('$caluser', '$caluserpwd', TRUE);
    SPOOL OFF
EOD


