#! /bin/sh
#
#/*******************************************************************************
#*  Script Name:    uninstall_oracle_calpont.sh
#*  Date Created:   2007.10.12
#*  Author:         Jason Lowe
#*  Purpose:        Script to uninstall all of the required CALPONT schema objects that enable the Calpont Oracle Connector.
#*                  NOTE: This script does not drop the tables owned by CALPONT,
#*                  the triggers owned by CALPONT that are created for each registered user, or revoke
#*                  the privileges granted to CALPONT.
#*                  Therefore this script can be used to remove all objects that can be replaced by running:
#*                  install_procs_funcs.sql, install_libraries.sql, and install_packages.sql (in that order)
#/******************************************************************************/
#
echo Specify the SYS user password:
read -s syspwd
echo
echo Connection identifier valid values:
echo "1.  tnsnames service name (ORACLE_SID),"
echo "2.  host/database name (EZCONNECT), or"
echo 3.  blank if connecting internally when the listener is down or database in restricted mode
echo Specify the connection identifier for your database:
read connid
calpontuser="CALPONT"
echo
echo NOTE: This script does not drop the tables owned by CALPONT,
echo the triggers owned by CALPONT that are created for each registered user, or revoke the privileges granted to CALPONT.
echo Therefore this script can be used to remove all objects that can be replaced by running:
echo "install_procs_funcs.sql, install_libraries.sql, and install_packages.sql (in that order)."
read -p 'Please press Enter to continue.'
echo

if [ "$connid" = "" ]; then
    sysargs="SYS/$syspwd AS SYSDBA"
else
    sysargs="SYS/$syspwd@$connid AS SYSDBA"
fi

sqlplus -S $sysargs <<EOD
SET VERIFY OFF
--SET ECHO ON
DEFINE calpontuser = $calpontuser
@drop_calpont_objects.sql
EOD
