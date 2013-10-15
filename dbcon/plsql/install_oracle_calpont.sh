#! /bin/sh
#
#/*******************************************************************************
#*  Script Name:    install_oracle_calpont.sh
#*  Date Created:   2007.10.09
#*  Author:         Jason Lowe
#*  Purpose:        Script to install CALPONT user and schema objects that enable the Calpont Oracle Connector.
#*                  NOTE: This script can not be used when the Listener is stopped, or if the database
#*                        is in restricted mode.  Please use install_oracle_calpont.sh.
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
echo
echo A user named CALPONT will be created as the schema owner for Calpont Oracle Connector.
read -p 'Please press Enter to continue.'
calpontuser="CALPONT"
calpontuserpwd="calpont"
echo
echo "Specify the default tablespace for the CALPONT user (e.g. USERS):"
read calpontts
echo

if [ "$connid" = "" ]; then
    sysargs="SYS/$syspwd AS SYSDBA"
    calargs="$calpontuser/$calpontuserpwd"
else
    sysargs="SYS/$syspwd@$connid AS SYSDBA"
    calargs="$calpontuser/$calpontuserpwd@$connid"
fi

sqlplus -S $sysargs <<EOD
    SET VERIFY OFF
    --SET ECHO ON
    WHENEVER SQLERROR EXIT FAILURE;
    /* Create the specified Calpont user, if it doesn't exist */
    DEFINE calpontuser = $calpontuser
    DEFINE calpontuserpwd = $calpontuserpwd
    DEFINE calpontts = $calpontts
    @install_user.sql
EOD
if [ $? != 0 ]; then
    exit
fi

sqlplus -S $calargs <<EOD
    --SET ECHO ON
    WHENEVER SQLERROR EXIT FAILURE;
    /* Create required tables and sequences */
    @install_tables.sql
    
    /* Create required procedures and functions */
    @install_procs_funcs.sql
    
    /* Create required libraries and functions */
    @install_libraries.sql
    
    /* Create required packages */
    @install_packages.sql
    
    /* Loads Oracle Error messages into CAL_ERROR_CODE table - takes a minute to run*/
    EXECUTE pkg_error.cal_refresh_error_codes();
EOD

