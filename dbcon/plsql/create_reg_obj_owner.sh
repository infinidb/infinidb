#! /bin/sh
#
#/*******************************************************************************
#*  Script Name:    create_reg_obj_owner.sh
#*  Date Created:   2006.10.09
#*  Author:         Jason Lowe
#*  Purpose:        Script to create and register a new user for use with the Calpont Appliance.
#******************************************************************************/
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
calpontuserpwd="calpont"
echo
echo "Specify the name of the user (object owner) you want to create and register with Calpont (e.g. CALUSER01)."
echo This user will be used for QUERY and DML operations.
echo "A shadow user (e.g. S_CALUSER01) will be created during registration to be used for DDL operations."
echo Specify the name of the object owner user to create and register:
read caluser
echo
echo Specify the password for the object owner user:
read caluserpwd
echo
echo "Specify the default tablespace for the object owner user (e.g. USERS):"
read caluserts
echo

if [ "$connid" = "" ]; then
    sysargs="SYS/$syspwd AS SYSDBA"
    calargs="$calpontuser/$calpontuserpwd"
else
    sysargs="SYS/$syspwd@$connid AS SYSDBA"
    calargs="$calpontuser/$calpontuserpwd@$connid"
fi

sqlplus -S $sysargs <<EOD
    SPOOL create_reg_obj_owner.log
    WHENEVER SQLERROR EXIT FAILURE;
    CREATE USER $caluser PROFILE "DEFAULT" IDENTIFIED BY "$caluserpwd"
    DEFAULT TABLESPACE $caluserts TEMPORARY TABLESPACE "TEMP" QUOTA UNLIMITED ON $caluserts ACCOUNT UNLOCK;
    SPOOL OFF
EOD
if [ $? != 0 ]; then
    exit
fi

sqlplus -S $calargs <<EOD
    SPOOL create_reg_obj_owner.log APPEND
    WHENEVER SQLERROR EXIT FAILURE;
    EXECUTE pkg_calpont.cal_register_object_owner('$caluser', '$caluserpwd', TRUE);
    SPOOL OFF
EOD
