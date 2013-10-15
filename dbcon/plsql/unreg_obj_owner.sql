/*******************************************************************************
*  $Id: unreg_obj_owner.sql 3677 2007-12-21 18:04:22Z jlowe $
*  Script Name:    unreg_obj_owner.sql
*  Date Created:   2006.09.14, 2007.10.12
*  Author:         Jason Lowe
*  Purpose:        Script to unregister a user for use with the Calpont Appliance.
/******************************************************************************/

spool unreg_obj_owner.log

SET VERIFY OFF

PROMPT Connection identifier valid values:
PROMPT 1.  tnsnames service name (ORACLE_SID), or
PROMPT 2.  host/database name (EZCONNECT)
ACCEPT connid CHAR PROMPT 'Specify the connection identifier for your database: '
DEFINE calpontuser = CALPONT
DEFINE calpontuserpwd = calpont
PROMPT
PROMPT Specify the name of the user (object owner) you want to unregister with Calpont (e.g. CALUSER01).
PROMPT This is the user used for QUERY and DML operations.
PROMPT This user or the associated shadow user will NOT be dropped, but connectivity to the Calpont Appliance will be removed.
ACCEPT caluser PROMPT 'Specify the name of the object owner user to unregister: '
PROMPT
ACCEPT caluserpwd PROMPT 'Specify the password for the object owner user: '

--SET ECHO ON

WHENEVER SQLERROR EXIT FAILURE;

CONNECT &&calpontuser/&&calpontuserpwd@&&connid

EXECUTE pkg_calpont.cal_register_object_owner('&&caluser', '&&caluserpwd', FALSE);

spool off

EXIT
