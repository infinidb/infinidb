/*******************************************************************************
*  $Id: reg_obj_owner.sql 3677 2007-12-21 18:04:22Z jlowe $
*  Script Name:    reg_obj_owner.sql
*  Date Created:   2006.09.14, 2007.10.12
*  Author:         Jason Lowe
*  Purpose:        Script to register a user for use with the Calpont Appliance.
/******************************************************************************/

spool reg_obj_owner.log

SET VERIFY OFF

PROMPT Connection identifier valid values:
PROMPT 1.  tnsnames service name (ORACLE_SID), or
PROMPT 2.  host/database name (EZCONNECT)
ACCEPT connid CHAR PROMPT 'Specify the connection identifier for your database: '
DEFINE calpontuser = CALPONT
DEFINE calpontuserpwd = calpont
PROMPT
PROMPT Specify the name of the existing user (object owner) you want to register with Calpont (e.g. CALUSER01).
PROMPT This user will be used for QUERY and DML operations.
PROMPT A shadow user (e.g. S_CALUSER01) will be created during registration to be used for DDL operations.
ACCEPT caluser PROMPT 'Specify the name of the existing object owner user to register: '
PROMPT
ACCEPT caluserpwd PROMPT 'Specify the password for the existing object owner user: '

--SET ECHO ON

WHENEVER SQLERROR EXIT FAILURE;

CONNECT &&calpontuser/&&calpontuserpwd@&&connid

EXECUTE pkg_calpont.cal_register_object_owner('&&caluser', '&&caluserpwd', TRUE);

spool off

EXIT
