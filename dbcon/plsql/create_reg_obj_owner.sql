/*******************************************************************************
*  $Id: create_reg_obj_owner.sql 3702 2008-01-10 16:25:57Z jlowe $
*  Script Name:    create_reg_obj_owner.sql
*  Date Created:   2007.10.09
*  Author:         Jason Lowe
*  Purpose:        Script to create and register a new user for use with the Calpont Appliance.
*                  NOTE: This script can not be used when the Listener is stopped, or if the database
*                        is in restricted mode.  Please use create_reg_obj_owner.sh.
/******************************************************************************/

SPOOL create_reg_obj_owner.log

SET VERIFY OFF

ACCEPT syspwd CHAR PROMPT 'Specify the SYS user password: ' HIDE
PROMPT
PROMPT Connection identifier valid values:
PROMPT 1.  tnsnames service name (ORACLE_SID), or
PROMPT 2.  host/database name (EZCONNECT)
ACCEPT connid CHAR PROMPT 'Specify the connection identifier for your database: '
DEFINE calpontuser = CALPONT
DEFINE calpontuserpwd = calpont
PROMPT
PROMPT Specify the name of the user (object owner) you want to create and register with Calpont (e.g. CALUSER01).
PROMPT This user will be used for QUERY and DML operations.
PROMPT A shadow user (e.g. S_CALUSER01) will be created during registration to be used for DDL operations.
ACCEPT caluser PROMPT 'Specify the name of the object owner user to create and register: '
PROMPT
ACCEPT caluserpwd PROMPT 'Specify the password for the object owner user: '
PROMPT
ACCEPT caluserts PROMPT 'Specify the default tablespace for the object owner user (e.g. USERS): '

--SET ECHO ON

WHENEVER SQLERROR EXIT FAILURE;

CONNECT SYS/&&syspwd@&&connid as SYSDBA

CREATE USER &&caluser PROFILE "DEFAULT" IDENTIFIED BY "&&caluserpwd"
DEFAULT TABLESPACE &&caluserts TEMPORARY TABLESPACE "TEMP" QUOTA UNLIMITED ON &&caluserts ACCOUNT UNLOCK;


CONNECT &&calpontuser/&&calpontuserpwd@&&connid

EXECUTE pkg_calpont.cal_register_object_owner('&&caluser', '&&caluserpwd', TRUE);

SPOOL OFF

EXIT
