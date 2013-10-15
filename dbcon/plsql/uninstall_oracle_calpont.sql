/*******************************************************************************
*  $Id: uninstall_oracle_calpont.sql 3504 2007-10-30 18:31:45Z jlowe $
*  Script Name:    uninstall_oracle_calpont.sql
*  Date Created:   2006.08.28, 2007.10.12
*  Author:         Jason Lowe
*  Purpose:        Script to uninstall all of the required CALPONT schema objects that enable the Calpont Oracle Connector.
*                  NOTE: This script does not drop the tables owned by CALPONT,
*                  the triggers owned by CALPONT that are created for each registered user, or revoke
*                  the privileges granted to CALPONT.
*                  Therefore this script can be used to remove all objects that can be replaced by running:
*                  install_procs_funcs.sql, install_libraries.sql, and install_packages.sql (in that order)
/******************************************************************************/
spool uninstall_oracle_calpont.log

SET VERIFY OFF

ACCEPT syspwd CHAR PROMPT 'Specify the SYS user password: ' HIDE
PROMPT
PROMPT Connection identifier valid values:
PROMPT 1.  tnsnames service name (ORACLE_SID), or
PROMPT 2.  host/database name (EZCONNECT)
ACCEPT connid CHAR PROMPT 'Specify the connection identifier for your database: '
PROMPT
PROMPT NOTE: This script does not drop the tables owned by CALPONT, 
PROMPT the triggers owned by CALPONT that are created for each registered user, or revoke the privileges granted to CALPONT.
PROMPT Therefore this script can be used to remove all objects that can be replaced by running:
PROMPT install_procs_funcs.sql, install_libraries.sql, and install_packages.sql (in that order).
PAUSE Please press Enter to continue.
DEFINE calpontuser = CALPONT

--SET ECHO ON

CONNECT SYS/&&syspwd@&&connid as SYSDBA

@drop_calpont_objects.sql

spool off

EXIT;


