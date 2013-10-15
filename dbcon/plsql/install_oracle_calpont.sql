/*******************************************************************************
*  $Id: install_oracle_calpont.sql 3677 2007-12-21 18:04:22Z jlowe $
*  Script Name:    install_oracle_calpont.sql
*  Date Created:   2006.08.02, 2007.10.09
*  Author:         Sean Turner - Enkitec, Jason Lowe
*  Purpose:        Script to install CALPONT user and schema objects that enable the Calpont Oracle Connector.
*                  NOTE: This script can not be used when the Listener is stopped, or if the database
*                        is in restricted mode.  Please use install_oracle_calpont.sh.
/******************************************************************************/

SET VERIFY OFF

ACCEPT syspwd CHAR PROMPT 'Specify the SYS user password: ' HIDE
PROMPT
PROMPT Connection identifier valid values:
PROMPT 1.  tnsnames service name (ORACLE_SID), or
PROMPT 2.  host/database name (EZCONNECT)
ACCEPT connid CHAR PROMPT 'Specify the connection identifier for your database: '
PROMPT
PROMPT A user named CALPONT will be created as the schema owner for Calpont Oracle Connector.
PAUSE Please press Enter to continue.
DEFINE calpontuser = CALPONT
DEFINE calpontuserpwd = calpont
ACCEPT calpontts PROMPT 'Specify the default tablespace for the CALPONT user (e.g. USERS): '

--SET ECHO ON

WHENEVER SQLERROR EXIT FAILURE;

CONNECT SYS/&&syspwd@&&connid as SYSDBA

/* Create the specified Calpont user, if it doesn't exist */
@install_user.sql


CONNECT &&calpontuser/&&calpontuserpwd@&&connid

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


EXIT
