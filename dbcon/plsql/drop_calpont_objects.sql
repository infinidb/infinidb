/*******************************************************************************
*  Script Name:    drop_calpont_objects.sql
*  Date Created:   2007.10.12
*  Author:         Jason Lowe
*  Purpose:        Script to drop all of the required CALPONT schema objects that enable the Calpont Oracle Connector.
*                  NOTE: This script does not drop the tables owned by CALPONT, 
*                  the triggers owned by CALPONT that are created for each registered user, or revoke
*                  the privileges granted to CALPONT.
*                  Therefore this script can be used to remove all objects that can be replaced by running:
*                  install_procs_funcs.sql, install_libraries.sql, and install_packages.sql (in that order)
/******************************************************************************/

--DO NOT UNCOMMENT THESE UNLESS YOU WANT TO LOSE LOG AND REGISTERED USER DATA
--DROP TABLE &&calpontuser..cal_action_log;
--DROP TABLE &&calpontuser..cal_error_code;
--DROP TABLE &&calpontuser..cal_registered_schema;
--DROP SEQUENCE &&calpontuser..seq_action_id;

DROP PACKAGE &&calpontuser..pkg_calpont;
DROP PACKAGE &&calpontuser..pkg_error;
DROP PACKAGE &&calpontuser..pkg_logging;

DROP FUNCTION &&calpontuser..cal_get_explain_plan;
DROP FUNCTION &&calpontuser..cal_get_DML_explain_plan;
DROP FUNCTION &&calpontuser..cal_get_sql_text;
DROP FUNCTION &&calpontuser..cal_execute_procedure;
DROP FUNCTION &&calpontuser..cal_process_ddl;
DROP FUNCTION &&calpontuser..cal_process_dml;
DROP FUNCTION &&calpontuser..cal_table_scan;
DROP FUNCTION &&calpontuser..cal_commit;
DROP FUNCTION &&calpontuser..cal_rollback;
DROP FUNCTION &&calpontuser..cal_get_bind_values;
DROP FUNCTION &&calpontuser..cal_trace_on;
DROP FUNCTION &&calpontuser..cal_last_update_count;
DROP FUNCTION &&calpontuser..cal_set_env;
DROP FUNCTION &&calpontuser..cal_setparms;

DROP PROCEDURE &&calpontuser..cal_explain_plan;
DROP PROCEDURE &&calpontuser..cal_format_explain_plan;
DROP PROCEDURE &&calpontuser..cal_sql_text;
DROP PROCEDURE &&calpontuser..cal_logoff;
DROP PROCEDURE &&calpontuser..cal_logon;
DROP PROCEDURE &&calpontuser..cal_setstats;
DROP PROCEDURE &&calpontuser..calcommit;
DROP PROCEDURE &&calpontuser..calrollback;
DROP PROCEDURE &&calpontuser..cal_clean_sql;
DROP PROCEDURE &&calpontuser..caltraceon;
DROP PROCEDURE &&calpontuser..caltraceoff; 
DROP PROCEDURE &&calpontuser..calsetenv; 
DROP PROCEDURE &&calpontuser..calsetparms; 

DROP LIBRARY &&calpontuser..calpontlib;

DROP TYPE &&calpontuser..calpontimpl;
DROP TYPE &&calpontuser..BindValueSet;
DROP TYPE &&calpontuser..BindValue;
DROP TYPE &&calpontuser..ColValueSet;
DROP TYPE &&calpontuser..colValue;
