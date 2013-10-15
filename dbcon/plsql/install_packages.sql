/*******************************************************************************
*  $Id: install_packages.sql 2624 2007-06-04 20:26:13Z jlowe $
*  Script Name:    install_packages.sql
*  Date Created:   2006.08.22
*  Author:         Jason Lowe
*  Purpose:        Create packages used to support the Calpont schema.
/******************************************************************************/

spool install_packages.log

-- Note: pkg_error references pkg_logging and vice versa, so on initial install pkg_error will get compilation errors
@@install_pkg_error.sql;
@@install_pkg_logging.sql;
@@install_pkg_calpont.sql;

spool off
