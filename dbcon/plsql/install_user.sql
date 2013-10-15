/*******************************************************************************
*  $Id: install_user.sql 4168 2008-06-20 21:07:09Z jlowe $
*  Script Name:    install_user.sql
*  Date Created:   2006.10.11
*  Author:         Jason Lowe
*  Purpose:        Create CALPONT user, and grant needed privs.
*                  Raise error if CALPONT already exists.
/******************************************************************************/

spool install_user.log

WHENEVER SQLERROR EXIT FAILURE;

DECLARE
    sUser   VARCHAR2(30);
BEGIN
    /* See if the specified user exists */
    SELECT  username
    INTO    sUser
    FROM    dba_users
    WHERE   username = UPPER( '&&calpontuser' );
    
    RAISE_APPLICATION_ERROR(-20000, 'CALPONT user already exists.  If you wish to continue first DROP the CALPONT user if approriate.');
        
EXCEPTION
    WHEN NO_DATA_FOUND THEN
        /* Create the specified user */
        EXECUTE IMMEDIATE 'CREATE USER &&calpontuser' || ' IDENTIFIED BY "&&calpontuserpwd" DEFAULT TABLESPACE &&calpontts ' || 
            ' TEMPORARY TABLESPACE TEMP QUOTA UNLIMITED ON &&calpontts';
        
        /* Grant user the appropriate privileges */
    EXECUTE IMMEDIATE 'GRANT CREATE ANY SYNONYM TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE ANY TRIGGER TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE ANY TYPE TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE ANY VIEW TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE PUBLIC DATABASE LINK TO &&calpontuser';    
    EXECUTE IMMEDIATE 'GRANT CREATE LIBRARY TO &&calpontuser';   
    EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO &&calpontuser WITH ADMIN OPTION';
    EXECUTE IMMEDIATE 'GRANT CREATE SEQUENCE TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO &&calpontuser WITH ADMIN OPTION';
    EXECUTE IMMEDIATE 'GRANT CREATE SYNONYM TO &&calpontuser WITH ADMIN OPTION';
    EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO &&calpontuser WITH ADMIN OPTION';
    EXECUTE IMMEDIATE 'GRANT CREATE TRIGGER TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE USER TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO &&calpontuser WITH ADMIN OPTION';
    EXECUTE IMMEDIATE 'GRANT DROP ANY SYNONYM TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT DROP ANY TABLE TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT DROP ANY TRIGGER TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT DROP ANY TYPE TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT DROP ANY VIEW TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT DROP PUBLIC DATABASE LINK TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO &&calpontuser WITH ADMIN OPTION';
    EXECUTE IMMEDIATE 'GRANT SELECT ANY TABLE TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT EXECUTE ANY TYPE TO &&calpontuser';
    EXECUTE IMMEDIATE 'GRANT ANALYZE ANY TO &&calpontuser WITH ADMIN OPTION';
END;
/
spool off
