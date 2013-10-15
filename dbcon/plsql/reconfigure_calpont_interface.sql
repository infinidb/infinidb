/*******************************************************************************
*  $Id: reconfigure_calpont_interface.sql 2646 2007-06-08 21:02:42Z jlowe $
*  Script Name:    reconfigure_calpont_interface.sql
*  Date Created:   2006.09.07
*  Author:         Jason Lowe
*  Purpose:        Script to recompile the package pkg_calpont to use a different
*                  Calpont interface.  Either HSODBC or PL/SQL.  This is also
*                  useful if you compiled pkg_calpont without altering your session
*                  with the required inquiry directive.
*                  NOTE:  AS OF 6/8/07 THIS IS NOT NEEDED BECAUSE HSODBC CALPONT
*                         INTERFACE HAS BEEN DISABLED.
/******************************************************************************/
spool reconfigure_calpont_interface.log

SET VERIFY OFF

PROMPT Specify the Calpont system user you wish to reconfigure the Calpont interface for (i.e. CALPONT):;
DEFINE calpontuser = &1
--PROMPT Specify the password of the Calpont system user:;
DEFINE calpontpwd = CALPONT
PROMPT Specify which interface you would like to reconfigure for, either HSODBC (valid value = H) or PL/SQL (valid value = C):;
DEFINE calpontinterface = &3

CONNECT &&calpontuser/&&calpontpwd@&_CONNECT_IDENTIFIER
SET ECHO ON

WHENEVER SQLERROR EXIT;

/* Setup session for conditional compliation based on calpontinterface variable */
VARIABLE calInterface VARCHAR2(1);
EXEC :calInterface := '&&calpontinterface';
DECLARE
    eInvalidCalpontInterface    EXCEPTION;
    sDBLink                     VARCHAR2(255) := NULL;
    CURSOR cDBLink IS
        SELECT  db_link
        FROM    user_db_links
        WHERE   db_link LIKE 'CALPONT%';

BEGIN
    IF UPPER(:calInterface) != 'H' AND UPPER(:calInterface) != 'C' THEN
        RAISE eInvalidCalpontInterface;
    END IF;

    FOR rDBLink IN cDBLink LOOP
        -- Only expect on record
        sDBLink := rDBLink.db_link;
    END LOOP;

    IF UPPER(:calInterface) = 'H' THEN
        EXECUTE IMMEDIATE 'ALTER SESSION SET PLSQL_CCFLAGS = ''bHSODBC:true''';
        IF sDBLink IS NULL THEN
            EXECUTE IMMEDIATE 'CREATE PUBLIC DATABASE LINK CALPONT CONNECT TO DHARMA IDENTIFIED BY TEMP USING ''CALPONT''';
        END IF;
    ELSE
        EXECUTE IMMEDIATE 'ALTER SESSION SET PLSQL_CCFLAGS = ''bHSODBC:false''';
        IF sDBLink IS NOT NULL THEN
            EXECUTE IMMEDIATE 'DROP PUBLIC DATABASE LINK CALPONT';
        END IF;
    END IF;
EXCEPTION
    WHEN eInvalidCalpontInterface THEN
        dbms_output.put_line('Invalid Calpont interface provided!');
        RAISE;
END;
/

ALTER PACKAGE pkg_calpont COMPILE;

spool off

EXIT;
