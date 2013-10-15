/*
*  $Id: install_pkg_calpont.sql 4799 2009-01-26 20:36:33Z bwelch $
*/
-- Start of DDL Script for Package Body PKG_CALPONT
CREATE OR REPLACE
PACKAGE pkg_calpont IS
/*******************************************************************************
*
*   Purpose: Contains functionality to handle user DML, DDL, and queries
*
*   MODIFICATION HISTORY:
*
*   Person                  Date            Comments
*   ---------               ----------      ------------------------------------
*   Sean Turner - Enkitec   2006.07.25      Initial package creation
*   Jason Lowe              2006.08.22      Enhancements
*   Jason Lowe              2007.07.30      Enhancements for Alter Table Add Column (bug 245)
*
*******************************************************************************/

/******************************************************************************/
/* Global Variables */
/******************************************************************************/
g_nErrorId          cal_error_code.error_id%TYPE;
g_sErrorDesc        cal_error_code.error_desc%TYPE;
g_sSQL              VARCHAR2(32000);
g_xException        EXCEPTION;

/******************************************************************************/
/* Global Constants */
/******************************************************************************/

/* Remote DDL/DML execution methods */
RM_HS               CONSTANT    CHAR(1) := 'H';
RM_CCALL            CONSTANT    CHAR(1) := 'C';

/* Object Types */
OT_TABLE            CONSTANT    CHAR(5) := 'TABLE';
OT_INDEX            CONSTANT    CHAR(5) := 'INDEX';

/* Occurrences (Triggers) */
OC_AFTER            CONSTANT    CHAR(5) := 'AFTER';
OC_BEFORE           CONSTANT    CHAR(6) := 'BEFORE';

/* DDL Actions */
DA_ALTER            CONSTANT    CHAR(5) := 'ALTER';
DA_CREATE           CONSTANT    CHAR(6) := 'CREATE';
DA_DROP             CONSTANT    CHAR(4) := 'DROP';
DA_RENAME           CONSTANT    CHAR(6) := 'RENAME';

/* Remote Data Source */
/* This also had to be hardcoded in cal_remote_ddl */
REMOTE_DATA_SOURCE  CONSTANT    CHAR(7) := 'CALPONT';

/* CALPONT user password, hardcoded for now will need to be moved external later */
CALPONT_USER_PASSWORD   CONSTANT    CHAR(7) := 'CALPONT';

/* CALPONT front end database SID */
CALPONT_LOCAL_SID       CONSTANT    CHAR(9) := 'CALFEDBMS';

/******************************************************************************/
/* Procedures/Functions */
/******************************************************************************/

--PROCEDURE cal_capture_sessionid(
--    i_nSessionId    IN  v$session.sid%TYPE );

PROCEDURE cal_create_table (
    i_sOwner          IN  VARCHAR2,
    i_sObject         IN  VARCHAR2,
    i_sColwDTypeList  IN  VARCHAR2,
    i_sColList        IN  VARCHAR2,
    i_sMethod         IN  VARCHAR2 );

PROCEDURE cal_drop_table (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_sMethod   IN  VARCHAR2 );

PROCEDURE cal_alter_table (
    i_sOwner          IN  VARCHAR2,
    i_sObject         IN  VARCHAR2,
    i_sMethod         IN  VARCHAR2,
    i_sObjectNewName  IN  VARCHAR2,
    i_sColList        IN  VARCHAR2,
    i_sColwDTypeList  IN  VARCHAR2 );

PROCEDURE cal_after_ddl (
    i_nActionId       IN  cal_action_log.action_id%TYPE,
    i_sOwner          IN  cal_action_log.object_owner%TYPE,
    i_sObject         IN  cal_action_log.object_name%TYPE,
    i_sObjectNewName  IN  all_objects.object_name%TYPE,
    i_sObjectType     IN  all_objects.object_type%TYPE,
    i_sDDL            IN  VARCHAR2,
    i_sMethod         IN  VARCHAR2,
    i_nSessionId      IN  NUMBER );

PROCEDURE cal_register_object_owner (
    i_sUser    IN  VARCHAR2,
    i_sUserPwd  IN  VARCHAR2,
    i_bRegister   IN  BOOLEAN := TRUE );
    
PROCEDURE cal_create_dml_trigger (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_sView     IN  VARCHAR2 );

PROCEDURE cal_create_logon_trigger (
    i_sUser   IN  all_objects.owner%TYPE );
    
PROCEDURE cal_create_ddl_triggers (
    i_sUser   IN  all_objects.owner%TYPE,
    i_sMethod   IN  VARCHAR2 );
    
END;
/

CREATE OR REPLACE
PACKAGE BODY pkg_calpont IS
/******************************************************************************/
FUNCTION cal_get_columns (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_bWithType IN  BOOLEAN := FALSE ) RETURN VARCHAR2 IS

    /* Variable Declarations */
    sCols           VARCHAR2(32000);
    sColsWithType   VARCHAR2(32000);
    CURSOR cCol(sObject VARCHAR2, sOwner VARCHAR2) IS
        SELECT      column_name, data_type, data_length, data_scale, data_precision, column_name || ' ' ||
                    SUBSTR( DECODE( data_type_owner, NULL, NULL, data_type_owner || '.' ) || data_type ||
                        DECODE( data_type, 'VARCHAR2', '(' || data_length || ')', 'CHAR', '(' || data_length || ')',
                            'VARCHAR', '(' || data_length || ')', 'NUMBER',
                            DECODE( data_precision, NULL, NULL, '(' || data_precision ||
                                DECODE( data_scale, NULL, NULL, 0, NULL, ',' || data_scale ) || ')' ), 'FLOAT',
                            DECODE( data_precision, NULL, NULL, '(' || data_precision || ')' ) ), 1, 80 ) col_with_type
        FROM        dba_tab_columns
        WHERE       table_name = UPPER( sObject )
        AND         owner = UPPER( sOwner )
        ORDER BY    column_id;
BEGIN
    dbms_output.put_line('In cal_get_columns');
    FOR rCol IN cCol(i_sObject, i_sOwner) LOOP
        IF sCols IS NULL THEN
            sCols := rCol.column_name;
            sColsWithType := rCol.col_with_type;
        ELSE
            sCols := sCols || ', ' || rCol.column_name;
            sColsWithType := sColsWithType || ', ' || rCol.col_with_type;
        END IF;
    END LOOP;

    IF i_bWithType THEN
        dbms_output.put_line('In cal_get_columns - sColsWithType = ' || sColsWithType);
        RETURN sColsWithType;
    ELSE
        dbms_output.put_line('In cal_get_columns - sCols = ' || sCols);
        RETURN sCols;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_get_columns EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        RETURN NULL;
END cal_get_columns;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_shadow_name (
    i_sObject   IN  VARCHAR2 ) RETURN VARCHAR2 IS

BEGIN
    RETURN 'S_' || SUBSTR( i_sObject, 1, 28 );
END cal_get_shadow_name;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_object_name (
    i_sObject   IN  VARCHAR2 ) RETURN VARCHAR2 IS

BEGIN
    RETURN SUBSTR( i_sObject, 3, 28 );
END cal_get_object_name;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_trigger_name (
    i_sObject   IN  VARCHAR2 ) RETURN VARCHAR2 IS

    /* Variable Declarations */

BEGIN
    RETURN 'TR_' || SUBSTR( i_sObject, 1, 27 );
END cal_get_trigger_name;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_view_name (
    i_sObject   IN  VARCHAR2 ) RETURN VARCHAR2 IS

    /* Variable Declarations */

BEGIN
    RETURN 'VW_' || SUBSTR( i_sObject, 1, 27 );
END cal_get_view_name;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_obj_type_name (
    i_sObject   IN  VARCHAR2 ) RETURN VARCHAR2 IS

    /* Variable Declarations */

BEGIN
    RETURN 'OT_' || SUBSTR( i_sObject, 1, 27 );
END cal_get_obj_type_name;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_tbl_type_name (
    i_sObject   IN  VARCHAR2 ) RETURN VARCHAR2 IS

    /* Variable Declarations */

BEGIN
    RETURN 'TT_' || SUBSTR( i_sObject, 1, 27 );
END cal_get_tbl_type_name;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_get_object_owner (
    i_sObject   IN  VARCHAR2 ) RETURN all_objects.owner%TYPE IS

    /* Variable Declarations */
    sOwner  all_objects.object_type%TYPE;

BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_get_object_owner');

    SELECT  DISTINCT owner
    INTO    sOwner
    FROM    all_objects
    WHERE   object_name = i_sObject;

    RETURN sOwner;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_get_object_owner EXCEPTION HANDLER: ' || SQLCODE || ' ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_get_object_owner;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_data_objects (
    i_sOwner          IN  VARCHAR2,
    i_sObject         IN  VARCHAR2,
    i_sColwDTypeList  IN VARCHAR2) IS

    /* Variable Declarations */

BEGIN
    dbms_output.put_line('In cal_create_data_objects');
    /* Create the object type based on the columns of the passed in table */
    g_sSQL := 'CREATE TYPE ' || i_sOwner || '.' || cal_get_obj_type_name(i_sObject) || ' AS OBJECT ( ' ||
        i_sColwDTypeList || ' )';
    EXECUTE IMMEDIATE g_sSQL;
    dbms_output.put_line('In cal_create_data_objects - g_sSQL = ' || g_sSQL);
    /* create type as table of object */
    g_sSQL := 'CREATE TYPE ' || i_sOwner || '.' || cal_get_tbl_type_name(i_sObject) || ' AS TABLE OF ' || i_sOwner || '.' || cal_get_obj_type_name(i_sObject);
    EXECUTE IMMEDIATE g_sSQL;
    dbms_output.put_line('In cal_create_data_objects - g_sSQL = ' || g_sSQL);
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_create_data_objects EXCEPTION HANDLER: ' || SQLCODE || ' ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_create_data_objects;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_data_objects (
    i_sOwner  IN  VARCHAR2,
    i_sObject IN  VARCHAR2 ) IS

    /* Variable Declarations */

BEGIN
    dbms_output.put_line('In cal_drop_data_objects');
    /* Drop table type */
    g_sSQL := 'DROP TYPE ' || i_sOwner || '.' || cal_get_tbl_type_name(i_sObject);
    EXECUTE IMMEDIATE g_sSQL;

    /* Drop object type */
    g_sSQL := 'DROP TYPE ' || i_sOwner || '.' || cal_get_obj_type_name(i_sObject);
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_drop_data_objects EXCEPTION HANDLER: ' || SQLCODE || ' ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_data_objects;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_synonym (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_sName     IN  VARCHAR2 ) IS

    /* Variable Declarations */
    nJob  NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_create_synonym');
    g_sSQL := 'CREATE SYNONYM ' || i_sOwner || '.' || i_sObject || ' FOR ' || i_sName;
    EXECUTE IMMEDIATE g_sSQL;
 EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_create_synonym EXCEPTION HANDLER: ' || SQLCODE || ' ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_create_synonym;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_synonym (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_drop_synonym_alter_table');
    g_sSQL :='DROP SYNONYM ' || i_sOwner || '.' || i_sObject;
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_drop_synonym_alter_table EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_synonym;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_view (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_sView     IN  VARCHAR2,
    i_sColList  IN  VARCHAR2 ) IS

    /* Variable Declarations */
    sOwner      all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );
BEGIN
/*ZZ*/
    DBMS_OUTPUT.PUT_LINE('In cal_create_view');
    g_sSQL := 'CREATE OR REPLACE VIEW ' || i_sOwner || '.' || i_sView || ' AS ' || '
              SELECT ' || i_sColList || ' FROM TABLE( ' || sOwner || '.cal_table_scan( SYS_CONTEXT( ''USERENV'', ''SESSIONID'' ), ''' || i_sOwner || ''', ''' ||
              i_sObject || ''', ''' || i_sOwner  || ''', ''' || cal_get_tbl_type_name(i_sObject) || ''', ''' || cal_get_obj_type_name(i_sObject) || ''', SYS_CONTEXT( ''USERENV'', ''CURRENT_SCHEMA'' ), calpont.cal_get_bind_values() ))';
    dbms_output.put_line('In cal_create_view - g_sSQL = ' || g_sSQL);
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_create_view EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_create_view;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_view (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2 ) IS

    /* Variable Declarations */
    nJob        NUMBER;
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_drop_view');
    /* Drop the function-based view of remote data */
    g_sSQL := 'DROP VIEW ' || i_sOwner || '.' || cal_get_view_name( i_sObject );
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_drop_view EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_view;
/******************************************************************************/

/******************************************************************************/
FUNCTION cal_object_exists(
    i_sObject   IN  all_objects.object_name%TYPE ) RETURN BOOLEAN IS

    /* Variable Declarations */
    sObject all_objects.object_name%TYPE;
BEGIN
    dbms_output.put_line('In cal_object_exists');
    SELECT  object_name
    INTO    sObject
    FROM    user_objects
    WHERE   object_name = UPPER( i_sObject );

    RETURN TRUE;
EXCEPTION
    WHEN others THEN
        dbms_output.put_line('In cal_object_exists - EXCEPTION HANDLER - Object not found');
        RETURN FALSE;
END cal_object_exists;
/******************************************************************************/

--/******************************************************************************/
--PROCEDURE cal_capture_sessionid(
--    i_nSessionId    IN  v$session.sid%TYPE ) IS
--
--    /* Variable Declarations */
--
--BEGIN
--    /* Select i_nSessionId from virtual table through HSODBC for capture; to be used
--       as a unique identifier for later callbacks */
--
--    --SELECT i_nSessionId FROM syssession@REMOTE_DATA_SOURCE;
--    --SELECT * FROM syssession@REMOTE_DATA_SOURCE WHERE sid = i_nSessionId;
--    null;
--EXCEPTION
--    WHEN OTHERS THEN
--        g_nErrorId := ABS( SQLCODE );
--        g_sErrorDesc := SQLERRM;
--        RAISE g_xException;
--END cal_capture_sessionid;
--/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_ddl_triggers (
    i_sUser   IN  all_objects.owner%TYPE,
    i_sMethod   IN  VARCHAR2 ) IS

    /* Variable Declarations */
    sOwner  all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );

BEGIN
    dbms_output.put_line('In cal_create_ddl_triggers');
    /* Ensure that the specified user is not the same as the current one */
    IF UPPER( i_sUser ) = sOwner THEN
        g_nErrorId := -20000;
        g_sErrorDesc := 'Specified user cannot be the same as the Calpont user.';
        RAISE g_xException;
    END IF;

    /* Create the after DDL trigger on the specified user */
    g_sSQL := 'CREATE OR REPLACE TRIGGER TR_ADDL_' || i_sUser || ' ' || CHR(10) || CHR(9) ||
        'AFTER CREATE OR ALTER OR DROP OR RENAME ON ' || i_sUser || '.SCHEMA' || CHR(10) ||
        'DECLARE' || CHR(10) || CHR(9) ||
        'nActionId       cal_action_log.action_id%TYPE;' || CHR(10) || CHR(9) ||
        'nCount          INTEGER;' || CHR(10) || CHR(9) ||
        'sAction         cal_action_log.action_name%TYPE     := ora_sysevent;' || CHR(10) || CHR(9) ||
        'sObject         cal_action_log.object_name%TYPE     := ora_dict_obj_name;' || CHR(10) || CHR(9) ||
        'sOwner          cal_action_log.object_owner%TYPE    := ora_dict_obj_owner;' || CHR(10) || CHR(9) ||
        'sObjectType     all_objects.object_type%TYPE        := ora_dict_obj_type;' || CHR(10) || CHR(9) ||
        'sObjectNewName  all_objects.object_name%TYPE        := NULL;' || CHR(10) || CHR(9) ||
        'sDDL            VARCHAR2(32000);' || CHR(10) || CHR(9) ||
        'sDDLText        ora_name_list_t;' || CHR(10) || CHR(9) ||
        'sCurrActionId   VARCHAR2(42);' || CHR(10) || CHR(9) ||
        'sCurrAction     cal_action_log.action_name%TYPE;' || CHR(10) || CHR(9) ||
        'nSessionId      NUMBER;' || CHR(10) || CHR(9) ||
        'sTriggerRunning VARCHAR2(64)                        := NULL;' || CHR(10) ||
        'g_xException        EXCEPTION;' || CHR(10) || CHR(9) ||
        'BEGIN' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''TR_ADDL_' || i_sUser || ' FIRED!'');' || CHR(10) || CHR(9) || CHR(9) ||
        '/* Get SessionId for shadow user this trigger is defined and being executed for */' || CHR(10) || CHR(9) ||
        'nSessionId := SYS_CONTEXT( ''USERENV'', ''SESSIONID'' );' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''In TR_ADDL_' || i_sUser || ' - nSessionId = '' || nSessionId);' || CHR(10) || CHR(9) ||
        '/* Rebuild the DDL statement */' || CHR(10) || CHR(9) ||
        'nCount := ora_sql_txt( sDDLText );' || CHR(10) ||CHR(9) ||
        'FOR i IN 1..nCount LOOP' || CHR(10) || CHR(9) || CHR(9) ||
        '  sDDL := sDDL || sDDLText(i);' || CHR(10) || CHR(9) ||
        'END LOOP;' || CHR(10) || CHR(10) ||  CHR(9) ||
        '/* Log the action, and set nActionId and sAction in session for tracking */' || CHR(10) ||  CHR(9) ||
        'pkg_logging.cal_insert_log( nActionId, sAction, sOwner, sObject, sDDL, USER, SYSDATE );' || CHR(10) || CHR(10) || CHR(9) ||
        '/* If a "RENAME" was issued, modify the action to "RENAME" instead of "ALTER" unless renaming a COLUMN.  sAction will already be RENAME if RENAME T1 TO T2 syntax used. */' || CHR(10) || CHR(9) ||
        'IF (sAction=''ALTER'') AND (INSTR(UPPER(sDDL),''RENAME'')>0) AND NOT (INSTR(UPPER(sDDL),''COLUMN'')>0) THEN' || CHR(10) || CHR(9) || CHR(9) ||
        '  sAction := ''RENAME'';' || CHR(10) || CHR(9) ||
        'END IF;' || CHR(10) || CHR(9) ||
        'IF sAction = ''RENAME''  THEN' || CHR(10) || CHR(9) || CHR(9) ||
        '  /* Lookup new object name if ALTER TABLE T1 RENAME TO T2 or RENAME T1 TO T2 syntax used */' || CHR(10) || CHR(9) || CHR(9) ||
        '  SELECT TRIM( SUBSTR( UPPER( sql_text ), INSTR( UPPER( sql_text ), '' TO '' ) + 4 ) )' || CHR(10) || CHR(9) || CHR(9) ||
        '    INTO sObjectNewName' || CHR(10) || CHR(9) || CHR(9) ||
        '    FROM v$open_cursor'  || CHR(10) || CHR(9) || CHR(9) ||
        '   WHERE UPPER( sql_text ) LIKE ''%RENAME%'';' || CHR(10) || CHR(9) || CHR(9) ||
        'END IF;' || CHR(10) || CHR(10) || CHR(9) ||
        'IF sObjectType IN ( ''TABLE'', ''INDEX'' ) THEN' || CHR(10) || CHR(9) || CHR(9) ||
        '  dbms_output.put_line(''In TR_ADDL_' || i_sUser || ' PROCESSING!'');' || CHR(10) || CHR(9) ||
        '  dbms_output.put_line(''In TR_ADDL_' || i_sUser || ' - sAction = '' || ' || 'sAction' || ' || ' || ''', sObjectType = '' || ' || 'sObjectType' || ' || ' || ''', sOwner = '' || ' || 'sOwner' || ' || ' || ''', sObject = '' || ' || 'sObject' || ');' || CHR(10) || CHR(9) || CHR(9) ||
        '  dbms_output.put_line(''In TR_ADDL_' || i_sUser || ' - sDDL = '' || ' || 'sDDL' || ');' || CHR(10) || CHR(9) || CHR(9) ||
        '  pkg_calpont.cal_after_ddl( nActionId, sOwner, sObject, sObjectNewName, sObjectType, sDDL, ''' || i_sMethod || ''', nSessionId );' || CHR(10) || CHR(9) ||
        'END IF;' || CHR(10) ||
        'EXCEPTION' || CHR(10) || CHR(9) ||
        '  WHEN OTHERS THEN' || CHR(10) || CHR(9) || CHR(9) ||
        '    dbms_output.put_line(''In TR_ADDL_' || i_sUser || ' EXCEPTION HANDLER ''' || ' || ' || 'SQLCODE' || ' || ' || ''', ''' || ' || ' || 'SQLERRM' || ');' || CHR(10) ||
        ' RAISE;' || CHR(10) ||
        'END;';

    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_create_ddl_triggers - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE g_xException;
END cal_create_ddl_triggers;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_ddl_triggers(
    i_sUser    IN  all_objects.owner%TYPE ) IS

    /* Variable Declarations */
    sTrigger    all_triggers.trigger_name%TYPE;

BEGIN
    dbms_output.put_line('In cal_drop_ddl_triggers');

    sTrigger := 'TR_ADDL_' || UPPER ( i_sUser );
    IF cal_object_exists( sTrigger ) THEN
        g_sSQL := 'DROP TRIGGER ' || sTrigger;
        dbms_output.put_line('In cal_drop_ddl_triggers - g_sSQL = ' || g_sSQL);
        EXECUTE IMMEDIATE g_sSQL;
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_drop_ddl_triggers - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_ddl_triggers;
/******************************************************************************/

--/******************************************************************************/
--PROCEDURE cal_create_logon_trigger (
--    i_sUser   IN  all_objects.owner%TYPE ) IS
--
--    /* Variable Declarations */
--    sOwner  all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );
--
--BEGIN
--    dbms_output.put_line('In cal_create_logon_trigger');
--    /* Ensure that the specified user is not the same as the current one */
--    IF UPPER( i_sUser ) = sOwner THEN
--        g_nErrorId := -20000;
--        g_sErrorDesc := 'Specified user cannot be the same as the Calpont user.';
--        RAISE g_xException;
--    END IF;
--
--    /* Create the after logon trigger on the specified user */
--    g_sSQL := 'CREATE OR REPLACE TRIGGER TR_ALO_' || i_sUser || ' ' || CHR(10) || CHR(9) ||
--              'AFTER LOGON ON ' || i_sUser || '.SCHEMA' || CHR(10) ||
--              'DECLARE' || CHR(10) || CHR(9) ||
--              '  nSessionId  v$session.sid%TYPE;' || CHR(10) ||
--              'BEGIN' || CHR(10) || CHR(9) ||
--              'dbms_output.put_line(''TR_ALO_' || i_sUser || ' FIRED!'');' || CHR(10) || CHR(9) ||
--              '/* Retrieve the sessionId for the user that just logged in */' || CHR(10) || CHR(9) ||
--              'SELECT SYS_CONTEXT( ''USERENV'', ''SESSIONID'' )' || CHR(10) || CHR(9) ||
--              '  INTO nSessionId' || CHR(10) || CHR(9) ||
--              '  FROM DUAL;' || CHR(10) || CHR(9) ||
--              '/* Pass sessionId through HSODBC for capture to provide unique identifier for later callbacks */' || CHR(10) || CHR(9) ||
--              'pkg_calpont.cal_capture_sessionid( nSessionId );' || CHR(10) ||
--              'EXCEPTION' || CHR(10) || CHR(9) ||
--              '  WHEN OTHERS THEN' || CHR(10) || CHR(9) ||
--              '    dbms_output.put_line(''In TR_ALO_' || i_sUser || ' EXCEPTION HANDLER ''' || ' || ' || 'SQLCODE' || ' || ' || ''', ''' || ' || ' || 'SQLERRM' || ');' || CHR(10) ||
--              'END;';
--    EXECUTE IMMEDIATE g_sSQL;
--EXCEPTION
--    WHEN OTHERS THEN
--        dbms_output.put_line('In cal_create_logon_trigger - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
--        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
--        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
--        RAISE g_xException;
--END cal_create_logon_trigger;
--/******************************************************************************/
--
--/******************************************************************************/
--PROCEDURE cal_drop_logon_trigger(
--    i_sUser    IN  all_objects.owner%TYPE ) IS
--
--    /* Variable Declarations */
--    sTrigger    all_triggers.trigger_name%TYPE;
--
--BEGIN
--    dbms_output.put_line('In cal_drop_logon_trigger');
--
--    sTrigger := 'TR_ALO_' || UPPER ( i_sUser );
--    IF cal_object_exists( sTrigger ) THEN
--        g_sSQL := 'DROP TRIGGER ' || sTrigger;
--        dbms_output.put_line('In cal_drop_logon_triggers - g_sSQL = ' || g_sSQL);
--        EXECUTE IMMEDIATE g_sSQL;
--    END IF;
--
--EXCEPTION
--    WHEN OTHERS THEN
--        dbms_output.put_line('In cal_drop_logon_trigger - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
--        g_nErrorId := ABS( SQLCODE );
--        g_sErrorDesc := SQLERRM;
--        RAISE g_xException;
--END cal_drop_logon_trigger;
--/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_logon_trigger (
    i_sUser   IN  all_objects.owner%TYPE ) IS

    /* Variable Declarations */
    sOwner  all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );

BEGIN
    dbms_output.put_line('In cal_create_logon_trigger');
    /* Ensure that the specified user is not the same as the current one */
    IF UPPER( i_sUser ) = sOwner THEN
        g_nErrorId := -20000;
        g_sErrorDesc := 'Specified user cannot be the same as the Calpont user.';
        RAISE g_xException;
    END IF;

    /* Create the after logon trigger on the specified user */
    g_sSQL := 'CREATE OR REPLACE TRIGGER TR_ALO_' || i_sUser || ' ' || CHR(10) || CHR(9) ||
              'AFTER LOGON ON ' || i_sUser || '.SCHEMA' || CHR(10) ||
              'DECLARE' || CHR(10) || CHR(9) ||
              '  nSessionId  v$session.sid%TYPE;' || CHR(10) ||
              'BEGIN' || CHR(10) || CHR(9) ||
              'dbms_output.put_line(''TR_ALO_' || i_sUser || ' FIRED!'');' || CHR(10) || CHR(9) ||
              '/* Retrieve the sessionId for the user that just logged in */' || CHR(10) || CHR(9) ||
              'SELECT SYS_CONTEXT( ''USERENV'', ''SESSIONID'' )' || CHR(10) || CHR(9) ||
              '  INTO nSessionId' || CHR(10) || CHR(9) ||
              '  FROM DUAL;' || CHR(10) || CHR(9) ||
              '/* Call Calpont passing sessionId to log user on to Calpont */' || CHR(10) || CHR(9) ||
              sOwner || '.cal_logon( nSessionId, ''' || i_sUser || ''' );' || CHR(10) ||
              'EXCEPTION' || CHR(10) || CHR(9) ||
              '  WHEN OTHERS THEN' || CHR(10) || CHR(9) ||
              '    dbms_output.put_line(''In TR_ALO_' || i_sUser || ' EXCEPTION HANDLER ''' || ' || ' || 'SQLCODE' || ' || ' || ''', ''' || ' || ' || 'SQLERRM' || ');' || CHR(10) ||
              'END;';
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_create_logon_trigger - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE g_xException;
END cal_create_logon_trigger;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_logon_trigger(
    i_sUser    IN  all_objects.owner%TYPE ) IS

    /* Variable Declarations */
    sTrigger    all_triggers.trigger_name%TYPE;

BEGIN
    dbms_output.put_line('In cal_drop_logon_trigger');

    sTrigger := 'TR_ALO_' || UPPER ( i_sUser );
    IF cal_object_exists( sTrigger ) THEN
        g_sSQL := 'DROP TRIGGER ' || sTrigger;
        dbms_output.put_line('In cal_drop_logon_triggers - g_sSQL = ' || g_sSQL);
        EXECUTE IMMEDIATE g_sSQL;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_drop_logon_trigger - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_logon_trigger;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_logoff_trigger (
    i_sUser   IN  all_objects.owner%TYPE ) IS

    /* Variable Declarations */
    sOwner  all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );

BEGIN
    dbms_output.put_line('In cal_create_logoff_trigger');
    /* Ensure that the specified user is not the same as the current one */
    IF UPPER( i_sUser ) = sOwner THEN
        g_nErrorId := -20000;
        g_sErrorDesc := 'Specified user cannot be the same as the Calpont user.';
        RAISE g_xException;
    END IF;

    /* Create the before alter trigger on the specified user */
    g_sSQL := 'CREATE OR REPLACE TRIGGER TR_BLF_' || i_sUser || ' ' || CHR(10) || CHR(9) ||
              'BEFORE LOGOFF ON ' || i_sUser || '.SCHEMA' || CHR(10) ||
              'DECLARE' || CHR(10) || CHR(9) ||
              '  nSessionId  v$session.sid%TYPE;' || CHR(10) ||
              'BEGIN' || CHR(10) || CHR(9) ||
              'dbms_output.put_line(''TR_BLF_' || i_sUser || ' FIRED!'');' || CHR(10) || CHR(9) ||
              '/* Retrieve the sessionId for the user that just logged in */' || CHR(10) || CHR(9) ||
              'SELECT SYS_CONTEXT( ''USERENV'', ''SESSIONID'' )' || CHR(10) || CHR(9) ||
              '  INTO nSessionId' || CHR(10) || CHR(9) ||
              '  FROM DUAL;' || CHR(10) || CHR(9) ||
              '/* Call Calpont passing sessionId to log user off Calpont */' || CHR(10) || CHR(9) ||
              sOwner || '.cal_logoff( nSessionId );' || CHR(10) ||
              'EXCEPTION' || CHR(10) || CHR(9) ||
              '  WHEN OTHERS THEN' || CHR(10) || CHR(9) ||
              '    dbms_output.put_line(''In TR_BLF_' || i_sUser || ' EXCEPTION HANDLER ''' || ' || ' || 'SQLCODE' || ' || ' || ''', ''' || ' || ' || 'SQLERRM' || ');' || CHR(10) ||
              'END;';
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_create_logoff_trigger - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE g_xException;
END cal_create_logoff_trigger;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_logoff_trigger(
    i_sUser    IN  all_objects.owner%TYPE ) IS

    /* Variable Declarations */
    sTrigger    all_triggers.trigger_name%TYPE;

BEGIN
    dbms_output.put_line('In cal_drop_logoff_trigger');

    sTrigger := 'TR_BLF_' || UPPER ( i_sUser );
    IF cal_object_exists( sTrigger ) THEN
        g_sSQL := 'DROP TRIGGER ' || sTrigger;
        dbms_output.put_line('In cal_drop_logoff_trigger - g_sSQL = ' || g_sSQL);
        EXECUTE IMMEDIATE g_sSQL;
    END IF;

EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_drop_logoff_trigger - EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_logoff_trigger;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_dml_trigger (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_sView     IN  VARCHAR2 ) IS

    /* Variable Declarations */
    /* sOwner is current logged in user's current_schema from their session,
       but it is currently not used in cal_process_dml...candidate for removal */
    sOwner  all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );
    sTrName VARCHAR2(30) := cal_get_trigger_name( i_sObject );

BEGIN
    dbms_output.put_line('In cal_create_dml_trigger');

    /* DDL to create the instead of trigger for a new (remote) object */
    g_sSQL :=
      'CREATE OR REPLACE TRIGGER ' || i_sOwner || '.' || sTrName || CHR(10) || CHR(9) ||
        'INSTEAD OF INSERT OR DELETE OR UPDATE' || CHR(10) || CHR(9) ||
        'ON ' || i_sOwner || '.' || i_sView || CHR(10) || CHR(9) ||
        'REFERENCING OLD AS OLD NEW AS NEW' || CHR(10) ||
      'DECLARE' || CHR(10) || CHR(9) ||
        'sOwner      VARCHAR2(30);' || CHR(10) || CHR(9) ||
        'nSessionId  NUMBER;' || CHR(10) || CHR(9) ||
        'sDateFormat VARCHAR2(62);' || CHR(10) || CHR(9) ||
        'sDatetimeFormat VARCHAR2(62);' || CHR(10) || CHR(9) ||
        'nRc         INTEGER;' || CHR(10) ||
      'BEGIN' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''In ' || sTrName || ''');' || CHR(10) || CHR(10) || CHR(9) ||
        'sOwner := SYS_CONTEXT( ''USERENV'', ''CURRENT_SCHEMA'');' || CHR(10) || CHR(9) ||
        'nSessionId := SYS_CONTEXT( ''USERENV'', ''SESSIONID'');' || CHR(10) || CHR(10) || CHR(9) ||
        'select s.value into sDateFormat from nls_database_parameters d, nls_instance_parameters i, nls_session_parameters s where d.parameter = i.parameter (+) and d.parameter = s.parameter (+) and s.parameter=''NLS_DATE_FORMAT'';' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''In ' || sTrName || ' - sDateFormat = '' || sDateFormat);' || CHR(10) || CHR(9) ||
        'select s.value into sDatetimeFormat from nls_database_parameters d, nls_instance_parameters i, nls_session_parameters s where d.parameter = i.parameter (+) and d.parameter = s.parameter (+) and s.parameter=''NLS_TIMESTAMP_FORMAT'';' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''In ' || sTrName || ' - sDatetimeFormat = '' || sDatetimeFormat);' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''In ' || sTrName || ' - sOwner = '' || sOwner);' || CHR(10) || CHR(9) ||
        'dbms_output.put_line(''In ' || sTrName || ' - nSessionId = '' || nSessionId);' || CHR(10) || CHR(10) || CHR(9) ||
        '/* Call Calpont to process the DML statement */' || CHR(10) || CHR(9) ||
        'nRc := ' || sOwner || '.cal_process_dml( nSessionId, sDateFormat, sDatetimeFormat, sOwner, ''' || sOwner || ''', ''' || CALPONT_USER_PASSWORD || ''' );' || CHR(10) || CHR(9) ||
        'IF nRc = 0 THEN' || CHR(10) || CHR(9) || CHR(9) ||
          'dbms_output.put_line(''In ' || sTrName || ' - cal_process_dml was successful'');' || CHR(10) || CHR(9) ||
        'END IF;' || CHR(10) || CHR(10) ||
      'EXCEPTION' || CHR(10) || CHR(9) ||
        'WHEN OTHERS THEN' || CHR(10) || CHR(9) || CHR(9) ||
          'dbms_output.put_line(''In ' || sTrName || ' EXCEPTION HANDLER '' || SQLCODE || '', '' || SQLERRM);' || CHR(10) || CHR(9) || CHR(9) ||
          'RAISE;' || CHR(10) ||
      'END;';
    dbms_output.put_line(g_sSQL);
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_create_dml_trigger EXCEPTION HANDLER: ' || SQLCODE || ' ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_create_dml_trigger;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_dml_trigger (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    dbms_output.put_line('In cal_drop_dml_trigger');
    g_sSQL := 'DROP TRIGGER ' || i_sOwner || '.' || cal_get_trigger_name( i_sObject );
    EXECUTE IMMEDIATE g_sSQL;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_drop_dml_trigger EXCEPTION HANDLER: ' || SQLCODE || ' ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_drop_dml_trigger;
/******************************************************************************/

/*****************************************************************************
* This version of cal_remote_ddl was used to toggle between the HSODBC and
* regular PL/SQL interface to Calpont, since HSODBC is not an option now I am
* commenting it out for possible later use.  The new version below now removes
* the dependence on the compiler directive inquiry variable bHSODBC for pkg_calpont.
******************************************************************************/
--PROCEDURE cal_remote_ddl (i_nSessionId NUMBER,
--    i_sOwner    IN  all_objects.owner%TYPE,
--    i_sDDL      IN  VARCHAR2) IS
--
--    /* Variable Declarations */
--    /* $IF cond. structures are compiler directives using the inquiry variable bHSODBC */
--    $IF $$bHSODBC $THEN
--      nRows       INTEGER;
--      /* Start automous transaction because generic hsodbc gateway does not support
--      distributed transactions */
--      PRAGMA      AUTONOMOUS_TRANSACTION;
--    $ELSE
--      nRc         INTEGER;
--    $END
--
--BEGIN
--    dbms_output.put_line('In cal_remote_ddl');
--    $IF $$bHSODBC $THEN
--        /* Perform DDL using HSODBC passthrough */
--        nRows := DBMS_HS_PASSTHROUGH.EXECUTE_IMMEDIATE@CALPONT (i_sDDL);
--        COMMIT; /* autonomous transaction must be commited before returning */
--    $ELSIF NOT $$bHSODBC $THEN
--        /* Call to C function */
--        nRc := cal_process_ddl( i_nSessionId, i_sOwner, i_sDDL );
--        NULL;
--    $ELSE
--        dbms_output.put_line('In cal_remote_ddl - bHSODBC inquiry directive not supplied');
--        NULL;
--    $END
--EXCEPTION
--    WHEN OTHERS THEN
--        dbms_output.put_line('In cal_remote_ddl EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
--        g_nErrorId := ABS( SQLCODE );
--        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
--        RAISE g_xException;
--END cal_remote_ddl;
--/******************************************************************************/

/*****************************************************************************
* This version of cal_remote_ddl only supports the PL/SQL interface to Calpont.
******************************************************************************/
PROCEDURE cal_remote_ddl (i_nSessionId NUMBER,
    i_sOwner        IN  all_objects.owner%TYPE,
    i_sDDL          IN  VARCHAR2,
    o_tColValueSet  OUT COLVALUESET) IS

    /* Variable Declarations */
    nRc         INTEGER;

BEGIN
    dbms_output.put_line('In cal_remote_ddl');
    nRc := cal_process_ddl( i_nSessionId, i_sOwner, i_sDDL, o_tColValueSet );
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_remote_ddl EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE;
END cal_remote_ddl;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_table (
    i_sOwner          IN  VARCHAR2,
    i_sObject         IN  VARCHAR2,
    i_sColwDTypeList  IN  VARCHAR2,
    i_sColList        IN  VARCHAR2,
    i_sMethod         IN  VARCHAR2 ) IS

    /* Variable Declarations */
    sName       VARCHAR2(61);
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_create_table');
    IF i_sMethod = RM_HS THEN
        /* No views created -- create synonym name based on object name and data source */
        sName := i_sObject || '@' || REMOTE_DATA_SOURCE;
    ELSE
        sName := cal_get_view_name( i_sObject );
        /* Create object and table types for table being created */
        cal_create_data_objects( i_sOwner, i_sObject, i_sColwDTypeList );
        /* Create the view of the new table based on a function retrieving remote data */
        cal_create_view( i_sOwner, i_sObject, sName, i_sColList );
        /* Create the DML "instead of" trigger on new view */
        cal_create_dml_trigger( i_sOwner, i_sObject, sName );
        /* Add the owner name to the view name for use in creating the synonym against the view */
        sName := i_sOwner || '.' || sName;
    END IF;
    /* Create a synonym for sName in the user's original schema with their object name. */
    cal_create_synonym( i_sOwner, i_sObject, sName );
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_create_table EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE g_xException;
END cal_create_table;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_drop_table (
    i_sOwner    IN  VARCHAR2,
    i_sObject   IN  VARCHAR2,
    i_sMethod   IN  VARCHAR2 ) IS

    /* Variable Declarations */

BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_drop_table');
    cal_drop_synonym( i_sOwner, i_sObject );
    IF i_sMethod = RM_CCALL THEN
      /* Drop the instead of DML trigger on the function based view */
      cal_drop_dml_trigger ( i_sOwner, i_sObject );
      /* Drop the function-based view of remote data */
      cal_drop_view( i_sOwner, i_sObject );
      /* Drop the object and table types */
      cal_drop_data_objects ( i_sOwner, i_sObject );
    END IF;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_drop_table EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE g_xException;
END cal_drop_table;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_alter_table (
    i_sOwner          IN  VARCHAR2,
    i_sObject         IN  VARCHAR2,
    i_sMethod         IN  VARCHAR2,
    i_sObjectNewName  IN  VARCHAR2,
    i_sColList        IN  VARCHAR2,
    i_sColwDTypeList  IN  VARCHAR2 ) IS

    /* Variable Declarations */
    sSynonym  VARCHAR2(61)    := i_sObject;
    sObject   VARCHAR2(30)    := NULL;
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_alter_table');
    /* If proc called for "ALTER TABLE RENAME" or "RENAME" i_sObjectNewName will not be the 'NULL' literal string */
    IF i_sObjectNewName <> 'NULL' THEN
      sObject := i_sObjectNewName;
    ELSE
      /* Otherwise non-rename "ALTER TABLE" executed */
      sObject := i_sObject;
    END IF;

    /* Drop the synonym for the view or remote object */
    cal_drop_synonym( i_sOwner, i_sObject );
    IF i_sMethod = RM_CCALL THEN
        /* Drop Objects */
        /* Drop the instead of DML trigger on the function based view */
        cal_drop_dml_trigger( i_sOwner, i_sObject );
        /* Drop the function-based view of remote data */
        cal_drop_view( i_sOwner, i_sObject );
        /* Drop the object and table types */
        cal_drop_data_objects ( i_sOwner, i_sObject );

        /* Recreate Objects */
        /* Set the name of the synonym to the view name */
        sSynonym := cal_get_view_name( sObject );
        /* Create object and table types for table being altered */
        cal_create_data_objects( i_sOwner, i_sObject, i_sColwDTypeList );        
        /* Create the view of the altered table based on a function retrieving remote data */
        cal_create_view( i_sOwner, sObject, sSynonym, i_sColList );
        /* Create the DML "instead of" trigger on new view */
        cal_create_dml_trigger( i_sOwner, sObject, sSynonym );
        /* Add the owner name to the view name for use in creating the synonym against the view */
        sSynonym := i_sOwner || '.' || sSynonym;
    ELSE
        /* No views created -- create synonym name based on object name and data source */
        sSynonym := sObject || '@' || REMOTE_DATA_SOURCE;
    END IF;
    /* Re-create the synonym for the view or remote object */
    cal_create_synonym( i_sOwner, sObject, sSynonym );
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_alter_table EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        RAISE g_xException;
END cal_alter_table;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_after_ddl (
    i_nActionId         IN  cal_action_log.action_id%TYPE,
    i_sOwner            IN  cal_action_log.object_owner%TYPE,
    i_sObject           IN  cal_action_log.object_name%TYPE,
    i_sObjectNewName    IN  all_objects.object_name%TYPE,
    i_sObjectType       IN  all_objects.object_type%TYPE,
    i_sDDL              IN  VARCHAR2,
    i_sMethod           IN  VARCHAR2,
    i_nSessionId        IN  NUMBER ) IS

    /* Variable Declarations */
    /*Get the orginal user name from the shadow user name */
    sOwner          all_objects.owner%TYPE := cal_get_object_name(i_sOwner);
    sAction         cal_action_log.action_name%TYPE;
    sCalUser        all_objects.owner%TYPE := cal_get_object_owner( 'PKG_CALPONT' );
    iRC             INTEGER;
    sObjectNewName  all_objects.object_name%TYPE;
    sColwDTypeList  VARCHAR2(32000);
    sColList        VARCHAR2(32000);
    sSchemaSyncFlag INTEGER:=0;
    /* cal_remote_ddl will return COLVALUESET table type populated with the new columns and
       their attributes for ALTER TABLE ADD (columns(s)) statements, otherwise NULL. */
    tColValueSet    COLVALUESET;  
    sColwDType      VARCHAR2(175);    
    
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_after_ddl');

    /* Pass DDL action on object to remote data source */
    SELECT schema_sync INTO sSchemaSyncFlag
        FROM cal_registered_schema
        WHERE schema_owner = sOwner;
    IF sSchemaSyncFlag = 1 THEN
        cal_remote_ddl( i_nSessionId, sOwner, i_sDDL, tColValueSet );
    END IF;

    /* Look up action name by action id */
    sAction := pkg_logging.cal_get_action_name( i_nActionId );

    /* Convert i_sObjectNewName to 'NULL' literal if NULL, which will be handled appropriately in cal_alter_table */
    IF i_sObjectNewName IS NULL THEN
      sObjectNewName := 'NULL';
    ELSE
      sObjectNewName := i_sObjectNewName;
    END IF;

    IF i_sMethod = RM_CCALL AND i_sObjectType = OT_TABLE THEN
        /* Get column list with and without datatypes from shadow user */    
        IF sAction = DA_CREATE OR sAction = DA_ALTER THEN
            /* i_sObject is NOT being renamed, so use ORIGINAL object name in column lookup */
            /* Note:  For ALTER TABLE T1 ADD one or more columns syntax this only returnes
               the current columns, not including the new column(s) */  
            sColwDTypeList := cal_get_columns( i_sOwner, i_sObject, TRUE );
            sColList := cal_get_columns( i_sOwner, i_sObject );
            
            IF sAction = DA_ALTER THEN
              IF tColValueSet IS NOT NULL THEN 
                /* Add new columns for Alter Table Add Column statements returned in table tColValueSet from cal_remote_ddl. */
                DBMS_OUTPUT.PUT_LINE('In cal_after_ddl DA_ALTER - alter table add column(s)');
                /*column datatype is lower case in sync with ddlparser */
                FOR rNewCol IN (SELECT column_name, data_type, data_length, data_precision, data_scale
                                  FROM TABLE(tColValueSet)) LOOP
                    DBMS_OUTPUT.PUT_LINE('In cal_after_ddl DA_ALTER - rNewCol:  ' || rNewCol.column_name || ', ' || rNewCol.data_type || ', ' || rNewCol.data_length || ', ' || rNewCol.data_precision || ', ' || rNewCol.data_scale );                  
                    SELECT rNewCol.column_name || ' ' || SUBSTR( rNewCol.data_type ||
                           DECODE( rNewCol.data_type, 'varchar2', '(' || rNewCol.data_length || ')', 'char', '(' || rNewCol.data_length || ')',
                                'varchar', '(' || rNewCol.data_length || ')', 'number',
                                DECODE( rNewCol.data_precision, NULL, NULL, 0, NULL,'(' || rNewCol.data_precision ||
                                DECODE( rNewCol.data_scale, NULL, NULL, 0, NULL, ',' || rNewCol.data_scale ) || ')' ), 'decimal',
                                DECODE( rNewCol.data_precision, NULL, NULL,0, NULL, '(' || rNewCol.data_precision ||
                                DECODE( rNewCol.data_scale, NULL, NULL, 0, NULL, ',' || rNewCol.data_scale ) || ')' ), 'float',
                                DECODE( rNewCol.data_precision, NULL, NULL,0, NULL, '(' || rNewCol.data_precision || ')' ) ), 1, 80 )
                     INTO sColwDType
                     FROM DUAL;
                     sColwDTypeList := sColwDTypeList || ', ' || sColwDType;
                     sColList := sColList || ', ' || rNewCol.column_name;
                END LOOP;
                
                DBMS_OUTPUT.PUT_LINE('In cal_after_ddl DA_ALTER extended sColwDTypeList: ' || sColwDTypeList);
                DBMS_OUTPUT.PUT_LINE('In cal_after_ddl DA_ALTER extended sColList: ' || sColList);                
              END IF;            
            END IF;
            
        ELSIF sAction = DA_RENAME THEN
            /* i_sObject is being renamed, so use NEW object name in column lookup...for ALTER RENAME and RENAME */
            -- DEBUG: THIS DOES NOT WORK FOR ALTER TABLE RENAME or RENAME syntax...
            sColwDTypeList := cal_get_columns( i_sOwner, sObjectNewName, TRUE );
            sColList := cal_get_columns( i_sOwner, sObjectNewName );    
        END IF;
    ELSE
        /* Setting these to 'NULL' string because of how cal_execute_procedure deals with parameters */
        sColwDTypeList := 'NULL';
        sColList := 'NULL';
    END IF;

    CASE sAction
        WHEN DA_CREATE THEN
            CASE i_sObjectType
                WHEN OT_TABLE THEN
                  iRC := cal_execute_procedure( sCalUser, CALPONT_USER_PASSWORD,
                         CALPONT_LOCAL_SID, 'pkg_calpont.cal_create_table', sOwner, i_sObject, sColwDTypeList, sColList, i_sMethod, '' );
                ELSE NULL;
            END CASE;
        WHEN DA_DROP THEN
            CASE i_sObjectType
                WHEN OT_TABLE THEN
                  iRC := cal_execute_procedure( sCalUser, CALPONT_USER_PASSWORD,
                         CALPONT_LOCAL_SID, 'pkg_calpont.cal_drop_table', sOwner, i_sObject, i_sMethod, '', '', '' );
                ELSE NULL;
            END CASE;
        WHEN DA_ALTER THEN
            CASE i_sObjectType
                WHEN OT_TABLE THEN
                  iRC := cal_execute_procedure( sCalUser, CALPONT_USER_PASSWORD,
                         CALPONT_LOCAL_SID, 'pkg_calpont.cal_alter_table', sOwner, i_sObject, i_sMethod, sObjectNewName, sColList, sColwDTypeList );
                ELSE NULL;
            END CASE;
        WHEN DA_RENAME THEN
            CASE i_sObjectType
                WHEN OT_TABLE THEN
                  /* Currently only difference between DA_ALTER and DA_RENAME is sObjectNewName is not null */
                  iRC := cal_execute_procedure( sCalUser, CALPONT_USER_PASSWORD,
                         CALPONT_LOCAL_SID, 'pkg_calpont.cal_alter_table', sOwner, i_sObject, i_sMethod, sObjectNewName, sColList, sColwDTypeList );
                ELSE NULL;
            END CASE;
    END CASE;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_after_ddl EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := NVL( g_nErrorId, ABS( SQLCODE ) );
        g_sErrorDesc := NVL( g_sErrorDesc, SQLERRM );
        --commenting this out until connector and Calpont error handling more coordinated
        --pkg_error.cal_handle_error( i_nActionId, g_nErrorId, g_sSQL );
        RAISE;
END cal_after_ddl;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_create_shadow_user (
    i_sUser    IN  VARCHAR2,
    i_sUserPwd  IN  VARCHAR2 ) IS

    /* Variable Declarations */
    sDefTablespace   dba_users.default_tablespace%TYPE;
    sTempTablespace  dba_users.temporary_tablespace%TYPE;

BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_create_shadow_user');
    /* Create shadow user for i_sUser being passed in */
    SELECT default_tablespace, temporary_tablespace
      INTO sDefTablespace, sTempTablespace
      FROM dba_users
     WHERE username = UPPER( i_sUser );
    /* The shadow user to i_sUser will have password given by caller of this proc */
    EXECUTE IMMEDIATE 'CREATE USER S_' || SUBSTR( i_sUser, 1, 28 ) ||
                      ' IDENTIFIED BY ' || '"' || i_sUserPwd || '"' || ' DEFAULT TABLESPACE ' || sDefTablespace ||
                      ' TEMPORARY TABLESPACE ' || sTempTablespace || ' QUOTA UNLIMITED ON ' || sDefTablespace;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_create_shadow_user EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_create_shadow_user;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_grant_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_grant_privs');
    /* Grant privs to user */
    EXECUTE IMMEDIATE 'GRANT CREATE SESSION TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT ANALYZE ANY TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_grant_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_grant_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_revoke_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_revoke_privs');
    /* Revoke privs from specified user */
    EXECUTE IMMEDIATE 'REVOKE CREATE SESSION FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE ANALYZE ANY FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE SELECT ANY DICTIONARY FROM ' || i_sUser;
    
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_revoke_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_revoke_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_grant_appluser_obj_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_grant_appluser_obj_privs');
    /* Grant privs to user */
    /* Under current configuration assumptions these are the privs needed for the registered appl. user: jlowe 10/23/06, 6/19/08 */
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON cal_table_scan TO ' || i_sUser || ' WITH GRANT OPTION';
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON cal_process_dml TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON calcommit TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON calrollback TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON cal_setstats TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON cal_get_bind_values TO ' || i_sUser || ' WITH GRANT OPTION';
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON caltraceon TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON caltraceoff TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON cal_last_update_count TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON calsetenv TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON getstats TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON calsetparms TO ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_grant_appluser_obj_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_grant_appluser_obj_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_revoke_appluser_obj_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_revoke_appluser_obj_privs');
    /* Revoke privs from specified user */
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON cal_table_scan FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON cal_process_dml FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON calcommit FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON calrollback FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON cal_setstats FROM ' || i_sUser;  
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON cal_get_bind_values FROM ' || i_sUser;     
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON caltraceon FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON caltraceoff FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON cal_last_update_count FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON calsetenv FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON getstats FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON calsetparms FROM ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_revoke_appluser_obj_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_revoke_appluser_obj_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_grant_shaduser_obj_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_grant_shaduser_obj_privs');
    /* Grant privs to user */
    /* Under current configuration assumptions these are the privs needed for the registered shadow user: jlowe 10/23/06, 6/19/08 */
    EXECUTE IMMEDIATE 'GRANT EXECUTE ON calsetenv TO ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_grant_shaduser_obj_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_grant_shaduser_obj_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_revoke_shaduser_obj_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_revoke_shaduser_obj_privs');
    /* Revoke privs from specified user */
    EXECUTE IMMEDIATE 'REVOKE EXECUTE ON calsetenv FROM ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_revoke_shaduser_obj_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_revoke_shaduser_obj_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_grant_appluser_ddl_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_grant_appluser_ddl_privs');
    /* Grant privs to user */
    EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_grant_appluser_ddl_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_grant_appluser_ddl_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_revoke_appluser_ddl_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_revoke_appluser_ddl_privs');
    /* Grant privs to user */
    EXECUTE IMMEDIATE 'REVOKE CREATE PROCEDURE FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE CREATE VIEW FROM ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_revoke_appluser_ddl_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_revoke_appluser_ddl_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_grant_shaduser_ddl_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_grant_shaduser_ddl_privs');
    /* Grant privs to user */
    EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT CREATE VIEW TO ' || i_sUser;
    EXECUTE IMMEDIATE 'GRANT CREATE SYNONYM TO ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_grant_shaduser_ddl_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_grant_shaduser_ddl_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_revoke_shaduser_ddl_privs (
    i_sUser    IN  VARCHAR2 ) IS

    /* Variable Declarations */
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_revoke_shaduser_ddl_privs');
    /* Grant privs to user */
    EXECUTE IMMEDIATE 'REVOKE CREATE TABLE FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE CREATE VIEW FROM ' || i_sUser;
    EXECUTE IMMEDIATE 'REVOKE CREATE SYNONYM FROM ' || i_sUser;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.PUT_LINE('In cal_revoke_shaduser_ddl_privs EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        RAISE g_xException;
END cal_revoke_shaduser_ddl_privs;
/******************************************************************************/

/******************************************************************************/
PROCEDURE cal_register_object_owner (
    i_sUser       IN  VARCHAR2,
    i_sUserPwd    IN  VARCHAR2,
    i_bRegister   IN  BOOLEAN := TRUE ) IS

    /* Variable Declarations */
    sFlag            CHAR(1) := 'Y';
    sDefTablespace   dba_users.default_tablespace%TYPE;
    sTempTablespace  dba_users.temporary_tablespace%TYPE;
    nCount           NUMBER := 0;
    /* Creating and setting i_sMethod to 'C' to force PL/SQL Calpont interface.
       This procedure use to accept an i_sMethod parameter to allow
       toggling between HSODBC and PL/SQL Calpont interface.  The
       HSODBC interface is no longer needed at this time. */
    i_sMethod        VARCHAR2(1) := 'C';    
    sMethod          VARCHAR2(1);

BEGIN
    dbms_output.put_line('In cal_register_user');
    /* If interface method supplied is not equal to capital H or C then throw exception */
    IF UPPER(i_sMethod) != 'H' AND UPPER(i_sMethod) != 'C' THEN
        g_nErrorId := -20000;
        g_sErrorDesc := 'Interface method parameter must be either ''H'' for HSODBC or ''C'' for PL/SQL.';
        RAISE g_xException;
    END IF;
    /* If user to be registered is longer than 20 characters then throw exception */
    IF LENGTH(i_sUser) > 20 THEN
        g_nErrorId := -20000;
        g_sErrorDesc := 'User to be registered is longer than 20 characters.  Must be 20 characters or shorter.';
        RAISE g_xException;
    END IF;
    /* If registering user does not already exist then throw exception */
    SELECT count(*) INTO nCount FROM dba_users WHERE username = UPPER( i_sUser );
    IF nCount = 0 THEN
        g_nErrorId := -20000;
        g_sErrorDesc := 'User to be registered must previously exist.';
        RAISE g_xException;
    END IF;

    /* Ensure i_sMethod is uppercase if not already */
    sMethod := UPPER(i_sMethod);

    IF NOT i_bRegister THEN
        sFlag := 'N';
    END IF;

    /* Update the appropriate user to indicate they're registered */
    UPDATE  cal_registered_schema
    SET     register_flag = sFlag
    WHERE   schema_owner = UPPER( i_sUser );

    /* Check to see if any rows updated -- if not, and registering, then insert new user */
    IF ( SQL%ROWCOUNT = 0 ) AND i_bRegister THEN
        INSERT INTO cal_registered_schema ( schema_owner, register_flag )
        SELECT username, sFlag
          FROM dba_users
         WHERE username = UPPER( i_sUser );

        /* Create shadow user for user being registered */
        cal_create_shadow_user( i_sUser, i_sUserPwd );
    END IF;

    IF i_bRegister THEN
        /* Grant privileges */
        cal_grant_privs( i_sUser );
        cal_grant_privs( cal_get_shadow_name( i_sUser ) );
        cal_grant_appluser_ddl_privs( i_sUser );
        cal_grant_shaduser_ddl_privs( cal_get_shadow_name( i_sUser ) );
        IF sMethod = RM_CCALL THEN
          cal_grant_appluser_obj_privs( i_sUser );
          cal_grant_shaduser_obj_privs( cal_get_shadow_name( i_sUser ) );
          cal_create_logon_trigger( i_sUser );
          cal_create_logoff_trigger( i_sUser );
        END IF;
        --IF sMethod = RM_HS THEN
          --cal_create_logon_trigger( i_sUser );
        --END IF;        
        /* Create the required DDL triggers on the shadow user */
        cal_create_ddl_triggers( cal_get_shadow_name( i_sUser ), sMethod );
    ELSE
        /* Revoke privileges */
        cal_revoke_privs( i_sUser );
        cal_revoke_privs( cal_get_shadow_name( i_sUser ) );
        cal_revoke_appluser_ddl_privs ( i_sUser );
        cal_revoke_shaduser_ddl_privs( cal_get_shadow_name( i_sUser ) );
        IF sMethod = RM_CCALL THEN
          cal_revoke_appluser_obj_privs( i_sUser );
          cal_revoke_shaduser_obj_privs( cal_get_shadow_name( i_sUser ) );
          cal_drop_logon_trigger( i_sUser );
          cal_drop_logoff_trigger( i_sUser );          
        END IF;
        --IF sMethod = RM_HS THEN
          --cal_drop_logon_trigger( i_sUser );
        --END IF;        
        /* Drop the DDL triggers on the shadow user */
        cal_drop_ddl_triggers( cal_get_shadow_name( i_sUser ) );
    END IF;

    COMMIT;
EXCEPTION
    WHEN g_xException THEN
        ROLLBACK;
        dbms_output.put_line('In cal_register_user EXCEPTION HANDLER ' || g_nErrorId || ', ' || g_sErrorDesc);
    WHEN OTHERS THEN
        /* jlowe:  This rollback is not completing if a sql error occurs before the commit */
        ROLLBACK;
        g_nErrorId := ABS( SQLCODE );
        g_sErrorDesc := SQLERRM;
        dbms_output.put_line('In cal_register_user EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        RAISE g_xException;
END cal_register_object_owner;
/******************************************************************************/

END;
/
-- End of DDL Script for Package Body PKG_CALPONT
