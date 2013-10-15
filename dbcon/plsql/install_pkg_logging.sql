/*
*  $Id: install_pkg_logging.sql 2618 2007-06-04 12:26:25Z rdempsey $
*/
-- Start of DDL Script for Package Body PKG_LOGGING
CREATE OR REPLACE
PACKAGE pkg_logging IS
/*******************************************************************************
*
*   Purpose:    Contains functionality to handle logging of application actions.
*
*   MODIFICATION HISTORY:
*
*   Person                  Date            Comments
*   ---------               ----------      ------------------------------------
*   Sean Turner - Enkitec   2006.07.31      Initial package creation
*   Jason Lowe              2006.08.22      Enhancements
*
*******************************************************************************/

/*******************************************************************************/
/*** Global Variables ***/
/*******************************************************************************/


/*******************************************************************************/

/*******************************************************************************/
/*** Global Constants ***/
/*******************************************************************************/


/*******************************************************************************/

/*******************************************************************************/
/*** Procedures/Functions ***/
/*******************************************************************************/
PROCEDURE cal_insert_log (
    o_nActionId     OUT cal_action_log.action_id%TYPE,
    i_sAction       IN  cal_action_log.action_name%TYPE,
    i_sOwner        IN  cal_action_log.object_owner%TYPE,
    i_sObject       IN  cal_action_log.object_name%TYPE,
    i_sSQLText      IN  cal_action_log.sql_text%TYPE,
    i_sActionUser   IN  cal_action_log.action_username%TYPE,
    i_dtActionDate  IN  cal_action_log.action_date%TYPE,
    i_nErrorId      IN  cal_action_log.error_id%TYPE := NULL );

PROCEDURE cal_update_log (
    i_nActionId     IN  cal_action_log.action_id%TYPE,
    i_sAction       IN  cal_action_log.action_name%TYPE := NULL,
    i_sOwner        IN  cal_action_log.object_owner%TYPE := NULL,
    i_sObject       IN  cal_action_log.object_name%TYPE := NULL,
    i_sSQLText      IN  cal_action_log.sql_text%TYPE := NULL,
    i_sActionUser   IN  cal_action_log.action_username%TYPE := NULL,
    i_dtActionDate  IN  cal_action_log.action_date%TYPE := NULL,
    i_nErrorId      IN  cal_action_log.error_id%TYPE := NULL );

FUNCTION cal_get_action_name (
    i_nActionId IN  cal_action_log.action_id%TYPE ) RETURN cal_action_log.action_name%TYPE;

END;
/

CREATE OR REPLACE
PACKAGE BODY pkg_logging IS
/*******************************************************************************/
PROCEDURE cal_insert_log (
    o_nActionId     OUT cal_action_log.action_id%TYPE,
    i_sAction       IN  cal_action_log.action_name%TYPE,
    i_sOwner        IN  cal_action_log.object_owner%TYPE,
    i_sObject       IN  cal_action_log.object_name%TYPE,
    i_sSQLText      IN  cal_action_log.sql_text%TYPE,
    i_sActionUser   IN  cal_action_log.action_username%TYPE,
    i_dtActionDate  IN  cal_action_log.action_date%TYPE,
    i_nErrorId      IN  cal_action_log.error_id%TYPE := NULL ) IS

    /* Variable Declarations */
    PRAGMA         AUTONOMOUS_TRANSACTION;
BEGIN
    dbms_output.put_line('In cal_insert_log');
    dbms_output.put_line('In cal_insert_log - i_nErrorId = ' || i_nErrorId);
    INSERT INTO cal_action_log (
                action_name, object_owner, object_name, sql_text, action_username, action_date, error_id )
    VALUES (    i_sAction, i_sOwner, i_sObject, i_sSQLText, i_sActionUser, i_dtActionDate, i_nErrorId )
    RETURNING   action_id
    INTO        o_nActionId;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_insert_log EXCEPTION HANDLER: ' || SQLCODE || ', ' || SQLERRM);
        pkg_error.cal_handle_error( o_nActionId, ABS( SQLCODE ) );
END cal_insert_log;
/*******************************************************************************/

/*******************************************************************************/
PROCEDURE cal_update_log (
    i_nActionId     IN  cal_action_log.action_id%TYPE,
    i_sAction       IN  cal_action_log.action_name%TYPE := NULL,
    i_sOwner        IN  cal_action_log.object_owner%TYPE := NULL,
    i_sObject       IN  cal_action_log.object_name%TYPE := NULL,
    i_sSQLText      IN  cal_action_log.sql_text%TYPE := NULL,
    i_sActionUser   IN  cal_action_log.action_username%TYPE := NULL,
    i_dtActionDate  IN  cal_action_log.action_date%TYPE := NULL,
    i_nErrorId      IN  cal_action_log.error_id%TYPE := NULL ) IS

    /* Variable Declarations */
    PRAGMA         AUTONOMOUS_TRANSACTION;
BEGIN
    dbms_output.put_line('In cal_update_log');
    dbms_output.put_line('In cal_update_log - i_nErrorId = ' || i_nErrorId);
    UPDATE  cal_action_log
    SET     action_name = NVL( i_sAction, action_name ),
            object_owner = NVL( i_sOwner, object_owner ),
            object_name = NVL( i_sObject, object_name ),
            sql_text = NVL( i_sSQLText, sql_text ),
            action_username = NVL( i_sActionUser, action_username ),
            action_date = NVL( i_dtActionDate, action_date ),
            error_id = NVL( i_nErrorId, error_id )
    WHERE   action_id = i_nActionId;

    COMMIT;
EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_update_log EXCEPTION HANDLER: ' || SQLCODE || ', ' || SQLERRM);
        pkg_error.cal_handle_error( i_nActionId, ABS( SQLCODE ) );
END cal_update_log;
/*******************************************************************************/

/*******************************************************************************/
FUNCTION cal_get_action_name (
    i_nActionId IN  cal_action_log.action_id%TYPE ) RETURN cal_action_log.action_name%TYPE IS

    /* Variable Declarations */
    sActionName cal_action_log.action_name%TYPE;

BEGIN
    SELECT  action_name
    INTO    sActionName
    FROM    cal_action_log
    WHERE   action_id = i_nActionId;

    RETURN sActionName;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END cal_get_action_name;
/*******************************************************************************/

END;
/
-- End of DDL Script for Package Body PKG_LOGGING
