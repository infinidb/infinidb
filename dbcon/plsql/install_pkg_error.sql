/*
*  $Id: install_pkg_error.sql 2618 2007-06-04 12:26:25Z rdempsey $
*/
-- Start of DDL Script for Package Body PKG_ERROR
CREATE OR REPLACE
PACKAGE pkg_error IS
/*******************************************************************************
*
*   Purpose:    Contains functionality to handle errors occurring in the
*               supporting application.
*
*   MODIFICATION HISTORY:
*
*   Person                  Date            Comments
*   ---------               ----------      ------------------------------------
*   Sean Turner - Enkitec   2006.07.31      Initial package creation
*   Jason Lowe              2006.08.22      Enhancements
*
*******************************************************************************/

/******************************************************************************/
/*** Global Variables ***/
/******************************************************************************/


/******************************************************************************/

/******************************************************************************/
/*** Global Constants ***/
/******************************************************************************/
MAX_ERROR_CODE          CONSTANT    NUMBER := 40000;

/******************************************************************************/

/******************************************************************************/
/*** Procedures/Functions ***/
/******************************************************************************/
PROCEDURE cal_handle_error (
    i_nActionId     IN  cal_action_log.action_id%TYPE,
    i_nErrorId      IN  cal_error_code.error_id%TYPE,
    i_sSQL          IN  VARCHAR2 := NULL );

FUNCTION cal_get_error_code (
    i_nErrorId      IN  cal_error_code.error_id%TYPE ) RETURN cal_error_code.error_code%TYPE;

PROCEDURE cal_get_error_info (
    i_nErrorId      IN  cal_error_code.error_id%TYPE,
    o_sErrorCode    OUT cal_error_code.error_code%TYPE,
    o_sErrorDesc    OUT cal_error_code.error_desc%TYPE );

PROCEDURE cal_insert_error_code(
    nErrorId    IN  cal_error_code.error_id%TYPE,
    sErrorCode  IN  cal_error_code.error_code%TYPE,
    sErrorDesc  IN  cal_error_code.error_desc%TYPE );

PROCEDURE cal_refresh_error_codes;

END;
/

CREATE OR REPLACE
PACKAGE BODY pkg_error IS

/*******************************************************************************/
PROCEDURE cal_handle_error (
    i_nActionId     IN  cal_action_log.action_id%TYPE,
    i_nErrorId      IN  cal_error_code.error_id%TYPE,
    i_sSQL          IN  VARCHAR2 := NULL ) IS

    /* Variable Declarations */
    sErrorCode  cal_error_code.error_code%TYPE;
    sErrorDesc  cal_error_code.error_desc%TYPE;
BEGIN
    DBMS_OUTPUT.PUT_LINE('In cal_handle_error');
    /* Log the error for the specified action */
    pkg_logging.cal_update_log( i_nActionId, i_nErrorId => i_nErrorId );

    /* Get the code and description of the specified error */
    cal_get_error_info( i_nErrorId, sErrorCode, sErrorDesc );

    /* If SQL statement caused an error, add it to the error message */
    IF i_sSQL IS NOT NULL THEN
        sErrorDesc := sErrorDesc || '; Caused by ' || i_sSQL;
    END IF;
    
    /* Display appropriate error message */
    DBMS_OUTPUT.put_line( pkg_logging.cal_get_action_name( i_nActionId ) || ' Error - ' || sErrorCode || ': ' || sErrorDesc );
END cal_handle_error;
/*******************************************************************************/

/*******************************************************************************/
FUNCTION cal_get_error_code (
    i_nErrorId  IN  cal_error_code.error_id%TYPE ) RETURN cal_error_code.error_code%TYPE IS

    /* Variable Declarations */
    sErrorCode  cal_error_code.error_code%TYPE;

BEGIN
    SELECT  error_code
    INTO    sErrorCode
    FROM    cal_error_code
    WHERE   error_id = i_nErrorId;

    RETURN sErrorCode;
EXCEPTION
    WHEN OTHERS THEN
        RETURN NULL;
END cal_get_error_code;
/*******************************************************************************/

/*******************************************************************************/
PROCEDURE cal_get_error_info (
    i_nErrorId      IN  cal_error_code.error_id%TYPE,
    o_sErrorCode    OUT cal_error_code.error_code%TYPE,
    o_sErrorDesc    OUT cal_error_code.error_desc%TYPE ) IS

BEGIN
    SELECT  error_code, error_desc
    INTO    o_sErrorCode, o_sErrorDesc
    FROM    cal_error_code
    WHERE   error_id = i_nErrorId;
EXCEPTION
    WHEN OTHERS THEN
        DBMS_OUTPUT.put_line ( SQLERRM );
END cal_get_error_info;
/*******************************************************************************/

/*******************************************************************************/
PROCEDURE cal_insert_error_code(
    nErrorId    IN  cal_error_code.error_id%TYPE,
    sErrorCode  IN  cal_error_code.error_code%TYPE,
    sErrorDesc  IN  cal_error_code.error_desc%TYPE ) IS

    /* Variable Declarations */

BEGIN
    INSERT INTO cal_error_code (
                error_id, error_code, error_desc )
    VALUES (    nErrorId, sErrorCode, sErrorDesc );

    COMMIT;
END cal_insert_error_code;
/*******************************************************************************/

/*******************************************************************************/
PROCEDURE cal_refresh_error_codes IS

    /* Variable Declarations */
    nErrorId    cal_error_code.error_id%TYPE;
    sErrorDesc  cal_error_code.error_desc%TYPE;

BEGIN
    FOR i IN 1..MAX_ERROR_CODE LOOP
        BEGIN
            SELECT  error_id
            INTO    nErrorId
            FROM    cal_error_code
            WHERE   error_id = i;
        EXCEPTION
            WHEN no_data_found THEN
                /* Get the description associated with the Oracle error raised */
                sErrorDesc := SQLERRM( -i );

                /* Only insert valid/defined Oracle error messages */
                IF INSTR( sErrorDesc, 'Message ' || i || ' not found' ) = 0 THEN
                    /* Create an application error, since one doesn't exist */
                    cal_insert_error_code( i, 'ORA-' || LPAD( TO_CHAR( i ), 5, '0' ) , sErrorDesc );
                END IF;
        END;
    END LOOP;
END cal_refresh_error_codes;
/*******************************************************************************/


END;
/
-- End of DDL Script for Package Body PKG_ERROR
