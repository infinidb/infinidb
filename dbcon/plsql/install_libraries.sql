/*******************************************************************************
*  $Id: install_libraries.sql 4799 2009-01-26 20:36:33Z bwelch $
*  Script Name:    install_libraries.sql
*  Date Created:   2006.08.22
*  Author:         Jason Lowe
*  Purpose:        Create library, function and procedure objects to support remote access to Calpont
*                  for the PL/SQL solution.
/******************************************************************************/
spool install_libraries.log
/* Place DDL to create the Oracle library for the C library */
/* Place DDL to create Oracle functions (process_dml, process_ddl, table_scan, execute_procedure) based on functions in C library */

CREATE OR REPLACE LIBRARY CalpontLib AS '/usr/local/Calpont/lib/libcalora.so';
/

--CREATE OR REPLACE LIBRARY CalpontLib AS 'c:\usr\local\calpont\lib\libcalora.dll';
--/

CREATE OR REPLACE
FUNCTION cal_execute_procedure ( i_sConnectAsUser VARCHAR2,
                                 i_sConnectAsUserPwd VARCHAR2,
                                 i_sConnectAsSID VARCHAR2,
                                 i_sProcedureName VARCHAR2,
                                 i_sParam1 VARCHAR2,
                                 i_sParam2 VARCHAR2,
                                 i_sParam3 VARCHAR2,
                                 i_sParam4 VARCHAR2,
                                 i_sParam5 VARCHAR2,
                                 i_sParam6 VARCHAR2 )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will connect as
  *            given user/password@SID and execute given procedure passing it
  *            up to six parameters.  Any parameter assigned the null string ''
  *            will be ignored, and any parameters after one with the null string ''.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.08.28      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "Execute_procedure"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_sConnectAsUser, i_sConnectAsUser indicator,
                i_sConnectAsUserPwd, i_sConnectAsUserPwd indicator,
                i_sConnectAsSID, i_sConnectAsSID indicator,
                i_sProcedureName, i_sProcedureName indicator,
                i_sParam1, i_sParam1 indicator,
                i_sParam2, i_sParam2 indicator,
                i_sParam3, i_sParam3 indicator,
                i_sParam4, i_sParam4 indicator,
                i_sParam5, i_sParam5 indicator,
                i_sParam6, i_sParam6 indicator );
/

CREATE OR REPLACE
FUNCTION cal_process_dml ( i_nSessionID NUMBER,
                           i_sDateFormat VARCHAR2,
                           i_sDatetimeFormat VARCHAR2,
                           i_sOwner VARCHAR2,
                           i_sCalUser VARCHAR2,
                           i_sCalPswd VARCHAR2 )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will execute given
  *            DML statement on the Calpont appliance.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.08.31      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "Process_dml"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator, 
                i_sDateFormat, i_sDateFormat indicator,
                i_sDatetimeFormat, i_sDatetimeFormat indicator,
                i_sOwner, i_sOwner indicator,
                i_sCalUser, i_sCalUser indicator,
                i_sCalPswd, i_sCalPswd indicator );
/

-- @bug 1025. drop types first to avoid errors while replacing dependent types.
BEGIN
execute immediate 'drop type ColValueSet force';
execute immediate 'drop type colValue force';
EXCEPTION
WHEN others THEN
null;
end;
/

CREATE OR REPLACE TYPE colValue AS OBJECT
(
  column_name VARCHAR2(30),
  data_type VARCHAR2(106),
  data_length NUMBER,
  data_precision NUMBER, 
  data_scale NUMBER
);
/

CREATE OR REPLACE TYPE ColValueSet AS TABLE OF colValue;
/
CREATE OR REPLACE
FUNCTION cal_process_ddl ( i_nSessionID NUMBER,
                           i_sOwner VARCHAR2,
                           i_sDDL VARCHAR2,
                           colValList OUT ColValueSet )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will execute given
  *            DDL statement on the Calpont appliance.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.08.31      Initial creation.
  *   Jason Lowe              2007.07.27      Added i_cNewCols for Alter Table Add Column fix (bug 245)
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "Process_ddl"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator,
                i_sOwner, i_sOwner indicator,
                i_sDDL, i_sDDL indicator,
                colValList, colValList indicator );
/

CREATE OR REPLACE
FUNCTION cal_last_update_count
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will execute after given
  *            UPDATE statement on the Calpont appliance.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Cindy Hao               2007.10.31      Initial creation.
  *   
  *
  *******************************************************************************/
    RETURN binary_integer
    AS LANGUAGE C
    NAME "Get_rowcount"
    LIBRARY CalpontLib
    ;
/
CREATE OR REPLACE TYPE CalpontImpl AS OBJECT (
  /*******************************************************************************
  *
  *   Purpose: Object to define the OCI table function interfaces.
  *            Note:  currSchemaName is the user's current_schema from thier session, which
  *            can be altered, and is different than schemaName.  schemaName is the
  *            same as ownerName, and both are the name of the user that owns tableName.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.08.31      Initial creation.
  *   Bob Dempsey             2006.09.12      Updates
  *   Jason Lowe              2007.05.31      Added currSchemaName to all input params
  *
  ********************************************************************************/

  key RAW(4),

/*ZZ*/
  STATIC FUNCTION ODCITableStart(
      sctx OUT CalpontImpl,
      sessionID IN NUMBER,
      schemaName IN VARCHAR,
      tableName IN VARCHAR,
      ownerName IN VARCHAR,
      tblTypeName IN VARCHAR,
      rowTypeName IN VARCHAR,
      currSchemaName IN VARCHAR,
      cal_bind_values IN BindValueSet
    )
    RETURN PLS_INTEGER
    AS LANGUAGE C
    LIBRARY CalpontLib
    NAME "ODCITableStart"
    WITH CONTEXT
    PARAMETERS (
      context,
      sctx,
      sctx INDICATOR STRUCT,
      sessionID,
      sessionID INDICATOR,
      schemaName,
      schemaName INDICATOR,
      tableName,
      tableName INDICATOR,
      ownerName,
      ownerName INDICATOR,
      tblTypeName,
      tblTypeName INDICATOR,
      rowTypeName,
      rowTypeName INDICATOR,
      currSchemaName,
      currSchemaName INDICATOR,
      cal_bind_values,
      cal_bind_values INDICATOR,
      RETURN INT
    ),

  STATIC FUNCTION ODCITableDescribe(
      rtype OUT SYS.ANYTYPE,
      sessionID IN NUMBER,
      schemaName IN VARCHAR,
      tableName IN VARCHAR,
      ownerName IN VARCHAR,
      tblTypeName IN VARCHAR,
      rowTypeName IN VARCHAR,
      currSchemaName IN VARCHAR,
      cal_bind_values IN BindValueSet
    )
    RETURN PLS_INTEGER
    AS LANGUAGE C
    LIBRARY CalpontLib
    NAME "ODCITableDescribe"
    WITH CONTEXT
    PARAMETERS (
      context,
      rtype,
      rtype INDICATOR,
      sessionID,
      sessionID INDICATOR,
      schemaName,
      schemaName INDICATOR,
      tableName,
      tableName INDICATOR,
      ownerName,
      ownerName INDICATOR,
      tblTypeName,
      tblTypeName INDICATOR,
      rowTypeName,
      rowTypeName INDICATOR,
      currSchemaName,
      currSchemaName INDICATOR,
      cal_bind_values,
      cal_bind_values INDICATOR,
      RETURN INT
    ),

    static function ODCITablePrepare(
        sctx OUT CalpontImpl,
        ti IN SYS.ODCITabFuncInfo,
        sessionID IN NUMBER,
        schemaName IN VARCHAR,
        tableName IN VARCHAR,
        ownerName IN VARCHAR,
        tblTypeName IN VARCHAR,
        rowTypeName IN VARCHAR,
        currSchemaName IN VARCHAR,
        cal_bind_values IN BindValueSet

    )
    RETURN PLS_INTEGER
    AS LANGUAGE C
    LIBRARY CalpontLib
    NAME "ODCITablePrepare"
    WITH CONTEXT
    PARAMETERS (
      context,
      sctx,
      sctx INDICATOR STRUCT,
      ti,
      ti INDICATOR STRUCT,
      sessionID,
      sessionID INDICATOR,
      schemaName,
      schemaName INDICATOR,
      tableName,
      tableName INDICATOR,
      ownerName,
      ownerName INDICATOR,
      tblTypeName,
      tblTypeName INDICATOR,
      rowTypeName,
      rowTypeName INDICATOR,
      currSchemaName,
      currSchemaName INDICATOR,
      cal_bind_values,
      cal_bind_values INDICATOR,
      RETURN INT
    ),

  MEMBER FUNCTION ODCITableFetch(
      self IN OUT CalpontImpl,
      nrows IN NUMBER,
      outSet OUT SYS.ANYDATASET
    )
    RETURN PLS_INTEGER
    AS LANGUAGE C
    LIBRARY CalpontLib
    NAME "ODCITableFetch"
    WITH CONTEXT
    PARAMETERS (
      context,
      self,
      self INDICATOR STRUCT,
      nrows,
      outSet,
      outSet INDICATOR,
      RETURN INT
    ),

  MEMBER FUNCTION ODCITableClose(
      self IN CalpontImpl
    )
    RETURN PLS_INTEGER
    AS LANGUAGE C
    LIBRARY CalpontLib
    NAME "ODCITableClose"
    WITH CONTEXT
    PARAMETERS (
      context,
      self,
      self INDICATOR STRUCT,
      RETURN INT
    )
);
/

CREATE OR REPLACE FUNCTION cal_table_scan (
  i_nSessionID IN NUMBER,
  i_sSchemaName IN VARCHAR,
  i_sTableName IN VARCHAR,
  i_sOwnerName IN VARCHAR,
  i_sTblTypeName IN VARCHAR,
  i_sRowTypeName IN VARCHAR,
  i_sCurrSchemaName IN VARCHAR,
  cal_bind_values IN BindValueSet
) 
  /*******************************************************************************
  *
  *   Purpose: Function to call external library functions that will fetch rows
  *            from the Calpont appliance for the given table.
  *            Note:  i_sCurrSchemaName is the user's current_schema from thier session, which
  *            can be altered, and is different than i_sSchemaName.  i_sSchemaName is the
  *            same as i_sOwnerName, and both are the name of the user that owns i_sTableName.  
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.08.31      Initial creation.
  *   Jason Lowe              2006.09.12      Updates
  *   Jason Lowe              2007.05.31      Added currSchemaName to input params  
  *
  *******************************************************************************/
RETURN SYS.ANYDATASET
PIPELINED USING CalpontImpl;
/

-- user logoff function
CREATE OR REPLACE PROCEDURE cal_logoff(
  i_nSessionID IN NUMBER
)
  /*******************************************************************************
  *
  *   Purpose: Procedure to call external library function that will perform necessary
  *            clean up for given sessionId on Calpont server.  Effectively logoff.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.09.13      Initial creation.
  *
  *******************************************************************************/
AS LANGUAGE C
LIBRARY CalpontLib
NAME "CalLogoff"
WITH CONTEXT
PARAMETERS (
  context,
  i_nSessionID
);
/

-- user logon function
CREATE OR REPLACE PROCEDURE cal_logon(
  i_nSessionID IN NUMBER,
  i_sUser IN VARCHAR
)
  /*******************************************************************************
  *
  *   Purpose: Procedure to call external library function that will perform necessary
  *            initialization for given sessionId on Calpont sever.  Effectively logon.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Jason Lowe              2006.09.22      Initial creation.
  *
  *******************************************************************************/
AS LANGUAGE C
LIBRARY CalpontLib
NAME "CalLogon"
WITH CONTEXT
PARAMETERS (
  context,
  i_nSessionID,
  i_nSessionID INDICATOR,
  i_sUser,
  i_sUser INDICATOR
);
/

CREATE OR REPLACE
FUNCTION cal_commit ( i_nSessionID NUMBER )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will transmit
  *            a COMMIT to the EC.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Bob Dempsey             2006.10.21      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "CalCommit"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator );
/

CREATE OR REPLACE
FUNCTION cal_rollback ( i_nSessionID NUMBER )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will transmit
  *            a ROLLBACK to the EC.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Bob Dempsey             2006.10.21      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "CalRollback"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator );
/

CREATE OR REPLACE
FUNCTION cal_trace_on ( i_nSessionID NUMBER, i_nFlags NUMBER )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will set
  *            statement tracing on in session
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Bob Dempsey             2007.09.12      Initial creation.
  *   Bob Dempsey             2007.12.19      Pass flags.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "CalTraceOn"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator,
                i_nFlags, i_nFlags indicator );
/

CREATE OR REPLACE
FUNCTION cal_set_env ( i_sName VARCHAR, i_sValue VARCHAR )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will set
  *            an environment var in extproc's runtime env
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Bob Dempsey             2008.02.12      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "CalSetEnv"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_sName, i_sName indicator,
                i_sValue, i_sValue indicator );
/

CREATE OR REPLACE
FUNCTION cal_get_query_stats ( i_nSessionID IN NUMBER, o_sStats OUT VARCHAR )
  /*******************************************************************************
  *
  *   Purpose: Return the query stats to Oracle
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Bob Dempsey             2008.07.15      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "CalGetQueryStats"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator,
                o_sStats, o_sStats indicator );
/

CREATE OR REPLACE
FUNCTION cal_setparms ( i_nSessionID NUMBER, i_nParam VARCHAR, i_nValue VARCHAR )
  /*******************************************************************************
  *
  *   Purpose: Function to call external library function that will set a session
  *            Resource Manager value.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person                  Date            Comments
  *   ---------               ----------      ------------------------------------
  *   Barbara Welch           2009.01.12      Initial creation.
  *
  *******************************************************************************/
    RETURN BINARY_INTEGER
    AS LANGUAGE C
    NAME "calSetParms"
    LIBRARY CalpontLib
    WITH CONTEXT
    PARAMETERS( context,
                i_nSessionID, i_nSessionID indicator,
                i_nParam, i_nParam indicator,
                i_nValue, i_nValue indicator );
/

spool off
