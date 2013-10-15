/**
-- install all procedures required for s_Calpontsys to perform schema sync corrections
--
**/

-- RestartCalpontPropagation
-- reset the schema sync flag to allow for calpont propagation
-- 
create or replace
PROCEDURE RESTARTCALPONTPROPAGATION ( schemaName IN VARCHAR2)
AS
BEGIN

  -- update schema_sync flag to on(1)
  
  update calpont.cal_registered_schema
    set schema_sync = 1
    WHERE schema_owner = schemaName;

  EXCEPTION
    WHEN OTHERS
    THEN
      NULL;
      
END RESTARTCALPONTPROPAGATION;
/

-- CorrectCalpontSyncError
-- drop and rebuild objects that contain sync errors
--

create or replace
procedure SchemaSyncCorrection
AS
  curSchemaName VARCHAR2(40) := '';
  curTableName VARCHAR2(256) :='';
  TYPE calSyncErrRec_tab IS TABLE
     OF calpontsys.calSchemaSyncErrors%ROWTYPE;

  ErrList CalSyncErrRec_tab := calSyncErrRec_tab();
 
BEGIN

  FOR aErrRec IN (
    SELECT *
    from calpontsys.calSchemaSyncErrors
    Where error_status = 1)
  LOOP
    ErrList.EXTEND;
    ErrList(ErrList.LAST).owner := aErrRec.owner;
    ErrList(ErrList.LAST).object_name := aErrRec.object_name;
  END LOOP;
 
  IF ErrList.LAST IS NULL OR ErrList.LAST <=0
  THEN
	RETURN;
  END IF;
 
  FOR ErrListIdx IN 1..ErrList.LAST
  LOOP
  
    curSchemaName := upper(ErrList(ErrListIdx).owner);
    curTableName := upper(ErrList(ErrListIdx).object_name);
    dbms_output.put_line('SCHEMA Owner ' || curSchemaName || ' table ' || curTableName);
    
    -- prevent ddl propagation to Calpont --
    StopCalpontPropagation(curSchemaName);
    
    -- drop the current object and its children --
    dropObjectCascade('S_'||curSchemaName, curTableName);
  
    -- rebuild the Calpont Object from scratch
    reBuildCalpontObjects(curSchemaName);
    
    -- update the current calSchemaSyncErrors record --
    update calpontsys.calSchemaSyncErrors
      set error_status = 2
      where owner = curSchemaName
      and   object_name = curTableName
      and   error_status = 1
      and   correction_status = 0;
  
    -- allow ddl propagation to Calpont --
    RestartCalpontPropagation(curSchemaName);
    
  END LOOP;
  
END SchemaSyncCorrection ;
/

-- DropObjectCascade
-- 
create or replace
PROCEDURE dropobjectcascade(rootobjectowner IN VARCHAR2,   rootobjectname IN VARCHAR2) AS

sqltext VARCHAR2(2000) := 'DROP TABLE ';
tablename VARCHAR2(2000);

BEGIN

  DBMS_OUTPUT.PUT_LINE('DROPPING TABLE');
  tablename := 'S_' || rootobjectowner || '.' || rootobjectname;
  sqltext := sqltext || tablename || ' CASCADE CONSTRAINTS';
  DBMS_OUTPUT.PUT_LINE(sqltext);
  EXECUTE IMMEDIATE sqltext;
  DBMS_OUTPUT.PUT_LINE('DROP TABLE complete');

  sqlText := 'DROP SYNONYM ' || rootObjectOwner || '.' || rootObjectName;
  EXECUTE IMMEDIATE sqltext;
  DBMS_OUTPUT.PUT_LINE('DROP Synonym complete');
  
  sqlText := 'DROP TYPE ' || rootObjectOwner || '.TT_' || rootObjectName;
  EXECUTE IMMEDIATE sqltext;
  DBMS_OUTPUT.PUT_LINE('DROP TT type complete');

  sqlText := 'DROP TYPE ' || rootObjectOwner || '.OT_' || rootObjectName;
  EXECUTE IMMEDIATE sqltext;
  DBMS_OUTPUT.PUT_LINE('DROP OT type complete');

  -- sqlText := 'DROP TRIGGER ' || rootObjectOwner || '.TR_' || rootObjectName;
  -- EXECUTE IMMEDIATE sqltext;
  -- DBMS_OUTPUT.PUT_LINE('DROP TRIGGER complete');

EXCEPTION
WHEN others THEN
  DBMS_OUTPUT.PUT_LINE('ERROR ON DROP TABLE [' || tableName || '] [' || sqltext || ']');
  dbms_output.put_line(dbms_utility.format_error_backtrace);
  
END dropobjectcascade;
/

-- Procedure StopCalpontPropagation
-- set the schema sync flag in cal_registered_schema table to off(0)
--

create or replace
PROCEDURE STOPCALPONTPROPAGATION ( schemaName IN VARCHAR2)
AS
BEGIN

  -- update schema_sync flag to off(0)
  
  update calpont.cal_registered_schema
    set schema_sync = 0
    WHERE schema_owner = schemaName;
  
  EXCEPTION
    WHEN OTHERS
    THEN
      dbms_output.put_line('StopCalpontPropagation error');
      
END STOPCALPONTPROPAGATION;
/

create or replace
PROCEDURE REBUILDCALPONTOBJECTS(schemaName IN VARCHAR2)
AS

  TYPE calSyncCorrRec_tab IS TABLE
     OF calpontsys.calSchemaSyncCorrections%ROWTYPE;

  SyncList CalSyncCorrRec_tab := calSyncCorrRec_tab();

BEGIN

  FOR aErrSyncRec IN (
    SELECT *
      from calpontsys.calSchemaSyncCorrections
      Where owner = schemaName
      and error_status = 1)
  LOOP
    SyncList.EXTEND;
    SyncList(SyncList.LAST).owner := aErrSyncRec.owner;
    SyncList(SyncList.LAST).object_name := aErrSyncRec.object_name;
    
    EXECUTE IMMEDIATE(aErrSyncRec.sqlText);

    update calpontsys.calSchemaSyncCorrections
      set error_status = 2
      where owner         = aErrSyncRec.owner
      and   object_name   = aErrSyncRec.object_name
      and   sqltext       = aErrSyncRec.sqlText
      and   error_status  = 1
      and   correction_status = 0;

  END LOOP;

  EXCEPTION
    WHEN OTHERS
    THEN
      NULL;
      
END REBUILDCALPONTOBJECTS;
/

spool off

exit

