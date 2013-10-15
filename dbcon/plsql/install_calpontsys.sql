/**
-- install all procedures required for calpontsys to perform schema sync tasks
--
**/

spool install_calpontsys.log

-- RecordSchemaSyncError
-- record a sync error in the sync error table.
--
create or replace
Procedure recordSchemaSyncError(owner in varchar2,
                                objName in varchar2,
                                objType in varchar2,
                                errState IN NUMBER)
as
BEGIN

  insert INTO calSchemaSyncErrors
        VALUES(
              owner,
              objName,
              objType,
              errState,
              0
        );
  commit;
END recordSchemaSyncError;
/

-- DropObjectCascade
-- 
create or replace
Procedure dropobjectcascade(rootobjectowner IN VARCHAR2,   rootobjectname IN VARCHAR2) AS

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

create or replace
Procedure StoreCorrectionTask(schemaName IN VARCHAR2)
AS
  curSchemaName VARCHAR2(40) := '';
  curTableName VARCHAR2(256) :='';
  TYPE calSyncErrRec_tab IS TABLE
     OF calpontsys.calSchemaSyncErrors%ROWTYPE;

  ErrList CalSyncErrRec_tab := calSyncErrRec_tab();
  
BEGIN

  -- iterate over calSchemaSyncErrors table for new sync errors --
  dbms_output.put_line('SCHEMA correct ' || schemaName);
  
  FOR aErrRec IN (
    SELECT *
    from calSchemaSyncErrors
    Where owner = schemaName
    and error_status = 1)
  LOOP
    ErrList.EXTEND;
    ErrList(ErrList.LAST).owner := aErrRec.owner;
    ErrList(ErrList.LAST).object_name := aErrRec.object_name;
  END LOOP;
  
  FOR ErrListIdx IN 1..ErrList.LAST
  LOOP
  
    curSchemaName := upper(ErrList(ErrListIdx).owner);
    curTableName := upper(ErrList(ErrListIdx).object_name);
    dbms_output.put_line('SCHEMA Owner ' || curSchemaName || ' table ' || curTableName);
    
    -- create the sql required to recreate a table object --
    getTableSql(lower(curSchemaName), lower(curTableName));
    
    -- update the current calSchemaSyncErrors record --
    update calSchemaSyncErrors
      set error_status = 2
      where owner = curSChemaName
      and   object_name = curTableName
      and   error_status = 1
      and   correction_status = 0;
  
  END LOOP;
  
END StoreCorrectionTask;
/

create or replace
Function CollTypesCount(userName in VARCHAR2,
                        collName in VARCHAR2)
        RETURN BOOLEAN
        AS
        
  typeCnt NUMBER :=0;
  
BEGIN

    select count(type_name) INTO typeCnt
      from sys.dba_types typ
      where typ.owner = userName
      and   typ.type_name = 'TT_' || collName
      and   typ.typecode = 'COLLECTION';
  
  RETURN (typeCnt=1);
  
END CollTypesCount;
/

create or replace
Function ObjectTypesCount(userName in VARCHAR2,
                          typeName in VARCHAR2)
        RETURN BOOLEAN AS

  typeCnt NUMBER;
  
BEGIN

    select count(type_name) INTO typeCnt
      from sys.dba_types typ
      where typ.owner = userName
      and   typ.type_name = 'OT_' || typeName
      and   typ.typecode = 'OBJECT';
  
  RETURN (typeCnt=1);
  
END ObjectTypesCount;
/

create or replace
Function OTObjectsCount(userName in VARCHAR2,
                        otName in VARCHAR2
                      )
        RETURN BOOLEAN AS
  otCnt NUMBER;
  
BEGIN

    select count(object_name) INTO otCnt
      from sys.dba_objects typ
      where typ.owner = userName
      and   typ.object_name = 'OT_' || otName
      and   typ.object_type = 'TYPE';

  RETURN (otCnt=1);
  
END OTObjectsCount;
/

create or replace
Function SYNObjectsCount(userName in VARCHAR2,
                        synName in VARCHAR2)
        RETURN BOOLEAN
        AS
  synCnt NUMBER;
BEGIN

    select count(object_name) INTO synCnt
      from sys.dba_objects typ
      where typ.owner = userName
      and   typ.object_name = synName
      and   typ.object_type = 'SYNONYM';
  
  RETURN (synCnt=1);
  
END SYNObjectsCount;
/

create or replace
Function SynonymCount(userName in VARCHAR2,
                      synonymName in VARCHAR2)
        RETURN BOOLEAN AS

  calUserSynObjectsCnt NUMBER :=0;
  
BEGIN
    select count(object_name) INTO calUserSynObjectsCnt
      from sys.dba_objects typ
      where typ.owner = userName
      and   typ.object_name = synonymName
      and   typ.object_type = 'SYNONYM';
      
  RETURN (calUserSYNObjectsCnt=1);
  
END SynonymCount;
/

create or replace
Function TriggerCount(userName in VARCHAR2,
                      trName in VARCHAR2)
        RETURN BOOLEAN
        AS
        
  trCnt NUMBER :=0;
  
BEGIN

    select count(trigger_name) INTO trCnt
      from sys.dba_triggers tr
      where tr.owner = userName
      and   tr.trigger_name = 'TR_' || trName;

  RETURN (trCnt=1);
  
END TriggerCount;
/

create or replace
Function TRObjectsCount(userName in VARCHAR2,
                        trName in VARCHAR2)
        RETURN BOOLEAN AS

  trCnt NUMBER :=0;
  
BEGIN

    select count(object_name) INTO trCnt
      from sys.dba_objects typ
      where typ.owner = userName
      and   typ.object_name = 'TR_' || trName
      and   typ.object_type = 'TRIGGER';

  RETURN (trCnt=1);
  
END TRObjectsCount;
/

create or replace
Function TTObjectsCount(userName in VARCHAR2,
                        ttName in VARCHAR2)
        RETURN BOOLEAN AS
  ttCnt NUMBER :=0;
  
BEGIN

    select count(object_name) INTO ttCnt
      from sys.dba_objects typ
      where typ.owner = userName
      and   typ.object_name = 'TT_' || ttName
      and   typ.object_type = 'TYPE';

  RETURN (ttCnt=1);
  
END TTObjectsCount;
/

create or replace
Function UserTableCount(userName in VARCHAR2, tblName in VARCHAR2)
        RETURN BOOLEAN AS
  tblCnt NUMBER :=0;
BEGIN

  select count(table_name) INTO tblCnt
    from sys.dba_tables tbl
    where tbl.owner = 'S_' || userName;
 
  RETURN (tblCnt=1);
  
END UserTableCount;
/

create or replace
Function ViewCount(userName in VARCHAR2,
                   viewName in VARCHAR2)
        RETURN BOOLEAN AS
        
  viewCnt NUMBER :=0;
  
BEGIN

  select count(view_name) INTO viewCnt
      from sys.dba_views vw
      where vw.owner = userName
      and vw.view_name = 'VW_' || viewName;
  
  RETURN (viewCnt=1);
  
END ViewCount;
/

create or replace
Function VWObjectsCount(userName in VARCHAR2,
                        vwName in VARCHAR2
                      )
        RETURN BOOLEAN
        AS

  vwCnt NUMBER :=0;
  
BEGIN

    select count(object_name) INTO vwCnt
      from sys.dba_objects typ
      where typ.owner = userName
      and   typ.object_name = 'VW_' || vwname
      and   typ.object_type = 'VIEW';

  RETURN (vwCnt=1);
  
END VWObjectsCount;
/

create or replace
Procedure getTableSql(owner_name IN VARCHAR2, table_Name IN VARCHAR2)
as
  curSchemaName syscolumn.schema%TYPE;
  curTblName    syscolumn.tablename%TYPE;
  curColPos     INTEGER;
  curColName    VARCHAR(256);
  curDataType   INTEGER;
  sqlDataType   VARCHAR(256);
  curColLength  INTEGER;
  curScale      INTEGER;
  curPrec       INTEGER;
  curDfltValue  VARCHAR2(128);
  curNullable   CHAR;
  SQLText VARCHAR2(2000) := 'CREATE TABLE ';
  fixcount number :=0;
  colCount number :=0;
BEGIN

  SQLText := SQLText || 'S_' || upper(owner_name) || '.' || upper(table_Name) || ' (';
  FOR aSqlREC IN (
    select distinct schema,
          tablename,
          columnposition,
          columnname, 
          datatype,
          columnlength,
          scale,
          prec,
          defaultValue,
          Nullable
      from syscolumn
      order by 1,2,3)
  LOOP
    curSchemaName := aSqlRec.schema;
    curTblName    := aSqlRec.tablename;
    curColPos     := aSqlRec.columnposition;
    curColName    := aSqlRec.columnname;
    curDataType   := aSqlRec.datatype;
    curColLength  := aSqlRec.columnlength;
    curScale      := aSqlRec.scale;
    curPrec       := aSqlRec.prec;
    curDfltValue  := aSqlRec.defaultValue;
    curNullable   := aSqlRec.Nullable;

    dbms_output.put_line('IN [' ||
                          owner_name      || '] cur [' ||
                          curSchemaName   || '] IN [' ||
                          table_name      || '] cur [' ||
                          curTblName      || '] col [' ||
                          curColName      || ']');

    IF curSchemaName = owner_name and curTblName = table_name
    THEN

      colCount := colCount+1;  
      sqlDataType := GetTableColSql(curDataType, curColLength, curPrec);
      SQLText := SQLText || ' ' || curColName;
      SQLText := SQLText || ' ' || sqlDataType;
    
      IF curDfltValue<>''
      THEN
        SQLText := SQLText || ' DEFAULT ' || curDfltValue;
      END IF;
    
      IF curNullable<>0
      THEN
        SQLText := SQLText || ' NOT NULL';
      END IF;
      
      SQLText := SQLText || ',';
      
    END IF;
  END LOOP; -- calpontTableColumns_cur
  
      dbms_output.put_line(curSchemaName || ' ' || curTblName || ' SQLText ' || SQLText);
  IF colCount>0
  THEN
    SQLText := rtrim(SQLText, ',');
    SQLText := SQLText || ')';
  
    dbms_output.put_line(curSchemaName || ' ' || curTblName || ' SQLText ' || SQLText);
    INSERT INTO calSchemaSyncCorrections
      VALUES(
          curSchemaName,
          curTblName,
          'UNKNOWN',
          1,
          0,
          SQLText
       );
  ELSE
    dbms_output.put_line('No Columns for ' || table_name );
  END IF;
  
END getTableSql;
/

create or replace
FUNCTION GETTABLECOLSQL(curDataType IN INTEGER,
                        curColLength IN INTEGER,
                        curPrec IN INTEGER) RETURN VARCHAR2 AS
  
  sqlDataType VARCHAR2(256);
  
BEGIN

    IF curDataType = 1 -- TINYINT
    THEN
       sqlDataType := 'NUMBER(1, 0)';
    ELSIF curDataType = 2 -- CHAR
    THEN
       sqlDataType := 'CHAR(' || curColLength || ')';
    ELSIF curDataType = 3 -- SMALLINT
    THEN
       sqlDataType := 'SMALLINT';
    ELSIF curDataType = 4 -- DECIMAL
    THEN
       sqlDataType := 'CHAR(' || curColLength || ')';
    ELSIF curDataType = 5 --MEDINT/INT
    THEN
       sqlDataType := 'INTEGER';
    ELSIF curDataType = 6 --INTEGER
    THEN
        sqlDataType := 'INTEGER';
    ELSIF curDataType = 7 -- FLOAT
    THEN
        sqlDataType := 'FLOAT(' || curPrec || ')'; 
    ELSIF curDataType = 8 -- DATE
    THEN
       sqlDataType := 'DATE';
    ELSIF curDataType = 9 -- BIGINT
    THEN
      sqlDataType := 'NUMBER';
      IF curPrec > 0
      THEN
        sqlDataType := sqlDataType || '(' || curPrec || ')';
      END IF;
    ELSIF curDataType = 10 -- DOUBLE
    THEN
       sqlDataType := 'BINARY_DOUBLE';
    ELSIF curDataType = 11 -- DATETIME
    THEN
      sqlDataType := 'DATETIME';
    ELSIF curDataType = 12 -- VARCHAR
    THEN
      sqlDataType := 'VARCHAR2(' || curColLength || ')';
    END IF;

  RETURN sqlDataType;
  
END GETTABLECOLSQL;
/

create or replace
Procedure SchemaSync
as
  
  owner_name  VARCHAR2(40);
  owner_name_lc VARCHAR2(40);
  curCalpontUserTable VARCHAR2(40);
  curCalpontUser VARCHAR2(40);
  curCalpontUserView dba_views.view_name%TYPE; 
  
  -- retrieve all calpont tables owned by calpontUser --
  TYPE calTables_tab IS TABLE
     OF calpontsys.systable.tablename%TYPE NOT NULL;

  TYPE calSchemaOwner_tab is TABLE
     OF calpont.cal_registered_schema.SCHEMA_OWNER%TYPE NOT NULL;
        
  OwnerList calSchemaOwner_tab := calSchemaOwner_tab();
  ownerlistSize NUMBER :=0;
  TableList calTables_tab := calTables_tab();
  TableListSize NUMBER := 0;
  done integer :=0;
  
BEGIN

  -- find all active schemas
  FOR aSchema IN (  
      SELECT schema_owner
          FROM calpont.cal_registered_schema s
          WHERE s.register_flag = 'Y'
          AND s.SCHEMA_OWNER <> 'CALPONTSYS')
  LOOP
    OwnerList.EXTEND;
    OwnerList(OwnerList.LAST) := aSchema.schema_owner;
  END LOOP;

  IF OwnerList.LAST is NULL or OwnerList.LAST <=0
  THEN
	RETURN;
  END IF;
  
  FOR OwnerListSize IN 1 .. OwnerList.LAST
  LOOP
    owner_name := upper(OwnerList(OwnerListSize));
    owner_name_lc := lower(OwnerList(OwnerListSize));
    dbms_output.put_line('name ' || OwnerList.LAST || ' ' || owner_name_lc);
    
    -- Get Calpont Tables for this calpont user --
    TableList.DELETE;
    FOR aTable IN (
      select s.schema, s.tablename
        from systable s)
    LOOP
      IF aTable.schema = owner_name_lc
      THEN
        TableList.EXTEND;
        TableList(TableList.LAST) := aTable.tablename;
      END IF;
    END LOOP;
   
    IF TableList.LAST is NULL OR TableList.LAST <= 0
    THEN
	  RETURN;
    END IF; 

    FOR TableListSize IN 1 .. TableList.LAST
    LOOP
    
      curCalpontUserTable := upper(TableList(TableListSize));
      dbms_output.put_line('Owner ' || owner_name ||
                          ' Table ' || curCalpontUserTable || ' '
                                    || TableList.LAST);
                
          IF done=0 and ViewCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table View counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'VIEW', 1);
            done:=1;
          END IF;
        
          -- get a count of all synonyms for this user --
          IF done=0 and SynonymCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table Synonym counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'SYNONYM', 1);
            done:=1;
          END IF;
  
          IF done=0 and ObjectTypesCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table Object Type counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'OBJECTTYPE', 1);
            done:=1;
          END IF;

          IF done=0 and CollTypesCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table Collection type counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'COLLECTION', 1);
            done:=1;            
          END IF;
        
          IF done=0 and TriggerCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table Trigger counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'TRIGGER', 1);
            done:=1;            
          END IF;
        
          IF done=0 and TTObjectsCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table TTObjects counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'TTOBJECTS', 1);
            done:=1;
          END IF;
        
          IF done=0 and OTObjectsCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table OTObjects counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'OTOBJECTS', 1);
            done:=1;            
          END IF;
        
          IF done=0 and VWObjectsCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table VWObjects counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'VWOBJECTS', 1);
            done:=1;            
          END IF;

          IF done=0 and SYNObjectsCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table SYNObjects counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'SYNOBJECTS', 1);
            done:=1;            
          END IF;

          IF done=0 and TRObjectsCount(owner_name, curCalpontUserTable)=FALSE
          THEN
            dbms_output.put_line('Table TRObjects counts do not match');
            recordSchemaSyncError(owner_name, curCalpontUserTable, 'TROBJECTS', 1);
            done:=1;            
          END IF;

      END LOOP;
      commit;
      dbms_output.put_line('prepare and process any corrections');
      StoreCorrectionTask(owner_name);
    END LOOP;
    
END SchemaSync;
/

spool off

exit
