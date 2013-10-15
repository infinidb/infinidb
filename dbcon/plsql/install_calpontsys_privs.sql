-- calpontsys user already exists. Grant the appropriate privileges to complete the
-- calpontsys install
--

spool install_calpontsys_schema_sync_tables.log

BEGIN

  EXECUTE IMMEDIATE 'GRANT SELECT, UPDATE ON CALPONT.CAL_REGISTERED_SCHEMA TO CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT SELECT ANY DICTIONARY TO CALPONTSYS';

  EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT CREATE TABLE TO CALPONTSYS';

	-- grant necessary priveges to s_calpontsys
  EXECUTE IMMEDIATE 'GRANT SELECT, UPDATE ON CALPONT.CAL_REGISTERED_SCHEMA TO S_CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT CREATE PROCEDURE TO S_CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT CREATE ANY TABLE TO S_CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT CREATE ANY INDEX TO S_CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT ALTER ANY TABLE TO S_CALPONTSYS';

-- will contain the which object is out of sync and status of the correction process

  EXECUTE IMMEDIATE 'create table calpontsys.calSchemaSyncErrors (
        owner             varchar2(40)  not null,
        object_name       varchar2(256) not null,
        object_type       varchar2(40)  not null,
        error_status      number        not null,
        correction_status number        not null)';

--alter table calSchemaSyncErrors 
--  add CONSTRAINT calSchemaSyncErrorsConstr1 UNIQUE (owner, object_name, object_type);

-- will contain the sql needed to recreate a table --
  EXECUTE IMMEDIATE 'create table calpontsys.calSchemaSyncCorrections (
        owner             varchar2(40)  not null,
        object_name       varchar2(256) not null,
        object_type       varchar2(40)  not null,
        error_status      number        not null,
        correction_status number        not null,
        sqltext           varchar2(1000) not null)';

--alter table calSchemaSyncCorrections 
--  add CONSTRAINT SyncCorrectionsConstr1 UNIQUE (owner, object_name, object_type, sqltext);

	-- must be done after calpontsys_install is complete
  EXECUTE IMMEDIATE 'GRANT SELECT, UPDATE ON CALPONTSYS.CALSCHEMASYNCERRORS TO S_CALPONTSYS';
  EXECUTE IMMEDIATE 'GRANT SELECT, UPDATE ON CALPONTSYS.CALSCHEMASYNCCORRECTIONS TO S_CALPONTSYS';

END ;
/

spool off

exit


