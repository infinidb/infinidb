/*******************************************************************************
*  $Id: install_tables.sql 2863 2007-07-31 19:25:01Z jrodriguez $
*  Script Name:    install_tables.sql
*  Date Created:   2006.08.22
*  Author:         Jason Lowe
*  Purpose:        Create tables, sequences, and triggers needed to support the Calpont schema.
/******************************************************************************/

spool install_tables.log

/******************************************************************************/
CREATE TABLE cal_error_code (
    error_id    NUMBER          NOT NULL,
    error_code  VARCHAR2(10)    NOT NULL,
    error_desc  CLOB            NOT NULL,
    CONSTRAINT pk_cal_error_code PRIMARY KEY ( error_id ) )
    LOB ( error_desc ) STORE AS lob_ec_error_desc;

-- Comments for CAL_ERROR_CODE
COMMENT ON COLUMN cal_error_code.error_code IS 'Code of the error (e.g., ORA-00600)';
COMMENT ON COLUMN cal_error_code.error_desc IS 'Description of the error';
COMMENT ON COLUMN cal_error_code.error_id IS 'Unique identifier of the error';
/******************************************************************************/

/******************************************************************************/
CREATE TABLE cal_action_log (
    action_id           NUMBER                          NOT NULL,
    action_name         VARCHAR2(10)                    NOT NULL,
    object_owner        VARCHAR2(30)                    NOT NULL,
    object_name         VARCHAR2(30)                    NOT NULL,
    sql_text            CLOB                            NOT NULL,
    action_username     VARCHAR2(30)                    NOT NULL,
    action_date         DATE            DEFAULT SYSDATE NOT NULL,
    error_id            NUMBER,
    CONSTRAINT pk_cal_action_log PRIMARY KEY ( action_id ) )
    LOB ( sql_text ) STORE AS cal_lob_sql_text;

-- Constraints for CAL_ACTION_LOG
ALTER TABLE cal_action_log ADD CONSTRAINT cc_cal_action_name 
    CHECK ( action_name IN ('ALTER','CREATE','DELETE','DROP','INSERT','RENAME','SELECT','UPDATE') );

-- Foreign Key
ALTER TABLE cal_action_log ADD CONSTRAINT fk_cal_error_id FOREIGN KEY ( error_id ) REFERENCES cal_error_code ( error_id );
/******************************************************************************/

/******************************************************************************/
CREATE TABLE cal_registered_schema (
    schema_owner    VARCHAR2(30)                NOT NULL,
    register_flag   CHAR(1)         DEFAULT 'N' NOT NULL,
	schema_sync		INTEGER			DEFAULT 1 	NOT NULL,
    CONSTRAINT pk_cal_registered_schema PRIMARY KEY ( schema_owner ) );

-- Constraints for CAL_REGISTERED_SCHEMA
ALTER TABLE cal_registered_schema ADD CONSTRAINT cc_cs_capture_flag CHECK ( "REGISTER_FLAG"='Y' OR "REGISTER_FLAG"='N' );

-- Comments for CAL_REGISTERED_SCHEMA
COMMENT ON COLUMN cal_registered_schema.register_flag IS 'Flag indicating whether to capture DDL on the associated schema';
COMMENT ON COLUMN cal_registered_schema.schema_owner IS 'Name of the user (schema owner)';
/******************************************************************************/

/*** Sequences ***/
CREATE SEQUENCE seq_action_id INCREMENT BY 1 START WITH 1 MINVALUE 1 MAXVALUE 999999999999999999999999999 NOCYCLE NOORDER CACHE 20;
--CREATE SEQUENCE seq_error_id INCREMENT BY 1 START WITH 1 MINVALUE 1 MAXVALUE 999999999999999999999999999 NOCYCLE NOORDER CACHE 20;

/*** Triggers ***/
CREATE OR REPLACE TRIGGER tr_cal_bef_insert_action
    BEFORE INSERT ON cal_action_log
    REFERENCING OLD AS OLD NEW AS NEW
    FOR EACH ROW
BEGIN
    SELECT seq_action_id.NEXTVAL INTO :new.action_id FROM dual;
END;
/

--This is not needed right now
--CREATE OR REPLACE TRIGGER tr_cal_bef_insert_error
--    BEFORE INSERT ON cal_error_code
--    REFERENCING OLD AS OLD NEW AS NEW
--    FOR EACH ROW
--BEGIN
--    SELECT seq_error_id.NEXTVAL INTO :new.error_id FROM dual;
--END;
--/

spool off
