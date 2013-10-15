/*******************************************************************************
*  $Id: install_procs_funcs.sql 4799 2009-01-26 20:36:33Z bwelch $
*  Script Name:	install_procs_funcs.sql
*  Date Created:   2006.08.22
*  Author:		 Jason Lowe
*  Purpose:		Create the procedures and function needed to support retrieving
*				  an explain plan for a given session's current SQL statement.
/******************************************************************************/

spool install_procs_funcs.log

create or replace
PROCEDURE cal_sql_text (
    i_nSessionId    IN  v$session.audsid%TYPE,
    o_sSQLText      OUT VARCHAR2 ) AUTHID CURRENT_USER IS
    /*******************************************************************************
    *
    *   Purpose: Looks up current SQL Text for passed in sessionId.
    *
    *   MODIFICATION HISTORY:
    *
    *   Person                  Date            Comments
    *   ---------               ----------      ------------------------------------
    *   Jason Lowe              2006.08.28      Initial creation.
    *
    *******************************************************************************/

    /* Variable Declarations */
    nErrorId          cal_error_code.error_id%TYPE;
    sErrorDesc        cal_error_code.error_desc%TYPE;
    xException        EXCEPTION;
    iPos              INTEGER;
    iLineEnd          INTEGER;
    iStartQt          INTEGER;
    iCloseQt          INTEGER;
    iNumCommChr       INTEGER;
    sTmpString		  VARCHAR2(1000);
    --dbug UTL_FILE.FILE_TYPE := UTL_FILE.FOPEN ('/tmp', 'plsql.log', 'W');

    CURSOR mSQL IS
    select sql_text from v$sqlarea where users_executing > 0;


    CURSOR cSQL IS
    select sqltext.sql_text
    from V$SQLTEXT_WITH_NEWLINES sqltext, v$session sesion
    where sesion.sql_hash_value = sqltext.hash_value
    and sesion.sql_address = sqltext.address
    and sesion.username is not null
    and sesion.audsid = i_nSessionId and sesion.status = 'ACTIVE'
    order by sqltext.piece;
    
    

begin
    dbms_output.put_line('In cal_sql_text');
    --UTL_FILE.PUT_LINE (dbug, 'In cal_sql_text: ');

    /* Get the SQL statement executed */
    iStartQt := 0;
    iCloseQt := 0;

    FOR rSQL IN cSQL LOOP
        --UTL_FILE.PUT_LINE (dbug, 'before: ' || rSQL.sql_text  );

        /* @bug 445. handle comment in sql query */
        iPos := instr(rSQL.sql_text, '--');
        while iPos != 0 loop
          --UTL_FILE.PUT_LINE (dbug, 'ipos: ' || iPos  );
          iStartQt := instr(rSQL.sql_text, chr(39), iCloseQt+1);
          if iStartQt != 0 then
            iCloseQt := instr(rSQL.sql_text, chr(39), iStartQt+1);
            if iCloseQt = 0 then
                RAISE xException;
            end if;
          end if;
          --UTL_FILE.PUT_LINE (dbug, 'startqt: ' || iStartQt  );
          --UTL_FILE.PUT_LINE (dbug, 'closeqt: ' || iCloseQt  );
          if (iPos < iStartQt or iPos > iCloseQt) then
              iLineEnd := instr(rSQL.sql_text, chr(10), iPos);
              --UTL_FILE.PUT_LINE (dbug, 'ilineend1: ' || iLineEnd  );
              if iLineEnd = 0 then
                iNumCommChr := length(rSQL.sql_text) - iPos;
              else
                iNumCommChr := iLineEnd - iPos + 1;
              end if;
              ---UTL_FILE.PUT_LINE (dbug, 'ipos: ' || iPos  );
              rSQL.sql_text := REPLACE (rSQL.sql_text, substr(rSQL.sql_text, iPos, iNumCommChr));
              --UTL_FILE.PUT_LINE (dbug, 'after: ' || rSQL.sql_text  );
              iPos := instr(rSQL.sql_text, '--', iPos+1);
          else
              iPos := instr(rSQL.sql_text, '--', iCloseQt);
          end if;
        end loop;
        rSQL.sql_text := replace(rSQL.sql_text, CHR(10), ' ');
        IF o_sSQLText IS NULL THEN
            o_sSQLText := rSQL.sql_text;
        ELSE
            o_sSQLText := o_sSQLText || rSQL.sql_text;
        END IF;
    END LOOP;
    
    -- @bug 1331. create materialized view
    if o_sSQLText is null then
      --UTL_FILE.PUT_LINE (dbug, 'sqltext--empty' );
      FOR rSQL IN mSQL LOOP
        IF o_sSQLText IS NULL THEN
            sTmpString := rSQL.sql_text;
            sTmpString := lower (sTmpString);
            if instr(sTmpString, 'create materialized view') != 0 then
              o_sSQLText := rSQL.sql_text;
              --UTL_FILE.PUT_LINE (dbug, 'sqltext--' || rSQL.sql_text );
            end if;
        end if;
      END LOOP;
    end if;

    dbms_output.put_line('In cal_sql_text - o_sSQLText = ' || o_sSQLText);
    --UTL_FILE.PUT_LINE (dbug, 'final sqltext: ' || o_sSQLText );
    --UTL_FILE.FCLOSE (dbug);

EXCEPTION
    WHEN OTHERS THEN
        dbms_output.put_line('In cal_sql_text EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
        nErrorId := ABS( SQLCODE );
        sErrorDesc := SQLERRM;
        --UTL_FILE.PUT_LINE (dbug, sErrorDesc);
        --UTL_FILE.FCLOSE (dbug);
        RAISE xException;
END cal_sql_text;
/

CREATE OR REPLACE PROCEDURE cal_clean_sql(
  i_nSessionId	IN v$session.audsid%TYPE,
  i_sSQLText	  IN  VARCHAR2,
  o_sCleanSQLText OUT VARCHAR2) AUTHID CURRENT_USER IS
  /*******************************************************************************
  *
  *   Purpose: Cleans passed in SQL text of unwanted characters, and replaces
  *			explicit owner.tablename with shadowowner.tablename.  Note:
  *			The procedure is dependent upon passed in SQL text being equal
  *			to the current 'ACTIVE' SQL for the passed in session id.
  *
  *   MODIFICATION HISTORY:
  *
  *   Person				  Date			Comments
  *   ---------			   ----------	  ------------------------------------
  *   Jason Lowe			  2007.05.31	  Initial creation, and moved Bob's
  *										   clean code to here from cal_explain_plan
  *
  *******************************************************************************/

  /* Variable Declarations */
  sSQL VARCHAR2(4000) := NULL;
  sDepObj VARCHAR2(61);
  sShadowDepObj VARCHAR2(61);
  nErrorId		  cal_error_code.error_id%TYPE;
  sErrorDesc		cal_error_code.error_desc%TYPE;
  xException		EXCEPTION;  
  i				 INTEGER;
  state			 INTEGER := 0;
  c				 CHAR;
BEGIN
	/* Clean the sql statement of any unwanted characters */
	i := 1;
	LOOP
		c := SUBSTR(i_sSQLText, i, 1);
		IF c = '''' THEN
			IF state = 1 THEN
				--dbms_output.put_line('going out of quoted-string state at char ' || i);
				state := 0;
			ELSE
				--dbms_output.put_line('going into quoted-string state at char ' || i);
				state := 1;
			END IF;
		END IF;
		IF state = 1 THEN
			GOTO no_xlate;
		END IF;
		IF c = CHR(9) THEN
			c := ' ';
		ELSIF c = CHR(10) THEN
			c := ' ';
		ELSE
			NULL;
		END IF;
		IF c = ' ' THEN
			IF state != 2 THEN
				--dbms_output.put_line('going into gobble-space state at char ' || i);
				state := 2;
				GOTO no_xlate;
			ELSE
				GOTO gobble;
			END IF;
		END IF;
		IF state = 2 THEN
			IF c = '.' THEN
				sSQL := SUBSTR(sSQL, 1, LENGTH(sSQL) - 1);
				sSQL := sSQL || c;
				i := i + 1;
				IF i <= LENGTH(i_sSQLText) THEN
					c := SUBSTR(i_sSQLText, i, 1);
					WHILE c = ' ' LOOP
						i := i + 1;
						EXIT WHEN i > LENGTH(i_sSQLText);
						c := SUBSTR(i_sSQLText, i, 1);
					END LOOP;
				END IF;
				GOTO no_xlate;
			END IF;
			--dbms_output.put_line('going into initial state at char ' || i);
			state := 0;
		ELSIF c = '.' THEN
			sSQL := sSQL || c;
			i := i + 1;
			IF i <= LENGTH(i_sSQLText) THEN
				c := SUBSTR(i_sSQLText, i, 1);
				WHILE c = ' ' LOOP
					i := i + 1;
					EXIT WHEN i > LENGTH(i_sSQLText);
					c := SUBSTR(i_sSQLText, i, 1);
				END LOOP;
			END IF;
		ELSE
			NULL;
		END IF;
<<no_xlate>>
		/* Uppercase any character not within quotes for next step */
		IF state != 1 THEN
		  c := UPPER(c);
		END IF;
		sSQL := sSQL || c;
<<gobble>>
		i := i + 1;
		EXIT WHEN i > LENGTH(i_sSQLText);
	END LOOP;
	
	/* Remove unprintable character found at end of string */
	/* sSQL := SUBSTR(sSQL, 0, LENGTH(i_sSQLText) -1); */
	
  /* Loop through distinct list of objects that the current sql is dependent upon,
	 and replace any explicit owner.tablename with shadowowner.tablename */
  FOR x IN ( SELECT DISTINCT *
			   FROM v$object_dependency
			  WHERE to_type = 4
				AND from_address = ( SELECT sql_address
									   FROM (SELECT sql_address
											   FROM v$session
											  WHERE audsid = i_nSessionId
												AND status = 'ACTIVE'
												AND username IS NOT NULL)
								   )
		   )
  LOOP
	/* Dependent owner.tablename */
	sDepObj := x.to_owner || '.' || x.to_name;
	/* Remove the calpont view prefix if exists 'VW_', this is due to view backed with table function */
	sDepObj := REPLACE(sDepObj, 'VW_', '');

	/* Prepend shadow schema prefix 'S_ to sDepObj */
	sShadowDepObj := 'S_' || sDepObj;

	/* Replace any explicit occurance of owner.tablename with shadowowner.tablename.
	   Note: sSQL is required to first be processed by the above clean code.
	   BigNote: This does not exclude at this time replacing owner.tablename that might be within quoted strings in the SQL */
	sSQL := REPLACE(sSQL, sDepObj, sShadowDepObj);
  END LOOP;

  /* Output final cleaned SQL Text */
  o_sCleanSQLText := sSQL;
  
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_clean_sql EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;  
END cal_clean_sql;
/

CREATE OR REPLACE PROCEDURE cal_explain_plan (
	i_nSessionId	  IN v$session.audsid%TYPE,
	i_sSQLText		IN  VARCHAR2,
	i_sCurrSchemaName IN VARCHAR2,
	o_sStmtId		 OUT VARCHAR2 ) AUTHID CURRENT_USER IS
	/*******************************************************************************
	*
	*   Purpose: Performs an explain plan for the SQL passed in,
	*			and returns the statementId for the explain plan.
	*			Note:  i_sCurrSchemaName is the user's current_schema from thier session,
	*			which can be altered by the user.
	*			Note:  This procedure being defined as invoker's right (authid current_user)
	*			is necessary for the alter session statements.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Jason Lowe			  2006.08.22	  Initial creation.
	*   Jason Lowe			  2007.05.31	  Added cal_clean_sql, moved Bob's clean code to it,
	*										   and changed how the current shadow schema was determined.
	*										   And added i_sCurrSchemaName to input params.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId		  cal_error_code.error_id%TYPE;
	sErrorDesc		cal_error_code.error_desc%TYPE;
	xException		EXCEPTION;
	sShadowOwner	  cal_action_log.object_owner%TYPE;
	sOrigUser		 cal_action_log.object_owner%TYPE;
	sSQL			  VARCHAR2(4000) := NULL;
BEGIN
	dbms_output.put_line('In cal_explain_plan');
	dbms_output.put_line('In cal_explain_plan - i_sSQLText before clean = ' || i_sSQLText);

	/* Create the shadow owner name based on the passed in current schema of the user performing
	   the query that indirectly invoked this procedure. i_sCurrSchemaName can be influenced by
	   the user altering their session current_schema. */
	sShadowOwner := 'S_' || i_sCurrSchemaName;	
	/* Save off the name of the current logged in user for resetting the session current_schema below...*/
	select sys_context('USERENV', 'SESSION_USER') into sOrigUser from dual;

	/* Clean passed in SQL Text of unwanted characters, and replace explicit owner.tablename with shadowowner.tablename */
	cal_clean_sql(i_nSessionId, i_sSQLText, sSQL);
	
	/* Execute explain plan as shadow table owner and reset current_schema back to original user */
	o_sStmtId := TO_CHAR( i_nSessionId ) || TO_CHAR( SYSTIMESTAMP, 'YYYYMMDDHHMISS.FF3' );
	EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA = ' || sShadowOwner;   
	/* @bug 942   */
	execute immediate 'Alter session set nls_date_format = ''yyyy-mm-dd hh24:mi:ss''';   
	EXECUTE IMMEDIATE 'EXPLAIN PLAN SET statement_id = ''' || o_sStmtId || ''' INTO SYS.PLAN_TABLE$ FOR ' || sSQL;
	EXECUTE IMMEDIATE 'ALTER SESSION SET CURRENT_SCHEMA = ' || sOrigUser;

	dbms_output.put_line('In cal_explain_plan - sShadowOwner = ' || sShadowOwner);
	dbms_output.put_line('In cal_explain_plan - i_sSQLText after clean = ' || sSQL);
	dbms_output.put_line('In cal_explain_plan - explain plan o_sStmtId = ' || o_sStmtId);
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_explain_plan EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END cal_explain_plan;
/

CREATE OR REPLACE PROCEDURE cal_format_explain_plan ( i_sStmtId IN VARCHAR2, o_cXPlan OUT SYS_REFCURSOR ) AUTHID CURRENT_USER IS
	/*******************************************************************************
	*
	*   Purpose: Formats the explain plan for passed in statementId, and returns a
	*			cursor of the resultset.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Jason Lowe			  2006.08.22	  Initial creation.
	*   Jason Lowe			  2006.08.22	  Will need to handle the REPLACE on object_owner
	*										   differently later since the orginal owner name
	*										   could have been truncated by 2 chars.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId		  cal_error_code.error_id%TYPE;
	sErrorDesc		cal_error_code.error_desc%TYPE;
	xException		EXCEPTION;

BEGIN
	dbms_output.put_line('In cal_format_explain_plan');
	OPEN o_cXPlan FOR SELECT
				   --LPAD(' ' , ptLEVEL) || lower(OPERATION) ||
				   lower(OPERATION) ||
				   decode(temp_space, null, '', ' ( Temp Space:' || to_char(temp_space, '999,999,999,999')  || ' )') OPERATION,
				   options, object_type, other, REPLACE(object_owner, 'S_', '') object_owner,
				   search_columns,
				   replace(replace(replace(REGEXP_REPLACE(REGEXP_REPLACE (projection,'\[.{1,15}\]' ,''), '\(\#.{6,7}\)'),'"',''), 'COUNT(*)', 'COUNT(ALL)'), ', ', '^') projection,
				   --decode(object_type,'TABLE',object_name,null) table_name,
				   object_name,
				   --|| ( SELECT LOWER(  LPAD('(',ptlevel + 3) || dic1.column_name ||
				   --		  DECODE(dic2.column_name, null,'' ,',' || dic2.column_name) ||
				   --		DECODE(dic3.column_name, null,'' ,',' || dic3.column_name) ||
				   --	  DECODE(dic4.column_name, null,')', ',...)')   )
				   --FROM dba_ind_columns dic1, dba_ind_columns dic2, dba_ind_columns dic3, dba_ind_columns dic4
				   -- WHERE dic1.index_name = OBJECT_NAME  AND dic1.index_owner = OBJECT_OWNER AND dic1.column_position = 1
				   -- AND dic2.index_name(+) = dic1.index_name  AND dic2.index_owner(+) = dic1.index_owner AND dic2.column_position(+) = 2
				   --AND dic3.index_name(+) = dic2.index_name  AND dic3.index_owner(+) = dic2.index_owner AND dic3.column_position(+) = 3
				   --AND dic4.index_name(+) = dic3.index_name  AND dic4.index_owner(+) = dic3.index_owner AND dic4.column_position(+) = 4 )
				   decode(object_alias,null,'', upper(object_alias)  ) alias,
				   --|| decode(object_alias,null,'',' (' || lower(object_alias)  || ') ' )
				   --|| decode(projection,null,'',' (' || lower(projection)  || ') ' )
				   --object_name,
				   DECODE(access_predicates,filter_predicates,
					  DECODE(access_predicates,null, '', LPAD('AFP:', ptlevel +6) ||
					  REPLACE(REPLACE(REPLACE(access_predicates, ' OR ', lpad('',ptlevel +2) || ' OR ' ),'"',''),' AND ', LPAD('',ptlevel +2) || ' AND ' )  )   ,
					  DECODE(access_predicates,null,'', LPAD('AP:',ptlevel +5) ||
					  REPLACE(REPLACE(REPLACE(access_predicates, ' OR ', lpad('',ptlevel +2) || ' OR ' ),'"',''),' AND ', LPAD('',ptlevel +2) || ' AND ' )  )
					  || DECODE(filter_predicates,null, '', LPAD('FP:',ptlevel +5) ||
					  REPLACE(REPLACE(REPLACE(filter_predicates, ' OR ', LPAD('',ptlevel +2) || ' OR ' ),'"',''),' AND ', LPAD('',ptlevel +2) || ' AND ' )  )
				   ) EXTENDED_INFORMATION,
				   replace(replace(replace(access_predicates, '"', ''), ' OR ', '|'), ' AND ', '&'),
				   replace(replace(replace(filter_predicates, '"', ''), ' OR ', '|'), ' AND ', '&'),
				   qblock_name select_level,
				   parent_id,
				   id,
				   cardinality
				   FROM ( SELECT 1  ptlevel, SYS.PLAN_TABLE$.*
							FROM SYS.PLAN_TABLE$
						   START WITH id = 0
							 AND statement_id = i_sStmtId
						 CONNECT BY PRIOR id = parent_id
							 AND statement_id = i_sStmtId
						   ORDER BY depth DESC, parent_id, position );
--TEST LOOP
--	FOR xplan IN () LOOP
--  dbms_output.put_line(rpad(xplan.operation  || '-' || xplan.options,30) ||  xplan.object_owner || '.' || xplan.object_name || ' (' || xplan.object_type || ')' );
--  dbms_output.put_line('---- '|| xplan.EXTENDED_INFORMATION);
--  dbms_output.put_line('---- '|| xplan.other);
--  dbms_output.put_line('---- '|| rpad(xplan.search_columns,30) || xplan.projection );
--	END LOOP;
--	EXECUTE IMMEDIATE 'DELETE FROM SYS.PLAN_TABLE$ WHERE statement_id = ''' || i_sStmtId || '''';
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_format_explain_plan EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END cal_format_explain_plan;
/


CREATE OR REPLACE
FUNCTION cal_get_explain_plan ( i_nSessionid IN v$session.audsid%TYPE, i_sCurrSchemaName IN VARCHAR2, i_sqltext IN VARCHAR2 ) RETURN SYS_REFCURSOR AUTHID CURRENT_USER IS
	/*******************************************************************************
	*
	*   Purpose: Based on passed in sessionId, gets and formats an explain plan for
	*			the current active SQL statement, and returns a cursor of the
	*			explain plan resultset.
	*			Note:  i_sCurrSchemaName is the user's current_schema from thier session,
	*			which can be altered by the user.
	*			Note:  This function is being defined, along with the procedures it calls, as
	*			invoker's right (authid current_user) which is necessary for the alter session in
	*			cal_explain_plan.	
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Jason Lowe			  2006.08.22	  Initial creation.
	*   Jason Lowe			  2007.05.31	  Added i_sCurrSchemaName to input params.
	*   Zhixuan Zhu			 2007.08.09	  Removed cal_sql_text call. sql_text was
	*										   obtained in external c++ procedure and
	*										   passed in as a parameter.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId		  cal_error_code.error_id%TYPE;
	sErrorDesc		cal_error_code.error_desc%TYPE;
	xException		EXCEPTION;
	sStmtId		   VARCHAR2(30);
	sSQLText		  VARCHAR2(4000);
	cXPlan			SYS_REFCURSOR;
BEGIN
	dbms_output.put_line('In cal_get_explain_plan');

	/* Get the SQL being executed by the specified session */
	-- cal_sql_text( i_nSessionid, sSQLText);

	/* Generate the explain plan for the SQL being executed by the specified session */
	cal_explain_plan( i_nSessionid, i_sqltext, i_sCurrSchemaName, sStmtId );

	/* Format the explain plan for return to caller */
	cal_format_explain_plan ( sStmtId, cXPlan );

	RETURN cXPlan;
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_get_explain_plan EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END cal_get_explain_plan;
/


CREATE OR REPLACE
FUNCTION cal_get_DML_explain_plan ( i_nSessionid IN v$session.audsid%TYPE, sSQLText IN VARCHAR2, i_sCurrSchemaName IN VARCHAR2 ) RETURN SYS_REFCURSOR AUTHID CURRENT_USER IS
	/*******************************************************************************
	*
	*   Purpose: Based on passed in sessionId and SQL text, gets and formats an explain plan for
	*			the current active SQL statement, and returns a cursor of the
	*			explain plan resultset.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Cindy Hao			   2007.04.11	  Initial creation.
	*   Jason Lowe			  2007.06.07	  Added i_sCurrSchemaName to input params.
	*										   Note:  i_sCurrSchemaName is the user's
	*										   current_schema from thier session,
	*										   which can be altered by the user.  
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId		  cal_error_code.error_id%TYPE;
	sErrorDesc		cal_error_code.error_desc%TYPE;
	xException		EXCEPTION;
	sStmtId		   VARCHAR2(30);
	cXPlan			SYS_REFCURSOR;
BEGIN
	dbms_output.put_line('In cal_get_DML_explain_plan');

	/* Generate the explain plan for the SQL being executed by the specified session */
	cal_explain_plan( i_nSessionid, sSQLText, i_sCurrSchemaName, sStmtId );

	/* Format the explain plan for return to caller */
	cal_format_explain_plan ( sStmtId, cXPlan );

	RETURN cXPlan;
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_get_DML_explain_plan EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END cal_get_DML_explain_plan;
/

CREATE OR REPLACE
FUNCTION cal_get_sql_text ( i_nSessionId IN v$session.audsid%TYPE ) RETURN VARCHAR2 AUTHID CURRENT_USER IS
	/*******************************************************************************
	*
	*   Purpose: Based on passed in sessionId, gets current sql text being executed.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Jason Lowe			  2006.08.28	  Initial creation.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId		  cal_error_code.error_id%TYPE;
	sErrorDesc		cal_error_code.error_desc%TYPE;
	xException		EXCEPTION;
	sSQLText		  VARCHAR2(4000);
BEGIN
	dbms_output.put_line('In cal_get_sql_text');

	/* Get the SQL being executed by the specified session */
	cal_sql_text( i_nSessionid, sSQLText);

	RETURN sSQLText;
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_get_sql_text EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END cal_get_sql_text;
/

CREATE OR REPLACE
PROCEDURE calcommit IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to look up session id and call external library function
	*			that will transmit a COMMIT to the EC.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Jason Lowe			  2006.10.23	  Initial creation.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId	cal_error_code.error_id%TYPE;
	sErrorDesc  cal_error_code.error_desc%TYPE;
	xException  EXCEPTION;
	nSessionId  v$session.sid%TYPE;
	nRc		 NUMBER;
BEGIN
	dbms_output.put_line('In calcommit');

	/* Retrieve the sessionId for the user that just logged in */
	nSessionId := SYS_CONTEXT( 'USERENV', 'SESSIONID');
	dbms_output.put_line('In calcommit - nSessionId = ' || nSessionId);

	/* Call external function to transmit a COMMIT to the EC for the session */
	nRc := cal_commit( nSessionId );
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In calcommit EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END calcommit;
/

CREATE OR REPLACE
PROCEDURE calrollback IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to look up session id and call external library function
	*			that will transmit a ROLLBACK to the EC.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Jason Lowe			  2006.10.23	  Initial creation.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId	cal_error_code.error_id%TYPE;
	sErrorDesc  cal_error_code.error_desc%TYPE;
	xException  EXCEPTION;
	nSessionId  v$session.sid%TYPE;
	nRc		 NUMBER;
BEGIN
	dbms_output.put_line('In calrollback');

	/* Retrieve the sessionId for the user that just logged in */
	nSessionId := SYS_CONTEXT( 'USERENV', 'SESSIONID');
	dbms_output.put_line('In calrollback - nSessionId = ' || nSessionId);

	/* Call external function to transmit a ROLLBACK to the EC for the session */
	nRc := cal_rollback( nSessionId );
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In calrollback EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END calrollback;
/

CREATE or REPLACE
PROCEDURE cal_setstats (sTbName IN VARCHAR2 := 'ALL') AUTHID CURRENT_USER IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to retrieve Calpont table and column statistics and set
	*			Oracle's statistics.
	*   Parameter: sTbName the table that the user want to set stats
	*					  default all tables in the current schema
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				Date			Comments
	*   ---------			----------		------------------------------------
	*   Zhixuan Zhu			2007.05.03		Initial creation.
	*   Zhixuan Zhu			2007.11.06		clean up srec after each set
	*	Zhixuan Zhu			2008.10.06		trim chararray value to be 32 bytes
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId		cal_error_code.error_id%TYPE;
	sErrorDesc		cal_error_code.error_desc%TYPE;
	xException		EXCEPTION;
	TYPE Stats_type	IS REF CURSOR;
	cTableStats		Stats_type;
	cColStats		Stats_type;
	sSqlTable		VARCHAR2(1000);
	sSqlCol			VARCHAR2(1000);
	sTableName		VARCHAR2(30);
	sColumnName		VARCHAR(30);
	sShadowUserName	VARCHAR2(30);
	sColumnType		VARCHAR2(30);
	nNumRows		NUMBER;
	nNumBlks		NUMBER;
	nAvgrLen		NUMBER;
	nDistCtn		NUMBER;
	nDensity		NUMBER;
	nNullCtn		NUMBER;
	nColLen			NUMBER;
	srec			dbms_stats.StatRec;
	nVals			dbms_stats.numarray := dbms_stats.numarray();
	dVals			dbms_stats.datearray := dbms_stats.datearray();
	cVals			dbms_stats.chararray := dbms_stats.chararray();
	sVals			dbms_stats.chararray := dbms_stats.chararray();


	CURSOR cTables(sOwner VARCHAR2) IS
		SELECT table_name FROM all_tables where owner = sOwner;
	CURSOR cCols(sOwner VARCHAR2, sTable VARCHAR2) IS
		SELECT column_name from all_tab_cols where owner=sOwner and table_name=sTable;
	CURSOR cColType(sOwner VARCHAR2, sTable VARCHAR2, sCol VARCHAR2) IS
		SELECT data_type, data_length FROM all_tab_cols WHERE owner=sOwner AND table_name=sTable AND column_name=sCol;

BEGIN
	nVals.extend(2);
	dVals.extend(2);
	cVals.extend(2);
	sVals.extend(2);
	srec.epc := 2;

	sShadowUserName := 'S_' || UPPER( SYS_CONTEXT( 'USERENV', 'SESSION_USER' ));

	FOR rTable IN cTables(sShadowUserName) LOOP
		-- get all tables belong to this registered user
		sTableName := UPPER( rTable.table_name);
		-- one table option
		IF sTbName != 'ALL' and sTableName != upper(sTbName) then
			GOTO end_table;
		END IF;

		dbms_output.put_line('tablename: ' || sTableName);

		--get table stats
		sSqlTable := 'SELECT count(*) numofrows , 8 avgrowlen, Ceil(8 * count(*) / 8192)  numofblocks from ' || sTableName;
		OPEN cTableStats for sSqlTable;
			FETCH cTableStats INTO nNumRows, nNumBlks, nAvgrLen;
			dbms_output.put_line('nNumRow = ' || nNumRows || ' nNumBlks = ' || nNumBlks || ' nAvgLen = ' || nAvgrLen);
			-- set oracle table stats
			dbms_stats.set_table_stats(sShadowUserName, sTableName, numrows => nNumRows, numblks => nNumBlks, avgrlen => nAvgrLen);
			dbms_output.put_line('Table stats set for ' || sTableName || CHR(10) || CHR(9));
		CLOSE cTableStats;
		-- get table columns
		FOR rCol IN cCols(sShadowUserName, sTableName) LOOP
			sColumnName := rCol.column_name;
			-- get column stats one by one
			FOR rColType IN cColType(sShadowUserName, upper(sTableName), upper(sColumnName)) LOOP
				sColumnType := rColType.data_type;
				nColLen := rColType.data_length;
				dbms_output.put_line(CHR(10) || CHR(9) || sColumnName || ' ' || sColumnType || ' ' || nColLen);
				EXIT;
			END LOOP;

			sSqlCol := 'SELECT count(distinct(' || sColumnName || ')), sum(decode(' || sColumnName || ',null,1,0)), min(' || sColumnName || '), max(' || sColumnName || ') from ' || sTableName;

			-- don't get stats for string column and size>40
			IF (sColumnType = 'CHAR' or sColumnType = 'VARCHAR2') AND nColLen > 40 THEN
				GOTO end_loop;
			END IF;
			OPEN cColStats FOR sSqlCol;
				-- check column type
				IF sColumnType = 'VARCHAR2' THEN
					FETCH cColStats INTO nDistCtn, nNullCtn, sVals(1), sVals(2);
					sVals(1) := substr(sVals(1), 1, 32);
					sVals(2) := substr(sVals(2), 1, 32);
					dbms_output.put_line('nDistCtn = ' || nDistCtn || ' nNullCtn = ' || nNullCtn || ' minVal = ' || sVals(1) || ' maxVal = ' || sVals(2));
					dbms_stats.prepare_column_values(srec, sVals);
				ELSIF sColumnType = 'CHAR' THEN
					FETCH cColStats INTO nDistCtn, nNullCtn, cVals(1), cVals(2); 
					cVals(1) := substr(cVals(1), 1, 32);
					cVals(2) := substr(cVals(2), 1, 32);				   
					dbms_output.put_line('nDistCtn = ' || nDistCtn || ' nNullCtn = ' || nNullCtn || ' minVal = ' || cVals(1) || ' maxVal = ' || cVals(2));
					dbms_stats.prepare_column_values(srec, cVals);
				ELSIF sColumnType = 'NUMBER' THEN
					FETCH cColStats INTO nDistCtn, nNullCtn, nVals(1), nVals(2);
					dbms_output.put_line('nDistCtn = ' || nDistCtn || ' nNullCtn = ' || nNullCtn || ' minVal = ' || nVals(1) || ' maxVal = ' || nVals(2));
					dbms_stats.prepare_column_values(srec, nVals);
				ELSIF sColumnType = 'DATE' THEN
					FETCH cColStats INTO nDistCtn, nNullCtn, dVals(1), dVals(2);
					dbms_output.put_line('nDistCtn = ' || nDistCtn || ' nNullCtn = ' || nNullCtn || ' minVal = ' || dVals(1) || ' maxVal = ' || dVals(2));
					dbms_stats.prepare_column_values(srec, dVals);
				ELSE
					--RAISE_APPLICATION_ERROR(-20001, 'Datatype unhandled');
					dbms_output.put_line('exception' || sColumnName);				   
				END IF;

				-- set oracle column stats
				IF nDistCtn = 0 THEN
					nDensity := 0;
				ELSE
					nDensity := 1 / nDistCtn;
				END IF;
				dbms_stats.set_column_stats(sShadowUserName, sTableName, sColumnName, distcnt => nDistCtn, density => nDensity, nullcnt => nNullctn, srec => srec, avgclen => 5);
				dbms_output.put_line('Column stats set for ' || sColumnName);

				-- reset srec
				srec.epc := 2;
				srec.minval := '';
				srec.maxval := '';
				srec.bkvals := null;
				srec.novals := null;
				srec.chvals := null;
				srec.eavs := 0;			   
			CLOSE cColStats;
			<<end_loop>>
			NULL;	   
		END LOOP;
		<<end_table>>
		NULL;
	  END LOOP;
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In cal_setstats EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END cal_setstats;
/

-- Create the types for the table function's output collection 
-- and collection elements

-- @bug 1024. drop types first to avoid errors while replacing dependent types.
BEGIN
execute immediate 'drop type BindValueSet force';
execute immediate 'drop type BindValue force';
EXCEPTION
WHEN others THEN
null;
end;
/

CREATE TYPE BindValue AS OBJECT
(
  sqltext VARCHAR2(15000),
  bindVar VARCHAR2(32),
  bindVal VARCHAR2(15000)
);
/

CREATE TYPE BindValueSet AS TABLE OF BindValue;
/

create or replace
FUNCTION cal_get_bind_values return BindValueSet authid current_user IS
	/*******************************************************************************
	*
	*   Purpose: Function to retrieve bind values from Oracle for the current stmt.
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Zhixuan Zhu			  2007.08.03	  Initial creation.
	*   Zhixuan Zhu			  2007.08.18	  Pass back nested table instead of cursor
	*
	*******************************************************************************/
	/* Variable Declarations */
		cBindValues	 SYS_REFCURSOR;
		nUserId		 NUMBER;
		sSqlText		varchar(15000);
		nCount		  number;
		sBindName	  varchar(32);
		sBindValue	 varchar(15000);
		bindValueList   BindValueSet := BindValueSet();

BEGIN
--dbms_output.put_line('In cal_get_bind_values');
	nUserId := sys_context('USERENV', 'CURRENT_SCHEMAID');
	nCount := 0;
	open cBindValues for select
		  v$sql_bind_metadata.BIND_NAME,
		  decode(v$sql_bind_data.datatype, 1, '''' || v$sql_bind_data.value || '''', v$sql_bind_data.VALUE) value,
		  sql_fulltext
		from v$sql_cursor, v$sql, v$sql_bind_data, v$sql_bind_metadata
		where v$sql.address = v$sql_cursor.PARENT_HANDLE
		  and v$sql_cursor.CURNO = v$sql_bind_data.CURSOR_NUM
		  and v$sql.CHILD_ADDRESS =v$sql_bind_metadata.address
		  and v$sql_bind_data.POSITION = v$sql_bind_metadata.POSITION
		  and parsing_user_id = nUserId
		order by v$sql_cursor.CURNO, v$sql_bind_data.POSITION;

	-- fetch cBindValues and build nested table
	loop
	 fetch cBindValues into sBindName, sBindValue, sSqlText;
	  exit when cBindValues%NOTFOUND = true;
	  nCount := nCount + 1;
	  bindValueList.extend;
	  bindValueList(nCount) := BindValue(sSqlText, sBindName, sBindValue);
	end loop;
	close cBindValues;
	return bindValueList;
END cal_get_bind_values;
/

CREATE OR REPLACE
PROCEDURE caltraceon (nFlags IN NUMBER := 1) IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to turn on statement tracing in Calpont
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Bob Dempsey			 2007.09.12	  Initial creation.
	*   Bob Dempsey			 2007.12.19	  Pass flags
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId	cal_error_code.error_id%TYPE;
	sErrorDesc  cal_error_code.error_desc%TYPE;
	xException  EXCEPTION;
	nSessionId  v$session.sid%TYPE;
	nRc		 NUMBER;
BEGIN
	/* Retrieve the sessionId for the user that just logged in */
	nSessionId := SYS_CONTEXT( 'USERENV', 'SESSIONID');

	/* Call external function to set the trace flag for the session */
	nRc := cal_trace_on( nSessionId, nFlags );
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In caltraceon EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END caltraceon;
/

CREATE OR REPLACE
PROCEDURE caltraceoff IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to turn off statement tracing in Calpont
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Bob Dempsey			 2007.09.12	  Initial creation.
	*   Bob Dempsey			 2007.12.19	  Pass flags
	*
	*******************************************************************************/

BEGIN
	/* Call external function to clear the trace flag for the session */
	caltraceon( 0 );

END caltraceoff;
/

CREATE OR REPLACE
PROCEDURE calsetenv (sName IN VARCHAR, sValue IN VARCHAR) IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to set an environment var
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Bob Dempsey			 2008.02.12	  Initial creation.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId	cal_error_code.error_id%TYPE;
	sErrorDesc  cal_error_code.error_desc%TYPE;
	xException  EXCEPTION;
	nRc		 NUMBER;
BEGIN
	/* Call external function to set the env for the session */
	nRc := cal_set_env( sName, sValue );
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In calsetenv EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END calsetenv;
/

CREATE OR REPLACE
FUNCTION getstats return VARCHAR IS
	/*******************************************************************************
	*
	*   Purpose: Return query stats to Oracle
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Bob Dempsey			 2008.07.15	  Initial creation.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId	cal_error_code.error_id%TYPE;
	sErrorDesc  cal_error_code.error_desc%TYPE;
	xException  EXCEPTION;
	nRc		 NUMBER;
	nSessionId  v$session.sid%TYPE;
	sStats	  VARCHAR(4000);
BEGIN
	nSessionId := SYS_CONTEXT( 'USERENV', 'SESSIONID');
	/* Call external function to get the query stats for the previous query */
	nRc := cal_get_query_stats( nSessionId, sStats );
	IF nRc = 0 THEN
		RETURN sStats;
	ELSE
		RETURN NULL;
	END IF;
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In getstats EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
	RETURN NULL;
END getstats;
/

CREATE OR REPLACE
PROCEDURE calsetparms (param IN VARCHAR, value IN VARCHAR) IS
	/*******************************************************************************
	*
	*   Purpose: Procedure to set a Resource Manager parameter
	*
	*   MODIFICATION HISTORY:
	*
	*   Person				  Date			Comments
	*   ---------			   ----------	  ------------------------------------
	*   Barbara Welch		    2009.01.12	  Initial creation.
	*
	*******************************************************************************/

	/* Variable Declarations */
	nErrorId	cal_error_code.error_id%TYPE;
	sErrorDesc  cal_error_code.error_desc%TYPE;
	xException  EXCEPTION;
	nSessionId  v$session.sid%TYPE;
	nRc		 NUMBER;
BEGIN
	/* Retrieve the sessionId for the user that just logged in */
	nSessionId := SYS_CONTEXT( 'USERENV', 'SESSIONID');

	/* Call external function to set the trace flag for the session */
	nRc := cal_setparms( nSessionId, param, value );
EXCEPTION
	WHEN OTHERS THEN
		dbms_output.put_line('In calsetparms EXCEPTION HANDLER ' || SQLCODE || ', ' || SQLERRM);
		nErrorId := ABS( SQLCODE );
		sErrorDesc := SQLERRM;
		RAISE xException;
END calsetparms;
/
spool off
