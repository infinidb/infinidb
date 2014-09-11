/* Copyright (C) 2014 InfiniDB, Inc.

   This program is free software; you can redistribute it and/or
   modify it under the terms of the GNU General Public License
   as published by the Free Software Foundation; version 2 of
   the License.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/* $Id: ddl.y 7694 2011-05-11 19:14:46Z rdempsey $ */
/* This describes a substantial subset of SQL92 DDL with some
   enhancements from various vendors.  Most of the nomenclature in the
   grammar is drawn from SQL92. 

   If you have to care about this grammar, you should consult one or
   more of the following sources:

   Lex and Yacc book.
   ANSI SQL92 standard
   Understanding the New Sql book
   The postgress and mysql sources.  find x -name \*.y -o -name \*.yy.

   We don't support delimited identifiers.

   All literals are stored as unconverted strings.

   You can't say "NOT DEFERRABLE".  See the comment below.

   This is not a reentrant parser.  It uses the original global
   variable style method of communication between the parser and
   scanner.  If we ever needed more than one parser thread per
   processes, we would use the pure/reentrant options of bison and
   flex.  In that model, things that are traditionally global live
   inside a struct that is passed around.  We would need to upgrade to
   a more recent version of flex.  At the time of this writing, our
   development systems have: flex version 2.5.4

   */

%{
#include "sqlparser.h"

#include "ddl-gram.h"

using namespace std;
using namespace ddlpackage;	

/* The user is expect to pass a ParseTree* to grammar_init */
static ParseTree* parseTree;
static std::string db_schema;

int ddllex();
void ddlerror (char const *error);

%}

%expect 6

%debug

 /* Bison uses this to generate a C union definition.  This is used to
	store the application created values associated with syntactic
	constructs. */

%union {
  ddlpackage::AlterTableStatement *alterTableStmt;
  ddlpackage::AlterTableAction *ata;
  ddlpackage::AlterTableActionList *ataList;
  ddlpackage::DDL_CONSTRAINT_ATTRIBUTES cattr;
  std::pair<std::string, std::string> *tableOption;
  const char *columnOption;
  ddlpackage::ColumnConstraintDef *columnConstraintDef;
  ddlpackage::ColumnNameList *columnNameList;
  ddlpackage::ColumnType* columnType;
  ddlpackage::ConstraintAttributes *constraintAttributes;
  ddlpackage::ColumnConstraintList *constraintList;
  ddlpackage::DDL_CONSTRAINTS constraintType;
  double dval;
  bool flag;
  int ival;
  ddlpackage::QualifiedName *qualifiedName;
  ddlpackage::SchemaObject *schemaObject;
  ddlpackage::SqlStatement *sqlStmt;
  ddlpackage::SqlStatementList *sqlStmtList;
  const char *str;
  ddlpackage::TableConstraintDef *tableConstraint;
  ddlpackage::TableElementList *tableElementList;
  ddlpackage::TableOptionMap *tableOptionMap;
  ddlpackage::ColumnDefaultValue *colDefault;
  ddlpackage::DDL_MATCH_TYPE matchType;
  ddlpackage::DDL_REFERENTIAL_ACTION refActionCode;
  ddlpackage::ReferentialAction *refAction;
};

%{

%}

%token ACTION ADD ALTER AUTO_INCREMENT BIGINT BIT IDB_BLOB CASCADE IDB_CHAR CHARACTER CHECK CLOB COLUMN
COLUMNS COMMENT CONSTRAINT CONSTRAINTS CREATE CURRENT_USER DATE DATETIME DEC
DECIMAL DEFAULT DEFERRABLE DEFERRED IDB_DELETE DROP ENGINE
FOREIGN FULL IMMEDIATE INDEX INITIALLY IDB_INT INTEGER KEY MATCH MAX_ROWS
MIN_ROWS MODIFY NO NOT NULL_TOK NUMBER NUMERIC ON PARTIAL PRECISION PRIMARY
REFERENCES RENAME RESTRICT SET SMALLINT TABLE TIME 
TINYINT TO UNIQUE UPDATE USER SESSION_USER SYSTEM_USER VARCHAR VARBINARY
VARYING WITH ZONE DOUBLE IDB_FLOAT REAL CHARSET IF EXISTS CHANGE TRUNCATE

%token <str> IDENT FCONST SCONST CP_SEARCH_CONDITION_TEXT ICONST

/* Notes:
 * 1. "ata" stands for alter_table_action
 * 2. The %type statements are how bison figures out what element of
 * above union to use in generated code.  It's a little weird because
 * you say "type" but what you specify is an member the union.
 */
%type <ata>                  add_table_constraint_def
%type <ata>                  alter_column_def
%type <ata>                  alter_table_action
%type <ataList>              alter_table_actions
%type <sqlStmt>              alter_table_statement
%type <ata>                  ata_add_column
%type <ata>                  ata_rename_table
%type <columnType>           character_string_type
%type <columnType>           binary_string_type
%type <str>                  check_constraint_def
%type <columnConstraintDef>  column_constraint
%type <columnConstraintDef>  column_constraint_def
%type <schemaObject>         column_def
%type <str>                  column_name
%type <columnNameList>       column_name_list
%type <constraintAttributes> constraint_attributes
%type <cattr>                constraint_check_time
%type <str>                  constraint_name
%type <sqlStmt>              create_table_statement
%type <sqlStmt>              create_index_statement
%type <columnType>           data_type
%type <columnType>           datetime_type
%type <colDefault>           default_clause
%type <cattr>                deferrability_clause
%type <refActionCode>        delete_rule
%type <refActionCode>        drop_behavior
%type <ata>                  drop_column_def
%type <ata>                  drop_table_constraint_def
%type <sqlStmt>              drop_index_statement
%type <sqlStmt>              drop_table_statement
%type <refActionCode>        opt_delete_rule
%type <columnType>           exact_numeric_type
%type <str>                  literal
%type <matchType>            opt_match_type
%type <matchType>            match_type
%type <ata>                  modify_column
%type <columnType>           numeric_type
%type <constraintList>       column_qualifier_list
%type <constraintAttributes> opt_constraint_attributes
%type <str>                  opt_constraint_name
%type <cattr>                opt_deferrability_clause
%type <columnType>           opt_precision_scale
%type <refAction>            opt_referential_triggered_action
%type <ival>                 opt_time_precision
%type <flag>                 opt_with_time_zone
%type <qualifiedName>        qualified_name
%type <refActionCode>        referential_action
%type <refAction>            referential_triggered_action
%type <schemaObject>         referential_constraint_def
%type <ata>                  rename_column
%type <sqlStmt>              stmt
%type <sqlStmtList>          stmtblock
%type <sqlStmtList>          stmtmulti
%type <str>                  string_literal
%type <schemaObject>         table_constraint
%type <schemaObject>         table_constraint_def
%type <schemaObject>         table_element
%type <tableElementList>     table_element_list
%type <qualifiedName>        table_name
%type <tableOption>          table_option
%type <columnOption>         column_option
%type <tableOptionMap>       table_options
%type <tableOptionMap>       opt_table_options
%type <schemaObject>         unique_constraint_def
%type <constraintType>       unique_specifier
%type <refActionCode>        update_rule
%type <refActionCode>        opt_update_rule
%type <columnType>           approximate_numeric_type
%type <str>                  opt_display_width
%type <str> 		     	 opt_display_precision_scale_null
%type <str>                  opt_if_exists
%type <str>                  opt_if_not_exists
%type <sqlStmt>              trunc_table_statement

%%
stmtblock:	stmtmulti { parseTree = $1; }
		;


stmtmulti:
	stmtmulti ';' stmt
	{
		if ($3 != NULL) {
			$1->push_back($3);
			$$ = $1;
		}
		else {
			$$ = $1;
		}
	}
	| stmt
	{ 
	/* The user is supposed to supply a ParseTree* via grammar_init.
	So, it is already there. */
		if ($1 != NULL)
		{
			$$ = parseTree;
			$$->push_back($1);
		}
		else
		{
			$$ = NULL;
		}
	}


stmt:
	alter_table_statement
	| create_table_statement
	| drop_index_statement
	| drop_table_statement
	| create_index_statement
	| trunc_table_statement
	| { $$ = NULL; }
	;

drop_table_statement:
	DROP TABLE opt_if_exists qualified_name {$$ = new DropTableStatement($4, false);}
	| DROP TABLE opt_if_exists qualified_name CASCADE CONSTRAINTS
	{
		{$$ = new DropTableStatement($4, true);}
	}
	;

opt_if_exists:
	IF EXISTS {$$ = NULL;}
	| {$$ = NULL;}
	;

drop_index_statement:
	DROP INDEX qualified_name {$$ = new DropIndexStatement($3);}
	;
	
/* Notice that we allow table_options here (e.g. engine=infinidb) but
   we ignore them. */
create_index_statement:
	CREATE INDEX qualified_name ON qualified_name '(' column_name_list ')' opt_table_options
	{
		$$ = new CreateIndexStatement($3, $5, $7, false);
		delete $9;
	}
	| CREATE UNIQUE INDEX qualified_name ON qualified_name '(' column_name_list ')' opt_table_options
	{
		$$ = new CreateIndexStatement($4, $6, $8, true);
		delete $10;
	}
	;

opt_table_options:
	table_options
	| {$$ = NULL;}
	;

create_table_statement:
	CREATE TABLE opt_if_not_exists table_name '(' table_element_list ')' table_options
	{
		$$ = new CreateTableStatement(new TableDef($4, $6, $8));
	}
    |
	CREATE TABLE opt_if_not_exists table_name '(' table_element_list ')'
	{
		$$ = new CreateTableStatement(new TableDef($4, $6, NULL));
	}
	;

opt_if_not_exists:
	IF NOT EXISTS {$$ = NULL;}
	| {$$ = NULL;}
	;

trunc_table_statement:
	TRUNCATE TABLE qualified_name {$$ = new TruncTableStatement($3);}
	| TRUNCATE qualified_name { {$$ = new TruncTableStatement($2);} }
	;

table_element_list:
	table_element
	{
		$$ = new TableElementList();
		$$->push_back($1);
	}
	|
	table_element_list ',' table_element
	{
		$$ = $1;
		$$->push_back($3);
	}
	;

table_element:
	column_def
	| table_constraint_def
	;

table_constraint_def:
	CONSTRAINT opt_constraint_name table_constraint opt_constraint_attributes
	{
		$$ = $3;
		$3->fName = $2;
	}
	|
	opt_constraint_name table_constraint opt_constraint_attributes
	{
		$$ = $2;
		$2->fName = $1;
	}
	;

opt_constraint_name:
	constraint_name {$$ = $1;}
	| {$$ = "noname";}
	;

table_constraint:
	unique_constraint_def
	| referential_constraint_def
	| check_constraint_def {$$ = new TableCheckConstraintDef($1);}
	;

unique_constraint_def:
	unique_specifier '(' column_name_list ')'
	{
		if ($1 == DDL_UNIQUE)
		    $$ = new TableUniqueConstraintDef($3);
        else if ($1 == DDL_PRIMARY_KEY)
            $$ = new TablePrimaryKeyConstraintDef($3);
	}
	;

column_name_list:
	column_name
	{
		$$ = new vector<string>;
		$$->push_back($1);
	}
	| column_name_list ',' column_name
	{
		$$ = $1;
		$$->push_back($3);
	}
	;

unique_specifier:
	PRIMARY KEY {$$ = DDL_PRIMARY_KEY;}
	| UNIQUE {$$ = DDL_UNIQUE;}
	;

referential_constraint_def:
	FOREIGN KEY '(' column_name_list ')' REFERENCES	table_name '(' column_name_list ')' opt_match_type opt_referential_triggered_action
	{
		$$ = new TableReferencesConstraintDef($4, $7, $9, $11, $12);
	}
	;

opt_match_type:
	match_type
	| {$$ = DDL_FULL;}
	;

match_type:
	MATCH match_type {$$ = $2;}
	;

match_type:
	FULL {$$ = DDL_FULL;}
	| PARTIAL {$$ = DDL_PARTIAL;}
	;

opt_referential_triggered_action:
	referential_triggered_action
	| {$$ = NULL;}
	;

referential_triggered_action:
	update_rule opt_delete_rule
	{
		$$ = new ReferentialAction();
		$$->fOnUpdate = $1;
		$$->fOnDelete = $2;
	}
	| delete_rule opt_update_rule
	{
		$$ = new ReferentialAction();
		$$->fOnUpdate = $2;
		$$->fOnDelete = $1;
	}
	;

opt_delete_rule:
	delete_rule
	| {$$ = DDL_NO_ACTION;}
	;

opt_update_rule:
	update_rule
	| {$$ = DDL_NO_ACTION;}
	;

update_rule:
	ON UPDATE referential_action {$$ = $3;}
	;

delete_rule:
	ON IDB_DELETE referential_action {$$ = $3;}
	;

referential_action:
	CASCADE {$$ = DDL_CASCADE;}
	| SET NULL_TOK {$$ = DDL_SET_NULL;}
	| SET DEFAULT {$$ = DDL_SET_DEFAULT;}
	| NO ACTION {$$ = DDL_NO_ACTION;}
	| RESTRICT {$$ = DDL_RESTRICT;}
	;

table_options:
	table_option
	{
		$$ = new TableOptionMap();
		(*$$)[$1->first] = $1->second;
		delete $1;
	}	
	| table_options table_option
	{
		$$ = $1;
		(*$$)[$2->first] = $2->second;
		delete $2;
	}
	;

table_option:
 	ENGINE '=' IDENT {$$ = new pair<string,string>("engine", $3);}
	|
 	MAX_ROWS '=' ICONST {$$ = new pair<string,string>("max_rows", $3);}
 	|
 	MIN_ROWS '=' ICONST {$$ = new pair<string,string>("min_rows", $3);}
 	|
 	COMMENT '=' string_literal {$$ = new pair<string,string>("comment", $3);}
	|
	COMMENT string_literal {$$ = new pair<string,string>("comment", $2);}
 	|
	AUTO_INCREMENT '=' ICONST
    {
       $$ = new pair<string,string>("auto_increment", $3);
    }
 	|
 	DEFAULT CHARSET '=' IDENT {$$ = new pair<string,string>("default charset", $4);}
 	|
 	DEFAULT IDB_CHAR SET '=' IDENT {$$ = new pair<string,string>("default charset", $5);}
	;

alter_table_statement:
	ALTER TABLE table_name alter_table_actions
	{
		$$ = new AlterTableStatement($3, $4);
	}
	| ALTER TABLE table_name alter_table_actions COMMENT string_literal
	{
		$$ = new AlterTableStatement($3, $4);
	}
	;

alter_table_actions:
	alter_table_action
	{
		if ($1 != NULL) {
			$$ = new AlterTableActionList();
			$$->push_back($1);
		}
		else {
			/* An alter_table_statement requires at least one action.
			   So, this shouldn't happen. */
			$$ = NULL;
		}		
	}
	| alter_table_actions ',' alter_table_action
	{
		$$ = $1;
		$$->push_back($3);
	}
	| alter_table_actions alter_table_action
	{
		$$ = $1;
		$$->push_back($2);
	}
	;

alter_table_action:
	ata_add_column
	| drop_column_def
	| alter_column_def
	| add_table_constraint_def
	| drop_table_constraint_def
	| ata_rename_table
	| modify_column
	| rename_column
	;


modify_column:
	MODIFY column_name data_type
	{$$ = new AtaModifyColumnType($2,$3);}
	|
	MODIFY COLUMN column_name data_type
	{$$ = new AtaModifyColumnType($3,$4);}
	;


rename_column:
	CHANGE column_name column_name data_type
	{$$ = new AtaRenameColumn($2, $3, $4, NULL);}
	|CHANGE column_name column_name data_type column_option
	{$$ = new AtaRenameColumn($2, $3, $4, $5);}
	|CHANGE COLUMN column_name column_name data_type
	{$$ = new AtaRenameColumn($3, $4, $5, NULL);}
	|CHANGE COLUMN column_name column_name data_type column_option
	{$$ = new AtaRenameColumn($3, $4, $5, $6);}
	;

drop_table_constraint_def:
	DROP CONSTRAINT constraint_name drop_behavior
	{
		$$ = new AtaDropTableConstraint($3, $4);
	}
	;

add_table_constraint_def:
	ADD table_constraint_def {$$ = new AtaAddTableConstraint(dynamic_cast<TableConstraintDef *>($2));}
	;

ata_rename_table:
	RENAME table_name {$$ = new AtaRenameTable($2);}
	| RENAME TO table_name {$$ = new AtaRenameTable($3);}
	;

table_name:
	qualified_name
	;

qualified_name:
	IDENT '.' IDENT {$$ = new QualifiedName($1, $3);}
	| IDENT {
				if (db_schema.size())
					$$ = new QualifiedName((char*)db_schema.c_str(), $1);
				else
				    $$ = new QualifiedName($1);   
			}
	;

ata_add_column:
    /* See the documentation for SchemaObject for an explanation of why we are using
     * dynamic_cast here.
     */
	ADD column_def {$$ = new AtaAddColumn(dynamic_cast<ColumnDef*>($2));}
	| ADD COLUMN column_def {$$ = new AtaAddColumn(dynamic_cast<ColumnDef*>($3));}
	| ADD '(' table_element_list ')' {$$ = new AtaAddColumns($3);}
	| ADD COLUMN '(' table_element_list ')' {$$ = new AtaAddColumns($4);}
	;

column_name:
	IDENT
	;

constraint_name:
	IDENT
	;

column_option:
	COMMENT  string_literal {$$ = $2;}
	
column_def:	
	column_name data_type opt_null_tok
	{
		$$ = new ColumnDef($1, $2, NULL, NULL );
	}
	| column_name data_type opt_null_tok column_qualifier_list
	{
		$$ = new ColumnDef($1, $2, $4, NULL);
	}
	| column_name data_type opt_null_tok default_clause column_qualifier_list
	{
		$$ = new ColumnDef($1, $2, $5, $4);
	}
	| column_name data_type opt_null_tok default_clause
	{
		$$ = new ColumnDef($1, $2, NULL, $4, NULL);
	}
	| column_name data_type opt_null_tok column_option
	{
		$$ = new ColumnDef($1, $2, NULL, NULL, $4 );
	}
	| column_name data_type opt_null_tok column_qualifier_list column_option
	{
		$$ = new ColumnDef($1, $2, $4, NULL, $5);
	}
	| column_name data_type opt_null_tok default_clause column_qualifier_list column_option
	{
		$$ = new ColumnDef($1, $2, $5, $4, $6);
	}
	| column_name data_type opt_null_tok default_clause column_option
	{
		$$ = new ColumnDef($1, $2, NULL, $4, $5);
	}
	;

opt_null_tok:
	/* empty */
	|
	NULL_TOK
	;

default_clause:
	DEFAULT literal
	{
		$$ = new ColumnDefaultValue($2);
	}
	| DEFAULT NULL_TOK {$$ = new ColumnDefaultValue(NULL);}
	| DEFAULT USER {$$ = new ColumnDefaultValue("$USER");}
	| DEFAULT CURRENT_USER {$$ = new ColumnDefaultValue("$CURRENT_USER");}
	| DEFAULT SESSION_USER {$$ = new ColumnDefaultValue("$SESSION_USER");}
	| DEFAULT SYSTEM_USER {$$ = new ColumnDefaultValue("$SYSTEM_USER");}
	;

data_type:
	character_string_type
	| binary_string_type
	| numeric_type
	| datetime_type
	| IDB_BLOB
	{
		$$ = new ColumnType(DDL_BLOB);
		$$->fLength = DDLDatatypeLength[DDL_BLOB];
	}
	| CLOB
	{
		$$ = new ColumnType(DDL_CLOB);
		$$->fLength = DDLDatatypeLength[DDL_CLOB];
	}

	;

column_qualifier_list:
	column_constraint_def
	{
		$$ = new ColumnConstraintList();
		$$->push_back($1);
	}	
	| column_qualifier_list column_constraint_def
	{
		$$ = $1;
		$$->push_back($2);
	}
	;

column_constraint_def:
	column_constraint opt_constraint_attributes
	{
		$$ = $1;

		if($2 != NULL)
		{
			$1->fDeferrable = $2->fDeferrable;
			$1->fCheckTime = $2->fCheckTime;
		}

	}
	| CONSTRAINT constraint_name column_constraint opt_constraint_attributes
	{
		$$ = $3;
		$3->fName = $2;

		if($4 != NULL)
		{
			$3->fDeferrable = $4->fDeferrable;
			$3->fCheckTime = $4->fCheckTime;
		}
		
	}
	;

opt_constraint_attributes:
	constraint_attributes {$$ = $1;}
	| {$$ = NULL;}
	;

constraint_attributes:
	constraint_check_time opt_deferrability_clause
	{
		$$ = new ConstraintAttributes($1, ($2 != 0));
	}
	|
	DEFERRABLE constraint_check_time
	{
		$$ = new ConstraintAttributes($2, true);
	}
/*
	|
	NOT DEFERRABLE constraint_check_time
	{
		$$ = new ConstraintAttributes($3, false);
	}
*/
	;

opt_deferrability_clause:
	deferrability_clause
	| {$$ = DDL_NON_DEFERRABLE;}
	;

deferrability_clause:
	DEFERRABLE {$$ = DDL_DEFERRABLE;}

/* The rule below forces a shift/shift conflict.  This will require
scanner hacking or something. Since constraints are non-deferrable by
default, I'm putting this off. You can still express all the cases,
you just can't say NOT DEFERRABLE. */

/*	| NOT DEFERRABLE {$$ = DDL_DEFERRABLE;} */
	;

constraint_check_time:
	INITIALLY DEFERRED {$$ = DDL_INITIALLY_DEFERRED;}
	| INITIALLY IMMEDIATE {$$ = DDL_INITIALLY_IMMEDIATE;}
	;

column_constraint:
	NOT NULL_TOK {$$ = new ColumnConstraintDef(DDL_NOT_NULL);}
	| UNIQUE {$$ = new ColumnConstraintDef(DDL_UNIQUE);}
	| PRIMARY KEY {$$ = new ColumnConstraintDef(DDL_PRIMARY_KEY);}
	| AUTO_INCREMENT {$$ = new ColumnConstraintDef(DDL_AUTO_INCREMENT);}
	| check_constraint_def {$$ = new ColumnConstraintDef($1);}
	;

check_constraint_def:
	CHECK '(' CP_SEARCH_CONDITION_TEXT ')' {$$ = $3;}
	;

string_literal:
	'\'' SCONST '\'' {$$ = $2;}
	; 

character_string_type:
	CHARACTER
	{
		$$ = new ColumnType(DDL_CHAR);
		$$->fLength = 1;
	}
	| IDB_CHAR
	{
		$$ = new ColumnType(DDL_CHAR);
		$$->fLength = 1;
	}
	| CHARACTER '(' ICONST ')'
	{
		$$ = new ColumnType(DDL_CHAR);
		$$->fLength = atoi($3);
	}
	| IDB_CHAR '(' ICONST ')'
	{
		$$ = new ColumnType(DDL_CHAR);
		$$->fLength = atoi($3);
	}
	| CHARACTER VARYING '(' ICONST ')'
	{
		$$ = new ColumnType(DDL_VARCHAR);
		$$->fLength = atoi($4);
	}

	| IDB_CHAR VARYING '(' ICONST ')'
	{
		$$ = new ColumnType(DDL_VARCHAR);
		$$->fLength = atoi($4);
	}

	| VARCHAR '(' ICONST ')'
	{
		$$ = new ColumnType(DDL_VARCHAR);
		$$->fLength = atoi($3);
	}
	;

binary_string_type:
	VARBINARY '(' ICONST ')'
	{
		$$ = new ColumnType(DDL_VARBINARY);
		$$->fLength = atoi($3);
	}
	;

numeric_type:
	exact_numeric_type
	| approximate_numeric_type
	;

exact_numeric_type:
	NUMERIC opt_precision_scale
	{
		$2->fType = DDL_NUMERIC;
		$2->fLength = DDLDatatypeLength[DDL_NUMERIC];
		$$ = $2;
	}
	| DECIMAL opt_precision_scale
	{
		$2->fType = DDL_DECIMAL;
/*	   	$2->fLength = DDLDatatypeLength[DDL_DECIMAL]; */
		$$ = $2;
	}
	| NUMBER opt_precision_scale
	{
		$2->fType = DDL_DECIMAL;
		$2->fLength = DDLDatatypeLength[DDL_DECIMAL];
		$$ = $2;
	}
	| INTEGER opt_display_width
	{
		$$ = new ColumnType(DDL_INTEGER);
		$$->fLength = DDLDatatypeLength[DDL_INT];
	}
	| IDB_INT opt_display_width
	{
		$$ = new ColumnType(DDL_INTEGER);
		$$->fLength = DDLDatatypeLength[DDL_INT];
	}
	| SMALLINT opt_display_width
	{
		$$ = new ColumnType(DDL_SMALLINT);
		$$->fLength = DDLDatatypeLength[DDL_SMALLINT];
	}
	| TINYINT opt_display_width
	{
		$$ = new ColumnType(DDL_TINYINT);
		$$->fLength = DDLDatatypeLength[DDL_TINYINT];
	}
	| BIGINT opt_display_width
	{
		$$ = new ColumnType(DDL_BIGINT);
		$$->fLength = DDLDatatypeLength[DDL_BIGINT];
	}
	;
/* Bug 1570, change default scale to 0 from -1 */
opt_precision_scale:
	'(' ICONST ')' {$$ = new ColumnType(atoi($2), 0);}
	| '(' ICONST ',' ICONST ')' {$$ = new ColumnType(atoi($2), atoi($4));}
	| {$$ = new ColumnType(10,0);}
	;

opt_display_width:
	'(' ICONST ')' {$$ = NULL;}
	| {$$ = NULL;}
	;

approximate_numeric_type:
	DOUBLE opt_display_precision_scale_null
	{
		$$ = new ColumnType(DDL_DOUBLE);
		$$->fLength = DDLDatatypeLength[DDL_DOUBLE];
	}
	| REAL opt_display_precision_scale_null
	{
		$$ = new ColumnType(DDL_DOUBLE);
		$$->fLength = DDLDatatypeLength[DDL_DOUBLE];
	}
	| IDB_FLOAT opt_display_precision_scale_null
	{
		$$ = new ColumnType(DDL_FLOAT);
		$$->fLength = DDLDatatypeLength[DDL_FLOAT];
	}
	;

opt_display_precision_scale_null:
		'(' ICONST ')' {$$ = NULL;}
		|
        '(' ICONST ',' ICONST ')' {$$ = NULL;}
        | {$$ = NULL;}
        ;
		
literal:
	ICONST
	| string_literal
	| FCONST
	;

datetime_type:
	DATETIME
	{
		$$ = new ColumnType(DDL_DATETIME);
		$$->fLength = DDLDatatypeLength[DDL_DATETIME];
	}
	|
	DATE
	{
		$$ = new ColumnType(DDL_DATE);
		$$->fLength = DDLDatatypeLength[DDL_DATE];
	}
	|
	TIME opt_time_precision opt_with_time_zone
	{
		$$ = new ColumnType(DDL_DATETIME);
		$$->fLength = DDLDatatypeLength[DDL_DATETIME];
		$$->fPrecision = $2;
		$$->fWithTimezone = $3;
	}

opt_time_precision:
	'(' ICONST ')' {$$ = atoi($2);}
	| {$$ = -1;}
	;

opt_with_time_zone:
	WITH TIME ZONE {$$ = true;}
	| {$$ = false;}
	;

drop_column_def:
	DROP column_name drop_behavior {$$ = new AtaDropColumn($2, $3);}
	| DROP COLUMN column_name drop_behavior {$$ = new AtaDropColumn($3, $4);}
	| DROP COLUMN '(' column_name_list ')' {$$ = new AtaDropColumns($4);}
	| DROP '(' column_name_list ')' {$$ = new AtaDropColumns($3);}
	| DROP COLUMNS '(' column_name_list ')' {$$ = new AtaDropColumns($4);}
	;

drop_behavior:
	CASCADE {$$ = DDL_CASCADE;}
	| RESTRICT {$$ = DDL_RESTRICT;}
	| {$$ = DDL_NO_ACTION;}
	;

alter_column_def:
	ALTER opt_column column_name SET default_clause {$$ = new AtaSetColumnDefault($3, $5);}
	| 	ALTER opt_column column_name DROP DEFAULT {$$ = new AtaDropColumnDefault($3);}
	;

opt_column:
	COLUMN
	|
	;

%%

void grammar_init(ParseTree *_parseTree, bool debug)
{
	parseTree = _parseTree;
	
	if(debug)
		yydebug = 1;
}

void set_schema(std::string schema)
{
	db_schema = schema;
}
