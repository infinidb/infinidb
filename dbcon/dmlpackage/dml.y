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

/* $Id: dml.y 7407 2011-02-08 14:05:21Z rdempsey $ */
/* This describes a substantial subset of SQL92 DML with some
   enhancements from various vendors.  

   Most of the rule names in the grammar are taken directly from SQL92
   where that seemed to make sense.  Where spaces occur in the
   standard, we use '_' instead.

   If you want to know something about a rule like:

   foo_bar_specification:
      foo
      | bar
      ;

   Pull up the standard in Acroread and search for:
	 "<foo bar specification> ::"

   If you have to care about this grammar, you should consult one or
   more of the following sources:

      ANSI SQL98 standard
      Understanding the New Sql book (Melton and Simon)
      info files for bison and flex
      The postgress and mysql sources.  find x -name \*.y -o -name \*.yy.
      Lex and Yacc book.

   We don't support delimited identifiers.

   All literals are stored as unconverted strings.

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
#include <string.h>
#include "dmlparser.h"

#undef DECIMAL
#undef DELETE
#undef IN
#include "dml-gram.h"

using namespace std;
using namespace dmlpackage;	

int dmllex();

void dmlerror (char const *error);

namespace dmlpackage {

/* The user is expect to pass a ParseTree* to grammar_init */
static ParseTree* parseTree;
typedef std::vector<char*> copybuf_t;
static copybuf_t copy_buffer;
static std::string default_schema;
char* copy_string(const char *str);

}

%}
%debug
	/* symbolic tokens */

%union {
	int intval;
	double floatval;
	char *strval;
	int subtok;
	dmlpackage::SqlStatementList *sqlStmtList;
	dmlpackage::SqlStatement *sqlStmt;
	dmlpackage::TableName* tblName;
	dmlpackage::ColumnNameList* colNameList;
	dmlpackage::ValuesOrQuery* valsOrQuery;
	dmlpackage::ValuesList* valsList;
	dmlpackage::QuerySpec* querySpec;
	dmlpackage::TableNameList* tableNameList;
	dmlpackage::TableExpression* tableExpression;
	dmlpackage::WhereClause* whereClause;
	dmlpackage::SearchCondition* searchCondition;
	dmlpackage::ExistanceTestPredicate* existPredicate;
	dmlpackage::AllOrAnyPredicate* allOrAnyPredicate;
	dmlpackage::InPredicate* inPredicate;
	dmlpackage::NullTestPredicate* nullTestPredicate;
	dmlpackage::LikePredicate* likePredicate;
	dmlpackage::BetweenPredicate* betweenPredicate;
	dmlpackage::ComparisonPredicate* comparisonPredicate;
	dmlpackage::Predicate* predicate;
	dmlpackage::FromClause* fromClause;
	dmlpackage::SelectFilter* selectFilter;
	dmlpackage::GroupByClause* groupByClause;
	dmlpackage::HavingClause* havingClause;
	dmlpackage::Escape* escape;
	dmlpackage::AtomList* atomList;
	dmlpackage::ColumnAssignment* colAssignment;
	dmlpackage::ColumnAssignmentList* colAssignmentList;
}

%type <sqlStmt>         sql
%type <sqlStmtList>     sql_list
%type <sqlStmt>	        delete_statement_positioned
%type <sqlStmt>		    delete_statement_searched
%type <sqlStmt>		    insert_statement
%type <sqlStmt>		    update_statement_positioned
%type <sqlStmt>		    update_statement_searched 	
%type <sqlStmt>         commit_statement
%type <sqlStmt>         rollback_statement
%type <tblName>		    table_name
%type <tblName>         table 
%type <tblName>		    table_ref
%type <colNameList>	    column_commalist
%type <colNameList>	    opt_column_commalist
%type <colNameList>     column_ref_commalist
%type <strval>		    column
%type <valsOrQuery>	    values_or_query_spec
%type <valsList>	    insert_atom_commalist
%type <strval>		    insert_atom
%type <strval>		    atom
%type <querySpec>	    query_spec
%type <querySpec>       subquery
%type <strval>		    opt_all_distinct
%type <selectFilter>    selection
%type <colNameList>	    scalar_exp_commalist
%type <strval>		    scalar_exp
%type <strval>          function_ref
%type <tableExpression>	table_exp
%type <fromClause>	    from_clause
%type <tableNameList>   table_ref_commalist
%type <whereClause>     opt_where_clause
%type <whereClause>     where_clause   
%type <searchCondition> search_condition
%type <predicate>	    predicate
%type <comparisonPredicate> comparison_predicate
%type <betweenPredicate>    between_predicate
%type <likePredicate>       like_predicate
%type <nullTestPredicate>   test_for_null
%type <inPredicate>         in_predicate
%type <allOrAnyPredicate>   all_or_any_predicate
%type <existPredicate>      existence_test
%type <escape>		        opt_escape
%type <strval>              column_ref
%type <atomList>            atom_commalist;
%type <groupByClause>       opt_group_by_clause
%type <havingClause>        opt_having_clause
%type <strval> 		        parameter_ref
%type <strval>              literal
%type <strval>		        parameter
%type <strval>              any_all_some
%type <colAssignment>       assignment
%type <colAssignmentList>   assignment_commalist
%type <sqlStmt>         manipulative_statement
%type <sqlStmt>         close_statement
%type <sqlStmt>         fetch_statement
%type <sqlStmt>         open_statement
%type <sqlStmt>         select_statement
%{

%}
	
%token <strval> NAME
%token <strval> STRING
%token <strval> INTNUM 
%token <strval> APPROXNUM
%token <strval>	SELECT
%token <strval> ALL
%token <strval> DISTINCT
%token <strval> NULLX
%token <strval> USER
%token <strval> INDICATOR
%token <strval> AMMSC
%token <strval> PARAMETER
%token <strval> ANY
%token <strval> SOME

   /* operators */

%left OR
%left AND
%left NOT
%left <strval> COMPARISON /* = <> < > <= >= */
%left '+' '-'
%left '*' '/'
%nonassoc UMINUS

	/* literal keyword tokens */

%token AS ASC AUTHORIZATION BETWEEN BY
%token CHARACTER CHECK CLOSE COMMIT CONTINUE CREATE CURRENT
%token CURSOR IDB_DECIMAL DECLARE DEFAULT DELETE DESC IDB_DOUBLE
%token ESCAPE EXISTS FETCH IDB_FLOAT FOR FOREIGN FOUND FROM GOTO
%token GRANT IDB_GROUP HAVING IN INSERT INTEGER INTO
%token IS KEY LANGUAGE LIKE NUMERIC OF ON OPEN OPTION
%token ORDER PRECISION PRIMARY PRIVILEGES PROCEDURE
%token PUBLIC REAL REFERENCES ROLLBACK SCHEMA SET
%token SMALLINT SQLCODE SQLERROR TABLE TO UNION
%token UNIQUE UPDATE VALUES VIEW WHENEVER WHERE WITH WORK

%%
sql_list:
		sql ';'	{
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
		| sql_list sql ';' {
 							  
							    if ($1 != NULL)
								{
									parseTree = $1;
								}
								
							}
        ;

	/* schema definition language */
sql:		schema { $$ = NULL; }
	;
	
schema:
		CREATE SCHEMA AUTHORIZATION user opt_schema_element_list
	;

opt_schema_element_list:
		/* empty */
	|	schema_element_list
	;

schema_element_list:
		schema_element
	|	schema_element_list schema_element
	;

schema_element:
		base_table_def
	|	view_def
	|	privilege_def
	;

base_table_def:
		CREATE TABLE table '(' base_table_element_commalist ')'
	;

base_table_element_commalist:
		base_table_element
	|	base_table_element_commalist ',' base_table_element
	;

base_table_element:
		column_def
	|	table_constraint_def
	;

column_def:
		column data_type column_def_opt_list
	;

column_def_opt_list:
		/* empty */
	|	column_def_opt_list column_def_opt
	;

column_def_opt:
		NOT NULLX
	|	NOT NULLX UNIQUE
	|	NOT NULLX PRIMARY KEY
	|	DEFAULT literal
	|	DEFAULT NULLX
	|	DEFAULT USER
	|	CHECK '(' search_condition ')'
	|	REFERENCES table
	|	REFERENCES table '(' column_commalist ')'
	;

table_constraint_def:
		UNIQUE '(' column_commalist ')'
	|	PRIMARY KEY '(' column_commalist ')'
	|	FOREIGN KEY '(' column_commalist ')'
			REFERENCES table 
	|	FOREIGN KEY '(' column_commalist ')'
			REFERENCES table '(' column_commalist ')'
	|	CHECK '(' search_condition ')'
	;

column_commalist:
		column
		{
		    $$ = new ColumnNameList;
		    $$->push_back($1);
		}
	|	column_commalist ',' column
		{
			$$ = $1;
			$$->push_back($3);
		}
	;

view_def:
		CREATE VIEW table opt_column_commalist
		AS query_spec opt_with_check_option
	;
	
opt_with_check_option:
		/* empty */
	|	WITH CHECK OPTION
	;

opt_column_commalist:
		/* empty */ { $$ = NULL; }
	|	'(' column_commalist ')' {$$ = $2;}
	;

privilege_def:
		GRANT privileges ON table TO grantee_commalist
		opt_with_grant_option
	;

opt_with_grant_option:
		/* empty */
	|	WITH GRANT OPTION
	;

privileges:
		ALL PRIVILEGES
	|	ALL
	|	operation_commalist
	;

operation_commalist:
		operation
	|	operation_commalist ',' operation
	;

operation:
		SELECT
	|	INSERT
	|	DELETE
	|	UPDATE opt_column_commalist
	|	REFERENCES opt_column_commalist
	;


grantee_commalist:
		grantee
	|	grantee_commalist ',' grantee
	;

grantee:
		PUBLIC
	|	user
	;

	/* cursor definition */
sql:
		cursor_def { $$ = NULL;  }
	;


cursor_def:
		DECLARE cursor CURSOR FOR query_exp opt_order_by_clause
	;

opt_order_by_clause:
		/* empty */
	|	ORDER BY ordering_spec_commalist
	;

ordering_spec_commalist:
		ordering_spec
	|	ordering_spec_commalist ',' ordering_spec
	;

ordering_spec:
		INTNUM opt_asc_desc
	|	column_ref opt_asc_desc
	;

opt_asc_desc:
		/* empty */
	|	ASC
	|	DESC
	;

	/* manipulative statements */

sql:	manipulative_statement 
	;

manipulative_statement:
		close_statement
	|	commit_statement 
	|	delete_statement_positioned 
	|	delete_statement_searched   
	|	fetch_statement
	|	insert_statement 
	|	open_statement
	|	rollback_statement
	|	select_statement
	|	update_statement_positioned 
	|	update_statement_searched   
	;

close_statement:
		CLOSE cursor { }
	;

commit_statement:
		COMMIT WORK
		{
			$$ = new CommandSqlStatement("COMMIT");
		}
    |   COMMIT
		{
			$$ = new CommandSqlStatement("COMMIT");
		}
		
	;

delete_statement_positioned:
		DELETE FROM table WHERE CURRENT OF cursor
		{
			$$ = new DeleteSqlStatement($3);
		       
		}
	;

delete_statement_searched:
		DELETE FROM table opt_where_clause
		{
			$$ = new DeleteSqlStatement($3,$4);	
		}
	;

fetch_statement:
		FETCH cursor INTO target_commalist { }
	;

insert_statement:
		INSERT INTO table_name opt_column_commalist values_or_query_spec
		{
			if (NULL == $4)
				$$ = new InsertSqlStatement($3, $5);
			else
				$$ = new InsertSqlStatement($3, $4, $5);
		}
	;

values_or_query_spec:
		VALUES '(' insert_atom_commalist ')'
		{
			$$ = new ValuesOrQuery($3);
		}
	|	query_spec
		{
			$$ = new ValuesOrQuery($1);
		}
	;

insert_atom_commalist:
		insert_atom
		{
			$$ = new ValuesList;
			$$->push_back($1);
		}
	|	insert_atom_commalist ',' insert_atom
		{
			$$ = $1;
			$$->push_back($3);
		}
	;

insert_atom:
		atom   
	|	NULLX  
	;

open_statement:
		OPEN cursor { }
	;

rollback_statement:
		ROLLBACK WORK
		{
			$$ = new CommandSqlStatement("ROLLBACK");
		}
	|   ROLLBACK
		{
			$$ = new CommandSqlStatement("ROLLBACK");
		} 
	;

select_statement:
		SELECT opt_all_distinct selection
		INTO target_commalist
		table_exp { }
	;

opt_all_distinct:
		/* empty */ { $$ = NULL; }
	|	ALL         { $$ = $1; }  
	|	DISTINCT    { $$ = $1; } 
	;

update_statement_positioned:
		UPDATE table SET assignment_commalist
		WHERE CURRENT OF cursor
		{
	            $$ = new UpdateSqlStatement($2,$4);
		}
	;

assignment_commalist:
		assignment
		{
		   $$ = new ColumnAssignmentList();
		   $$->push_back($1);
		}
	|	assignment_commalist ',' assignment
		{
		   $$ = $1;
		   $$->push_back($3);
		}
	;

assignment:
		column COMPARISON scalar_exp
		{
		   $$ = new ColumnAssignment();
		   $$->fColumn = $1; 
		   $$->fOperator = $2;
		   $$->fScalarExpression = $3;
		}
	|	column COMPARISON NULLX
		{
		   $$ = new ColumnAssignment();
		   $$->fColumn = $1;
		   $$->fOperator = $2;
		   $$->fScalarExpression = $3;
		}
	;

update_statement_searched:
		UPDATE table SET assignment_commalist opt_where_clause
		{
		   $$ = new UpdateSqlStatement($2, $4, $5);
		}
	;

target_commalist:
		target
	|	target_commalist ',' target
	;

target:
		parameter_ref
	;

opt_where_clause:
		/* empty */ { $$ = NULL; } 
	|	where_clause { $$ = $1; }
	;

	/* query expressions */

query_exp:
		query_term
	|	query_exp UNION query_term
	|	query_exp UNION ALL query_term
	;

query_term:
		query_spec
	|	'(' query_exp ')'
	;

query_spec:
	 SELECT opt_all_distinct selection table_exp
	 {
                $$ = new QuerySpec();
		if (NULL != $2)
		   $$->fOptionAllOrDistinct = $2; 
		$$->fSelectFilterPtr = $3;
		$$->fTableExpressionPtr = $4;
                  		
	 }
	;

selection:
		scalar_exp_commalist { $$ = new SelectFilter($1); } 
	|	'*' { $$ = new SelectFilter(); }
	;

table_exp:
		from_clause
		opt_where_clause
		opt_group_by_clause
		opt_having_clause
		{
		   $$ = new TableExpression();
		   $$->fFromClausePtr = $1;
		   $$->fWhereClausePtr = $2;
		   $$->fGroupByPtr = $3;
		   $$->fHavingPtr  = $4;
		}
	;

from_clause:
		FROM table_ref_commalist
		{
		   $$ = new FromClause();
		   $$->fTableListPtr = $2;
		}
	;

table_ref_commalist:
		table_ref
		{
	           $$ = new TableNameList();
		   $$->push_back($1);
		}
	|	table_ref_commalist ',' table_ref
		{
		   $$ = $1;
		   $$->push_back($3);
		}
	;

table_ref:
		table 
	|	table range_variable
	;

where_clause:
		WHERE search_condition
		{
		   $$ = new WhereClause();
		   $$->fSearchConditionPtr = $2;
		}
	;

opt_group_by_clause:
		/* empty */ { $$ = NULL; }
	|	IDB_GROUP BY column_ref_commalist
		{
		    $$ = new GroupByClause();
		    $$->fColumnNamesListPtr = $3;	
		}
	;

column_ref_commalist:
		column_ref
		{
		    $$ = new ColumnNameList();
		    $$->push_back($1);	
		}
	|	column_ref_commalist ',' column_ref
		{
		    $$ = $1;
		    $$->push_back($3);	
		}
	;

opt_having_clause:
		/* empty */ { $$ = NULL; }
	|	HAVING search_condition
		{
		   $$ = new HavingClause();
		   $$->fSearchConditionPtr =  $2;		 
		}
	;

	/* search conditions */

search_condition:
		search_condition OR search_condition
		{
		   $$ = new SearchCondition;
		   $$->fLHSearchConditionPtr = $1;
		   $$->fOperator = "OR";
		   $$->fRHSearchConditionPtr = $3;
		}
	|	search_condition AND search_condition
		{
		   $$ = new SearchCondition;
		   $$->fLHSearchConditionPtr = $1;
		   $$->fOperator = "AND";
		   $$->fRHSearchConditionPtr = $3;
		}
	|	NOT search_condition
		{
		   $$ = new SearchCondition;
		   $$->fOperator = "NOT";	
		   $$->fRHSearchConditionPtr = $2;
		}	
	|	'(' search_condition ')'
		{
		   $$ = new SearchCondition;
	      	}
	|	predicate
		{
                  
		   $$ = new SearchCondition;
		   $$->fPredicatePtr = $1;		    
		}
         ;

predicate:
		comparison_predicate
		{
		   $$ = $1;
		}
	|	between_predicate
		{
		   $$ = $1;	
		}
	|	like_predicate
		{
		   $$ = $1;
		}
	|	test_for_null
		{
		  $$ = $1;
		}
	|	in_predicate
		{
		  $$ = $1;	
		}
	|	all_or_any_predicate
		{
		   $$ = $1;
		}
	|	existence_test
		{
		   $$ = $1;
		}
	;

comparison_predicate:
		scalar_exp COMPARISON scalar_exp
		{
		   $$ = new ComparisonPredicate();
		   $$->fLHScalarExpression = $1;
		   $$->fOperator = $2;
	       $$->fRHScalarExpression = $3;
		}
	|	scalar_exp COMPARISON subquery
		{
		   $$ = new ComparisonPredicate();
		   $$->fLHScalarExpression = $1;
		   $$->fOperator = $2;
		   $$->fSubQuerySpec = $3;
		}
	;

between_predicate:
		scalar_exp NOT BETWEEN scalar_exp AND scalar_exp
		{
		   $$ = new BetweenPredicate();
		   $$->fLHScalarExpression = $1;
		   $$->fOperator1 = "NOT BETWEEN";
		   $$->fRH1ScalarExpression = $4;
		   $$->fOperator2 = "AND";
		   $$->fRH2ScalarExpression = $6;
		}
	|	scalar_exp BETWEEN scalar_exp AND scalar_exp
		{
		   $$ = new BetweenPredicate();
		   $$->fLHScalarExpression = $1;
		   $$->fOperator1 = "BETWEEN";
		   $$->fRH1ScalarExpression = $3;
           $$->fOperator2 = "AND";
           $$->fRH2ScalarExpression = $5;
		}
	;

like_predicate:
		scalar_exp NOT LIKE atom opt_escape
		{
		   $$ = new LikePredicate();
		   $$->fLHScalarExpression = $1;
		   $$->fOperator = "NOT LIKE";
		   $$->fAtom = $4;
		   $$->fOptionalEscapePtr = $5;	
		}
	|	scalar_exp LIKE atom opt_escape
		{
		   $$ = new LikePredicate();
		   $$->fLHScalarExpression = $1;
		   $$->fOperator = "LIKE";
		   $$->fAtom = $3;
		   $$->fOptionalEscapePtr  = $4; 	
		}
	;

opt_escape:
	/* empty */ { $$ = NULL; }
	|	ESCAPE atom
		{
		   $$ = new Escape();
		   $$->fEscapeChar = $2;
		}
	;

test_for_null:
		column_ref IS NOT NULLX
		{
		   $$ = new NullTestPredicate();
		   $$->fOperator = "IS NOT NULL";
		   $$->fColumnRef = $1;	 
		}
	|	column_ref IS NULLX
		{
		   $$ = new NullTestPredicate();
		   $$->fOperator = "IS NULL";
		   $$->fColumnRef = $1;
		}
	;

in_predicate:
		scalar_exp NOT IN '(' subquery ')'
		{
		   $$ = new InPredicate();
		   $$->fScalarExpression = $1;
	       $$->fOperator = "NOT IN";
		   $$->fSubQuerySpecPtr = $5;			
		}
	|	scalar_exp IN '(' subquery ')'
		{
		   $$ = new InPredicate();
		   $$->fScalarExpression = $1;
		   $$->fOperator = "IN";
		   $$->fSubQuerySpecPtr = $4;
		}
	|	scalar_exp NOT IN '(' atom_commalist ')'
		{
		   $$ = new InPredicate();
		   $$->fScalarExpression = $1;
		   $$->fOperator = "NOT IN";
		   $$->fAtomList = *$5;	
		   delete $5;
		}
	|	scalar_exp IN '(' atom_commalist ')'
		{
		   $$ = new InPredicate();
		   $$->fScalarExpression = $1;
		   $$->fOperator = "IN";
		   $$->fAtomList = *$4;
		   delete $4;
		}
	;

atom_commalist:
		atom
		{
		    $$ = new AtomList();
	        $$->push_back($1);		 	
		}
	|	atom_commalist ',' atom
		{
		    $$ = $1;
		    $$->push_back($3); 	
		}
	;

all_or_any_predicate:
		scalar_exp COMPARISON any_all_some subquery
		{
		   $$ = new AllOrAnyPredicate();
		   $$->fScalarExpression = $1;
		   $$->fOperator = $2;
		   $$->fAnyAllSome = $3;
		   $$->fSubQuerySpecPtr = $4;
			
		}
	;
			
any_all_some:
		ANY
	|	ALL
	|	SOME
	;

existence_test:
		EXISTS subquery
		{
		   $$ = new ExistanceTestPredicate();
		   $$->fSubQuerySpecPtr = $2;
		}
	;

subquery:
		'(' SELECT opt_all_distinct selection table_exp ')'
		 {
		      $$ = new QuerySpec();
		      if (NULL != $3)
			      $$->fOptionAllOrDistinct = $3;
		      $$->fSelectFilterPtr = $4;
		      $$->fTableExpressionPtr = $5;
		 }
		
	;

	/* scalar expressions */

scalar_exp:
		scalar_exp '+' scalar_exp
		{
	       std::string str = $1;	
		   str +=  " + ";
		   str +=  $3;
		   $$ = copy_string(str.c_str());
		}
	|	scalar_exp '-' scalar_exp
		{
		   std::string str =  $1;
		   str +=  " - ";
 	       str +=  $3;
		   $$ = copy_string(str.c_str()); 	
		}
	|	scalar_exp '*' scalar_exp
		{
		   std::string str = $1;
	       str += " * ";
		   str += $3;
		   $$ = copy_string(str.c_str());	
		}
	|	scalar_exp '/' scalar_exp
		{
		   std::string str = $1;
		   str += " / ";
		   str += $3;
		   $$ = copy_string(str.c_str()); 	
		}
	|	'+' scalar_exp %prec UMINUS 
		{ 
		   std::string str = "+ ";
		   str += $2;
 	       $$ = copy_string(str.c_str());
	    }
	|	'-' scalar_exp %prec UMINUS
		{
		   std::string str = "- ";
		   str += $2;
		   $$ = copy_string(str.c_str()); 		
		}
	|	atom		
	|	column_ref
	|	function_ref
	|	'(' scalar_exp ')' { $$ = $2; }
	;

scalar_exp_commalist:
		scalar_exp
		{
		    $$ = new ColumnNameList;
		    $$->push_back($1);
	
		}
	|	scalar_exp_commalist ',' scalar_exp 
		{ 
		    $$ = $1;
		    $$->push_back($3);
		}
	;

atom:
		parameter_ref
	|	literal
	|	USER
	;

parameter_ref:
		parameter
	|	parameter parameter
		{
		   std::string str = $1;
		   str += " ";
		   str += $2;
		   $$ = copy_string(str.c_str()); 	
		}
	|	parameter INDICATOR parameter
		{
		   std::string str = $1;
		   str += " ";
		   str += $2;
		   str += " ";
		   str += $3;
		   $$ = copy_string(str.c_str());	
		}
	;

function_ref:
		AMMSC '(' '*' ')'
		{
		   std::string str = $1;
		   str += "(";
		   str +=  "*";
		   str +=  ")";
		   $$ = copy_string(str.c_str());
  	    }
	|	AMMSC '(' DISTINCT column_ref ')'
		{
		   std::string str = $1;
		   str += "(";
		   str += $3;
		   str += " ";
		   str += $4;
		   str += ")";
		   $$ = copy_string(str.c_str());
		  			   
		}
	|	AMMSC '(' ALL scalar_exp ')'
		{
		   std::string str = $1;
           str += "(";
           str += $3;
           str += " ";
           str += $4;
           str += ")";
           $$ = copy_string(str.c_str());	
		}
	|	AMMSC '(' scalar_exp ')'
		{
		   std::string str = $1;
		   str += "(";
		   str += $3;
		   str	+= ")";	
	       $$ = copy_string(str.c_str());	 	
		}
	;

literal:
		STRING
	|	INTNUM
	|	APPROXNUM
	;

	/* miscellaneous */

table:
	table_name
	;

table_name:
	NAME '.' NAME {$$ = new TableName($1, $3);}
	| NAME {
				if (default_schema.size())
					$$ = new TableName((char*)default_schema.c_str(), $1);
				else
				    $$ = new TableName($1);
		   }
	;
        /*
	   Column Reference
	*/
column_ref:
		NAME 
	|	NAME '.' NAME	/* needs semantics */
		{
		   std::string str = $1;
		   str += ".";
		   str += $3;	
		   $$ = copy_string(str.c_str());
		}
	|	NAME '.' NAME '.' NAME
		{ 
		  std::string str = $1;
		  str += ".";
		  str += $3;
		  str += ".";
		  str += $5;
		  $$ = copy_string(str.c_str());	
		}
	;

		/* data types */

data_type:
		CHARACTER
	|	CHARACTER '(' INTNUM ')'
	|	NUMERIC
	|	NUMERIC '(' INTNUM ')'
	|	NUMERIC '(' INTNUM ',' INTNUM ')'
	|	IDB_DECIMAL
	|	IDB_DECIMAL '(' INTNUM ')'
	|	IDB_DECIMAL '(' INTNUM ',' INTNUM ')'
	|	INTEGER
	|	SMALLINT
	|	IDB_FLOAT
	|	IDB_FLOAT '(' INTNUM ')'
	|	REAL
	|	IDB_DOUBLE PRECISION
	;

	/* the various things you can name */
/*
 Columns
*/
column:		NAME
	;

cursor:		NAME
	;

parameter:
		PARAMETER	/* :name handled in parser */
	;

range_variable:	NAME
	;

user:	NAME
	;

	/* embedded condition things */
sql:	WHENEVER NOT FOUND when_action         { $$ = NULL; }
	|	WHENEVER SQLERROR when_action  { $$ = NULL; }
	;

when_action: GOTO NAME
	|	CONTINUE
	;


%%

using namespace dmlpackage;

namespace dmlpackage
{

void grammar_init(ParseTree *_parseTree, bool debug)
{
	parseTree = _parseTree;
	
	if(debug)
	  yydebug = 1;
}

void free_copybuffer()
{
  
   unsigned int i;
   for(i = 0; i < copy_buffer.size(); i++)
   {
      if (copy_buffer[i])
	      free(copy_buffer[i]);	    	
   }
   copy_buffer.clear();		  
}

char* copy_string(const char *str)
{
    char* nv = strdup(str);
    if (nv)
	   copy_buffer.push_back(nv);

    return nv;		
}

void set_defaultSchema(std::string schema)
{
    default_schema = schema;
}

}
