
/* A Bison parser, made by GNU Bison 2.4.1.  */

/* Skeleton interface for Bison's Yacc-like parsers in C
   
      Copyright (C) 1984, 1989, 1990, 2000, 2001, 2002, 2003, 2004, 2005, 2006
   Free Software Foundation, Inc.
   
   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.
   
   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.
   
   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.  */

/* As a special exception, you may create a larger work that contains
   part or all of the Bison parser skeleton and distribute that work
   under terms of your choice, so long as that work isn't itself a
   parser generator using the skeleton or a modified version thereof
   as a parser skeleton.  Alternatively, if you modify or redistribute
   the parser skeleton itself, you may (at your option) remove this
   special exception, which will cause the skeleton and the resulting
   Bison output files to be licensed under the GNU General Public
   License without this special exception.
   
   This special exception was added by the Free Software Foundation in
   version 2.2 of Bison.  */


/* Tokens.  */
#ifndef YYTOKENTYPE
# define YYTOKENTYPE
   /* Put the tokens into the symbol table, so that GDB and other debuggers
      know about them.  */
   enum yytokentype {
     NAME = 258,
     STRING = 259,
     INTNUM = 260,
     APPROXNUM = 261,
     SELECT = 262,
     ALL = 263,
     DISTINCT = 264,
     NULLX = 265,
     USER = 266,
     INDICATOR = 267,
     AMMSC = 268,
     PARAMETER = 269,
     ANY = 270,
     SOME = 271,
     OR = 272,
     AND = 273,
     NOT = 274,
     COMPARISON = 275,
     UMINUS = 276,
     AS = 277,
     ASC = 278,
     AUTHORIZATION = 279,
     BETWEEN = 280,
     BY = 281,
     CHARACTER = 282,
     CHECK = 283,
     CLOSE = 284,
     COMMIT = 285,
     CONTINUE = 286,
     CREATE = 287,
     CURRENT = 288,
     CURSOR = 289,
     IDB_DECIMAL = 290,
     DECLARE = 291,
     DEFAULT = 292,
     DELETE = 293,
     DESC = 294,
     IDB_DOUBLE = 295,
     ESCAPE = 296,
     EXISTS = 297,
     FETCH = 298,
     IDB_FLOAT = 299,
     FOR = 300,
     FOREIGN = 301,
     FOUND = 302,
     FROM = 303,
     GOTO = 304,
     GRANT = 305,
     IDB_GROUP = 306,
     HAVING = 307,
     IN = 308,
     INSERT = 309,
     INTEGER = 310,
     INTO = 311,
     IS = 312,
     KEY = 313,
     LANGUAGE = 314,
     LIKE = 315,
     NUMERIC = 316,
     OF = 317,
     ON = 318,
     OPEN = 319,
     OPTION = 320,
     ORDER = 321,
     PRECISION = 322,
     PRIMARY = 323,
     PRIVILEGES = 324,
     PROCEDURE = 325,
     PUBLIC = 326,
     REAL = 327,
     REFERENCES = 328,
     ROLLBACK = 329,
     SCHEMA = 330,
     SET = 331,
     SMALLINT = 332,
     SQLCODE = 333,
     SQLERROR = 334,
     TABLE = 335,
     TO = 336,
     UNION = 337,
     UNIQUE = 338,
     UPDATE = 339,
     VALUES = 340,
     VIEW = 341,
     WHENEVER = 342,
     WHERE = 343,
     WITH = 344,
     WORK = 345
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


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



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE dmllval;


