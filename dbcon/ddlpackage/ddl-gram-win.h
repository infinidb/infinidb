
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
     ACTION = 258,
     ADD = 259,
     ALTER = 260,
     AUTO_INCREMENT = 261,
     BIGINT = 262,
     BIT = 263,
     IDB_BLOB = 264,
     CASCADE = 265,
     IDB_CHAR = 266,
     CHARACTER = 267,
     CHECK = 268,
     CLOB = 269,
     COLUMN = 270,
     COLUMNS = 271,
     COMMENT = 272,
     CONSTRAINT = 273,
     CONSTRAINTS = 274,
     CREATE = 275,
     CURRENT_USER = 276,
     DATETIME = 277,
     DEC = 278,
     DECIMAL = 279,
     DEFAULT = 280,
     DEFERRABLE = 281,
     DEFERRED = 282,
     IDB_DELETE = 283,
     DROP = 284,
     ENGINE = 285,
     FOREIGN = 286,
     FULL = 287,
     IMMEDIATE = 288,
     INDEX = 289,
     INITIALLY = 290,
     IDB_INT = 291,
     INTEGER = 292,
     KEY = 293,
     MATCH = 294,
     MAX_ROWS = 295,
     MIN_ROWS = 296,
     MODIFY = 297,
     NO = 298,
     NOT = 299,
     NULL_TOK = 300,
     NUMBER = 301,
     NUMERIC = 302,
     ON = 303,
     PARTIAL = 304,
     PRECISION = 305,
     PRIMARY = 306,
     REFERENCES = 307,
     RENAME = 308,
     RESTRICT = 309,
     SET = 310,
     SMALLINT = 311,
     TABLE = 312,
     TIME = 313,
     TINYINT = 314,
     TO = 315,
     UNIQUE = 316,
     UPDATE = 317,
     USER = 318,
     SESSION_USER = 319,
     SYSTEM_USER = 320,
     VARCHAR = 321,
     VARBINARY = 322,
     VARYING = 323,
     WITH = 324,
     ZONE = 325,
     DOUBLE = 326,
     IDB_FLOAT = 327,
     REAL = 328,
     CHARSET = 329,
     IF = 330,
     EXISTS = 331,
     CHANGE = 332,
     TRUNCATE = 333,
     IDENT = 334,
     FCONST = 335,
     SCONST = 336,
     CP_SEARCH_CONDITION_TEXT = 337,
     ICONST = 338,
     DATE = 339
   };
#endif



#if ! defined YYSTYPE && ! defined YYSTYPE_IS_DECLARED
typedef union YYSTYPE
{


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



} YYSTYPE;
# define YYSTYPE_IS_TRIVIAL 1
# define yystype YYSTYPE /* obsolescent; will be withdrawn */
# define YYSTYPE_IS_DECLARED 1
#endif

extern YYSTYPE ddllval;


