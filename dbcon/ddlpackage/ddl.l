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

/* $Id: ddl.l 9341 2013-03-27 14:10:35Z chao $ */
%{
#include <iostream>
#include <vector>
#include <stdio.h>
#include "ddlpkg.h"

#ifdef _MSC_VER
#include "ddl-gram-win.h"
#else
#include "ddl-gram.h"
#endif

int lineno = 1;
void ddlerror(char *s);

/* Handles to the buffer that the lexer uses internally */
static YY_BUFFER_STATE scanbufhandle;
static char *scanbuf;

static char* scanner_copy (char *str);

%}

%option noyywrap
%option nounput
/* %option header-file="ddl-scan.h" */

%x check1
%x check2
%x inquote
%x endquote

space [ \t\n\r\f]
horiz_space [ \t\f]
newline [\n\r]
non_newline [^\n\r]

comment ("--"{non_newline}*)
self			[,()\[\].;\:\+\-\*\/\%\^\<\>\=]
whitespace ({space}+|{comment})

digit [0-9]
ident_start [A-Za-z\200-\377_]
ident_cont [A-Za-z\200-\377_0-9\$]
identifier {ident_start}{ident_cont}*

integer			[-+]?{digit}+
decimal			([-+]?({digit}*\.{digit}+)|({digit}+\.{digit}*))
real			({integer}|{decimal})[Ee][-+]?{digit}+
realfail1		({integer}|{decimal})[Ee]
realfail2		({integer}|{decimal})[Ee][-+]

quote '
grave_accent `

%%

ACTION {return ACTION;}
ADD {return ADD;}
ALTER {return ALTER;}
AUTO_INCREMENT {return AUTO_INCREMENT;}
CASCADE {return CASCADE;}
CHAR {return IDB_CHAR;}
CHARACTER {return IDB_CHAR;}
BIGINT {return BIGINT;}
CHECK {BEGIN(check1);return CHECK;}
<check1>\( {BEGIN(check2); return '(';}
<check2>[^)]*/\) {BEGIN(check1); ddllval.str = scanner_copy(ddltext); return CP_SEARCH_CONDITION_TEXT;}
<check1>\) {BEGIN(0); return ')';}

{quote} {BEGIN(inquote);return yytext[0];}
<inquote>[^']*/' {BEGIN(endquote); ddllval.str = scanner_copy(ddltext); return SCONST;}
<endquote>' {BEGIN(0); return yytext[0];}

{integer} {ddllval.str = scanner_copy(ddltext); return ICONST;}
{decimal} {ddllval.str = scanner_copy(ddltext); return FCONST;}
{real} {ddllval.str = scanner_copy(ddltext); return FCONST;}

COMMENT {return COMMENT;}
COLUMN {return COLUMN;}
COLUMNS {return COLUMNS;}
CONSTRAINT {return CONSTRAINT;}
CONSTRAINTS {return CONSTRAINTS;}
CREATE {return CREATE;}
CURRENT_USER {return CURRENT_USER;}
DATE {ddllval.str=strdup("date"); return DATE;}
DATETIME {return DATETIME;}
DECIMAL {return DECIMAL;}
DEC {return DECIMAL;}
DEFAULT {return DEFAULT;}
DEFERRABLE {return DEFERRABLE;}
DEFERRED {return DEFERRED;}
DELETE {return IDB_DELETE;}
DROP {return DROP;}
ENGINE {return ENGINE;}
FOREIGN {return FOREIGN;}
FULL {return FULL;}
IMMEDIATE {return IMMEDIATE;}
INDEX {return INDEX;}
INITIALLY {return INITIALLY;}
INT {return IDB_INT;}
INTEGER {return INTEGER;}
KEY {return KEY;}
MATCH {return MATCH;}
MAX_ROWS {return MAX_ROWS;}
MIN_ROWS {return MIN_ROWS;}
MODIFY {return MODIFY;}
NO {return NO;}
NOT {return NOT;}
NULL {return NULL_TOK;}
NUMERIC {return NUMERIC;}
ON {return ON;}
PARTIAL {return PARTIAL;}
PRECISION {return PRECISION;}
PRIMARY {return PRIMARY;}
REFERENCES {return REFERENCES;}
RENAME {return RENAME;}
RESTRICT {return RESTRICT;}
SESSION_USER {return SESSION_USER;}
SYSTEM_USER {return SYSTEM_USER;}
SET {return SET;}
SMALLINT {return SMALLINT;}
TABLE {return TABLE;}
TINYINT {return TINYINT;}
TO {return TO;}
UNIQUE {return UNIQUE;}
UNSIGNED {return UNSIGNED;}
UPDATE {return UPDATE;}
USER {return USER;}
VARCHAR {return VARCHAR;}
FLOAT {return IDB_FLOAT;}
DOUBLE {return DOUBLE;}
REAL {return REAL;}
CHARSET {return CHARSET;}
IF {return IF;}
EXISTS {return EXISTS;}
CHANGE {return CHANGE;}
TRUNCATE {return TRUNCATE;}
VARBINARY {return VARBINARY;}

\n { lineno++;}

{whitespace} {
    /* ignore */
}

{identifier} {ddllval.str = scanner_copy(ddltext); return IDENT;}

{self} {
    return ddltext[0];
}

{grave_accent} {
    /* ignore */
}

%%

void ddlerror(char const *s)
{
	printf("yyerror: %d: %s at %s\n", lineno, s, yytext);
}

typedef std::vector<char*> valbuf_t;

static valbuf_t valbuf;

/*
 * Called before any actual parsing is done
 */
void scanner_init(const char *str)
{
	size_t slen = strlen(str);

	/*
	 * Might be left over after ereport()
	 */
	if (YY_CURRENT_BUFFER)
		yy_delete_buffer(YY_CURRENT_BUFFER);

	/*
	 * Make a scan buffer with special termination needed by flex.
	 */
	scanbuf =  (char *)malloc(slen + 2);
	memcpy(scanbuf, str, slen);
	scanbuf[slen] = scanbuf[slen + 1] = YY_END_OF_BUFFER_CHAR;
	scanbufhandle = yy_scan_buffer(scanbuf, slen + 2);

	BEGIN(INITIAL);

        valbuf.clear();
}


/*
 * Called after parsing is done to clean up after scanner_init()
 */


void scanner_finish(void)
{
  char* str;

   yy_delete_buffer(scanbufhandle);
   free(scanbuf);
   unsigned int i;
   for(i=0; i<valbuf.size(); i++) {
     str = valbuf[i];
     if(str) {
//        std::cout << "valbuf:(" << str << ")" << std::endl;
        free(valbuf[i]);
     }
   }
   valbuf.clear();
}

char* scanner_copy (char *str)
{
  char* nv = strdup(str);
  if(nv)
    valbuf.push_back(nv);
  return nv;
}
