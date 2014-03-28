/* Copyright (C) 2013 Calpont Corp.

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

/***********************************************************************
*   $Id: sqlparser.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/

#include <fstream>
#include <errno.h>

#define DDLPKGSQLPARSER_DLLEXPORT
#include "sqlparser.h"
#undef DDLPKGSQLPARSER_DLLEXPORT

#ifdef _MSC_VER
#include "ddl-gram-win.h"
#else
#include "ddl-gram.h"
#endif

void scanner_finish(void);
void scanner_init(const char *str);
void grammar_init(ddlpackage::ParseTree *ptree, bool);
void set_schema(std::string schema);
int ddlparse();

namespace ddlpackage {
	using namespace std;

	SqlParser::SqlParser() :
		fStatus(-1),
		fDebug(false)
	{
	}


	void SqlParser::SetDebug(bool debug)
	{
		fDebug = debug;
	}

	void SqlParser::setDefaultSchema(std::string schema)
    {
		set_schema(schema);
	}

	int SqlParser::Parse(const char* sqltext)
	{
		scanner_init(sqltext);
		grammar_init(&fParseTree, fDebug);
		fStatus = ddlparse();
		return fStatus;
	}
	

	const ParseTree& SqlParser::GetParseTree(void)
	{
		if(!Good()) {
			throw logic_error("The ParseTree is invalid");
		}
		return fParseTree;
	}


	bool SqlParser::Good()
	{
		return fStatus == 0;
	}


	SqlParser::~SqlParser()
	{
		scanner_finish(); // free scanner allocated memory
	}


	SqlFileParser::SqlFileParser() :
		SqlParser()
	{
	}
	

	int SqlFileParser::Parse(const string& sqlfile)
	{
		fStatus = -1;
		
int ddlparse();
		ifstream ifsql;
		ifsql.open(sqlfile.c_str());
		if(!ifsql.is_open()) {
			perror(sqlfile.c_str());
			return fStatus;
		}

		char sqlbuf[1024*1024];
		unsigned length;
		ifsql.seekg (0, ios::end);
		length = ifsql.tellg();
		ifsql.seekg (0, ios::beg);

		if(length > sizeof(sqlbuf) - 1) {
			throw length_error("SqlFileParser has file size hard limit of 16K.");
		}

		unsigned rcount;
		rcount = ifsql.readsome(sqlbuf, sizeof(sqlbuf) - 1);

		if(rcount < 0)
			return fStatus;

		sqlbuf[rcount] = 0;

		//cout << endl << sqlfile << "(" << rcount << ")" << endl;
		//cout << "----------------------" << endl;
		//cout << sqlbuf << endl;

		return SqlParser::Parse(sqlbuf);
	}
}
