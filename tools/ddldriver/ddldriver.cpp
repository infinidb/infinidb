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

// $Id: ddldriver.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include <unistd.h>
#include <cstdio>
#include <iostream>
#include <cctype>
#include <sstream>
using namespace std;

#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

namespace
{
void usage()
{
	cout << "usage: ddlriver [-h] schema sql_text" << endl;
}
const string toupper_(const string& in)
{
	string::const_iterator iter = in.begin();
	string::const_iterator end = in.end();
	ostringstream oss;

	while (iter != end)
	{
		oss << static_cast<char>(toupper(*iter));
		++iter;
	}

	return oss.str();
}
}

int main(int argc, char** argv)
{
	int c;

	opterr = 0;

	while ((c = getopt(argc, argv, "h")) != EOF)
		switch (c)
		{
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if (argc - optind < 2)
	{
		usage();
		return 1;
	}

	string owner(toupper_(argv[optind++]));

	SqlParser parser;
	parser.setDefaultSchema(owner);

	string stmtStr(toupper_(argv[optind++]));
	parser.Parse(stmtStr.c_str());

	if (!parser.Good())
	{
		cerr << "Failed to parse statement: " << stmtStr << endl;
		return 1;
	}

	const ParseTree &ptree = parser.GetParseTree();
	SqlStatement& stmt = *ptree.fList[0];

	stmt.fSessionID = 1;
	stmt.fSql = stmtStr;
	stmt.fOwner = owner;

	ByteStream bytestream;
	bytestream << stmt.fSessionID;
	stmt.serialize(bytestream);
	MessageQueueClient mq("DDLProc");
	ByteStream::byte b;
	string errorMsg;
	try
	{
		mq.write(bytestream);
		bytestream = mq.read();
		bytestream >> b;
		bytestream >> errorMsg;
	}
	catch (runtime_error& rex)
	{
		cerr << "runtime_error in engine: " << rex.what() << endl;
		return 1;
	}
	catch (...)
	{
		cerr << "uknown error in engine" << endl;
		return 1;
	}

	if (b != 0)
	{
		cerr << "DDLProc error: " << errorMsg << endl;
		return 1;
	}

	return 0;
}

