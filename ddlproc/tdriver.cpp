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

/***********************************************************************
 *   $Id: $
 *
 *
 ***********************************************************************/
#include <ctime>
#include <string>
#include <sstream>
#include <exception>
#include <iostream>
#include <fstream>
#include <unistd.h>
#include <vector>
#include <cassert>
#include "ddlpkg.h"
#include "sqlparser.h"
#include "messagequeue.h"

#include <boost/program_options.hpp>

namespace po = boost::program_options;

using namespace std;
using namespace ddlpackage;
using namespace messageqcpp;

void sendOne(string sqlfile)
{
	MessageQueueClient ddlproc("DDLProc");
 	ByteStream obs,ibs;
	ByteStream::byte status;

		
	SqlFileParser p;
	p.Parse(sqlfile);
	assert(p.Good());

	const ParseTree& stmts = p.GetParseTree();
	SqlStatement* stmt = stmts[0];
	assert(stmt);

 	stmt->serialize(obs);
 	ddlproc.write(obs);

	ibs = ddlproc.read();

	cout << "bs: " << ibs.length() << endl;

	if (ibs.length() > 0) {
		ibs >> status;
		cout << "status: " << (int)status << endl;
	}
	
 	cout << "Bye" << endl;
}

// ../dbcon/ddlpackage/sql/atac01.sql
// ../dbcon/ddlpackage/sql/atac02.sql
// ../dbcon/ddlpackage/sql/atac03.sql
// ../dbcon/ddlpackage/sql/atac04.sql
// ../dbcon/ddlpackage/sql/atac05.sql
// ../dbcon/ddlpackage/sql/atac06.sql
// ../dbcon/ddlpackage/sql/atac07.sql
// ../dbcon/ddlpackage/sql/atac08.sql
// ../dbcon/ddlpackage/sql/atac09.sql
// ../dbcon/ddlpackage/sql/atatc01.sql
// ../dbcon/ddlpackage/sql/atdc01.sql
// ../dbcon/ddlpackage/sql/atdtc01.sql
// ../dbcon/ddlpackage/sql/atmcdd01.sql
// ../dbcon/ddlpackage/sql/atmcsd01.sql
// ../dbcon/ddlpackage/sql/atmct01.sql
// ../dbcon/ddlpackage/sql/atrc01.sql
// ../dbcon/ddlpackage/sql/atrt01.sql
// ../dbcon/ddlpackage/sql/ci01.sql
// ../dbcon/ddlpackage/sql/ci02.sql
// ../dbcon/ddlpackage/sql/ct01.sql
// ../dbcon/ddlpackage/sql/ct02.sql
// ../dbcon/ddlpackage/sql/ct03.sql
// ../dbcon/ddlpackage/sql/ct04.sql
// ../dbcon/ddlpackage/sql/ct05.sql
// ../dbcon/ddlpackage/sql/ct06.sql
// ../dbcon/ddlpackage/sql/ct07.sql
// ../dbcon/ddlpackage/sql/ct08.sql
// ../dbcon/ddlpackage/sql/ct09.sql
// ../dbcon/ddlpackage/sql/ct10.sql
// ../dbcon/ddlpackage/sql/ct11.sql
// ../dbcon/ddlpackage/sql/di01.sql
// ../dbcon/ddlpackage/sql/dt01.sql
// ../dbcon/ddlpackage/sql/dt02.sql


int main(int argc, char* argv[])
{
	string sqlfile;

 	po::options_description desc ("Allowed options");
  	desc.add_options ()
 		("help", "produce help message")
 		("sql", po::value < string > (), "sql file");
 	po::variables_map vm;
  	po::store (po::parse_command_line (argc, argv, desc), vm);
  	po::notify (vm);

  	if (vm.count ("sql")) {
  		sqlfile = vm["sql"].as <string> ();
		sendOne(sqlfile);
		return 0;
	}
	
}

