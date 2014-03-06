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

#include <iostream>
#include <fstream>
#include <errno.h>

#include <boost/program_options.hpp>

#include "sqlparser.h"
using namespace std;
using namespace ddlpackage;

namespace po = boost::program_options;

#include "ddl-gram.h"

int main(int argc, char* argv[])
{
	string sqlfile;
	int count;

 	po::options_description desc ("Allowed options");
  	desc.add_options ()
 		("help", "produce help message")
		("bisond", /* po::value <string>(),*/ "Have bison produce debug output")
		("count", po::value <int>(), "number of runs")
 		("sql", po::value < string > (), "sql file");
 	po::variables_map vm;
  	po::store (po::parse_command_line (argc, argv, desc), vm);
  	po::notify (vm);
  	if (vm.count ("sql"))
  		sqlfile = vm["sql"].as <string> ();


	if (vm.count("count"))
		count = vm["count"].as<int>();
	
	SqlFileParser parser;
	if (vm.count ("bisond"))
		parser.SetDebug(true);
		
 	parser.Parse(sqlfile);

	if(parser.Good()) {
		const ParseTree &ptree = parser.GetParseTree();

		cout << "Parser succeeded." << endl;
		cout << ptree.fList.size() << " " << "SQL statements" << endl;
		cout << ptree;
		cout << endl;
	}
	else {
		cout << "Parser failed." << endl;
	}
	
	return parser.Good() ? 0 : -1;
}


