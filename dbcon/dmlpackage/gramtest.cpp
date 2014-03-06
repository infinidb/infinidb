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
#include <string>
#include <fstream>
#include <errno.h>

#include <boost/program_options.hpp>

using namespace std;

namespace po = boost::program_options;
#include "dmlparser.h"
#include "dml-gram.h"

using namespace dmlpackage;

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

    DMLFileParser parser;
    if (vm.count ("bisond"))
        parser.setDebug(true);

    parser.parse(sqlfile);

    if(parser.good())
    {
        const ParseTree &ptree = parser.getParseTree();

        cout << "Parser succeeded." << endl;
        cout << ptree.fList.size() << " " << "SQL statements" << endl;
        cout << ptree.fSqlText << endl;
        cout << ptree;

        SqlStatement* statementPtr = ptree[0];
        if (statementPtr)
            cout << statementPtr->getQueryString();
        cout << endl;
    }
    else
    {
        cout << "Parser failed." << endl;
    }

    return parser.good() ? 0 : -1;
}
