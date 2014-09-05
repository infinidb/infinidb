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

// $Id: tdriver2.cpp 9210 2013-01-21 14:10:42Z rdempsey $
#include <iostream>
#include <string>
#include <sstream>
#include <iomanip>
using namespace std;

#include "calpontsystemcatalog.h"
using namespace execplan;

namespace
{
typedef CalpontSystemCatalog::OID OID_t;
typedef CalpontSystemCatalog::ColType ColType_t;
typedef CalpontSystemCatalog::ROPair ROPair_t;

boost::shared_ptr<CalpontSystemCatalog> csc;

string toString(const string& tb, const string& col)
{
	ostringstream oss;

	OID_t colOID = csc->lookupOID(make_tcn("calpontsys", tb, col));
	oss << tb << " " << setw(24) << col << " " << colOID;
	ColType_t ct = csc->colType(colOID);
	oss << " " << setw(4) << ct.colWidth << " " << setw(4) << ct.ddn.dictOID << " " << setw(2) << ct.colPosition;

	return oss.str();
}

}

int main(int argc, char** argv)
{
	csc = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
	ROPair_t rp;

	string calpontsys("calpontsys");
	string table;

	table = "systable";
	rp = csc->tableRID(make_table(calpontsys, table));
	cout << "   " << table << ": " << rp.objnum << endl;
	cout << toString(table, "tablename") << endl;
	cout << toString(table, "schema") << endl;
	cout << toString(table, "objectid") << endl;
	cout << toString(table, "createdate") << endl;
	cout << toString(table, "lastupdate") << endl;
	cout << toString(table, "init") << endl;
	cout << toString(table, "next") << endl;
	cout << toString(table, "numofrows") << endl;
	cout << toString(table, "avgrowlen") << endl;
	cout << toString(table, "numofblocks") << endl;
	cout << toString(table, "autoincrement") << endl;
	cout << endl;

	table = "syscolumn";
	rp = csc->tableRID(make_table(calpontsys, table));
	cout << "   " << table << ": " << rp.objnum << endl;
	cout << toString(table, "schema") << endl;
	cout << toString(table, "tablename") << endl;
	cout << toString(table, "columnname") << endl;
	cout << toString(table, "objectid") << endl;
	cout << toString(table, "dictobjectid") << endl;
	cout << toString(table, "listobjectid") << endl;
	cout << toString(table, "treeobjectid") << endl;
	cout << toString(table, "datatype") << endl;
	cout << toString(table, "columnlength") << endl;
	cout << toString(table, "columnposition") << endl;
	cout << toString(table, "lastupdate") << endl;
	cout << toString(table, "defaultvalue") << endl;
	cout << toString(table, "nullable") << endl;
	cout << toString(table, "scale") << endl;
	cout << toString(table, "prec") << endl;
	cout << toString(table, "autoincrement") << endl;
	cout << toString(table, "distcount") << endl;
	cout << toString(table, "nullcount") << endl;
	cout << toString(table, "minvalue") << endl;
	cout << toString(table, "maxvalue") << endl;
	cout << toString(table, "compressiontype") << endl;
	cout << toString(table, "nextvalue") << endl;
	cout << endl;
#if 0
	table = "sysindexcol";
	rp = csc->tableRID(make_table(calpontsys, table));
	cout << "   " << table << ": " << rp.objnum << endl;
	cout << toString(table, "schema") << endl;
	cout << toString(table, "tablename") << endl;
	cout << toString(table, "columnname") << endl;
	cout << toString(table, "indexname") << endl;
	cout << toString(table, "columnposition") << endl;
	cout << endl;

	table = "sysconstraint";
	rp = csc->tableRID(make_table(calpontsys, table));
	cout << "   " << table << ": " << rp.objnum << endl;
	cout << toString(table, "constraintname") << endl;
	cout << toString(table, "schema") << endl;
	cout << toString(table, "tablename") << endl;
	cout << toString(table, "constrainttype") << endl;
	cout << toString(table, "constraintprimitive") << endl;
	cout << toString(table, "constrainttext") << endl;
	cout << toString(table, "constraintstatus") << endl;
	cout << toString(table, "indexname") << endl;
	cout << toString(table, "referencedtablename") << endl;
	cout << toString(table, "referencedschema") << endl;
	cout << toString(table, "referencedconstraintname") << endl;
	cout << endl;

	table = "sysconstraintcol";
	rp = csc->tableRID(make_table(calpontsys, table));
	cout << "   " << table << ": " << rp.objnum << endl;
	cout << toString(table, "schema") << endl;
	cout << toString(table, "tablename") << endl;
	cout << toString(table, "columnname") << endl;
	cout << toString(table, "constraintname") << endl;
	cout << endl;

	table = "sysindex";
	rp = csc->tableRID(make_table(calpontsys, table));
	cout << "   " << table << ": " << rp.objnum << endl;
	cout << toString(table, "schema") << endl;
	cout << toString(table, "tablename") << endl;
	cout << toString(table, "indexname") << endl;
	cout << toString(table, "listobjectid") << endl;
	cout << toString(table, "treeobjectid") << endl;
	cout << toString(table, "indextype") << endl;
	cout << toString(table, "multicolflag") << endl;
	cout << toString(table, "createdate") << endl;
	cout << toString(table, "lastupdate") << endl;
	cout << toString(table, "recordcount") << endl;
	cout << toString(table, "treelevel") << endl;
	cout << toString(table, "leafcount") << endl;
	cout << toString(table, "distinctkeys") << endl;
	cout << toString(table, "leafblocks") << endl;
	cout << toString(table, "averageleafcountperkey") << endl;
	cout << toString(table, "averagedatablockperkey") << endl;
	cout << toString(table, "samplesize") << endl;
	cout << toString(table, "clusterfactor") << endl;
	cout << toString(table, "lastanalysisdate") << endl;
	cout << endl;
#endif
	return 0;
}

