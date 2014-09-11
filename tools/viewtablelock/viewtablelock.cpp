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

/*
* $Id: viewtablelock.cpp 1762 2012-04-03 18:34:48Z dcathey $
*/

#include <iostream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <unistd.h>
using namespace std;

#include "rwlock.h"

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
using namespace execplan;

#include "exceptclasses.h"
using namespace logging;
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "dbrm.h"
using namespace BRM;


#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
using namespace oam;

namespace {

void usage()
{
	cout << "Usage: viewtablelock [schemaname] [tablename]" << endl
         << "  schemaname  tablename will display table lock information "
		"for the table." << endl
         << "  schema only or no option will display table lock information "
		"for the database." << endl
		;
}
}

int main(int argc, char** argv)
{
	int c;
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
	oamModuleInfo_t t;
	Oam oam;
	bool parentOAMModuleFlag = false;

	//get local module info; validate running on Active Parent OAM Module
	try {
		t = oam.getModuleInfo();
		parentOAMModuleFlag = boost::get<4>(t);
	}
	catch (exception&) {
		parentOAMModuleFlag = true;
	}

	if ( !parentOAMModuleFlag )
	{
		cerr << "Exiting, viewtablelock can only be run on the Active "
			"Parent OAM Module" << endl;
		return 1;
	}
	
	SessionManager sessionManager;
	
	string DMLProcProcessName("DMLProc");

	if ((argc - optind) > 2 )
	{
		usage();
		return 1;
	}
	else if ((argc - optind) == 2 )
	{
		//The specified table lock info is required
		string schema(argv[optind++]);
		string table( argv[optind++]);
		//Get table oid
		CalpontSystemCatalog *systemCatalogPtr =
			CalpontSystemCatalog::makeCalpontSystemCatalog(1);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);
		CalpontSystemCatalog::TableName tableName;
		tableName.schema = schema;
		tableName.table = table;
		CalpontSystemCatalog::ROPair roPair;

		try
		{
			roPair = systemCatalogPtr->tableRID( tableName );
		}
		
		catch (logging::IDBExcept& e)
		{
			cerr << e.what() << endl;
			return 0;
		}
		catch (runtime_error& e)
		{
			cerr << "Error searching for table in system catalog. " <<
			e.what() << endl;
			return 2;
		}
		catch (...)
		{
			cerr <<"Unknown error searching for table in system catalog."<<endl;
			return 3;
		}

		u_int32_t  processID;
		std::string  processName;
		bool  lockStatus;
		u_int32_t  sid;
		int rc = sessionManager.getTableLockInfo(
			roPair.objnum, processID, processName, lockStatus, sid);
		if ( rc != 0 )
		{
			cerr << " The lock information cannot be obtained due to "
				"system error" ;
			return 1;
		}
		
		if ( lockStatus )
		{
			cout << " table " << schema << "." << table <<
				" is locked by  " << endl;
			cout << "   ProcessName = " << processName << "    ProcessID = " <<
				processID;
//cout << " [sid: " << sid << "]";
			if ((processName == DMLProcProcessName) && (sid == 0))
			{
				cout << " (DMLProc startup adopted this lock from "
					"an aborted bulk load)";
			}
			cout << endl;
		}
		else
		{
			cout << " table " << schema << "." << table <<
				" is not locked by any process. " << endl;
		}
    }
    else
	{
		//All table lock info required
		std::vector<BRM::SIDTIDEntry>  sidTidentries;
		sessionManager.getTableLocksInfo(  sidTidentries );
		if ( sidTidentries.size() <= 0 )
			cout << " No tables are locked in the database." << endl;
		else
		{
			CalpontSystemCatalog *systemCatalogPtr =
				CalpontSystemCatalog::makeCalpontSystemCatalog();
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			CalpontSystemCatalog::TableName tableName;

			cout << " There are " <<  sidTidentries.size() <<
				" table(s) locked" << endl;
			for ( unsigned int i = 0; i < sidTidentries.size(); i++ )
			{
				try {
					tableName = systemCatalogPtr->tableName(
						sidTidentries[i].tableOID);
					cout << tableName << " is locked by ProcessName = " <<
						sidTidentries[i].processName 
						<< "  ProcessID = " << sidTidentries[i].processID;
//cout << " [sid: " << sidTidentries[i].sessionid << "]";
					if ((sidTidentries[i].processName == DMLProcProcessName) &&
						(sidTidentries[i].sessionid == 0))
					{
						cout << " (DMLProc startup adopted this lock from "
							"an aborted bulk load)";
					}
					cout << endl;
				}
				catch ( logging::IDBExcept&)
				{
					cout << "Table  with oid " << sidTidentries[i].tableOID <<
						" is not in systable."<< endl;
				}
			}			
		}
	}

	return 0;
}

// vim:ts=4 sw=4:

