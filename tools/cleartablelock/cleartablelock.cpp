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
* $Id: cleartablelock.cpp 1762 2012-04-03 18:34:48Z dcathey $
*/

#include <iostream>
#include <vector>
#include <stdexcept>
#include <sstream>
using namespace std;

#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
using namespace execplan;

#include "extentmap.h"
#include "dbrm.h"
using namespace BRM;

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"

#include "writeengine.h"
#include "we_brm.h"

namespace {

//------------------------------------------------------------------------------
// Print command line usage
//------------------------------------------------------------------------------
void usage()
{
	cout << "Usage: cleartablelock schemaname tablename" << endl
         << " -h to display this menu" << endl
         << " -d to enable debug"      << endl;
}

//------------------------------------------------------------------------------
// Verify we are running from a proper node, based on whether we are running
// shared-nothing or shared-everything.
//------------------------------------------------------------------------------
// DMC-SHARED_NOTHING: change cleartablelock to work on UM
void verifyNode()
{
	bool bSharedNothing = false;
	config::Config* cf = config::Config::makeConfig();
	string sn = cf->getConfig("Installation", "SharedNothing");
	if ((sn == "y") || (sn == "Y"))
		bSharedNothing = true;

	if (bSharedNothing) // Shared-nothing
	{
		//----------------------------------------------------------------------
		// Validate running on a PM
		//----------------------------------------------------------------------
		oam::Oam oam;
		std::string localModuleType;
		try {
			oam::oamModuleInfo_t t;
			t = oam.getModuleInfo();
			localModuleType = boost::get<1>(t);
		}
		catch (exception&) {
			localModuleType.assign("pm");
		}

		if (localModuleType != "pm")
		{
			cerr << "Exiting, cleartablelock "
				"can only be run on a PM node" << endl;

			exit(1);
		}
	}
	else                // Shared-everything
	{
		//----------------------------------------------------------------------
		// Validate running on Active Parent OAM Module
		//----------------------------------------------------------------------
		oam::Oam oam;
		bool parentOAMModuleFlag;
		try {
			oam::oamModuleInfo_t t;
			t = oam.getModuleInfo();
			parentOAMModuleFlag = boost::get<4>(t);
		}
		catch (exception&) {
			parentOAMModuleFlag = true;
		}

		if ( !parentOAMModuleFlag )
		{
			cerr << "Exiting, cleartablelock "
				"can only be run on the Active Parent OAM Module" << endl;

			exit(1);
		}
	}
}

}

//------------------------------------------------------------------------------
// Main entry point to this program
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
	int c;
	bool clearLockOnly = false;
	bool rollbackOnly  = false;
	bool debugFlag     = false;

	while ((c = getopt(argc, argv, "dhlr")) != EOF)
	{
		switch (c)
		{
			case 'd':
				debugFlag = true;
				break;
			case 'l':
				clearLockOnly = true;
				break;
//Only allow '-r' option for development/debugging
#if 0
			case 'r':
				rollbackOnly  = true;
				break;
#endif
			case 'h':
			case '?':
			default:
				usage();
				return (c == 'h' ? 0 : 1);
				break;
		}
	}

	if ((argc - optind) != 2 )
	{
		usage();
		return 1;
	}

	// If user specified both clearlock and rollback then we need to do both
	if (clearLockOnly && rollbackOnly)
	{
		clearLockOnly = false;
		rollbackOnly  = false;
	}

	// validate running on Active Parent OAM Module
	verifyNode();

	// Verify that BRM is up and in a read/write state
	int brmReadWriteStatus =
		WriteEngine::BRMWrapper::getInstance()->isReadWrite();
	if (brmReadWriteStatus != WriteEngine::NO_ERROR)
	{
		WriteEngine::WErrorCodes ec;
		cerr << ec.errorString(brmReadWriteStatus) <<
			"  Table lock cannot be cleared." << endl;
		return 1;
	}
	
	//The specified table lock info is required
	string schema(argv[optind++]);
	string table( argv[optind++]);
#ifndef _MSC_VER
	//Check whether PrimProc is runing
	int checkpid = system( "pidof 'PrimProc' > /dev/null" );
	if ( checkpid != 0 )
	{
		cerr << "PrimProc is not running" << endl;
		return 1;
	}
#else
	//FIXME: add a windows check?
#endif
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
	catch (logging::NoTableExcept&)
	{
		cerr << "Table " << tableName.schema <<"." << tableName.table <<
			" doesn't exist." << endl;
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
		cerr << "Unknown error searching for table in system catalog." << endl;
		return 3;
	}
	//@Bug 3711 Check whether the table is locked.
	DBRM brm;

	if (!rollbackOnly)
	{
		int rc = 0;
		u_int32_t  processID;
    	string     processName;
    	bool       lockStatus;
		u_int32_t  sid;
    	rc = brm.getTableLockInfo(roPair.objnum, processID,
			processName, lockStatus, sid);
    	if (rc == 0 && !lockStatus)
		{
			cerr << "Table " << tableName.schema <<"." << tableName.table <<
				" is not locked. "<< endl ;
			return 0;
		}
	}
	
	WriteEngine::WriteEngineWrapper writeEngine;

	if (clearLockOnly)
	{
		int rc = writeEngine.clearLockOnly( roPair.objnum );
		if ( rc == 2 )
		{
			cerr << " table is not locked. "<< endl ;
			return 0;
		}
		else if ( rc != 0 )
		{
			cerr << "Clear table lock failed!" ;
			return 1;
		}
		cout << " Lock for " << schema << "." << table << " is cleared." <<endl;
	}
	else
	{
		//update tablelock with current process name and process id
		int rc = 0;
		if (!rollbackOnly)
		{
			u_int32_t  processID = ::getpid();
			std::string  processName = "cleartablelock";
			rc = brm.updateTableLock( roPair.objnum, processID,
				processName );
			if ( rc > 0 )
			{
				cerr << " The table is still locked by an active process; " <<
					"table lock is not cleared." << endl;
				return 4;
			}
			else
			{
				cerr << " The table is locked by cleartablelock now. "
					"The table was previously locked by \"" << processName <<
					"\" with process ID " << processID <<  endl;
			}
		}

		std::string errorMsg;
        const std::string APPLNAME("cleartablelock command");
		rc = writeEngine.bulkRollback( roPair.objnum,
			tableName.toString(),
			APPLNAME, rollbackOnly, debugFlag, errorMsg );
		if (rc == WriteEngine::NO_ERROR)
		{
			cout << " Lock for " << schema << "." << table <<
				" is cleared." << endl;
		}
		else
		{
			if ((rc == WriteEngine::ERR_TBL_TABLE_HAS_VALID_CPIMPORT_LOCK) ||
				(rc == WriteEngine::ERR_TBL_TABLE_HAS_VALID_DML_DDL_LOCK)  ||
				(rc == WriteEngine::ERR_TBL_TABLE_LOCK_NOT_FOUND))
			{
				cerr << "Did not clear table lock for " <<
					schema << "." << table << "; " << errorMsg << endl;
			}
			else
			{
				cerr << "Error in clearing table lock for " <<
					schema << "." << table << "; " << errorMsg << endl;
			}
			cerr << "Table lock is not cleared." << endl;
			return 4;
		}
	}
	
	return 0;
}

// vim:ts=4 sw=4:

