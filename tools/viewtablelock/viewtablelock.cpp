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
* $Id: viewtablelock.cpp 2101 2013-01-21 14:12:52Z rdempsey $
*/

#include <ctime>
#include <iostream>
#include <vector>
#include <stdexcept>
#include <sstream>
#include <string>

#include "calpontsystemcatalog.h"
#include "dbrm.h"
#include "exceptclasses.h"

using namespace execplan;

namespace {

//------------------------------------------------------------------------------
// Print command line usage
//------------------------------------------------------------------------------
void usage()
{
	std::cout << "Usage: viewtablelock [schemaname tablename]" << std::endl
		<< "  If schema/table are specified, then that table's lock " <<
			"information is displayed." << std::endl
		<< "  If no schema/table are specified, then all table locks "
			"are displayed." << std::endl;
}

//------------------------------------------------------------------------------
// Print table locks.
// This function closely resembles CommandPackageProcessor::viewTableLock().
//------------------------------------------------------------------------------
int printTableLocks( boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr,
	const std::string& tableNameIn,
	const std::vector<BRM::TableLockInfo> tableLocks )
{
	if (tableLocks.size() == 1)
		std::cout << " There is " <<  tableLocks.size() <<
			" table lock" << std::endl << std::endl;
	else
		std::cout << " There are " <<  tableLocks.size() <<
			" table locks" << std::endl << std::endl;

	std::string tableName(tableNameIn);

	// Make preliminary pass through the table locks in order to determine our
	// output column widths based on the data.  Min column widths are based on
	// the width of the column heading (except for the 'state' column).
	uint64_t maxLockID                = 0;
	uint32_t maxPID                   = 0;
	int32_t  maxSessionID             = 0;
	int32_t  minSessionID             = 0;
	int32_t  maxTxnID                 = 0;

	unsigned int tableNameColumnWidth = 5; // "Table"
	unsigned int lockIDColumnWidth    = 6; // "LockID"
	unsigned int ownerColumnWidth     = 7; // "Process"
	unsigned int pidColumnWidth       = 3; // "PID"
	unsigned int sessionIDColumnWidth = 7; // "Session"
	unsigned int txnIDColumnWidth     = 3; // "Txn"
	unsigned int createTimeColumnWidth= 12;// "CreationTime"
	unsigned int pmColumnWidth        = 7; // "DBRoots"
	std::vector<std::string> createTimes;
	char cTimeBuffer[1024];
	std::vector<std::string> tableNames;

	std::ostringstream errMsgs;
	for (unsigned int i=0; i<tableLocks.size(); i++)
	{
		if (tableNameIn.empty())
		{
			try {
				CalpontSystemCatalog::TableName tableNameStruct;
				tableNameStruct = systemCatalogPtr->tableName(
					tableLocks[i].tableOID);
				tableName = tableNameStruct.toString();
			}
			catch ( logging::IDBExcept&)
			{
                tableName.clear();

				errMsgs << "Table with oid " << tableLocks[i].tableOID <<
					" (Lock " << tableLocks[i].id << ")" <<
					" is not in systable." << std::endl;
			}
			catch (std::runtime_error& e)
			{
                tableName.clear();

				errMsgs <<
					"Error searching for table " << tableLocks[i].tableOID <<
					" (Lock " << tableLocks[i].id << ")" <<
					" in system catalog. " << e.what() << std::endl;
			}
			catch (...)
			{
                tableName.clear();

				errMsgs <<
					"Unknown error searching for table " <<
					tableLocks[i].tableOID <<
					" (Lock " << tableLocks[i].id << ")" <<
					" in system catalog. " << std::endl;
			}
		}
		tableNames.push_back( tableName );

		tableNameColumnWidth = std::max(tableNameColumnWidth,
			static_cast<unsigned int>(tableName.length()));
		maxLockID = std::max(maxLockID, tableLocks[i].id);
		ownerColumnWidth = std::max(ownerColumnWidth,
			static_cast<unsigned int>(tableLocks[i].ownerName.length()));
		maxPID = std::max(maxPID, tableLocks[i].ownerPID);
		maxSessionID = std::max(maxSessionID, tableLocks[i].ownerSessionID);
		minSessionID = std::min(minSessionID, tableLocks[i].ownerSessionID);
		maxTxnID = std::max(maxTxnID, tableLocks[i].ownerTxnID);

		ctime_r( &tableLocks[i].creationTime, cTimeBuffer );
		cTimeBuffer[ strlen(cTimeBuffer)-1 ] = '\0'; // strip trailing '\n'
		std::string cTimeStr( cTimeBuffer );
		createTimeColumnWidth = std::max(createTimeColumnWidth,
			static_cast<unsigned int>(cTimeStr.length()));
		createTimes.push_back( cTimeStr );

		std::ostringstream pms;
		for (unsigned k=0; k<tableLocks[i].dbrootList.size(); k++)
		{
			if (k > 0)
				pms << ',';
			pms << tableLocks[i].dbrootList[k];
		}
		pmColumnWidth = std::max(pmColumnWidth,
			static_cast<unsigned int>(pms.str().length()));
	}
	tableNameColumnWidth  += 2;
	ownerColumnWidth      += 2;
	pmColumnWidth         += 2;
	createTimeColumnWidth += 2;

	std::ostringstream idString;
	idString << maxLockID;
	lockIDColumnWidth = std::max(lockIDColumnWidth,
		static_cast<unsigned int>(idString.str().length()));
	lockIDColumnWidth += 2;

	std::ostringstream pidString;
	pidString << maxPID;
	pidColumnWidth = std::max(pidColumnWidth,
		static_cast<unsigned int>(pidString.str().length()));
	pidColumnWidth += 2;

	const std::string sessionNoneStr("BulkLoad");
	std::ostringstream sessionString;
	sessionString << maxSessionID;
	sessionIDColumnWidth = std::max(sessionIDColumnWidth,
		static_cast<unsigned int>(sessionString.str().length()));
	if (minSessionID < 0)
		sessionIDColumnWidth = std::max(sessionIDColumnWidth,
			static_cast<unsigned int>(sessionNoneStr.length()));
	sessionIDColumnWidth += 2;

	const std::string txnNoneStr("n/a");
	std::ostringstream txnString;
	txnString << maxTxnID;
	txnIDColumnWidth = std::max(txnIDColumnWidth,
		static_cast<unsigned int>(txnString.str().length()));
	txnIDColumnWidth += 2;

	std::cout.setf(std::ios::left, std::ios::adjustfield);
	std::cout << "  " <<
		std::setw(tableNameColumnWidth) << "Table"        <<
		std::setw(lockIDColumnWidth)    << "LockID"       <<
		std::setw(ownerColumnWidth)     << "Process"      <<
		std::setw(pidColumnWidth)       << "PID"          <<
		std::setw(sessionIDColumnWidth) << "Session"      <<
		std::setw(txnIDColumnWidth)     << "Txn"          <<
		std::setw(createTimeColumnWidth)<< "CreationTime" <<
		std::setw(9)                    << "State"        <<
		std::setw(pmColumnWidth)        << "DBRoots"      << std::endl;

	// Make second pass through the table locks to display our result.
	for (unsigned idx=0; idx<tableLocks.size(); idx++)
	{
		std::ostringstream pms; //dbroots now
		for (unsigned k=0; k<tableLocks[idx].dbrootList.size(); k++)
		{
			if (k > 0)
				pms << ',';
			pms << tableLocks[idx].dbrootList[k];
		}
		
		std::cout << "  " <<
			std::setw(tableNameColumnWidth) << tableNames[idx]          <<
			std::setw(lockIDColumnWidth)    << tableLocks[idx].id       <<
			std::setw(ownerColumnWidth)     << tableLocks[idx].ownerName<<
			std::setw(pidColumnWidth)       << tableLocks[idx].ownerPID;

		// Log session ID, or "BulkLoad" if session is -1
		if (tableLocks[idx].ownerSessionID < 0)
			std::cout << std::setw(sessionIDColumnWidth) << sessionNoneStr;
		else
			std::cout << std::setw(sessionIDColumnWidth) <<
				tableLocks[idx].ownerSessionID;

		// Log txn ID, or "n/a" if txn is -1
		if (tableLocks[idx].ownerTxnID < 0)
			std::cout << std::setw(txnIDColumnWidth) << txnNoneStr;
		else
			std::cout << std::setw(txnIDColumnWidth) <<
				tableLocks[idx].ownerTxnID;

		std::cout <<
			std::setw(createTimeColumnWidth)<<
				createTimes[idx]       <<
			std::setw(9) << ((tableLocks[idx].state==BRM::LOADING) ?
				"LOADING" : "CLEANUP") <<
			std::setw(pmColumnWidth)        << pms.str() << std::endl;
	}

	if (!errMsgs.str().empty())
		std::cerr << std::endl << errMsgs.str() << std::endl;

	return 0;
}

}

//------------------------------------------------------------------------------
// Main entry point to this program
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
	int c;
	while ((c = getopt(argc, argv, "h")) != EOF)
	{
		switch (c)
		{
			case 'h':
			case '?':
			default:
				usage();
				return (c == 'h' ? 0 : 1);
				break;
		}
	}

	int nargs = argc - optind;
	if ((nargs > 2) || (nargs == 1))
	{
		usage();
		return 1;
	}

    BRM::DBRM dbrm;
	std::vector<BRM::TableLockInfo> tableLocks;
    try
    {
		tableLocks = dbrm.getAllTableLocks();
	}
	catch (std::exception& ex)
	{
		std::cerr << "Error getting list of table locks: " << ex.what() <<
			std::endl;
		return 2;
	}

	int rc = 0;
	if (nargs == 2) // List table lock information for a given table
	{
		std::string schema(argv[optind++]);
		std::string table( argv[optind++]);

		// Get table oid
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
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
			std::cerr << e.what() << std::endl;
			return 3;
		}
		catch (std::runtime_error& e)
		{
			std::cerr << "Error searching for table in system catalog. " <<
				e.what() << std::endl;
			return 4;
		}
		catch (...)
		{
			std::cerr << "Unknown error searching for table in system catalog."
				<<std::endl;
			return 5;
		}

		// Keep in mind the same table could have more than 1 lock
		// (on different PMs), so we don't exit loop after "first" match.
		std::vector<BRM::TableLockInfo> matchingTableLocks;
		for (unsigned int i=0; i<tableLocks.size(); i++)
		{
			if (roPair.objnum == (CalpontSystemCatalog::OID)
				tableLocks[i].tableOID)
			{
				matchingTableLocks.push_back( tableLocks[i] );
			}
		}
		if (matchingTableLocks.size() > 0)
		{
			std::string tableName(schema);
			tableName += '.';
			tableName += table;
			rc = printTableLocks( systemCatalogPtr,
				tableName, matchingTableLocks );
		}
		else
		{
			std::cout << " Table " << schema << "." << table <<
				" is not locked by any process. " << std::endl;
		}
    }
    else // List table lock information for all table locks
	{
		//All table lock info required
		if (tableLocks.size() == 0)
		{
			std::cout << " No tables are locked in the database." << std::endl;
		}
		else
		{
			boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
				CalpontSystemCatalog::makeCalpontSystemCatalog();
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);

			std::string tableName;
			rc = printTableLocks( systemCatalogPtr,
				tableName, tableLocks );
		}
	}

	return rc;
}
