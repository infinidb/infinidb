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
* $Id: cleartablelock.cpp 2336 2013-06-25 19:11:36Z rdempsey $
*/

#include <cstdlib>
#include <iostream>
#include <stdexcept>
#include <sstream>
#include <string>
#include <vector>
#include <set>
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>

#include "dbrm.h"

#include "calpontsystemcatalog.h"
#include "liboamcpp.h"

#include "writeengine.h"
#include "we_messages.h"

#include "messageobj.h"
#include "messagelog.h"
#include "messageids.h"

#include "cleartablelockthread.h"

namespace {

//------------------------------------------------------------------------------
// Print command line usage
//------------------------------------------------------------------------------
void usage()
{
	std::cout << "Usage: cleartablelock lockID" << std::endl
		<< " -h to display this menu" << std::endl;
}

//------------------------------------------------------------------------------
// Log initial syslog message
//   tableName   -
//   tableLockID -
//------------------------------------------------------------------------------
void logInitStatus(
	const std::string& tableName,
	uint64_t           tableLockID)
{
	const std::string APPLNAME("cleartablelock command");
	const int SUBSYSTEM_ID = 19; // writeengine?
	const int INIT_MSG_NUM = logging::M0088;
	logging::Message::Args msgArgs;
	logging::Message logMsg( INIT_MSG_NUM );
	msgArgs.add( APPLNAME );
	msgArgs.add( tableName );
	msgArgs.add( tableLockID );
	logMsg.format( msgArgs );
	logging::LoggingID  lid( SUBSYSTEM_ID );
	logging::MessageLog ml( lid );
	ml.logInfoMessage( logMsg );
}

//------------------------------------------------------------------------------
// Log final status syslog message
//   tableName   - Name of table of interest
//   tableLockID - Table lock ID associated with tableName
//   statusMsg   - Error message to be logged; if empty then success is assumed.
//------------------------------------------------------------------------------
void logFinalStatus(
	const std::string& tableName,
	uint64_t           tableLockID,
	const std::string& statusMsg )
{
	const std::string APPLNAME("cleartablelock command");
	const int SUBSYSTEM_ID = 19; // writeengine?
	const int COMP_MSG_NUM = logging::M0089;
	logging::Message::Args msgArgs;
	logging::Message logMsg( COMP_MSG_NUM );
	msgArgs.add( APPLNAME );
	msgArgs.add( tableName );
	msgArgs.add( tableLockID );
	std::string finalStatus;
	if ( statusMsg.empty() )
	{
		finalStatus = "Completed successfully";
	}
	else
	{
		finalStatus  = "Encountered errors: ";
		finalStatus += statusMsg;
	}
	msgArgs.add( finalStatus );
	logMsg.format( msgArgs );
	logging::LoggingID  lid( SUBSYSTEM_ID );
	logging::MessageLog ml( lid );
	ml.logInfoMessage( logMsg );
}

//------------------------------------------------------------------------------
// Construct list of all PMs.
//   pmList      - (output) List of "all" PMs where msgs are to be sent.
//------------------------------------------------------------------------------
int constructPMList( std::vector<uint32_t>& pmList )
{
	try {
		// Get OAM information
		oam::Oam oam;
		oam::ModuleTypeConfig modTypeConfig;
		oam.getSystemConfig("pm", modTypeConfig);

		unsigned int pmCount = modTypeConfig.ModuleCount;
		for (unsigned i=0; i<pmCount; i++)
		{
			std::string devName = modTypeConfig.ModuleNetworkList[i].DeviceName;
			int moduleID = ::atoi(
				devName.substr(oam::MAX_MODULE_TYPE_SIZE,
				oam::MAX_MODULE_ID_SIZE).c_str() );
			pmList.push_back( moduleID );
		}
	}
	catch (std::exception& ex)
	{
		std::cout << "Error getting PM systemConfig. " << ex.what() <<
			std::endl;
		return 4;
	}
	catch (...)
	{
		std::cout << "Error getting PM systemConfig.  Unknown exception." <<
			std::endl;
		return 4;
	}

	return 0;
}

//------------------------------------------------------------------------------
// Construct partial PM list based on specified list of DBRoots.
//   dbRootList - List of applicable DBRoots with PMs to be notified.
//   pmList     - (output) List of PMs to receive msgs, based on dbRootList.
//------------------------------------------------------------------------------
int constructPMList( const std::vector<uint32_t>& dbRootList,
	std::vector<uint32_t>& pmList )
{
	uint32_t dbRoot = 0;
	try {
		// Get OAM information
		oam::Oam oam;
		std::set<uint32_t> pmSet; // used to filter out duplicates
		int pm;

		for (unsigned j=0; j<dbRootList.size(); j++)
		{
			dbRoot = dbRootList[j];
			oam.getDbrootPmConfig( dbRootList[j], pm );
			pmSet.insert( pm );
		}

		// Store unique set of PM IDs into output vector
		for (std::set<uint32_t>::const_iterator iter = pmSet.begin();
			iter != pmSet.end();
			++iter)
		{
			pmList.push_back( *iter );
		}
	}
	catch (std::exception& ex)
	{
		std::cout << "Error mapping DBRoot " << dbRoot << " to a PM.  " <<
			ex.what() << ".  Verify that this DBRoot is in Calpont.xml."<<
			std::endl;
		return 5;
	}
	catch (...)
	{
		std::cout << "Error mapping DBRoot " << dbRoot << " to a PM.  " <<
			"Unknown exception.  Verify that this DBRoot is in Calpont.xml"<<
			std::endl;
		return 5;
	}

	return 0;
}

//------------------------------------------------------------------------------
// Create MessageQueueClient objects
//   pmList      - (input)  List of PMs where msgs are to be sent.
//   msgQueClts  - (output) Vector of created MessageQueueClients.
//   errMsg      - (output) Any applicable error message if nonzero return
//                 code is returned.
//------------------------------------------------------------------------------
int createMsgQueClts(
	const std::vector<uint32_t>& pmList,
	std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> >&
		msgQueClts,
	std::string& errMsg)
{
	errMsg.clear();

	std::string srvName;
	try {
		// Send bulk rollback request to each specified PM.
		for (unsigned k=0; k<pmList.size(); k++)
		{
			std::ostringstream weServerName;
			weServerName << "pm" << pmList[k] << "_WriteEngineServer";
			srvName = weServerName.str();
			//std::cout << "Connecting to " << srvName << std::endl;

			messageqcpp::MessageQueueClient* cl =
				new messageqcpp::MessageQueueClient( srvName );
			msgQueClts.push_back(
				boost::shared_ptr<messageqcpp::MessageQueueClient>(cl) );
		}
	}
	catch (std::exception& ex)
	{
		std::ostringstream ossStatus;
		ossStatus << "Error creating connection to " <<
			srvName << ": " << ex.what();
		errMsg = ossStatus.str();
		return 3;
	}
	catch (...)
	{
		std::ostringstream ossStatus;
		ossStatus << "Error creating connection to " << srvName;
		errMsg = ossStatus.str();
		return 3;
	}

	return 0;
}

//------------------------------------------------------------------------------
// Send Request to each PM and Wait for corresponding Responses
//   msgQueClts  - MessageQueueClients with connections to be used in sending
//                 messages.
//   pmList      - PM list corresponding to msgQueClts.
//   brm         - Handle to use in sending msgs to BRM.
//   tInfo       - Table lock of interest.
//   tblName     - Table name associated with tInfo.
//   msgType     - Message type to be sent to each MessageQueueClient.
//   errMsg      - (output) Any applicable error message if nonzero return
//                 code is returned.
//------------------------------------------------------------------------------
int sendRequest(
	const std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> >&
		msgQueClts,
	const std::vector<uint32_t>& pmList,
	BRM::DBRM* brm,
	const BRM::TableLockInfo& tInfo,
	const std::string& tblName,
	ClearTableLockThread::CLRTBLLOCK_MSGTYPE msgType,
	std::string& errMsg)
{
	unsigned int pmCount = msgQueClts.size();
	boost::thread_group tg;
	std::vector<ClearTableLockStatus*> statusVec;

	// Send request to each PM.
	for (unsigned i=0; i<pmCount; i++)
	{
		int moduleID = pmList[i];

		ClearTableLockStatus* pStatus = new ClearTableLockStatus(moduleID);
		statusVec.push_back( pStatus );

		tg.create_thread( ClearTableLockThread(
			brm, msgQueClts[i].get(), tInfo, tblName, msgType,
			pStatus ));
	}

	// Wait for all the requests to complete
	tg.join_all();

	// Determine the completion status of each request
	int rc = 0;
	std::ostringstream ossStatus;
	for (unsigned i=0; i<statusVec.size(); i++)
	{
		int retStatus = statusVec[i]->retStatus();
		if (retStatus != 0)
		{
			rc = retStatus;

			if (!statusVec[i]->retMsg().empty())
			{
				ossStatus << "From PM" << statusVec[i]->moduleID() <<
					": " << statusVec[i]->retMsg() << ". " << std::endl;
			}
		}
		delete statusVec[i];
	}
	if (!(ossStatus.str().empty()))
		errMsg = ossStatus.str();

	return rc;
}

//------------------------------------------------------------------------------
// We send the bulk rollback request to all the PMs and not just the ones
// referenced in the table lock, because there is always the possiblity that
// 1 or more DBRoots have been moved since the table lock was instantiated;
// in which case a portion of the table could reside on a PM other than the
// one(s) first listed in the table lock.
//   msgQueClts  - MessageQueueClients with connections to be used in sending
//                 rollback message.
//   pmList      - PM list corresponding to msgQueClts.
//   brm         - Handle to use in sending msgs to BRM.
//   tInfo       - Table lock of interest.
//   tblName     - Table name associated with tInfo.
//   rollbackOnly- true -> rollback only; false -> rollback and release lock
//   errMsg      - (output) Any applicable error message if nonzero return
//                 code is returned.
//------------------------------------------------------------------------------
int execBulkRollbackReq(
	const std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> >&
		msgQueClts,
	const std::vector<uint32_t>& pmList,
	BRM::DBRM* brm,
	const BRM::TableLockInfo& tInfo,
	const std::string& tblName,
	bool rollbackOnly,
	std::string& errMsg)
{
	errMsg.clear();

	try {
		if (!rollbackOnly)
		{
			// Take over ownership of stale lock.
			std::string processName("clearTableLock");
			uint32_t processID    = ::getpid();
			int32_t   sessionID    = -1;
			int32_t   transID      = -1;
			bool      ownerChanged = brm->changeOwner(
				tInfo.id, processName, processID, sessionID, transID);
			if (!ownerChanged)
			{
				errMsg = "Unable to grab lock; Lock not found or still in use.";
				return 2;
			}
		}
	}
	catch (std::exception& ex)
	{
		std::ostringstream oss;
		oss << "Error changing table lock owner; " << ex.what();
		errMsg = oss.str();
		return 2;
	}
	catch (...)
	{
		std::ostringstream oss;
		oss << "Unknown error changing table lock owner;";
		errMsg = oss.str();
		return 2;
	}

	// Perform bulk rollback if state is in LOADING state
	int rc = 0;
	if (tInfo.state == BRM::LOADING)
	{
		rc = sendRequest( msgQueClts, pmList,
			brm, tInfo, tblName,
			ClearTableLockThread::CLRTBLLOCK_MSGTYPE_ROLLBACK,
			errMsg );
	}

	return rc;
}

//------------------------------------------------------------------------------
// We send the file cleanup request to all the PMs and not just the ones
// referenced in the table lock, because there is always the possiblity that
// 1 or more DBRoots have been moved since the table lock was instantiated;
// in which case a portion of the table could reside on a PM other than the
// one(s) first listed in the table lock.
//   msgQueClts  - MessageQueueClients with connections to be used in sending
//                 cleanup message.
//   pmList      - PM list corresponding to msgQueClts.
//   brm         - Handle to use in sending msgs to BRM.
//   tInfo       - Table lock of interest.
//   tblName     - Table name associated with tInfo.
//   errMsg      - (output) Any applicable error message if nonzero return
//                 code is returned.
//------------------------------------------------------------------------------
int execFileCleanupReq(
	const std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> >&
		msgQueClts,
	const std::vector<uint32_t>& pmList,
	BRM::DBRM* brm,
	const BRM::TableLockInfo& tInfo,
	const std::string& tblName,
	std::string& errMsg)
{
	errMsg.clear();

	int rc = sendRequest( msgQueClts, pmList,
		brm, tInfo, tblName,
		ClearTableLockThread::CLRTBLLOCK_MSGTYPE_CLEANUP,
		errMsg );

	return rc;
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
	uint32_t tableOID  = 0;

	while ((c = getopt(argc, argv, "hlr:")) != EOF)
	{
		switch (c)
		{
			case 'l':
				clearLockOnly = true;
				break;
//Only allow '-r' option for development/debugging
#if 1
			case 'r': // hidden option to rollback specified table w/o a lock
				rollbackOnly  = true;
				tableOID      = ::strtoull(optarg, 0, 10);
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

	if ((argc - optind) != 1 )
	{
		usage();
		return 1;
	}

	// Get the table lock ID specified by the user
	uint64_t lockID = ::strtoull(argv[optind], 0, 10);

	// If user specified both clearlock and rollback then we need to do both
	if (clearLockOnly && rollbackOnly)
	{
		clearLockOnly = false;
		rollbackOnly  = false;
	}

	//--------------------------------------------------------------------------
	// Verify that BRM is up and in a read/write state
	//--------------------------------------------------------------------------
	BRM::DBRM brm;
	int brmRc = brm.isReadWrite();
	if (brmRc != BRM::ERR_OK)
	{
		std::string brmErrMsg;
		BRM::errString(brmRc, brmErrMsg);
		std::cout << "BRM error: " << brmErrMsg << std::endl;
		std::cout << "Table lock " << lockID << " is not cleared." << std::endl;
		return 1;
	}

	if (brm.getSystemReady() < 1)
	{
		std::cout << "System is not ready.  Verify that InfiniDB "
			"is up and ready before running this program" << std::endl;
		return 1;
	}

	//--------------------------------------------------------------------------
	//@Bug 3711 Check whether the table is locked; does the table lock exist.
	//--------------------------------------------------------------------------
	execplan::CalpontSystemCatalog::TableName tblName;
	BRM::TableLockInfo tInfo;
	std::string task;
	std::vector<uint32_t> pmList;
	int rc;
	const std::string taskSysCat("getting system catalog information");
	try {
		if (rollbackOnly)
		{
			tInfo.id             = lockID;
			tInfo.tableOID       = tableOID;
			tInfo.ownerPID       = 0;
			tInfo.ownerSessionID = 1;
			tInfo.state          = BRM::LOADING;

			// Construct PM list using all PMs, since we don't have a table
			// lock with the list of applicable DBRoots.
			task = "constructing total PM list";
			rc = constructPMList( pmList );
			if (rc != 0)
			{
				return 1;
			}
		}
		else
		{
			task = "getting table lock";
   			bool lockExists = brm.getTableLockInfo( lockID, &tInfo );
   			if (!lockExists)
			{
				std::cout << "Table lock " << lockID << " does not exist." <<
					std::endl;
				return 1;
			}

			// Construct PM list based on list of affected DBRoots
			task = "mapping DBRoots to PMs";
			rc = constructPMList( tInfo.dbrootList, pmList );
			if (rc != 0)
			{
				return 1;
			}
		}

		uint32_t sessionID = 1;
		task = taskSysCat;
		boost::shared_ptr<execplan::CalpontSystemCatalog> systemCatalogPtr =
			execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(
				sessionID );
		systemCatalogPtr->identity(execplan::CalpontSystemCatalog::EC);
		tblName = systemCatalogPtr->tableName( tInfo.tableOID );
	}
	catch (std::exception& ex)
	{
		std::cout << "Error " << task << ".  " << ex.what() << std::endl;
		if (clearLockOnly && (task == taskSysCat))
		{
			tblName.schema= "[unknown name]";
			tblName.table.clear();
			std::cout << "Will still try to clear table lock." << std::endl;
		}
		else
		{
			std::cout << "Table lock " << lockID << " is not cleared." <<
				std::endl;
			return 1;
		}
	}
	catch (...)
	{
		std::cout << "Error " << task << ".  " << std::endl;
		if (clearLockOnly && (task == taskSysCat))
		{
			tblName.schema= "[unknown name]";
			tblName.table.clear();
			std::cout << "Will still try to clear table lock." << std::endl;
		}
		else
		{
			std::cout << "Table lock " << lockID << " is not cleared." <<
				std::endl;
			return 1;
		}
	}

	logInitStatus( tblName.toString(), lockID );
	if (rollbackOnly)
	{
		std::cout << "Rolling back table " << tblName.toString() <<
			std::endl << std::endl;
	}
	else if (clearLockOnly)
	{
		std::cout << "Clearing table lock " << lockID <<
			" for table " << tblName.toString() << std::endl << std::endl;
	}
	else
	{
		std::cout << "Rolling back and clearing table lock for table " <<
			tblName.toString() << "; table lock " << lockID <<
			std::endl << std::endl;
	}

	//--------------------------------------------------------------------------
	// Perform bulk rollback
	//--------------------------------------------------------------------------
	std::string errMsg;
	if (!clearLockOnly)
	{
		std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> >
			msgQueClts;
		rc = createMsgQueClts(pmList, msgQueClts, errMsg);
		if (rc != 0)
		{
			logFinalStatus( tblName.toString(), lockID, errMsg );
			std::cout << errMsg << std::endl;
			std::cout << "Table lock " << lockID << " for table " <<
				tblName.toString() << " is not cleared." << std::endl;
			return rc;
		}

		rc = execBulkRollbackReq( msgQueClts, pmList,
			&brm, tInfo, tblName.toString(), rollbackOnly, errMsg );
		if (rc != 0)
		{
			logFinalStatus( tblName.toString(), lockID, errMsg );
			std::cout << "Rollback error: " << errMsg << std::endl;
			std::cout << "Table lock " << lockID << " for table " <<
				tblName.toString() << " is not cleared." << std::endl;
			return rc;
		}

		//----------------------------------------------------------------------
		// Change the state of the table lock to cleanup state.
		// We ignore the return stateChange flag.
		//----------------------------------------------------------------------
		if (!rollbackOnly)
		{
			try {
				brm.changeState( tInfo.id, BRM::CLEANUP );
			}
			catch (std::exception& ex)
			{
				std::ostringstream oss;
				oss << "Error changing state.  " << ex.what();
				logFinalStatus( tblName.toString(), lockID, oss.str() );
				std::cout << oss.str() << std::endl;
				std::cout << "Table lock " << lockID << " is not cleared." <<
					std::endl;
				return 1;
			}
			catch (...)
			{
				std::ostringstream oss;
				oss << "Error changing state.  " << std::endl;
				logFinalStatus( tblName.toString(), lockID, oss.str() );
				std::cout << oss.str() << std::endl;
				std::cout << "Table lock " << lockID << " is not cleared." <<
					std::endl;
				return 1;
			}
		}

		//----------------------------------------------------------------------
		// Delete the meta data files
		//----------------------------------------------------------------------
		rc = execFileCleanupReq( msgQueClts, pmList,
			&brm, tInfo, tblName.toString(), errMsg );
		if (rc != 0)
		{
			//@Bug 4517. Release tablelock if remove meta files failed
			const std::string APPLNAME("cleartablelock command");
			const int SUBSYSTEM_ID = 19; // writeengine?
			const int COMP_MSG_NUM = logging::M0089;
			logging::Message::Args msgArgs;
			logging::Message logMsg( COMP_MSG_NUM );
			msgArgs.add( APPLNAME );
			msgArgs.add( tblName.toString() );
			msgArgs.add( lockID );
			msgArgs.add( errMsg );
			logMsg.format( msgArgs );
			logging::LoggingID  lid( SUBSYSTEM_ID );
			logging::MessageLog ml( lid );
			ml.logWarningMessage( logMsg );
			
			std::cout << "File cleanup error: " << errMsg << std::endl;
		}
	} // end of: if (!clearLockOnly)

	//--------------------------------------------------------------------------
	// Finally, release the actual table lock
	//--------------------------------------------------------------------------
	std::cout << std::endl;
	if (rollbackOnly)
	{
		logFinalStatus( tblName.toString(), lockID, std::string() );
		std::cout << "Bulk Rollback Only for table " << tblName.toString() <<
			" completed successfully." << std::endl;
	}
	else
	{
		try {
			brm.releaseTableLock( lockID ); // ignore boolean return value
		}
		catch (std::exception& ex)
		{
			logFinalStatus( tblName.toString(), lockID, ex.what() );
			std::cout << "Error releasing table lock " << lockID <<
				" for table " << tblName.toString() << std::endl;
			std::cout << ex.what() << std::endl;
			return 1;
		}
		catch (...)
		{
			std::string unknownErr("Unknown exception");
			logFinalStatus( tblName.toString(), lockID, unknownErr );
			std::cout << "Error releasing table lock " << lockID <<
				" for table " << tblName.toString() << std::endl;
			std::cout << unknownErr << std::endl;
			return 1;
		}

		logFinalStatus( tblName.toString(), lockID, std::string() );
		std::cout << "Table lock " << lockID << " for table " <<
			tblName.toString() << " is cleared." << std::endl;
	}
	
	return 0;
}
