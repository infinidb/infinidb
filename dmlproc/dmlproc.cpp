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
*   $Id: dmlproc.cpp 841 2012-09-06 19:55:25Z chao $
*
*
***********************************************************************/

#include <unistd.h>
#include <signal.h>
#include <string>
#include <set>
#include <clocale>
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/progress.hpp"
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include "dmlproc.h"
#include "dmlprocessor.h"
using namespace dmlprocessor;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;

#include "configcpp.h"
using namespace config;

#include "sessionmanager.h"
using namespace execplan;

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "liboamcpp.h"
#include "messagelog.h"
using namespace oam;

#include "writeengine.h"

#undef READ //someone has polluted the namespace!
#include "vss.h"
#include "brmtypes.h"
using namespace BRM;

#include "bytestream.h"
using namespace messageqcpp;

#include "cacheutils.h"
namespace fs = boost::filesystem;

namespace
{

int toInt(const string& val)
{
        if (val.length() == 0)
                return -1;
        return static_cast<int>(Config::fromText(val));
}
class CleanUpThread
{
		public:
		CleanUpThread() {}
		~CleanUpThread() {}
		void operator()()
		{
		//Find the ddl log files under DBRMRoot directory
		logging::Message::Args args;
		logging::Message message(2);
		args.add("DDLProc starts cleaning up");
		message.format( args );
		logging::LoggingID lid(20);
		logging::MessageLog ml(lid);
		ml.logInfoMessage( message );
		
		string prefix,ddlLogDir;
		config::Config *config = config::Config::makeConfig();
		prefix = config->getConfig("SystemConfig", "DBRMRoot");
		if (prefix.length() == 0) {
			//cerr << "Need a valid DBRMRoot entry in Calpont configuation file";
		}
		uint64_t pos =  prefix.find_last_of ("/");
		if ( pos != string::npos )
		{
			ddlLogDir = prefix.substr(0, pos+1); //Get the file path
		}
		else
		{
			//cerr << "Cannot find the dbrm directory for the DDL log file";

		}
	
		boost::filesystem::path filePath;
		filePath = fs::system_complete( fs::path( ddlLogDir ) );
		if ( !fs::exists( filePath ) )
		{
			//cerr << "\nNot found: " << filePath.string() << std::endl;
		}

		std::vector<string> fileNames;
	
		if ( fs::is_directory( filePath ) )
		{
			fs::directory_iterator end_iter;
			for ( fs::directory_iterator dir_itr( filePath ); dir_itr != end_iter; ++dir_itr )
			{
				try
				{			
					if ( !fs::is_directory( *dir_itr ) )
					{
						fileNames.push_back ( dir_itr->string() );
					}
				}
				catch (std::exception&)
				{
					//cerr << dir_itr->string() << " " << ex.what() << std::endl;
				}
			}
		}

		std::vector<execplan::CalpontSystemCatalog::OID> oidList;
		std::ifstream	       ddlLogFile;
		execplan::CalpontSystemCatalog::OID fileoid;
		string tableName;
		std::vector<string> ddlFileNames;
		DDLPackageProcessor ddlpackageprocessor;
		BRM::DBRM dbrm;	
		bool error = false;
		WriteEngine::WriteEngineWrapper writeEngine;
		for ( unsigned i=0; i < fileNames.size(); i++ )
		{
			pos =  fileNames[i].find ("DDLLog_") ;
			if ( pos != string::npos )
			{	
				pos = fileNames[i].find_last_of( "_");
				string tableOidStr = fileNames[i].substr(pos+1, fileNames[i].length() - pos);
				//Read the file to get oids
				ostringstream oss;
				oss << "DDLProc will clean up(drop table) table OId = " << tableOidStr;
				logging::Message::Args args1;
				logging::Message message1(2);
				args1.add(oss.str());
				message1.format( args1 );
				ml.logInfoMessage( message1 );
				//cout << "Found file " << fileNames[i] << endl;
				ddlFileNames.push_back( fileNames[i] );
				ddlLogFile.open ( fileNames[i].c_str(), ios::in);
				if ( !ddlLogFile )
				{
					ostringstream oss;
					oss << "DDLProc cannot open DDL log file " << fileNames[i] << " and the table with oid " << tableOidStr << " is not cleaned";
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logWarningMessage( message1 );
					continue;
				}
				std::vector<execplan::CalpontSystemCatalog::OID> oidPartList;			
				while ( ddlLogFile >> fileoid )
					oidList.push_back( fileoid );
				
				ddlLogFile.close();
			
			}
			else //Find drop partition log file
			{
				pos =  fileNames[i].find ("DDL_DROPPARTITION_Log_") ;
				if ( pos != string::npos )
				{
					//cout << "Find partition file " << fileNames[i] << endl;
					char *ep = NULL;
					pos = fileNames[i].find_last_of( "_");
					string partitionStr = fileNames[i].substr(pos+1, fileNames[i].length() - pos);
					uint32_t partition = strtoll(partitionStr.c_str(), &ep, 10);
					string tableOidStr = fileNames[i].substr(0, pos);
					pos = tableOidStr.find_last_of( "_");
					tableOidStr = tableOidStr.substr(pos+1, tableOidStr.length() - pos);
					
					ostringstream oss;
					oss << "DDLProc will clean up(drop partition) table OId = " << tableOidStr << " and partition " << partition;
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logInfoMessage( message1 );
					//cout << "The partition number is " << partition << endl;
					ddlLogFile.open ( fileNames[i].c_str(), ios::in);
					if ( !ddlLogFile )
					{
						ostringstream oss;
						oss << "DDLProc cannot open DDL log file " << fileNames[i] << " and the table with oid " << tableOidStr << " is not cleaned";
						logging::Message::Args args1;
						logging::Message message1(2);
						args1.add(oss.str());
						message1.format( args1 );
						ml.logErrorMessage( message1 );
						continue;
					}
					std::vector<execplan::CalpontSystemCatalog::OID> oidPartList;		
					while ( ddlLogFile >> fileoid )
						oidPartList.push_back( fileoid );
					
					int rc = dbrm.markPartitionForDeletion( oidPartList, partition);
					if (( rc != 0 ) && ( rc !=BRM::ERR_PARTITION_DISABLED ))
					{
						string errMsg;
						BRM::errString(rc, errMsg);
						ostringstream oss;
						oss << "DDLProc didn't clean up files for table with oid " << tableOidStr << " due to "<< errMsg;
						logging::Message::Args args1;
						logging::Message message1(2);
						args1.add(oss.str());
						message1.format( args1 );
						ml.logErrorMessage( message1 );
						continue;
					}
					ddlpackageprocessor.removePartitionFiles( oidPartList, partition );	
					cacheutils::flushPartition( oidPartList, partition );				
					rc = dbrm.deletePartition( oidPartList, partition);
					if (rc == 0)
					{
						unlink ( fileNames[i].c_str() );	
						ostringstream oss;
						oss << "DDLProc clean up files for table with oid " << tableOidStr;
						logging::Message::Args args1;
						logging::Message message1(2);
						args1.add(oss.str());
						message1.format( args1 );
						ml.logInfoMessage( message1 );
					}
					else
					{
						string errMsg;
						BRM::errString(rc, errMsg);
						ostringstream oss;
						oss << "DDLProc didn't clean up files for table with oid " << tableOidStr << " due to "<< errMsg;
						logging::Message::Args args1;
						logging::Message message1(2);
						args1.add(oss.str());
						message1.format( args1 );
						ml.logErrorMessage( message1 );
						continue;
					}
				}
				else //Find truncate table log file
				{
					pos =  fileNames[i].find ("DDL_TRUNCATETABLE_Log_") ;
					if ( pos != string::npos )
					{
						char *ep = NULL;
						pos = fileNames[i].find_last_of( "_");
						string tableOidStr = fileNames[i].substr(pos+1, fileNames[i].length() - pos);
						uint32_t tableOid = strtoll(tableOidStr.c_str(), &ep, 10);					
						ostringstream oss;
						oss << "DDLProc will clean up(truncate) table OId = " << tableOidStr;
						logging::Message::Args args1;
						logging::Message message1(2);
						args1.add(oss.str());
						message1.format( args1 );
						ml.logInfoMessage( message1 );
						//Read the file to get oids
						ddlLogFile.open ( fileNames[i].c_str(), ios::in);
						if ( !ddlLogFile )
						{
							ostringstream oss;
							oss << "DDLProc cannot open DDL log file " << fileNames[i] << " and the table with oid " << tableOidStr << " is not cleaned";
							logging::Message::Args args1;
							logging::Message message1(2);
							args1.add(oss.str());
							message1.format( args1 );
							ml.logErrorMessage( message1 );
							continue;
						}
						std::vector<execplan::CalpontSystemCatalog::OID> truncateoidList;			
						while ( ddlLogFile >> fileoid )
							truncateoidList.push_back( fileoid );
						//Get a table lock if it not locked.Remove those files and create a new set of files, release the table		
						//Check table lock
						uint32_t processID = ::getpid();
						string processName = "DDLProc";
						int rc = dbrm.updateTableLock( tableOid, processID, processName) ;
						if (processName == "DDLProc")
							rc = 0;
						if ( rc != 0 ) //table is locked by an active process, cannot happen!
						{
							ostringstream oss;
							logging::Message::Args args;
							logging::Message message(2);
							args.add("DDLProc is not going to clean the table with oid = ");
							oss << tableOid << " due to table is still locked by active process \"" << processName << "\" with process ID " << processID;
							args.add( oss.str() );
							message.format(args);
							ml.logErrorMessage( message);
							continue;
						}
				
						//We should hold a valid table lock now
						rc = dbrm.markAllPartitionForDeletion( truncateoidList);
						if (rc != 0) //Log a message to err.log
						{
							string errMsg;
							BRM::errString(rc, errMsg);
							ostringstream oss;
							oss << "DDLProc didn't clean up files for table with oid " << tableOidStr << " due to "<< errMsg;
							logging::Message::Args args1;
							logging::Message message1(2);
							args1.add(oss.str());
							message1.format( args1 );
							ml.logErrorMessage( message1 );
							continue;
						}
						execplan::CalpontSystemCatalog::SCN txnID = 1;
						try {
							ddlpackageprocessor.removeFiles(txnID, truncateoidList);
							rc = cacheutils::flushOIDsFromCache(truncateoidList);	
							ddlpackageprocessor::DDLPackageProcessor::DDLResult result;
							ddlpackageprocessor.removeExtents( txnID, result, truncateoidList );
						}
						catch (std::exception& ex) 
						{
							logging::Message::Args args1;
							logging::Message message1(2);
							ostringstream oss;
							oss << "DDLProc didn't clean up files for table with oid " << tableOidStr << " due to " << ex.what();
							args1.add(oss.str());
							message1.format( args1 );
							ml.logErrorMessage( message1 );
							continue;
						}
						//create a new sets of files. Just find a dbroot according th the number of tables in syscat-1.
						CalpontSystemCatalog* systemCatalogPtr =
							CalpontSystemCatalog::makeCalpontSystemCatalog(txnID);
						int tableCount = systemCatalogPtr->getTableCount();
						int dbRootCnt = 1;
						int useDBRoot = 1;
						string DBRootCount = config::Config::makeConfig()->getConfig("SystemConfig", "DBRootCount");
						if (DBRootCount.length() != 0)
							dbRootCnt = static_cast<int>(config::Config::fromText(DBRootCount));

						useDBRoot =  ((tableCount-1) % dbRootCnt) + 1;
						
						//Create all column and dictionary files
						CalpontSystemCatalog::TableName aTableName = systemCatalogPtr->tableName(tableOid);
						CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(aTableName, true);
						
						for (unsigned col  = 0; col < ridList.size(); col++)
						{
							if ( rc != 0)
								break;
								
							CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(ridList[col].objnum);
							if (colType.ddn.dictOID > 3000)
							{
								rc = writeEngine.createDctnry(txnID, ridList[col].objnum, colType.colWidth, useDBRoot, 0, 0, colType.compressionType );	
							}
							else
							{
								rc = writeEngine.createColumn(txnID, ridList[col].objnum, static_cast<WriteEngine::ColDataType>(colType.colDataType), colType.colWidth, useDBRoot, 0, colType.compressionType);
							}
						}
						if ( rc != 0 ) //Log a message, don't remove log file, and keep table lock
						{
							logging::Message::Args args1;
							logging::Message message1(2);
							WriteEngine::WErrorCodes   ec;
							ostringstream oss;
							oss << "DDLProc didn't clean up files for table with oid " << tableOidStr << " due to " << ec.errorString(rc);
							args1.add(oss.str());
							message1.format( args1 );
							ml.logErrorMessage( message1 );
							continue;
						}
						ddlLogFile.close();
						unlink ( fileNames[i].c_str() );
						//unlock the table
						rc = dbrm.setTableLock( tableOid, txnID, processID, processName, false );
						logging::Message::Args args2;
						logging::Message message2(2);
						WriteEngine::WErrorCodes   ec;
						ostringstream oss2;
						oss2 << "DDLProc clean up files for table with oid " << tableOidStr;
						args2.add(oss2.str());
						message2.format( args2 );
						ml.logInfoMessage( message2 );
					}
				}
			}

		}
		
		execplan::CalpontSystemCatalog::SCN txnID = -1;
		DDLPackageProcessor::DDLResult result;
        result.result = DDLPackageProcessor::NO_ERROR; 
	
		try {
			ddlpackageprocessor.flushPrimprocCache ( oidList );
		}
		catch ( ... )
		{
		}
		
		try {
			ddlpackageprocessor.removeExtents( txnID, result, oidList );
		}
		catch ( ... )
		{
		}
		
		string errMsg;
		try {
			ddlpackageprocessor.removeFiles( txnID, oidList );
		}
		catch (std::exception& ex)
		{
			error = true;
			errMsg = ex.what();
		}
		
		if ( !error )
		{
			for ( unsigned i = 0; i < ddlFileNames.size(); i++ )
			{
				unlink ( ddlFileNames[i].c_str() );
				ostringstream oss;
				oss << "DDLProc cleaned up log file " << ddlFileNames[i];
				logging::Message::Args args1;
				logging::Message message1(2);
				args1.add(oss.str());
				message1.format( args1 );
				ml.logInfoMessage( message1 );
			}

			//Return oids		
			execplan::ObjectIDManager fObjectIDManager;
			for ( unsigned i = 0; i < oidList.size(); i++ )
			{
				if ( oidList[i] < 3000 )
					continue;
    	    
				fObjectIDManager.returnOID(oidList[i]);
			}
		}
		else
		{
			for ( unsigned i = 0; i < ddlFileNames.size(); i++ )
			{
				ostringstream oss;
				oss << "DDLProc didn't cleaned up log file " << ddlFileNames[i] << " due to " << errMsg;
				logging::Message::Args args1;
				logging::Message message1(2);
				args1.add(oss.str());
				message1.format( args1 );
				ml.logErrorMessage( message1 );
			}
		}
		
		logging::Message::Args args3;
		logging::Message message3(2);
		args3.add("DDLProc finished cleaning up");
		message3.format( args3 );
		ml.logInfoMessage( message3 );
	 }
 };

// This function rolls back any active transactions in case of an abnormal shutdown.
void rollbackAll() {
	//Log a message in info.log
	logging::Message::Args args;
    logging::Message message(2);
    args.add("DMLProc starts rollbackAll transactions.");
    message.format( args );
	logging::LoggingID lid(20);
    logging::MessageLog ml(lid);
    ml.logInfoMessage( message );

	set<VER_t> txnList;
	set<VER_t>::const_iterator curTxnID;
	WriteEngine::WriteEngineWrapper writeEngine;
	const BRM::SIDTIDEntry* activeTxns;
	BRM::TxnID txnID;
	SessionManager sessionManager;
	int len;
	DBRM dbrm;
	int rc = 0;
	rc = dbrm.isReadWrite();
	if (rc != 0 )
		throw std::runtime_error("Rollback will be deffered due to DBRM is in read only state.");
	dbrm.systemIsReady(SessionManagerServer::SS_NOT_READY);	
	//@Bug 2554, 4176 Start clean up thread
	thread_group tg;
	tg.create_thread(CleanUpThread());
	// Get the set of current transactions from the VSS and roll them back.  We use the VSS for
	// the active transactions instead of the SessionManager list of sessions and transactions
	// because the VSS is persisted.
	
		
	rc = dbrm.getCurrentTxnIDs(txnList);

	if(rc != 0)
	{
		throw std::runtime_error(" cannot obtain transaction information ");
	}
	
	if(txnList.size() > 0) {
		ostringstream oss;
		oss << "DMLProc will rollback " << txnList.size() << " transactions.";
		cout << oss.str() << endl;
		logging::Message::Args args2;
		logging::Message message2(2);
		args2.add(oss.str());
		message2.format( args2 );
		ml.logInfoMessage( message2 );
		
		for(curTxnID = txnList.begin(); curTxnID != txnList.end(); ++curTxnID) {
			//@Bug 2299 check write engine error.
			vector<LBID_t> lbidList;
			dbrm.getUncommittedExtentLBIDs(static_cast<VER_t>(*curTxnID), lbidList);
			vector<LBID_t>::const_iterator iter = lbidList.begin();
			vector<LBID_t>::const_iterator end = lbidList.end();
			while (iter != end)
			{
				dbrm.setExtentMaxMin(*iter, numeric_limits<int64_t>::min()+1, numeric_limits<int64_t>::max()-1, -1);
				++iter;
			}
			rc = writeEngine.rollbackTran(*curTxnID, 0);
			if ( rc == 0 )
				logging::logCommand(0, *curTxnID, "ROLLBACK;");
			else
			{
				ostringstream oss;
				WriteEngine::WErrorCodes   ec;
				oss << " problem with rollback. " << ec.errorString(rc);
				rc = dbrm.setReadOnly(true);
				throw std::runtime_error(oss.str());	
			}
		}
		cout << "DMLProc rolledback " << txnList.size() << " transactions." << endl;
	}	
	
	// Clear out the session manager session list of sessions / transactions.
	
	activeTxns = sessionManager.SIDTIDMap(len);
	
	//@Bug 1627 Don't start DMLProc if either controller or worker node is not up
	if ( activeTxns == NULL )
	{
		std::string err = "DBRM problem is encountered.";
        throw std::runtime_error(err);	
	}	
		
	for(int i=0; i < len; i++) {
		txnID = activeTxns[i].txnid;
		sessionManager.rolledback(txnID);
	}

	// Clear out the DBRM.
	
	dbrm.clear();

	//Flush the cache
	cacheutils::flushPrimProcCache();
	
	//Log a message in info.log
	logging::Message::Args args1;
    logging::Message message1(2);
    args1.add("DMLProc finished rollbackAll transactions.");
    message1.format( args1 );
    ml.logInfoMessage( message1 );

	dbrm.systemIsReady(SessionManagerServer::SS_READY);
}
	
// This function rolls back bulkload in case of an abnormal shutdown.
void bulkRollback ()
{
		//Log a message in info.log
		logging::Message::Args args;
        logging::Message message(2);
        args.add("DMLProc starts bulk roll back.");
        message.format( args );
		logging::LoggingID lid(20);
        logging::MessageLog ml(lid);

        ml.logInfoMessage( message );
		
		//Find the BulkRollbackDir directory
		string bulkRollbackDir;
		config::Config *config = config::Config::makeConfig();
		bulkRollbackDir = config->getConfig("WriteEngine", "BulkRollbackDir" );
		if (bulkRollbackDir.length() == 0) {
			//Get the default path
			bulkRollbackDir.assign( WriteEngine::Config::getBulkRoot() );
            bulkRollbackDir += "/rollback";
		}
	
		boost::filesystem::path filePath; 
		filePath = fs::system_complete( fs::path( bulkRollbackDir ) );

		if ( !fs::exists( filePath ) )
		{
			logging::Message::Args args3;
			logging::Message message3(2);
			args3.add("Bulk roll back directory is not found. No bulk rollback performed");
			message3.format( args3 );
			ml.logInfoMessage( message3 );
			return;
		}
		//Get all table oids
		std::vector<execplan::CalpontSystemCatalog::OID> tableOids;
		char *ep = NULL;
		string fileName;
		if ( fs::is_directory( filePath ) )
		{
			fs::directory_iterator end_iter;
			for ( fs::directory_iterator dir_itr( filePath ); dir_itr != end_iter; ++dir_itr )
			{
				try
				{			
					if ( !fs::is_directory( *dir_itr ) )
					{
						//fileName = (dir_itr->string()).substr( bulkRollbackDir.length()+1, (dir_itr->string()).length() - bulkRollbackDir.length() ) ;
						fileName = dir_itr->leaf();
						errno = 0;
						const char *str = fileName.c_str();
						int64_t val = strtoll(fileName.c_str(), &ep, 10 );

						//  (no digits) || (more chars)  || (other errors & value = 0)
						if ((ep == str) || (*ep != '\0') || (errno != 0 && val == 0))
							continue;

						if (errno == ERANGE && (val == LLONG_MAX || val == LLONG_MIN))
							continue;
						
						tableOids.push_back ( val );
					}
				}
				catch ( const std::exception & ex )
				{
					cerr << dir_itr->string() << " " << ex.what() << std::endl;					
				}
			}
		}
		
		// Set table lock for those oids
		DBRM dbrm;
		ostringstream oss;
		int rc = 0;
		std::vector<execplan::CalpontSystemCatalog::OID> validTableOids;
		if ( tableOids.size() > 0 )
		{
			logging::Message::Args args4;
			logging::Message message4(2);
			args4.add("Bulk rollback is going to rollback the following table oids:");
			for ( unsigned i=0; i < tableOids.size(); i++ )
			{
				u_int32_t  processID = ::getpid();
				u_int32_t  oldProcessID = processID;
				std::string  processName = "DMLProc";
				rc = dbrm.updateTableLock( tableOids[i], processID, processName) ;
				if ( rc != 0 )
				{
					//Log a info message to indicate the table is still locked by other active process
					ostringstream oss1;
					logging::Message::Args args8;
					logging::Message message8(2);
					args8.add("Bulk rollback is not going to rollback table with oid = ");
					oss1 << tableOids[i] << " due to table is still locked by active process \"" << processName << "\" with process ID " << processID;
					args8.add( oss1.str() );
					message8.format( args8 );
					ml.logInfoMessage( message8);
				}
				else
				{
					//@bug 3884
					if (processID == oldProcessID)
						oss << tableOids[i] << " , table lock is updated and held by DMLProc.";
					else
						oss << tableOids[i] << " , table lock is updated and held by DMLProc. Table lock was held previously by \"" << processName << "\" with process ID " << processID << "; " << endl;

					validTableOids.push_back(tableOids[i]);
				}
			}
			args4.add( oss.str() );
			message4.format( args4 );
			ml.logInfoMessage( message4 );
		}
		
		if ( validTableOids.size() <= 0 )
		{
			logging::Message::Args args4;
			logging::Message message4(2);
			args4.add("No table need rollback");
			message4.format( args4 );
			ml.logInfoMessage( message4 );
		}
		//Perform bulk roll back
		CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(1);
		systemCatalogPtr->identity(CalpontSystemCatalog::EC);
		CalpontSystemCatalog::TableName tableName;
		std::string errorMsg;	
		WriteEngine::WriteEngineWrapper writeEngine;

		for ( unsigned i=0; i < validTableOids.size(); i++ )
		{
			cout << "DMLProc will rollback " << validTableOids.size() << " tables." << endl;
			//Check whether the table is still exist
			try {
				const std::string APPLNAME("DMLProc");
				tableName = systemCatalogPtr->tableName( validTableOids[i]);
				rc = writeEngine.bulkRollback( validTableOids[i],
					tableName.toString(),
					APPLNAME, false, false, errorMsg );
				if ( rc != WriteEngine::NO_ERROR )
				{
					//Log an error
					logging::Message::Args args5;
					logging::Message message5(2);
					args5.add("Error occured in write engine bulkrollback: ");
					args5.add ( errorMsg );
					message5.format( args5 );
					ml.logInfoMessage( message5 );
				}
			}
			catch ( logging::IDBExcept& exp )
			{
				//Just clean the file and clear the table lock				
				logging::Message::Args args1;
				logging::Message message1(2);
				args1.add(exp.what());
				args1.add ( "No bulk rollback performed. The table lock is cleared and bulkrollback file is removed." );
				message1.format( args1 );
				ml.logInfoMessage( message1 );
				rc = writeEngine.clearLockOnly( tableOids[i] );
				
				std::ostringstream oss;
				oss << "/" << tableOids[i];
				string metaFileName ( bulkRollbackDir + oss.str() );
				unlink( metaFileName.c_str() );
			}
			cout << "DMLProc rolledback " << validTableOids.size() << " tables." << endl;
		}
		logging::Message::Args args6;
        logging::Message message6(2);
		args6.add("DMLProc finished bulk roll back.");
        message6.format( args6 );

        ml.logInfoMessage( message6 );
		
}
	
    void setupCwd()
    {
        string workdir = Config::makeConfig()->getConfig("SystemConfig", "WorkingDir");
        if (workdir.length() == 0)
            workdir = ".";
        (void)chdir(workdir.c_str());
        if (access(".", W_OK) != 0)
            (void)chdir("/tmp");
    }
}

int main(int argc, char* argv[])
{
    // get and set locale language
	string systemLang = "C";

    Oam oam;
    try{
        oam.getSystemConfig("SystemLang", systemLang);
    }
    catch(...)
    {
		systemLang = "C";
	}

    setlocale(LC_ALL, systemLang.c_str());

    printf ("Locale is : %s\n", systemLang.c_str() );

	//BUG 2991
	setlocale(LC_NUMERIC, "C");

    Config* cf = Config::makeConfig();

    setupCwd();

    WriteEngine::WriteEngineWrapper::init( WriteEngine::SUBSYSTEM_ID_DMLPROC );
	
	//set BUSY_INIT state
    {
        try
        {
            oam.processInitComplete("DMLProc", oam::BUSY_INIT);
        }
        catch (...)
        {
        }
    }
	
	//@Bug 2656
	try {
		bulkRollback(); //Rollback any cpimport job
	}
	catch ( ...)
	{
	
	}
	
	//@Bug 1627
	try {
    	rollbackAll(); // Rollback any 
	}
	catch ( std::exception &e )
	{
		//@Bug 2299 Set DMLProc process to fail and log a message 
		try
		{
			oam.processInitFailure();
		}
		catch (...)
        {
        }
		logging::Message::Args args;
        logging::Message message(2);
        args.add("DMLProc failed to start due to :");
		args.add(e.what());
        message.format( args );
		logging::LoggingID lid(20);
        logging::MessageLog ml(lid);

        ml.logCriticalMessage( message );
		
		cerr << "DMLProc failed to start due to : " << e.what() << endl;
		return 1;
	}

	int temp;
	int serverThreads = 100;
	int serverQueueSize = 200;
	const string DMLProc("DMLProc");

	temp = toInt(cf->getConfig(DMLProc, "ServerThreads"));
	if (temp > 0)
			serverThreads = temp;

	temp = toInt(cf->getConfig(DMLProc, "ServerQueueSize"));
	if (temp > 0)
			serverQueueSize = temp;


    DMLServer dmlserver(serverThreads, serverQueueSize);

	//set ACTIVE state
    {
        Oam oam;
        try
        {
            oam.processInitComplete("DMLProc");
        }
        catch (...)
        {
        }
    }
#ifndef _MSC_VER
	//@Bug 1851 Register the signal handler
	struct sigaction ign;
	memset(&ign, 0, sizeof(ign));
	ign.sa_handler = SIG_IGN;
	sigaction(SIGHUP, &ign, 0);
	sigaction(SIGPIPE, &ign, 0);
#endif

    dmlserver.start();

    return 1;
}
// vim:ts=4 sw=4:

