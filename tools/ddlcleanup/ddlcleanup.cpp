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
* $Id: ddlcleanup.cpp 967 2009-10-15 13:57:29Z rdempsey $
*/

#include <iostream>
#include <fstream>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <unistd.h>
#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/progress.hpp"
using namespace std;

#include "rwlock.h"

#include "blocksize.h"
#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
using namespace execplan;

#include "exceptclasses.h"
using namespace logging;

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
using namespace oam;

#include "ddlpackageprocessor.h"
using namespace ddlpackageprocessor;
#include "cacheutils.h"

namespace fs = boost::filesystem;

namespace {


void usage()
{
	cout << "Usage: ddlcleanup" << endl;
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
		cerr << "Exiting, ddlcleanup can only be run on the Active "
			"Parent OAM Module" << endl;
		return 1;
	}
	
	string prefix,ddlLogDir;
	config::Config *config = config::Config::makeConfig();
	prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		cerr << "Need a valid DBRMRoot entry in Calpont configuation file";
		return 1;
	}
	uint64_t pos =  prefix.find_last_of ("/");
	if ( pos != string::npos )
	{
		ddlLogDir = prefix.substr(0, pos+1); //Get the file path
	}
	else
	{
		cerr << "Cannot find the dbrm directory for the DDL log file";
		return 1;
	}
	
	boost::filesystem::path filePath;
	filePath = fs::system_complete( fs::path( ddlLogDir ) );
	if ( !fs::exists( filePath ) )
	{
		cerr << "\nNot found: " << filePath.string() << std::endl;
		return 1;
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
	logging::LoggingID lid(15);
	logging::MessageLog ml(lid);
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
				ml.logErrorMessage( message1 );
				cerr << oss.str();
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
				cout << oss.str();
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
					cerr << oss.str();
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
					cerr << oss.str();
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
					cout << oss.str();
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
					cerr << oss.str();
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
					cout << oss.str();
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
						cerr << oss.str();
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
						cerr << oss.str();
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
						cerr << oss.str();
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
						cerr << oss.str();
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
						cerr << oss.str();
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
					cout << oss2.str();
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
			cout << oss.str();
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
			cerr << oss.str();
		}
	}
		
	logging::Message::Args args3;
	logging::Message message3(2);
	args3.add("DDLProc finished cleaning up");
	message3.format( args3 );
	ml.logInfoMessage( message3 );
	cout << "DDLProc finished cleaning up" << endl;;	
	return 0;
}

// vim:ts=4 sw=4:

