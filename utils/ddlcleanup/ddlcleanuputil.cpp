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
* $Id: ddlcleanuputil.cpp 3904 2013-06-18 12:22:51Z rdempsey $
*/

#include <unistd.h>
#include <iostream>
#include <fstream>
#include <vector>
#include <map>
#include <cassert>
#include <stdexcept>
#include <sstream>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

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
using namespace cacheutils;

#include "dbrm.h"
using namespace BRM;

#include "messageobj.h"
#include "messagelog.h"
#include "configcpp.h"
using namespace logging;

namespace ddlcleanuputil
{

int ddl_cleanup()
{
	scoped_ptr<DBRM> dbrmp(new DBRM());
	DDLPackageProcessor ddlpackageprocessor(dbrmp.get());
	DDLPackageProcessor::TableLogInfo tableLogs;
	int rc = 0;
	uint64_t uniqueId = dbrmp->getUnique64();
	logging::LoggingID lid(20);     // This is running in the DMLProc space, so we use DML's lid
	logging::MessageLog ml(lid);
	//Get the logs information back first.
	try {
		ddlpackageprocessor.fetchLogFile(tableLogs, uniqueId);
	}
	catch (runtime_error& ex)
	{
		//Log to err.log
		ostringstream oss;
		oss << "DDLProc cannot get clean up information from DDL log files due to " << ex.what();
		logging::Message::Args args1;
		logging::Message message1(2);
		args1.add(oss.str());
		message1.format( args1 );
		ml.logErrorMessage( message1 );
	}	

	DDLPackageProcessor::TableLogInfo::const_iterator it ;
	for (it = tableLogs.begin(); it != tableLogs.end(); it++)
	{
		ostringstream oss;
		oss << "DDLCleanup is cleaning table with oid " << it->first;
		logging::Message::Args args1;
		logging::Message message1(2);
		args1.add(oss.str());
		message1.format( args1 );
		ml.logInfoMessage( message1 );
		DDLPackageProcessor::LogInfo aLogInfo = it->second;
		switch (aLogInfo.fileType)
		{
			case DDLPackageProcessor::DROPTABLE_LOG:
			{
				try {
					ddlpackageprocessor.flushPrimprocCache ( aLogInfo.oids );
					ddlpackageprocessor.removeExtents( aLogInfo.oids );
					ddlpackageprocessor.removeFiles( uniqueId, aLogInfo.oids );
					//delete log file
					ddlpackageprocessor.deleteLogFile(DDLPackageProcessor::DROPTABLE_LOG, it->first, uniqueId);
					ostringstream oss;
					oss << "DDLProc has cleaned up drop table left over for table " << it->first;
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logInfoMessage( message1 );
				}
				catch (runtime_error& ex)
				{
					ostringstream oss;
					oss << "DDLProc cannot clean up drop table for table " << it->first << " due to " << ex.what();
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logErrorMessage( message1 );
				}
				break;
			}
			case DDLPackageProcessor::DROPPART_LOG:
			{
				string emsg;
				rc = dbrmp->markPartitionForDeletion( aLogInfo.oids, aLogInfo.partitionNums, emsg);
				if (( rc != 0 ) && ( rc !=BRM::ERR_PARTITION_DISABLED ))
				{
					continue;
				}
				try {
					ddlpackageprocessor.removePartitionFiles( aLogInfo.oids, aLogInfo.partitionNums, uniqueId );	
					cacheutils::flushPartition( aLogInfo.oids, aLogInfo.partitionNums ); 
					emsg.clear();
					rc = dbrmp->deletePartition( aLogInfo.oids, aLogInfo.partitionNums, emsg);
					ddlpackageprocessor.deleteLogFile(DDLPackageProcessor::DROPPART_LOG, it->first, uniqueId);
					ostringstream oss;
					oss << "DDLProc has cleaned up drop partitions left over for table " << it->first;
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logInfoMessage( message1 );
				}
				catch (runtime_error& ex)
				{
					ostringstream oss;
					oss << "DDLProc cannot clean up drop partitions for table " << it->first << " due to " << ex.what();
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logErrorMessage( message1 );
				}
				break;
			}
			case DDLPackageProcessor::TRUNCATE_LOG:
			{
				rc = dbrmp->markAllPartitionForDeletion( aLogInfo.oids);
				if (rc != 0) //Log a message to err.log
				{
					string errMsg;
					BRM::errString(rc, errMsg);
					ostringstream oss;
					oss << "DDLProc didn't clean up files for truncate table with oid " << it->first << " due to "<< errMsg;
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logErrorMessage( message1 );
					continue;
				}
				try {
					ddlpackageprocessor.removeFiles(uniqueId, aLogInfo.oids);
					rc = cacheutils::flushOIDsFromCache(aLogInfo.oids);	
					ddlpackageprocessor.removeExtents( aLogInfo.oids );
					//create a new sets of files. Just find a dbroot according th the number of tables in syscat-1.
					boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr =
						CalpontSystemCatalog::makeCalpontSystemCatalog(1);
					int tableCount = systemCatalogPtr->getTableCount();
					int dbRootCnt = 1;
					int useDBRoot = 1;
					string DBRootCount = config::Config::makeConfig()->getConfig("SystemConfig", "DBRootCount");
					if (DBRootCount.length() != 0)
						dbRootCnt = static_cast<int>(config::Config::fromText(DBRootCount));

					useDBRoot =  ((tableCount-1) % dbRootCnt) + 1;
					
					//Create all column and dictionary files
					CalpontSystemCatalog::TableName aTableName = systemCatalogPtr->tableName(it->first);
					ddlpackageprocessor.createFiles(aTableName,useDBRoot,uniqueId, static_cast<uint32_t>(aLogInfo.oids.size()));				
					ddlpackageprocessor.deleteLogFile(DDLPackageProcessor::TRUNCATE_LOG, it->first, uniqueId);
					ostringstream oss;
					oss << "DDLProc has cleaned up truncate table left over for table " << it->first << " and the table lock is released.";
					logging::Message::Args args1;
					logging::Message message1(2);
					args1.add(oss.str());
					message1.format( args1 );
					ml.logInfoMessage( message1 );
				}
				catch (std::exception& ex) 
				{
					logging::Message::Args args1;
					logging::Message message1(2);
					ostringstream oss;
					oss << "DDLProc didn't clean up truncate table left over for table with oid " << it->first << " due to " << ex.what();
					args1.add(oss.str());
					message1.format( args1 );
					ml.logErrorMessage( message1 );
					continue;
				}
				break;
			}
			default:
				break;
		}
	}
	//unlock the table
	std::vector<BRM::TableLockInfo> tableLocks;
	tableLocks = dbrmp->getAllTableLocks();
	for (unsigned idx=0; idx<tableLocks.size(); idx++)
	{	
		if (tableLocks[idx].ownerName == "DDLProc")
		{
			try
			{
				(void)dbrmp->releaseTableLock(tableLocks[idx].id);
			}
			catch ( ... ) {}
		}
	}
	return 0;
}

} //namespace ddlcleanuputil
// vim:ts=4 sw=4:

