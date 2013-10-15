/* Copyright (C) 2013 Calpont Corp.

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

/*****************************************************************************
 * $Id: we_getfilesizes.cpp 4450 2013-01-21 14:13:24Z rdempsey $
 *
 ****************************************************************************/
#include "we_getfilesizes.h"

#include <iostream>
#include <stdexcept>
using namespace std;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "threadpool.h"
using namespace threadpool;

#include "bytestream.h"
using namespace messageqcpp;
#include "we_fileop.h"

#include "IDBFileSystem.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

namespace WriteEngine
{
struct FileInfo                             
{
        u_int32_t      partition;          /** @brief Partition for a file*/
        u_int16_t      segment;            /** @brief Segment for a file */
        u_int16_t      dbRoot;             /** @brief DbRoot for a file */
        std::string    segFileName;        /** @brief seg file path */
		double  	   fileSize;		   /** @brief seg file size in giga bytes */
		void serialize(messageqcpp::ByteStream& bs)
		{
			bs << partition;
			bs << segment;
			bs << dbRoot;
			bs << segFileName;
			bs << (*(uint64_t*)(&fileSize));
		}
};
typedef std::vector<FileInfo> Files;
typedef std::map<u_int32_t, Files> columnMap;
columnMap columnsMap;
boost::mutex columnMapLock;	
ActiveThreadCounter *activeThreadCounter;
struct ColumnThread
{
    ColumnThread(u_int32_t oid) : fOid(oid)
    {}
    void operator()()
    {
		Config config;
		config.initConfigCache();
		std::vector<u_int16_t> rootList;
		config.getRootIdList( rootList );
		FileOp fileOp;
		Files aFiles;

		// This function relies on IDBPolicy being initialized by
		// IDBPolicy::init().  This is done when WriteEngineServer main() calls
		// IDBPolicy::configIDBPolicy();
		IDBDataFile::Types fileType;
		bool bUsingHdfs = IDBPolicy::useHdfs();
		if (bUsingHdfs)
			fileType = IDBDataFile::HDFS;
		else
			fileType = IDBDataFile::UNBUFFERED;
		IDBFileSystem& fs = IDBFileSystem::getFs( fileType );
		for (uint i=0; i < rootList.size(); i++)
		{
			std::vector<struct BRM::EMEntry> entries;
			(void)BRMWrapper::getInstance()->getExtents_dbroot(fOid, entries, rootList[i]);
			std::vector<struct BRM::EMEntry>::const_iterator iter = entries.begin();
			while ( iter != entries.end() ) //organize extents into files
			{	
				//Find the size of this file
				//string fileName;
				char fileName[200];
				(void)fileOp.getFileName( fOid, fileName, rootList[i], entries[0].partitionNum, entries[0].segmentNum);
				string aFile(fileName); //convert between char* and string
				off64_t fileSize = fs.size( fileName );
				if (fileSize > 0) // File exists, add to list
				{
					FileInfo aFileInfo;
					aFileInfo.partition = entries[0].partitionNum;
					aFileInfo.segment = entries[0].segmentNum;
					aFileInfo.dbRoot = rootList[i];
					aFileInfo.segFileName = aFile;
					aFileInfo.fileSize = (double)fileSize / (1024 * 1024 * 1024);
					aFiles.push_back(aFileInfo);
					//cout.precision(15);
					//cout << "The file " << aFileInfo.segFileName << " has size " << fixed << aFileInfo.fileSize << "GB" << endl;
				}
				
				//erase the entries from this dbroot.
				std::vector<struct BRM::EMEntry> entriesTrimed;
				for (uint m=0; m<entries.size(); m++)
				{
					if ((entries[0].partitionNum != entries[m].partitionNum) || (entries[0].segmentNum != entries[m].segmentNum))
					entriesTrimed.push_back(entries[m]);
				}
				entriesTrimed.swap(entries);
				iter = entries.begin();
			}
		}
		
		boost::mutex::scoped_lock lk(columnMapLock);
		//cout << "Current size of columnsMap is " << columnsMap.size() << endl;
		columnsMap[fOid] = aFiles;	
		activeThreadCounter->decr();
		//cout << "Added to columnsMap aFiles with size " << aFiles.size() << " for oid " << fOid << endl;		
	}
	u_int32_t fOid;
};
//------------------------------------------------------------------------------
// Process a table size based on input from the
// bytestream object.
//------------------------------------------------------------------------------
int WE_GetFileSizes::processTable(
	messageqcpp::ByteStream& bs,
	std::string& errMsg)
{
	uint8_t rc = 0;
	errMsg.clear();

	try {
		std::string aTableName;
		std::string schemaName;

		bs >> schemaName;
		//cout << "schema: "<< schemaName << endl;

		bs >> aTableName;
		//cout << "tableName: " << aTableName << endl;
		//get column oids
		CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
		CalpontSystemCatalog::TableName tableName;
		tableName.schema = schemaName;
		tableName.table = aTableName;
		CalpontSystemCatalog::RIDList columnList = systemCatalogPtr->columnRIDs(tableName);
		CalpontSystemCatalog::DictOIDList dictOidList = systemCatalogPtr->dictOIDs(tableName);
		int serverThreads = 20;
		int serverQueueSize = serverThreads * 100;
		threadpool::ThreadPool tp(serverThreads,serverQueueSize);
		int totalSize = columnList.size() + dictOidList.size();
		activeThreadCounter = new ActiveThreadCounter(totalSize);
		
		for (uint i=0; i < columnList.size(); i++)
		{
			tp.invoke(ColumnThread(columnList[i].objnum));		
		}
		for (uint i=0; i < dictOidList.size(); i++)
		{
			tp.invoke(ColumnThread(dictOidList[i].dictOID));		
		}
		//check whether all threads finish
		int sleepTime = 100; // sleep 100 milliseconds between checks
		struct timespec rm_ts;

		rm_ts.tv_sec = sleepTime/1000;
		rm_ts.tv_nsec = sleepTime%1000 *1000000;
		uint32_t currentActiveThreads = 10;
		while (currentActiveThreads > 0)
		{
#ifdef _MSC_VER
			Sleep(sleepTime);
#else
			struct timespec abs_ts;
			do
			{
				abs_ts.tv_sec = rm_ts.tv_sec;
				abs_ts.tv_nsec = rm_ts.tv_nsec;
			}
			while(nanosleep(&abs_ts,&rm_ts) < 0);
#endif
			currentActiveThreads = activeThreadCounter->cur();
		}
	}
	catch(std::exception& ex)
	{
		//cout << "WE_GetFileSizes got exception-" << ex.what() <<
		//	std::endl;
		errMsg = ex.what();
		rc     = 1;
	}
	//Build the message to send to the caller
	bs.reset();
	boost::mutex::scoped_lock lk(columnMapLock);
	columnMap::iterator iter = columnsMap.begin();
	uint64_t size;
	Files::iterator it;
	while ( iter != columnsMap.end())
	{
		bs << iter->first;
		//cout << "processTable::coloid = " << iter->first << endl;

		size = iter->second.size();
		bs << size;
		for (it = iter->second.begin(); it != iter->second.end(); it++)
			it->serialize(bs);
		//cout << "length now is " << bs.length() << endl;
		iter++;
	}
	columnsMap.clear();
	return rc;
}


}
