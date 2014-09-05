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
#include "idbcompress.h"
using namespace compress;

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
typedef std::map<uint32_t, Files> columnMap;
typedef std::map<int, columnMap*> allColumnMap;
allColumnMap wholeMap;
boost::mutex columnMapLock;	
ActiveThreadCounter *activeThreadCounter;

off64_t getCompressedDataSize(string& fileName)
{
		off64_t dataSize = 0;
		ifstream ifs(fileName.c_str());
		if (!ifs.good())
		{
			std::ostringstream oss;
			oss << "Cannot open file " << fileName << " for read.";
			throw std::runtime_error(oss.str());
		}
		
		IDBCompressInterface decompressor;

		//--------------------------------------------------------------------------
		// Read headers and extract compression pointers
		//--------------------------------------------------------------------------
		char hdr1[IDBCompressInterface::HDR_BUF_LEN];
		ifs.read(hdr1, IDBCompressInterface::HDR_BUF_LEN);
		if (ifs.eof())
		{
			std::ostringstream oss;
			oss << "Error reading header from file " << fileName;
			throw std::runtime_error(oss.str());
		}

		int64_t ptrSecSize = decompressor.getHdrSize(hdr1) -
		IDBCompressInterface::HDR_BUF_LEN;
		char* hdr2 = new char[ptrSecSize];
		ifs.read(hdr2, ptrSecSize);
		if (ifs.eof())
		{
			std::ostringstream oss;
			oss << "Error reading header2 from file " << fileName;
			throw std::runtime_error(oss.str());
		}
		CompChunkPtrList chunkPtrs;
		int rc = decompressor.getPtrList(hdr2, ptrSecSize, chunkPtrs);
		delete[] hdr2;
		if (rc != 0)
		{
			std::ostringstream oss;
			oss << "getPtrList error from file " << fileName;
			throw std::runtime_error(oss.str());
		}

		unsigned k = chunkPtrs.size();
		// last header's offset + length will be the data bytes
		dataSize = chunkPtrs[k-1].first + chunkPtrs[k-1].second;
		return dataSize;
}

struct ColumnThread
{
    ColumnThread(uint32_t oid, int32_t compressionType, bool reportRealUse, int key) 
    : fOid(oid), fCompressionType(compressionType), fReportRealUse(reportRealUse), fKey(key) 
    {}
    void operator()()
    {
		Config config;
		config.initConfigCache();
		std::vector<u_int16_t> rootList;
		config.getRootIdList( rootList );
		FileOp fileOp;
		Files aFiles;
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
				struct stat stFileInfo; 
				string aFile(fileName); //convert between char* and string
				off64_t fileSize = 0;
				if (fReportRealUse && (fCompressionType > 0))
				{
					try {
						fileSize = getCompressedDataSize(aFile);
					}
					catch (std::exception& ex)
					{
						cerr << ex.what();
					}
				}
				else
				{					
					int intStat = stat(aFile.c_str(), &stFileInfo); 
					if ( intStat == 0 ) //File exists	stFileInfo.st_size is the size
						fileSize = stFileInfo.st_size;
				}
				
				if ( fileSize > 0 ) //File exists	stFileInfo.st_size is the size
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
		allColumnMap::iterator colMapiter = wholeMap.find(fKey);
		if (colMapiter != wholeMap.end())
		{
			(colMapiter->second)->insert(make_pair(fOid,aFiles));	
			activeThreadCounter->decr();
		//cout << "Added to columnsMap aFiles with size " << aFiles.size() << " for oid " << fOid << endl;		
		}
	}
	u_int32_t fOid;
	int32_t fCompressionType;
	bool fReportRealUse;
	int fKey;
};
//------------------------------------------------------------------------------
// Process a table size based on input from the
// bytestream object.
//------------------------------------------------------------------------------
int WE_GetFileSizes::processTable(
	messageqcpp::ByteStream& bs,
	std::string& errMsg, int key)
{
	uint8_t rc = 0;
	errMsg.clear();

	try {
		std::string aTableName;
		std::string schemaName;
		bool reportRealUse = false;
		ByteStream::byte tmp8;
		
		bs >> schemaName;
		//cout << "schema: "<< schemaName << endl;

		bs >> aTableName;
		//cout << "tableName: " << aTableName << endl;
		bs >> tmp8;
		reportRealUse = tmp8;
		//get column oids
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
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
		CalpontSystemCatalog::ColType colType;
		
		columnMap *columnsMap = new columnMap();
		{	
			boost::mutex::scoped_lock lk(columnMapLock);
			wholeMap[key] = columnsMap;
		}

		for (uint32_t i=0; i < columnList.size(); i++)
		{
			colType = systemCatalogPtr->colType(columnList[i].objnum);
			tp.invoke(ColumnThread(columnList[i].objnum, colType.compressionType, reportRealUse, key));	
			if (colType.ddn.dictOID > 0)
				tp.invoke(ColumnThread(colType.ddn.dictOID, colType.compressionType, reportRealUse, key));	
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
	allColumnMap::iterator colMapiter = wholeMap.find(key);
	if (colMapiter != wholeMap.end())
	{
		columnMap::iterator iter = colMapiter->second->begin();
		uint64_t size;
		Files::iterator it;
		while ( iter != colMapiter->second->end())
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
		wholeMap.erase(colMapiter);
	}
	return rc;
}


}
