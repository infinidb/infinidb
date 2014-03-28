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

/*
* $Id: we_redistributeworkerthread.cpp 4646 2013-05-23 20:58:08Z xlou $
*/

#include <iostream>
#include <set>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <string>
#include <unistd.h>
using namespace std;

#include "boost/scoped_ptr.hpp"
#include "boost/scoped_array.hpp"
#include "boost/thread/mutex.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
using namespace boost;

#include "installdir.h"

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
#include "oamcache.h"
using namespace oam;

#include "dbrm.h"
using namespace BRM;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "exceptclasses.h"
using namespace logging;

#include "IDBFileSystem.h"
#include "IDBPolicy.h"
using namespace idbdatafile;

#include "we_fileop.h"
#include "we_messages.h"
#include "we_convertor.h"
#include "we_redistributedef.h"
#include "we_redistributecontrol.h"
#include "we_redistributeworkerthread.h"




namespace redistribute
{

// need be consistent with we_config.cpp
const unsigned DEFAULT_FILES_PER_COLUMN_PARTITION  = 4;

// static variables
boost::mutex RedistributeWorkerThread::fActionMutex;
volatile bool RedistributeWorkerThread::fStopAction = false;
volatile bool RedistributeWorkerThread::fCommitted = false;
string RedistributeWorkerThread::fWesInUse;


RedistributeWorkerThread::RedistributeWorkerThread(ByteStream& bs, IOSocket& ios) :
	fBs(bs),
	fIOSocket(ios),
	fTableLockId(0),
	fErrorCode(RED_EC_OK),
	fNewFilePtr(NULL),
	fOldFilePtr(NULL)
{
	fWriteBuffer.reset(new char[CHUNK_SIZE]);
}


RedistributeWorkerThread::~RedistributeWorkerThread()
{
	mutex::scoped_lock lock(fActionMutex);
	if (fNewFilePtr)
		closeFile(fNewFilePtr);

	if (fOldFilePtr)
		closeFile(fOldFilePtr);

	// make sure releasing the table lock.
	if (fTableLockId > 0)
	{
		fDbrm->releaseTableLock(fTableLockId);

		// use the interface, line# replaced with lock id.
		logMessage("Releasing table lock in destructor. ", fTableLockId);
	}
}


void RedistributeWorkerThread::operator()()
{
	memcpy(&fMsgHeader, fBs.buf(), sizeof(RedistributeMsgHeader));
	fBs.advance(sizeof(RedistributeMsgHeader));
	if (fMsgHeader.messageId == RED_ACTN_REQUEST)
		handleRequest();
	else if (fMsgHeader.messageId == RED_ACTN_STOP)
		handleStop();
	else if (fMsgHeader.messageId == RED_DATA_INIT)
		handleData();
	else
		handleUnknowJobMsg();
}


void RedistributeWorkerThread::handleRequest()
{
	try
	{
		// clear stop flag if ever set.
		{
			mutex::scoped_lock lock(fActionMutex);
			fStopAction = false;
			fCommitted = false;
		}

		if (setup() == 0)
		{
			if (fBs.length() >= sizeof(RedistributePlanEntry))
			{
				memcpy(&fPlanEntry, fBs.buf(), sizeof(RedistributePlanEntry));
				fBs.advance(sizeof(RedistributePlanEntry));
				OamCache::dbRootPMMap_t dbrootToPM = fOamCache->getDBRootToPMMap();
				fMyId.first = fPlanEntry.source;
				fMyId.second = (*dbrootToPM)[fMyId.first];
				fPeerId.first = fPlanEntry.destination;
				fPeerId.second = (*dbrootToPM)[fPeerId.first];
				if (grabTableLock() == 0)
				{
					// workaround extentmap slow update
					sleep(1);

					// build segment & entry list after grabbing the table lock.
					if (buildEntryList() == 0)
					{
						if (sendData() == 0)
						{
							// do bulk update
							updateDbrm();
						}
					}

					// conversation to peer after got table lock
					// confirm commit or abort
					confirmToPeer();
				}
			}
		}
	}
	catch (const std::exception&)
	{
	}
	catch (...)
	{
	}

	sendResponse(RED_ACTN_REQUEST);

	mutex::scoped_lock lock(fActionMutex);
	fWesInUse.clear();
	fMsgQueueClient.reset();

	fStopAction = false;
	fCommitted = false;
}


int RedistributeWorkerThread::setup()
{
	int ret = 0;

	try
	{
		fConfig = Config::makeConfig();
		fOamCache = oam::OamCache::makeOamCache();
		fDbrm = RedistributeControl::instance()->fDbrm;

		// for segment file # workaround
		// string tmp = fConfig->getConfig("ExtentMap", "FilesPerColumnPartition");
		// int filesPerPartition = fConfig->fromText(tmp);
		// if (filesPerPartition == 0)
		//	filesPerPartition = DEFAULT_FILES_PER_COLUMN_PARTITION;
		// int dbrootNum = fOamCache->getDBRootNums().size();
		// if (dbrootNum == 0)
		// {
		//	fErrorMsg = "OamCache->getDBRootNums() failed.";
		//	logMessage(fErrorMsg, __LINE__);
		//	return 1;
		// }
		// if ((filesPerPartition % dbrootNum) != 0)
		// {
		//	fErrorMsg = "ExtentMap::FilesPerColumnPartition is not a multiple of db root number.";
		//	logMessage(fErrorMsg, __LINE__);
		//	return 1;
		// }
		// fSegPerRoot = filesPerPartition / dbrootNum;
	}
	catch (const std::exception&)
	{
		ret = 2;
	}
	catch (...)
	{
		ret = 2;
	}

	return ret;
}


int RedistributeWorkerThread::grabTableLock()
{
	fTableLockId = 0;
	try
	{
		vector<uint> pms;
		pms.push_back(fMyId.second);
		if (fMyId.second != fPeerId.second)
			pms.push_back(fPeerId.second);

		struct timespec ts;
		ts.tv_sec = 0;
		ts.tv_nsec = 100 * 1000000;
		while (fTableLockId == 0 && !fStopAction)
		{
			// make sure it's not stopped.
			if (fStopAction)
					return RED_EC_USER_STOP;

			// always wait long enough for ddl/dml/cpimport to get table lock
			// for now, triple the ddl/dml/cpimport retry interval: 3 * 100ms
#ifdef _MSC_VER
			Sleep(ts.tv_nsec * 1000);
#else
			struct timespec tmp = ts;
			while (nanosleep(&tmp ,&ts) < 0);
				tmp = ts;
#endif

			try
			{
				uint32_t processID = ::getpid();
				int32_t txnId = 0;
				int32_t sessionId = 0;
				string processName = "WriteEngineServer";
				fTableLockId = fDbrm->getTableLock(pms, fPlanEntry.table,
						&processName, &processID, &sessionId, &txnId, BRM::LOADING );
			}
			catch (const std::exception& ex)
			{
				fErrorCode = RED_EC_IDB_HARD_FAIL;
				logMessage(string("getTableLock exception") + ex.what(), __LINE__);
			}
			catch (...)
			{
				fErrorCode = RED_EC_IDB_HARD_FAIL;
				logMessage("getTableLock exception", __LINE__);

				// no need to throw
				// throw IDBExcept(ERR_HARD_FAILURE);
			}
		}
	}
	catch (const std::exception& ex)
	{
		// use the interface, line# replaced with lock id.
		logMessage(string(ex.what()) + " when try to get table lock: ", fTableLockId);
	}
	catch (...)
	{
		// use the interface, line# replaced with lock id.
		logMessage("Unknown exception when try to get table lock: ", fTableLockId);
	}

	// use the interface, line# replaced with lock id.
	logMessage("Got table lock: ", fTableLockId);

	return ((fTableLockId > 0) ? 0 : -1);
}


int RedistributeWorkerThread::buildEntryList()
{
	int ret = 0;

	try
	{
		// get all column oids
		boost::shared_ptr<CalpontSystemCatalog> csc = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
		const CalpontSystemCatalog::TableName table = csc->tableName(fPlanEntry.table);
		CalpontSystemCatalog::RIDList cols = csc->columnRIDs(table, true);
		for (CalpontSystemCatalog::RIDList::iterator i = cols.begin(); i != cols.end(); i++)
			fOids.push_back(i->objnum);

		CalpontSystemCatalog::DictOIDList dicts = csc->dictOIDs(table);
		for (CalpontSystemCatalog::DictOIDList::iterator i = dicts.begin(); i != dicts.end(); i++)
			fOids.push_back(i->dictOID);

		bool firstOid = true;  // for adding segments, all columns have the same lay out.
		uint16_t source = fPlanEntry.source;
		uint16_t target = fPlanEntry.destination;
		uint16_t partition = fPlanEntry.partition;
		uint32_t minWidth = 8;  // column width greater than 8 will be dictionary.
		for (vector<int64_t>::iterator i = fOids.begin(); i != fOids.end(); i++)
		{
			vector<EMEntry> entries;
			int rc = fDbrm->getExtents(*i, entries, false, false, true);
			if (rc != 0 || entries.size() == 0)
			{
				ostringstream oss;
				oss << "Error in DBRM getExtents; oid:" << *i << "; returnCode: " << rc;
				throw runtime_error(oss.str());
			}

			// same oid has the same column width
			uint32_t colWid = entries.front().colWid;
			vector<EMEntry>::iterator targetHwmEntry = entries.end();  // for HWM_0 workaround
			if (colWid > 0 && colWid < minWidth)
				minWidth = colWid;

			for (vector<EMEntry>::iterator j = entries.begin(); j != entries.end(); j++)
			{
				if (j->dbRoot == source && j->partitionNum == partition)
				{
					fUpdateRtEntries.push_back(BulkUpdateDBRootArg(j->range.start, target));

					if (firstOid)
						fSegments.insert(j->segmentNum);
				}
				else if (j->dbRoot == target && j->partitionNum == partition)
				{
					// the partition already exists on the target dbroot
					fErrorCode = RED_EC_PART_EXIST_ON_TARGET;
					ostringstream oss;
					oss << "oid:" << *i << ", partition:" << partition << " exists, source:" 
						<< source << ", destination:" << target;
					fErrorMsg = oss.str();
					logMessage(fErrorMsg, __LINE__);
					return fErrorCode;
				}

				// workaround for HWM_0 of highest extents of the oid on target dbroot.
				if (j->dbRoot == target)
				{
					if (targetHwmEntry == entries.end())
					{
						targetHwmEntry = j;
					}
					else
					{
						if (j->partitionNum > targetHwmEntry->partitionNum)
						{
							targetHwmEntry = j;
						}
						else if (j->partitionNum == targetHwmEntry->partitionNum &&
								 j->blockOffset   > targetHwmEntry->blockOffset)
						{
							targetHwmEntry = j;
						}
						else if (j->partitionNum == targetHwmEntry->partitionNum &&
								 j->blockOffset  == targetHwmEntry->blockOffset &&
								 j->segmentNum    > targetHwmEntry->segmentNum)
						{
							targetHwmEntry = j;
						}
					}
				}
			} // for em entries

			// HWM_0 workaround
			// HWM 0 has two possibilities:
			//     1. segment file has one extent, and the first block is not full yet.
			//     2. segment file has more than one extents, the HWM of the extents other than
			//        the last extent is set to 0, that is only last extent has none-zero HWM.
			// In tuple-bps::makeJob, there is a check to handle last extent has 0 hwm:
			//  (scannedExtents[i].HWM == 0 && (int) i < lastExtent[scannedExtents[i].dbRoot-1])
			//          lbidsToScan = scannedExtents[i].range.size * 1024;
			// Based on this check, the number of block to scan is caculated.
			// After redistributing the partitions, the original case 1 extent on destination
			// may not be the highest extent in the dbroot, and result in a full extent scan.
			// This scan will fail because there is no enough blocks if this is an abbreviated
			// extent or not enough chunks if the column is compressed.
			// The workaround is to bump up the HWM to 1 if moved in partitions are greater.
			if (targetHwmEntry != entries.end() &&                // exclude no extent case
				targetHwmEntry->colWid > 0 &&                      // exclude dictionary
				targetHwmEntry->HWM == 0 &&
				targetHwmEntry->partitionNum < partition)
			{
				BulkSetHWMArg arg;
				arg.oid = *i;
				arg.partNum = targetHwmEntry->partitionNum;
				arg.segNum = targetHwmEntry->segmentNum;
				arg.hwm = targetHwmEntry->colWid;  // will correct later based on minWidth

				fUpdateHwmEntries.push_back(arg);
			}
		} // for oids


		// HWM_0 workaround
		// Caculate the min(column width), the HWM(bump up to) for each column.
		if (fUpdateHwmEntries.size() > 0)
		{
			// update the HWM based in column width, not include dictionary extents
			for (vector<BRM::BulkSetHWMArg>::iterator j = fUpdateHwmEntries.begin();
					j != fUpdateHwmEntries.end(); j++)
			{
				if (j->hwm <= 8)
					j->hwm /= minWidth;
				else
					j->hwm = 1;  // not needed, but in case
			}
		}
	}
	catch (const std::exception& ex)
	{
		fErrorCode = RED_EC_EXTENT_ERROR;
		fErrorMsg = ex.what();
		logMessage(fErrorMsg, __LINE__);
		ret = fErrorCode;
	}
	catch (...)
	{
		fErrorCode = RED_EC_EXTENT_ERROR;
		fErrorMsg = "get extent error.";
		logMessage(fErrorMsg, __LINE__);
		ret = fErrorCode;
	}

	return ret;
}


int RedistributeWorkerThread::sendData()
{
	WriteEngine::FileOp fileOp;  // just to get filename, not for file operations
	bool remotePM = (fMyId.second != fPeerId.second);
	uint32_t dbroot = fPlanEntry.source;
	uint32_t partition = fPlanEntry.partition;
	int16_t source = fPlanEntry.source;
	int16_t dest = fPlanEntry.destination;

	IDBDataFile::Types fileType = 
		(IDBPolicy::useHdfs() ? IDBDataFile::HDFS : IDBDataFile::UNBUFFERED);
	IDBFileSystem& fs = IDBFileSystem::getFs( fileType );

	if ((remotePM) && (fileType != IDBDataFile::HDFS))
	{
		if (connectToWes(fPeerId.second) != 0)
		{
			fErrorCode = RED_EC_CONNECT_FAIL;
			ostringstream oss;
			oss << "Failed to connect to PM" << fPeerId.second << " from PM" << fMyId.second;
			fErrorMsg = oss.str();
			logMessage(fErrorMsg, __LINE__);
			return fErrorCode;
		}

		// start to send each segment file
		uint32_t seq = 0;
		ByteStream bs;

		// start conversion with peer, hand shaking.
		RedistributeMsgHeader header(dest, source, seq++, RED_DATA_INIT);
		bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
		bs.append((const ByteStream::byte*) &header, sizeof(header));
		fMsgQueueClient->write(bs);

		SBS sbs = fMsgQueueClient->read();
		if (!checkDataTransferAck(sbs, 0))
			return fErrorCode;

		for (vector<int64_t>::iterator i = fOids.begin(); i != fOids.end(); i++)
		{
			for (set<int16_t>::iterator j = fSegments.begin(); j != fSegments.end(); ++j)
			{
				char fileName[WriteEngine::FILE_NAME_SIZE];
				int rc = fileOp.oid2FileName(*i, fileName, false, dbroot, partition, *j);
				if (rc == WriteEngine::NO_ERROR)
				{
					ostringstream oss;
					oss << "<=redistributing: " << fileName << ", oid=" << *i << ", db="
						<< source << ", part=" << partition << ", seg=" << *j << " to db="
						<< dest;
					logMessage(oss.str(), __LINE__);
				}
				else
				{
					fErrorCode = RED_EC_OID_TO_FILENAME;
					ostringstream oss;
					oss << "Failed to get file name: oid=" << *i << ", dbroot=" << dbroot
						<< ", partition=" << partition << ", segment=" << *j;
					fErrorMsg = oss.str();
					logMessage(fErrorMsg, __LINE__);
					return fErrorCode;
				}

				if (fOldFilePtr != NULL)
					closeFile(fOldFilePtr);

				errno = 0;
				FILE* fOldFilePtr = fopen(fileName, "rb");
				if (fOldFilePtr != NULL)
				{
					ostringstream oss;
					oss << "open " << fileName << ", oid=" << *i << ", dbroot=" << dbroot
						<< ", partition=" << partition << ", segment=" << *j
						<< ". " << fOldFilePtr;
					logMessage(oss.str(), __LINE__);
				}
				else
				{
					int e = errno;
					fErrorCode = RED_EC_OPEN_FILE_FAIL;
					ostringstream oss;
					oss << "Failed to open " << fileName << ", oid=" << *i << ", dbroot=" << dbroot
						<< ", partition=" << partition << ", segment=" << *j
						<< ". " << strerror(e) << " (" << e << ")";
					fErrorMsg = oss.str();
					logMessage(fErrorMsg, __LINE__);
					return fErrorCode;
				}

				// add to set for remove after commit
				addToDirSet(fileName, true);

				char chunk[CHUNK_SIZE];
				errno = 0;
				fseek(fOldFilePtr, 0, SEEK_END);       // go to end of file
				long fileSize = ftell(fOldFilePtr);    // get current file size
				if (fileSize < 0)
				{
					int e = errno;
					ostringstream oss;
					oss << "Fail to tell file size: " << strerror(e) << " (" << e << ")";
					fErrorMsg = oss.str();
					fErrorCode = RED_EC_FSEEK_FAIL;
					logMessage(fErrorMsg, __LINE__);
					return fErrorCode;
				}

				// send start message to have the file of fileSize created at target dbroot.
				bs.restart();
				RedistributeMsgHeader header(dest, source, seq++, RED_DATA_START);
				RedistributeDataControl dataControl(*i, dest, partition, *j, fileSize);
				bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
				bs.append((const ByteStream::byte*) &header, sizeof(header));
				bs.append((const ByteStream::byte*) &dataControl, sizeof(dataControl));
				fMsgQueueClient->write(bs);

				sbs = fMsgQueueClient->read();
				if (!checkDataTransferAck(sbs, fileSize))
					return fErrorCode;

				// now send the file chunk by chunk.
				rewind(fOldFilePtr);
				int64_t bytesLeft = fileSize;
				size_t  bytesSend = CHUNK_SIZE;
				header.messageId = RED_DATA_CONT;
				while (bytesLeft > 0)
				{
					if (fStopAction)
					{
						closeFile(fOldFilePtr);
						fOldFilePtr = NULL;
						return RED_EC_USER_STOP;
					}

					if (bytesLeft < (long) CHUNK_SIZE)
						bytesSend = bytesLeft;

					errno = 0;
					size_t n = fread(chunk, 1, bytesSend, fOldFilePtr);
					if (n != bytesSend)
					{
						int e = errno;
						ostringstream oss;
						oss << "Fail to read: " << strerror(e) << " (" << e << ")";
						fErrorMsg = oss.str();
						fErrorCode = RED_EC_FREAD_FAIL;
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}

					header.sequenceNum = seq++;
					bs.restart();
					bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
   				 	bs.append((const ByteStream::byte*) &header, sizeof(header));
   				 	bs << (size_t) bytesSend;
					bs.append((const ByteStream::byte*) chunk, bytesSend);
   				 	fMsgQueueClient->write(bs);

					sbs = fMsgQueueClient->read();
					if (!checkDataTransferAck(sbs, bytesSend))
						return fErrorCode;

					bytesLeft -= bytesSend;
				}

				closeFile(fOldFilePtr);
				fOldFilePtr = NULL;

				header.messageId = RED_DATA_FINISH;
				header.sequenceNum = seq++;
				bs.restart();
				bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
  			 	bs.append((const ByteStream::byte*) &header, sizeof(header));
  			 	bs << (uint64_t) fileSize;
  			 	fMsgQueueClient->write(bs);

				sbs = fMsgQueueClient->read();
				if (!checkDataTransferAck(sbs, fileSize))
					return fErrorCode;

			}  // segments
		}  // for oids
	}   // remote peer non-hdfs
	else                                           // local or HDFS file copy
	{
		std::map<int,std::string> rootToPathMap;

		// use cp, in case failed in middle.  May consider to use rename if possible.
		for (vector<int64_t>::iterator i = fOids.begin(); i != fOids.end(); i++)
		{
			for (set<int16_t>::iterator j = fSegments.begin(); j != fSegments.end(); ++j)
			{
				if (fStopAction)
					return RED_EC_USER_STOP;

				if (fileType == IDBDataFile::HDFS) // HDFS file copy
				{
					string sourceName;
					int rc = buildFullHdfsPath(
						rootToPathMap, // map of root to path
                        *i,            // OID
						source,        // dbroot
						partition,     // partition
						*j,            // segment
						sourceName );  // full path name
					if (rc != 0)
					{
						fErrorCode = RED_EC_OID_TO_FILENAME;
						ostringstream oss;
						oss << "Failed to get src file name: oid=" << *i
							<< ", dbroot=" << source
							<< ", partition=" << partition
							<< ", segment=" << *j;
						fErrorMsg = oss.str();
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}

					string destName;
					rc = buildFullHdfsPath(
						rootToPathMap, // map of root to path
                        *i,            // OID
						dest,          // dbroot
						partition,     // partition
						*j,            // segment
						destName );    // full path name
					if (rc != 0)
					{
						fErrorCode = RED_EC_OID_TO_FILENAME;
						ostringstream oss;
						oss << "Failed to get dest file name: oid=" << *i
							<< ", dbroot=" << dest
							<< ", partition=" << partition
							<< ", segment=" << *j;
						fErrorMsg = oss.str();
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}

					ostringstream oss;
					oss << "<=redistributing(hdfs): " << sourceName << ", oid="
						<< *i << ", db=" << source << ", part=" << partition
						<< ", seg=" << *j << " to db=" << dest;
					logMessage(oss.str(), __LINE__);

					// add to set for remove after commit/abort
					addToDirSet(sourceName.c_str(), true);
					addToDirSet(destName.c_str(), false);

					int ret = fs.copyFile(sourceName.c_str(), destName.c_str());
					if (ret != 0)
					{
						fErrorCode = RED_EC_COPY_FILE_FAIL;
						ostringstream oss;
						oss << "Failed to copy " << sourceName << " to " <<
							destName << "; error is: " << strerror(errno);
						fErrorMsg = oss.str();
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}
				}
				else                               // local file copy
				{
					char sourceName[WriteEngine::FILE_NAME_SIZE];
					int rc = fileOp.oid2FileName(*i, sourceName, false, source,
						partition, *j);
					if (rc != WriteEngine::NO_ERROR)
					{
						fErrorCode = RED_EC_OID_TO_FILENAME;
						ostringstream oss;
						oss << "Failed to get file name: oid=" << *i
							<< ", dbroot=" << source
							<< ", partition=" << partition
							<< ", segment=" << *j;
						fErrorMsg = oss.str();
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}

					char destName[WriteEngine::FILE_NAME_SIZE];
					rc = fileOp.oid2FileName(*i, destName, true,
						dest, partition, *j);
					if (rc != WriteEngine::NO_ERROR)
					{
						fErrorCode = RED_EC_OID_TO_FILENAME;
						ostringstream oss;
						oss << "Failed to get file name: oid=" << *i
							<< ", dbroot=" << dest
							<< ", partition=" << partition
							<< ", segment=" << *j;
						fErrorMsg = oss.str();
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}

					ostringstream oss;
					oss << "<=redistributing(copy): " << sourceName << ", oid="
						<< *i << ", db=" << source << ", part=" << partition
						<< ", seg=" << *j << " to db=" << dest;
					logMessage(oss.str(), __LINE__);

					// add to set for remove after commit/abort
					addToDirSet(sourceName, true);
					addToDirSet(destName, false);

					// Using boost::copy_file() instead of IDBFileSystem::copy-
					// File() so we can capture/report any boost exception error
					// msg that IDBFileSystem::copyFile() currently swallows.
					try
					{
						filesystem::copy_file(sourceName, destName);
					}
#if BOOST_VERSION >= 105200
					catch(filesystem::filesystem_error& e)
#else
					catch(filesystem::basic_filesystem_error<filesystem::path>& e)
#endif
					{
						fErrorCode = RED_EC_COPY_FILE_FAIL;
						ostringstream oss;
						oss << "Failed to copy " << sourceName << " to " <<
							destName << "; error is: " << e.what();
						fErrorMsg = oss.str();
						logMessage(fErrorMsg, __LINE__);
						return fErrorCode;
					}
				}
			}  // segment
		}  // oid
	}  // !remote

	return 0;
}


//------------------------------------------------------------------------------
// Construct a full path name based on the given oid, root, partition, and seg.
// The rootToPathMap is the map of dbroot to dbrootPath that we are using.  We
// are using this function instead of the usual FileOp::oid2FileName() function,
// because that function only works with "local" DBRoots.  In the case of
// an HDFS copy, we will be copying files from/to DBRoots that are not on the
// local PM.
//------------------------------------------------------------------------------
int  RedistributeWorkerThread::buildFullHdfsPath(
	std::map<int,std::string>& rootToPathMap,
	int64_t      colOid,
	int16_t      dbRoot,
	uint32_t     partition,
	int16_t      segment,
	std::string& fullFileName)
{
	std::map<int,std::string>::const_iterator iter = rootToPathMap.find(dbRoot);
	if (iter == rootToPathMap.end())
	{
		ostringstream oss;
		oss << "DBRoot" << dbRoot;
		std::string dbRootPath = fConfig->getConfig("SystemConfig", oss.str());
		if (dbRootPath.empty())
		{
			return 1;
		}
		rootToPathMap[ dbRoot ] = dbRootPath;
		iter = rootToPathMap.find( dbRoot );
	}

	char tempFileName[WriteEngine::FILE_NAME_SIZE];
	char dbDir[WriteEngine::MAX_DB_DIR_LEVEL][WriteEngine::MAX_DB_DIR_NAME_SIZE];

	int rc = WriteEngine::Convertor::oid2FileName(
		colOid, tempFileName, dbDir, partition, segment );
	if (rc != WriteEngine::NO_ERROR)
	{
		return 2;
	}

	ostringstream fullFileNameOss;
	fullFileNameOss << iter->second << '/' << tempFileName;
	fullFileName = fullFileNameOss.str();

	return 0;
}


int  RedistributeWorkerThread::connectToWes(int pmId)
{
	int ret = 0;
	ostringstream oss;
	oss << "pm" << pmId << "_WriteEngineServer";
	try
	{
		fMsgQueueClient.reset(new MessageQueueClient(oss.str(), fConfig));
	}
	catch (const std::exception& ex)
	{
		fErrorMsg = "Caught exception when connecting to " + oss.str() + " -- " + ex.what();
		ret = 1;
	}
	catch (...)
	{
		fErrorMsg = "Caught exception when connecting to " + oss.str() + " -- unknown";
		ret = 2;
	}

	return ret;
}


int  RedistributeWorkerThread::updateDbrm()
{
	int rc1 = BRM::ERR_OK;
	int rc2 = BRM::ERR_OK;
	mutex::scoped_lock lock(fActionMutex);
	// cannot stop after extent map is updated.
	if (!fStopAction)
	{
		if (fUpdateHwmEntries.size() > 0)
			rc1 = fDbrm->bulkSetHWM(fUpdateHwmEntries, 0);

		if (rc1 == BRM::ERR_OK)
		{
			int rc2 = fDbrm->bulkUpdateDBRoot(fUpdateRtEntries);
			if (rc2 == 0)
				fCommitted = true;
			else
				fErrorCode = RED_EC_UPDATE_DBRM_FAIL;
		}

		// logging for debug
		{
			if (fUpdateHwmEntries.size() > 0)
			{
				ostringstream oss;
				oss << "HWM_0 workaround, updateHWM(oid,part,seg,hwm)";
				vector<BRM::BulkSetHWMArg>::iterator i = fUpdateHwmEntries.begin();
				for (; i != fUpdateHwmEntries.end(); i++)
				{
					oss << ":(" << i->oid << "," << i->partNum << "," << i->segNum << ","
						<< i->hwm << ")";
				}
				oss << ((rc1 == BRM::ERR_OK) ? " success" : " failed");
				logMessage(oss.str(), __LINE__);
			}

			if (rc1 == BRM::ERR_OK)
			{
				ostringstream oss;
				oss << "updateDBRoot(startLBID,dbRoot)";
				vector<BRM::BulkUpdateDBRootArg>::iterator i = fUpdateRtEntries.begin();
				for (; i != fUpdateRtEntries.end(); i++)
					oss << ":(" << i->startLBID << "," << i->dbRoot << ")";
				oss << ((rc2 == BRM::ERR_OK) ? " success" : " failed");
				logMessage(oss.str(), __LINE__);
			}
		}
	}

	return ((rc1 == BRM::ERR_OK && rc2 == BRM::ERR_OK) ? 0 : -1);
}


bool RedistributeWorkerThread::checkDataTransferAck(SBS& sbs, size_t size)
{
	if (sbs->length() == 0)
	{
		ostringstream oss;
		oss << "Zero byte read, Network error.";
		fErrorMsg = oss.str();
		logMessage(fErrorMsg, __LINE__);
		fErrorCode = RED_EC_NETWORK_FAIL;
	}
	else if (sbs->length() < (sizeof(RedistributeMsgHeader) + 1))
	{
		ostringstream oss;
		oss << "Short message, length=" << sbs->length();
		fErrorMsg = oss.str();
		logMessage(fErrorMsg, __LINE__);
		fErrorCode = RED_EC_WKR_MSG_SHORT;
	}
	else
	{
		// Need check header info
		ByteStream::byte wesMsgId;
		*sbs >> wesMsgId;
		//const RedistributeMsgHeader* h = (const RedistributeMsgHeader*) sbs->buf();
		sbs->advance(sizeof(RedistributeMsgHeader));
		size_t ack;
		*sbs >> ack;

		if (ack != size)
		{
			ostringstream oss;
			oss << "Acked size does not match request: " << ack << "/" << size;
			fErrorMsg = oss.str();
			logMessage(fErrorMsg, __LINE__);
			fErrorCode = RED_EC_SIZE_NACK;
		}
	}

	sbs.reset();

	return (fErrorCode == RED_EC_OK);
}


void RedistributeWorkerThread::confirmToPeer()
{
	if (fTableLockId > 0)
	{
		bool rc = false;
		try
		{
			rc = fDbrm->releaseTableLock(fTableLockId);

			// use the interface, line# replaced with lock id.
			logMessage("Releasing table lock... ", fTableLockId);
		}
		catch (const std::exception& ex)
		{
			// too bad, the talbe lock is messed up.
			fErrorMsg = ex.what();

			// use the interface, line# replaced with lock id.
			logMessage("Release table exception: " + fErrorMsg, fTableLockId);
		}
		catch (...)
		{
			// use the interface, line# replaced with lock id.
			logMessage("Release table lock unknown exception. ", fTableLockId);
		}

		if (rc == true)
		{
			// use the interface, line# replaced with lock id.
			logMessage("Release table lock return true. ", fTableLockId);
			fTableLockId = 0;
		}
		else
		{
			// let destructor try again.
			// use the interface, line# replaced with lock id.
			logMessage("Release table lock return false. ", fTableLockId);
		}
	}

	IDBFileSystem& fs = IDBFileSystem::getFs(
		(IDBPolicy::useHdfs() ? IDBDataFile::HDFS : IDBDataFile::UNBUFFERED) );

	uint32_t confirmCode = RED_DATA_COMMIT;
	if (fErrorCode != RED_EC_OK || fStopAction == true) // fCommitted must be false
		confirmCode = RED_DATA_ABORT;

	if (fMyId.second != fPeerId.second)
	{
		if (fMsgQueueClient.get() != NULL)
		{
			ByteStream bs;
			RedistributeMsgHeader header(fPeerId.first, fMyId.first, -1, confirmCode);
			bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;
			bs.append((const ByteStream::byte*) &header, sizeof(header));
			fMsgQueueClient->write(bs);

			// not going to retry for now, ignore the ack and close the connection.
			fMsgQueueClient->read();
			fMsgQueueClient.reset();
		}
	}
	else if (confirmCode != RED_DATA_COMMIT)
	{
		for (set<string>::iterator i = fNewDirSet.begin(); i != fNewDirSet.end(); i++)
		{
			fs.remove(i->c_str()); // ignoring return code
		}
	}

	// new files committed, remove old ones.
	if (confirmCode == RED_DATA_COMMIT)
	{
		for (set<string>::iterator i = fOldDirSet.begin(); i != fOldDirSet.end(); i++)
		{
			fs.remove(i->c_str()); // ignoring return code
		}
	}

	fNewDirSet.clear();
	fOldDirSet.clear();
}


void RedistributeWorkerThread::addToDirSet(const char* fileName, bool isSource)
{
	string path(fileName);
	size_t found = path.find_last_of("/\\");
	path = path.substr(0,found);

	if (isSource)
		fOldDirSet.insert(path);
	else
		fNewDirSet.insert(path);
}


void RedistributeWorkerThread::handleStop()
{
	mutex::scoped_lock lock(fActionMutex);
	// cannot stop after extent map is updated.
	if (!fCommitted)
		fStopAction = true;
	lock.unlock();

	logMessage("User stop", __LINE__);
	sendResponse(RED_ACTN_STOP);
}


void RedistributeWorkerThread::sendResponse(uint32_t type)
{
	uint32_t tmp = fMsgHeader.destination;
	fMsgHeader.destination = fMsgHeader.source;
	fMsgHeader.source = tmp;
	fMsgHeader.messageId = RED_ACTN_RESP;

	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));

	if (type == RED_ACTN_REQUEST)
	{
		if (fErrorCode == RED_EC_OK && fStopAction == false)
			fPlanEntry.status = RED_TRANS_SUCCESS;
		else if (fErrorCode == RED_EC_PART_EXIST_ON_TARGET)
			fPlanEntry.status = RED_TRANS_SKIPPED;
		else if (fErrorCode != RED_EC_OK)
			fPlanEntry.status = RED_TRANS_FAILED;
		// else -- stopped, may try again if support resume

		fBs.append((const ByteStream::byte*) &fPlanEntry, sizeof(fPlanEntry));
	}

	fIOSocket.write(fBs);
}


void RedistributeWorkerThread::handleData()
{
	bool done = false;
	bool noExcept = true;
	SBS sbs;
	size_t size = 0;

	try
	{
		do
		{
			switch (fMsgHeader.messageId)
			{
				case RED_DATA_INIT:
					handleDataInit();
					break;

				case RED_DATA_START:
					handleDataStart(sbs, size);
					break;

				case RED_DATA_CONT:
					handleDataCont(sbs, size);
					break;

				case RED_DATA_FINISH:
					handleDataFinish(sbs, size);
					break;

				case RED_DATA_COMMIT:
					handleDataCommit(sbs, size);
					done = true;
					break;

				case RED_DATA_ABORT:
					handleDataAbort(sbs, size);
					done = true;
					break;

				default:
					handleUnknowDataMsg();
					done = true;
					break;
			}

			if (!done)
			{
				// get next message
				sbs = fIOSocket.read();
				ByteStream::byte wesMsgId;
				*sbs >> wesMsgId;
				memcpy(&fMsgHeader, sbs->buf(), sizeof(RedistributeMsgHeader));
				sbs->advance(sizeof(RedistributeMsgHeader));
			}
		}
		while (!done);  // will break after commit/abort or catch an exception
	}
	catch (const std::exception& ex)
	{
		noExcept = false;
		logMessage(ex.what(), __LINE__);
	}
	catch (...)
	{
		noExcept = false;
	}

	if (noExcept == false)
	{
		// send NACK to peer
		fBs.restart();
		fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
		fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
		fBs << ((size_t) -1);
		fIOSocket.write(fBs);
	}

	fBs.reset();
	fIOSocket.close();
}


void RedistributeWorkerThread::handleDataInit()
{
	uint32_t tmp = fMsgHeader.destination;
	fMsgHeader.destination = fMsgHeader.source;
	fMsgHeader.source = tmp;
	fMsgHeader.messageId = RED_DATA_ACK;
	size_t size = 0;

	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
	fBs << size;

	// finish the hand shaking
	fIOSocket.write(fBs);
}


void RedistributeWorkerThread::handleDataStart(SBS& sbs, size_t& size)
{
	char fileName[WriteEngine::FILE_NAME_SIZE];

	try
	{
		// extract the control data for the segment file
		RedistributeDataControl dc;
		if (sbs->length() >= sizeof(RedistributeDataControl))
		{
			memcpy(&dc, sbs->buf(), sizeof(RedistributeDataControl));
			sbs->advance(sizeof(RedistributeDataControl));
			size = dc.size;
		}
		else
		{
			ostringstream oss;
			oss << "Short message, length=" << sbs->length();
			fErrorMsg = oss.str();
			fErrorCode = RED_EC_WKR_MSG_SHORT;
			logMessage(fErrorMsg, __LINE__);
			throw runtime_error(fErrorMsg);
		}

		// create and open the file for writing.
		WriteEngine::FileOp fileOp;  // just to get filename, not for file operations
		int rc = fileOp.oid2FileName(dc.oid, fileName, true, dc.dbroot, dc.partition, dc.segment);
		if (rc == WriteEngine::NO_ERROR)
		{
			ostringstream oss;
			oss << "=>redistributing: " << fileName << ", oid=" << dc.oid << ", db=" << dc.dbroot
				<< ", part=" << dc.partition << ", seg=" << dc.segment << " from db="
				<< fMsgHeader.destination;  // fMsgHeader has swapped source and destination.
			logMessage(oss.str(), __LINE__);
		}
		else
		{
			fErrorCode = RED_EC_OID_TO_FILENAME;
			ostringstream oss;
			oss << "Failed to get file name: oid=" << dc.oid << ", dbroot=" << dc.dbroot
				<< ", partition=" << dc.partition << ", segment=" << dc.segment;
			fErrorMsg = oss.str();
			logMessage(fErrorMsg, __LINE__);
			throw runtime_error(fErrorMsg);
		}

		if (fNewFilePtr != NULL)
			closeFile(fNewFilePtr);

		errno = 0;
		fNewFilePtr = fopen(fileName, "wb");
		if (fNewFilePtr != NULL)
		{
			ostringstream oss;
			oss << "open " << fileName << ", oid=" << dc.oid << ", dbroot="
				<< dc.dbroot << ", partition=" << dc.partition << ", segment=" << dc.segment
				<< ". " << fNewFilePtr;
			logMessage(oss.str(), __LINE__);
		}
		else
		{
			int e = errno;
			fErrorCode = RED_EC_OPEN_FILE_FAIL;
			ostringstream oss;
			oss << "Failed to open " << fileName << ", oid=" << dc.oid << ", dbroot="
				<< dc.dbroot << ", partition=" << dc.partition << ", segment=" << dc.segment
				<< ". " << strerror(e) << " (" << e << ")";
			fErrorMsg = oss.str();
			logMessage(fErrorMsg, __LINE__);
			throw runtime_error(fErrorMsg);
		}

		// set output buffering
		errno = 0;
		if (setvbuf(fNewFilePtr, fWriteBuffer.get(), _IOFBF, CHUNK_SIZE))
		{
			int e = errno;
			ostringstream oss;
			oss << "Failed to set i/o buffer: " << strerror(e) << " (" << e << ")";
			fErrorMsg = oss.str();
			logMessage(fErrorMsg, __LINE__);

			// not throwing an exception now.
		}

		// add to set for remove after abort
		addToDirSet(fileName, false);

		// do a fseek will show the right size, but will not actually allocate the continuous block.
		// do write 4k block till file size.
		char buf[PRE_ALLOC_SIZE] = {1};
		size_t nmemb = size / PRE_ALLOC_SIZE;
		while (nmemb-- > 0)
		{
			errno = 0;
			size_t n = fwrite(buf, PRE_ALLOC_SIZE, 1, fNewFilePtr);
			if (n != 1)
			{
				int e = errno;
				ostringstream oss;
				oss << "Fail to preallocate file: " << strerror(e) << " (" << e << ")";
				fErrorMsg = oss.str();
				fErrorCode = RED_EC_FWRITE_FAIL;
				logMessage(fErrorMsg, __LINE__);
				throw runtime_error(fErrorMsg);
			}
		}

		// move back to beging to write real data
		fflush(fNewFilePtr);
		rewind(fNewFilePtr);
	}
	catch (const std::exception& ex)
	{
		// NACK
		size = -1;
		logMessage(ex.what(), __LINE__);
	}
	catch (...)
	{
		// NACK
		size = -1;
	}

	// ack file size
	fMsgHeader.messageId = RED_DATA_ACK;
	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
	fBs << size;
	fIOSocket.write(fBs);

	// reset to count the data received
	size = 0;
	sbs.reset();
}


void RedistributeWorkerThread::handleDataCont(SBS& sbs, size_t& size)
{
	size_t ack = 0;

	try
	{
		size_t bytesRcvd = 0;
		*sbs >> bytesRcvd;

		if (bytesRcvd != sbs->length())
		{
			ostringstream oss;
			oss << "Incorrect data length: " << sbs->length() << ", expecting " << bytesRcvd;
			fErrorMsg = oss.str();
			fErrorCode = RED_EC_BS_TOO_SHORT;
			logMessage(fErrorMsg, __LINE__);
			throw runtime_error(fErrorMsg);
		}

		errno = 0;
		size_t n = fwrite(sbs->buf(), 1, bytesRcvd, fNewFilePtr);
		if (n != bytesRcvd)
		{
			int e = errno;
			ostringstream oss;
			oss << "Fail to write file: " << strerror(e) << " (" << e << ")";
			fErrorMsg = oss.str();
			fErrorCode = RED_EC_FWRITE_FAIL;
			logMessage(fErrorMsg, __LINE__);
			throw runtime_error(fErrorMsg);
		}

		ack = bytesRcvd;
		size += ack;
	}
	catch (const std::exception&)
	{
		// NACK
		size = -1;
	}
	catch (...)
	{
		// NACK
		ack = -1;
	}

	// ack received data
	sbs.reset();
	fMsgHeader.messageId = RED_DATA_ACK;
	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
	fBs << ack;
	fIOSocket.write(fBs);
}


void RedistributeWorkerThread::handleDataFinish(SBS& sbs, size_t& size)
{
	size_t ack = 0;

	// close open file
	closeFile(fNewFilePtr);
	fNewFilePtr = NULL;

	try
	{
		size_t fileSize = 0;
		*sbs >> fileSize;

		if (fileSize != size)
		{
			ostringstream oss;
			oss << "File size not match: local=" << size << ", remote=" << fileSize;
			fErrorMsg = oss.str();
			fErrorCode = RED_EC_FILE_SIZE_NOT_MATCH;
			logMessage(fErrorMsg, __LINE__);
			throw runtime_error(fErrorMsg);
		}

		ack = size;
	}
	catch (const std::exception&)
	{
		// NACK
		size = -1;
	}
	catch (...)
	{
		// NACK
		ack = -1;
	}

	// ack received data
	sbs.reset();
	fMsgHeader.messageId = RED_DATA_ACK;
	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
	fBs << ack;
	fIOSocket.write(fBs);
}


void RedistributeWorkerThread::handleDataCommit(SBS& sbs, size_t& size)
{
	size_t ack = 0;
	sbs.reset();
	fMsgHeader.messageId = RED_DATA_ACK;
	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
	fBs << ack;
	fIOSocket.write(fBs);
}


void RedistributeWorkerThread::handleDataAbort(SBS& sbs, size_t& size)
{
	// close open file
	if (fNewFilePtr != NULL)
		closeFile(fNewFilePtr);

	IDBFileSystem& fs = IDBFileSystem::getFs(
		(IDBPolicy::useHdfs() ? IDBDataFile::HDFS : IDBDataFile::UNBUFFERED) );

	// remove local files
	for (set<string>::iterator i = fNewDirSet.begin(); i != fNewDirSet.end(); i++)
	{
		fs.remove(i->c_str()); // ignoring return code
	}

	// send ack
	sbs.reset();
	size_t ack = 0;
	fMsgHeader.messageId = RED_DATA_ACK;
	fBs.restart();
	fBs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	fBs.append((const ByteStream::byte*) &fMsgHeader, sizeof(fMsgHeader));
	fBs << ack;
	fIOSocket.write(fBs);
}


void RedistributeWorkerThread::handleUnknowDataMsg()
{
	ostringstream oss;
	oss << "Unknown data message: " << fMsgHeader.messageId;
	fErrorMsg = oss.str();
	fErrorCode = RED_EC_UNKNOWN_DATA_MSG;
	logMessage(fErrorMsg, __LINE__);
	throw runtime_error(fErrorMsg);
}


void RedistributeWorkerThread::handleUnknowJobMsg()
{
	ostringstream oss;
	oss << "Unknown job message: " << fMsgHeader.messageId;
	fErrorMsg = oss.str();
	fErrorCode = RED_EC_UNKNOWN_JOB_MSG;
	logMessage(fErrorMsg, __LINE__);

	//protocol error, ignore and close connection.
}


void RedistributeWorkerThread::closeFile(FILE* f)
{
	if (f == NULL)
		return;

	ostringstream oss;
	oss << "close file* " << f << " ";

	errno = 0;
	int rc = fclose(f);
	if (rc != 0)
		oss << "error: " << strerror(errno) << " (" << errno << ")";
	else
		oss << "OK";

	logMessage(oss.str(), __LINE__);
}


void RedistributeWorkerThread::logMessage(const string& msg, int line)
{
	ostringstream oss;
	oss << msg << " @workerThread:" << line;
    RedistributeControl::instance()->logMessage(oss.str());
}


} // namespace

// vim:ts=4 sw=4:

