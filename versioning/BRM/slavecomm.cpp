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

/*****************************************************************************
 * $Id: slavecomm.cpp 1839 2013-02-01 17:42:03Z pleblanc $
 *
 ****************************************************************************/
#include <unistd.h>
#include <iostream>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <cstdio>
#include <ctime>
#ifdef _MSC_VER
#include <io.h>
#include <psapi.h>
#endif

#include "messagequeue.h"
#include "bytestream.h"
#include "socketclosed.h"
#include "configcpp.h"
#define SLAVECOMM_DLLEXPORT
#include "slavecomm.h"
#undef SLAVECOMM_DLLEXPORT

using namespace std;
using namespace messageqcpp;

namespace {
void timespec_sub(const struct timespec &tv1,
	const struct timespec &tv2,
	double &tm)
{
    tm = (double)(tv2.tv_sec - tv1.tv_sec) + 1.e-9*(tv2.tv_nsec - tv1.tv_nsec);
}
}

namespace BRM {

SlaveComm::SlaveComm(string hostname, SlaveDBRMNode *s) :
	slave(s)
#ifdef _MSC_VER
	, fPids(0), fMaxPids(64)
#endif
{
	config::Config *config = config::Config::makeConfig();
	string tmp;

	bool tellUser = true;
    for (;;)
    {
		try {
			server = new MessageQueueServer(hostname);
            break;
		}
		catch (runtime_error& re) {
            string what = re.what();
            if (what.find("Address already in use") != string::npos)
            {
                if (tellUser)
                {
                    cerr << "Address already in use, retrying..." << endl;
                    tellUser = false;
                }
                sleep(5);
            }
            else
            {
                throw;
            }
        }
	}
	
	/* NOTE: this string has to match whatever is designated as the first slave */
	if (hostname == "DBRM_Worker1") {
		try {
			savefile = config->getConfig("SystemConfig", "DBRMRoot");
		}
		catch (exception &e) {
			savefile = "/tmp/BRM_SaveFiles";
		}
		if (savefile == "") 
			savefile = "/tmp/BRM_SaveFiles";

		tmp = "";
		try {
			tmp = config->getConfig("SystemConfig", "DBRMSnapshotInterval");
		}
		catch (exception &e) { }
		if (tmp == "")
			snapshotInterval = 100000;
		else
			snapshotInterval = config->fromText(tmp);
		journalCount = 0;

		firstSlave = true;
		currentSaveFD = -1;
		journalName = savefile + "_journal";
		uint utmp = ::umask(0);
		journal.open(journalName.c_str(), ios_base::binary | ios_base::out);
		::umask(utmp);
		if (!journal)
			throw runtime_error("Could not open the BRM journal for writing!");
	}
	else {
		savefile = "";
		firstSlave = false;
	}

	takeSnapshot = false;
	doSaveDelta = false;
	saveFileToggle = true;	// start with the suffix "A" rather than "B".  Arbitrary.
	release = false;
	die = false;
	standalone = false;
	printOnly = false;
	//@Bug 2258 DBRMTimeOut is default to 20 seconds
	//@BUG 3189 set timeout to 1 second, don't use config setting
//	std::string retStr = config->getConfig("SystemConfig", "DBRMTimeOut");
//	int secondsToWait = config->fromText(retStr);
	MSG_TIMEOUT.tv_nsec = 0;
//	if ( secondsToWait > 0 )
//		MSG_TIMEOUT.tv_sec = secondsToWait;
//	else
	MSG_TIMEOUT.tv_sec = 1;
}

SlaveComm::SlaveComm()
#ifdef _MSC_VER
	: fPids(0), fMaxPids(64)
#endif
{
	config::Config *config = config::Config::makeConfig();

	try {
		savefile = config->getConfig("SystemConfig", "DBRMRoot");
	}
	catch (exception &e) {
		savefile = "/tmp/BRM_SaveFiles";
	}
	if (savefile == "") 
		savefile = "/tmp/BRM_SaveFiles";

	journalName = savefile + "_journal";

	takeSnapshot = false;
	doSaveDelta = false;
	saveFileToggle = true;	// start with the suffix "A" rather than "B".  Arbitrary.
	release = false;
	die = false;
	firstSlave = false;
	server = NULL;
	standalone = true;
	printOnly = false;
	slave = new SlaveDBRMNode();
}

SlaveComm::~SlaveComm()
{
	delete server;
	if (firstSlave)
		close(currentSaveFD);
}

void SlaveComm::stop()
{
	die = true;
}

void SlaveComm::reset()
{
        release = true;
}

void SlaveComm::run()
{
	ByteStream msg;

	while (!die) {
#ifdef BRM_VERBOSE
//		cerr << "WorkerComm: waiting for a connection" << endl;
#endif
		master = server->accept(&MSG_TIMEOUT);
		while (!die && master.isOpen()) {
			try {
				msg = master.read(&MSG_TIMEOUT);
			}
			catch (SocketClosed &e) {
#ifdef BRM_VERBOSE
				cerr << "WorkerComm: remote closed" << endl;
#endif
				break;
			}
			catch (...) {
#ifdef BRM_VERBOSE
				cerr << "WorkerComm: read failed, closing connection" << endl;
#endif
				break;
			}
			if (release)
				break;
			if (die)   // || msg.length() == 0)
				break;
  			if (msg.length() == 0)
  				continue;

#ifdef BRM_VERBOSE
			cerr << "WorkerComm: got a command" << endl;
#endif
			try {
				processCommand(msg);
			}
			catch (exception &e) {
				/* 
				 * The error is either that msg was too short (really slow sender possibly),
				 * there was a bigger communication failure, or there was a file IO
				 * error.  Closing the connection for now.
				 */
				cerr << e.what() << endl;
				do_undo();
				master.close();
			}
		}
		release = false;
		master.close();
	}
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: exiting..." << endl;
#endif
}

void SlaveComm::processCommand(ByteStream &msg)
{
	uint8_t cmd;

	if (firstSlave) {
		msg.peek(cmd);
		if (cmd != CONFIRM)
			delta = msg;
	}
	msg >> cmd;
#ifdef BRM_VERBOSE
    cerr << "WorkerComm: command " << (int) cmd << endl;
#endif
    switch (cmd) {
		case CREATE_STRIPE_COLUMN_EXTENTS:
			do_createStripeColumnExtents(msg); break;
		case CREATE_COLUMN_EXTENT_DBROOT:
			do_createColumnExtent_DBroot(msg); break;
		case CREATE_COLUMN_EXTENT_EXACT_FILE:
			do_createColumnExtentExactFile(msg); break;
		case CREATE_DICT_STORE_EXTENT: do_createDictStoreExtent(msg); break;
		case ROLLBACK_COLUMN_EXTENTS_DBROOT:
			do_rollbackColumnExtents_DBroot(msg); break;
		case ROLLBACK_DICT_STORE_EXTENTS_DBROOT:
			do_rollbackDictStoreExtents_DBroot(msg); break;
		case DELETE_EMPTY_COL_EXTENTS:do_deleteEmptyColExtents(msg); break;
		case DELETE_EMPTY_DICT_STORE_EXTENTS:
			do_deleteEmptyDictStoreExtents(msg); break;
		case DELETE_OID: do_deleteOID(msg); break;
		case DELETE_OIDS: do_deleteOIDs(msg); break;
		case SET_LOCAL_HWM: do_setLocalHWM(msg); break;
		case BULK_SET_HWM: do_bulkSetHWM(msg); break;
		case BULK_SET_HWM_AND_CP: do_bulkSetHWMAndCP(msg); break;
		case WRITE_VB_ENTRY: do_writeVBEntry(msg); break;
		case BEGIN_VB_COPY: do_beginVBCopy(msg); break;
		case END_VB_COPY: do_endVBCopy(msg); break;
		case VB_ROLLBACK1: do_vbRollback1(msg); break;
		case VB_ROLLBACK2: do_vbRollback2(msg); break;
		case VB_COMMIT: do_vbCommit(msg); break;
		case BRM_UNDO: do_undo(); break;
		case CONFIRM: do_confirm(); break;
		case FLUSH_INODE_CACHES: do_flushInodeCache(); break;
		case BRM_CLEAR: do_clear(); break;
		case MARKEXTENTINVALID: do_markInvalid(msg); break;
		case MARKMANYEXTENTSINVALID: do_markManyExtentsInvalid(msg); break;
		case SETEXTENTMAXMIN: do_setExtentMaxMin(msg); break;
		case SETMANYEXTENTSMAXMIN: do_setExtentsMaxMin(msg); break;
		case TAKE_SNAPSHOT: do_takeSnapshot(); break;
		case MERGEMANYEXTENTSMAXMIN: do_mergeExtentsMaxMin(msg); break;
		case DELETE_PARTITION: do_deletePartition(msg); break;
		case MARK_PARTITION_FOR_DELETION: do_markPartitionForDeletion(msg); break;
		case MARK_ALL_PARTITION_FOR_DELETION: do_markAllPartitionForDeletion(msg); break;
		case RESTORE_PARTITION: do_restorePartition(msg); break;
		case OWNER_CHECK: do_ownerCheck(msg); break;
		case LOCK_LBID_RANGES: do_dmlLockLBIDRanges(msg); break;
		case RELEASE_LBID_RANGES: do_dmlReleaseLBIDRanges(msg); break;
		case DELETE_DBROOT: do_deleteDBRoot(msg); break;
		case BULK_UPDATE_DBROOT: do_bulkUpdateDBRoot(msg); break;

		default:
			cerr << "WorkerComm: unknown command " << (int) cmd << endl;
	}
}

//------------------------------------------------------------------------------
// Process a request to create a column extent for a specific OID and DBRoot.
//------------------------------------------------------------------------------
void SlaveComm::do_createStripeColumnExtents(ByteStream &msg)
{
	int        err;
	uint16_t   tmp16;
	uint16_t   tmp32;
	uint16_t   dbRoot;
	uint32_t   partitionNum;
	uint16_t   segmentNum;
	std::vector<CreateStripeColumnExtentsArgIn>  cols;
	std::vector<CreateStripeColumnExtentsArgOut> extents;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createStripeColumnExtents()" << endl;
#endif

	deserializeInlineVector(msg, cols);
	msg >> tmp16;
	dbRoot = tmp16;
	msg >> tmp32;
	partitionNum = tmp32;

	if (printOnly) {
		cout << "createStripeColumnExtents().  " <<
			"DBRoot=" << dbRoot << "; Part#=" << partitionNum << endl;
		for (uint i = 0; i < cols.size(); i++)
			cout << "StripeColExt arg "    << i + 1 <<
				": oid="  << cols[i].oid   <<
				" width=" << cols[i].width << endl;
		return;
	}

	err = slave->createStripeColumnExtents(cols,dbRoot,
		partitionNum, segmentNum, extents);
	reply << (uint8_t) err;
	if (err == ERR_OK) {
		reply << partitionNum;
		reply << segmentNum;
		serializeInlineVector( reply, extents );
	}

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createStripeColumnExtents() err code is " <<
		err << endl;
#endif

	if (!standalone)
		master.write(reply);

	// see bug 3596.  Need to make sure a snapshot file exists.
	if ((cols.size() > 0) && (cols[0].oid < 3000))
		takeSnapshot = true;
	else
		doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Process a request to create a column extent for a specific OID and DBRoot.
//------------------------------------------------------------------------------
void SlaveComm::do_createColumnExtent_DBroot(ByteStream &msg)
{
	int allocdSize, err;
	uint8_t    tmp8;
	uint16_t   tmp16;
	uint32_t   tmp32;
	OID_t      oid;
	uint32_t   colWidth;
	uint16_t   dbRoot;
	uint32_t   partitionNum;
	uint16_t   segmentNum;
	LBID_t     lbid;
	uint32_t   startBlockOffset;
	ByteStream reply;
	execplan::CalpontSystemCatalog::ColDataType colDataType;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createColumnExtent_DBroot()" << endl;
#endif

	msg >> tmp32;
	oid = tmp32;
	msg >> tmp32;
	colWidth = tmp32;
	msg >> tmp16;
	dbRoot = tmp16;
	msg >> tmp32;
	partitionNum = tmp32;
	msg >> tmp16;
	segmentNum = tmp16;
	msg >> tmp8;
	colDataType = (execplan::CalpontSystemCatalog::ColDataType)tmp8;

	if (printOnly) {
		cout << "createColumnExtent_DBroot: oid=" << oid <<
			" colWidth=" << colWidth <<
			" dbRoot="   << dbRoot   <<
			" partitionNum=" << partitionNum <<
			" segmentNum=" << segmentNum << endl;
		return;
	}

	err = slave->createColumnExtent_DBroot(oid, colWidth, dbRoot, colDataType,
		partitionNum, segmentNum, lbid, allocdSize, startBlockOffset);
	reply << (uint8_t) err;
	if (err == ERR_OK) {
		reply << partitionNum;
		reply << segmentNum;
		reply << (uint64_t) lbid;
		reply << (uint32_t) allocdSize;
		reply << (uint32_t) startBlockOffset;
	}

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createColumnExtent_DBroot() err code is " <<
		err << endl;
#endif
	if (!standalone)
		master.write(reply);
	if (oid < 3000)  // see bug 3596.  Need to make sure a snapshot file exists.
		takeSnapshot = true;
	else
		doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Process a request to create a column extent for the exact segment file
// specified by the requested OID, DBRoot, partition, and segment.
//------------------------------------------------------------------------------
void SlaveComm::do_createColumnExtentExactFile(ByteStream &msg)
{
	int allocdSize, err;
	uint8_t    tmp8;
	uint16_t   tmp16;
	uint32_t   tmp32;
	OID_t      oid;
	uint32_t   colWidth;
	uint16_t   dbRoot;
	uint32_t   partitionNum;
	uint16_t   segmentNum;
	LBID_t     lbid;
	uint32_t   startBlockOffset;
	ByteStream reply;
	execplan::CalpontSystemCatalog::ColDataType colDataType;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createColumnExtentExactFile()" << endl;
#endif

	msg >> tmp32;
	oid = tmp32;
	msg >> tmp32;
	colWidth = tmp32;
	msg >> tmp16;
	dbRoot = tmp16;
	msg >> tmp32;
	partitionNum = tmp32;
	msg >> tmp16;
	segmentNum = tmp16;
	msg >> tmp8;
	colDataType = (execplan::CalpontSystemCatalog::ColDataType)tmp8;
	if (printOnly) {
		cout << "createColumnExtentExactFile: oid=" << oid <<
			" colWidth=" << colWidth <<
			" dbRoot="   << dbRoot   <<
			" partitionNum=" << partitionNum <<
			" segmentNum=" << segmentNum << endl;
		return;
	}

	err = slave->createColumnExtentExactFile(oid, colWidth, dbRoot,
		partitionNum, segmentNum, colDataType, lbid, allocdSize, startBlockOffset);
	reply << (uint8_t) err;
	if (err == ERR_OK) {
		reply << partitionNum;
		reply << segmentNum;
		reply << (uint64_t) lbid;
		reply << (uint32_t) allocdSize;
		reply << (uint32_t) startBlockOffset;
	}

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createColumnExtentExactFile() err code is " <<
		err << endl;
#endif
	if (!standalone)
		master.write(reply);
	if (oid < 3000)  // see bug 3596.  Need to make sure a snapshot file exists.
		takeSnapshot = true;
	else
		doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Process a request to create a dictionary store extent.
//------------------------------------------------------------------------------
void SlaveComm::do_createDictStoreExtent(ByteStream &msg)
{
	int allocdSize, err;
	uint16_t   tmp16;
	uint32_t   tmp32;
	OID_t      oid;
	uint16_t   dbRoot;
	uint32_t   partitionNum;
	uint16_t   segmentNum;
	LBID_t     lbid;
	ByteStream reply;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createDictStoreExtent()" << endl;
#endif

	msg >> tmp32;
	oid = tmp32;
	msg >> tmp16;
	dbRoot = tmp16;
	msg >> tmp32;
	partitionNum = tmp32;
	msg >> tmp16;
	segmentNum = tmp16;
	if (printOnly) {
		cout << "createDictStoreExtent: oid=" << oid << " dbRoot=" << dbRoot << 
			" partitionNum=" << partitionNum << " segmentNum=" << segmentNum << endl;
		return;
	}

	err = slave->createDictStoreExtent(oid, dbRoot,
		partitionNum, segmentNum, lbid, allocdSize);
	reply << (uint8_t) err;
	if (err == ERR_OK) {
		reply << (uint64_t) lbid;
		reply << (uint32_t) allocdSize;
	}

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_createDictStoreExtent() err code is " << err <<endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Process a request to rollback (delete) a set of column extents.
// for a given OID and DBRoot.
//------------------------------------------------------------------------------
void SlaveComm::do_rollbackColumnExtents_DBroot(ByteStream &msg)
{
	int        err;
	OID_t      oid;
	bool       bDeleteAll;
	uint32_t   partitionNum;
	uint16_t   segmentNum;
	uint16_t   dbRoot;
	HWM_t      hwm;
	uint8_t    tmp8;
	uint16_t   tmp16;
	uint32_t   tmp32;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_rollbackColumnExtents_DBroot()" << endl;
#endif

	msg >> tmp32;
	oid = tmp32;
	msg >> tmp8;
	bDeleteAll = tmp8;
	msg >> tmp16;
	dbRoot = tmp16;
	msg >> tmp32;
	partitionNum = tmp32;
	msg >> tmp16;
	segmentNum = tmp16;
	msg >> tmp32;
	hwm = tmp32;

	if (printOnly) {
		cout << "rollbackColumnExtents_DBroot: oid=" << oid <<
			" bDeleteAll=" << bDeleteAll << " dbRoot=" << dbRoot <<
			" partitionNum=" << partitionNum <<
			" segmentNum=" << segmentNum << " hwm=" << hwm << endl;
		return;
	}

	err = slave->rollbackColumnExtents_DBroot(
		oid, bDeleteAll, dbRoot, partitionNum, segmentNum, hwm);
	reply << (uint8_t) err;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_rollbackColumnExtents_DBroot() err code is " <<
		err << endl;
#endif

	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Process a request to rollback (delete) a set of column extents.
// for a given OID and DBRoot.
//------------------------------------------------------------------------------
void SlaveComm::do_rollbackDictStoreExtents_DBroot(ByteStream &msg)
{
	int        err;
	OID_t      oid;
	uint32_t   partitionNum;
	uint16_t   dbRoot;
	uint32_t   tmp32;
	uint16_t   tmp16;
	ByteStream reply;
	vector<uint16_t> segNums;
	vector<HWM_t> hwms;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_rollbackDictStoreExtents()" << endl;
#endif

	msg >> tmp32;
	oid = tmp32;
	msg >> tmp16;
	dbRoot = tmp16;
	msg >> tmp32;
	partitionNum = tmp32;
	deserializeVector(msg, segNums);
	deserializeVector(msg, hwms);
	
	if (printOnly) {
		cout << "rollbackDictStore: oid=" << oid <<
			" dbRoot=" << dbRoot <<
			" partitionNum=" << partitionNum <<
			" hwms..." << endl;
		for (uint i = 0; i < hwms.size(); i++)
			cout << "   " << i << ": " << hwms[i] << endl;
		return;
	}
		
	
	err = slave->rollbackDictStoreExtents_DBroot(
		oid, dbRoot, partitionNum, segNums, hwms);
	reply << (uint8_t) err;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_rollbackDictStoreExtents() err code is " <<
		err << endl;
#endif

	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_deleteEmptyColExtents(messageqcpp::ByteStream &msg)
{
	OID_t oid;
	uint32_t tmp1;
	uint16_t tmp2;
	int err;
	ByteStream reply;
	uint32_t size;
	ExtentsInfoMap_t extentsInfoMap;
	
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteEmptyColExtents()" << endl;
#endif
	
	msg >> size;
	if (printOnly)
		cout << "deleteEmptyColExtents: size=" << size << " extentsInfoMap..." << endl;
	
	for ( unsigned i = 0; i < size; i++ )
	{
		msg >> tmp1;
		oid = tmp1;
		extentsInfoMap[oid].oid = oid;
		msg >> tmp1;
		extentsInfoMap[oid].partitionNum = tmp1;
		msg >> tmp2;
		extentsInfoMap[oid].segmentNum = tmp2;
		msg >> tmp2;
		extentsInfoMap[oid].dbRoot = tmp2;	
		msg >> tmp1;
		extentsInfoMap[oid].hwm = tmp1;
		if (printOnly) {
			cout << "   oid=" << oid << " partitionNum=" << extentsInfoMap[oid].partitionNum
				<< " segmentNum=" << extentsInfoMap[oid].segmentNum << " dbRoot=" <<
				extentsInfoMap[oid].dbRoot << " hwm=" << extentsInfoMap[oid].hwm << endl;
		}
	}
	
	if (printOnly)
		return;
	
	err = slave->deleteEmptyColExtents(extentsInfoMap);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteEmptyColExtents() err code is " << err << endl;
#endif
	
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_deleteEmptyDictStoreExtents(messageqcpp::ByteStream &msg)
{
	OID_t oid;
	uint32_t tmp1;
	uint16_t tmp2;
	uint8_t  tmp3;
	int err;
	ByteStream reply;
	uint32_t size;
	ExtentsInfoMap_t extentsInfoMap;
	
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteEmptyDictStoreExtents()" << endl;
#endif
	
	msg >> size;
	
	if (printOnly)
		cout << "deleteEmptyDictStoreExtents: size=" << size << " extentsInfoMap..." << endl;
	
	for ( unsigned i = 0; i < size; i++ )
	{
		msg >> tmp1;
		oid = tmp1;
		extentsInfoMap[oid].oid = oid;
		msg >> tmp1;
		extentsInfoMap[oid].partitionNum = tmp1;
		msg >> tmp2;
		extentsInfoMap[oid].segmentNum = tmp2;
		msg >> tmp2;
		extentsInfoMap[oid].dbRoot = tmp2;	
		msg >> tmp1;
		extentsInfoMap[oid].hwm = tmp1;
		msg >> tmp3;
		extentsInfoMap[oid].newFile = (bool) tmp3;
		if (printOnly) {
			cout << "  oid=" << oid << " partitionNum=" << extentsInfoMap[oid].partitionNum <<
				" segmentNum=" << extentsInfoMap[oid].segmentNum << " dbRoot=" <<
				extentsInfoMap[oid].dbRoot << " hwm=" << extentsInfoMap[oid].hwm <<
				" newFile=" << (int) extentsInfoMap[oid].newFile << endl;
		}
	}
	
	if (printOnly)
		return;
	
	err = slave->deleteEmptyDictStoreExtents(extentsInfoMap);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteEmptyDictStoreExtents() err code is " << err << endl;
#endif
	
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_deleteOID(ByteStream &msg)
{
	OID_t oid;
	uint32_t tmp;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteOID()" << endl;
#endif
	
	msg >> tmp;
	oid = tmp;

	if (printOnly) {
		cout << "deleteOID: oid=" << oid << endl;
		return;
	}

	err = slave->deleteOID(oid);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteOID() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_deleteOIDs(ByteStream &msg)
{
	OID_t oid;
	uint32_t tmp;
	int err;
	ByteStream reply;
	uint32_t size;
	std::vector<OID_t> Oids;
	OidsMap_t oidsMap;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteOIDs()" << endl;
#endif
	
	msg >> size;
	
	if (printOnly)
		cout << "deleteOIDs: size=" << size << endl;
	
	for ( unsigned i = 0; i < size; i++ )
	{
		msg >> tmp;
		oid = tmp;
		oidsMap[oid] = oid;
		if (printOnly)
			cout << "  oid=" << oid << endl;
	}
	
	if (printOnly)
		return;
	
	err = slave->deleteOIDs(oidsMap);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteOIDs() err code is " << err << endl;
#endif

	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Process a request to set the local HWM relative to a specific OID, partition,
// and segment number.
//------------------------------------------------------------------------------
void SlaveComm::do_setLocalHWM(ByteStream &msg)
{
	OID_t      oid;
	HWM_t      hwm;
	uint32_t   partitionNum;
	uint16_t   segmentNum;
	int        err;
	uint16_t   tmp16;
	uint32_t   tmp32;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setLocalHWM()" << endl;
#endif

	msg >> tmp32;
	oid = tmp32;
	msg >> tmp32;
	partitionNum = tmp32;
	msg >> tmp16;
	segmentNum = tmp16;
	msg >> tmp32;
	hwm = tmp32;

	if (printOnly) {
		cout << "setLocalHWM: oid=" << oid << " partitionNum=" << partitionNum <<
			" segmentNum=" << segmentNum << " hwm=" << hwm << endl;
		return;
	}

	err = slave->setLocalHWM(oid, partitionNum, segmentNum, hwm, firstSlave);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setLocalHWM() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_bulkSetHWM(ByteStream &msg)
{
	vector<BulkSetHWMArg> args;
	int        err;
	VER_t transID;
	uint32_t tmp32;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setLocalHWM()" << endl;
#endif

	deserializeInlineVector(msg, args);
	msg >> tmp32;
	transID = tmp32;

	if (printOnly) {
		cout << "bulkSetHWM().  TransID = " << transID << endl;
		for (uint i = 0; i < args.size(); i++)
			cout << "bulkSetHWM arg " << i + 1 << ": oid=" << args[i].oid << " partitionNum=" << args[i].partNum <<
					" segmentNum=" << args[i].segNum << " hwm=" << args[i].hwm << endl;
		return;
	}

	err = slave->bulkSetHWM(args, transID, firstSlave);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setLocalHWM() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_bulkSetHWMAndCP(ByteStream &msg)
{
	vector<BulkSetHWMArg> hwmArgs;
	vector<CPInfo> setCPDataArgs;
	vector<CPInfoMerge> mergeCPDataArgs;
	int        err;
	VER_t transID;
	uint32_t tmp32;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setLocalHWM()" << endl;
#endif

	deserializeInlineVector(msg, hwmArgs);
	deserializeInlineVector(msg, setCPDataArgs);
	deserializeInlineVector(msg, mergeCPDataArgs);
	msg >> tmp32;
	transID = tmp32;

#if 0
	if (printOnly) {
		cout << "bulkSetHWM().  TransID = " << transID << endl;
		for (uint i = 0; i < args.size(); i++)
			cout << "bulkSetHWM arg " << i + 1 << ": oid=" << args[i].oid << " partitionNum=" << args[i].partNum <<
					" segmentNum=" << args[i].segNum << " hwm=" << args[i].hwm << endl;
		return;
	}
#endif

	err = slave->bulkSetHWMAndCP(hwmArgs, setCPDataArgs, mergeCPDataArgs, transID, firstSlave);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setLocalHWM() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_bulkUpdateDBRoot(ByteStream &msg)
{
	vector<BulkUpdateDBRootArg> args;
	ByteStream reply;
	int err;

	deserializeInlineVector(msg, args);
	err = slave->bulkUpdateDBRoot(args);
	reply << (uint8_t) err;
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_markInvalid(ByteStream &msg)
{
	LBID_t lbid;
	uint32_t colDataType;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markInvalid()" << endl;
#endif

	msg >> lbid;
	msg >> colDataType;
	
	if (printOnly) {
		cout << "markExtentInvalid: lbid=" << lbid << "colDataType=" << colDataType << endl;
		return;
	}
	
	err = slave->markExtentInvalid(lbid, (execplan::CalpontSystemCatalog::ColDataType)colDataType);
	reply << (uint8_t)err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markInvalid() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_markManyExtentsInvalid(ByteStream &msg)
{
	uint64_t tmp64;
	uint32_t colDataType;
	int err;
	ByteStream reply;
	vector<LBID_t> lbids;
	vector<execplan::CalpontSystemCatalog::ColDataType> colDataTypes;
	uint32_t size, i;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markManyExtentsInvalid()" << endl;
#endif

	msg >> size;
	
	if (printOnly)
		cout << "markManyExtentsInvalid: size=" << size << " lbids..." << endl;
	
	for (i = 0; i < size; ++i) {
		msg >> tmp64;
		msg >> colDataType;
		lbids.push_back(tmp64);
		colDataTypes.push_back((execplan::CalpontSystemCatalog::ColDataType)colDataType);
		if (printOnly)
			cout << "   " << tmp64 << " " << colDataType << endl;
	}

	if (printOnly)
		return;
		
	err = slave->markExtentsInvalid(lbids, colDataTypes);
	reply << (uint8_t)err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markManyExtentsInvalid() err code is " << err<<endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_setExtentMaxMin(ByteStream &msg)
{
	LBID_t lbid;
	int64_t max;
	int64_t min;
	int32_t sequence;
	uint64_t tmp64;
	uint32_t tmp32;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setExtentMaxMin()" << endl;
#endif

	msg >>tmp64;
	lbid=tmp64;

	msg >> tmp64;
	max = tmp64;

	msg >> tmp64;
	min = tmp64;

	msg >> tmp32;
	sequence = tmp32;

	if (printOnly) {
		cout << "setExtentMaxMin: lbid=" << lbid << " max=" << max << " min=" << min << 
			" sequence=" << sequence << endl;
		return;
	}

	err = slave->setExtentMaxMin(lbid, max, min, sequence, firstSlave);
	reply << (uint8_t)err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setExtentMaxMin() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

// @bug 1970 - added do_setExtentsMaxMin to set multiple extents CP info.
void SlaveComm::do_setExtentsMaxMin(ByteStream &msg)
{
	LBID_t lbid;
	uint64_t tmp64;
	uint32_t tmp32;
	int err;
	ByteStream reply;
	int32_t updateCount;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setExtentsMaxMin()" << endl;
#endif

	msg >> tmp32;
	updateCount = tmp32;
	CPMaxMinMap_t cpMap;
	CPMaxMin cpMaxMin;

	if (printOnly)
		cout << "setExtentsMaxMin: size=" << updateCount << " CPdata..." << endl;
	
	// Loop through extents and add each one to a map.  
	for(int64_t i = 0; i < updateCount; i++)
	{
		msg >> tmp64;
		lbid = tmp64;

		msg >> tmp64;
		cpMaxMin.max = tmp64;

		msg >> tmp64;
		cpMaxMin.min = tmp64;

		msg >> tmp32;
		cpMaxMin.seqNum = tmp32;	

		cpMap[lbid] = cpMaxMin;
		if (printOnly)
			cout << "   lbid=" << lbid << " max=" << cpMaxMin.max << " min=" << 
				cpMaxMin.min << " sequenceNum=" << cpMaxMin.seqNum << endl;
	}

	if (printOnly)
		return;

	err = slave->setExtentsMaxMin(cpMap, firstSlave);
	reply << (uint8_t)err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_setExtentsMaxMin() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// @bug 2117 - added do_mergeExtentsMaxMin to merge multiple extents CP info.
//------------------------------------------------------------------------------
void SlaveComm::do_mergeExtentsMaxMin(ByteStream &msg)
{
	LBID_t     startLbid;
	uint64_t   tmp64;
	uint32_t   tmp32;
	int        err;
	ByteStream reply;
	int32_t    mergeCount;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_mergeExtentsMaxMin()" << endl;
#endif

	msg >> tmp32;
	mergeCount = tmp32;
	CPMaxMinMergeMap_t cpMap;
	CPMaxMinMerge      cpMaxMin;

	if (printOnly)
		cout << "mergeExtentsMaxMin: size=" << mergeCount << " CPdata..." << endl;

	// Loop through extents and add each one to a map.  
	for(int64_t i = 0; i < mergeCount; i++)
	{
		msg >> tmp64;
		startLbid = tmp64;

		msg >> tmp64;
		cpMaxMin.max = tmp64;

		msg >> tmp64;
		cpMaxMin.min = tmp64;

		msg >> tmp32;
		cpMaxMin.seqNum = tmp32;	

		msg >> tmp32;
		cpMaxMin.type = (execplan::CalpontSystemCatalog::ColDataType)tmp32;	

		msg >> tmp32;
		cpMaxMin.newExtent = tmp32;	

		cpMap[startLbid] = cpMaxMin;
		if (printOnly)
			cout << "   startLBID=" << startLbid << " max=" << cpMaxMin.max << " min=" <<
				cpMaxMin.min << " sequenceNum=" << cpMaxMin.seqNum << " type=" << (int)
				cpMaxMin.type << " newExtent=" << (int) cpMaxMin.newExtent << endl;
	}

	if (printOnly)
		return;
		
	err = slave->mergeExtentsMaxMin(cpMap);
	reply << (uint8_t)err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_mergeExtentsMaxMin() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Delete all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
void SlaveComm::do_deletePartition(ByteStream &msg)
{
	OID_t      oid;
	uint32_t   tmp32;
	int        err;
	ByteStream reply;
	uint32_t   size;
	std::set<OID_t> oids;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deletePartition()" << endl;
#endif
	
	set<LogicalPartition> partitionNums;
	deserializeSet<LogicalPartition>(msg, partitionNums);

	msg >> size;
	
	if (printOnly)
	{
		cout << "deletePartition: partitionNum: "; 
		set<LogicalPartition>::const_iterator it;
		for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
			cout << (*it) << " ";
		cout << "\nsize=" << size << " oids..." << endl;
	}
	for (unsigned i=0; i<size; i++)
	{
		msg >> tmp32;
		oid = tmp32;
		oids.insert( oid );
		if (printOnly)
			cout << "   " << oid << endl;
	}

	if (printOnly)
		return;
	
	string emsg;
	err = slave->deletePartition(oids, partitionNums, emsg);
	reply << (uint8_t) err;
	if (err != 0)
		reply << emsg;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deletePartition() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
//------------------------------------------------------------------------------
void SlaveComm::do_markPartitionForDeletion(ByteStream &msg)
{
	OID_t      oid;
	uint32_t   tmp32;
	int        err;
	ByteStream reply;
	uint32_t   size;
	std::set<OID_t> oids;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markPartitionForDeletion()" << endl;
#endif

	set<LogicalPartition> partitionNums;
	deserializeSet<LogicalPartition>(msg, partitionNums);
	msg >> size;
	
	if (printOnly)
	{
		cout << "markPartitionForDeletion: partitionNum: ";
		set<LogicalPartition>::const_iterator it;
		for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
			cout << (*it) << " ";
		cout << "\nsize=" << size << " oids..." << endl;
	}
	for (unsigned i=0; i<size; i++)
	{
		msg >> tmp32;
		oid = tmp32;
		oids.insert( oid );
		if (printOnly)
			cout << "   " << oid << endl;
	}

	if (printOnly)
		return;
	
	string emsg;
	err = slave->markPartitionForDeletion(oids, partitionNums, emsg);
	reply << (uint8_t) err;
	if (err != 0)
		reply << emsg;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markPartitionforDeletion() err code is " <<
		err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
//------------------------------------------------------------------------------
void SlaveComm::do_markAllPartitionForDeletion(ByteStream &msg)
{
	OID_t      oid;
	uint32_t   tmp32;
	int        err;
	ByteStream reply;
	uint32_t   size;
	std::set<OID_t> oids;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markAllPartitionForDeletion()" << endl;
#endif
	msg >> size;
	
	if (printOnly)
		cout << "markAllPartitionForDeletion: size=" 
			<< size << " oids..." << endl;
	
	for (unsigned i=0; i<size; i++)
	{
		msg >> tmp32;
		oid = tmp32;
		oids.insert( oid );
		if (printOnly)
			cout << "   " << oid << endl;
	}

	if (printOnly)
		return;
		
	err = slave->markAllPartitionForDeletion(oids);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_markAllPartitionforDeletion() err code is " <<
		err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Restore all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
void SlaveComm::do_restorePartition(ByteStream &msg)
{
	OID_t      oid;
	uint32_t   tmp32;
	int        err;
	ByteStream reply;
	uint32_t   size;
	std::set<OID_t> oids;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_restorePartition()" << endl;
#endif
	
	set<LogicalPartition> partitionNums;
	deserializeSet<LogicalPartition>(msg, partitionNums);

	msg >> size;
	
	if (printOnly)
	{
		cout << "restorePartition: partitionNum: ";
		set<LogicalPartition>::const_iterator it;
		for (it = partitionNums.begin(); it != partitionNums.end(); ++it)
			cout << (*it) << " ";
		cout << "\nsize=" << size << " oids..." << endl;
	}
	
	for (unsigned i=0; i<size; i++)
	{
		msg >> tmp32;
		oid = tmp32;
		oids.insert( oid );
		if (printOnly)
			cout << "   " << oid << endl;
	}

	if (printOnly)
		return;
	
	string emsg;
	err = slave->restorePartition(oids, partitionNums, emsg);
	reply << (uint8_t) err;
	if (err != 0)
		reply << emsg;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_restorePartition() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

//------------------------------------------------------------------------------
// Delete all extents for the specified dbroot
//------------------------------------------------------------------------------
void SlaveComm::do_deleteDBRoot(ByteStream &msg)
{
	int        err;
	ByteStream reply;
	uint32_t   q;
	uint16_t   dbroot;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteDBroot()" << endl;
#endif
	
	msg >> q;
	dbroot = static_cast<uint16_t>(q);
	
	if (printOnly)
	{
		cout << "deleteDBRoot: " << dbroot << endl; 
		return;
	}
	
	err = slave->deleteDBRoot(dbroot);
	reply << (uint8_t) err;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_deleteDBRoot() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_writeVBEntry(ByteStream &msg)
{
	VER_t transID;
	LBID_t lbid;
	OID_t vbOID;
	uint32_t vbFBO, tmp;
	uint64_t tmp64;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_writeVBEntry()" << endl;
#endif

	msg >> tmp;
	transID = tmp;
	msg >> tmp64;
	lbid = tmp64;
	msg >> tmp;
	vbOID = tmp;
	msg >> vbFBO;
	
	if (printOnly) {
		cout << "writeVBEntry: transID=" << transID << " lbid=" << lbid << " vbOID=" <<
			vbOID << " vbFBO=" << vbFBO << endl;
		return;
	}

	err = slave->writeVBEntry(transID, lbid, vbOID, vbFBO);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_writeVBEntry() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_beginVBCopy(ByteStream &msg)
{
	VER_t transID;
	LBIDRange_v ranges;
	VBRange_v freeList;
	uint32_t tmp32;
	uint16_t dbRoot;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_beginVBCopy()" << endl;
#endif

	msg >> tmp32;
	transID = tmp32;
	msg >> dbRoot;
	deserializeVector(msg, ranges);
	
	if (printOnly) {
		cout << "beginVBCopy: transID=" << transID << " dbRoot=" << dbRoot << " size="
				<< ranges.size() <<	" ranges..." << endl;
		for (uint i = 0; i < ranges.size(); i++)
			cout << "   start=" << ranges[i].start << " size=" << ranges[i].size << endl;
		return;
	}
	
	err = slave->beginVBCopy(transID, dbRoot, ranges, freeList, firstSlave && !standalone);
	reply << (uint8_t) err;
	if (err == ERR_OK)
		serializeVector(reply, freeList);
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_beginVBCopy() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_endVBCopy(ByteStream &msg)
{
	VER_t transID;
	LBIDRange_v ranges;
	uint32_t tmp;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_endVBCopy()" << endl;
#endif

	msg >> tmp;
	transID = tmp;
	deserializeVector(msg, ranges);
	
	if (printOnly) {
		cout << "endVBCopy: transID=" << transID << " size=" << ranges.size() << 
			" ranges..." << endl;
		for (uint i = 0; i < ranges.size(); i++)
			cout << "   start=" << ranges[i].start << " size=" << ranges[i].size << endl;
		return;
	}
	
	err = slave->endVBCopy(transID, ranges);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_endVBCopy() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_vbRollback1(ByteStream &msg)
{
	VER_t transID;
	LBIDRange_v lbidList;
	uint32_t tmp;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_vbRollback1()" << endl;
#endif

	msg >> tmp;
	transID = tmp;
	deserializeVector(msg, lbidList);
	
	if (printOnly) {
		cout << "vbRollback1: transID=" << transID << " size=" << lbidList.size() <<
			" lbids..." << endl;
		for (uint i = 0; i < lbidList.size(); i++)
			cout << "   start=" << lbidList[i].start << " size=" << lbidList[i].size << endl;
		return;
	}
	
	err = slave->vbRollback(transID, lbidList, firstSlave && !standalone);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_vbRollback1() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	takeSnapshot = true;
}

void SlaveComm::do_vbRollback2(ByteStream &msg)
{
	VER_t transID;
	vector<LBID_t> lbidList;
	uint32_t tmp;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_vbRollback2()" << endl;
#endif

	msg >> tmp;
	transID = tmp;
	deserializeVector(msg, lbidList);
	
	if (printOnly) {
		cout << "vbRollback2: transID=" << transID << " size=" << lbidList.size() <<
			" lbids..." << endl;
		for (uint i = 0; i < lbidList.size(); i++)
			cout << "   " << lbidList[i] << endl;
		return;
	}
	
	err = slave->vbRollback(transID, lbidList, firstSlave && !standalone);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_vbRollback2() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	takeSnapshot = true;
}

void SlaveComm::do_vbCommit(ByteStream &msg)
{
	VER_t transID;
	uint32_t tmp;
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_vbCommit()" << endl;
#endif

	msg >> tmp;
	transID = tmp;
	
	if (printOnly) {
		cout << "vbCommit: transID=" << transID << endl;
		return;
	}
	
	err = slave->vbCommit(transID);
	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_vbCommit() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	takeSnapshot = true;
}

void SlaveComm::do_undo()
{
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_undo()" << endl;
#endif

	if (printOnly) {
		cout << "undoChanges" << endl;
		return;
	}

	slave->undoChanges();
	takeSnapshot = false;
	doSaveDelta = false;
}

void SlaveComm::do_confirm()
{
	string tmp;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_confirm()" << endl;
#endif

	if (printOnly) {
		cout << "confirmChanges" << endl;
		return;
	}

	if (firstSlave && doSaveDelta && (journalCount < snapshotInterval || snapshotInterval < 0)) {
		doSaveDelta = false;
		saveDelta();
	}

	slave->confirmChanges();

	if (firstSlave && (takeSnapshot ||
		(journalCount >= snapshotInterval && snapshotInterval >= 0))) {
		if (currentSaveFD < 0) {
			tmp = savefile + "_current";
			currentSaveFD = open(tmp.c_str(), O_WRONLY | O_CREAT, 0666);
			if (currentSaveFD < 0) {
				ostringstream os;
				os << "WorkerComm: failed to open the current savefile. errno: " 
					<< strerror(errno);
				log(os.str());
				throw runtime_error(os.str());
			}
#ifndef _MSC_VER
			fchmod(currentSaveFD, 0666);
#endif
		}
		tmp = savefile + (saveFileToggle ? 'A' : 'B');
		slave->saveState(tmp);
#ifndef _MSC_VER
		tmp += '\n';
#endif
		lseek(currentSaveFD, 0, SEEK_SET);
		int err = write(currentSaveFD, tmp.c_str(), tmp.length());
		if (err < (int) tmp.length()) {
			ostringstream os;
			os << "WorkerComm: currentfile write() returned " << err << " fd is " << currentSaveFD;
			if (err < 0) 
				os << " errno: " << strerror(errno);
			log(os.str());
		}
#ifdef _MSC_VER
		//FIXME: Do we need to account for Windows EOL conversions?
		_chsize_s(currentSaveFD, tmp.length());
		_commit(currentSaveFD);
#else
		err = ftruncate(currentSaveFD, tmp.length());
		fsync(currentSaveFD);
#endif
		saveFileToggle = !saveFileToggle;

		/* Is there a nicer way to truncate the file using an ofstream? */
		journal.close();
		uint utmp = ::umask(0);
		journal.open(journalName.c_str(), ios_base::binary | ios_base::out |
		  ios_base::trunc);
		::umask(utmp);
		takeSnapshot = false;
		doSaveDelta = false;
		journalCount = 0;
	}
}

void SlaveComm::do_flushInodeCache()
{
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_flushInodeCache()" << endl;
#endif

	if (printOnly) {
		cout << "flushInodeCache" << endl;
		return;
	}

#ifdef __linux__
#ifdef USE_VERY_COMPLEX_DROP_CACHES
	double elapsedTime = 0.0;
	char   msgChString[100];
	struct timespec tm1, tm2;
	clock_gettime(CLOCK_REALTIME, &tm1);

	int fd;
	fd = open("/proc/sys/vm/drop_caches", O_WRONLY);
	if (fd >= 0) {
		ssize_t writeCnt = write(fd, "3\n", 2);
		clock_gettime(CLOCK_REALTIME, &tm2);
		timespec_sub(tm1, tm2, elapsedTime);
		if (writeCnt == 2)
		{
			snprintf(msgChString, sizeof(msgChString),
				"WorkerNode updating drop_caches to flush cache; "
				"elapsedTime-%f sec", elapsedTime);
			log(string(msgChString),
				logging::LOG_TYPE_DEBUG);
		}
		else {
			snprintf(msgChString, sizeof(msgChString),
				"WorkerNode unable to update drop_caches and flush cache; "
				"elapsedTime-%f sec", elapsedTime);
			log_errno(string(msgChString),
				logging::LOG_TYPE_WARNING);
		}
		close(fd);
	}
	else {
		clock_gettime(CLOCK_REALTIME, &tm2);
		timespec_sub(tm1, tm2, elapsedTime);
		snprintf(msgChString, sizeof(msgChString),
			"WorkerNode unable to open drop_caches and flush cache; "
			"elapsedTime-%f sec", elapsedTime);
		log_errno(string(msgChString),
			logging::LOG_TYPE_WARNING);
	}
#else
	int fd=-1;
	if ((fd = open("/proc/sys/vm/drop_caches", O_WRONLY)) >= 0) {
		write(fd, "3\n", 2);
		close(fd);
	}
#endif
#endif
	reply << (uint8_t) ERR_OK;
	if (!standalone)
		master.write(reply);
}

void SlaveComm::do_clear()
{
	int err;
	ByteStream reply;

#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_clear()" << endl;
#endif
	
	if (printOnly) {
		cout << "clear" << endl;
		return;
	}
	
	err = slave->clear();
	if (err)
		throw runtime_error("Clear failed.");
	/* This doesn't get confirmed, so we have to save its delta here */
	if (firstSlave)
		saveDelta();

	reply << (uint8_t) (err == 0 ? ERR_OK : ERR_FAILURE);
	if (!standalone)
		master.write(reply);
}

int SlaveComm::replayJournal(string prefix)
{
	ByteStream cmd;
	uint32_t len;
	int ret = 0;
	ifstream journalf;


	// @Bug 2667+  
	// Fix for issue where load_brm was using the journal file from DBRMRoot instead of the one from the command line
	// argument.

	// If A or B files are being loaded, strip off the A or B for the journal file name as there is only one journal file.
	// For example, if prefix is "/usr/local/Calpont/data1/systemFiles/dbrm/BRM_savesA" the journal file name will be
	// "/usr/local/Calpont/data1/systemFiles/dbrm/BRM_saves_journal".

	string tmp = prefix.substr(prefix.length()-1);
	string fName;
	if ((tmp.compare("A") == 0) || (tmp.compare("B") == 0))
	{
		fName = prefix.substr(0, prefix.length()-1) + "_journal";
	}
#ifdef _MSC_VER
	else if (tmp == "a" || tmp == "b")
		fName = prefix.substr(0, prefix.length()-1) + "_journal";
#endif
	else
	{
		fName = prefix + "_journal";
	}

	// journalf.open(journalName.c_str(), ios_base::in | ios_base::binary);
	journalf.open(fName.c_str(), ios_base::in | ios_base::binary);
	if (!journalf.is_open()) {
		cout << "Error opening journal file " << fName << endl;
		return -1;
	}

	// @Bug 2667-
	
	while (journalf) {
		journalf.read((char *) &len, sizeof(len));
		if (!journalf)
			break;
		cmd.needAtLeast(len);
		journalf.read((char *) cmd.getInputPtr(), len);
		cmd.advanceInputPtr(len);
		try {
			processCommand(cmd);
		}
		catch(exception &e) {
			cout << e.what() << "  Journal replay was incomplete." << endl;
			slave->undoChanges();
			return -1;
		}
		slave->confirmChanges();
		cmd.restart();
		ret++;
	}
	return ret;
}

void SlaveComm::do_takeSnapshot()
{
	ByteStream reply;
	
	if (printOnly) {
		cout << "takeSnapshot" << endl;
		return;
	}
	
	takeSnapshot = true;
	do_confirm();
	reply << (uint8_t) 0;
	if (!standalone)
		master.write(reply);
}

void SlaveComm::saveDelta()
{
	try {
		uint32_t len = delta.length();
		journal.seekg(0, ios_base::end);
		journal.write((const char *) &len, sizeof(len));
		journal.write((const char *) delta.buf(), delta.length());
		journal.sync();
		journalCount++;
	}
	catch (exception &e) {
		cerr << "Journal write error: " << e.what() << endl;
		throw;
	}
}

int SlaveComm::printJournal(string prefix)
{
   int ret;
   printOnly = true;
   ret = replayJournal(prefix);
   printOnly = false;
   return ret;
}

void SlaveComm::do_ownerCheck(ByteStream& msg)
{
	string processName;
	uint32_t pid;
	ByteStream::byte rb = 0;

	msg >> processName >> pid;
	idbassert(msg.length() == 0);

	if (standalone)
		return;

	if (processExists(pid, processName))
		rb++;

	ByteStream reply;
	reply << rb;
	master.write(reply);
}

//FIXME: needs to be refactored along with SessionManagerServer::lookupProcessStatus()
#if defined(__linux__)
bool SlaveComm::processExists(const uint32_t pid, const string& pname)
{
	string stat;
	ostringstream procFileName;
	ostringstream statProcessField;
	ifstream in;
	string::size_type pos;
	ByteStream reply;
	char buf[2048];

	procFileName << "/proc/" << pid << "/stat";
	in.open(procFileName.str().c_str());
	if (!in) {
		return false;
	}

	statProcessField << "(" << pname << ")";

	in.getline(buf, 1024);
	stat = buf;
	pos = stat.find(statProcessField.str());
	if (pos == string::npos) {
		return false;
	}

	return true;
}


#elif defined(_MSC_VER)
//FIXME
bool SlaveComm::processExists(const uint32_t pid, const string& pname)
{
	boost::mutex::scoped_lock lk(fPidMemLock);	
	if (!fPids)
		fPids = (DWORD*)malloc(fMaxPids * sizeof(DWORD));
	DWORD needed = 0;
	if (EnumProcesses(fPids, fMaxPids * sizeof(DWORD), &needed) == 0)
		return false;
	while (needed == fMaxPids * sizeof(DWORD))
	{
		fMaxPids *= 2;
		fPids = (DWORD*)realloc(fPids, fMaxPids * sizeof(DWORD));
		if (EnumProcesses(fPids, fMaxPids * sizeof(DWORD), &needed) == 0)
			return false;
	}
	DWORD numPids = needed / sizeof(DWORD);
	for (DWORD i = 0; i < numPids; i++)
	{
		if (fPids[i] == pid)
		{
			TCHAR szProcessName[MAX_PATH] = TEXT("<unknown>");

			// Get a handle to the process.
			HANDLE hProcess = OpenProcess(PROCESS_QUERY_INFORMATION |
										   PROCESS_VM_READ,
										   FALSE, fPids[i]);
			// Get the process name.
			if (hProcess != NULL)
			{
				HMODULE hMod;
				DWORD cbNeeded;

				if (EnumProcessModules(hProcess, &hMod, sizeof(hMod), &cbNeeded))
					GetModuleBaseName(hProcess, hMod, szProcessName, 
									   sizeof(szProcessName)/sizeof(TCHAR));

				CloseHandle(hProcess);

				if (pname == szProcessName)
					return true;
			}
		}
	}
	return false;
}
#elif defined(__FreeBSD__)
//FIXME
bool SlaveComm::processExists(const uint32_t pid, const string& pname)
{
	return false;
}
#else
#error Need to port processExists()
#endif

void SlaveComm::do_dmlLockLBIDRanges(ByteStream &msg)
{
	ByteStream reply;
	vector<LBIDRange> ranges;
	int txnID;
	uint32_t tmp32;
	int err;

	deserializeVector<LBIDRange>(msg, ranges);
	msg >> tmp32;
	assert(msg.length() == 0);
	txnID = (int) tmp32;

	if (printOnly) {
		cout << "dmlLockLBIDRanges: transID=" << txnID << " size="
				<< ranges.size() <<	" ranges..." << endl;
		for (uint i = 0; i < ranges.size(); i++)
			cout << "   start=" << ranges[i].start << " size=" << ranges[i].size << endl;
		return;
	}

	err = slave->dmlLockLBIDRanges(ranges, txnID);

	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_dmlLockLBIDRanges() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

void SlaveComm::do_dmlReleaseLBIDRanges(ByteStream &msg)
{
	ByteStream reply;
	vector<LBIDRange> ranges;
	int err;

	deserializeVector<LBIDRange>(msg, ranges);

	if (printOnly) {
		cout << "dmlLockLBIDRanges: size=" << ranges.size() << " ranges..." << endl;
		for (uint i = 0; i < ranges.size(); i++)
			cout << "   start=" << ranges[i].start << " size=" << ranges[i].size << endl;
		return;
	}

	err = slave->dmlReleaseLBIDRanges(ranges);

	reply << (uint8_t) err;
#ifdef BRM_VERBOSE
	cerr << "WorkerComm: do_dmlReleaseLBIDRanges() err code is " << err << endl;
#endif
	if (!standalone)
		master.write(reply);
	doSaveDelta = true;
}

}

// vim:ts=4 sw=4:
