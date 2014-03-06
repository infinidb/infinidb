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

/******************************************************************************
 * $Id: slavecomm.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#ifndef SLAVECOMM_H_
#define SLAVECOMM_H_

#include <unistd.h>
#include <iostream>

#include <boost/thread/mutex.hpp>

#include "brmtypes.h"
#include "slavedbrmnode.h"
#include "messagequeue.h"
#include "bytestream.h"

#if defined(_MSC_VER) && defined(xxxSLAVECOMM_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

// forward reference
namespace idbdatafile
{
class IDBDataFile;
}


namespace BRM {

class SlaveComm {
	public:
		/** Use this ctor to instantiate a standalone SlaveComm ie, for replaying
			the journal.  Should be used only by load_brm. */
		EXPORT SlaveComm();

		/** Use this ctor to have it connected to the rest of the DBRM system */
		EXPORT SlaveComm(std::string hostname, SlaveDBRMNode *s);  //hostname = 'DBRM_WorkerN'
		EXPORT ~SlaveComm();

		EXPORT void run();
		EXPORT void stop();
		EXPORT void reset();

		EXPORT int replayJournal(std::string prefix);
		EXPORT int printJournal(std::string prefix);

	private:

		SlaveComm& operator=(const SlaveComm& s);

		void processCommand(messageqcpp::ByteStream &msg);

		void do_createStripeColumnExtents(messageqcpp::ByteStream &msg);
		void do_createColumnExtent_DBroot(messageqcpp::ByteStream &msg);
		void do_createColumnExtentExactFile(messageqcpp::ByteStream &msg);
		void do_createDictStoreExtent(messageqcpp::ByteStream &msg);
		void do_rollbackColumnExtents_DBroot(messageqcpp::ByteStream &msg);
		void do_deleteEmptyColExtents(messageqcpp::ByteStream &msg);
		void do_deleteEmptyDictStoreExtents(messageqcpp::ByteStream &msg);
		void do_rollbackDictStoreExtents_DBroot(messageqcpp::ByteStream &msg);
		void do_deleteOID(messageqcpp::ByteStream &msg); 
		void do_deleteOIDs(messageqcpp::ByteStream &msg);
		void do_setLocalHWM(messageqcpp::ByteStream &msg);
		void do_bulkSetHWM(messageqcpp::ByteStream &msg);
		void do_bulkSetHWMAndCP(messageqcpp::ByteStream &msg);
		void do_writeVBEntry(messageqcpp::ByteStream &msg); 
		void do_beginVBCopy(messageqcpp::ByteStream &msg);
		void do_endVBCopy(messageqcpp::ByteStream &msg); 
		void do_vbRollback1(messageqcpp::ByteStream &msg);
		void do_vbRollback2(messageqcpp::ByteStream &msg);
		void do_vbCommit(messageqcpp::ByteStream &msg);
		void do_markInvalid(messageqcpp::ByteStream &msg);
		void do_markManyExtentsInvalid(messageqcpp::ByteStream &msg);
		void do_mergeExtentsMaxMin(messageqcpp::ByteStream &msg);
		void do_setExtentMaxMin(messageqcpp::ByteStream &msg);
		void do_setExtentsMaxMin(messageqcpp::ByteStream &msg); // bug 1970
		void do_deletePartition(messageqcpp::ByteStream &msg);
		void do_markPartitionForDeletion(messageqcpp::ByteStream &msg);
		void do_markAllPartitionForDeletion(messageqcpp::ByteStream &msg);
		void do_restorePartition(messageqcpp::ByteStream &msg);
		void do_dmlLockLBIDRanges(messageqcpp::ByteStream &msg);
		void do_dmlReleaseLBIDRanges(messageqcpp::ByteStream &msg);
		void do_deleteDBRoot(messageqcpp::ByteStream &msg);
		void do_bulkUpdateDBRoot(messageqcpp::ByteStream &msg);

		void do_undo();
		void do_confirm();
		void do_flushInodeCache();
		void do_clear();
		void do_ownerCheck(messageqcpp::ByteStream &msg);
		void do_takeSnapshot();
		void saveDelta();
		bool processExists(const uint32_t pid, const std::string& pname);

		messageqcpp::MessageQueueServer *server;
		messageqcpp::IOSocket master;
		SlaveDBRMNode *slave;
		std::string savefile;
		bool release, die, firstSlave, saveFileToggle, takeSnapshot, doSaveDelta, standalone, printOnly;
		messageqcpp::ByteStream delta;
		int currentSaveFD;
		idbdatafile::IDBDataFile* currentSaveFile;
		std::string journalName;
		std::fstream journal;
		idbdatafile::IDBDataFile* journalh;
		int64_t snapshotInterval, journalCount;
		struct timespec MSG_TIMEOUT;
#ifdef _MSC_VER
		boost::mutex fPidMemLock;
		DWORD* fPids;
		DWORD fMaxPids;
#endif
};

}

#undef EXPORT

#endif
