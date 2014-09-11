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
 * $Id: brmtypes.h 1623 2012-07-03 20:17:56Z pleblanc $
 *
 *****************************************************************************/

/** @file 
 */

#ifndef BRMTYPES_H_
#define BRMTYPES_H_

#include <vector>
#include <sys/types.h>
#include <climits>
#include <string>

#ifndef _MSC_VER
#include <tr1/unordered_map>
#ifndef _UNORDERED_MAP_FIX_
#define _UNORDERED_MAP_FIX_
#ifndef __LP64__

#if __GNUC__ == 4 && __GNUC_MINOR__ < 2
// This is needed for /usr/include/c++/4.1.1/tr1/functional on 32-bit compiles
// tr1_hashtable_define_trivial_hash(long long int);
namespace std
{
namespace tr1
{
  template<>
    struct hash<long long int>
    : public std::unary_function<long long int, std::size_t>
    {
      std::size_t
      operator()(long long int val) const
      { return static_cast<std::size_t>(val); }
    };
}
}
#endif //if __GNUC__

#endif //if !__LP64__
#endif //_UNORDERED_MAP_FIX_
#else
#include <unordered_map>
#endif //_MSC_VER

#include "calpontsystemcatalog.h"
#include "bytestream.h"
#include "messageobj.h"
#include "messagelog.h"
#include "loggingid.h"

#ifdef UNDO
#undef UNDO
#endif

#if defined(_MSC_VER) && defined(BRMTYPES_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

/* these types should be defined in the system catalog header */
typedef int64_t LBID_t; /// The LBID space is currently 36 bits.  We use 64 here.
typedef u_int32_t HWM_t;
typedef int32_t VER_t;
/// Object ID type.  Currently a 32-bit number.  24 for Object number, 8 for partition number.
typedef execplan::CalpontSystemCatalog::OID OID_t;  

typedef std::pair<LBID_t, VER_t> LVP_t;

typedef std::vector<LVP_t> BlockList_t;

/* LBIDRange and VBRange used to be simple structs and were converted to classes
to inherit Serializeable, hence the all-public definitions */

/// The InlineLBIDRange struct is for use internally by the ExtentMap
struct InlineLBIDRange {
		LBID_t start;
		u_int32_t size;
#ifndef __LP64__
		int32_t pad1;
#endif
};

#define MAX_PROCNAME 16

		/** @brief SID = Session ID */
		typedef u_int32_t SID;
		/** @brief A type describing a single transaction ID */
		struct _TxnID {
			/// The TransactionID number
			execplan::CalpontSystemCatalog::SCN		id;
			/// True iff the id is valid.
			bool							valid;   
			EXPORT _TxnID();
		};
		/** @brief A type describing a single transaction ID */
		typedef struct _TxnID TxnID;
		
		/** @brief A type associating a session with a transaction.
         *  txnid.valid is TRUE  for DML/DDL  transaction entries.
		 *  txnid.valid is FALSE for cpimport transaction entries.
		 *  A nonzero tableOID is used to denote an entry being used to write-
		 *  lock a table being updated by DML, DDL, or cpimport.  So, for ex-
		 *  ample, a DML transaction could have multiple entries with the
		 *  initial transaction entry having a 0 tableOID, while the other
		 *  entries would have nonzero tableOID's for the affected tables.
		 */
		struct _SIDTIDEntry {
			/// The Transaction ID.  txnid.valid determines whether or not this SIDTIDEntry is valid
			TxnID			txnid;
			/// The session doing the transaction
			SID				sessionid;	
			/// tableOID the table OID involved in the transaction or cpimport
			execplan::CalpontSystemCatalog::OID   tableOID;
			/// processID the process ID which locked the table, only valid when table is locked.
			u_int32_t  processID;
			///processName the process name which locked the table, only valid when table is locked.
			char   processName[MAX_PROCNAME];
			EXPORT _SIDTIDEntry();
			EXPORT void init();
		};
		/** @brief A type associating a session with a transaction */
		typedef struct _SIDTIDEntry SIDTIDEntry;

// @bug 1970 - Added CPInfo and CPMaxMin structs used by new interface that allows setting the max and min CP data 
// for multiple extents.

// Used in vectors.
struct CPInfo {
	LBID_t firstLbid;
	int64_t max;
	int64_t min;
	int32_t seqNum;
};
typedef std::vector<CPInfo> CPInfoList_t;

// Used for map where lbid is the key.
struct CPMaxMin {
	int64_t max;
	int64_t min;
	int32_t seqNum;
};
typedef std::tr1::unordered_map<LBID_t, CPMaxMin> CPMaxMinMap_t;

// @bug 2117 - Add second CP struct to use in merging CP info

struct CPInfoMerge {
	LBID_t  startLbid; // starting LBID for relevant extent
	int64_t max;       // max value to be merged with current max value
	int64_t min;       // min value to be merged with current min value
	int32_t seqNum;    // sequence number (not currently used)
	bool    isChar;    // does this extent carry non-dictionary string data
	bool	newExtent; // is this to be treated as a new extent
};
typedef std::vector<CPInfoMerge> CPInfoMergeList_t;

// Used for map where lbid is the key.  Data members have same meaning as
// those in CPInfoMerge.
struct CPMaxMinMerge {
	int64_t max;
	int64_t min;
	int32_t seqNum;
	bool    isChar;
	bool    newExtent;
};
typedef std::tr1::unordered_map<LBID_t, CPMaxMinMerge> CPMaxMinMergeMap_t;


// end of bug 2117

typedef std::tr1::unordered_map<execplan::CalpontSystemCatalog::OID, execplan::CalpontSystemCatalog::OID> OidsMap_t;

struct ExtentInfo {
	execplan::CalpontSystemCatalog::OID oid;
	uint32_t	partitionNum; // starts at 0
	uint16_t	segmentNum;   // starts at 0
	uint16_t	dbRoot;       // starts at 1 to match Calpont.xml
	HWM_t		hwm;
	bool 		newFile;
};
typedef std::tr1::unordered_map<execplan::CalpontSystemCatalog::OID, ExtentInfo> ExtentsInfoMap_t;

/// A Serializeable version of InlineLBIDRange
class LBIDRange : public messageqcpp::Serializeable {

	public:
		LBID_t start;
		u_int32_t size;

		EXPORT LBIDRange();
		EXPORT LBIDRange(const LBIDRange& l);
		EXPORT LBIDRange(const InlineLBIDRange& l);
		EXPORT LBIDRange& operator=(const LBIDRange& l);
		EXPORT LBIDRange& operator=(const InlineLBIDRange& l);
		EXPORT virtual ~LBIDRange();

		/** The Serializeable interface.  Exports the instance to the bytestream */
		EXPORT virtual void serialize(messageqcpp::ByteStream& bs) const;
		/** The Serializeable interface.  Initializes itself from the bytestrem. */
		EXPORT virtual void deserialize(messageqcpp::ByteStream& bs);
};

/* To support bulkVSSLookup() */
struct VSSData {
	VER_t verID;
	bool vbFlag;
	int returnCode;
};

/* Arg type for DBRM::bulkSetHWM() */
struct BulkSetHWMArg {
	OID_t oid;
	uint32_t partNum;
	uint16_t segNum;
	HWM_t hwm;
};

/// A container for LBIDRanges
typedef std::vector<LBIDRange> LBIDRange_v; 

/// Describes a contiguous range of blocks in the Version Buffer
class VBRange : public messageqcpp::Serializeable {

	public:
		OID_t vbOID;
		u_int32_t vbFBO;
		u_int32_t size;

		EXPORT VBRange();
		EXPORT VBRange(const VBRange& v);
		EXPORT VBRange& operator= (const VBRange& v);
		EXPORT virtual ~VBRange();
		EXPORT virtual void serialize(messageqcpp::ByteStream& bs) const;
		EXPORT virtual void deserialize(messageqcpp::ByteStream& bs);
};

/// A container for VBRanges
typedef std::vector<VBRange> VBRange_v;  

/* Definitions to support 'undo' operations */

#define ID_MAXSIZE 200

/** @brief ImageDeltas describe how an image in memory changed.
 *
 * ImageDelta objects contain the data that occupied the space at 'start'
 * before the last write operation was performed.  To reverse the change,
 * memcpy(ImageDelta.start, ImageDelta.data, ImageDelta.size).  The
 * shared memory segments should not be unlinked or unlocked since the
 * write operation.
 * 
 * Right now it is used specifically to record how BRM data is modified.
 * Usage can be generalized.
 */
struct ImageDelta {
	void *start;
	int size;
	char data[ID_MAXSIZE];	/// XXXPAT: the largest thing currently copied into this is 36 bytes (EMEntry).  Adjust as neccesary...
};

// SubSystemLogId enumeration values should be in sync with SubsystemID[]
// that is defined in messagelog.cpp
enum SubSystemLogId {
	SubSystemLogId_controllerNode = 29,
	SubSystemLogId_workerNode     = 30
};
EXPORT void logInit ( SubSystemLogId subSystemId );
EXPORT void log(const std::string &msg, logging::LOG_TYPE = logging::LOG_TYPE_CRITICAL);
EXPORT void log_errno(const std::string &msg, logging::LOG_TYPE = logging::LOG_TYPE_CRITICAL);
EXPORT void errString( int rc, std::string& errMsg );

const struct timespec FIVE_MIN_TIMEOUT = {300, 0};

/* Function identifiers used for master-slave communication.

	The format of the messages from DBRM to Master and from Master to Slaves
	will look like
		<fcnID, in-params>
	The format of the messages from the Slaves to Master and Master to DBRM 
	will look like
		<error code, out-params>
*/
//FIXME: put these in a enum. The current arrangement is error-prone.
const uint8_t DELETE_OID = 1;
const uint8_t WRITE_VB_ENTRY = 3;
const uint8_t BEGIN_VB_COPY = 4;
const uint8_t END_VB_COPY = 5;
const uint8_t VB_ROLLBACK1 = 6;
const uint8_t VB_ROLLBACK2 = 7;
const uint8_t VB_COMMIT = 8;
const uint8_t UNDO = 9;
const uint8_t CONFIRM = 10;
const uint8_t HALT = 11;
const uint8_t RESUME = 12;
const uint8_t RELOAD = 13;
const uint8_t SETREADONLY = 14;
const uint8_t SETREADWRITE = 15;
const uint8_t FLUSH_INODE_CACHES = 16;
const uint8_t CLEAR = 17;
const uint8_t MARKEXTENTINVALID = 18;
const uint8_t MARKMANYEXTENTSINVALID = 19;
const uint8_t GETREADONLY = 20;
const uint8_t SETEXTENTMAXMIN = 21;
const uint8_t ROLLBACK_COLUMN_EXTENTS = 22;
const uint8_t ROLLBACK_DICT_STORE_EXTENTS = 23;
const uint8_t DELETE_EMPTY_COL_EXTENTS = 24;
const uint8_t DELETE_EMPTY_DICT_STORE_EXTENTS = 25;
const uint8_t SETMANYEXTENTSMAXMIN = 26;
const uint8_t CREATE_COLUMN_EXTENT = 27;
const uint8_t CREATE_DICT_STORE_EXTENT = 28;
const uint8_t SET_LOCAL_HWM = 29;
const uint8_t DELETE_OIDS = 30;
const uint8_t TAKE_SNAPSHOT = 31;
const uint8_t MERGEMANYEXTENTSMAXMIN = 44;
const uint8_t DELETE_PARTITION = 45;
const uint8_t MARK_PARTITION_FOR_DELETION = 46;
const uint8_t RESTORE_PARTITION = 47;
const uint8_t MARK_ALL_PARTITION_FOR_DELETION = 51;
const uint8_t BULK_SET_HWM_AND_CP = 52;


/* SessionManager interface */
const uint8_t VER_ID = 32;
const uint8_t NEW_TXN_ID = 33;
const uint8_t COMMITTED = 34;
const uint8_t ROLLED_BACK = 35;
const uint8_t GET_TXN_ID = 36;
const uint8_t SID_TID_MAP = 37;
const uint8_t GET_SHM_CONTENTS = 38;
const uint8_t GET_UNIQUE_UINT32 = 39;
const uint8_t SYSCAT_VER_ID = 40;
const uint8_t SET_TABLE_LOCK = 41;
const uint8_t GET_TABLE_LOCK = 42;
const uint8_t GET_TABLE_LOCKS = 43;
const uint8_t GET_SYSTEM_STATE = 48;
const uint8_t SET_SYSTEM_STATE = 49;
const uint8_t UPDATE_TABLE_LOCK = 50;

/* Copylock interface */
const uint8_t LOCK_LBID_RANGES = 90;
const uint8_t RELEASE_LBID_RANGES = 91;


/* Error codes returned by the DBRM functions. */
/// The operation was successful
const int8_t ERR_OK = 0; 

/// There was some unspecific failure and if the operation was a "write", no change was made
const int8_t ERR_FAILURE = 1;

/// The operation failed because at least one slave has a different image than the others.  No change was made.
const int8_t ERR_SLAVE_INCONSISTENCY = 2;

/// The operation failed because of a communication problem.
const int8_t ERR_NETWORK = 3;

/// The operation failed because one slave did not send a response within 10 seconds.  The administrator should look into it.
const int8_t ERR_TIMEOUT = 4;

/// The operation failed because the Master is in read-only mode.  Either it detected an serious error (ie ERR_SLAVE_INCONSISTENCY), or the administrator set it using dbrmctl.  The system needs the administrator to fix the problem and enable read-write mode.
const int8_t ERR_READONLY = 5;

/// beginVBCopy was attempted, but deadlocked and was cancelled.  The transaction must be rolled back after receiving this error code.
const int8_t ERR_DEADLOCK = 6;

/// While waiting for beginVBCopy to allocate the requested LBIDs, the transaction was killed by another thread.  The caller must roll back after receiving this error code.
const int8_t ERR_KILLED = 7;

/// version buffer overflow error
const int8_t ERR_VBBM_OVERFLOW = 8;

const int8_t ERR_TABLE_LOCKED_ALREADY = 9;

/// Invalid operation against last partition in a table column
const int8_t ERR_INVALID_OP_LAST_PARTITION = 10;
const int8_t ERR_PARTITION_DISABLED = 11;
const int8_t ERR_NOT_EXIST_PARTITION = 12;
const int8_t ERR_PARTITION_ENABLED = 13;
const int8_t ERR_TABLE_NOT_LOCKED = 14;
const int8_t ERR_SNAPSHOT_TOO_OLD = 15;

}

#undef EXPORT

#endif
