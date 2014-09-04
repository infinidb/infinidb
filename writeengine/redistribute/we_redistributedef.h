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
* $Id: we_redistributedef.h 4299 2012-11-02 06:00:33Z xlou $
*/

#ifndef WE_REDISTRIBUTEDEF_H
#define WE_REDISTRIBUTEDEF_H

#include <time.h>

namespace redistribute
{

// state values
const uint32_t RED_STATE_UNDEF   = 0;  // indication of failed to retrieve redistribute status.
const uint32_t RED_STATE_IDLE    = 1;
const uint32_t RED_STATE_ACTIVE  = 2;
const uint32_t RED_STATE_FINISH  = 3;
const uint32_t RED_STATE_STOPPED = 4;
const uint32_t RED_STATE_FAILED  = 5;
const uint32_t RED_STATE_INCOMP  = 6;

// partition transfer state
const uint32_t RED_TRANS_READY   = 20;
const uint32_t RED_TRANS_SUCCESS = 21;
const uint32_t RED_TRANS_SKIPPED = 22;
const uint32_t RED_TRANS_FAILED  = 23;


// return code to shell or between redistribute entities
enum RED_ErrorCode {
	RED_EC_OK,

	// return code for util, for shell script to check exit status value is [0, 255]
	RED_EC_HELP,
	RED_EC_INVALID_OPTION,
	RED_EC_EXTRA_OPERAND,
	RED_EC_INVALID_DBROOTID,
	RED_EC_GET_DBROOT1,
	RED_EC_CONNECT_FAIL,
	RED_EC_GET_DBROOT_EXCEPT,
	RED_EC_NETWORK_FAIL,
	RED_EC_PROTOCOL_ERROR,
	RED_EC_NO_ARC_NAME,
	RED_EC_ARC_NOT_EXIST,
	RED_EC_READ_ARC_FAIL,

	// return code for control and workers
	RED_EC_CNTL_SETUP_FAIL,
	RED_EC_MAKEPLAN_FAIL,
	RED_EC_EXECUTE_FAIL,
	RED_EC_UPDATE_STATE,
	RED_EC_USER_STOP,

	RED_EC_WKR_SETUP_FAIL,
	RED_EC_WKR_MSG_SHORT,
	RED_EC_FILE_LIST_FAIL,
	RED_EC_NO_TABLE_LOCK,
	RED_EC_IDB_HARD_FAIL,
	RED_EC_EXTENT_ERROR,
	RED_EC_PART_EXIST_ON_TARGET,
	RED_EC_OID_TO_FILENAME,
	RED_EC_OPEN_FILE_FAIL,
	RED_EC_FSEEK_FAIL,
	RED_EC_FREAD_FAIL,
	RED_EC_FWRITE_FAIL,
	RED_EC_SIZE_NACK,
	RED_EC_COPY_FILE_FAIL,
	RED_EC_UPDATE_DBRM_FAIL,
	RED_EC_BS_TOO_SHORT,
	RED_EC_FILE_SIZE_NOT_MATCH,
	RED_EC_UNKNOWN_DATA_MSG,
	RED_EC_UNKNOWN_JOB_MSG,

};


// redistribute message ID
const uint32_t RED_CNTL_START   = 1;
const uint32_t RED_CNTL_STATUS  = 2;
const uint32_t RED_CNTL_STOP    = 3;
const uint32_t RED_CNTL_CLEAR   = 4;
const uint32_t RED_CNTL_RESP    = 5;
const uint32_t RED_CNTL_VIEW    = 6;

const uint32_t RED_ACTN_REQUEST = 21;
const uint32_t RED_ACTN_STOP    = 22;
const uint32_t RED_ACTN_RESP    = 23;
const uint32_t RED_ACTN_REPORT  = 24;

const uint32_t RED_DATA_INIT    = 51;
const uint32_t RED_DATA_START   = 52;
const uint32_t RED_DATA_CONT    = 53;
const uint32_t RED_DATA_FINISH  = 54;
const uint32_t RED_DATA_COMMIT  = 55;
const uint32_t RED_DATA_ABORT   = 56;
const uint32_t RED_DATA_ACK     = 57;


// file transfer chunk size
const size_t CHUNK_SIZE = 1024 * 1024;
const size_t PRE_ALLOC_SIZE = 4 * 1024;



// redistribute message header
struct RedistributeMsgHeader
{
	uint32_t  destination;
	uint32_t  source;
	uint32_t  sequenceNum;
	uint32_t  messageId;

	RedistributeMsgHeader(uint32_t d=0, uint32_t s=0, uint32_t n=0, uint32_t i=0) :
		destination(d), source(s), sequenceNum(n), messageId(i) {};
};


// redistribute data transfer control block
struct RedistributeDataControl
{
	uint64_t  oid;
	uint16_t  dbroot;
	uint32_t  partition;
	uint16_t  segment;
	uint64_t  size;

	RedistributeDataControl(uint64_t o=0, uint16_t d=0, uint32_t p=0, uint16_t s=0, uint32_t z=0) :
		oid(o), dbroot(d), partition(p), segment(s), size(z) {};
};


// extent entry
struct RedistributeExtentEntry
{
	int64_t  oid;
	int16_t  dbroot;
	int32_t  partition;
	int16_t  segment;
	int64_t  lbid;
	int64_t  range;
    RedistributeExtentEntry() :
		oid(0), dbroot(0), partition(0), segment(0), lbid(0), range(0) {}
};


// RedistributePlanEntry
struct RedistributePlanEntry
{
	int64_t  table;
	int32_t  source;
	int32_t  partition;
	int32_t  destination;
	int32_t  status;
	time_t   starttime;
	time_t   endtime;

	RedistributePlanEntry() :
		table(0), source(0), partition(0), destination(0), status(0), starttime(0), endtime(0) {}
};


// RedistributeInfo
struct RedistributeInfo
{
	uint64_t state;
	uint64_t planned;
	uint64_t success;
	uint64_t skipped;
	uint64_t failed;
	time_t   startTime;
	time_t   endTime;

	RedistributeInfo() :
		state(RED_STATE_UNDEF), planned(0), success(0), skipped(0), failed(0),
		startTime(0), endTime(0) {}
};


}  // namespace


#endif  // WE_REDISTRIBUTEDEF_H

// vim:ts=4 sw=4:

