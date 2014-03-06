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
 * $Id: brmtypes.cpp 1896 2013-06-07 19:50:51Z rdempsey $
 *
 ****************************************************************************/

/*
 * Definitions of the functions declared in brmtypes.h
 */

#include "config.h"

#include <errno.h>
#include <cstring>
#include <unistd.h>
#include <sstream>

#include "calpontsystemcatalog.h"
#include "bytestream.h"
#include "messageobj.h"
#include "messagelog.h"
#include "loggingid.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

#define BRMTYPES_DLLEXPORT
#include "brmtypes.h"
#undef BRMTYPES_DLLEXPORT

using namespace messageqcpp;
using namespace std;
using namespace logging;
using namespace idbdatafile;

// local unnamed namespace
namespace {
	unsigned int subSystemLoggingId =
		(unsigned int)BRM::SubSystemLogId_controllerNode;
}

namespace BRM {

LBIDRange::LBIDRange()
{
	start = 0;
	size = 0;
}

LBIDRange::LBIDRange(const LBIDRange& l)
{
	start = l.start;
	size = l.size;
}

LBIDRange::LBIDRange(const InlineLBIDRange& l)
{
	start = l.start;
	size = l.size;
}

LBIDRange::~LBIDRange()
{
}

LBIDRange& LBIDRange::operator=(const LBIDRange& l)
{
	start = l.start;
	size = l.size;
	return *this;
}

LBIDRange& LBIDRange::operator=(const InlineLBIDRange& l)
	{
	start = l.start;
	size = l.size;
	return *this;
}

// this value should be impossible in version 1
// to add revisions, add 1
//
// version 1 was 2 32-bit #s, |start lbid|size|

#define RANGE_V2_MAGIC 0x80000000ffff0000ULL

void LBIDRange::deserialize(ByteStream& bs)
{
	uint64_t tmp;

	bs >> tmp;
	if (tmp == RANGE_V2_MAGIC) {
		bs >> tmp;
		start = tmp;
		bs >> size;
	}
	else {
		// version 1
		size = (tmp & 0xffffffff00000000ULL) >> 32;
		start = (tmp & 0x00000000ffffffffULL);
	}
}

void LBIDRange::serialize(ByteStream& bs) const
{
	bs << (uint64_t) RANGE_V2_MAGIC;
	bs << (uint64_t) start;
	bs << size;
}

_TxnID::_TxnID()
{
	id = 0;
	valid = false;
}

_SIDTIDEntry::_SIDTIDEntry()
{
	init();
}

void _SIDTIDEntry::init()
{
	sessionid      = 0;
	txnid.id       = 0;
	txnid.valid    = false;
}

VBRange::VBRange()
{
	vbOID = 0;
	vbFBO = 0;
	size = 0;
}

VBRange::VBRange(const VBRange& v)
{
	vbOID = v.vbOID;
	vbFBO = v.vbFBO;
	size = v.size;
}

VBRange::~VBRange()
{
}

VBRange& VBRange::operator= (const VBRange& v)
{
	vbOID = v.vbOID;
	vbFBO = v.vbFBO;
	size = v.size;
	return *this;
}

void VBRange::deserialize(ByteStream& bs)
{
	uint32_t tmp;

	bs >> tmp;
	vbOID = tmp;
	bs >> vbFBO;
	bs >> size;
}

void VBRange::serialize(ByteStream& bs) const
{
	bs << (uint32_t) vbOID;
	bs << vbFBO;
	bs << size;
}

void logInit ( SubSystemLogId subSystemId )
{
	subSystemLoggingId = (unsigned int)subSystemId;
}

void log(const string &msg, logging::LOG_TYPE level)
{
	logging::MessageLog logger((logging::LoggingID(subSystemLoggingId)));
	logging::Message message;
	logging::Message::Args args;

	args.add(msg);
	message.format(args);

	switch (level) {
		case logging::LOG_TYPE_DEBUG:
			logger.logDebugMessage(message); break;
		case logging::LOG_TYPE_INFO:
			logger.logInfoMessage(message); break;
		case logging::LOG_TYPE_WARNING:
			logger.logWarningMessage(message); break;
		case logging::LOG_TYPE_ERROR:
			logger.logErrorMessage(message); break;
		case logging::LOG_TYPE_CRITICAL:
			logger.logCriticalMessage(message); break;
		default:
			logger.logInfoMessage(message);
	}
}

void log_errno(const string &msg, logging::LOG_TYPE level)
{
 	int tmp = errno;
	char test[1000];
	logging::MessageLog logger((logging::LoggingID(subSystemLoggingId)));
	logging::Message message;
	logging::Message::Args args;

	args.add(msg + ": ");

#if STRERROR_R_CHAR_P
	const char* cMsg;
	cMsg = strerror_r(tmp, &test[0], 1000);
	if (cMsg == NULL)
		args.add(string("strerror failed"));
	else
		args.add(string(cMsg));
#else
	int cMsg;
	cMsg = strerror_r(tmp, &test[0], 1000);
	args.add(string(test));
#endif
	message.format(args);

	switch (level) {
		case logging::LOG_TYPE_DEBUG:
			logger.logDebugMessage(message); break;
		case logging::LOG_TYPE_INFO:
			logger.logInfoMessage(message); break;
		case logging::LOG_TYPE_WARNING:
			logger.logWarningMessage(message); break;
		case logging::LOG_TYPE_ERROR:
			logger.logErrorMessage(message); break;
		case logging::LOG_TYPE_CRITICAL:
			logger.logCriticalMessage(message); break;
		default:
			logger.logInfoMessage(message);
	}
}

//------------------------------------------------------------------------------
// Map a BRM error code to an error string.
//------------------------------------------------------------------------------
void errString(int rc, string& errMsg)
{
	switch (rc)
	{
		case ERR_OK:
		{
			errMsg = "OKAY";
			break;
		}
		case ERR_FAILURE:
		{
			errMsg = "FAILED";
			break;
		}
		case ERR_SLAVE_INCONSISTENCY:
		{
			errMsg = "image inconsistency";
			break;
		}
		case ERR_NETWORK:
		{
			errMsg = "network error";
			break;
		}
		case ERR_TIMEOUT:
		{
			errMsg = "network timeout";
			break;
		}
		case ERR_READONLY:
		{
			errMsg = "DBRM is in READ-ONLY mode";
			break;
		}
		case ERR_DEADLOCK:
		{
			errMsg = "deadlock reserving LBID range";
			break;
		}
		case ERR_KILLED:
		{
			errMsg = "killed reserving LBID range";
			break;
		}
		case ERR_VBBM_OVERFLOW:
		{
			errMsg = "VBBM overflow";
			break;
		}
		case ERR_TABLE_LOCKED_ALREADY:
		{
			errMsg = "table already locked";
			break;
		}
		case ERR_INVALID_OP_LAST_PARTITION:
		{
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_INVALID_LAST_PARTITION);
			break;
		}
		case ERR_PARTITION_DISABLED:
		{
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_DISABLED);
			break;
		}
		case ERR_NOT_EXIST_PARTITION:
		{
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_NOT_EXIST);
			break;
		}
		case ERR_PARTITION_ENABLED:
		{
			errMsg = IDBErrorInfo::instance()->errorMsg(ERR_PARTITION_ALREADY_ENABLED);
			break;
		}
		case ERR_OLDTXN_OVERWRITING_NEWTXN:
		{
			errMsg = "A newer transaction has already written to the same block(s)";
			break;
		}
		default:
		{
			ostringstream oss;
			oss << "UNKNOWN (" << rc << ")";
			errMsg = oss.str();
			break;
		}
	}
}

/* Tablelock impl */

bool TableLockInfo::overlaps(const TableLockInfo &t, const std::set<uint32_t> &sDbrootList) const
{
	if (tableOID != t.tableOID)
		return false;
	for (uint32_t i = 0; i < dbrootList.size(); i++)
		if (sDbrootList.find(dbrootList[i]) != sDbrootList.end())
			return true;
	return false;
}

void TableLockInfo::serialize(ByteStream &bs) const
{
	bs << id << tableOID << ownerName << ownerPID << (uint32_t) ownerSessionID <<
			(uint32_t) ownerTxnID << (uint8_t) state;
	bs << (uint64_t) creationTime;
	serializeInlineVector(bs, dbrootList);
}

void TableLockInfo::deserialize(ByteStream &bs)
{
	uint8_t tmp8;
	uint32_t tmp321, tmp322;
	uint64_t tmp64;
	bs >> id >> tableOID >> ownerName >> ownerPID >> tmp321 >> tmp322 >> tmp8 >> tmp64;
	ownerSessionID = tmp321;
	ownerTxnID = tmp322;
	state = (LockState) tmp8;
	creationTime = tmp64;
	deserializeInlineVector(bs, dbrootList);
}

void TableLockInfo::serialize(ostream &o) const
{
	uint16_t nameLen = ownerName.length();
	uint16_t dbrootListSize = dbrootList.size();

	o.write((char *) &id, 8);
	o.write((char *) &tableOID, 4);
	o.write((char *) &ownerPID, 4);
	o.write((char *) &state, 4);
	o.write((char *) &ownerSessionID, 4);
	o.write((char *) &ownerTxnID, 4);
	o.write((char *) &creationTime, sizeof(time_t));
	o.write((char *) &nameLen, 2);
	o.write((char *) ownerName.c_str(), nameLen);
	o.write((char *) &dbrootListSize, 2);
	for (uint32_t j = 0; j < dbrootListSize; j++)
		o.write((char *) &dbrootList[j], 4);
}

void TableLockInfo::deserialize(istream &i)
{
	uint16_t nameLen;
	uint16_t dbrootListSize;
	boost::scoped_array<char> buf;

	i.read((char *) &id, 8);
	i.read((char *) &tableOID, 4);
	i.read((char *) &ownerPID, 4);
	i.read((char *) &state, 4);
	i.read((char *) &ownerSessionID, 4);
	i.read((char *) &ownerTxnID, 4);
	i.read((char *) &creationTime, sizeof(time_t));
	i.read((char *) &nameLen, 2);
	buf.reset(new char[nameLen]);
	i.read(buf.get(), nameLen);
	ownerName = string(buf.get(), nameLen);
	i.read((char *) &dbrootListSize, 2);
	dbrootList.resize(dbrootListSize);
	for (uint32_t j = 0; j < dbrootListSize; j++)
		i.read((char *) &dbrootList[j], 4);
}

void TableLockInfo::serialize(IDBDataFile* o) const
{
	uint16_t nameLen = ownerName.length();
	uint16_t dbrootListSize = dbrootList.size();

	o->write((char *) &id, 8);
	o->write((char *) &tableOID, 4);
	o->write((char *) &ownerPID, 4);
	o->write((char *) &state, 4);
	o->write((char *) &ownerSessionID, 4);
	o->write((char *) &ownerTxnID, 4);
	o->write((char *) &creationTime, sizeof(time_t));
	o->write((char *) &nameLen, 2);
	o->write((char *) ownerName.c_str(), nameLen);
	o->write((char *) &dbrootListSize, 2);
	for (uint32_t j = 0; j < dbrootListSize; j++)
		o->write((char *) &dbrootList[j], 4);
}

void TableLockInfo::deserialize(IDBDataFile* i)
{
	uint16_t nameLen;
	uint16_t dbrootListSize;
	boost::scoped_array<char> buf;

	i->read((char *) &id, 8);
	i->read((char *) &tableOID, 4);
	i->read((char *) &ownerPID, 4);
	i->read((char *) &state, 4);
	i->read((char *) &ownerSessionID, 4);
	i->read((char *) &ownerTxnID, 4);
	i->read((char *) &creationTime, sizeof(time_t));
	i->read((char *) &nameLen, 2);
	buf.reset(new char[nameLen]);
	i->read(buf.get(), nameLen);
	ownerName = string(buf.get(), nameLen);
	i->read((char *) &dbrootListSize, 2);
	dbrootList.resize(dbrootListSize);
	for (uint32_t j = 0; j < dbrootListSize; j++)
		i->read((char *) &dbrootList[j], 4);
}

bool TableLockInfo::operator<(const TableLockInfo &tli) const
{
	return (id < tli.id);
}

ostream & operator<<(ostream &os, const QueryContext &qc)
{
	os << "  SCN: " << qc.currentScn << endl;
	os << "  Txns: ";
	for (uint32_t i = 0; i < qc.currentTxns->size(); i++)
		os << (*qc.currentTxns)[i] << " ";
	return os;
}

}  //namespace
