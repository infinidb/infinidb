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
 * TableLockServer.h
 *
 *  Created on: Nov 30, 2011
 *      Author: pleblanc
 */

#include "brmtypes.h"
#include "sessionmanagerserver.h"

#ifndef TABLELOCKSERVER_H_
#define TABLELOCKSERVER_H_

#if defined(_MSC_VER) && defined(xxxBRMTBLLOCKSVR_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

class TableLockServer {
public:
	EXPORT TableLockServer(SessionManagerServer *);
	virtual ~TableLockServer();

	EXPORT uint64_t lock(TableLockInfo *);
	EXPORT bool unlock(uint64_t id);
	EXPORT bool changeState(uint64_t id, LockState state);
	EXPORT bool changeOwner(uint64_t id, const std::string &ownerName, uint32_t pid, int32_t sessionID,
			int32_t txnID);
	EXPORT std::vector<TableLockInfo> getAllLocks() const;
	EXPORT void releaseAllLocks();
	EXPORT bool getLockInfo(uint64_t id, TableLockInfo *out) const;

private:
	void load();
	void save();

	mutable boost::mutex mutex;
	std::map<uint64_t, TableLockInfo> locks;
	typedef std::map<uint64_t, TableLockInfo>::iterator lit_t;
	typedef std::map<uint64_t, TableLockInfo>::const_iterator constlit_t;
	std::string filename;
	SessionManagerServer *sms;
};

}

#undef EXPORT

#endif /* TABLELOCKSERVER_H_ */
