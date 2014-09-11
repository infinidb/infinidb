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
 * $Id: sessionmanager.cpp 8434 2012-04-03 18:31:24Z dcathey $
 *
 ****************************************************************************/

/** @file
 * This class issues Transaction ID and keeps track of the current version ID
 */
 
#include <iostream>
#include <fstream>
#include <string>
#include <stdexcept>
#include <ios>

#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
#include "configcpp.h"
#include "messagelog.h"
#include "dbrm.h"

using namespace std;
using namespace BRM;

namespace execplan {



SessionManager::SessionManager()
{	
	config::Config* conf;
	
	conf = config::Config::makeConfig();
	txnidFilename = conf->getConfig("SessionManager", "TxnIDFile");
}

SessionManager::SessionManager(bool nolock)
{	
	config::Config* conf;
	string stmp;
	
	conf = config::Config::makeConfig();
	txnidFilename = conf->getConfig("SessionManager", "TxnIDFile");
}

SessionManager::SessionManager(const SessionManager& sm)
{
	txnidFilename = sm.txnidFilename;
}

SessionManager::~SessionManager()
{
}

const CalpontSystemCatalog::SCN SessionManager::verID()
{
	return dbrm.verID();
}

const CalpontSystemCatalog::SCN SessionManager::sysCatVerID()
{
	return dbrm.sysCatVerID();
}

const TxnID SessionManager::newTxnID(const SID session, bool block, bool isDDL) 
{
	TxnID tmp;	
	TxnID ret;

	tmp = dbrm.newTxnID(session, block, isDDL);
	ret.id = tmp.id;
	ret.valid = tmp.valid;
	return ret;
}

void SessionManager::committed(TxnID& txn)
{
	TxnID tmp;

	tmp.id = txn.id;
	tmp.valid = txn.valid;
	dbrm.committed(tmp);
	txn.id = tmp.id;
	txn.valid = tmp.valid;
}

void SessionManager::rolledback(TxnID& txn)
{
	TxnID tmp;

	tmp.id = txn.id;
	tmp.valid = txn.valid;
	dbrm.rolledback(tmp);
	txn.id = tmp.id;
	txn.valid = tmp.valid;
}

const TxnID SessionManager::getTxnID(const SID session)
{
	TxnID tmp;	
	TxnID ret;

	tmp = dbrm.getTxnID(session);
	ret.id = tmp.id;
	ret.valid = tmp.valid;

	return ret;
}

const SIDTIDEntry* SessionManager::SIDTIDMap(int& len)
{
	// is this cast valid?
	return dbrm.SIDTIDMap(len);
}

// delete with delete []
char * SessionManager::getShmContents(int &len)
{
	return dbrm.getShmContents(len);
}

string SessionManager::getTxnIDFilename() const 
{
	return txnidFilename;
}

int SessionManager::verifySize()
{
	return 1;
}

void SessionManager::reset()
{
}

const uint32_t SessionManager::getUnique32()
{
	return dbrm.getUnique32();
}

const bool SessionManager::checkActiveTransaction( const SID sessionId, bool& bIsDbrmUp, SIDTIDEntry& blocker )
{
	bIsDbrmUp = true;
	int arrayLenth = 0;
	bool ret = false;
	const SIDTIDEntry* sIDTIDMap;

	sIDTIDMap = SIDTIDMap( arrayLenth );
	
	if (sIDTIDMap)
	{
		for ( int i = 0; i < arrayLenth; i++ )
		{
			if ( sIDTIDMap[i].txnid.valid && ( sIDTIDMap[i].sessionid != sessionId ) )
			{
				blocker = sIDTIDMap[i];
				ret = true;
				//FIXME: there's a bug somewhere: there can be multile entries in this
				// table with the same valid flg & sid, but the first one has tableOID = 0
				// and the _second_ one has the correct oid...
				if (blocker.tableOID != 0)
					break;
			}
		}
	
		delete [] sIDTIDMap;
	}
	else
	{
		bIsDbrmUp = false;
	}

	return ret;
}

int8_t SessionManager::setTableLock (  const OID_t tableOID, const u_int32_t sessionID,  const u_int32_t processID, const string processName, bool lock )
{
	return dbrm.setTableLock ( tableOID, sessionID, processID, processName, lock );
}

int8_t SessionManager::getTableLockInfo ( const OID_t tableOID, u_int32_t & processID,
	std::string & processName, bool & lockStatus, SID & sid )
{
	return dbrm.getTableLockInfo ( tableOID, processID, processName, lockStatus, sid );

}

void  SessionManager::getTableLocksInfo (std::vector<SIDTIDEntry> & sidTidentries)
{
	dbrm.getTableLocksInfo( sidTidentries );
}


}  //namespace
