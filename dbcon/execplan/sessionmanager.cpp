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
 * $Id: sessionmanager.cpp 9215 2013-01-24 18:40:12Z pleblanc $
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

const QueryContext SessionManager::verID()
{
	return dbrm.verID();
}

const QueryContext SessionManager::sysCatVerID()
{
	return dbrm.sysCatVerID();
}

const TxnID SessionManager::newTxnID(const SID session, bool block, bool isDDL) 
{
	TxnID ret;

	ret = dbrm.newTxnID(session, block, isDDL);
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

boost::shared_array<SIDTIDEntry> SessionManager::SIDTIDMap(int& len)
{
	// is this cast valid?
	return dbrm.SIDTIDMap(len);
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
	dbrm.sessionmanager_reset();
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
	boost::shared_array<SIDTIDEntry> sIDTIDMap;

	sIDTIDMap = SIDTIDMap( arrayLenth );
	
	if (sIDTIDMap)
	{
		for ( int i = 0; i < arrayLenth; i++ )
		{
			if ( sIDTIDMap[i].txnid.valid && ( sIDTIDMap[i].sessionid != sessionId || sessionId == 0 ) )
			{
				blocker = sIDTIDMap[i];
				ret = true;
			}
		}
	}
	else
	{
		bIsDbrmUp = false;
	}

	return ret;
}

const bool SessionManager::isTransactionActive(const SID sessionId, bool& bIsDbrmUp)
{
	bIsDbrmUp = true;
	int arrayLenth = 0;
	bool ret = false;
	boost::shared_array<SIDTIDEntry> sIDTIDMap;

	sIDTIDMap = SIDTIDMap(arrayLenth);

	if (sIDTIDMap)
	{
		for ( int i = 0; i < arrayLenth; i++ )
		{
			if (sIDTIDMap[i].txnid.valid && (sIDTIDMap[i].sessionid == sessionId))
			{
				ret = true;
				break;
			}
		}
	}
	else
	{
		bIsDbrmUp = false;
	}

	return ret;
}

}  //namespace
