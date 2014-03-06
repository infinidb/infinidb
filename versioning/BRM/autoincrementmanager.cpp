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
 * $Id$
 *
 ****************************************************************************/

#define BRMAUTOINCMGR_DLLEXPORT
#include "autoincrementmanager.h"
#undef BRMAUTOINCMGR_DLLEXPORT

#include <math.h>
#include <boost/date_time/posix_time/posix_time.hpp>

using namespace std;
using namespace boost;
using namespace boost::posix_time;

namespace BRM {

AutoincrementManager::AutoincrementManager()
{
}

AutoincrementManager::~AutoincrementManager()
{
}

void AutoincrementManager::startSequence(uint32_t oid, uint64_t firstNum, uint32_t colWidth,
                                         execplan::CalpontSystemCatalog::ColDataType colDataType)
{
	mutex::scoped_lock lk(lock);
	map<uint64_t, sequence>::iterator it;
	sequence s;

	it = sequences.find(oid);
	if (it != sequences.end())
		return;

	s.value = firstNum;
    if (isUnsigned(colDataType))
    {
        s.overflow = (0xFFFFFFFFFFFFFFFFULL >> (64-colWidth*8)) - 1;
    }
    else
    {
        s.overflow = (1ULL << (colWidth*8-1));
    }
	sequences[oid] = s;
}

bool AutoincrementManager::getAIRange(uint32_t oid, uint64_t count, uint64_t *firstNum)
{
	mutex::scoped_lock lk(lock);
	map<uint64_t, sequence>::iterator it;

	it = sequences.find(oid);
	if (it == sequences.end())
		throw runtime_error("There is no sequence with that lock");
	if ((count >= it->second.overflow ||
	  count + it->second.value > it->second.overflow ||
	  count + it->second.value <= it->second.value)
	  && count != 0)
		return false;
	*firstNum = it->second.value;
	it->second.value += count;
	return true;
}

void AutoincrementManager::resetSequence(uint32_t oid, uint64_t value)
{
	mutex::scoped_lock lk(lock);
	map<uint64_t, sequence>::iterator it;

	it = sequences.find(oid);
	if (it == sequences.end())
		return;
	it->second.value = value;
}

void AutoincrementManager::getLock(uint32_t oid)
{
	mutex::scoped_lock lk(lock);
	map<uint64_t, sequence>::iterator it;
	ptime stealTime = microsec_clock::local_time() + seconds(lockTime);

	bool gotIt = false;

	it = sequences.find(oid);
	if (it == sequences.end())
		throw runtime_error("There is no sequence with that lock");
	lk.unlock();

	while (!gotIt && microsec_clock::local_time() < stealTime) {
		gotIt = it->second.lock.try_lock();
		if (!gotIt)
			usleep(100000);
	}
	// If !gotIt, take possession
}

void AutoincrementManager::releaseLock(uint32_t oid)
{
	mutex::scoped_lock lk(lock);
	map<uint64_t, sequence>::iterator it;

	it = sequences.find(oid);
	if (it == sequences.end())
		return;   // it's unlocked if the lock doesn't exist...
	lk.unlock();

	it->second.lock.unlock();
}

void AutoincrementManager::deleteSequence(uint32_t oid)
{
	mutex::scoped_lock lk(lock);
	map<uint64_t, sequence>::iterator it;

	it = sequences.find(oid);
	if (it != sequences.end())
		sequences.erase(it);
}


} /* namespace BRM */
