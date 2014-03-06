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

// $Id: autoincrementdata.h 525 2010-01-19 23:18:05Z xlou $
//
/** @file */

#include <cassert>
#include <limits>
using namespace std;

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#include "autoincrementdata.h"

/*static*/
boost::mutex AutoincrementData::map_mutex;
/*static*/
AutoincrementData::AutoincDataMap AutoincrementData::fAutoincDataMap;
/* static */
AutoincrementData* AutoincrementData::makeAutoincrementData(uint32_t sessionID)
{
    boost::mutex::scoped_lock lock(map_mutex);
    AutoincrementData* instance;
    AutoincDataMap::const_iterator it = fAutoincDataMap.find(sessionID);

    if (it == fAutoincDataMap.end())
    {
        instance = new AutoincrementData();	 
        fAutoincDataMap[sessionID] = instance;
        return instance;
    }

    return it->second;    
}

/* static */
void AutoincrementData::removeAutoincrementData(uint32_t sessionID)
{
    boost::mutex::scoped_lock lock(map_mutex);
    AutoincDataMap::iterator it = fAutoincDataMap.find(sessionID);
    if (it != fAutoincDataMap.end())
    {
        delete (*it).second;
        fAutoincDataMap.erase(it);
    }    
}	

AutoincrementData::AutoincrementData()
{
}
AutoincrementData::~AutoincrementData()
{
}

void AutoincrementData::setNextValue(uint32_t columnOid, long long nextValue)
{
	boost::mutex::scoped_lock lk(fOIDnextvalLock);	
	fOidNextValueMap[columnOid] = nextValue;	
}

long long AutoincrementData::getNextValue(uint32_t columnOid)
{
	boost::mutex::scoped_lock lk(fOIDnextvalLock);	
	long long nextValue = 0;
	OIDNextValue::iterator it = fOidNextValueMap.find(columnOid);
	if (it != fOidNextValueMap.end())
	{
		nextValue = it->second;
	}
	return nextValue;
}

AutoincrementData::OIDNextValue & AutoincrementData::getOidNextValueMap()
{
	boost::mutex::scoped_lock lk(fOIDnextvalLock);	
		
	return fOidNextValueMap;
}
// vim:ts=4 sw=4:
