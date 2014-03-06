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

// $Id: tablelockdata.h 525 2010-01-19 23:18:05Z xlou $
//
/** @file */

#include <cassert>
#include <limits>
using namespace std;

#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#include "tablelockdata.h"

namespace dmlpackageprocessor
{

/*static*/
boost::mutex TablelockData::map_mutex;
/*static*/
TablelockData::TablelockDataMap TablelockData::fTablelockDataMap;
/* static */
TablelockData* TablelockData::makeTablelockData(uint32_t sessionID)
{
    boost::mutex::scoped_lock lock(map_mutex);
    TablelockData* instance;
    TablelockDataMap::const_iterator it = fTablelockDataMap.find(sessionID);

    if (it == fTablelockDataMap.end())
    {
        instance = new TablelockData();
        fTablelockDataMap[sessionID] = instance;
        return instance;
    }

    return it->second;
}

/* static */
void TablelockData::removeTablelockData(uint32_t sessionID)
{
    boost::mutex::scoped_lock lock(map_mutex);
    TablelockDataMap::iterator it = fTablelockDataMap.find(sessionID);
    if (it != fTablelockDataMap.end())
    {
        delete (*it).second;
        fTablelockDataMap.erase(it);
    }
}

TablelockData::TablelockData()
{
}
TablelockData::~TablelockData()
{
}

void TablelockData::setTablelock(uint32_t tableOid, uint64_t tablelockId)
{
	boost::mutex::scoped_lock lk(fOIDTablelock);
	fOIDTablelockMap[tableOid] = tablelockId;
}

uint64_t TablelockData::getTablelockId(uint32_t tableOid)
{
	boost::mutex::scoped_lock lk(fOIDTablelock);
	uint64_t tablelockId = 0;
	OIDTablelock::iterator it = fOIDTablelockMap.find(tableOid);
	if (it != fOIDTablelockMap.end())
	{
		tablelockId = it->second;
	}
	return tablelockId;
}

TablelockData::OIDTablelock & TablelockData::getOidTablelockMap()
{
	boost::mutex::scoped_lock lk(fOIDTablelock);

	return fOIDTablelockMap;
}

}
// vim:ts=4 sw=4:
