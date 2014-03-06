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

// $Id$
//
/** @file */

#include <cassert>
#include <limits>
using namespace std;

#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>

#include "we_tablemetadata.h"

namespace WriteEngine
{
/*static*/
boost::mutex TableMetaData::map_mutex;
/*static*/
TableMetaData::TableMetaDataMap TableMetaData::fTableMetaDataMap;


TableMetaData* TableMetaData::makeTableMetaData(uint32_t tableOid)
{
	boost::mutex::scoped_lock lock(map_mutex);
	TableMetaData* instance;
    TableMetaDataMap::const_iterator it = fTableMetaDataMap.find(tableOid);
    if (it == fTableMetaDataMap.end())
	{		
		instance = new TableMetaData();
		fTableMetaDataMap[tableOid] = instance;
		return instance;
	}
    return it->second;    
}

/* static */
void TableMetaData::removeTableMetaData(uint32_t tableOid)
{
    boost::mutex::scoped_lock lock(map_mutex);
    TableMetaDataMap::iterator it = fTableMetaDataMap.find(tableOid);
    if (it != fTableMetaDataMap.end())
    {
        delete (*it).second;
        fTableMetaDataMap.erase(it);
    }    
}

TableMetaData::TableMetaData()
{
}
TableMetaData::~TableMetaData()
{
}

ColExtsInfo & TableMetaData::getColExtsInfo (OID columnOid)
{
	boost::mutex::scoped_lock lock(fColsExtsInfoLock);
	ColsExtsInfoMap::iterator it = fColsExtsInfoMap.find(columnOid);
    if (it != fColsExtsInfoMap.end())
	{
		return it->second;
	}
	else
	{
		ColExtsInfo colExtsInfo;
		fColsExtsInfoMap[columnOid] = colExtsInfo;
		return fColsExtsInfoMap[columnOid];
	}
}

void TableMetaData::setColExtsInfo (OID columnOid, ColExtsInfo colExtsInfo)
{
	boost::mutex::scoped_lock lock(fColsExtsInfoLock);
	ColsExtsInfoMap::iterator it = fColsExtsInfoMap.find(columnOid);
    if (it != fColsExtsInfoMap.end())
	{
		it->second = colExtsInfo;
	}
	else
	{
		fColsExtsInfoMap[columnOid] = colExtsInfo;
	}
}

ColsExtsInfoMap& TableMetaData::getColsExtsInfoMap()
{
	boost::mutex::scoped_lock lock(fColsExtsInfoLock);
	return fColsExtsInfoMap;
}
} //end of namespace
// vim:ts=4 sw=4:
