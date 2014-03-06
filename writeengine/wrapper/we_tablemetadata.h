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

#ifndef WE_TABLEMETADATA_H__
#define WE_TABLEMETADATA_H__

#include <stdint.h>
#include "we_type.h"
#include "brmtypes.h"
#include <map>
#include <vector>

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine
{

struct ColExtInfo {
	uint16_t dbRoot; 
	uint32_t partNum;
	uint16_t segNum;
	HWM hwm;
	RID lastRid;
	int64_t max;
	int64_t min;
	bool isNewExt;
	bool current;
	uint16_t	compType;
	bool isDict;
	
	ColExtInfo() :
	dbRoot(0),
	partNum(0),
	segNum(0),
	hwm(0),
	lastRid(0),
	max(0),
	min(0),
	isNewExt(false),
	current(true),
	compType(2),
	isDict(false)
	{}
};

typedef std::vector<ColExtInfo> ColExtsInfo;

typedef std::map<OID, ColExtsInfo> ColsExtsInfoMap;

class TableMetaData
{
public:
	typedef std::map <uint32_t,  TableMetaData*>  TableMetaDataMap;
	EXPORT static TableMetaData* makeTableMetaData(uint32_t tableOid);
	/** remove and delete the instance map to the tableOid
     *  @param tableOid
     */ 
    EXPORT static void removeTableMetaData(uint32_t tableOid);
	
	EXPORT ColExtsInfo & getColExtsInfo (OID columnOid);
	EXPORT void setColExtsInfo (OID columnOid, ColExtsInfo colExtsInfo);
	EXPORT ColsExtsInfoMap& getColsExtsInfoMap();

private:
	/** Constuctors */
    explicit TableMetaData();
    explicit TableMetaData(const TableMetaData& rhs);
	~TableMetaData();
	static boost::mutex map_mutex;
	static TableMetaDataMap fTableMetaDataMap;
	boost::mutex fColsExtsInfoLock;
	ColsExtsInfoMap fColsExtsInfoMap;
};

} //end of namespace

#undef EXPORT

#endif
// vim:ts=4 sw=4:

