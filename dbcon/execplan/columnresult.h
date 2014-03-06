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

/******************************************************************************
 * $Id$
 *
 *****************************************************************************/
/** @file */

#ifndef EXECPLAN_COLUMNRESULT_H_
#define EXECPLAN_COLUMNRESULT_H_

#include <sys/types.h>
#include <vector>
#include <string>
#include <stdint.h>

namespace execplan {

/** @file 
 	This serves as a bridge between the new and old joblist code.
	ColumnResults are column data created from the TableBands 
	returned by the NJL.  In getSysData, we convert TableBands
	to NJLCR's to avoid having to rewrite all of the systemcatalog
	code that used the old ColumnResult class.  It only implements
	the part of the ColumnResult API used by the system catalog. 
	Hack as necessary.
*/
class ColumnResult
{
public:
	ColumnResult() : oid(0), dcount(0) { }
	// the other defaults are OK.

	int64_t GetData(uint32_t index) const
		{ return intData[index]; }

	void PutData(int64_t d)
		{ intData.push_back(d); dcount++; }

	const std::string& GetStringData(uint32_t index) const
		{ return stringData[index]; }

	void PutStringData(const std::string& s)
		{ stringData.push_back(s); dcount++; }

	int ColumnOID() const
		{ return oid; }

	void SetColumnOID(int o)
		{ oid = o; }

	uint64_t GetRid(uint32_t index) const
		{ return rids[index]; }

	void PutRid(uint64_t rid)
		{ rids.push_back(rid); }

	void PutRidOnly(uint64_t rid)
		{ rids.push_back(rid); dcount++; }

	int dataCount() const
		{ return dcount; }

private:
	//defaults okay
	//ColumnResult(const ColumnResult& rhs);
	//ColumnResult& operator=(const ColumnResult& rhs);

	std::vector<int64_t> intData;
	std::vector<std::string> stringData;
	std::vector<uint64_t> rids;
	int oid;
	int dcount;  // data, string, and row counters
};

}

#endif
