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
 * $Id: tablecolumn.cpp 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

#include <stdint.h>
#include <vector>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "columnresult.h"

#include "tablecolumn.h"

namespace joblist
{

/** @brief constructor
*/
TableColumn::TableColumn(const execplan::CalpontSystemCatalog::OID columnOID, const supportedType columnType) :
	fColumnOID(columnOID), fIsNullColumn(true), fColumnType(columnType)
{
	preserialized.reset(new ByteStream());
}

TableColumn::TableColumn() : fColumnOID(0), fIsNullColumn(true), fColumnType(UNDEFINED)
{
	preserialized.reset(new ByteStream());
};

void TableColumn::serialize()
{
// 	cerr << "pre-serializing" << endl;
	messageqcpp::ByteStream::octbyte rowCount;
	messageqcpp::ByteStream::octbyte oid;
	messageqcpp::ByteStream::byte nullFlag;
	messageqcpp::ByteStream::byte columnType;

	oid = fColumnOID;
	*preserialized << oid;

	columnType = fColumnType;
	*preserialized << columnType;

	if (fIsNullColumn)
		nullFlag = 1;
	else
		nullFlag = 0;
	*preserialized << nullFlag;

	if(!fIsNullColumn) {
		if(fColumnType == UINT64) {
			rowCount = fIntValues->size();
			*preserialized << rowCount;
			preserialized->append((uint8_t *) &(*fIntValues)[0], 8 * rowCount);
		}
		else if(fColumnType == STRING) {
			rowCount = fStrValues->size();
			*preserialized << rowCount;
			for(uint32_t i = 0; i < rowCount; i++)
				*preserialized << (*fStrValues)[i];
		}
	}
}

/** @brief serializes the object into the passed byte stream.
*/
void TableColumn::serialize(messageqcpp::ByteStream& b)
{
	if (preserialized->length() != 0) {
		b += *preserialized;
 		preserialized->reset();
// 		cerr << "returning a preserialized column" << endl;
		return;
	}

	messageqcpp::ByteStream::octbyte rowCount;
	messageqcpp::ByteStream::octbyte oid;
	messageqcpp::ByteStream::byte nullFlag;
	messageqcpp::ByteStream::byte columnType;

	oid = fColumnOID;
	b << oid;

	columnType = fColumnType;
	b << columnType;

	if (fIsNullColumn)
		nullFlag = 1;
	else
		nullFlag = 0;
	b << nullFlag;

	if(!fIsNullColumn) {
		if(fColumnType == UINT64) {
			rowCount = fIntValues->size();
			b << rowCount;
			b.append((uint8_t *) &(*fIntValues)[0], 8 * rowCount);
		}
		else if(fColumnType == STRING) {
			rowCount = fStrValues->size();
			b << rowCount;
			for(uint32_t i = 0; i < rowCount; i++)
				b << (*fStrValues)[i];
		}
	}


}

/** @brief inflates the object from the passed byte stream.
*/
void TableColumn::unserialize(messageqcpp::ByteStream& b) {
	messageqcpp::ByteStream::octbyte rowCount;
	messageqcpp::ByteStream::octbyte oid;
	messageqcpp::ByteStream::byte nullFlag;
	messageqcpp::ByteStream::byte columnType;
	uint32_t val32;
	uint16_t val16;
	uint8_t val8;

	b >> oid;
	fColumnOID = oid;

// 	cout << "UN: oid = " << oid << endl;

	b >> columnType;
	/* Fudge fColumnType for onlookers. */
	if (columnType != STRING)
		fColumnType = UINT64;
	else
		fColumnType = STRING;

	b >> nullFlag;
	fIsNullColumn = (nullFlag != 0);
// 	cout << "UN (" << oid << "): is null: " << (int) nullFlag << endl;

	if(!fIsNullColumn) {
		b >> rowCount;
// 		cout << "UN (" << oid << "): rowCount = " << rowCount << endl;
		if (columnType != STRING)
			fIntValues.reset(new std::vector<uint64_t>());

		/* XXXPAT: A switch on fColumnType is more concise, but I suspect this is
		   a little faster b/c of fewer jumps in the loop.  Since it's a row-by-row operation, it
		   has to scream. */
		if (columnType == UINT8) {
// 			cout << "UN (" << oid << "): is an 8\n";
			fIntValues->reserve(rowCount);
			for (uint32_t i = 0; i < rowCount; ++i) {
				b >> val8;
// 				cout << "UN (" << oid << "): " << (int) val8 << " at " << i << endl;
				fIntValues->push_back(val8);
			}
		}
		else if (columnType == UINT16) {
// 			cout << "UN (" << oid << "): is a 16\n";
			fIntValues->reserve(rowCount);
			for (uint32_t i = 0; i < rowCount; ++i) {
				b >> val16;
// 				cout << "UN (" << oid << "): " << val16 << " at " << i << endl;
				fIntValues->push_back(val16);
			}
		}
		else if (columnType == UINT32) {
// 			cout << "UN (" << oid << "): is a 32\n";
			fIntValues->reserve(rowCount);
			for (uint32_t i = 0; i < rowCount; ++i) {
				b >> val32;
// 				cout << "UN (" << oid << "): " << val32 << " at " << i << endl;
				fIntValues->push_back(val32);
			}
		}
		else if (columnType == UINT64) {
			fIntValues->resize(rowCount);
  			memcpy(&(*fIntValues)[0], b.buf(), 8 * rowCount);
  			b.advance(8 * rowCount);
		}
		else if (columnType == STRING) {
			fStrValues.reset(new std::vector<std::string>());
			fStrValues->reserve(rowCount);
			std::string value;
			for(uint32_t i = 0; i < rowCount; i++) {
				b >> value;
// 				cout << "UN: " << value << endl;
				fStrValues->push_back(value);
			}
		}
	}
}

/** @brief adds the column and it's values to the passed NJLSysDataList or appends the values if the column is already included in the NJLSysDataList.
*/
void TableColumn::addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList, const std::vector<uint64_t>& rids) {

	execplan::ColumnResult *cr;
	int idx = sysDataList.findColumn(fColumnOID);
	if(idx >= 0) {
		cr = sysDataList.sysDataVec[idx];
	}
	else {
		cr = new execplan::ColumnResult();
		cr->SetColumnOID(fColumnOID);
		sysDataList.push_back(cr);
	}
	if(fColumnType == UINT64) {
		uint32_t vsize = fIntValues->size();
		bool putRids = (rids.size() == vsize);
 		for(uint32_t i = 0; i < vsize; i++) {
			cr->PutData((*fIntValues)[i]);
			if(putRids) {
				cr->PutRid(rids[i]);
			}
			else {
				cr->PutRid(0);
			}
		}
	}
	else {
		uint32_t vsize = fStrValues->size();
		bool putRids = (rids.size() == vsize);
		for(uint32_t i = 0; i < vsize; i++) {
			cr->PutStringData((*fStrValues)[i]);
			if(putRids) {
				cr->PutRid(rids[i]);
			}
			else {
				cr->PutRid(0);
			}
		}
	}
}
#if 0
void TableColumn::addToSysDataRids(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList, const std::vector<uint64_t>& rids)
{

	execplan::ColumnResult *cr;
	int idx = sysDataList.findColumn(fColumnOID);
	if(idx >= 0) {
		cr = sysDataList.sysDataVec[idx];
	}
	else {
		cr = new execplan::ColumnResult();
		cr->SetColumnOID(fColumnOID);
		sysDataList.push_back(cr);
	}

	uint32_t vsize = (fIntValues) ? fIntValues->size() : fStrValues->size();

	bool putRids = (rids.size() == vsize);
	for(uint32_t i = 0; i < vsize; i++) {
		if(putRids) {
			cr->PutRidOnly(rids[i]);
		}
		else {
			cr->PutRidOnly(0);
		}
	}

}
#endif

}  // namespace

