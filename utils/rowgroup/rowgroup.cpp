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

//  $Id: rowgroup.cpp 2978 2012-01-16 20:16:44Z zzhu $

//
// C++ Implementation: rowgroup
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <vector>
//#define NDEBUG
#include <cassert>
#include <string>
#include <sstream>
#include <iterator>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "joblisttypes.h"

#include "rowgroup.h"

namespace rowgroup
{

Row::Row() { }

Row::Row(const Row &r) : columnCount(r.columnCount), baseRid(r.baseRid),
	offsets(r.offsets), types(r.types), data(r.data), scale(r.scale),
	precision(r.precision)
{ }

Row::~Row() { }

Row & Row::operator=(const Row &r)
{
	columnCount = r.columnCount;
	baseRid = r.baseRid;
	offsets = r.offsets;
	types = r.types;
	data = r.data;
	scale = r.scale;
	precision = r.precision;
	return *this;
}

string Row::toString() const
{
	ostringstream os;
	uint i;

	os << getRid() << ": ";
	for (i = 0; i < columnCount; i++) {
		if (isNullValue(i))
			os << "NULL ";
		else
			switch (types[i]) {
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR:
					os << getStringField(i) << " ";
					break;
				case CalpontSystemCatalog::FLOAT:
					os << getFloatField(i) << " ";
					break;
				case CalpontSystemCatalog::DOUBLE:
					os << getDoubleField(i) << " ";
					break;
				case CalpontSystemCatalog::LONGDOUBLE:
					os << getLongDoubleField(i) << " ";
					break;
				case CalpontSystemCatalog::VARBINARY: {
					uint len = getVarBinaryLength(i);
					uint8_t* val = getVarBinaryField(i);
					os << "0x" << hex;
					while (len-- > 0) {
						os << (uint)(*val >> 4);
						os << (uint)(*val++ & 0x0F);
					}
					os << " " << dec;
					break;
				}
				default:
					os << getIntField(i) << " ";
					break;
			}
	}
	return os.str();
}

void Row::initToNull()
{
	uint i;

	for (i = 0; i < columnCount; i++) {
		switch (types[i]) {
			case CalpontSystemCatalog::TINYINT:
				data[offsets[i]] = joblist::TINYINTNULL; break;
			case CalpontSystemCatalog::SMALLINT:
				*((uint16_t *) &data[offsets[i]]) = joblist::SMALLINTNULL; break;
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
				*((uint32_t *) &data[offsets[i]]) = joblist::INTNULL; break;
			case CalpontSystemCatalog::FLOAT:
				*((uint32_t *) &data[offsets[i]]) = joblist::FLOATNULL; break;
			case CalpontSystemCatalog::DATE:
				*((uint32_t *) &data[offsets[i]]) = joblist::DATENULL; break;
			case CalpontSystemCatalog::BIGINT:
				if (precision[i] != 9999)
					*((uint64_t *) &data[offsets[i]]) = joblist::BIGINTNULL;
				else  // work around for count() in outer join result.
					*((uint64_t *) &data[offsets[i]]) = 0;
				break;
			case CalpontSystemCatalog::DOUBLE:
				*((uint64_t *) &data[offsets[i]]) = joblist::DOUBLENULL; break;
			case CalpontSystemCatalog::DATETIME:
				*((uint64_t *) &data[offsets[i]]) = joblist::DATETIMENULL; break;
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR: {
				uint len = getColumnWidth(i);
				switch (len) {
					case 1:	data[offsets[i]] = joblist::CHAR1NULL; break;
					case 2: *((uint16_t *) &data[offsets[i]]) = joblist::CHAR2NULL; break;
					case 3:
					case 4: *((uint32_t *) &data[offsets[i]]) = joblist::CHAR4NULL; break;
					case 5:
					case 6:
					case 7:
					case 8: *((uint64_t *) &data[offsets[i]]) = joblist::CHAR8NULL;
							break;
					default:
						memset(&data[offsets[i]], 0, len);
						strcpy((char *) &data[offsets[i]], joblist::CPNULLSTRMARK.c_str());
						break;
				}
				break;
			}
			case CalpontSystemCatalog::DECIMAL: {
				uint len = getColumnWidth(i);
				switch (len) {
					case 1 : data[offsets[i]] = joblist::TINYINTNULL; break;
					case 2 : *((uint16_t *) &data[offsets[i]]) = joblist::SMALLINTNULL; break;
					case 4 : *((uint32_t *) &data[offsets[i]]) = joblist::INTNULL; break;
					default: *((uint64_t *) &data[offsets[i]]) = joblist::BIGINTNULL; break;
				}
				break;
			}
			case CalpontSystemCatalog::VARBINARY: {
				// zero length and NULL are treated the same
				memset(&data[offsets[i]], 0, offsets[i + 1] - offsets[i]);
				break;
			}
			case CalpontSystemCatalog::LONGDOUBLE: {
				// no NULL value for long double yet, this is a nan.
				memset(&data[offsets[i]], 0xFF, getColumnWidth(i));
				break;
			}
			default:
				ostringstream os;
				os << "Row::initToNull(): got bad column type (" << types[i] <<
					").  Width=" << getColumnWidth(i) << endl;
				os << toString();
				throw logic_error(os.str());
		}
	}
}

bool Row::isNullValue(uint colIndex) const
{
	switch (types[colIndex]) {
		case CalpontSystemCatalog::TINYINT:
			return (data[offsets[colIndex]] == joblist::TINYINTNULL);
		case CalpontSystemCatalog::SMALLINT:
			return (*((uint16_t *) &data[offsets[colIndex]]) == joblist::SMALLINTNULL);
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::INTNULL);
		case CalpontSystemCatalog::FLOAT:
			return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::FLOATNULL);
		case CalpontSystemCatalog::DATE:
			return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::DATENULL);
		{
             uint len = getColumnWidth(colIndex);
             switch (len) {
                case 1: return (data[offsets[colIndex]] == joblist::TINYINTNULL);
                case 2: return (*((uint16_t *) &data[offsets[colIndex]]) == joblist::SMALLINTNULL);
                case 4: return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::INTNULL);
                case 8: return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::BIGINTNULL);
             }
        }

		case CalpontSystemCatalog::BIGINT:
			return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::BIGINTNULL);
		case CalpontSystemCatalog::DOUBLE:
			return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::DOUBLENULL);
		case CalpontSystemCatalog::DATETIME:
			return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::DATETIMENULL);
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR: {
			uint len = getColumnWidth(colIndex);
			if (data[offsets[colIndex]] == 0) return true;
			switch (len) {
				case 1: return (data[offsets[colIndex]] == joblist::CHAR1NULL);
				case 2: return (*((uint16_t *) &data[offsets[colIndex]]) == joblist::CHAR2NULL);
				case 3:
				case 4: return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::CHAR4NULL);
				case 5:
				case 6:
				case 7:
				case 8: return
					(*((uint64_t *) &data[offsets[colIndex]]) == joblist::CHAR8NULL);
				default:
					return (!strncmp((char *) &data[offsets[colIndex]], joblist::CPNULLSTRMARK.c_str(), 8));
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL: {
			uint len = getColumnWidth(colIndex);
			switch (len) {
				case 1 : return (data[offsets[colIndex]] == joblist::TINYINTNULL);
				case 2 : return (*((uint16_t *) &data[offsets[colIndex]]) == joblist::SMALLINTNULL);
				case 4 : return (*((uint32_t *) &data[offsets[colIndex]]) == joblist::INTNULL);
				default: return (*((uint64_t *) &data[offsets[colIndex]]) == joblist::BIGINTNULL);
			}
			break;
		}
		case CalpontSystemCatalog::VARBINARY: {
			uint pos = offsets[colIndex];
			if (*((uint16_t*) &data[pos]) == 0)
				return true;
			else if ((strncmp((char *) &data[pos+2], joblist::CPNULLSTRMARK.c_str(), 8) == 0) &&
				*((uint16_t*) &data[pos]) == joblist::CPNULLSTRMARK.length())
				return true;

			break;
		}
		case CalpontSystemCatalog::LONGDOUBLE: {
			// return false;  // no NULL value for long double yet
			break;
		}
		default: {
			ostringstream os;
			os << "Row::isNullValue(): got bad column type (" << types[colIndex] <<
				").  Width=" << getColumnWidth(colIndex) << endl;
			os << toString() << endl;
			throw logic_error(os.str());
		}
	}

	return false;
}

uint64_t Row::getNullValue(uint colIndex) const
{
	switch (types[colIndex]) {
		case CalpontSystemCatalog::TINYINT:
			return joblist::TINYINTNULL;
		case CalpontSystemCatalog::SMALLINT:
			return joblist::SMALLINTNULL;
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return joblist::INTNULL;
		case CalpontSystemCatalog::FLOAT:
			return joblist::FLOATNULL;
		case CalpontSystemCatalog::DATE:
			return joblist::DATENULL;
		case CalpontSystemCatalog::BIGINT:
			return joblist::BIGINTNULL;
		case CalpontSystemCatalog::DOUBLE:
			return joblist::DOUBLENULL;
		case CalpontSystemCatalog::DATETIME:
			return joblist::DATETIMENULL;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR: {
			uint len = getColumnWidth(colIndex);
			switch (len) {
				case 1: return joblist::CHAR1NULL;
				case 2: return joblist::CHAR2NULL;
				case 3:
				case 4: return joblist::CHAR4NULL;
				case 5:
				case 6:
				case 7:
				case 8: return joblist::CHAR8NULL;
				default:
					throw logic_error("Row::getNullValue() Can't return the NULL string");
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL: {
			uint len = getColumnWidth(colIndex);
			switch (len) {
				case 1 : return joblist::TINYINTNULL;
				case 2 : return joblist::SMALLINTNULL;
				case 4 : return joblist::INTNULL;
				default: return joblist::BIGINTNULL;
			}
			break;
		}
		case CalpontSystemCatalog::LONGDOUBLE:
			return -1;  // no NULL value for long double yet, this is a nan.
		case CalpontSystemCatalog::VARBINARY:
		default:
			ostringstream os;
			os << "Row::getNullValue(): got bad column type (" << types[colIndex] <<
				").  Width=" << getColumnWidth(colIndex) << endl;
			os << toString() << endl;
			throw logic_error(os.str());
	}
}

/* This fcn might produce overflow warnings from the compiler, but that's OK.
 * The overflow is intentional...
 */
int64_t Row::getSignedNullValue(uint colIndex) const
{
	switch (types[colIndex]) {
		case CalpontSystemCatalog::TINYINT:
			return (int64_t) ((int8_t) joblist::TINYINTNULL);
		case CalpontSystemCatalog::SMALLINT:
			return (int64_t) ((int16_t) joblist::SMALLINTNULL);
		case CalpontSystemCatalog::MEDINT:
		case CalpontSystemCatalog::INT:
			return (int64_t) ((int32_t) joblist::INTNULL);
		case CalpontSystemCatalog::FLOAT:
			return (int64_t) ((int32_t) joblist::FLOATNULL);
		case CalpontSystemCatalog::DATE:
			return (int64_t) ((int32_t) joblist::DATENULL);
		case CalpontSystemCatalog::BIGINT:
			return joblist::BIGINTNULL;
		case CalpontSystemCatalog::DOUBLE:
			return joblist::DOUBLENULL;
		case CalpontSystemCatalog::DATETIME:
			return joblist::DATETIMENULL;
		case CalpontSystemCatalog::CHAR:
		case CalpontSystemCatalog::VARCHAR: {
			uint len = getColumnWidth(colIndex);
			switch (len) {
				case 1: return (int64_t) ((int8_t) joblist::CHAR1NULL);
				case 2: return (int64_t) ((int16_t) joblist::CHAR2NULL);
				case 3:
				case 4: return (int64_t) ((int32_t) joblist::CHAR4NULL);
				case 5:
				case 6:
				case 7:
				case 8: return joblist::CHAR8NULL;
				default:
					throw logic_error("Row::getSignedNullValue() Can't return the NULL string");
			}
			break;
		}
		case CalpontSystemCatalog::DECIMAL: {
			uint len = getColumnWidth(colIndex);
			switch (len) {
				case 1 : return (int64_t) ((int8_t)  joblist::TINYINTNULL);
				case 2 : return (int64_t) ((int16_t) joblist::SMALLINTNULL);
				case 4 : return (int64_t) ((int32_t) joblist::INTNULL);
				default: return joblist::BIGINTNULL;
			}
			break;
		}
		case CalpontSystemCatalog::LONGDOUBLE:
			return -1;  // no NULL value for long double yet, this is a nan.
		case CalpontSystemCatalog::VARBINARY:
		default:
			ostringstream os;
			os << "Row::getSignedNullValue(): got bad column type (" << types[colIndex] <<
				").  Width=" << getColumnWidth(colIndex) << endl;
			os << toString() << endl;
			throw logic_error(os.str());
	}
}

RowGroup::RowGroup() : columnCount(-1), data(NULL)
{ }

RowGroup::RowGroup(uint colCount,
	const vector<uint> &positions,
	const vector<uint> &roids,
	const vector<uint> &tkeys,
	const vector<CalpontSystemCatalog::ColDataType> &colTypes,
	const vector<uint> &cscale,
	const vector<uint> &cprecision) :
	columnCount(colCount), data(NULL), offsets(positions), oids(roids), keys(tkeys),
	types(colTypes), scale(cscale), precision(cprecision)
{ }

RowGroup::RowGroup(const RowGroup &r) :
	columnCount(r.columnCount), data(r.data), offsets(r.offsets), oids(r.oids), keys(r.keys),
	types(r.types), scale(r.scale), precision(r.precision)
{ }

RowGroup & RowGroup::operator=(const RowGroup &r)
{
	columnCount = r.columnCount;
	offsets = r.offsets;
	oids = r.oids;
	keys = r.keys;
	types = r.types;
	data = r.data;
	scale = r.scale;
	precision = r.precision;
	return *this;
}

RowGroup::~RowGroup() { }

void RowGroup::resetRowGroup(uint64_t rid)
{
	*((uint32_t *) &data[rowCountOffset]) = 0;
	*((uint64_t *) &data[baseRidOffset]) = rid & ~0x1fff;  // mask off lower 13 bits
	*((uint16_t *) &data[statusOffset]) = 0;
}

const vector<uint> & RowGroup::getOffsets() const
{
	return offsets;
}

const vector<uint> & RowGroup::getOIDs() const
{
	return oids;
}

const vector<uint> & RowGroup::getKeys() const
{
	return keys;
}

const vector<CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes() const
{
	return types;
}

vector<CalpontSystemCatalog::ColDataType>& RowGroup::getColTypes()
{
	return types;
}

const vector<uint> & RowGroup::getScale() const
{
	return scale;
}

const vector<uint> & RowGroup::getPrecision() const
{
	return precision;
}

void RowGroup::serialize(ByteStream &bs) const
{
	bs << (ByteStream::quadbyte)columnCount;
	serializeInlineVector<uint>(bs, offsets);
	serializeInlineVector<uint>(bs, oids);
	serializeInlineVector<uint>(bs, keys);
	serializeInlineVector<CalpontSystemCatalog::ColDataType>(bs, types);
	serializeInlineVector<uint>(bs, scale);
	serializeInlineVector<uint>(bs, precision);
}

void RowGroup::deserialize(ByteStream &bs)
{
	ByteStream::quadbyte qb;
	bs >> qb;
	columnCount = qb;
	deserializeInlineVector<uint>(bs, offsets);
	deserializeInlineVector<uint>(bs, oids);
	deserializeInlineVector<uint>(bs, keys);
	deserializeInlineVector<CalpontSystemCatalog::ColDataType>(bs, types);
	deserializeInlineVector<uint>(bs, scale);
	deserializeInlineVector<uint>(bs, precision);
}

uint RowGroup::getDataSize() const
{
	return headerSize + (getRowCount() * offsets[columnCount]);
}

uint RowGroup::getDataSize(uint64_t n) const
{
	assert(offsets.size() > columnCount);
	return headerSize + (n * offsets[columnCount]);
}

uint RowGroup::getMaxDataSize() const
{
	assert(offsets.size() > columnCount);
	return headerSize + (8192 * offsets[columnCount]);
}

uint RowGroup::getEmptySize() const
{
	return headerSize;
}

uint RowGroup::getStatus() const
{
	return *((uint16_t *) &data[statusOffset]);
}

void RowGroup::setStatus(uint16_t err)
{
	*((uint16_t *) &data[statusOffset]) = err;
}

uint RowGroup::getColumnWidth(uint col) const
{
	return offsets[col+1] - offsets[col];
}

uint RowGroup::getColumnCount() const
{
	return columnCount;
}

string RowGroup::toString() const
{
	ostringstream os;
	ostream_iterator<int> oIter1(os, "\t");

	os << "columncount = " << columnCount << endl;
	os << "oids:\t\t\t"; copy(oids.begin(), oids.end(), oIter1);
	os << endl;
	os << "keys:\t\t\t"; copy(keys.begin(), keys.end(), oIter1);
	os << endl;
	os << "offsets:\t"; copy(offsets.begin(), offsets.end(), oIter1);
	os << endl;
	os << "types:\t\t\t"; copy(types.begin(), types.end(), oIter1);
	os << endl;
	os << "scales:\t\t\t"; copy(scale.begin(), scale.end(), oIter1);
	os << endl;
	os << "precisions:\t\t"; copy(precision.begin(), precision.end(), oIter1);
	if (data != NULL) {
		Row r;
		initRow(&r);
		getRow(0, &r);
		os << "rowcount = " << getRowCount() << endl;
		os << "base rid = " << getBaseRid() << endl;
		os << "row data...\n";
		for (uint i = 0; i < getRowCount(); i++) {
			os << r.toString() << endl;
			r.nextRow();
		}
	}
	return os.str();
}

boost::shared_array<int> makeMapping(const RowGroup &r1, const RowGroup &r2)
{
	shared_array<int> ret(new int[r1.getColumnCount()]);
	//bool reserved[r2.getColumnCount()];
	bool* reserved = (bool*)alloca(r2.getColumnCount() * sizeof(bool));
	uint i, j;

	for (i = 0; i < r2.getColumnCount(); i++)
		reserved[i] = false;

	for (i = 0; i < r1.getColumnCount(); i++) {
		for (j = 0; j < r2.getColumnCount(); j++)
			if ((r1.getKeys()[i] == r2.getKeys()[j]) && !reserved[j]) {
				ret[i] = j;
				reserved[j] = true;
				break;
			}
		if (j == r2.getColumnCount())
			ret[i] = -1;
	}
	return ret;
}

void applyMapping(const boost::shared_array<int>& mapping, const Row &in, Row *out)
{
	uint i;

	for (i = 0; i < in.getColumnCount(); i++)
		if (mapping[i] != -1)
		{
			if (UNLIKELY(in.getColumnWidth(i) > 7 &&
			  (in.getColTypes()[i] == execplan::CalpontSystemCatalog::CHAR ||
			  in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARCHAR)))
				out->setStringField(in.getStringField(i), mapping[i]);
			else if (UNLIKELY(in.getColTypes()[i] == execplan::CalpontSystemCatalog::VARBINARY))
				out->setVarBinaryField(in.getVarBinaryField(i), in.getVarBinaryLength(i), mapping[i]);
			else
				out->setIntField(in.getIntField(i), mapping[i]);
		}
}

RowGroup& RowGroup::operator+=(const RowGroup& rhs)
{
	//not appendable if data is set
	assert(!data);

	columnCount += rhs.columnCount;

	oids.insert(oids.end(), rhs.oids.begin(), rhs.oids.end());
	keys.insert(keys.end(), rhs.keys.begin(), rhs.keys.end());
	types.insert(types.end(), rhs.types.begin(), rhs.types.end());
	scale.insert(scale.end(), rhs.scale.begin(), rhs.scale.end());
	precision.insert(precision.end(), rhs.precision.begin(), rhs.precision.end());

	//    +4  +4  +8       +2 +4  +8
	// (2, 6, 10, 18) + (2, 4, 8, 16) = (2, 6, 10, 18, 20, 24, 32)
	for (unsigned i = 1; i < rhs.offsets.size(); i++)
		offsets.push_back(offsets.back() + rhs.offsets[i] - rhs.offsets[i - 1]);

	return *this;
}

RowGroup operator+(const RowGroup& lhs, const RowGroup& rhs)
{
	RowGroup temp(lhs);
	return temp += rhs;
}

void RowGroup::addToSysDataList(execplan::CalpontSystemCatalog::NJLSysDataList& sysDataList)
{
	execplan::ColumnResult *cr;
	
	rowgroup::Row row;
	initRow(&row);
	uint rowCount = getRowCount();
	uint columnCount = getColumnCount();
	
	for (uint i = 0; i < rowCount; i++)
	{
		getRow(i, &row);
		for (uint j = 0; j < columnCount; j++)
		{
			int idx = sysDataList.findColumn(getOIDs()[j]);
			if(idx >= 0) {
				cr = sysDataList.sysDataVec[idx];
			}
			else {
				cr = new execplan::ColumnResult();
				cr->SetColumnOID(getOIDs()[j]);
				sysDataList.push_back(cr);
			}
			// @todo more data type checking. for now only check string, midint and bigint
			switch ((getColTypes()[j]))
			{
				case CalpontSystemCatalog::CHAR:
				case CalpontSystemCatalog::VARCHAR:
				{
					switch (getColumnWidth(j))
					{
						case 1:
							cr->PutData(row.getUintField<1>(j));
							break;
						case 2:
							cr->PutData(row.getUintField<2>(j));
							break;
						case 4:
							cr->PutData(row.getUintField<4>(j));
							break;
						case 8:
							cr->PutData(row.getUintField<8>(j));
							break;
						default:
						{
							string s = row.getStringField(j);
							cr->PutStringData(string(s.c_str(), strlen(s.c_str())));
						}
					}	
					break;
				}
				case CalpontSystemCatalog::MEDINT:
				case CalpontSystemCatalog::INT:
					cr->PutData(row.getIntField<4>(j));
					break;
				case CalpontSystemCatalog::DATE:
					cr->PutData(row.getUintField<4>(j));
					break;
				default:
					cr->PutData(row.getIntField<8>(j));
			}
			cr->PutRid(row.getRid());
		}
	}
}

}
// vim:ts=4 sw=4:

