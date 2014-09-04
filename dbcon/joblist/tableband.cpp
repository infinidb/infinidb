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
 * $Id: tableband.cpp 8582 2012-06-01 20:09:05Z xlou $
 *
 *****************************************************************************/
#include <boost/any.hpp>
#include <stdexcept>
#include <string>
#include <boost/version.hpp>
#if BOOST_VERSION <= 103200
#error your boost version is too old
#endif
using namespace std;

#define TABLEBAND_DLLEXPORT
#include "tableband.h"
#undef TABLEBAND_DLLEXPORT

#include "bytestream.h"
using namespace messageqcpp;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "dataconvert.h"
using namespace dataconvert;

#include "joblisttypes.h"

#include "tablecolumn.h"
#include "datalist.h"
#include "elementtype.h"

namespace
{
const string boldStart = "\033[0;1m";
const string boldStop = "\033[0;39m";
}

namespace joblist
{

TableBand::TableBand(CalpontSystemCatalog::OID tableOID) :
	fTableOID(tableOID), fRowCount(0), numColumns(0), fStatus(0)
{

}

void TableBand::serialize(ByteStream& b) const {
	ByteStream::octbyte ob;

	ob = fTableOID;
	b << ob;
	ob = numColumns;
	b << ob;
	ob = fRowCount;
	b << ob;
	ob = fStatus;
	b << ob;
	b << (uint8_t) false;	// the "got rows" var.  when serialized this way, there are never rids
	
	VBA::size_type i;
	for(i = 0; i < numColumns; i++)
		fColumns[i]->serialize(b);
}

void TableBand::unserialize(ByteStream& b) {
	ByteStream::octbyte colSize;
	ByteStream::octbyte status;
	ByteStream::octbyte ob;
	uint8_t tmp8;
	bool gotRids;

	b >> ob;
	fTableOID = ob;
	b >> colSize;
	b >> ob;
	fRowCount = ob;
	b >> status;
	fStatus = status;

	if (fStatus)
		return;

	b >> tmp8;
	gotRids = (tmp8 != 0);
	if (gotRids) {
		fRids.resize(fRowCount);
		memcpy(&fRids[0], b.buf(), fRowCount << 3);
		b.advance(fRowCount << 3);
	}

	fColumns.clear();
	fColumns.reserve(colSize);
	for(ByteStream::octbyte i = 0; i < colSize; i++) {
		boost::shared_ptr<TableColumn> colSPtr(new TableColumn);
		colSPtr->unserialize(b);
		fColumns.push_back(colSPtr);
	}
	numColumns = colSize;
}

int TableBand::find(execplan::CalpontSystemCatalog::OID columnOID) const
{
	VBA::const_iterator iter = fColumns.begin();
	VBA::const_iterator end = fColumns.end();

	int idx = 0;
	while (iter != end)
	{
		if ((*iter)->getColumnOID() == columnOID)
			return idx;
		idx++;
		++iter;
	}
	return -1;
}

void TableBand::addColumn(FifoDataList *fifo, uint it, uint32_t flushInterval, bool firstColumn, bool isExeMgr)
{
	boost::shared_ptr<vector<uint64_t> > sv;
	vector<uint64_t> *values;
	boost::shared_ptr<TableColumn> stc;
	TableColumn *tc;
	int index;

	// Create a new TableColumn through a shared pointer.
	stc.reset(new TableColumn(fifo->OID(), TableColumn::UINT64));
	tc = stc.get();

	// Find the column in the band.
 	index = find(tc->getColumnOID());
	if (index < 0) {
		std::cerr << "find failed?" << std::endl;
		throw std::exception();
	}

	// Create a vector of values for the TableColumn through a shared pointer.
	sv.reset(new vector<uint64_t>());
	values = sv.get();
    if (fRowCount > 0) values->reserve(fRowCount);	
	
	UintRowGroup rw;
	uint64_t ridCount = 0;
	bool more = fifo->next(it, &rw);
		
	if (firstColumn)
	{
		fRowCount = 0;		
		while (more) {
			for (uint64_t i = 0; i < rw.count; ++i)
			{
				values->push_back(rw.et[i].second);	
				fRids.push_back(rw.et[i].first);	
			}
			ridCount += rw.count;

			if (ridCount >= flushInterval)
				more = false;
			else
				more = fifo->next(it, &rw);
		}
		fRowCount = ridCount;
	}
	else 
	{
		while (more) {
			for (uint64_t i = 0; i < rw.count; ++i)
				values->push_back(rw.et[i].second);	
			ridCount += rw.count;

			if (ridCount >= flushInterval)
				more = false;
			else
				more = fifo->next(it, &rw);
		}

		if ((fRowCount > 0) && (ridCount != fRowCount))
		{
			ostringstream oss;
			oss << "TableBand::addColumn(uint64 pairs): attempt to add column " << fifo->OID() << " w/ " <<
				ridCount << " to a table w/ " << fRowCount << " rows!";
			cerr << boldStart << oss.str() << boldStop << endl;
			throw runtime_error(oss.str());
		}
	}
	tc->setIntValues(sv);
	if (isExeMgr)
		tc->serialize();
	fColumns[index] = stc;
}

void TableBand::addColumn(StringFifoDataList *fifo, uint it, uint32_t flushInterval, bool firstColumn, bool isExeMgr)
{

	boost::shared_ptr<vector<string> > sv;
	vector<string> *values;
	boost::shared_ptr<TableColumn> stc;
	TableColumn *tc;
	int index;

	// Create a new TableColumn through a shared pointer.
	stc.reset(new TableColumn(fifo->OID(), TableColumn::STRING));
	tc = stc.get();		

	// Find the column in the band.
 	index = find(tc->getColumnOID());
	if (index < 0) {
		std::cerr << "find failed?" << std::endl;
		throw std::exception();
	}

	// Create a vector of values for the TableColumn through a shared pointer.
	sv.reset(new vector<string>());
	values = sv.get();

	// Add the values to the vector.
	if (fRowCount > 0) values->reserve(fRowCount);	
	
	StringRowGroup rw;
	bool more = fifo->next(it, &rw);
	uint64_t ridCount = 0;
	
	if (firstColumn)
	{
		fRowCount = 0;
		while (more) {
			for (uint64_t i = 0; i < rw.count; ++i)
			{
				values->push_back(rw.et[i].second);	
				fRids.push_back(rw.et[i].first);	
			}
			ridCount += rw.count;			

			if (ridCount >= flushInterval)
				more = false;
			else
				more = fifo->next(it, &rw);
		}
		fRowCount = ridCount;
	}
	else 
	{
		while (more) {
			for (uint64_t i = 0; i < rw.count; ++i)
				values->push_back(rw.et[i].second);	
			ridCount += rw.count;

			if (ridCount >= flushInterval)
				more = false;
			else
				more = fifo->next(it, &rw);
		}

		if ((fRowCount > 0) && (ridCount != fRowCount))
		{
			ostringstream oss;
			oss << "TableBand::addColumn(string pairs): attempt to add column " << fifo->OID() << " w/ " <<
				ridCount << " to a table w/ " << fRowCount << " rows!";
			cerr << boldStart << oss.str() << boldStop << endl;
			throw runtime_error(oss.str());
		}
	}
	tc->setStrValues(sv);
	if (isExeMgr)
		tc->serialize();
	fColumns[index] = stc;
}

void TableBand::addNullColumn(execplan::CalpontSystemCatalog::OID columnOID)
{
	boost::shared_ptr<TableColumn> stc;
	stc.reset(new TableColumn(columnOID, TableColumn::UNDEFINED));

	int idx = find(columnOID);
	if (idx != -1) cout << "TableBand::addNullColumn: adding a null column on top of a non-null column! " << columnOID << endl;
	if (idx >= 0)
		fColumns[idx] = stc;
	else {
		fColumns.push_back(stc);
		++numColumns;
	}
}

void TableBand::convertToSysDataList(CalpontSystemCatalog::NJLSysDataList& sysDataList,  CalpontSystemCatalog *csc)
{
	uint i;
	for(i = 0; i < numColumns; i++)
		if(!fColumns[i]->isNullColumn())
			fColumns[i]->addToSysDataList(sysDataList, fRids);
}
#if 0
void TableBand::convertToSysDataRids(CalpontSystemCatalog::NJLSysDataList& sysDataList)
{
	uint i;
	for(i = 0; i < numColumns; i++)
		if(!fColumns[i]->isNullColumn())
			fColumns[i]->addToSysDataRids(sysDataList, fRids);
}
#endif

void TableBand::clearRows()
{
	for(uint i = 0; i < numColumns; i++) {
		fColumns[i]->getStrValues()->clear();
		fColumns[i]->getIntValues()->clear();
	}
	fRowCount = 0;
}

void TableBand::toString() const
{
	cout << endl;
	cout << "---------------------------------------------------------------------";
	cout << "TableBand Table OID:  " << fTableOID << endl;
	cout << "# of Columns = " << numColumns << endl;
	cout << "Rows = " << fRowCount << endl;
	cout << endl;
	cout << "Columns:" << endl;
	uint i;
	for(i = 0; i < numColumns; i++) {
		cout << "Column OID:  " << fColumns[i]->getColumnOID();
		if(fColumns[i]->isNullColumn())
			cout << " (null column)";
		cout << endl;
	}
	cout << "---------------------------------------------------------------------" << endl << endl;
}
} //namespace joblist

namespace
{
boost::mutex sysCatMutex;

CalpontSystemCatalog::ColType getColType(execplan::CalpontSystemCatalog::OID oid)
{
	boost::mutex::scoped_lock lk(sysCatMutex);
	return CalpontSystemCatalog::makeCalpontSystemCatalog()->colType(oid);
}
}

namespace joblist
{
ostream& TableBand::formatToCSV(ostream& os, char sep) const
{
	vector<CalpontSystemCatalog::ColType> colTypes(numColumns);
	ostringstream oss;
	unsigned numRealCols = numColumns;
	for (unsigned col = (numColumns - 1); true; --col)
	{
		if (!fColumns[col]->isNullColumn())
			break;
		numRealCols--;
		if (col == 0)
			break;
	}
	for (unsigned row = 0; row < fRowCount; row++)
	{
		for (unsigned col = 0; col < numRealCols; col++)
		{
			if (fColumns[col]->isNullColumn())
			{
				oss << sep;
				continue;
			}
			if (colTypes[col].columnOID == 0)
				colTypes[col] = getColType(fColumns[col]->getColumnOID());
			//lifted more or less from dbcon/sm/dhcs_ti.cpp:dhcs_tpl_scan_fetch_fillrow()
			if (fColumns[col]->getColumnType() == TableColumn::UINT64)
			{
				uint64_t rowVal;
				rowVal = (*fColumns[col]->getIntValues())[row];
				if (colTypes[col].colDataType == CalpontSystemCatalog::DATE)
				{
					if (rowVal != 0xfffffffeLL)
					{
						//oss << DataConvert::dateToString(rowVal);
						char ds[11];
						unsigned t = 0;
						rowVal >>= 6;
						t = rowVal & 0x3f;
						ds[10] = 0;
						ds[9] = (t % 10) + '0';
						ds[8] = (t / 10) + '0';
						ds[7] = '-';
						rowVal >>= 6;
						t = rowVal & 0xf;
						ds[6] = (t % 10) + '0';
						ds[5] = (t / 10) + '0';
						ds[4] = '-';
						rowVal >>= 4;
						t = rowVal & 0xffff;
						//TODO: do we need to handle this case better?
						//t %= 10000;
						ds[3] = (t % 10) + '0';
						ds[2] = ((t / 10) % 10) + '0';
						ds[1] = ((t / 100) % 10) + '0';
						ds[0] = (t / 1000) + '0';
						oss << ds;
					}
				}
				else if (colTypes[col].colDataType == CalpontSystemCatalog::DATETIME)
				{
					if (rowVal != 0xfffffffffffffffeLL)
					{
						//oss << DataConvert::dateToString(rowVal);
						char ds[20];
						unsigned t = 0;
						rowVal >>= 20;
						t = rowVal & 0x3f;
						ds[19] = 0;
						ds[18] = (t % 10) + '0';
						ds[17] = (t / 10) + '0';
						ds[16] = ':';
						rowVal >>= 6;
						t = rowVal & 0x3f;
						ds[15] = (t % 10) + '0';
						ds[14] = (t / 10) + '0';
						ds[13] = ':';
						rowVal >>= 6;
						t = rowVal & 0x3f;
						ds[12] = (t % 10) + '0';
						ds[11] = (t / 10) + '0';
						ds[10] = ' ';
						rowVal >>= 6;
						t = rowVal & 0x3f;
						ds[9] = (t % 10) + '0';
						ds[8] = (t / 10) + '0';
						ds[7] = '-';
						rowVal >>= 6;
						t = rowVal & 0xf;
						ds[6] = (t % 10) + '0';
						ds[5] = (t / 10) + '0';
						ds[4] = '-';
						rowVal >>= 4;
						t = rowVal & 0xffff;
						//TODO: do we need to handle this case better?
						//t %= 10000;
						ds[3] = (t % 10) + '0';
						ds[2] = ((t / 10) % 10) + '0';
						ds[1] = ((t / 100) % 10) + '0';
						ds[0] = (t / 1000) + '0';
						oss << ds;
					}
				}
				else if (colTypes[col].colDataType == CalpontSystemCatalog::CHAR ||
					colTypes[col].colDataType == CalpontSystemCatalog::VARCHAR)
				{
					int cw = colTypes[col].colWidth;
					bool isVC = (colTypes[col].colDataType == CalpontSystemCatalog::VARCHAR);
					if (cw == 1)
					{
						if ( (!isVC && rowVal != 0xfeLL) ||
							(isVC && rowVal != 0xfeffLL) )
							oss << static_cast<char>(rowVal);
					}
					else if (cw == 2)
					{
						if ( (!isVC && rowVal != 0xfeffLL) ||
							(isVC && rowVal != 0xfeffffffLL) )
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8);
					}
					else if (cw == 3)
					{
						if (rowVal != 0xfeffffffLL)
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8)
								<< static_cast<char>(rowVal>>16);
					}
					else if (cw == 4)
					{
						if ( (!isVC && rowVal != 0xfeffffffLL) ||
							(isVC && rowVal != 0xfeffffffffffffffLL) )
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8)
								<< static_cast<char>(rowVal>>16)
								<< static_cast<char>(rowVal>>24);
					}
					else if (cw == 5)
					{
						if (rowVal != 0xfeffffffffffffffLL)
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8)
								<< static_cast<char>(rowVal>>16)
								<< static_cast<char>(rowVal>>24)
								<< static_cast<char>(rowVal>>32);
					}
					else if (cw == 6)
					{
						if (rowVal != 0xfeffffffffffffffLL)
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8)
								<< static_cast<char>(rowVal>>16)
								<< static_cast<char>(rowVal>>24)
								<< static_cast<char>(rowVal>>32)
								<< static_cast<char>(rowVal>>40);
					}
					else if (cw == 7)
					{
						if (rowVal != 0xfeffffffffffffffLL)
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8)
								<< static_cast<char>(rowVal>>16)
								<< static_cast<char>(rowVal>>24)
								<< static_cast<char>(rowVal>>32)
								<< static_cast<char>(rowVal>>40)
								<< static_cast<char>(rowVal>>48);
					}
					else if (cw == 8)
					{
						if (rowVal != 0xfeffffffffffffffLL)
							oss << static_cast<char>(rowVal)
								<< static_cast<char>(rowVal>>8)
								<< static_cast<char>(rowVal>>16)
								<< static_cast<char>(rowVal>>24)
								<< static_cast<char>(rowVal>>32)
								<< static_cast<char>(rowVal>>40)
								<< static_cast<char>(rowVal>>48)
								<< static_cast<char>(rowVal>>56);
					}
					else
					{
						//TODO: handle more cases
						oss << static_cast<int64_t>(rowVal);
					}
				}
				else
				{
					if ( (colTypes[col].colWidth == 8 && rowVal != 0x8000000000000000LL) ||
						(colTypes[col].colWidth == 4 && rowVal != 0x80000000LL) ||
						(colTypes[col].colWidth == 2 && rowVal != 0x8000LL) ||
						(colTypes[col].colWidth == 1 && rowVal != 0x80LL) )
					{
						int64_t* sRowValp = reinterpret_cast<int64_t*>(&rowVal);
#ifndef __LP64__
						//Something is amiss in the 32-bit versions...rowVal always
						//  seems to have the upper 32-bits cleared. This will sign-extend
						//  the negative values so the dumped values are correct.
						if (*sRowValp & 0x0000000080000000ULL)
							*sRowValp |= 0xffffffff80000000ULL;
#endif
						int negAdj = (*sRowValp < 0 ? 2 : 1);
						if (colTypes[col].scale > 0)
						{
							//biggest 64-bit number is 19 digits, + 1 for sign,
							//     + 1 for dp, + 1 for NULL
							char tmp[22];
							snprintf(&tmp[1],
								21,
#ifndef __LP64__
								"%lld",
#else
								"%ld",
#endif
								*sRowValp);
							size_t l = strlen(&tmp[negAdj]);
							ssize_t n = l - colTypes[col].scale;
							//we need to put some leading zeros
							//  ("2" needs to look like "02", etc.)
							if (n < 0)
							{
								n = -n;
								memmove(&tmp[negAdj + n], &tmp[negAdj], (l + 1));
								do
								{
									tmp[negAdj + --n] = '0';
								} while (n > 0);
							}
							n += (negAdj - 1);
							memmove(&tmp[0], &tmp[1], n);
							tmp[n] = '.';
							oss << tmp;
						}
						else
						{
							oss << *sRowValp;
						}
					}
				}
			}
			else if (fColumns[col]->getColumnType() == TableColumn::STRING)
			{
				string rowVal;
				rowVal = (*fColumns[col]->getStrValues())[row];
				if (rowVal != CPNULLSTRMARK)
					oss << rowVal;
			}
			oss << sep;
		}
		oss << endl;
	}
	const string& s = oss.str();
	os.write(s.c_str(), s.length());

	return os;
}

}  // namespace

