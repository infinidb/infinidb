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

//  $Id: rowaggregation.cpp 4017 2013-07-26 16:20:29Z pleblanc $


/** @file rowaggregation.cpp
 *
 * File contains classes used to perform RowGroup aggregation.  RowAggregation
 * is the primary class.
 */

#include <unistd.h>
#include <sstream>
#include <stdexcept>
#include <limits>

#include "joblisttypes.h"
#include "resourcemanager.h"
#include "groupconcat.h"

#include "blocksize.h"
#include "errorcodes.h"
#include "exceptclasses.h"
#include "errorids.h"
#include "idberrorinfo.h"
#include "dataconvert.h"
#include "returnedcolumn.h"
#include "arithmeticcolumn.h"
#include "functioncolumn.h"
#include "simplecolumn.h"
#include "rowgroup.h"
#include "funcexp.h"
#include "rowaggregation.h"
#include "calpontsystemcatalog.h"
#include "utils_utf8.h"

//..comment out NDEBUG to enable assertions, uncomment NDEBUG to disable
//#define NDEBUG
#include <cassert>

using namespace std;
using namespace boost;
using namespace dataconvert;

// inlines of RowAggregation that used only in this file
namespace
{

// @bug3522, use smaller rowgroup size to conserve memory.
const int64_t AGG_ROWGROUP_SIZE = 256;

template <typename T>
bool minMax(T d1, T d2, int type)
{
	if (type == rowgroup::ROWAGG_MIN) return d1 < d2;
	else                              return d1 > d2;
}


inline int64_t getIntNullValue(int colType)
{
	switch (colType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
			return joblist::TINYINTNULL;
		case execplan::CalpontSystemCatalog::SMALLINT:
			return joblist::SMALLINTNULL;
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
			return joblist::INTNULL;
		case execplan::CalpontSystemCatalog::BIGINT:
		default:
			return joblist::BIGINTNULL;
	}
}


inline uint64_t getUintNullValue(int colType, int colWidth = 0)
{
	switch (colType)
	{
		case execplan::CalpontSystemCatalog::CHAR:
		{
			if (colWidth == 1) return joblist::CHAR1NULL;
			else if (colWidth == 2) return joblist::CHAR2NULL;
			else if (colWidth < 5) return joblist::CHAR4NULL;
			break;
		}
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			if (colWidth < 3) return joblist::CHAR2NULL;
			else if (colWidth < 5) return joblist::CHAR4NULL;
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			return joblist::DATENULL;
		}
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			return joblist::DATETIMENULL;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			switch (colWidth)
			{
				case 1:
				{
					return joblist::TINYINTNULL;
				}
				case 2:
				{
					return joblist::SMALLINTNULL;
				}
				case 4:
				{
					return joblist::INTNULL;
				}
				default:
				{
					return joblist::BIGINTNULL;
				}
			}
		}
		case execplan::CalpontSystemCatalog::UTINYINT:
		{
			return joblist::UTINYINTNULL;
		}
		case execplan::CalpontSystemCatalog::USMALLINT:
		{
			return joblist::USMALLINTNULL;
		}
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		{
			return joblist::UINTNULL;
		}
		case execplan::CalpontSystemCatalog::UBIGINT:
		{
			return joblist::UBIGINTNULL;
		}
		default:
		{
			break;
		}
	}

	return joblist::CHAR8NULL;
}


inline double getDoubleNullValue()
{
	uint64_t x = joblist::DOUBLENULL;
	double* y = (double*)&x;
	return *y;
}


inline float getFloatNullValue()
{
	uint32_t x = joblist::FLOATNULL;
	float* y = (float*)&x;
	return *y;
}


inline string getStringNullValue()
{
	return joblist::CPNULLSTRMARK;
}

}


namespace rowgroup
{

KeyStorage::KeyStorage(const RowGroup &keys, Row **tRow) :tmpRow(tRow), rg(keys)
{
	RGData data(rg);

	rg.setData(&data);
	rg.resetRowGroup(0);
	rg.initRow(&row);
	rg.getRow(0, &row);
	storage.push_back(data);
	memUsage = 0;
}

inline RowPosition KeyStorage::addKey()
{
	RowPosition pos;

	if (rg.getRowCount() == 8192)
	{
		RGData data(rg);
		rg.setData(&data);
		rg.resetRowGroup(0);
		rg.getRow(0, &row);
		storage.push_back(data);
	}

	copyRow(**tmpRow, &row);
	memUsage += row.getRealSize();
	pos.group = storage.size()-1;
	pos.row = rg.getRowCount();
	rg.incRowCount();
	row.nextRow();
	return pos;
}

inline uint64_t KeyStorage::getMemUsage()
{
	return memUsage;
}


ExternalKeyHasher::ExternalKeyHasher(const RowGroup &r, KeyStorage *k, uint32_t keyColCount, Row **tRow) :
	tmpRow(tRow), lastKeyCol(keyColCount-1), ks(k)
{
	r.initRow(&row);
}

inline uint64_t ExternalKeyHasher::operator()(const RowPosition &pos) const
{
	if (pos.group == RowPosition::MSB)
		return (*tmpRow)->hash(lastKeyCol);

	RGData &rgData = ks->storage[pos.group];
	rgData.getRow(pos.row, &row);
	return row.hash(lastKeyCol);
}


ExternalKeyEq::ExternalKeyEq(const RowGroup &r, KeyStorage *k, uint32_t keyColCount, Row **tRow) :
	tmpRow(tRow), lastKeyCol(keyColCount-1), ks(k)
{
	r.initRow(&row1);
	r.initRow(&row2);
}

inline bool ExternalKeyEq::operator()(const RowPosition &pos1, const RowPosition &pos2) const
{
	Row *r1, *r2;

	if (pos1.group == RowPosition::MSB)
		r1 = *tmpRow;
	else {
		ks->storage[pos1.group].getRow(pos1.row, &row1);
		r1 = &row1;
	}
	if (pos2.group == RowPosition::MSB)
		r2 = *tmpRow;
	else {
		ks->storage[pos2.group].getRow(pos2.row, &row2);
		r2 = &row2;
	}

	return r1->equals(*r2, lastKeyCol);
}


static const string overflowMsg("Aggregation overflow.");

inline void RowAggregation::updateIntMinMax(int64_t val1, int64_t val2, int64_t col, int func)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setIntField(val1, col);
	else if (minMax(val1, val2, func))
		fRow.setIntField(val1, col);
}


inline void RowAggregation::updateUintMinMax(uint64_t val1, uint64_t val2, int64_t col, int func)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setUintField(val1, col);
	else if (minMax(val1, val2, func))
		fRow.setUintField(val1, col);
}


inline void RowAggregation::updateCharMinMax(uint64_t val1, uint64_t val2, int64_t col, int func)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setUintField(val1, col);
	else if (minMax(uint64ToStr(val1), uint64ToStr(val2), func))
		fRow.setUintField(val1, col);
}


inline void RowAggregation::updateDoubleMinMax(double val1, double val2, int64_t col, int func)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setDoubleField(val1, col);
	else if (minMax(val1, val2, func))
		fRow.setDoubleField(val1, col);
}


inline void RowAggregation::updateFloatMinMax(float val1, float val2, int64_t col, int func)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setFloatField(val1, col);
	else if (minMax(val1, val2, func))
		fRow.setFloatField(val1, col);
}


#define STRCOLL_ENH__

void RowAggregation::updateStringMinMax(string val1, string val2, int64_t col, int func)
{
	if (isNull(fRowGroupOut, fRow, col))
	{
		fRow.setStringField(val1, col);
	}
#ifdef STRCOLL_ENH__
	else
	{
		int tmp = funcexp::utf8::idb_strcoll(val1.c_str(), val2.c_str());
		if ((tmp < 0 && func == rowgroup::ROWAGG_MIN) ||
			(tmp > 0 && func == rowgroup::ROWAGG_MAX))
		{
			fRow.setStringField(val1, col);
		}
	}
#else
	else if (minMax(val1, val2, func))
	{
		fRow.setStringField(val1, col);
	}
#endif
}


inline void RowAggregation::updateIntSum(int64_t val1, int64_t val2, int64_t col)
{
	if (isNull(fRowGroupOut, fRow, col))
	{
		fRow.setIntField(val1, col);
	}
	else
	{
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
		if (((val2 >= 0) && ((numeric_limits<int64_t>::max() - val2) >= val1)) ||
			((val2 <  0) && ((numeric_limits<int64_t>::min() - val2) <= val1)))
			fRow.setIntField(val1 + val2, col);
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
		if (fRow.getColTypes()[col] != execplan::CalpontSystemCatalog::DOUBLE &&
			fRow.getColTypes()[col] != execplan::CalpontSystemCatalog::UDOUBLE)
		{
			if (((val2 >= 0) && ((numeric_limits<int64_t>::max() - val2) >= val1)) ||
				((val2 <  0) && ((numeric_limits<int64_t>::min() - val2) <= val1)))
			{
				fRow.setIntField(val1 + val2, col);
			}
			else
			{
				execplan::CalpontSystemCatalog::ColDataType* cdtp = fRow.getColTypes();
				cdtp += col;
				*cdtp = execplan::CalpontSystemCatalog::DOUBLE;
				updateDoubleSum((double)val1, (double)val2, col);
			}
		}
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
		else
		{
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
			ostringstream oss;
			oss << overflowMsg << ": " << val2 << "+" << val1;
			if (val2 > 0)
				oss << " > " << numeric_limits<uint64_t>::max();
			else
				oss << " < " << numeric_limits<uint64_t>::min();
			throw logging::QueryDataExcept(oss.str(), logging::aggregateDataErr);
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
			double* dp2 = (double*)&val2;
			updateDoubleSum((double)val1, *dp2, col);
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
		}
	}
}

inline void RowAggregation::updateUintSum(uint64_t val1, uint64_t val2, int64_t col)
{
	if (isNull(fRowGroupOut, fRow, col))
	{
		fRow.setUintField(val1, col);
	}
	else
	{
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
		if ((numeric_limits<uint64_t>::max() - val2) >= val1)
			fRow.setUintField(val1 + val2, col);
		else
		{
			ostringstream oss;
			oss << overflowMsg << ": " << val2 << "+" << val1 << " > " << numeric_limits<uint64_t>::max();
			throw logging::QueryDataExcept(oss.str(), logging::aggregateDataErr);
		}
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
		if (fRow.getColTypes()[col] != execplan::CalpontSystemCatalog::DOUBLE &&
			fRow.getColTypes()[col] != execplan::CalpontSystemCatalog::UDOUBLE)
		{
			if ((numeric_limits<uint64_t>::max() - val2) >= val1)
			{
				fRow.setUintField(val1 + val2, col);
			}
			else
			{
				execplan::CalpontSystemCatalog::ColDataType* cdtp = fRow.getColTypes();
				cdtp += col;
				*cdtp = execplan::CalpontSystemCatalog::DOUBLE;
				updateDoubleSum((double)val1, (double)val2, col);
			}
		}
		else
		{
			double* dp2 = (double*)&val2;
			updateDoubleSum((double)val1, *dp2, col);
		}
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
	}
}

inline void RowAggregation::updateDoubleSum(double val1, double val2, int64_t col)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setDoubleField(val1, col);
	else
		fRow.setDoubleField(val1 + val2, col);
}


inline void RowAggregation::updateFloatSum(float val1, float val2, int64_t col)
{
	if (isNull(fRowGroupOut, fRow, col))
		fRow.setFloatField(val1, col);
	else
		fRow.setFloatField(val1 + val2, col);
}


//------------------------------------------------------------------------------
// Verify if the column value is NULL
// row(in) - Row to be included in aggregation.
// col(in) - column in the input row group
// return  - equal to null or not
//------------------------------------------------------------------------------
inline bool RowAggregation::isNull(const RowGroup* pRowGroup, const Row& row, int64_t col)
{
	/* TODO: Can we replace all of this with a call to row.isNullValue(col)? */
	bool ret = false;

	int colDataType = (pRowGroup->getColTypes())[col];
	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		{
			ret = ((uint8_t)row.getIntField(col) == joblist::TINYINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::UTINYINT:
		{
			ret = ((uint8_t)row.getIntField(col) == joblist::UTINYINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			int colWidth = pRowGroup->getColumnWidth(col);
			// bug 1853, use token to check null
			// scale here is used to indicate token, not real string.
			if ((pRowGroup->getScale())[col] > 0)
			{
				if (row.getIntField(col) &  joblist::BIGINTNULL)
					ret = true;

				// break the case block
				break;
			}

			// real string to check null
			if (colWidth <= 8)
			{
				if (colWidth == 1)
					ret = ((uint8_t)row.getUintField(col) == joblist::CHAR1NULL);
				else if (colWidth == 2)
					ret = ((uint16_t)row.getUintField(col) == joblist::CHAR2NULL);
				else if (colWidth < 5)
					ret = ((uint32_t)row.getUintField(col) == joblist::CHAR4NULL);
				else
					ret = ((uint64_t)row.getUintField(col) == joblist::CHAR8NULL);
			}
			else
			{
				//@bug 1821
				ret = (row.equals("", col) || row.equals(joblist::CPNULLSTRMARK, col));
			}
			break;
		}
		case execplan::CalpontSystemCatalog::SMALLINT:
		{
			ret = ((uint16_t)row.getIntField(col) == joblist::SMALLINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::USMALLINT:
		{
			ret = ((uint16_t)row.getIntField(col) == joblist::USMALLINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			ret = ((uint64_t)row.getUintField(col) == joblist::DOUBLENULL);
			break;
		}
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		{
			ret = ((uint32_t)row.getIntField(col) == joblist::INTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		{
			ret = ((uint32_t)row.getIntField(col) == joblist::UINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			ret = ((uint32_t)row.getUintField(col) == joblist::FLOATNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		{
			ret = ((uint32_t)row.getUintField(col) == joblist::DATENULL);
			break;
		}
		case execplan::CalpontSystemCatalog::BIGINT:
		{
			ret = ((uint64_t)row.getIntField(col) == joblist::BIGINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::UBIGINT:
		{
			ret = ((uint64_t)row.getIntField(col) == joblist::UBIGINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			int colWidth = pRowGroup->getColumnWidth(col);
			int64_t val = row.getIntField(col);
			if (colWidth == 1)
				ret = ((uint8_t)val == joblist::TINYINTNULL);
			else if (colWidth == 2)
				ret = ((uint16_t)val == joblist::SMALLINTNULL);
			else if (colWidth == 4)
				ret = ((uint32_t)val == joblist::INTNULL);
			else
				ret = ((uint64_t)val == joblist::BIGINTNULL);
			break;
		}
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			ret = ((uint64_t)row.getUintField(col) == joblist::DATETIMENULL);
			break;
		}
		case execplan::CalpontSystemCatalog::VARBINARY:
		{
			ret = (row.equals("", col) || row.equals(joblist::CPNULLSTRMARK, col));
			break;
		}
		default:
			break;
	}

	return ret;
}


//------------------------------------------------------------------------------
// Row Aggregation default constructor
//------------------------------------------------------------------------------
RowAggregation::RowAggregation() :
	fAggMapPtr(NULL), fRowGroupOut(NULL),
	fTotalRowCount(0), fMaxTotalRowCount(AGG_ROWGROUP_SIZE),
	fSmallSideRGs(NULL), fLargeSideRG(NULL), fSmallSideCount(0)
{
}


RowAggregation::RowAggregation(const vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
                               const vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols) :
	fAggMapPtr(NULL), fRowGroupOut(NULL),
	fTotalRowCount(0), fMaxTotalRowCount(AGG_ROWGROUP_SIZE),
	fSmallSideRGs(NULL), fLargeSideRG(NULL), fSmallSideCount(0)
{
	fGroupByCols.assign(rowAggGroupByCols.begin(), rowAggGroupByCols.end());
	fFunctionCols.assign(rowAggFunctionCols.begin(), rowAggFunctionCols.end());
}


RowAggregation::RowAggregation(const RowAggregation& rhs):
	fAggMapPtr(NULL), fRowGroupOut(NULL),
	fTotalRowCount(0), fMaxTotalRowCount(AGG_ROWGROUP_SIZE),
	fSmallSideRGs(NULL), fLargeSideRG(NULL), fSmallSideCount(0)
{
	//fGroupByCols.clear();
	//fFunctionCols.clear();

	fGroupByCols.assign(rhs.fGroupByCols.begin(), rhs.fGroupByCols.end());
	fFunctionCols.assign(rhs.fFunctionCols.begin(), rhs.fFunctionCols.end());
}


//------------------------------------------------------------------------------
// Row Aggregation destructor.
//------------------------------------------------------------------------------
RowAggregation::~RowAggregation()
{
	if (fAggMapPtr)
	{
		delete fAggMapPtr;
		fAggMapPtr = NULL;
	}
}


//------------------------------------------------------------------------------
// Aggregate the rows in pRows.  User should make Multiple calls to
// addRowGroup() to aggregate multiple RowGroups. When all RowGroups have
// been input, a call should be made to endOfInput() to signal the end of data.
// nextRowGroup() can then be called iteratively to access the aggregated
// results.
//
// pRows(in) - RowGroup to be aggregated.
//------------------------------------------------------------------------------
void RowAggregation::addRowGroup(const RowGroup* pRows)
{
	// no group by == no map, everything done in fRow
	if (fGroupByCols.empty())
	{
		fRowGroupOut->setRowCount(1);

		// special, but very common case -- count(*) without groupby columns
		if (fFunctionCols.size() == 1 && fFunctionCols[0]->fAggFunction == ROWAGG_COUNT_ASTERISK)
		{
			if (countSpecial(pRows))
				return;
		}
	}
	fRowGroupOut->setDBRoot(pRows->getDBRoot());

	Row rowIn;
	pRows->initRow(&rowIn);
	pRows->getRow(0, &rowIn);
	for (uint64_t i = 0; i < pRows->getRowCount(); ++i)
	{
		aggregateRow(rowIn);
		rowIn.nextRow();
	}
}


void RowAggregation::addRowGroup(const RowGroup* pRows, vector<Row::Pointer>& inRows)
{
	// this function is for threaded aggregation, which is for group by and distinct.
	// if (countSpecial(pRows))

	Row rowIn;
	pRows->initRow(&rowIn);
	for (uint32_t i = 0; i < inRows.size(); i++)
	{
		rowIn.setData(inRows[i]);
		aggregateRow(rowIn);
	}
}


//------------------------------------------------------------------------------
// Set join rowgroups and mappings
//------------------------------------------------------------------------------
void RowAggregation::setJoinRowGroups(vector<RowGroup> *pSmallSideRG, RowGroup *pLargeSideRG)
{
	fSmallSideRGs = pSmallSideRG;
	fLargeSideRG = pLargeSideRG;
	fSmallSideCount = fSmallSideRGs->size();
	fSmallMappings.reset(new shared_array<int>[fSmallSideCount]);

	for (uint32_t i = 0; i < fSmallSideCount; i++)
		fSmallMappings[i] = makeMapping((*fSmallSideRGs)[i], fRowGroupIn);

	fLargeMapping = makeMapping(*fLargeSideRG, fRowGroupIn);

	rowSmalls.reset(new Row[fSmallSideCount]);
	for (uint32_t i = 0; i < fSmallSideCount; i++)
		(*fSmallSideRGs)[i].initRow(&rowSmalls[i]);
}


//------------------------------------------------------------------------------
// Initilalize the data members to meaningful values, setup the hashmap.
// The fRowGroupOut must have a valid data pointer before this.
//------------------------------------------------------------------------------
void RowAggregation::initialize()
{
	// Calculate the length of the hashmap key.
	fAggMapKeyCount = fGroupByCols.size();

	// Initialize the work row.
	fRowGroupOut->resetRowGroup(0);
	fRowGroupOut->initRow(&fRow);
	fRowGroupOut->getRow(0, &fRow);
	makeAggFieldsNull(fRow);

	// Keep a copy of the null row to initialize new map entries.
	fRowGroupOut->initRow(&fNullRow, true);
	fNullRowData.reset(new uint8_t[fNullRow.getSize()]);
	fNullRow.setData(fNullRowData.get());
	copyRow(fRow, &fNullRow);

	// save the original output rowgroup data as primary row data
	fPrimaryRowData = fRowGroupOut->getRGData();

	// Need map only if groupby list is not empty.
	if (!fGroupByCols.empty())
	{
		fHasher.reset(new AggHasher(fRow, &tmpRow, fGroupByCols.size(), this));
		fEq.reset(new AggComparator(fRow, &tmpRow, fGroupByCols.size(), this));
		fAlloc.reset(new utils::STLPoolAllocator<RowPosition>());
		fAggMapPtr = new RowAggMap_t(10, *fHasher, *fEq, *fAlloc);
	}
	else
	{
		fRowGroupOut->setRowCount(1);
		attachGroupConcatAg();
	}


	// Save the RowGroup data pointer
	fResultDataVec.push_back(fRowGroupOut->getRGData());

	// for 8k poc: an empty output row group to match message count
	fEmptyRowGroup = *fRowGroupOut;
	fEmptyRowData.reinit(*fRowGroupOut, 1);
	fEmptyRowGroup.setData(&fEmptyRowData);
	fEmptyRowGroup.resetRowGroup(0);
	fEmptyRowGroup.initRow(&fEmptyRow);
	fEmptyRowGroup.getRow(0, &fEmptyRow);

	copyRow(fNullRow, &fEmptyRow);
	if (fGroupByCols.empty())  // no groupby
		fEmptyRowGroup.setRowCount(1);
}

//------------------------------------------------------------------------------
// Reset the working data to aggregate next logical block
//------------------------------------------------------------------------------
void RowAggregation::reset()
{
	fTotalRowCount = 0;
	fMaxTotalRowCount = AGG_ROWGROUP_SIZE;
	fRowGroupOut->setData(fPrimaryRowData);
	fRowGroupOut->resetRowGroup(0);
	fRowGroupOut->getRow(0, &fRow);
	copyNullRow(fRow);
	attachGroupConcatAg();

	if (!fGroupByCols.empty())
	{
		fHasher.reset(new AggHasher(fRow, &tmpRow, fGroupByCols.size(), this));
		fEq.reset(new AggComparator(fRow, &tmpRow, fGroupByCols.size(), this));
		fAlloc.reset(new utils::STLPoolAllocator<RowPosition>());
		delete fAggMapPtr;
		fAggMapPtr = new RowAggMap_t(10, *fHasher, *fEq, *fAlloc);
	}

	fResultDataVec.clear();
	fResultDataVec.push_back(fRowGroupOut->getRGData());
}


void RowAggregationUM::reset()
{
	RowAggregation::reset();

	if (fKeyOnHeap)
	{
		fKeyRG = fRowGroupIn.truncate(fGroupByCols.size());
		fKeyStore.reset(new KeyStorage(fKeyRG, &tmpRow));
		fExtEq.reset(new ExternalKeyEq(fKeyRG, fKeyStore.get(), fKeyRG.getColumnCount(), &tmpRow));
		fExtHash.reset(new ExternalKeyHasher(fKeyRG, fKeyStore.get(), fKeyRG.getColumnCount(), &tmpRow));
		fExtKeyMapAlloc.reset(new utils::STLPoolAllocator<pair<RowPosition, RowPosition> >());
		fExtKeyMap.reset(new ExtKeyMap_t(10, *fExtHash, *fExtEq, *fExtKeyMapAlloc));
	}
}


void RowAggregationUM::aggregateRowWithRemap(Row& row)
{
	pair<ExtKeyMap_t::iterator, bool> inserted;
	RowPosition pos(RowPosition::MSB, 0);

	tmpRow = &row;
	inserted = fExtKeyMap->insert(pair<RowPosition, RowPosition>(pos, pos));
	if (inserted.second)
	{
		// if it was successfully inserted, fix the inserted values
		if (++fTotalRowCount > fMaxTotalRowCount && !newRowGroup())
		{
			throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
				errorMsg(logging::ERR_AGGREGATION_TOO_BIG), logging::ERR_AGGREGATION_TOO_BIG);
		}

		pos = fKeyStore->addKey();
		fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &fRow);
		fRowGroupOut->incRowCount();
		initMapData(row);     //seems heavy-handed
		attachGroupConcatAg();
		inserted.first->second = RowPosition(fResultDataVec.size()-1, fRowGroupOut->getRowCount()-1);

		// replace the key value with an equivalent copy, yes this is OK
		const_cast<RowPosition &>((inserted.first->first)) = pos;
	}
	else
	{
		pos = inserted.first->second;
		fResultDataVec[pos.group]->getRow(pos.row, &fRow);
	}

	updateEntry(row);
}



void RowAggregationUM::aggregateRow(Row &row)
{
	if (UNLIKELY(fKeyOnHeap))
		aggregateRowWithRemap(row);
	else
		RowAggregation::aggregateRow(row);
}

void RowAggregation::aggregateRow(Row& row)
{
	// groupby column list is not empty, find the entry.
	if (!fGroupByCols.empty())
	{
		pair<RowAggMap_t::iterator, bool> inserted;

		// do a speculative insert
		tmpRow = &row;
		inserted = fAggMapPtr->insert(RowPosition(RowPosition::MSB, 0));
		if (inserted.second)
		{
			// if it was successfully inserted, fix the inserted values
			if (++fTotalRowCount > fMaxTotalRowCount && !newRowGroup())
			{
				throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
					errorMsg(logging::ERR_AGGREGATION_TOO_BIG), logging::ERR_AGGREGATION_TOO_BIG);
			}

			fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &fRow);
			fRowGroupOut->incRowCount();
			initMapData(row);     //seems heavy-handed

			attachGroupConcatAg();

			// replace the key value with an equivalent copy, yes this is OK
			const_cast<RowPosition &>(*(inserted.first)) =
				RowPosition(fResultDataVec.size() - 1, fRowGroupOut->getRowCount() - 1);
		}
		else {
			//fRow.setData(*(inserted.first));
			const RowPosition &pos = *(inserted.first);
			fResultDataVec[pos.group]->getRow(pos.row, &fRow);
		}
	}

	updateEntry(row);
}


//------------------------------------------------------------------------------
// Initialize the working row, all aggregation fields to all null values or 0.
//------------------------------------------------------------------------------
void RowAggregation::initMapData(const Row& rowIn)
{
	// First, copy the null row.
	copyNullRow(fRow);

	// Then, populate the groupby cols.
	for (uint64_t i = 0; i < fGroupByCols.size(); i++)
	{
		int64_t colOut = fGroupByCols[i]->fOutputColumnIndex;
		if (colOut == numeric_limits<unsigned int>::max())
			continue;

		int64_t colIn = fGroupByCols[i]->fInputColumnIndex;
		int colDataType = ((fRowGroupIn.getColTypes())[colIn]);
		switch (colDataType)
		{
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::SMALLINT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::BIGINT:
			case execplan::CalpontSystemCatalog::DECIMAL:
			case execplan::CalpontSystemCatalog::UDECIMAL:
			{
				fRow.setIntField(rowIn.getIntField(colIn), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::UTINYINT:
			case execplan::CalpontSystemCatalog::USMALLINT:
			case execplan::CalpontSystemCatalog::UMEDINT:
			case execplan::CalpontSystemCatalog::UINT:
			case execplan::CalpontSystemCatalog::UBIGINT:
			{
				fRow.setUintField(rowIn.getUintField(colIn), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::CHAR:
			case execplan::CalpontSystemCatalog::VARCHAR:
			{
				int colWidth = fRowGroupIn.getColumnWidth(colIn);
				if (colWidth <= 8)
				{
					fRow.setUintField(rowIn.getUintField(colIn), colOut);
				}
				else
				{
					fRow.setStringField(rowIn.getStringPointer(colIn),
										rowIn.getStringLength(colIn), colOut);
				}
				break;
			}
			case execplan::CalpontSystemCatalog::DOUBLE:
			case execplan::CalpontSystemCatalog::UDOUBLE:
			{
				fRow.setDoubleField(rowIn.getDoubleField(colIn), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::FLOAT:
			case execplan::CalpontSystemCatalog::UFLOAT:
			{
				fRow.setFloatField(rowIn.getFloatField(colIn), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::DATE:
			case execplan::CalpontSystemCatalog::DATETIME:
			{
				fRow.setUintField(rowIn.getUintField(colIn), colOut);
				break;
			}
			default:
			{
				break;
			}
		}
	}
}


//------------------------------------------------------------------------------
//  Add group_concat to the initialized working row
//------------------------------------------------------------------------------
void RowAggregation::attachGroupConcatAg()
{
}


//------------------------------------------------------------------------------
// Make all aggregation fields to null.
//------------------------------------------------------------------------------
void RowAggregation::makeAggFieldsNull(Row& row)
{
	// initialize all bytes to 0
	memset(row.getData(), 0, row.getSize());
	//row.initToNull();

	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		// Initial count fields to 0.
		int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;
		if (fFunctionCols[i]->fAggFunction == ROWAGG_COUNT_ASTERISK ||
			fFunctionCols[i]->fAggFunction == ROWAGG_COUNT_COL_NAME ||
			fFunctionCols[i]->fAggFunction == ROWAGG_COUNT_DISTINCT_COL_NAME ||
			fFunctionCols[i]->fAggFunction == ROWAGG_COUNT_NO_OP ||
			fFunctionCols[i]->fAggFunction == ROWAGG_GROUP_CONCAT ||
			fFunctionCols[i]->fAggFunction == ROWAGG_STATS)
		{
//			done by memset
//			row.setIntField(0, colOut);
			continue;
		}

		// ROWAGG_BIT_AND : 0xFFFFFFFFFFFFFFFFULL;
		// ROWAGG_BIT_OR/ROWAGG_BIT_XOR : 0 (already set).
		if (fFunctionCols[i]->fAggFunction == ROWAGG_BIT_OR ||
			fFunctionCols[i]->fAggFunction == ROWAGG_BIT_XOR)
		{
			continue;
		}
		else if (fFunctionCols[i]->fAggFunction == ROWAGG_BIT_AND)
		{
			row.setUintField(0xFFFFFFFFFFFFFFFFULL, colOut);
			continue;
		}

		// Initial other aggregation fields to null.
		int colDataType = (fRowGroupOut->getColTypes())[colOut];
		switch (colDataType)
		{
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::SMALLINT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::BIGINT:
			{
				row.setIntField(getIntNullValue(colDataType), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::UTINYINT:
			case execplan::CalpontSystemCatalog::USMALLINT:
			case execplan::CalpontSystemCatalog::UMEDINT:
			case execplan::CalpontSystemCatalog::UINT:
			case execplan::CalpontSystemCatalog::UBIGINT:
			{
				row.setUintField(getUintNullValue(colDataType), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::DECIMAL:
			case execplan::CalpontSystemCatalog::UDECIMAL:
			{
				int colWidth = fRowGroupOut->getColumnWidth(colOut);
				row.setIntField(getUintNullValue(colDataType, colWidth), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::CHAR:
			case execplan::CalpontSystemCatalog::VARCHAR:
			{
				int colWidth = fRowGroupOut->getColumnWidth(colOut);
				if (colWidth <= 8)
				{
					row.setUintField(getUintNullValue(colDataType, colWidth), colOut);
				}
				else
				{
					row.setStringField(getStringNullValue(), colOut);
				}
				break;
			}
			case execplan::CalpontSystemCatalog::DOUBLE:
			case execplan::CalpontSystemCatalog::UDOUBLE:
			{
				row.setDoubleField(getDoubleNullValue(), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::FLOAT:
			case execplan::CalpontSystemCatalog::UFLOAT:
			{
				row.setFloatField(getFloatNullValue(), colOut);
				break;
			}
			case execplan::CalpontSystemCatalog::DATE:
			case execplan::CalpontSystemCatalog::DATETIME:
			{
				row.setUintField(getUintNullValue(colDataType), colOut);
				break;
			}
			default:
			{
				break;
			}
		}
	}
}


//------------------------------------------------------------------------------
// Update the min/max/sum fields if input is not null.
// rowIn(in)    - Row to be included in aggregation.
// colIn(in)    - column in the input row group
// colOut(in)   - column in the output row group
// funcType(in) - aggregation function type
// Note: NULL value check must be done on UM & PM
//       UM may receive NULL values, too.
//------------------------------------------------------------------------------
void RowAggregation::doMinMaxSum(const Row& rowIn, int64_t colIn, int64_t colOut, int funcType)
{
	int colDataType = (fRowGroupIn.getColTypes())[colIn];

	if (isNull(&fRowGroupIn, rowIn, colIn) == true)
		return;

	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			int64_t valIn = rowIn.getIntField(colIn);
			int64_t valOut = fRow.getIntField(colOut);
			if (funcType == ROWAGG_SUM || funcType == ROWAGG_DISTINCT_SUM)
				updateIntSum(valIn, valOut, colOut);
			else
				updateIntMinMax(valIn, valOut, colOut, funcType);
			break;
		}
		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
		{
			uint64_t valIn = rowIn.getUintField(colIn);
			uint64_t valOut = fRow.getUintField(colOut);
			if (funcType == ROWAGG_SUM || funcType == ROWAGG_DISTINCT_SUM)
				updateUintSum(valIn, valOut, colOut);
			else
				updateUintMinMax(valIn, valOut, colOut, funcType);
			break;
		}
		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			if (funcType == ROWAGG_SUM)
			{
				std::ostringstream errmsg;
				errmsg << "RowAggregation: sum(CHAR[]) is not supported.";
				cerr << errmsg.str() << endl;
				throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
			}

			int colWidth = fRowGroupIn.getColumnWidth(colIn);
			if (colWidth <= 8)
			{
				uint64_t valIn = rowIn.getUintField(colIn);
				uint64_t valOut = fRow.getUintField(colOut);
				updateCharMinMax(valIn, valOut, colOut, funcType);
			}
			else
			{
				string valIn = rowIn.getStringField(colIn);
				string valOut = fRow.getStringField(colOut);
				updateStringMinMax(valIn, valOut, colOut, funcType);
			}
			break;
		}
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double valIn = rowIn.getDoubleField(colIn);
			double valOut = fRow.getDoubleField(colOut);
			if (funcType == ROWAGG_SUM)
				updateDoubleSum(valIn, valOut, colOut);
			else
				updateDoubleMinMax(valIn, valOut, colOut, funcType);
			break;
		}
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			float valIn = rowIn.getFloatField(colIn);
			float valOut = fRow.getFloatField(colOut);
			if (funcType == ROWAGG_SUM)
				updateFloatSum(valIn, valOut, colOut);
			else
				updateFloatMinMax(valIn, valOut, colOut, funcType);
			break;
		}
		case execplan::CalpontSystemCatalog::DATE:
		case execplan::CalpontSystemCatalog::DATETIME:
		{
			if (funcType == ROWAGG_SUM)
			{
				std::ostringstream errmsg;
				errmsg << "RowAggregation: sum(date|date time) is not supported.";
				cerr << errmsg.str() << endl;
				throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
			}

			uint64_t valIn = rowIn.getUintField(colIn);
			uint64_t valOut = fRow.getUintField(colOut);
			updateUintMinMax(valIn, valOut, colOut, funcType);
			break;
		}
		default:
		{
			break;
		}
	}
}


//------------------------------------------------------------------------------
// Update the and/or/xor fields if input is not null.
// rowIn(in)    - Row to be included in aggregation.
// colIn(in)    - column in the input row group
// colOut(in)   - column in the output row group
// funcType(in) - aggregation function type
// Note: NULL value check must be done on UM & PM
//       UM may receive NULL values, too.
//------------------------------------------------------------------------------
void RowAggregation::doBitOp(const Row& rowIn, int64_t colIn, int64_t colOut, int funcType)
{
	int colDataType = (fRowGroupIn.getColTypes())[colIn];

	if (isNull(&fRowGroupIn, rowIn, colIn) == true)
		return;

	int64_t valIn = 0;
	uint64_t uvalIn = 0;
	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			valIn = rowIn.getIntField(colIn);
			if ((fRowGroupIn.getScale())[colIn] != 0)
			{
				valIn = rowIn.getIntField(colIn);
				valIn /= IDB_pow[fRowGroupIn.getScale()[colIn]-1];
				if (valIn > 0)
					valIn += 5;
				else if (valIn < 0)
					valIn -= 5;
				valIn /= 10;
			}
			break;
		}

		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
		{
			uvalIn = rowIn.getUintField(colIn);
			uint64_t uvalOut = fRow.getUintField(colOut);
			if (funcType == ROWAGG_BIT_AND)
				fRow.setUintField(uvalIn & uvalOut, colOut);
			else if (funcType == ROWAGG_BIT_OR)
				fRow.setUintField(uvalIn | uvalOut, colOut);
			else
				fRow.setUintField(uvalIn ^ uvalOut, colOut);
			return;
			break;
		}

		case execplan::CalpontSystemCatalog::CHAR:
		case execplan::CalpontSystemCatalog::VARCHAR:
		{
			string str = rowIn.getStringField(colIn);
			valIn = strtoll(str.c_str(), NULL, 10);
			break;
		}

		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			double dbl = 0.0;
			if (colDataType == execplan::CalpontSystemCatalog::DOUBLE ||
				colDataType == execplan::CalpontSystemCatalog::UDOUBLE)
				dbl = rowIn.getDoubleField(colIn);
			else
				dbl = rowIn.getFloatField(colIn);

			int64_t maxint = 0x7FFFFFFFFFFFFFFFLL;
			int64_t minint = 0x8000000000000000LL;
			if (dbl > maxint)
			{
				valIn = maxint;
			}
			else if (dbl < minint)
			{
				valIn = minint;
			}
			else
			{
				dbl += (dbl >= 0) ? 0.5 : -0.5;
				valIn = (int64_t) dbl;
			}

			break;
		}

		case execplan::CalpontSystemCatalog::DATE:
		{
			uint64_t dt = rowIn.getUintField(colIn);
			dt = dt & 0xFFFFFFC0;  // no need to set spare bits to 3E, will shift out
			valIn = ((dt >> 16) * 10000) + (((dt >> 12) & 0xF) * 100) + ((dt >> 6) & 077);
			break;
		}

		case execplan::CalpontSystemCatalog::DATETIME:
		{
			uint64_t dtm = rowIn.getUintField(colIn);
			valIn = ((dtm >> 48) * 10000000000LL) + (((dtm >> 44) & 0xF) * 100000000) +
					(((dtm >> 38) & 077) * 1000000) + (((dtm >> 32) & 077) * 10000) +
					(((dtm >> 26) & 077) * 100) + ((dtm >> 20) & 077);
			break;
		}

		default:
		{
			break;
		}
	}

	int64_t valOut = fRow.getIntField(colOut);
	if (funcType == ROWAGG_BIT_AND)
		fRow.setIntField(valIn & valOut, colOut);
	else if (funcType == ROWAGG_BIT_OR)
		fRow.setIntField(valIn | valOut, colOut);
	else
		fRow.setIntField(valIn ^ valOut, colOut);
}


//------------------------------------------------------------------------------
// Marks the end of input into aggregation when aggregating multiple RowGroups.
//------------------------------------------------------------------------------
void RowAggregation::endOfInput()
{
}


//------------------------------------------------------------------------------
// Serialize this RowAggregation object into the specified ByteStream.
// Primary information to be serialized is the RowAggGroupByCol and
// RowAggFunctionCol vectors.
// bs(out) - ByteStream to be used in serialization.
//------------------------------------------------------------------------------
void RowAggregation::serialize(messageqcpp::ByteStream& bs) const
{
	// groupby
	uint64_t groupbyCount = fGroupByCols.size();
	bs << groupbyCount;

	for (uint64_t i = 0; i < groupbyCount; i++)
		bs << *(fGroupByCols[i].get());

	// aggregate function
	uint64_t functionCount = fFunctionCols.size();
	bs << functionCount;

	for (uint64_t i = 0; i < functionCount; i++)
		bs << *(fFunctionCols[i].get());
}


//------------------------------------------------------------------------------
// Unserialze the specified ByteStream into this RowAggregation object.
// Primary information to be deserialized is the RowAggGroupByCol and
// RowAggFunctionCol vectors.
// bs(in) - ByteStream to be deserialized
//------------------------------------------------------------------------------
void RowAggregation::deserialize(messageqcpp::ByteStream& bs)
{
	// groupby
	uint64_t groupbyCount = 0;
	bs >> groupbyCount;

	for (uint64_t i = 0; i < groupbyCount; i++)
	{
		SP_ROWAGG_GRPBY_t grpby(new RowAggGroupByCol(0, 0));
		bs >> *(grpby.get());
		fGroupByCols.push_back(grpby);
	}

	// aggregate function
	uint64_t functionCount = 0;
	bs >> functionCount;

	for (uint64_t i = 0; i < functionCount; i++)
	{
		SP_ROWAGG_FUNC_t funct(
			new RowAggFunctionCol(ROWAGG_FUNCT_UNDEFINE, ROWAGG_FUNCT_UNDEFINE, 0, 0));
		bs >> *(funct.get());
		fFunctionCols.push_back(funct);
	}
}


//------------------------------------------------------------------------------
// Update the aggregation totals in the internal hashmap for the specified row.
// NULL values are recognized and ignored for all agg functions except for
// COUNT(*), which counts all rows regardless of value.
// rowIn(in) - Row to be included in aggregation.
//------------------------------------------------------------------------------
void RowAggregation::updateEntry(const Row& rowIn)
{
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		int64_t colIn  = fFunctionCols[i]->fInputColumnIndex;
		int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;

		switch (fFunctionCols[i]->fAggFunction)
		{
			case ROWAGG_COUNT_COL_NAME:
				// if NOT null, let execution fall through.
				if (isNull(&fRowGroupIn, rowIn, colIn) == true) break;
			case ROWAGG_COUNT_ASTERISK:
				fRow.setIntField<8>(fRow.getIntField<8>(colOut) + 1, colOut);
				break;

			case ROWAGG_MIN:
			case ROWAGG_MAX:
			case ROWAGG_SUM:
				doMinMaxSum(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;

			case ROWAGG_AVG:
				// count(column) for average is inserted after the sum,
				// colOut+1 is the position of the count column.
				doAvg(rowIn, colIn, colOut, colOut + 1);
				break;

			case ROWAGG_STATS:
				doStatistics(rowIn, colIn, colOut, colOut + 1);
				break;

			case ROWAGG_BIT_AND:
			case ROWAGG_BIT_OR:
			case ROWAGG_BIT_XOR:
			{
				doBitOp(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;
			}

			case ROWAGG_COUNT_NO_OP:
			case ROWAGG_DUP_FUNCT:
			case ROWAGG_DUP_AVG:
			case ROWAGG_DUP_STATS:
			case ROWAGG_CONSTANT:
			case ROWAGG_GROUP_CONCAT:
				break;

			default:
			{
				std::ostringstream errmsg;
				errmsg << "RowAggregation: function (id = " <<
					(uint64_t) fFunctionCols[i]->fAggFunction << ") is not supported.";
				cerr << errmsg.str() << endl;
				throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
				break;
			}
		}
	}
}


//------------------------------------------------------------------------------
// Update the sum and count fields for average if input is not null.
// rowIn(in)  - Row to be included in aggregation.
// colIn(in)  - column in the input row group
// colOut(in) - column in the output row group stores the sum
// colAux(in) - column in the output row group stores the count
//------------------------------------------------------------------------------
void RowAggregation::doAvg(const Row& rowIn, int64_t colIn, int64_t colOut, int64_t colAux)
{
	int colDataType = (fRowGroupIn.getColTypes())[colIn];

	if (isNull(&fRowGroupIn, rowIn, colIn) == true)
		return;

	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			int64_t valIn = rowIn.getIntField(colIn);
			if (fRow.getIntField(colAux) == 0)
			{
				fRow.setIntField(valIn, colOut);
				fRow.setIntField(1, colAux);
			}
			else
			{
				int64_t valOut = fRow.getIntField(colOut);
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
				if (((valOut >= 0) && ((numeric_limits<int64_t>::max() - valOut) >= valIn)) ||
					((valOut <  0) && ((numeric_limits<int64_t>::min() - valOut) <= valIn)))
					fRow.setIntField(valIn + valOut, colOut);
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
				if (fRow.getColTypes()[colOut] != execplan::CalpontSystemCatalog::DOUBLE)
				{
					if (((valOut >= 0) && ((numeric_limits<int64_t>::max() - valOut) >= valIn)) ||
						((valOut <  0) && ((numeric_limits<int64_t>::min() - valOut) <= valIn)))
					{
						fRow.setIntField(valIn + valOut, colOut);
						fRow.setIntField(fRow.getIntField(colAux) + 1, colAux);
					}
					else
					{
						execplan::CalpontSystemCatalog::ColDataType* cdtp = fRow.getColTypes();
						cdtp += colOut;
						*cdtp = execplan::CalpontSystemCatalog::DOUBLE;
						fRow.setDoubleField((double)valIn + (double)valOut, colOut);
						fRow.setIntField(fRow.getIntField(colAux) + 1, colAux);
					}
				}
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
				else
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
				{
					ostringstream oss;
					oss << overflowMsg << ": " << valOut << "+" << valIn;
					if (valOut > 0)
						oss << " > " << numeric_limits<uint64_t>::max();
					else
						oss << " < " << numeric_limits<uint64_t>::min();
					throw logging::QueryDataExcept(oss.str(), logging::aggregateDataErr);
				}

				fRow.setIntField(fRow.getIntField(colAux) + 1, colAux);
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
				{
					double* dp = (double*)&valOut;
					fRow.setDoubleField((double)valIn + *dp, colOut);
					fRow.setIntField(fRow.getIntField(colAux) + 1, colAux);
				}
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
			}
			break;
		}
		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
		{
			uint64_t valIn = rowIn.getUintField(colIn);
			if (fRow.getUintField(colAux) == 0)
			{
				fRow.setUintField(valIn, colOut);
				fRow.setUintField(1, colAux);
			}
			else
			{
				uint64_t valOut = fRow.getUintField(colOut);
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
				if ((numeric_limits<uint64_t>::max() - valOut) >= valIn)
				{
					fRow.setUintField(valIn + valOut, colOut);
					fRow.setUintField(fRow.getUintField(colAux) + 1, colAux);
				}
				else
				{
					ostringstream oss;
					oss << overflowMsg << ": " << valOut << "+" << valIn << " > " << numeric_limits<uint64_t>::max();
					throw logging::QueryDataExcept(oss.str(), logging::aggregateDataErr);
				}
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
				if (fRow.getColTypes()[colOut] != execplan::CalpontSystemCatalog::DOUBLE)
				{
					if ((numeric_limits<uint64_t>::max() - valOut) >= valIn)
					{
						fRow.setUintField(valIn + valOut, colOut);
						fRow.setUintField(fRow.getUintField(colAux) + 1, colAux);
					}
					else
					{
						execplan::CalpontSystemCatalog::ColDataType* cdtp = fRow.getColTypes();
						cdtp += colOut;
						*cdtp = execplan::CalpontSystemCatalog::DOUBLE;
						fRow.setDoubleField((double)valIn + (double)valOut, colOut);
						fRow.setUintField(fRow.getUintField(colAux) + 1, colAux);
					}
				}
				else
				{
					double* dp = (double*)&valOut;
					fRow.setDoubleField((double)valIn + *dp, colOut);
					fRow.setUintField(fRow.getUintField(colAux) + 1, colAux);
				}
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
			}
			break;
		}
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double valIn = rowIn.getDoubleField(colIn);
			if (fRow.getIntField(colAux) == 0)
			{
				fRow.setDoubleField(valIn, colOut);
				fRow.setIntField(1, colAux);
			}
			else
			{
				double valOut = fRow.getDoubleField(colOut);
				fRow.setDoubleField(valIn + valOut, colOut);
				fRow.setIntField(fRow.getIntField(colAux) + 1, colAux);
			}
			break;
		}
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			float valIn = rowIn.getFloatField(colIn);
			if (fRow.getIntField(colAux) == 0)
			{
				fRow.setFloatField(valIn, colOut);
				fRow.setIntField(1, colAux);
			}
			else
			{
				float valOut = fRow.getFloatField(colOut);
				fRow.setFloatField(valIn + valOut, colOut);
				fRow.setIntField(fRow.getIntField(colAux) + 1, colAux);
			}
			break;
		}
		default:
		{
			std::ostringstream errmsg;
			errmsg << "RowAggregation: no average for data type: " << colDataType;
			cerr << errmsg.str() << endl;
			throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
			break;
		}
	}
}


//------------------------------------------------------------------------------
// Update the sum and count fields for average if input is not null.
// rowIn(in)  - Row to be included in aggregation.
// colIn(in)  - column in the input row group
// colOut(in) - column in the output row group stores the count
// colAux(in) - column in the output row group stores the sum(x)
// colAux + 1 - column in the output row group stores the sum(x**2)
//------------------------------------------------------------------------------
void RowAggregation::doStatistics(const Row& rowIn, int64_t colIn, int64_t colOut, int64_t colAux)
{
	int colDataType = (fRowGroupIn.getColTypes())[colIn];

	if (isNull(&fRowGroupIn, rowIn, colIn) == true)
		return;

	long double valIn = 0.0;
	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:   // handle scale later
		case execplan::CalpontSystemCatalog::UDECIMAL:  // handle scale later
			valIn = (long double) rowIn.getIntField(colIn);
			break;

		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
			valIn = (long double) rowIn.getUintField(colIn);
			break;

		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
			valIn = (long double) rowIn.getDoubleField(colIn);
			break;

		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
			valIn = (long double) rowIn.getFloatField(colIn);
			break;

		default:
			std::ostringstream errmsg;
			errmsg << "RowAggregation: no average for data type: " << colDataType;
			cerr << errmsg.str() << endl;
			throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
			break;
	}

	fRow.setDoubleField(fRow.getDoubleField(colOut) + 1.0, colOut);
	fRow.setLongDoubleField(fRow.getLongDoubleField(colAux) + valIn, colAux);
	fRow.setLongDoubleField(fRow.getLongDoubleField(colAux+1) + valIn*valIn, colAux+1);
}


//------------------------------------------------------------------------------
// Allocate a new data array for the output RowGroup
// return - true if successfully allocated
//------------------------------------------------------------------------------
bool RowAggregation::newRowGroup()
{
	// For now, n*n relation is not supported, no memory limit.
	// May apply a restriction when more resarch is done -- bug 1604
	boost::shared_ptr<RGData> data(new RGData(*fRowGroupOut, AGG_ROWGROUP_SIZE));

	if (data.get() != NULL)
	{
		fRowGroupOut->setData(data.get());
		fRowGroupOut->resetRowGroup(0);
		fSecondaryRowDataVec.push_back(data);
		fResultDataVec.push_back(data.get());
		fMaxTotalRowCount += AGG_ROWGROUP_SIZE;
	}

	return (data.get() != NULL);
}


//------------------------------------------------------------------------------
// Concatenate multiple RowGroup data into one byte stream.  This is for matching
// the message counts of request and response.
//
// This function should be used by PM when result set is large than one RowGroup.
//
//------------------------------------------------------------------------------
void RowAggregation::loadResult(messageqcpp::ByteStream& bs)
{
	uint32_t size = fResultDataVec.size();
	bs << size;
	for (uint32_t i = 0; i < size; i++)
	{
		fRowGroupOut->setData(fResultDataVec[i]);
		fRowGroupOut->serializeRGData(bs);
	}

	fResultDataVec.clear();
	fSecondaryRowDataVec.clear();
}


void RowAggregation::loadEmptySet(messageqcpp::ByteStream& bs)
{
	bs << (uint32_t) 1;
	fEmptyRowGroup.serializeRGData(bs);
}


//------------------------------------------------------------------------------
// Row Aggregation constructor used on UM
// For one-phase case, from projected RG to final aggregated RG
//------------------------------------------------------------------------------
RowAggregationUM::RowAggregationUM(const vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
                                   const vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
                                   joblist::ResourceManager *r, boost::shared_ptr<int64_t> sessionLimit) :
	RowAggregation(rowAggGroupByCols, rowAggFunctionCols), fHasAvg(false), fKeyOnHeap(false),
	fHasStatsFunc(false), fTotalMemUsage(0), fRm(r), fSessionMemLimit(sessionLimit),
	fLastMemUsage(0), fNextRGIndex(0)
{
	// Check if there are any avg functions.
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		if (fFunctionCols[i]->fAggFunction == ROWAGG_AVG ||
			fFunctionCols[i]->fAggFunction==ROWAGG_DISTINCT_AVG)
			fHasAvg = true;
		else if (fFunctionCols[i]->fAggFunction == ROWAGG_STATS)
			fHasStatsFunc = true;
	}

	// Check if all groupby column selected
	for (uint64_t i = 0; i < fGroupByCols.size(); i++)
	{
		if (fGroupByCols[i]->fInputColumnIndex != fGroupByCols[i]->fOutputColumnIndex)
		{
			fKeyOnHeap = true;
			break;
		}
	}
}


RowAggregationUM::RowAggregationUM(const RowAggregationUM& rhs) :
	RowAggregation(rhs),
	fHasAvg(rhs.fHasAvg),
	fKeyOnHeap(rhs.fKeyOnHeap),
	fHasStatsFunc(rhs.fHasStatsFunc),
	fExpression(rhs.fExpression),
	fTotalMemUsage(rhs.fTotalMemUsage),
	fRm(rhs.fRm),
	fConstantAggregate(rhs.fConstantAggregate),
	fGroupConcat(rhs.fGroupConcat),
	fSessionMemLimit(rhs.fSessionMemLimit),
	fLastMemUsage(rhs.fLastMemUsage),
	fNextRGIndex(0)
{

}


RowAggregationUM::~RowAggregationUM()
{
	// on UM, a groupby column may be not a projected column, key is separated from output
	// and is stored on heap, need to return the space to heap at the end.
	clearAggMap();

	// fAggMapPtr deleted by base destructor.

	fRm->returnMemory(fTotalMemUsage, fSessionMemLimit);
}


//------------------------------------------------------------------------------
// Marks the end of RowGroup input into aggregation.
//
// This function should be used by UM when aggregating multiple RowGroups.
//------------------------------------------------------------------------------
void RowAggregationUM::endOfInput()
{
}


//------------------------------------------------------------------------------
// Initilalize the Group Concat data
//------------------------------------------------------------------------------
void RowAggregationUM::initialize()
{
	if (fGroupConcat.size() > 0)
		fFunctionColGc = fFunctionCols;

	RowAggregation::initialize();

	if (fKeyOnHeap)
	{
		fKeyRG = fRowGroupIn.truncate(fGroupByCols.size());
		fKeyStore.reset(new KeyStorage(fKeyRG, &tmpRow));
		fExtEq.reset(new ExternalKeyEq(fKeyRG, fKeyStore.get(), fKeyRG.getColumnCount(), &tmpRow));
		fExtHash.reset(new ExternalKeyHasher(fKeyRG, fKeyStore.get(), fKeyRG.getColumnCount(), &tmpRow));
		fExtKeyMapAlloc.reset(new utils::STLPoolAllocator<pair<RowPosition, RowPosition> >());
		fExtKeyMap.reset(new ExtKeyMap_t(10, *fExtHash, *fExtEq, *fExtKeyMapAlloc));
	}
}


//------------------------------------------------------------------------------
// Aggregation finalization can be performed here.  For example, this is
// where fixing the duplicates and dividing the SUM by COUNT to get the AVG.
//
// This function should be used by UM when aggregating multiple RowGroups.
//------------------------------------------------------------------------------
void RowAggregationUM::finalize()
{
	// copy the duplicates functions, except AVG
	fixDuplicates(ROWAGG_DUP_FUNCT);

	// UM: it is time to divide SUM by COUNT for any AVG cols.
	if (fHasAvg)
	{
		calculateAvgColumns();

		// copy the duplicate AVGs, if any
		fixDuplicates(ROWAGG_DUP_AVG);
	}

	// UM: it is time to calculate statistics functions
	if (fHasStatsFunc)
	{
		// covers duplicats, too.
		calculateStatisticsFunctions();
	}

	if (fGroupConcat.size() > 0)
		setGroupConcatString();

	if (fConstantAggregate.size() > 0)
		fixConstantAggregate();

	if (fExpression.size() > 0)
		evaluateExpression();
}


//------------------------------------------------------------------------------
//  Add group_concat to the initialized working row
//------------------------------------------------------------------------------
void RowAggregationUM::attachGroupConcatAg()
{
	if (fGroupConcat.size() > 0)
	{
		uint8_t* data = fRow.getData();
		uint64_t i = 0, j = 0;
		for (; i < fFunctionColGc.size(); i++)
		{
			int64_t colOut = fFunctionColGc[i]->fOutputColumnIndex;
			if (fFunctionColGc[i]->fAggFunction == ROWAGG_GROUP_CONCAT)
			{
				// save the object's address in the result row
				SP_GroupConcatAg gcc(new joblist::GroupConcatAgUM(fGroupConcat[j++]));
				fGroupConcatAg.push_back(gcc);
				*((GroupConcatAg**)(data + fRow.getOffset(colOut))) = gcc.get();
			}
		}
	}
}


//------------------------------------------------------------------------------
// Update the aggregation totals in the internal hashmap for the specified row.
// NULL values are recognized and ignored for all agg functions except for count
// rowIn(in) - Row to be included in aggregation.
//------------------------------------------------------------------------------
void RowAggregationUM::updateEntry(const Row& rowIn)
{
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		int64_t colIn  = fFunctionCols[i]->fInputColumnIndex;
		int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;

		switch (fFunctionCols[i]->fAggFunction)
		{
			case ROWAGG_COUNT_COL_NAME:
				// if NOT null, let execution fall through.
				if (isNull(&fRowGroupIn, rowIn, colIn) == true) break;
			case ROWAGG_COUNT_ASTERISK:
				fRow.setIntField<8>(fRow.getIntField<8>(colOut) + 1, colOut);
				break;

			case ROWAGG_MIN:
			case ROWAGG_MAX:
			case ROWAGG_SUM:
				doMinMaxSum(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;

			case ROWAGG_AVG:
			{
				// The sum and count on UM may not be put next to each other:
				//   use colOut to store the sum;
				//   use colAux to store the count.
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				doAvg(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_STATS:
			{
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				doStatistics(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_BIT_AND:
			case ROWAGG_BIT_OR:
			case ROWAGG_BIT_XOR:
			{
				doBitOp(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;
			}

			case ROWAGG_GROUP_CONCAT:
			{
				doGroupConcat(rowIn, colIn, colOut);
				break;
			}

			case ROWAGG_COUNT_NO_OP:
			case ROWAGG_DUP_FUNCT:
			case ROWAGG_DUP_AVG:
			case ROWAGG_DUP_STATS:
			case ROWAGG_CONSTANT:
				break;

			default:
			{
				// need a exception to show the value
				std::ostringstream errmsg;
				errmsg << "RowAggregationUM: function (id = " <<
					(uint64_t) fFunctionCols[i]->fAggFunction << ") is not supported.";
				cerr << errmsg.str() << endl;
				throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
				break;
			}
		}
	}
}


//------------------------------------------------------------------------------
// Concat columns.
// rowIn(in) - Row that contains the columns to be concatenated.
//------------------------------------------------------------------------------
void RowAggregationUM::doGroupConcat(const Row& rowIn, int64_t, int64_t o)
{
	uint8_t* data = fRow.getData();
	joblist::GroupConcatAgUM* gccAg = *((joblist::GroupConcatAgUM**)(data + fRow.getOffset(o)));
	gccAg->processRow(rowIn);
}


//------------------------------------------------------------------------------
// After all PM rowgroups received, calculate the average value.
//------------------------------------------------------------------------------
void RowAggregationUM::calculateAvgColumns()
{
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		if (fFunctionCols[i]->fAggFunction == ROWAGG_AVG ||
			fFunctionCols[i]->fAggFunction==ROWAGG_DISTINCT_AVG)
		{
			int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;
			int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
			int colDataType = (fRowGroupOut->getColTypes())[colOut];

			int scale = fRowGroupOut->getScale()[colOut];
			int scale1 = scale >> 8;
			int scale2 = scale & 0x000000FF;
			long double factor = pow(10.0, scale2 - scale1);
			for (uint64_t j = 0; j < fRowGroupOut->getRowCount(); j++)
			{
				fRowGroupOut->getRow(j, &fRow);
				uint64_t cnt = fRow.getIntField(colAux);
				if (cnt == 0) // empty set, value is initialized to null.
					continue;

				long double sum = 0.0;
				long double avg = 0.0;
				switch (colDataType)
				{
					case execplan::CalpontSystemCatalog::DOUBLE:
					case execplan::CalpontSystemCatalog::UDOUBLE:
						sum = fRow.getDoubleField(colOut);
						avg = sum / cnt;
						fRow.setDoubleField(avg, colOut);
						break;
					case execplan::CalpontSystemCatalog::FLOAT:
					case execplan::CalpontSystemCatalog::UFLOAT:
						sum = fRow.getFloatField(colOut);
						avg = sum / cnt;
						fRow.setFloatField(avg, colOut);
						break;
					case execplan::CalpontSystemCatalog::TINYINT:
					case execplan::CalpontSystemCatalog::SMALLINT:
					case execplan::CalpontSystemCatalog::MEDINT:
					case execplan::CalpontSystemCatalog::INT:
					case execplan::CalpontSystemCatalog::BIGINT:
					case execplan::CalpontSystemCatalog::DECIMAL:
					case execplan::CalpontSystemCatalog::UDECIMAL:
						sum = static_cast<long double>(fRow.getIntField(colOut));
						avg = sum / cnt;
						avg *= factor;
						avg += (avg < 0) ? (-0.5) : (0.5);
						if (avg > (long double) numeric_limits<int64_t>::max() ||
							avg < (long double) numeric_limits<int64_t>::min())
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
						{
							ostringstream oss;
							oss << overflowMsg << ": " << avg << "(incl factor " << factor;
							if (avg > 0)
								oss << ") > " << numeric_limits<uint64_t>::max();
							else
								oss << ") < " << numeric_limits<uint64_t>::min();
							throw logging::QueryDataExcept(oss.str(), logging::aggregateDataErr);
						}
						fRow.setIntField((int64_t) avg, colOut);
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
						{
							sum = fRow.getDoubleField(colOut);
							avg = sum / cnt;
							avg += (avg < 0) ? (-0.5) : (0.5);
							fRow.getColTypes()[colOut] = execplan::CalpontSystemCatalog::DOUBLE;
							fRow.setDoubleField(avg, colOut);
						}
						else
							fRow.setIntField((int64_t)avg, colOut);
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
						break;
					case execplan::CalpontSystemCatalog::UTINYINT:
					case execplan::CalpontSystemCatalog::USMALLINT:
					case execplan::CalpontSystemCatalog::UMEDINT:
					case execplan::CalpontSystemCatalog::UINT:
					case execplan::CalpontSystemCatalog::UBIGINT:
						sum = static_cast<long double>(fRow.getUintField(colOut));
						avg = sum / cnt;
						avg *= factor;
						avg += (avg < 0) ? (-0.5) : (0.5);
						if (avg > (long double) numeric_limits<uint64_t>::max())
#ifndef PROMOTE_AGGR_OVRFLW_TO_DBL
						{
							ostringstream oss;
							oss << overflowMsg << ": " << avg << "(incl factor " << factor << ") > " << numeric_limits<uint64_t>::max();
							throw logging::QueryDataExcept(oss.str(), logging::aggregateDataErr);
						}
						fRow.setUintField((uint64_t) avg, colOut);
#else /* PROMOTE_AGGR_OVRFLW_TO_DBL */
						{
							sum = fRow.getDoubleField(colOut);
							avg = sum / cnt;
							avg += 0.5;
							fRow.getColTypes()[colOut] = execplan::CalpontSystemCatalog::DOUBLE;
							fRow.setDoubleField(avg, colOut);
						}
						else
							fRow.setUintField((uint64_t)avg, colOut);
#endif /* PROMOTE_AGGR_OVRFLW_TO_DBL */
						break;
				}
			}
		}
	}
}


//------------------------------------------------------------------------------
// After all PM rowgroups received, calculate the statistics.
//------------------------------------------------------------------------------
void RowAggregationUM::calculateStatisticsFunctions()
{
	// ROWAGG_DUP_STATS may be not strictly duplicates, covers for statistics functions.
	// They are calculated based on the same set of data: sum(x), sum(x**2) and count.
	// array of <aux index, count> for duplicates
	boost::scoped_array<pair<double, uint64_t> >
		auxCount(new pair<double, uint64_t>[fRow.getColumnCount()]);

	fRowGroupOut->getRow(0, &fRow);
	for (uint64_t j = 0; j < fRowGroupOut->getRowCount(); j++, fRow.nextRow())
	{
		for (uint64_t i = 0; i < fFunctionCols.size(); i++)
		{
			if (fFunctionCols[i]->fAggFunction == ROWAGG_STATS ||
				fFunctionCols[i]->fAggFunction == ROWAGG_DUP_STATS)
			{
				int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;

				double cnt = fRow.getDoubleField(colOut);
				if (fFunctionCols[i]->fAggFunction == ROWAGG_STATS)
				{
					auxCount[colOut].first  = cnt;
					auxCount[colOut].second = colAux;
				}
				else // ROWAGG_DUP_STATS
				{
					cnt = auxCount[colAux].first;
					colAux = auxCount[colAux].second;
				}

				if (cnt == 0.0) // empty set, set null.
				{
					fRow.setUintField(joblist::DOUBLENULL, colOut);
				}
				else if (cnt == 1.0)
				{
					if (fFunctionCols[i]->fStatsFunction == ROWAGG_STDDEV_SAMP ||
						fFunctionCols[i]->fStatsFunction == ROWAGG_VAR_SAMP)
						fRow.setUintField(joblist::DOUBLENULL, colOut);
					else
						fRow.setDoubleField(0.0, colOut);
				}
				else // count > 1
				{
					long double sum1 = fRow.getLongDoubleField(colAux);
					long double sum2 = fRow.getLongDoubleField(colAux+1);

					int scale = fRow.getScale(colOut);
					long double factor = pow(10.0, scale);
					if (scale != 0) // adjust the scale if necessary
					{
						sum1 /= factor;
						sum2 /= factor*factor;
					}

					long double stat = sum1 * sum1 / cnt;
					stat = sum2 - stat;

					if (fFunctionCols[i]->fStatsFunction == ROWAGG_STDDEV_POP)
						stat = sqrt(stat / cnt);
					else if (fFunctionCols[i]->fStatsFunction == ROWAGG_STDDEV_SAMP)
						stat = sqrt(stat / (cnt-1));
					else if (fFunctionCols[i]->fStatsFunction == ROWAGG_VAR_POP)
						stat = stat / cnt;
					else if (fFunctionCols[i]->fStatsFunction == ROWAGG_VAR_SAMP)
						stat = stat / (cnt-1);

					fRow.setDoubleField(stat, colOut);
				}
			}
		}
	}
}


//------------------------------------------------------------------------------
// Fix the duplicate function columns -- same function same column id repeated
//------------------------------------------------------------------------------
void RowAggregationUM::fixDuplicates(RowAggFunctionType funct)
{
	// find out if any column matches funct
	vector<SP_ROWAGG_FUNC_t> dup;
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		if (fFunctionCols[i]->fAggFunction == funct)
			dup.push_back(fFunctionCols[i]);
	}

	if (0 == dup.size())
		return;

	// fix each row in the row group
	fRowGroupOut->getRow(0, &fRow);
	for (uint64_t i = 0; i < fRowGroupOut->getRowCount(); i++, fRow.nextRow())
	{
		for (uint64_t j = 0; j < dup.size(); j++)
			fRow.copyField(dup[j]->fOutputColumnIndex,dup[j]->fAuxColumnIndex);
	}
}


//------------------------------------------------------------------------------
// Evaluate the functions and expressions
//------------------------------------------------------------------------------
void RowAggregationUM::evaluateExpression()
{
	funcexp::FuncExp* fe = funcexp::FuncExp::instance();
	fRowGroupOut->getRow(0, &fRow);
	for (uint64_t i = 0; i < fRowGroupOut->getRowCount(); i++, fRow.nextRow())
	{
		fe->evaluate(fRow, fExpression);
	}
}


//------------------------------------------------------------------------------
// Calculate the aggregate(constant) columns
//------------------------------------------------------------------------------
void RowAggregationUM::fixConstantAggregate()
{
	// find the field has the count(*).
	int64_t cntIdx = 0;
	for (uint64_t k = 0; k < fFunctionCols.size(); k++)
	{
		if (fFunctionCols[k]->fAggFunction == ROWAGG_CONSTANT)
		{
			cntIdx = fFunctionCols[k]->fAuxColumnIndex;
			break;
		}
	}

	fRowGroupOut->getRow(0, &fRow);
	for (uint64_t i = 0; i < fRowGroupOut->getRowCount(); i++, fRow.nextRow())
	{
		int64_t rowCnt = fRow.getIntField(cntIdx);
		vector<ConstantAggData>::iterator j = fConstantAggregate.begin();
		for (uint64_t k = 0; k < fFunctionCols.size(); k++)
		{
			if (fFunctionCols[k]->fAggFunction == ROWAGG_CONSTANT)
			{
				if (j->fIsNull || rowCnt == 0)
					doNullConstantAggregate(*j, k);
				else
					doNotNullConstantAggregate(*j, k);

				j++;
			}
		}
	}
}


//------------------------------------------------------------------------------
// Calculate the aggregate(null) columns
//------------------------------------------------------------------------------
void RowAggregationUM::doNullConstantAggregate(const ConstantAggData& aggData, uint64_t i)
{
	int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;
	int colDataType = (fRowGroupOut->getColTypes())[colOut];

	switch(aggData.fOp)
	{
		case ROWAGG_MIN:
		case ROWAGG_MAX:
		case ROWAGG_AVG:
		case ROWAGG_SUM:
		case ROWAGG_DISTINCT_AVG:
		case ROWAGG_DISTINCT_SUM:
		case ROWAGG_STATS:
		{
			switch (colDataType)
			{
				case execplan::CalpontSystemCatalog::TINYINT:
				case execplan::CalpontSystemCatalog::SMALLINT:
				case execplan::CalpontSystemCatalog::MEDINT:
				case execplan::CalpontSystemCatalog::INT:
				case execplan::CalpontSystemCatalog::BIGINT:
				case execplan::CalpontSystemCatalog::DECIMAL:
				case execplan::CalpontSystemCatalog::UDECIMAL:
				{
					fRow.setIntField(getIntNullValue(colDataType), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::UTINYINT:
				case execplan::CalpontSystemCatalog::USMALLINT:
				case execplan::CalpontSystemCatalog::UMEDINT:
				case execplan::CalpontSystemCatalog::UINT:
				case execplan::CalpontSystemCatalog::UBIGINT:
				{
					fRow.setUintField(getUintNullValue(colDataType), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DOUBLE:
				case execplan::CalpontSystemCatalog::UDOUBLE:
				{
					fRow.setDoubleField(getDoubleNullValue(), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::FLOAT:
				case execplan::CalpontSystemCatalog::UFLOAT:
				{
					fRow.setFloatField(getFloatNullValue(), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DATE:
				case execplan::CalpontSystemCatalog::DATETIME:
				{
					fRow.setUintField(getUintNullValue(colDataType), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::CHAR:
				case execplan::CalpontSystemCatalog::VARCHAR:
				default:
				{
					fRow.setStringField("", colOut);
				}
				break;
			}
		}
		break;

		case ROWAGG_COUNT_COL_NAME:
		case ROWAGG_COUNT_DISTINCT_COL_NAME:
		{
			fRow.setIntField(0, colOut);
		}
		break;

		case ROWAGG_BIT_AND:
		{
			fRow.setUintField(0xFFFFFFFFFFFFFFFFULL, colOut);
		}
		break;

		case ROWAGG_BIT_OR:
		case ROWAGG_BIT_XOR:
		{
			fRow.setUintField(0, colOut);
		}
		break;

		default:
		{
			fRow.setStringField("", colOut);
		}
		break;
	}
}


//------------------------------------------------------------------------------
// Calculate the aggregate(const) columns
//------------------------------------------------------------------------------
void RowAggregationUM::doNotNullConstantAggregate(const ConstantAggData& aggData, uint64_t i)
{
	int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;
	int colDataType = (fRowGroupOut->getColTypes())[colOut];
	int64_t rowCnt = fRow.getIntField(fFunctionCols[i]->fAuxColumnIndex);

	switch(aggData.fOp)
	{
		case ROWAGG_MIN:
		case ROWAGG_MAX:
		case ROWAGG_AVG:
		case ROWAGG_DISTINCT_AVG:
		case ROWAGG_DISTINCT_SUM:
		{
			switch (colDataType)
			{
				// AVG should not be int result type.
				case execplan::CalpontSystemCatalog::TINYINT:
				case execplan::CalpontSystemCatalog::SMALLINT:
				case execplan::CalpontSystemCatalog::MEDINT:
				case execplan::CalpontSystemCatalog::INT:
				case execplan::CalpontSystemCatalog::BIGINT:
				{
					fRow.setIntField(strtol(aggData.fConstValue.c_str(), 0, 10), colOut);
				}
				break;

				// AVG should not be uint32_t result type.
				case execplan::CalpontSystemCatalog::UTINYINT:
				case execplan::CalpontSystemCatalog::USMALLINT:
				case execplan::CalpontSystemCatalog::UMEDINT:
				case execplan::CalpontSystemCatalog::UINT:
				case execplan::CalpontSystemCatalog::UBIGINT:
				{
					fRow.setUintField(strtoul(aggData.fConstValue.c_str(), 0, 10), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DECIMAL:
				case execplan::CalpontSystemCatalog::UDECIMAL:
				{
					double dbl = strtod(aggData.fConstValue.c_str(), 0);
					double scale = pow(10.0, (double) fRowGroupOut->getScale()[i]);
					fRow.setIntField((int64_t)(scale*dbl), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DOUBLE:
				case execplan::CalpontSystemCatalog::UDOUBLE:
				{
					fRow.setDoubleField(strtod(aggData.fConstValue.c_str(), 0), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::FLOAT:
				case execplan::CalpontSystemCatalog::UFLOAT:
				{
#ifdef _MSC_VER
					fRow.setFloatField(strtod(aggData.fConstValue.c_str(), 0), colOut);
#else
					fRow.setFloatField(strtof(aggData.fConstValue.c_str(), 0), colOut);
#endif
				}
				break;

				case execplan::CalpontSystemCatalog::DATE:
				{
					fRow.setUintField(DataConvert::stringToDate(aggData.fConstValue), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DATETIME:
				{
					fRow.setUintField(DataConvert::stringToDatetime(aggData.fConstValue), colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::CHAR:
				case execplan::CalpontSystemCatalog::VARCHAR:
				default:
				{
					fRow.setStringField(aggData.fConstValue, colOut);
				}
				break;
			}
		}
		break;

		case ROWAGG_SUM:
		{
			switch (colDataType)
			{
				case execplan::CalpontSystemCatalog::TINYINT:
				case execplan::CalpontSystemCatalog::SMALLINT:
				case execplan::CalpontSystemCatalog::MEDINT:
				case execplan::CalpontSystemCatalog::INT:
				case execplan::CalpontSystemCatalog::BIGINT:
				{
					int64_t constVal = strtol(aggData.fConstValue.c_str(), 0, 10);
					if (constVal != 0)
					{
						int64_t tmp = numeric_limits<int64_t>::max() / constVal;
						if (constVal < 0)
							tmp = numeric_limits<int64_t>::min() / constVal;

						if (rowCnt > tmp)
							throw logging::QueryDataExcept(overflowMsg, logging::aggregateDataErr);
					}

					fRow.setIntField(constVal*rowCnt, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::UTINYINT:
				case execplan::CalpontSystemCatalog::USMALLINT:
				case execplan::CalpontSystemCatalog::UMEDINT:
				case execplan::CalpontSystemCatalog::UINT:
				case execplan::CalpontSystemCatalog::UBIGINT:
				{
					uint64_t constVal = strtoul(aggData.fConstValue.c_str(), 0, 10);
					fRow.setUintField(constVal*rowCnt, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DECIMAL:
				case execplan::CalpontSystemCatalog::UDECIMAL:
				{
					double dbl = strtod(aggData.fConstValue.c_str(), 0);
					dbl *= pow(10.0, (double) fRowGroupOut->getScale()[i]);
					dbl *= rowCnt;
					if ((dbl > 0 && dbl > (double) numeric_limits<int64_t>::max()) ||
						(dbl < 0 && dbl < (double) numeric_limits<int64_t>::min()))
						throw logging::QueryDataExcept(overflowMsg, logging::aggregateDataErr);

					fRow.setIntField((int64_t) dbl, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DOUBLE:
				case execplan::CalpontSystemCatalog::UDOUBLE:
				{
					double dbl = strtod(aggData.fConstValue.c_str(), 0) * rowCnt;
					fRow.setDoubleField(dbl, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::FLOAT:
				case execplan::CalpontSystemCatalog::UFLOAT:
				{
					double flt;
#ifdef _MSC_VER
					flt = strtod(aggData.fConstValue.c_str(), 0) * rowCnt;
#else
					flt = strtof(aggData.fConstValue.c_str(), 0) * rowCnt;
#endif
					fRow.setFloatField(flt, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DATE:
				case execplan::CalpontSystemCatalog::DATETIME:
				case execplan::CalpontSystemCatalog::CHAR:
				case execplan::CalpontSystemCatalog::VARCHAR:
				default:
				{
					// will not be here, checked in tupleaggregatestep.cpp.
					fRow.setStringField("", colOut);
				}
				break;
			}
		}
		break;

		case ROWAGG_STATS:
		{
			switch (colDataType)
			{
				case execplan::CalpontSystemCatalog::TINYINT:
				case execplan::CalpontSystemCatalog::SMALLINT:
				case execplan::CalpontSystemCatalog::MEDINT:
				case execplan::CalpontSystemCatalog::INT:
				case execplan::CalpontSystemCatalog::BIGINT:
				case execplan::CalpontSystemCatalog::DECIMAL:
				case execplan::CalpontSystemCatalog::UDECIMAL:
				{
					fRow.setIntField(0, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::UTINYINT:
				case execplan::CalpontSystemCatalog::USMALLINT:
				case execplan::CalpontSystemCatalog::UMEDINT:
				case execplan::CalpontSystemCatalog::UINT:
				case execplan::CalpontSystemCatalog::UBIGINT:
				{
					fRow.setUintField(0, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DOUBLE:
				case execplan::CalpontSystemCatalog::UDOUBLE:
				{
					fRow.setDoubleField(0.0, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::FLOAT:
				case execplan::CalpontSystemCatalog::UFLOAT:
				{
					fRow.setFloatField(0.0, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DATE:
				{
					fRow.setUintField(0, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::DATETIME:
				{
					fRow.setUintField(0, colOut);
				}
				break;

				case execplan::CalpontSystemCatalog::CHAR:
				case execplan::CalpontSystemCatalog::VARCHAR:
				default:
				{
					fRow.setStringField(0, colOut);
				}
				break;
			}
		}
		break;

		case ROWAGG_COUNT_COL_NAME:
		{
			fRow.setIntField(rowCnt, colOut);
		}
		break;

		case ROWAGG_COUNT_DISTINCT_COL_NAME:
		{
			fRow.setIntField(1, colOut);
		}
		break;

		case ROWAGG_BIT_AND:
		case ROWAGG_BIT_OR:
		{
			double dbl = strtod(aggData.fConstValue.c_str(), 0);
			dbl += (dbl > 0) ? 0.5 : -0.5;
			int64_t intVal = (int64_t) dbl;
			fRow.setUintField(intVal, colOut);
		}
		break;

		case ROWAGG_BIT_XOR:
		{
			fRow.setUintField(0, colOut);
		}
		break;

		default:
		{
			fRow.setStringField(aggData.fConstValue, colOut);
		}
		break;
	}
}


//------------------------------------------------------------------------------
// Allocate a new data array for the output RowGroup
// return - true if successfully allocated
//------------------------------------------------------------------------------
void RowAggregationUM::setGroupConcatString()
{
	fRowGroupOut->getRow(0, &fRow);
	for (uint64_t i = 0; i < fRowGroupOut->getRowCount(); i++, fRow.nextRow())
	{
		for (uint64_t j = 0; j < fFunctionCols.size(); j++)
		{
			uint8_t* data = fRow.getData();

			if (fFunctionCols[j]->fAggFunction == ROWAGG_GROUP_CONCAT)
			{
				uint8_t* buff = data + fRow.getOffset(fFunctionCols[j]->fOutputColumnIndex);
				uint8_t* gcString;
				joblist::GroupConcatAgUM* gccAg = *((joblist::GroupConcatAgUM**)buff);
				gcString = gccAg->getResult();
				fRow.setStringField((char *) gcString, fFunctionCols[j]->fOutputColumnIndex);
				//gccAg->getResult(buff);
			}
		}
	}
}


//------------------------------------------------------------------------------
// Allocate a new data array for the output RowGroup
// return - true if successfully allocated
//------------------------------------------------------------------------------
bool RowAggregationUM::newRowGroup()
{
	uint64_t allocSize = 0;
	uint64_t memDiff = 0;
	bool     ret = false;

	allocSize = fRowGroupOut->getSizeWithStrings();
	if (fKeyOnHeap)
		memDiff = fKeyStore->getMemUsage() + fExtKeyMapAlloc->getMemUsage() - fLastMemUsage;
	else
		memDiff = fAlloc->getMemUsage() - fLastMemUsage;

	fLastMemUsage += memDiff;

	fTotalMemUsage += allocSize + memDiff;
	if (fRm->getMemory(allocSize + memDiff, fSessionMemLimit))
	{
		boost::shared_ptr<RGData> data(new RGData(*fRowGroupOut, AGG_ROWGROUP_SIZE));

		if (data.get() != NULL)
		{
			fMaxTotalRowCount += AGG_ROWGROUP_SIZE;
			fSecondaryRowDataVec.push_back(data);
			fRowGroupOut->setData(data.get());
			fResultDataVec.push_back(data.get());
			fRowGroupOut->resetRowGroup(0);

			ret = true;
		}
	}

	return ret;
}

void RowAggregationUM::setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut)
{
	RowAggregation::setInputOutput(pRowGroupIn, pRowGroupOut);
	if (fKeyOnHeap)
	{
		fKeyRG = fRowGroupIn.truncate(fGroupByCols.size());
		fKeyStore.reset(new KeyStorage(fKeyRG, &tmpRow));
		fExtEq.reset(new ExternalKeyEq(fKeyRG, fKeyStore.get(), fKeyRG.getColumnCount(), &tmpRow));
		fExtHash.reset(new ExternalKeyHasher(fKeyRG, fKeyStore.get(), fKeyRG.getColumnCount(), &tmpRow));
		fExtKeyMapAlloc.reset(new utils::STLPoolAllocator<pair<RowPosition, RowPosition> >());
		fExtKeyMap.reset(new ExtKeyMap_t(10, *fExtHash, *fExtEq, *fExtKeyMapAlloc));
	}
}


//------------------------------------------------------------------------------
// Returns the next group of aggregated rows.
// We do not yet cache large aggregations (more than 1 RowGroup result set)
// to disk, which means, the hashmap is limited to the size of RowGroups in mem
// (since we use the memory from the output RowGroups for our internal hashmap).
//
// This function should be used by UM when aggregating multiple RowGroups.
//
// return     - false indicates all aggregated RowGroups have been returned,
//              else more aggregated RowGroups remain.
//------------------------------------------------------------------------------
bool RowAggregationUM::nextRowGroup()
{
	bool more = (fResultDataVec.size() > 0);
	if (more)
	{
		// load the top result set
		fRowGroupOut->setData(fResultDataVec.back());
		fResultDataVec.pop_back();
	}

	return more;
}


//------------------------------------------------------------------------------
// Row Aggregation constructor used on UM
// For 2nd phase of two-phase case, from partial RG to final aggregated RG
//------------------------------------------------------------------------------
RowAggregationUMP2::RowAggregationUMP2(const vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
										const vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
										joblist::ResourceManager *r,
										boost::shared_ptr<int64_t> sessionLimit) :
	RowAggregationUM(rowAggGroupByCols, rowAggFunctionCols, r, sessionLimit)
{
}


RowAggregationUMP2::RowAggregationUMP2(const RowAggregationUMP2& rhs) :
	RowAggregationUM(rhs)
{
}


RowAggregationUMP2::~RowAggregationUMP2()
{
}


//------------------------------------------------------------------------------
// Update the aggregation totals in the internal hashmap for the specified row.
// NULL values are recognized and ignored for all agg functions except for count
// rowIn(in) - Row to be included in aggregation.
//------------------------------------------------------------------------------
void RowAggregationUMP2::updateEntry(const Row& rowIn)
{
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		int64_t colIn  = fFunctionCols[i]->fInputColumnIndex;
		int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;

		switch (fFunctionCols[i]->fAggFunction)
		{
			case ROWAGG_COUNT_ASTERISK:
			case ROWAGG_COUNT_COL_NAME:
			{
				int64_t count = fRow.getIntField<8>(colOut) + rowIn.getIntField<8>(colIn);
				fRow.setIntField<8>(count, colOut);
				break;
			}

			case ROWAGG_MIN:
			case ROWAGG_MAX:
			case ROWAGG_SUM:
				doMinMaxSum(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;

			case ROWAGG_AVG:
			{
				// The sum and count on UM may not be put next to each other:
				//   use colOut to store the sum;
				//   use colAux to store the count.
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				doAvg(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_STATS:
			{
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				doStatistics(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_BIT_AND:
			case ROWAGG_BIT_OR:
			case ROWAGG_BIT_XOR:
			{
				doBitOp(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;
			}

			case ROWAGG_GROUP_CONCAT:
			{
				doGroupConcat(rowIn, colIn, colOut);
				break;
			}

			case ROWAGG_COUNT_NO_OP:
			case ROWAGG_DUP_FUNCT:
			case ROWAGG_DUP_AVG:
			case ROWAGG_DUP_STATS:
			case ROWAGG_CONSTANT:
				break;

			default:
			{
				std::ostringstream errmsg;
				errmsg << "RowAggregationUMP2: function (id = " <<
					(uint64_t) fFunctionCols[i]->fAggFunction << ") is not supported.";
				cerr << errmsg.str() << endl;
				throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
				break;
			}
		}
	}
}


//------------------------------------------------------------------------------
// Update the sum and count fields for average if input is not null.
// rowIn(in)  - Row to be included in aggregation.
// colIn(in)  - column in the input row group
// colOut(in) - column in the output row group stores the sum
// colAux(in) - column in the output row group stores the count
//------------------------------------------------------------------------------
void RowAggregationUMP2::doAvg(const Row& rowIn, int64_t colIn, int64_t colOut, int64_t colAux)
{
	int colDataType = (fRowGroupIn.getColTypes())[colIn];

	if (isNull(&fRowGroupIn, rowIn, colIn) == true)
		return;

	switch (colDataType)
	{
		case execplan::CalpontSystemCatalog::TINYINT:
		case execplan::CalpontSystemCatalog::SMALLINT:
		case execplan::CalpontSystemCatalog::MEDINT:
		case execplan::CalpontSystemCatalog::INT:
		case execplan::CalpontSystemCatalog::BIGINT:
		case execplan::CalpontSystemCatalog::DECIMAL:
		case execplan::CalpontSystemCatalog::UDECIMAL:
		{
			int64_t valIn = rowIn.getIntField(colIn);
			if (fRow.getIntField(colAux) == 0)
			{
				fRow.setIntField(valIn, colOut);
				fRow.setIntField(rowIn.getIntField(colIn+1), colAux);
			}
			else
			{
				int64_t valOut = fRow.getIntField(colOut);
				fRow.setIntField(valIn + valOut, colOut);
				fRow.setIntField(rowIn.getIntField(colIn+1)+fRow.getIntField(colAux), colAux);
			}
			break;
		}
		case execplan::CalpontSystemCatalog::UTINYINT:
		case execplan::CalpontSystemCatalog::USMALLINT:
		case execplan::CalpontSystemCatalog::UMEDINT:
		case execplan::CalpontSystemCatalog::UINT:
		case execplan::CalpontSystemCatalog::UBIGINT:
		{
			uint64_t valIn = rowIn.getUintField(colIn);
			if (fRow.getUintField(colAux) == 0)
			{
				fRow.setUintField(valIn, colOut);
				fRow.setUintField(rowIn.getUintField(colIn+1), colAux);
			}
			else
			{
				uint64_t valOut = fRow.getUintField(colOut);
				fRow.setUintField(valIn + valOut, colOut);
				fRow.setUintField(rowIn.getUintField(colIn+1)+fRow.getUintField(colAux), colAux);
			}
			break;
		}
		case execplan::CalpontSystemCatalog::DOUBLE:
		case execplan::CalpontSystemCatalog::UDOUBLE:
		{
			double valIn = rowIn.getDoubleField(colIn);
			if (fRow.getIntField(colAux) == 0)
			{
				fRow.setDoubleField(valIn, colOut);
				fRow.setIntField(rowIn.getIntField(colIn+1), colAux);
			}
			else
			{
				double valOut = fRow.getDoubleField(colOut);
				fRow.setDoubleField(valIn + valOut, colOut);
				fRow.setIntField(rowIn.getIntField(colIn+1)+fRow.getIntField(colAux), colAux);
			}
			break;
		}
		case execplan::CalpontSystemCatalog::FLOAT:
		case execplan::CalpontSystemCatalog::UFLOAT:
		{
			float valIn = rowIn.getFloatField(colIn);
			if (fRow.getIntField(colAux) == 0)
			{
				fRow.setFloatField(valIn, colOut);
				fRow.setIntField(rowIn.getIntField(colIn+1), colAux);
			}
			else
			{
				float valOut = fRow.getFloatField(colOut);
				fRow.setFloatField(valIn + valOut, colOut);
				fRow.setIntField(rowIn.getIntField(colIn+1)+fRow.getIntField(colAux), colAux);
			}
			break;
		}
		default:
		{
			std::ostringstream errmsg;
			errmsg << "RowAggregationUMP2: no average for data type: " << colDataType;
			cerr << errmsg.str() << endl;
			throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
			break;
		}
	}
}


//------------------------------------------------------------------------------
// Update the sum and count fields for stattistics if input is not null.
// rowIn(in)  - Row to be included in aggregation.
// colIn(in)  - column in the input row group stores the count/logical block
// colIn + 1  - column in the input row group stores the sum(x)/logical block
// colIn + 2  - column in the input row group stores the sum(x**2)/logical block
// colOut(in) - column in the output row group stores the count
// colAux(in) - column in the output row group stores the sum(x)
// colAux + 1 - column in the output row group stores the sum(x**2)
//------------------------------------------------------------------------------
void RowAggregationUMP2::doStatistics(
		const Row& rowIn, int64_t colIn, int64_t colOut, int64_t colAux)
{
	fRow.setDoubleField(fRow.getDoubleField(colOut) + rowIn.getDoubleField(colIn), colOut);
	fRow.setLongDoubleField(
		fRow.getLongDoubleField(colAux) + rowIn.getLongDoubleField(colIn+1), colAux);
	fRow.setLongDoubleField(
		fRow.getLongDoubleField(colAux+1) + rowIn.getLongDoubleField(colIn+2), colAux+1);
}


//------------------------------------------------------------------------------
// Concat columns.
// rowIn(in) - Row that contains the columns to be concatenated.
//------------------------------------------------------------------------------
void RowAggregationUMP2::doGroupConcat(const Row& rowIn, int64_t i, int64_t o)
{
	uint8_t* data = fRow.getData();
	joblist::GroupConcatAgUM* gccAg = *((joblist::GroupConcatAgUM**)(data + fRow.getOffset(o)));
	gccAg->merge(rowIn, i);
}


//------------------------------------------------------------------------------
// Update the and/or/xor fields if input is not null.
// rowIn(in)    - Row to be included in aggregation.
// colIn(in)    - column in the input row group
// colOut(in)   - column in the output row group
// funcType(in) - aggregation function type
// Note: NULL value check must be done on UM & PM
//       UM may receive NULL values, too.
//------------------------------------------------------------------------------
void RowAggregationUMP2::doBitOp(const Row& rowIn, int64_t colIn, int64_t colOut, int funcType)
{
	uint64_t valIn = rowIn.getUintField(colIn);
	uint64_t valOut = fRow.getUintField(colOut);
	if (funcType == ROWAGG_BIT_AND)
		fRow.setUintField(valIn & valOut, colOut);
	else if (funcType == ROWAGG_BIT_OR)
		fRow.setUintField(valIn | valOut, colOut);
	else
		fRow.setUintField(valIn ^ valOut, colOut);
}


//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
RowAggregationDistinct::RowAggregationDistinct(const vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
									const vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
									joblist::ResourceManager *r,
									boost::shared_ptr<int64_t> sessionLimit) :
	RowAggregationUMP2(rowAggGroupByCols, rowAggFunctionCols, r, sessionLimit)
{

}


RowAggregationDistinct::RowAggregationDistinct(const RowAggregationDistinct& rhs):
	RowAggregationUMP2(rhs),
	fRowGroupDist(rhs.fRowGroupDist)
{
	fAggregator.reset(rhs.fAggregator->clone());
}


RowAggregationDistinct::~RowAggregationDistinct()
{
}


//------------------------------------------------------------------------------
// Aggregation
//
//------------------------------------------------------------------------------
void RowAggregationDistinct::setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut)
{
	fRowGroupIn = fRowGroupDist;
	fRowGroupOut = pRowGroupOut;
	initialize();
	fDataForDist.reinit(fRowGroupDist, AGG_ROWGROUP_SIZE);
	fRowGroupDist.setData(&fDataForDist);
	fAggregator->setInputOutput(pRowGroupIn, &fRowGroupDist);
}


//------------------------------------------------------------------------------
// Aggregation DISTINCT columns
//
//------------------------------------------------------------------------------
void RowAggregationDistinct::addAggregator(const boost::shared_ptr<RowAggregation>& agg,
											const RowGroup& rg)
{
	fRowGroupDist = rg;
	fAggregator = agg;
}


//------------------------------------------------------------------------------
// Aggregation DISTINCT columns
//
//------------------------------------------------------------------------------
void RowAggregationDistinct::addRowGroup(const RowGroup* pRows)
{
	fAggregator->addRowGroup(pRows);
}


void RowAggregationDistinct::addRowGroup(const RowGroup* pRows, vector<Row::Pointer>& inRows)
{
	fAggregator->addRowGroup(pRows, inRows);
}


//------------------------------------------------------------------------------
// Aggregation DISTINCT columns
//
//------------------------------------------------------------------------------
void RowAggregationDistinct::doDistinctAggregation()
{
	while (dynamic_cast<RowAggregationUM*>(fAggregator.get())->nextRowGroup())
	{
		fRowGroupIn.setData(fAggregator.get()->getOutputRowGroup()->getRGData());

		Row rowIn;
		fRowGroupIn.initRow(&rowIn);
		fRowGroupIn.getRow(0, &rowIn);
		for (uint64_t i = 0; i < fRowGroupIn.getRowCount(); ++i, rowIn.nextRow())
		{
			aggregateRow(rowIn);
		}
	}
}


void RowAggregationDistinct::doDistinctAggregation_rowVec(vector<Row::Pointer>& inRows)
{
		Row rowIn;
		fRowGroupIn.initRow(&rowIn);
		for (uint64_t i = 0; i < inRows.size(); ++i)
		{
			rowIn.setData(inRows[i]);
			aggregateRow(rowIn);
		}
}


//------------------------------------------------------------------------------
// Update the aggregation totals in the internal hashmap for the specified row.
// for non-DISTINCT columns works partially aggregated results
// rowIn(in) - Row to be included in aggregation.
//------------------------------------------------------------------------------
void RowAggregationDistinct::updateEntry(const Row& rowIn)
{
	for (uint64_t i = 0; i < fFunctionCols.size(); i++)
	{
		int64_t colIn  = fFunctionCols[i]->fInputColumnIndex;
		int64_t colOut = fFunctionCols[i]->fOutputColumnIndex;

		switch (fFunctionCols[i]->fAggFunction)
		{
			case ROWAGG_COUNT_ASTERISK:
			case ROWAGG_COUNT_COL_NAME:
			{
				int64_t count = fRow.getIntField<8>(colOut) + rowIn.getIntField<8>(colIn);
				fRow.setIntField<8>(count, colOut);
				break;
			}

			case ROWAGG_COUNT_DISTINCT_COL_NAME:
				if (isNull(&fRowGroupIn, rowIn, colIn) != true)
					fRow.setIntField<8>(fRow.getIntField<8>(colOut) + 1, colOut);
				break;

			case ROWAGG_MIN:
			case ROWAGG_MAX:
			case ROWAGG_SUM:
			case ROWAGG_DISTINCT_SUM:

				doMinMaxSum(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;

			case ROWAGG_AVG:
			{
				// The sum and count on UM may not be put next to each other:
				//   use colOut to store the sum;
				//   use colAux to store the count.
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				doAvg(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_DISTINCT_AVG:
			{
				// The sum and count on UM may not be put next to each other:
				//   use colOut to store the sum;
				//   use colAux to store the count.
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				RowAggregation::doAvg(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_STATS:
			{
				int64_t colAux = fFunctionCols[i]->fAuxColumnIndex;
				doStatistics(rowIn, colIn, colOut, colAux);
				break;
			}

			case ROWAGG_BIT_AND:
			case ROWAGG_BIT_OR:
			case ROWAGG_BIT_XOR:
			{
				doBitOp(rowIn, colIn, colOut, fFunctionCols[i]->fAggFunction);
				break;
			}

			case ROWAGG_GROUP_CONCAT:
			{
				doGroupConcat(rowIn, colIn, colOut);
				break;
			}

			case ROWAGG_COUNT_NO_OP:
			case ROWAGG_DUP_FUNCT:
			case ROWAGG_DUP_AVG:
			case ROWAGG_DUP_STATS:
			case ROWAGG_CONSTANT:
				break;

			default:
			{
				std::ostringstream errmsg;
				errmsg << "RowAggregationDistinct: function (id = " <<
					(uint64_t) fFunctionCols[i]->fAggFunction << ") is not supported.";
				cerr << errmsg.str() << endl;
				throw logging::QueryDataExcept(errmsg.str(), logging::aggregateFuncErr);
				break;
			}
		}
	}
}


//------------------------------------------------------------------------------
// Constructor / destructor
//------------------------------------------------------------------------------
RowAggregationSubDistinct::RowAggregationSubDistinct(
								const vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
								const vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
								joblist::ResourceManager *r,
								boost::shared_ptr<int64_t> sessionLimit) :
	RowAggregationUM(rowAggGroupByCols, rowAggFunctionCols, r, sessionLimit)
{
}


RowAggregationSubDistinct::RowAggregationSubDistinct(const RowAggregationSubDistinct& rhs):
	RowAggregationUM(rhs)
{
}


RowAggregationSubDistinct::~RowAggregationSubDistinct()
{
}


//------------------------------------------------------------------------------
// Setup the rowgroups and data associations
//
//------------------------------------------------------------------------------
void RowAggregationSubDistinct::setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut)
{
	// set up input/output association
	RowAggregation::setInputOutput(pRowGroupIn, pRowGroupOut);

	// initialize the aggregate row
	fRowGroupOut->initRow(&fDistRow, true);
	fDistRowData.reset(new uint8_t[fDistRow.getSize()]);
	fDistRow.setData(fDistRowData.get());
}


//------------------------------------------------------------------------------
//------------------------------------------------------------------------------
// Add rowgroup
//
//------------------------------------------------------------------------------
void RowAggregationSubDistinct::addRowGroup(const RowGroup *pRows)
{
	Row rowIn;
	pair<RowAggMap_t::iterator, bool> inserted;
	uint32_t i, j;

	pRows->initRow(&rowIn);
	pRows->getRow(0, &rowIn);

	for (i = 0; i < pRows->getRowCount(); ++i, rowIn.nextRow())
	{
		/* TODO: We can make the functors a little smarter and avoid doing this copy before the
		 * tentative insert */
		for (j = 0; j < fGroupByCols.size(); j++)
		{
			rowIn.copyField(fDistRow, j, fGroupByCols[j]->fInputColumnIndex);
		}

		tmpRow = &fDistRow;
		inserted = fAggMapPtr->insert(RowPosition(RowPosition::MSB, 0));
		if (inserted.second)
		{
			// if it was successfully inserted, fix the inserted values
			if (++fTotalRowCount > fMaxTotalRowCount && !newRowGroup())
			{
				throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
					errorMsg(logging::ERR_AGGREGATION_TOO_BIG), logging::ERR_AGGREGATION_TOO_BIG);
			}

			fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &fRow);
			fRowGroupOut->incRowCount();
			copyRow(fDistRow, &fRow);

			// replace the key value with an equivalent copy, yes this is OK
			const_cast<RowPosition &>(*(inserted.first)) =
				RowPosition(fResultDataVec.size() - 1, fRowGroupOut->getRowCount() - 1);
		}
	}
}

void RowAggregationSubDistinct::addRowGroup(const RowGroup* pRows, std::vector<Row::Pointer>& inRows)
{
	Row rowIn;
	pair<RowAggMap_t::iterator, bool> inserted;
	uint32_t i, j;

	pRows->initRow(&rowIn);

	for (i = 0; i < inRows.size(); ++i, rowIn.nextRow())
	{
		rowIn.setData(inRows[i]);
		/* TODO: We can make the functors a little smarter and avoid doing this copy before the
		 * tentative insert */
		for (j = 0; j < fGroupByCols.size(); j++)
			rowIn.copyField(fDistRow, j, fGroupByCols[j]->fInputColumnIndex);

		tmpRow = &fDistRow;
		inserted = fAggMapPtr->insert(RowPosition(RowPosition::MSB, 0));
		if (inserted.second)
		{
			// if it was successfully inserted, fix the inserted values
			if (++fTotalRowCount > fMaxTotalRowCount && !newRowGroup())
			{
				throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
					errorMsg(logging::ERR_AGGREGATION_TOO_BIG), logging::ERR_AGGREGATION_TOO_BIG);
			}

			fRowGroupOut->getRow(fRowGroupOut->getRowCount(), &fRow);
			fRowGroupOut->incRowCount();
			copyRow(fDistRow, &fRow);

			// replace the key value with an equivalent copy, yes this is OK
			const_cast<RowPosition &>(*(inserted.first)) =
				RowPosition(fResultDataVec.size() - 1, fRowGroupOut->getRowCount() - 1);
		}
	}
}


//------------------------------------------------------------------------------
// Concat columns.
// rowIn(in) - Row that contains the columns to be concatenated.
//------------------------------------------------------------------------------
void RowAggregationSubDistinct::doGroupConcat(const Row& rowIn, int64_t i, int64_t o)
{
	uint8_t* data = fRow.getData();
	joblist::GroupConcatAgUM* gccAg = *((joblist::GroupConcatAgUM**)(data + fRow.getOffset(o)));
	gccAg->merge(rowIn, i);
}


//------------------------------------------------------------------------------
// Constructor / destructor
//------------------------------------------------------------------------------
RowAggregationMultiDistinct::RowAggregationMultiDistinct(
								const vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
								const vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
								joblist::ResourceManager *r,
								boost::shared_ptr<int64_t> sessionLimit) :
	RowAggregationDistinct(rowAggGroupByCols, rowAggFunctionCols, r, sessionLimit)
{
}


RowAggregationMultiDistinct::RowAggregationMultiDistinct(const RowAggregationMultiDistinct &rhs):
	RowAggregationDistinct(rhs),
	fSubRowGroups(rhs.fSubRowGroups),
	fSubFunctions(rhs.fSubFunctions)
{
	fAggregator.reset(rhs.fAggregator->clone());

	boost::shared_ptr<RGData> data;
	fSubAggregators.clear();
	fSubRowData.clear();

	boost::shared_ptr<RowAggregationUM> agg;
	for (uint32_t i = 0; i < rhs.fSubAggregators.size(); i++)
	{
#if 0
		fTotalMemUsage += fSubRowGroups[i].getDataSize(AGG_ROWGROUP_SIZE);
		if (!fRm->getMemory(fSubRowGroups[i].getDataSize(AGG_ROWGROUP_SIZE, fSessionMemLimit)))
			throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
				errorMsg(logging::ERR_AGGREGATION_TOO_BIG), logging::ERR_AGGREGATION_TOO_BIG);
#endif
		data.reset(new RGData(fSubRowGroups[i], AGG_ROWGROUP_SIZE));
		fSubRowData.push_back(data);
		fSubRowGroups[i].setData(data.get());
		agg.reset(rhs.fSubAggregators[i]->clone());
		fSubAggregators.push_back(agg);
	}
}


RowAggregationMultiDistinct::~RowAggregationMultiDistinct()
{
}


//------------------------------------------------------------------------------
// Setup the rowgroups and data associations
//
//------------------------------------------------------------------------------
void RowAggregationMultiDistinct::setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut)
{
	// set up base class aggregators
	RowAggregationDistinct::setInputOutput(pRowGroupIn, pRowGroupOut);

	// set up sub aggregators
	for (uint64_t i = 0; i < fSubAggregators.size(); ++i)
		fSubAggregators[i]->setInputOutput(pRowGroupIn, &fSubRowGroups[i]);
}


//------------------------------------------------------------------------------
// Add sub aggregator for each distinct column with aggregate functions
//
//------------------------------------------------------------------------------
void RowAggregationMultiDistinct::addSubAggregator(const boost::shared_ptr<RowAggregationUM>& agg,
													const RowGroup& rg,
													const vector<SP_ROWAGG_FUNC_t>& funct)
{
	boost::shared_ptr<RGData> data;
#if 0
	fTotalMemUsage += rg.getDataSize(AGG_ROWGROUP_SIZE);
	if (!fRm->getMemory(rg.getDataSize(AGG_ROWGROUP_SIZE), fSessionMemLimit))
		throw logging::IDBExcept(logging::IDBErrorInfo::instance()->
			errorMsg(logging::ERR_AGGREGATION_TOO_BIG), logging::ERR_AGGREGATION_TOO_BIG);
#endif
	data.reset(new RGData(rg, AGG_ROWGROUP_SIZE));
	fSubRowData.push_back(data);

	//assert (agg->aggMapKeyLength() > 0);

	fSubAggregators.push_back(agg);
	fSubRowGroups.push_back(rg);
	fSubRowGroups.back().setData(data.get());
	fSubFunctions.push_back(funct);
}


void RowAggregationMultiDistinct::addRowGroup(const RowGroup* pRows)
{
	// aggregate to sub-subAggregators
	for (uint64_t i = 0; i < fSubAggregators.size(); ++i)
		fSubAggregators[i]->addRowGroup(pRows);
}


//------------------------------------------------------------------------------
// Aggregation DISTINCT columns
//
//------------------------------------------------------------------------------
void RowAggregationMultiDistinct::addRowGroup(const RowGroup* pRowGroupIn,
                                              vector<vector<Row::Pointer> >& inRows)
{
	for (uint64_t i = 0; i < fSubAggregators.size(); ++i)
	{
		fSubAggregators[i]->addRowGroup(pRowGroupIn, inRows[i]);
		inRows[i].clear();
	}
}


//------------------------------------------------------------------------------
// Aggregation DISTINCT columns
//
//------------------------------------------------------------------------------
void RowAggregationMultiDistinct::doDistinctAggregation()
{
	// backup the function column vector for finalize().
	vector<SP_ROWAGG_FUNC_t> origFunctionCols = fFunctionCols;

	// aggregate data from each sub-aggregator to distinct aggregator
	for (uint64_t i = 0; i < fSubAggregators.size(); ++i)
	{
		fFunctionCols = fSubFunctions[i];
		fRowGroupIn = fSubRowGroups[i];
		Row rowIn;
		fRowGroupIn.initRow(&rowIn);
		while (dynamic_cast<RowAggregationUM*>(fSubAggregators[i].get())->nextRowGroup())
		{
			fRowGroupIn.setData(fSubAggregators[i]->getOutputRowGroup()->getRGData());
			// no group by == no map, everything done in fRow
			if (fGroupByCols.empty())
				fRowGroupOut->setRowCount(1);

			fRowGroupIn.getRow(0, &rowIn);
			for (uint64_t j = 0; j < fRowGroupIn.getRowCount(); ++j, rowIn.nextRow())
			{
				aggregateRow(rowIn);
			}
		}
	}

	// restore the function column vector
	fFunctionCols = origFunctionCols;
}


void RowAggregationMultiDistinct::doDistinctAggregation_rowVec(vector<vector<Row::Pointer> >& inRows)
{
	// backup the function column vector for finalize().
	vector<SP_ROWAGG_FUNC_t> origFunctionCols = fFunctionCols;

	// aggregate data from each sub-aggregator to distinct aggregator
	for (uint64_t i = 0; i < fSubAggregators.size(); ++i)
	{
		fFunctionCols = fSubFunctions[i];
		fRowGroupIn = fSubRowGroups[i];
		Row rowIn;
		fRowGroupIn.initRow(&rowIn);

		for (uint64_t j = 0; j < inRows[i].size(); ++j)
		{
			rowIn.setData(inRows[i][j]);
			aggregateRow(rowIn);
		}
		inRows[i].clear();
	}

	// restore the function column vector
	fFunctionCols = origFunctionCols;
}


GroupConcatAg::GroupConcatAg(SP_GroupConcat& gcc) : fGroupConcat(gcc)
{
}


GroupConcatAg::~GroupConcatAg()
{
}



AggHasher::AggHasher(const Row &row, Row **tRow, uint32_t keyCount, RowAggregation *ra)
	: agg(ra), tmpRow(tRow), r(row), lastKeyCol(keyCount-1)
{
}

inline uint64_t AggHasher::operator()(const RowPosition &data) const
{
	uint64_t ret;
	Row *row;

	if (data.group == RowPosition::MSB)
		row = *tmpRow;
	else {
		agg->fResultDataVec[data.group]->getRow(data.row, &r);
		row = &r;
	}

	ret = row->hash(lastKeyCol);
	//cout << "hash=" << ret << " keys=" << keyColCount << " row=" << r.toString() << endl;
	return ret;
}


AggComparator::AggComparator(const Row &row, Row **tRow, uint32_t keyCount, RowAggregation *ra)
	: agg(ra), tmpRow(tRow), r1(row), r2(row), lastKeyCol(keyCount-1)
{
}

inline bool AggComparator::operator()(const RowPosition &d1, const RowPosition &d2) const
{
	bool ret;
	Row *pr1, *pr2;

	if (d1.group == RowPosition::MSB)
		pr1 = *tmpRow;
	else {
		agg->fResultDataVec[d1.group]->getRow(d1.row, &r1);
		pr1 = &r1;
	}

	if (d2.group == RowPosition::MSB)
		pr2 = *tmpRow;
	else {
		agg->fResultDataVec[d2.group]->getRow(d2.row, &r2);
		pr2 = &r2;
	}

	ret = pr1->equals(*pr2, lastKeyCol);
	//cout << "eq=" << (int) ret << " keys=" << keyColCount << ": r1=" << r1.toString() <<
	//"\n             r2=" << r2.toString() << endl;
	return ret;
}


} // end of rowgroup namespace
