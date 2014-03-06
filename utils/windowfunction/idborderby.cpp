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

//  $Id: idborderby.cpp 3932 2013-06-25 16:08:10Z xlou $


#include <iostream>
//#define NDEBUG
#include <cassert>
#include <string>
#include <stack>
using namespace std;

#include <boost/shared_array.hpp>
using namespace boost;

#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "resourcemanager.h"
using namespace joblist;

#include "rowgroup.h"
using namespace rowgroup;

#include "idborderby.h"


namespace ordering
{

int IntCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fSpec.fIndex);
	bool b2 = l->row2().isNullValue(fSpec.fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = fSpec.fNf;
		else if (b1 == true && b2 == false)
			ret = -fSpec.fNf;
	}
	else
	{
		int64_t v1 = l->row1().getIntField(fSpec.fIndex);
		int64_t v2 = l->row2().getIntField(fSpec.fIndex);

		if (v1 > v2)
			ret = fSpec.fAsc;
		else if (v1 < v2)
			ret = -fSpec.fAsc;
	}

	return ret;
}


int UintCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fSpec.fIndex);
	bool b2 = l->row2().isNullValue(fSpec.fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = fSpec.fNf;
		else if (b1 == true && b2 == false)
			ret = -fSpec.fNf;
	}
	else
	{
		uint64_t v1 = l->row1().getUintField(fSpec.fIndex);
		uint64_t v2 = l->row2().getUintField(fSpec.fIndex);

		if (v1 > v2)
			ret = fSpec.fAsc;
		else if (v1 < v2)
			ret = -fSpec.fAsc;
	}

	return ret;
}


int StringCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fSpec.fIndex);
	bool b2 = l->row2().isNullValue(fSpec.fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = fSpec.fNf;
		else if (b1 == true && b2 == false)
			ret = -fSpec.fNf;
	}
	else
	{
		string v1 = l->row1().getStringField(fSpec.fIndex);
		string v2 = l->row2().getStringField(fSpec.fIndex);

		if (v1 > v2)
			ret = fSpec.fAsc;
		else if (v1 < v2)
			ret = -fSpec.fAsc;
	}

	return ret;
}


int DoubleCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fSpec.fIndex);
	bool b2 = l->row2().isNullValue(fSpec.fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = fSpec.fNf;
		else if (b1 == true && b2 == false)
			ret = -fSpec.fNf;
	}
	else
	{
		double v1 = l->row1().getDoubleField(fSpec.fIndex);
		double v2 = l->row2().getDoubleField(fSpec.fIndex);

		if (v1 > v2)
			ret = fSpec.fAsc;
		else if (v1 < v2)
			ret = -fSpec.fAsc;
	}

	return ret;
}


int FloatCompare::operator()(IdbCompare* l, Row::Pointer r1, Row::Pointer r2)
{
	l->row1().setData(r1);
	l->row2().setData(r2);

	bool b1 = l->row1().isNullValue(fSpec.fIndex);
	bool b2 = l->row2().isNullValue(fSpec.fIndex);

	int ret = 0;

	if (b1 == true || b2 == true)
	{
		if (b1 == false && b2 == true)
			ret = fSpec.fNf;
		else if (b1 == true && b2 == false)
			ret = -fSpec.fNf;
	}
	else
	{
		float v1 = l->row1().getFloatField(fSpec.fIndex);
		float v2 = l->row2().getFloatField(fSpec.fIndex);

		if (v1 > v2)
			ret = fSpec.fAsc;
		else if (v1 < v2)
			ret = -fSpec.fAsc;
	}

	return ret;
}


bool CompareRule::less(Row::Pointer r1, Row::Pointer r2)
{
	for (vector<Compare*>::iterator i = fCompares.begin(); i != fCompares.end(); i++)
	{
		int c = ((*(*i))(fIdbCompare, r1, r2));

		if (c < 0)
			return true;
		else if (c > 0)
			return false;
	}

	return false;
}


void CompareRule::compileRules(const std::vector<IdbSortSpec>& spec, const rowgroup::RowGroup& rg)
{
	const vector<CalpontSystemCatalog::ColDataType>& types = rg.getColTypes();
	for (vector<IdbSortSpec>::const_iterator i = spec.begin(); i != spec.end(); i++)
	{
		switch (types[i->fIndex])
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
			{
				Compare* c = new IntCompare(*i);
				fCompares.push_back(c);
				break;
			}
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
            {
                Compare* c = new UintCompare(*i);
                fCompares.push_back(c);
                break;
            }
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				Compare* c = new StringCompare(*i);
				fCompares.push_back(c);
				break;
			}
            case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
			{
				Compare* c = new DoubleCompare(*i);
				fCompares.push_back(c);
				break;
			}
			case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
			{
				Compare* c = new FloatCompare(*i);
				fCompares.push_back(c);
				break;
			}
			case CalpontSystemCatalog::DATE:
			case CalpontSystemCatalog::DATETIME:
			{
				Compare* c = new UintCompare(*i);
				fCompares.push_back(c);
				break;
			}
			default:
			{
				break;
			}
		}
	}
}


void IdbCompare::initialize(const RowGroup& rg)
{
	fRowGroup = rg;
	fRowGroup.initRow(&fRow1);
	fRowGroup.initRow(&fRow2);
}


void IdbCompare::setStringTable(bool b)
{
	fRowGroup.setUseStringTable(b);
	fRowGroup.initRow(&fRow1);
	fRowGroup.initRow(&fRow2);
}


// OrderByData class implementation
OrderByData::OrderByData(const std::vector<IdbSortSpec>& spec, const rowgroup::RowGroup& rg)
{
	IdbCompare::initialize(rg);
	fRule.compileRules(spec, rg);
	fRule.fIdbCompare = this;
}


// IdbOrderBy class implementation
IdbOrderBy::IdbOrderBy() :
	fDistinct(false), fMemSize(0), fRowsPerRG(8192), fErrorCode(0), fRm(NULL)
{
}


IdbOrderBy::~IdbOrderBy()
{
	if (fRm)
		fRm->returnMemory(fMemSize);

	// delete compare objects
	vector<Compare*>::iterator i = fRule.fCompares.begin();
	while (i != fRule.fCompares.end())
		delete *i++;
}


void IdbOrderBy::initialize(const RowGroup& rg)
{
	// initialize rows
	IdbCompare::initialize(rg);

	uint64_t newSize = fRowsPerRG * rg.getRowSize();
	if (!fRm->getMemory(newSize))
	{
		cerr << IDBErrorInfo::instance()->errorMsg(fErrorCode)
			 << " @" << __FILE__ << ":" << __LINE__;
		throw IDBExcept(fErrorCode);
	}
	fMemSize += newSize;
	fData.reinit(fRowGroup, fRowsPerRG);
	fRowGroup.setData(&fData);
	fRowGroup.resetRowGroup(0);
	fRowGroup.initRow(&fRow0);
	fRowGroup.getRow(0, &fRow0);

	// set compare functors
	fRule.compileRules(fOrderByCond, fRowGroup);

	fRowGroup.initRow(&row1);
	fRowGroup.initRow(&row2);
	if (fDistinct)
	{
		fDistinctMap.reset(new DistinctMap_t(10, Hasher(this, getKeyLength()), Eq(this, getKeyLength())));
	}
}


bool IdbOrderBy::getData(RGData& data)
{
	if (fDataQueue.empty())
		return false;

	data = fDataQueue.front();
	fDataQueue.pop();

	return true;
}


bool EqualCompData::operator()(Row::Pointer a, Row::Pointer b)
{
	bool eq = true;
	fRow1.setData(a);
	fRow2.setData(b);

	for (vector<uint64_t>::const_iterator i = fIndex.begin(); i != fIndex.end() && eq; i++)
	{
		CalpontSystemCatalog::ColDataType type = fRow1.getColType(*i);
		switch (type)
		{
			case CalpontSystemCatalog::TINYINT:
			case CalpontSystemCatalog::SMALLINT:
			case CalpontSystemCatalog::MEDINT:
			case CalpontSystemCatalog::INT:
			case CalpontSystemCatalog::BIGINT:
			case CalpontSystemCatalog::DECIMAL:
            case CalpontSystemCatalog::UDECIMAL:
            case CalpontSystemCatalog::UTINYINT:
            case CalpontSystemCatalog::USMALLINT:
            case CalpontSystemCatalog::UMEDINT:
            case CalpontSystemCatalog::UINT:
            case CalpontSystemCatalog::UBIGINT:
			case CalpontSystemCatalog::DATE:
			case CalpontSystemCatalog::DATETIME:
            {
				// equal compare. ignore sign and null
				eq = (fRow1.getUintField(*i) == fRow2.getUintField(*i));
                break;
            }
			case CalpontSystemCatalog::CHAR:
			case CalpontSystemCatalog::VARCHAR:
			{
				eq = (fRow1.getStringField(*i) == fRow2.getStringField(*i));
				break;
			}
            case CalpontSystemCatalog::DOUBLE:
			case CalpontSystemCatalog::UDOUBLE:
			{
				eq = (fRow1.getDoubleField(*i) == fRow2.getDoubleField(*i));
				break;
			}
			case CalpontSystemCatalog::FLOAT:
            case CalpontSystemCatalog::UFLOAT:
			{
				eq = (fRow1.getFloatField(*i) == fRow2.getFloatField(*i));
				break;
			}
			default:
			{
				eq = false;
				uint64_t ec = ERR_WF_UNKNOWN_COL_TYPE;
				cerr << IDBErrorInfo::instance()->errorMsg(ec, type)
					 << " @" << __FILE__ << ":" << __LINE__;
				throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ec, type), ec);
				break;
			}
		}
	}

	return eq;
}

uint64_t IdbOrderBy::Hasher::operator()(const Row::Pointer &p) const
{
	Row &row = ts->row1;
	row.setPointer(p);
	uint64_t ret = row.hash(colCount);
	//cout << "hash(): returning " << ret << " for row: " << row.toString() << endl;
	return ret;
}

bool IdbOrderBy::Eq::operator()(const Row::Pointer &d1, const Row::Pointer &d2) const
{
	Row &r1 = ts->row1, &r2 = ts->row2;
	r1.setPointer(d1);
	r2.setPointer(d2);
	bool ret = r1.equals(r2, colCount);
	//cout << "equals(): returning " << (int) ret << " for r1: " << r1.toString() << " r2: " << r2.toString()
	//	<< endl;

	return ret;
}


}
// vim:ts=4 sw=4:

