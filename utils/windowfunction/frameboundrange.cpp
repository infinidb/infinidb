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

//  $Id: frameboundrange.cpp 3932 2013-06-25 16:08:10Z xlou $


//#define NDEBUG
#include <cassert>
#include <cmath>
#include <sstream>
#include <iomanip>
using namespace std;

#include "idberrorinfo.h"
#include "errorids.h"
#include "exceptclasses.h"
using namespace logging;

#include "idborderby.h"
using namespace ordering;

#include "frameboundrange.h"


namespace windowfunction
{


int64_t FrameBoundRange::getBound(int64_t b, int64_t e, int64_t c)
{
	if (fStart)
	{
		while (c > b)
		{
			if (!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(c-1))))
				break;

			c--;
		}
	}
	else
	{
		while (c < e)
		{
			if (!fPeer->operator()(getPointer(fRowData->at(c)), getPointer(fRowData->at(c+1))))
				break;

			c++;
		}
	}

	return c;
}


const string FrameBoundRange::toString() const
{
	return FrameBound::toString();
}


template<typename T>
int64_t FrameBoundConstantRange<T>::getBound(int64_t b, int64_t e, int64_t c)
{
	// set row data
	fRow.setData(getPointer(fRowData->at(c)));
	getValue(fValue, fIndex[2]);

	// make sure the expr is not negative
	validate();

	// calculate the offset, and move
	if (fIsZero)
		c = FrameBoundRange::getBound(b, e, c);
	else if (fBoundType < WF__CURRENT_ROW)
		c -= getPrecedingOffset(c, b);
	else
		c += getFollowingOffset(c, e);

	return c;
}


template<typename T>
int64_t FrameBoundConstantRange<T>::getPrecedingOffset(int64_t c, int64_t b)
{
	// test each row to find the bound
	bool next = true;
	int64_t i = c;
	int64_t j = 0;

	for (i--, j++; i >= b && next; i--, j++)
	{
		// set row data, get order by column value
		fRow.setData(getPointer(fRowData->at(i)));
		ValueType<T> v;
		getValue(v, fIndex[0]);
		if (v.fIsNull)
		{
			next = fValue.fIsNull; // let null = null
		}
		else if (fValue.fIsNull)
		{
			next = false;
		}
		else if (fAsc && v.fValue < fValue.fValue)
		{
			next = false;
		}
		else if (!fAsc && v.fValue > fValue.fValue)
		{
			next = false;
		}
		else if (!fStart && v.fValue == fValue.fValue)
		{
			next = false;
		}
	}

	if (!next)
	{
		if (fStart)
			j -= 2;
		else
			j -= 1;
	}

	return j;
}


template<typename T>
int64_t FrameBoundConstantRange<T>::getFollowingOffset(int64_t c, int64_t e)
{
	// test each row to find the bound
	bool next = true;
	int64_t i = c;
	int64_t j = 0;

	for (i++, j++; i <= e && next; i++, j++)
	{
		// set row data, get order by column value
		fRow.setData(getPointer(fRowData->at(i)));
		ValueType<T> v;
		getValue(v, fIndex[0]);
		if (v.fIsNull)
		{
			next = fValue.fIsNull; // let null = null
		}
		else if (fValue.fIsNull)
		{
			next = false;
		}
		else if (fAsc && v.fValue > fValue.fValue)
		{
			next = false;
		}
		else if (!fAsc && v.fValue < fValue.fValue)
		{
			next = false;
		}
		else if (fStart && v.fValue == fValue.fValue)
		{
			next = false;
		}
	}

	if (!next)
	{
		if (fStart)
			j -= 1;
		else
			j -= 2;
	}

	return j;
}


template<typename T>
void FrameBoundConstantRange<T>::getValue(ValueType<T>& v, int64_t i)
{
	v.fIsNull = fRow.isNullValue(i);
	if (!v.fIsNull)
		v.fValue = fRow.getIntField(i);
}


template<typename T>
T FrameBoundConstantRange<T>::getValueByType(int64_t i)
{
	T t;
	return t;
}


template<> int64_t FrameBoundConstantRange<int64_t>::getValueByType(int64_t i)
{
	return fRow.getIntField(i);
}


template<> uint64_t FrameBoundConstantRange<uint64_t>::getValueByType(int64_t i)
{
	uint64_t v = fRow.getUintField(i);

	// convert date to datetime, [refer to treenode.h]
	if (fRow.getColType(fIndex[0]) == execplan::CalpontSystemCatalog::DATE && i == 0)
		v = v << 32;

	return v;
}


template<> double FrameBoundConstantRange<double>::getValueByType(int64_t i)
{
	return fRow.getDoubleField(i);
}


template<> float FrameBoundConstantRange<float>::getValueByType(int64_t i)
{
	return fRow.getFloatField(i);
}


template<typename T>
const string FrameBoundConstantRange<T>::toString() const
{
	ostringstream oss;
	oss << fValue.fValue << " " << FrameBound::toString();
	return oss.str();
}


template<typename T>
int64_t FrameBoundExpressionRange<T>::getPrecedingOffset(int64_t c, int64_t b)
{
	return FrameBoundConstantRange<T>::getPrecedingOffset(c, b);
}


template<typename T>
int64_t FrameBoundExpressionRange<T>::getFollowingOffset(int64_t c, int64_t e)
{
	return FrameBoundConstantRange<T>::getFollowingOffset(c, e);
}


template<typename T>
void FrameBoundExpressionRange<T>::validate()
{
	bool invalid = false;
	ostringstream oss;

	if (this->fRow.isNullValue(this->fIndex[1]))
	{
		invalid = true;
		oss << "NULL";
	}
	else
	{
		switch (this->fRow.getColType(this->fIndex[1]))
		{
			case execplan::CalpontSystemCatalog::TINYINT:
			case execplan::CalpontSystemCatalog::SMALLINT:
			case execplan::CalpontSystemCatalog::MEDINT:
			case execplan::CalpontSystemCatalog::INT:
			case execplan::CalpontSystemCatalog::BIGINT:
			case execplan::CalpontSystemCatalog::DECIMAL:
			{
				int64_t tmp = this->fRow.getIntField(this->fIndex[1]);
				this->fIsZero = (tmp == 0);
				if (tmp < 0)
				{
					invalid = true;
					oss << tmp;
				}

				break;
			}

			case execplan::CalpontSystemCatalog::DOUBLE:
			case execplan::CalpontSystemCatalog::UDOUBLE:
			{
				double tmp = this->fRow.getDoubleField(this->fIndex[1]);
				this->fIsZero = (tmp == 0.0);
				if (tmp < 0)
				{
					invalid = true;
					oss << tmp;
				}
				break;
			}

			case execplan::CalpontSystemCatalog::FLOAT:
			case execplan::CalpontSystemCatalog::UFLOAT:
			{
				float tmp = this->fRow.getFloatField(this->fIndex[1]);
				this->fIsZero = (tmp == 0.0);
				if (tmp < 0)
				{
					invalid = true;
					oss << tmp;
				}
				break;
			}

			case execplan::CalpontSystemCatalog::UTINYINT:
			case execplan::CalpontSystemCatalog::USMALLINT:
			case execplan::CalpontSystemCatalog::UMEDINT:
			case execplan::CalpontSystemCatalog::UINT:
			case execplan::CalpontSystemCatalog::UBIGINT:
			case execplan::CalpontSystemCatalog::UDECIMAL:
			default:
			{
				int64_t tmp = this->fRow.getIntField(this->fIndex[1]);
				this->fIsZero = (tmp == 0);
				break;
			}
		}
	}

	if (invalid)
	{
		oss << " (expr)";
		throw IDBExcept(IDBErrorInfo::instance()->errorMsg(ERR_WF_BOUND_OUT_OF_RANGE, oss.str()),
				ERR_WF_BOUND_OUT_OF_RANGE);
	}
}


template<typename T>
const string FrameBoundExpressionRange<T>::toString() const
{
	ostringstream oss;
	oss << " value_expr " << FrameBound::toString();
	return oss.str();
}


template class    FrameBoundConstantRange<int64_t>;
template class    FrameBoundConstantRange<uint64_t>;
template class    FrameBoundConstantRange<double>;
template class    FrameBoundConstantRange<float>;

template class    FrameBoundExpressionRange<int64_t>;
template class    FrameBoundExpressionRange<double>;
template class    FrameBoundExpressionRange<float>;
template class    FrameBoundExpressionRange<uint64_t>;

}   //namespace
// vim:ts=4 sw=4:

