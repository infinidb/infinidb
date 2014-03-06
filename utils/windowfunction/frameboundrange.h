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

//  $Id: frameboundrange.h 3868 2013-06-06 22:13:05Z xlou $


#ifndef UTILS_FRAMEBOUNDRANGE_H
#define UTILS_FRAMEBOUNDRANGE_H


#include "framebound.h"


namespace windowfunction
{


/** @brief class FrameBoundRange
 *
 */
class FrameBoundRange : public FrameBound
{
public:
	/** @brief FrameBoundRange constructor
	 *  @param  t, frame type
	 *  @param  a, order by sort spec: asc | desc
	 *  @param  n, order by sort spec: null first | null last
	 *  @param  v, order by column data type
	 */
	FrameBoundRange(int t = 0, bool a = true, bool n = true)
		: FrameBound(t), fAsc(a), fNullFirst(n), fIsZero(false) {};

	/** @brief FrameBoundRange destructor
	 */
	virtual ~FrameBoundRange() {};

	/** @brief clone
	 */
	virtual FrameBound* clone() { return NULL; }  // abstract class

	/** @brief virtual void getBound
	 */
	int64_t getBound(int64_t, int64_t, int64_t);

	const std::string toString() const;

	void setTupleId(std::vector<uint64_t> ids)  { fTupleId = ids;   }
	std::vector<uint64_t> getTupleId() const    { return fTupleId;  }

	void setIndex(vector<int> idx)              { fIndex = idx;     }
	const std::vector<int>& getIndex() const    { return fIndex;    }

	void isZero(bool z) { fIsZero = z;    }
	bool isZero() const { return fIsZero; }

protected:

	// for range calculation
	// data, row meta data, order by column index, ascending | descending
	// [0]: order by; [1]: interval; [2]: [0] +/- [1]
	std::vector<uint64_t>                       fTupleId;
	std::vector<int>                            fIndex;

	// order by sort specification
	bool                                        fAsc;
	bool                                        fNullFirst;

	// in case expr evaluates to 0
	bool                                        fIsZero;

};


/** @brief struct ValueType
 *
 */
template<typename T>
struct ValueType
{
	T                                       fValue;
	bool                                    fIsNull;

	// constructor
	ValueType() : fValue(0), fIsNull(false) {}
};


/** @brief class FrameBoundConstantRange
 *
 */
template<typename T>
class FrameBoundConstantRange : public FrameBoundRange
{
public:
	/** @brief FrameBoundConstant constructor
	 *  @param  t, frame type
	 *  @param  a, order by sort spec: asc | desc
	 *  @param  n, order by sort spec: null first | null last
	 *  @param  c, constant value.  !! caller need to check NULL or negative value !!
	 */
	FrameBoundConstantRange(int t = 0, bool a = true, bool n = true, void* c = NULL)
		: FrameBoundRange(t, a, n)
	{
		fValue.fIsNull = (c == NULL);
		if (!fValue.fIsNull)
			fValue.fValue = *((T*) c);
	};

	/** @brief FrameBoundConstantRange destructor
	 */
	virtual ~FrameBoundConstantRange() {};

	/** @brief clone
	 */
	virtual FrameBound* clone()
	{ return new FrameBoundConstantRange(*this); }

	/** @brief virtual void getBound
	 */
	int64_t getBound(int64_t, int64_t, int64_t);

	const std::string toString() const;

protected:

	// find the range offset
	// i: partition upper bound
    // j: current row
	// k: partition lower bound
	virtual int64_t getPrecedingOffset(int64_t j, int64_t i);
	virtual int64_t getFollowingOffset(int64_t j, int64_t k);

	// validate value is not negative
	virtual void validate() {}

	// get value at fIndex[x]
	void getValue(ValueType<T>&, int64_t);
	T getValueByType(int64_t);

	// order by column value type
	ValueType<T> fValue;

};


/** @brief class FrameBoundExpressionRange
 *
 */
template<typename T>
class FrameBoundExpressionRange : public FrameBoundConstantRange<T>
{
public:
	/** @brief FrameBoundExpressionRange constructor
	 *  @param  t, frame type
	 *  @param  a, order by sort spec: asc | desc
	 *  @param  n, order by sort spec: null first | null last
	 */
	FrameBoundExpressionRange(int t = 0, bool a = true, bool n = true) :
		FrameBoundConstantRange<T>(t, a, n) {};

	/** @brief FrameBoundExpressionRange destructor
	 */
	virtual ~FrameBoundExpressionRange() {};

	/** @brief clone
	 */
	virtual FrameBound* clone()
	{ return new FrameBoundExpressionRange(*this); }

	/** @brief virtual void getBound
	 */
	// int64_t getBound(int64_t, int64_t, int64_t);

	const std::string toString() const;


protected:


	// virtual in FrameBoundRange
	int64_t getPrecedingOffset(int64_t j, int64_t i);
	int64_t getFollowingOffset(int64_t j, int64_t k);

	// validate the expression is not negative
	void validate();

};


} // namespace

#endif  // UTILS_FRAMEBOUNDRANGE_H

// vim:ts=4 sw=4:
