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

//  $Id: frameboundrow.h 3868 2013-06-06 22:13:05Z xlou $


#ifndef UTILS_FRAMEBOUNDROW_H
#define UTILS_FRAMEBOUNDROW_H


#include "framebound.h"


namespace windowfunction
{


/** @brief class FrameBoundRow
 *
 */
class FrameBoundRow : public FrameBound
{
public:
	/** @brief FrameBoundRow constructor
	 *  @param  t, frame type
	 */
	FrameBoundRow(int t = 0) : FrameBound(t) {};

	/** @brief FrameBoundRow destructor
	 */
	virtual ~FrameBoundRow() {};

	/** @brief clone
	 */
	virtual FrameBound* clone()
	{ return new FrameBoundRow(*this); }

	/** @brief virtual void getBound
	 */
	int64_t getBound(int64_t, int64_t, int64_t);

	const std::string toString() const;


protected:

};


/** @brief class FrameBoundConstantRow
 *
 */
class FrameBoundConstantRow : public FrameBoundRow
{
public:
	/** @brief FrameBoundConstant constructor
	 *  @param  t, frame type
	 *  @param  c, constant value.  !! caller need to check NULL or negative value !!
	 */
	FrameBoundConstantRow(int t = 0, int c = 0) : FrameBoundRow(t), fOffset(c) {};

	/** @brief FrameBoundConstantRow destructor
	 */
	virtual ~FrameBoundConstantRow() {};

	/** @brief clone
	 */
	virtual FrameBound* clone()
	{ return new FrameBoundConstantRow(*this); }

	/** @brief virtual void getBound
	 */
	int64_t getBound(int64_t, int64_t, int64_t);

	const std::string toString() const;


protected:

	// constant offset
	int64_t                                     fOffset;

};


/** @brief class FrameBoundExpressionRow
 *
 */
template<typename T>
class FrameBoundExpressionRow : public FrameBoundConstantRow
{
public:
	/** @brief FrameBoundExpressionRow constructor
	 *  @param  t, frame type
	 */
	FrameBoundExpressionRow(int t, uint64_t id = -1, int idx = -1) :
		FrameBoundConstantRow(t), fExprTupleId(id), fExprIdx(idx) {};

	/** @brief FrameBoundExpressionRow destructor
	 */
	virtual ~FrameBoundExpressionRow() {};

	/** @brief clone
	 */
	virtual FrameBound* clone()
	{ return new FrameBoundExpressionRow<T>(*this); }

	/** @brief virtual void getBound
	 */
	int64_t getBound(int64_t, int64_t, int64_t);

	const std::string toString() const;

	void setExprTupleId(int id)                 { fExprTupleId = id;    }
	uint64_t getExprTupleId() const             { return fExprTupleId;  }

	void setExprIndex(int i)                    { fExprIdx = i;         }
	uint64_t getExprIndex() const               { return fExprIdx;      }


protected:

	void getOffset();

	// id and index in row
	uint64_t                                    fExprTupleId;
	int                                         fExprIdx;

};




} // namespace

#endif  // UTILS_FRAMEBOUNDROW_H

// vim:ts=4 sw=4:
