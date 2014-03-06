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

//  $Id: windowfunctiontype.h 3932 2013-06-25 16:08:10Z xlou $


#ifndef UTILS_WINDOWFUNCTIONTYPE_H
#define UTILS_WINDOWFUNCTIONTYPE_H

#include <map>
#include <string>
#include <utility>
#include <vector>
#include <boost/shared_ptr.hpp>

#include "calpontsystemcatalog.h"
#include "returnedcolumn.h"
#include "rowgroup.h"
#include "windowframe.h"


namespace ordering
{
// forward reference
class EqualCompData;
};

namespace execplan
{
// forward reference
class ConstantColumn;
};

namespace joblist
{
// forward reference
class WindowFunctionStep;
};

namespace windowfunction
{

// forward reference
class WindowFunctionType;
class IdbOrderBy;
class WindowFrame;

const int WF__UNDEFINED       = 0;
const int WF__COUNT_ASTERISK  = 1;
const int WF__COUNT           = 2;
const int WF__SUM             = 3;
const int WF__AVG             = 4;
const int WF__MIN             = 5;
const int WF__MAX             = 6;
const int WF__COUNT_DISTINCT  = 7;
const int WF__SUM_DISTINCT    = 8;
const int WF__AVG_DISTINCT    = 9;
const int WF__STDDEV_POP      = 10;
const int WF__STDDEV_SAMP     = 11;
const int WF__VAR_POP         = 12;
const int WF__VAR_SAMP        = 13;

const int WF__ROW_NUMBER      = 14;
const int WF__RANK            = 15;
const int WF__PERCENT_RANK    = 16;
const int WF__DENSE_RANK      = 17;
const int WF__CUME_DIST       = 18;

const int WF__FIRST_VALUE     = 19;
const int WF__LAST_VALUE      = 20;
const int WF__NTH_VALUE       = 21;
const int WF__LAG             = 22;
const int WF__LEAD            = 23;
const int WF__NTILE           = 24;
const int WF__PERCENTILE_CONT = 25;
const int WF__PERCENTILE_DISC = 26;

const int WF__REGR_SLOPE      = 27;
const int WF__REGR_INTERCEPT  = 28;
const int WF__REGR_COUNT      = 29;
const int WF__REGR_R2         = 30;
const int WF__REGR_AVGX       = 31;
const int WF__REGR_AVGY       = 32;
const int WF__REGR_SXX        = 33;
const int WF__REGR_SXY        = 34;
const int WF__REGR_SYY        = 35;



/** @brief class WindowFunction
 *
 */
class WindowFunctionType
{
public:
	// @brief WindowFunctionType constructor
	WindowFunctionType(int id = 0, const std::string& name = "") :
		fFunctionId(id), fFunctionName(name), fFrameUnit(0) {};

	// use default copy construct
	//WindowFunctionType(const WindowFunctionType&);

	// @brief WindowFunctionType destructor
	virtual ~WindowFunctionType() {};

	// @brief virtual operator(begin, end, current, data, row)
	virtual void operator()(int64_t, int64_t, int64_t) = 0;

	// @brief virtual clone()
	virtual WindowFunctionType* clone() const = 0;

	// @brief virtual resetData()
	virtual void resetData() { fPrev = -1; }

	// @brief virtual parseParms()
	virtual void parseParms(const std::vector<execplan::SRCP>&) {}

	// @brief virtual display method
	virtual const std::string toString() const;

	// @brief access methods
	int64_t functionId() const                      { return fFunctionId; }
	void functionId(int id)                         { fFunctionId = id; }
	const std::vector<int64_t>& fieldIndex() const  { return fFieldIndex; }
	void fieldIndex(const std::vector<int64_t>& v)  { fFieldIndex = v; }
	void setRowMetaData(const rowgroup::RowGroup& g, const rowgroup::Row& r)
		{ fRowGroup = g; fRow = r; }
	void setRowData(const boost::shared_ptr<std::vector<joblist::RowPosition> >& d) {fRowData = d;}
	int64_t frameUnit() const                       { return fFrameUnit; }
	void frameUnit(int u)                           { fFrameUnit = u; }
	std::pair<int64_t, int64_t> partition() const   { return fPartition; }
	void partition(std::pair<int64_t, int64_t>& p)  { fPartition = p; }
	const boost::shared_ptr<ordering::EqualCompData>& peer() const  { return fPeer; }
	void peer(const boost::shared_ptr<ordering::EqualCompData>& p)  { fPeer = p; }
	void setCallback(joblist::WindowFunctionStep* step)             { fStep = step; }

	static boost::shared_ptr<WindowFunctionType> makeWindowFunction(const std::string&, int ct);

protected:

	static std::map<std::string, int> windowFunctionId;

	// utility methods
	template<typename T> void getValue(uint64_t, T&);
	template<typename T> void setValue(int, int64_t, int64_t, int64_t, T* = NULL);
	template<typename T> void setValue(uint64_t, T&);
	template<typename T> void implicit2T(uint64_t, T&, int);
	template<typename T> void getConstValue(execplan::ConstantColumn*, T&, bool&);

	virtual void* getNullValueByType(int, int);

	int64_t getIntValue(uint64_t i)                 { return fRow.getIntField(i);    }
	double  getDoubleValue(uint64_t i)              { return fRow.getDoubleField(i); }
	void    setIntValue(int64_t i, int64_t v)       { fRow.setIntField(v, i);        }
	void    setDoubleValue(int64_t i, double  v)    { fRow.setDoubleField(v, i);     }


	// for string table
	rowgroup::Row::Pointer getPointer(joblist::RowPosition& r)
	{ return fStep->getPointer(r, fRowGroup, fRow); }

	// function type
	int64_t                                     fFunctionId;
	std::string                                 fFunctionName;

	// output and input field indices: [0] - output
	std::vector<int64_t>                        fFieldIndex;

	// row meta data
	rowgroup::RowGroup	                        fRowGroup;
	rowgroup::Row                               fRow;

	// data set
	boost::shared_ptr<std::vector<joblist::RowPosition> > fRowData;

	// frame unit ( ROWS | RANGE )
	int64_t                                     fFrameUnit;

	// partition
	std::pair<int64_t, int64_t>                 fPartition;

	// functor for peer checking
	boost::shared_ptr<ordering::EqualCompData>  fPeer;
	int64_t                                     fPrev;

	// for checking if query is cancelled
	joblist::WindowFunctionStep*                fStep;

};


extern std::map<int, std::string> colType2String;


} // namespace

#endif  // UTILS_WINDOWFUNCTIONTYPE_H

// vim:ts=4 sw=4:

