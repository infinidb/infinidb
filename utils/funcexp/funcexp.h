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

/****************************************************************************
* $Id: funcexp.h 3495 2013-01-21 14:09:51Z rdempsey $
*
*
****************************************************************************/
/** @file */

#ifndef FUNCEXP_H
#define FUNCEXP_H

#include <string>
#include <vector>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <boost/thread/mutex.hpp>

#include "rowgroup.h"
#include "returnedcolumn.h"
#include "parsetree.h"

namespace execplan
{
	class FunctionColumn;
	class ArithmeticColumn;
}

namespace funcexp
{
	class Func;
	
	typedef std::tr1::unordered_map<std::string, Func*> FuncMap;
	
/** @brief FuncExp is a component for evaluate function and expression filters
  */
class FuncExp {
public:		
	
	/** Singleton pattern */
	static FuncExp* instance();
	
	/********************************************************************
	* Row based evaluation APIs
	********************************************************************/
	
	/** @brief evaluate a filter stack on row. used for F&E on the where clause
	*
	* @param row input row that contains all the columns in the filter stack
	* @param filters parsetree of filters to evaluate
	* @return boolean of whether or not the row passed evaluation
	*/
	inline bool evaluate(rowgroup::Row& row, execplan::ParseTree* filters);
	
	/** @brief evaluate a filter stack on rowgroup
	 *
	 * @param row input rowgroup that contains all the columns in the filter stack
	 * @param filters parse tree of filters to evaluate. The failed rows are removed from the rowgroup  
	 */
	inline void evaluate(rowgroup::RowGroup& rowgroup, execplan::ParseTree* filters);
	
	/** @brief evaluate a F&E column on row. used for F&E on the select and group by clause
	*
	* @param row input row that contains all the columns in all the expressions
	* @param expressions vector of F&Es that needs evaluation. The results are filled on the row.
	*/
	void evaluate(rowgroup::Row& row, std::vector<execplan::SRCP>& expressions);
		
	/** @brief evaluate a F&E column on rowgroup. used for F&E on the select and group by clause
	*
	* @param row input rowgroup that contains all the columns in all the expressions
	* @param expressions vector of F&Es that needs evaluation. The results are filled on each row.
	*/
	inline void evaluate(rowgroup::RowGroup& rowgroup, std::vector<execplan::SRCP>& expressions);
				
	/** @brief get functor from functor map
	*
	* @param funcName function name
	* @return functor pointer. If non-support function, return NULL
	*/
	Func* getFunctor(std::string& funcName);

private:
	static FuncExp* fInstance;
	static boost::mutex fInstanceMutex;
	FuncMap fFuncMap;
	FuncExp();
};

inline bool FuncExp::evaluate( rowgroup::Row& row, execplan::ParseTree* filters )
{	
	bool isNull = false;
	return (filters->getBoolVal(row, isNull));
}

inline void FuncExp::evaluate( rowgroup::RowGroup& rowgroup, execplan::ParseTree* filters )
{	
}

}

#endif

