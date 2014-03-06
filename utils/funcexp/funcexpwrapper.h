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

//
//
// $Id: funcexpwrapper.h 3495 2013-01-21 14:09:51Z rdempsey $
//
// C++ Interface: funcexpwrapper
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//

/** @file */

#ifndef FUNCEXPWRAPPER_H_
#define FUNCEXPWRAPPER_H_

#include <parsetree.h>
#include <returnedcolumn.h>
#include "funcexp.h"

namespace funcexp {

/** @brief FuncExpWrapper class
  */
class FuncExpWrapper : public messageqcpp::Serializeable
{
	public:
		FuncExpWrapper();
		FuncExpWrapper(const FuncExpWrapper &);
		virtual ~FuncExpWrapper();

		void operator=(const FuncExpWrapper &);

		void serialize(messageqcpp::ByteStream &) const;
		void deserialize(messageqcpp::ByteStream &);

		bool evaluate(rowgroup::Row *);
		inline bool evaluateFilter(uint32_t num, rowgroup::Row *r);
		inline uint32_t getFilterCount() const;

		void addFilter(const boost::shared_ptr<execplan::ParseTree>&);
		void addReturnedColumn(const boost::shared_ptr<execplan::ReturnedColumn>&);

	private:
		std::vector<boost::shared_ptr<execplan::ParseTree> > filters;
		std::vector<boost::shared_ptr<execplan::ReturnedColumn> > rcs;
		FuncExp *fe;
};

inline bool FuncExpWrapper::evaluateFilter(uint32_t num, rowgroup::Row *r)
{
	return fe->evaluate(*r, filters[num].get());
}

inline uint32_t FuncExpWrapper::getFilterCount() const
{
	return filters.size();
}

}

#endif
