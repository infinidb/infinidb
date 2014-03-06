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

/***********************************************************************
*   $Id: filtercommand.h 2035 2013-01-21 14:12:19Z rdempsey $
*
*
***********************************************************************/
/** @file
 * class FilterCommand interface
 */

#ifndef PRIMITIVES_FILTERCOMMAND_H_
#define PRIMITIVES_FILTERCOMMAND_H_

#include <string>
#include <boost/scoped_array.hpp>
#include "command.h"
#include "blocksize.h"
#include "calpontsystemcatalog.h"


namespace primitiveprocessor
{

class FilterCommand : public Command
{
	public:
		FilterCommand();
		virtual ~FilterCommand();

		// returns a FilterCommand based on column types
		static Command* makeFilterCommand(messageqcpp::ByteStream&, std::vector<SCommand>& cmds);

		// virtuals from base class -- Command
		void execute();
		void project();
		void projectIntoRowGroup(rowgroup::RowGroup &rg, uint32_t col);
		uint64_t getLBID();
		void nextLBID();
		void createCommand(messageqcpp::ByteStream&);
		void resetCommand(messageqcpp::ByteStream&);
		SCommand duplicate();
		void prep(int8_t outputType, bool makeAbsRids);

		void setColTypes(const execplan::CalpontSystemCatalog::ColType& left,
						 const execplan::CalpontSystemCatalog::ColType& right);

		// operator override
		bool operator==(const FilterCommand&) const;
		bool operator!=(const FilterCommand&) const;

		int getCompType() const { return 0; }

	protected:
		// filter operation
		virtual void doFilter();

		// compare method, take the indices to the values array
		virtual bool compare(uint64_t, uint64_t);

		// binary operator
		uint8_t fBOP;

		// column type for null check
		execplan::CalpontSystemCatalog::ColType leftColType;
		execplan::CalpontSystemCatalog::ColType rightColType;

	private:
		// disabled copy constructor and operator
		FilterCommand(const FilterCommand&);
		FilterCommand& operator=(const FilterCommand&);
};


class ScaledFilterCmd : public FilterCommand
{
	public:
		ScaledFilterCmd();
		virtual ~ScaledFilterCmd();
		SCommand duplicate();

        void setFactor(double);
        double factor();

		// operator override
		bool operator==(const ScaledFilterCmd&) const;
		bool operator!=(const ScaledFilterCmd&) const;

	protected:
		// compare method, take the indices to the values array
		bool compare(uint64_t, uint64_t);

		// value used in comparison;
		double fFactor;

	private:
		// disabled copy constructor and operator
		ScaledFilterCmd(const ScaledFilterCmd &);
		ScaledFilterCmd& operator=(const ScaledFilterCmd &);
};


class StrFilterCmd : public FilterCommand
{
	public:
		StrFilterCmd();
		virtual ~StrFilterCmd();

		// override FilterCommand methods
		void execute();
		SCommand duplicate();

        void setCompareFunc(uint32_t);
        void setCharLength(size_t);
        size_t charLength();

		// operator override
		bool operator==(const StrFilterCmd&) const;
		bool operator!=(const StrFilterCmd&) const;

	protected:
		// compare method, take the indices to the values array
		bool compare(uint64_t, uint64_t);

		// compare method for differernt column combination, c--char[], s--string
		// compare char[]-char[] is not the same as int-int due to endian issue.
		bool compare_cc(uint64_t, uint64_t);
		bool compare_ss(uint64_t, uint64_t);
		bool compare_cs(uint64_t, uint64_t);
		bool compare_sc(uint64_t, uint64_t);
		bool (StrFilterCmd::*fCompare)(uint64_t, uint64_t);

		// colWidth of columns the don't need a dictionary
		size_t fCharLength;

	private:
		// disabled copy constructor and operator
		StrFilterCmd(const StrFilterCmd &);
		StrFilterCmd& operator=(const StrFilterCmd &);
};


};


#endif // PRIMITIVES_FILTERCOMMAND_H_

