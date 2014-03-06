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
*   $Id: filtercommand-jl.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file
 * class FilterCommand interface
 */

#ifndef JOBLIST_FILTERCOMMANDJL_H_
#define JOBLIST_FILTERCOMMANDJL_H_

#include "joblist.h"
#include "command-jl.h"

namespace joblist
{

class FilterCommandJL : public CommandJL
{
	public:
		FilterCommandJL(const FilterStep&);
		virtual ~FilterCommandJL();

		void setLBID(uint64_t rid, uint32_t dbroot);
		uint8_t getTableColumnType();
		CommandType getCommandType();
		std::string toString();
		void createCommand(messageqcpp::ByteStream &bs) const;
		void runCommand(messageqcpp::ByteStream &bs) const;
		uint16_t getWidth();
		const uint8_t getBOP() const { return fBOP; };

	private:
		FilterCommandJL();
		FilterCommandJL(const FilterCommandJL &);

		uint8_t  fBOP;
		execplan::CalpontSystemCatalog::ColType fColType;
};

};


#endif // JOBLIST_FILTERCOMMANDJL_H_

