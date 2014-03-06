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
// $Id: rtscommand-jl.h 9655 2013-06-25 23:08:13Z xlou $
// C++ Interface: ridtostringcommand-jl
//
// Description:
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//
/** @file */

#ifndef RTSCOMMANDJL_H_
#define RTSCOMMANDJL_H_

#include "jobstep.h"
#include "command-jl.h"
#include <boost/scoped_ptr.hpp>

namespace joblist
{

class RTSCommandJL : public CommandJL
{
	public:
		RTSCommandJL(const pColStep &, const pDictionaryStep &);
		RTSCommandJL(const PassThruStep &, const pDictionaryStep &);
		virtual ~RTSCommandJL();

		void setLBID(uint64_t data, uint32_t dbroot);		// converts a rid or dictionary token to an LBID.  For ColumnCommandJL it's a RID, for a DictStep it's a token.
		uint8_t getTableColumnType();
		std::string toString();
		bool isPassThru() { return (passThru != 0); }
		uint16_t getWidth();
		CommandType getCommandType() { return RID_TO_STRING; }

		void createCommand(messageqcpp::ByteStream &) const;
		void runCommand(messageqcpp::ByteStream &) const;

	private:
		RTSCommandJL();
		RTSCommandJL(const RTSCommandJL &);

		boost::scoped_ptr<ColumnCommandJL> col;
		boost::scoped_ptr<DictStepJL> dict;
		uint8_t passThru;
};

};

#endif
