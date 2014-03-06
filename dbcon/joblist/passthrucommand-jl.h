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
// $Id: passthrucommand-jl.h 9655 2013-06-25 23:08:13Z xlou $
// C++ Interface: passthrucommand-jl
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

#ifndef PASSTHRUCOMMAND_JL
#define PASSTHRUCOMMAND_JL

#include "jobstep.h"
#include "command-jl.h"

namespace joblist {

class PassThruCommandJL : public CommandJL
{
	public:
		PassThruCommandJL(const PassThruStep &);
		virtual ~PassThruCommandJL();

		void setLBID(uint64_t data, uint32_t dbroot);		// converts a rid or dictionary token to an LBID.  For ColumnCommandJL it's a RID, for a DictStep it's a token.
		uint8_t getTableColumnType();
		std::string toString();

		void createCommand(messageqcpp::ByteStream &) const;
		void runCommand(messageqcpp::ByteStream &) const;
		uint16_t getWidth();
		CommandType getCommandType() { return PASS_THRU; }

	private:
		PassThruCommandJL();
		PassThruCommandJL(const PassThruCommandJL &);

		uint8_t colWidth;
		uint8_t tableColumnType;
};

};

#endif
