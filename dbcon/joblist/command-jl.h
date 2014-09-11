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
// $Id: command-jl.h 8272 2012-01-19 16:28:34Z xlou $
// C++ Interface: command
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//
/** @file */

#ifndef COMMANDJL_H
#define COMMANDJL_H

#include <string>
#include "serializeable.h"
#include "bytestream.h"

namespace joblist
{

class CommandJL
{
// #warning got CommandJL definition
	public:
		CommandJL();
		CommandJL(const CommandJL &);
		virtual ~CommandJL();
		
		virtual void setLBID(uint64_t data) = 0;		// converts a rid or dictionary token to an LBID.  For ColumnCommandJL it's a RID, for a DictStep it's a token.
		virtual uint8_t getTableColumnType() = 0;
		virtual std::string toString() = 0;
		virtual void createCommand(messageqcpp::ByteStream &) const;
		virtual void runCommand(messageqcpp::ByteStream &) const;
		virtual uint32_t getOID();
		virtual const std::string& getColName();
		virtual void setTupleKey(uint32_t tkey);
		virtual uint32_t getTupleKey();
		virtual uint16_t getWidth() = 0;

		void setBatchPrimitiveProcessor(BatchPrimitiveProcessorJL *);

		enum CommandType {
			NONE,
			COLUMN_COMMAND,
			DICT_STEP,
			DICT_SCAN,
			PASS_THRU,
			RID_TO_STRING,
			FILTER_COMMAND
		};

		virtual CommandType getCommandType() = 0;

	protected:
		BatchPrimitiveProcessorJL *bpp;
		uint32_t OID;
		uint32_t tupleKey;
		std::string colName;  // for stats

	private:

};

typedef boost::shared_ptr<CommandJL> SCommand;

};

#endif
