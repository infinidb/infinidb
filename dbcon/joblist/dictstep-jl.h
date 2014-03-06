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
// $Id: dictstep-jl.h 9655 2013-06-25 23:08:13Z xlou $
// C++ Interface: dictstep-js
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

#ifndef DICTSTEP_JL_H_
#define DICTSTEP_JL_H_

#include "jobstep.h"
#include "command-jl.h"

namespace joblist {

class DictStepJL : public CommandJL
{
	public:
		DictStepJL();
		DictStepJL(const pDictionaryStep &);
		virtual ~DictStepJL();

		void setLBID(uint64_t data, uint32_t dbroot);		// converts a rid or dictionary token to an LBID.  For ColumnCommandJL it's a RID, for a DictStep it's a token.
		uint8_t getTableColumnType();
		std::string toString();

		/*  XXXPAT: The width is only valid for projection steps and the output
			type is TUPLE at the moment. */
		void setWidth(uint16_t);
		uint16_t getWidth();

		CommandType getCommandType() { return DICT_STEP; }

		void createCommand(messageqcpp::ByteStream &) const;
		void runCommand(messageqcpp::ByteStream &) const;

	private:
		DictStepJL(const DictStepJL &);

// 		uint64_t lbid;
// 		uint32_t fbo;
		uint32_t traceFlags;  // probably move this to Command
		uint8_t BOP;
		uint16_t colWidth;
		int compressionType;
		messageqcpp::ByteStream filterString;
		uint32_t filterCount;
		std::vector<std::string> eqFilter;
		bool hasEqFilter;
		uint8_t eqOp;    // COMPARE_EQ or COMPARE_NE
};

};  // namespace

#endif
