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
// $Id: passthrucommand.h 1855 2012-04-04 18:20:09Z rdempsey $
// C++ Interface: passthrucommand
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#ifndef PASSTHRUCOMMAND_H_
#define PASSTHRUCOMMAND_H_

#include "command.h"

namespace primitiveprocessor
{

class PassThruCommand : public Command
{
	public:
		PassThruCommand();
		virtual ~PassThruCommand();

		void prep(int8_t outputType, bool makeAbsRids);
		void execute();
		void project();
		void projectIntoRowGroup(rowgroup::RowGroup &rg, uint col);
		uint64_t getLBID();
		void nextLBID();
		void createCommand(messageqcpp::ByteStream &);
		void resetCommand(messageqcpp::ByteStream &);
		SCommand duplicate();
		bool operator==(const PassThruCommand &) const;
		bool operator!=(const PassThruCommand &) const;

		int getCompType() const { return 0; }
	private:
		PassThruCommand(const PassThruCommand &);

		uint8_t colWidth;

		/* Minor optimization for projectIntoRowGroup() */
		rowgroup::Row r;
		uint rowSize;
};

}

#endif
