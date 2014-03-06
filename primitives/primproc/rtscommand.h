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
// $Id: rtscommand.h 2035 2013-01-21 14:12:19Z rdempsey $
// C++ Interface: rtscommand
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#ifndef RTSCOMMAND_H_
#define RTSCOMMAND_H_

#include "command.h"
#include <boost/scoped_ptr.hpp>

namespace primitiveprocessor
{

class RTSCommand : public Command
{
	public:
		RTSCommand();
		virtual ~RTSCommand();

		void execute();
		void project();
		void projectIntoRowGroup(rowgroup::RowGroup &rg, uint32_t col);
		uint64_t getLBID();
		void nextLBID();
		void createCommand(messageqcpp::ByteStream &);
		void resetCommand(messageqcpp::ByteStream &);
		SCommand duplicate();
		bool operator==(const RTSCommand &) const;
		bool operator!=(const RTSCommand &) const;
	
		void setBatchPrimitiveProcessor(BatchPrimitiveProcessor *);

		/* Put bootstrap code here (ie, build the template primitive msg) */
		void prep(int8_t outputType, bool makeAbsRids);
		uint8_t isPassThru() { return passThru; }
		void setAbsNull(bool a = true) { absNull = a; }
		void getLBIDList(uint32_t loopCount, std::vector<int64_t> *lbids);

		//TODO: do we need to reference either col or dict?
		int getCompType() const { return dict.getCompType(); }
	private:
		RTSCommand(const RTSCommand &);

		ColumnCommand col;
		DictStep dict;
		uint8_t passThru;
		bool absNull;
};

}

#endif
