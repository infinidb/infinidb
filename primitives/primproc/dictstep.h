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
// $Id: dictstep.h 2110 2013-06-19 15:51:38Z bwilkinson $
// C++ Interface: dictstep
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#ifndef DICTSTEP_H_
#define DICTSTEP_H_

#include "command.h"
#include "primitivemsg.h"

namespace primitiveprocessor {


class DictStep : public Command
{
	public:
		DictStep();
		virtual ~DictStep();

		void execute();
		void project();
		void project(int64_t *vals);		//used by RTSCommand to redirect input
		void projectIntoRowGroup(rowgroup::RowGroup &rg, uint32_t row);
		void projectIntoRowGroup(rowgroup::RowGroup &rg, int64_t *vals, uint32_t col);
		uint64_t getLBID();

		/* This doesn't do anything for this class...  make it column-specific or not? */
		void nextLBID();
		void createCommand(messageqcpp::ByteStream &);
		void resetCommand(messageqcpp::ByteStream &);

		/* Put bootstrap code here (ie, build the template primitive msg) */
		void prep(int8_t outputType, bool makeAbsRids);

		SCommand duplicate();
		bool operator==(const DictStep &) const;
		bool operator!=(const DictStep &) const;

		int getCompType() const { return compressionType; }
		void setCompType(int ct) { compressionType = ct; }

	private:
		DictStep(const DictStep &);
 		DictStep& operator=(const DictStep &);

 		struct StringPtr {
 			const uint8_t* ptr;
 			unsigned    len;

 			StringPtr() : ptr(NULL), len(0) {;}
			StringPtr(const uint8_t* p, unsigned l) : ptr(p), len(l) {;}
 		};

		void _execute();
		void issuePrimitive(bool isProjection);
		void processResult();
		void projectResult(std::string* tmpStrings);
		void projectResult(StringPtr *tmpStrings);
		void _project();
		void _projectToRG(rowgroup::RowGroup &rg, uint32_t col);

		// struct used for scratch space
		struct OrderedToken {
			uint64_t rid;
			uint64_t token;
			uint16_t pos;
			std::string str;
			bool inResult;
			OrderedToken() : inResult(false) { }
			~OrderedToken() { }
		};
		struct TokenSorter {
			inline bool operator()(const OrderedToken &c1, const OrderedToken &c2) const
			{ return (c1.token < c2.token);	}
		};
		struct PosSorter {
			inline bool operator()(const OrderedToken &c1, const OrderedToken &c2) const
			{ return (c1.pos < c2.pos); }
		};

		//bug 3679.  FilterCommand depends on the result being in the same relative
		//order as the input.  These fcns help restore the original order.
		void copyResultToTmpSpace(OrderedToken *ot);
		void copyResultToFinalPosition(OrderedToken *ot);

		// Worst case, 8192 tokens in the msg.  Each is 10 bytes. */
		boost::scoped_array<uint8_t> inputMsg;
		uint32_t tmpResultCounter;
		uint32_t totalResultLength;
		DictInput *primMsg;
		std::vector<uint8_t> result;

		uint64_t lbid;
		uint32_t fbo;
		uint32_t traceFlags;  // probably move this to Command
		uint8_t BOP;
		int64_t *values;
		boost::scoped_array<std::string>* strValues;
		int compressionType;
		ByteStream filterString;
		uint32_t filterCount;
		uint32_t bufferSize;
		uint16_t inputRidCount;

		bool hasEqFilter;
		boost::shared_ptr<primitives::DictEqualityFilter> eqFilter;
		boost::shared_array<primitives::idb_regex_t> likeFilter;
		uint8_t eqOp;   // COMPARE_EQ or COMPARE_NE

		friend class RTSCommand;
};

}  // namespace

#endif
