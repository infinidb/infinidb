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

/*
 * autoincrementmanager.h
 *
 *  Created on: Dec 16, 2011
 *      Author: pleblanc
 */

#include <unistd.h>
#include <boost/thread.hpp>
#include <map>
#include "calpontsystemcatalog.h"

#ifndef AUTOINCREMENTMANAGER_H_
#define AUTOINCREMENTMANAGER_H_

#if defined(_MSC_VER) && defined(xxxBRMAUTOINCMGR_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

class AutoincrementManager {
public:
	EXPORT AutoincrementManager();
	EXPORT virtual ~AutoincrementManager();

	EXPORT void startSequence(uint32_t OID, uint64_t firstNum, uint32_t colWidth,
                              execplan::CalpontSystemCatalog::ColDataType colDataType);
	EXPORT bool getAIRange(uint32_t OID, uint64_t count, uint64_t *firstNum);
	EXPORT void resetSequence(uint32_t OID, uint64_t value);
	EXPORT void getLock(uint32_t OID);
	EXPORT void releaseLock(uint32_t OID);
	EXPORT void deleteSequence(uint32_t OID);

private:
	static const uint32_t lockTime = 30;   // 30 seconds
	struct sequence {
		sequence() : value(0), overflow(0) { }
		sequence(const sequence &s) : value(s.value), overflow(s.overflow) { }
		sequence & operator=(const sequence &s) { value = s.value; overflow = s.overflow; return *this; }
		uint64_t value;
		uint64_t overflow;
		boost::mutex lock;
	};

	boost::mutex lock;
	std::map<uint64_t, sequence> sequences;
};

} /* namespace BRM */

#undef EXPORT

#endif /* AUTOINCREMENTMANAGER_H_ */
