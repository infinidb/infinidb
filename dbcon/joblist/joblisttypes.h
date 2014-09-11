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

/**
* $Id: joblisttypes.h 8323 2012-02-15 16:28:09Z pleblanc $
*/
/** @file */

#ifndef JOBLISTTYPES_
#define JOBLISTTYPES_

#include <stdint.h>
#include <string>

namespace joblist {

const uint64_t BIGINTNULL = 0x8000000000000000ULL;
const uint64_t BIGINTEMPTYROW = 0x8000000000000001ULL;
const uint32_t INTNULL = 0x80000000;
const uint32_t INTEMPTYROW = 0x80000001;
const uint16_t SMALLINTNULL = 0x8000;
const uint16_t SMALLINTEMPTYROW = 0x8001;
const uint8_t TINYINTNULL = 0x80;
const uint8_t TINYINTEMPTYROW = 0x81;

const uint32_t FLOATNULL = 0xFFAAAAAA;
const uint32_t FLOATEMPTYROW = 0xFFAAAAAB;
const uint64_t DOUBLENULL = 0xFFFAAAAAAAAAAAAAULL;
const uint64_t DOUBLEEMPTYROW = 0xFFFAAAAAAAAAAAABULL;

const uint32_t DATENULL = 0xFFFFFFFE;
const uint32_t DATEEMPTYROW = 0xFFFFFFFF;
const uint64_t DATETIMENULL = 0xFFFFFFFFFFFFFFFEULL;
const uint64_t DATETIMEEMPTYROW = 0xFFFFFFFFFFFFFFFFULL;

const uint8_t CHAR1NULL = 0xFE;
const uint8_t CHAR1EMPTYROW = 0xFF;
const uint16_t CHAR2NULL = 0xFEFF;
const uint16_t CHAR2EMPTYROW = 0xFFFF;
const uint32_t CHAR4NULL = 0xFEFFFFFF;
const uint32_t CHAR4EMPTYROW = 0xFFFFFFFF;
const uint64_t CHAR8NULL = 0xFEFFFFFFFFFFFFFFULL;
const uint64_t CHAR8EMPTYROW = 0xFFFFFFFFFFFFFFFFULL;

const int8_t NULL_INT8 = TINYINTNULL;
const int16_t NULL_INT16 = SMALLINTNULL;
const int32_t NULL_INT32 = INTNULL;
const int64_t NULL_INT64 = BIGINTNULL;

const std::string CPNULLSTRMARK("_CpNuLl_");
const std::string CPSTRNOTFOUND("_CpNoTf_");

/** @brief enum used to define the join type.
 */
typedef uint32_t JoinType;
const JoinType INIT = 0,
	INNER = 0x1,
	LARGEOUTER = 0x2,
	SMALLOUTER = 0x4,
	LEFTOUTER = 0x3,    // deprecated, using impossible values for backward compat
	RIGHTOUTER = 0x5,	// deprecated..
	SEMI = 0x8,
	ANTI = 0x10,
	SCALAR = 0x20,
	MATCHNULLS = 0x40,
	WITHFCNEXP = 0x80,
	CORRELATED = 0x100;
}

#endif

