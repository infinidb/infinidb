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

/******************************************************************************
 * $Id: unique32generator.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *****************************************************************************/

/** @file Contains class that controls unique 32-bit generation for joblist.
 *
 */

#ifndef _UNIQUE32_GENERATOR_H_
#define _UNIQUE32_GENERATOR_H_

#include <stdint.h>
#include <boost/thread.hpp>

#include "dbrm.h"

namespace joblist {

/** @brief Controls unique 32-bit generation for joblist
 *
 * Maintains a single DBRM connection that is used for the generation of all
 * unique 32-bit ints.  All joblist classes should go through this singleton
 * class which maintains a single DBRM connection exclusively reserved for
 * this purpose.
 */
class UniqueNumberGenerator
{
	public:
		static UniqueNumberGenerator* instance();      // singleton accessor
		static void               deleteInstance();// singleton cleanup
		uint32_t                  getUnique32();   // generate unique 32-bit int
		uint64_t				  getUnique64();	// generate unique 64-bit int
		
	private:
		UniqueNumberGenerator()  { }
		~UniqueNumberGenerator() { }

		static UniqueNumberGenerator* fUnique32Generator;
		static boost::mutex       fLock;
		BRM::DBRM                 fDbrm;
};

}  // namespace

#endif
