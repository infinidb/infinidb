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
// $Id: bppseeder.h 1960 2012-09-27 17:35:56Z pleblanc $
// C++ Interface: bppseeder
//
// Description: 
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#ifndef BPPSEEDER_H_
#define BPPSEEDER_H_

#include <fstream>
#include <iostream>
#include <iomanip>
#include <unistd.h>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <boost/shared_ptr.hpp>

#include "batchprimitiveprocessor.h"
#include "primitiveserver.h"
#include "umsocketselector.h"
#include "prioritythreadpool.h"

namespace primitiveprocessor
{
class BPPSeeder : public threadpool::PriorityThreadPool::Functor
{
	public:
		BPPSeeder(const messageqcpp::SBS &,
				const SP_UM_MUTEX& wLock, 
				const SP_UM_IOSOCK& ios,
				const int pmThreads,
				const bool trace=false);
		BPPSeeder(const BPPSeeder &b);

		virtual ~BPPSeeder();

		int operator()();

		bool isSysCat();
		boost::shared_ptr<std::ofstream> spof;

		uint32_t getID();

		void priority(uint p) { _priority = p; }
		uint priority() { return _priority; }

	private:
		BPPSeeder();
		void catchHandler(const std::string& s, uint32_t uniqueID, uint32_t step);
		void sendErrorMsg(uint32_t id, uint16_t status, uint32_t step);
		void flushSyscatOIDs();

		messageqcpp::SBS bs;
		SP_UM_MUTEX  writelock;
		SP_UM_IOSOCK sock;
		int fPMThreads;
		bool fTrace;
		
		/* To support reentrancy */
		uint32_t uniqueID, sessionID, stepID, failCount;
		boost::shared_ptr<BatchPrimitiveProcessor> bpp;
		SBPPV bppv;
		bool firstRun;

		uint _priority;
};

};

#endif
