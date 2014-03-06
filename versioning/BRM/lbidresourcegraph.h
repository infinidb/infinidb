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
 * $Id: lbidresourcegraph.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#ifndef LBIDRESOURCEGRAPH_H_
#define LBIDRESOURCEGRAPH_H_

#include <map>
#include <set>
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif
#include <boost/thread.hpp>

#include "brmtypes.h"
#include "transactionnode.h"
#include "resourcenode.h"

#if defined(_MSC_VER) && defined(xxxLBIDRESOURCEGRAPH_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM
{

class LBIDResourceGraph
{

	public:
		typedef std::tr1::unordered_set<ResourceNode *, RNHasher, RNEquals> RNodes_t;

		EXPORT LBIDResourceGraph();
		EXPORT ~LBIDResourceGraph();

		EXPORT int reserveRange(LBID_t start, LBID_t end, VER_t txn, boost::mutex &mutex);
		
		/// releases all resources held by txn
		EXPORT void releaseResources(VER_t txn);

		/// releases one resource
		EXPORT void releaseResource(LBID_t start);

	private:
		uint64_t color;

		LBIDResourceGraph(const LBIDResourceGraph &);
		LBIDResourceGraph & operator=(const LBIDResourceGraph &);
		
		void connectResources(LBID_t start, LBID_t end, TransactionNode *txnNode);
		bool checkDeadlock(TransactionNode &);
		bool DFSStep(RGNode *, uint64_t, uint64_t) const;

		std::map<VER_t, TransactionNode *> txns;
		RNodes_t resources;

};

}

#undef EXPORT

#endif
