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
 * $Id: transactionnode.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#ifndef TRANSACTIONNODE_H_
#define TRANSACTIONNODE_H_

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "brmtypes.h"
#include "rgnode.h"

#if defined(_MSC_VER) && defined(xxxTRANSACTIONNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

class TransactionNode : public RGNode
{
	public:
		EXPORT explicit TransactionNode(int txnid=0);
		EXPORT virtual ~TransactionNode();

		EXPORT void setTxnID(VER_t);
		EXPORT int getTxnID() const;

		EXPORT void sleep(boost::mutex &mutex);
		EXPORT void wake();
		EXPORT void die();
		EXPORT bool dead();
		EXPORT bool sleeping();

	private:
		TransactionNode(const TransactionNode &);
		TransactionNode& operator=(const TransactionNode &);
		
		boost::condition condVar;
		VER_t txnID;
		bool _die, _sleeping;
};

#undef EXPORT

} // namespace

#endif		// TRANSACTIONNODE_H_
