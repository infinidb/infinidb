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

/*****************************************************************************
 * $Id: transactionnode.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#define TRANSACTIONNODE_DLLEXPORT
#include "transactionnode.h"
#undef TRANSACTIONNODE_DLLEXPORT

namespace BRM {

TransactionNode::TransactionNode(int txn) : txnID(txn), _die(false), _sleeping(false)
{
}

TransactionNode::~TransactionNode()
{
}

void TransactionNode::setTxnID(VER_t txn)
{
	txnID = txn;
}

int TransactionNode::getTxnID() const
{
	return txnID;
}

void TransactionNode::sleep(boost::mutex &mutex)
{
	_sleeping = true;
	condVar.wait(mutex);
}

void TransactionNode::wake()
{
	condVar.notify_one();
	_sleeping = false;
}

void TransactionNode::die()
{
	_die = true;
}

bool TransactionNode::dead()
{
	return _die;
}

bool TransactionNode::sleeping()
{
	return _sleeping;
}

}  // namespace
