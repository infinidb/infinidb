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
 * $Id: lbidresourcegraph.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include <stdexcept>
#include <boost/thread.hpp>

#define LBIDRESOURCEGRAPH_DLLEXPORT
#include "lbidresourcegraph.h"
#undef LBIDRESOURCEGRAPH_DLLEXPORT

using namespace std;

namespace BRM
{

LBIDResourceGraph::LBIDResourceGraph() : color(0)
{
}

LBIDResourceGraph::LBIDResourceGraph(const LBIDResourceGraph &r)
{
	throw logic_error("Don't do that");
}

LBIDResourceGraph::~LBIDResourceGraph()
{
	std::map<VER_t, TransactionNode *>::iterator tnit;
	RNodes_t::iterator rit;
	TransactionNode *txnNode;

	for (tnit = txns.begin(); tnit != txns.end(); ) {
		txnNode = (*tnit).second;
		if (txnNode->sleeping()) {
			txnNode->die();
			txnNode->wake();
			++tnit;
		}
		else {
			txns.erase(tnit++);
			delete txnNode;
		}	
	}
	
	for (rit = resources.begin(); rit != resources.end(); ) {
		delete *rit;
		resources.erase(rit++);
	}
}

LBIDResourceGraph & LBIDResourceGraph::operator=(const LBIDResourceGraph &r)
{
	throw logic_error("Don't do that");
}

void LBIDResourceGraph::connectResources(LBID_t start, LBID_t end, 
	TransactionNode *txnNode)
{
	vector<ResourceNode *> intersection, reserveList;
	RNodes_t::iterator sit;
	vector<ResourceNode *>::iterator it, next;
	LBID_t i;

#if 0
	/* This version creates ResourceNodes that span a range of LBIDs.
	   It's deprecated, but worth saving in case we go back to ranges.  This is
	   the most sensitive code in that impl. */

	// get the list of existing resources intersecting the requested range
	for (sit = resources.begin(); sit != resources.end() && (*sit)->start() <= end; sit++) {
		if ((*sit)->intersects(start, end))
			intersection.push_back(*sit);
	}

	// make a new node for every gap in the requested range and prepare to reserve it
	if (intersection.size() == 0) {
		tmp = new ResourceNode(start, end);
		resources.insert(tmp);
		reserveList.push_back(tmp);
	}
	else {
		if (intersection[0]->start() > start) {
			tmp = new ResourceNode(start, intersection[0]->start() - 1);
			resources.insert(tmp);
			reserveList.push_back(tmp);
		}
		if (intersection.back()->end() < end) {
			tmp = new ResourceNode(intersection.back()->end() + 1, end);
			resources.insert(tmp);
			reserveList.push_back(tmp);
		}
		
		for (it = intersection.begin(), next = it + 1; next != intersection.end(); 
		  it = next, next++) {
			if ((*it)->end() != (*next)->start() - 1) {
				tmp = new ResourceNode((*it)->end() + 1, (*next)->start() - 1);
				resources.insert(tmp);
				reserveList.push_back(tmp);
			}
		}
	}
#endif

	/* This version creates one ResourceNode per LBID requested */

	/*
		Search for each LBID in the range
		if it exists, put the node in intersections
		else, put a new node in the reserve list
		continue...
	*/

	for (i = start; i <= end; i++) {
		ResourceNode rn(i);
		sit = resources.find(&rn);
		if (sit == resources.end()) {
			ResourceNode *tmp = new ResourceNode(i);
			resources.insert(tmp);
			reserveList.push_back(tmp);
		}
		else
			intersection.push_back(*sit);
	}

	// at this point, reserveList and intersection compose a contiguous range
	// including [start, end].  reserveList has newly created ranges, intersection
	// has the previously existing ones.
	
	// of the previously existing resources; if it's not already owned (existing in edge), 
	// then it's one this transaction has to wait on (new out edge). 
	// (the set class takes care of duplicates in the out edges)
	for (it = intersection.begin(); it != intersection.end(); it++) {
		if (txnNode->in.find(*it) == txnNode->in.end())
			txnNode->addOutEdge(*it);
	}
	
	// reserve the new resources
	for (it = reserveList.begin(); it != reserveList.end(); it++)
		txnNode->addInEdge(*it);

	// at this point, txnNode is adjacent to a set of ResourcesNodes s.t.
	// [start, end] is contained within it.
}


/* 
	0 = OK
   	1 = transaction node was not found on wake
	-1 = deadlock detected, transaction node destroyed and resources released

	mutex should be slavelock
*/
int LBIDResourceGraph::reserveRange(LBID_t start, LBID_t end, VER_t txn,
	boost::mutex &mutex)
{
	TransactionNode *txnNode;
	map<VER_t, TransactionNode *>::iterator it;

	/* 	
		look for existing transaction node T
			- make one if necessary
		connectResources();
		checkDeadlock();
		while (txnNode.out.size() > 0)
			block on T's condvar
			connectResources();
			checkDeadlock();
		}
	*/

	it = txns.find(txn);
	if (it == txns.end()) {
		txnNode = new TransactionNode(txn);
		txns[txn] = txnNode;
	}
	else
		txnNode = (*it).second;

	connectResources(start, end, txnNode);

	// "If txnNode is waiting on at least one LBID range..."
	while (txnNode->out.size() > 0) {
		// make sure there's no deadlock before blocking
		if (checkDeadlock(*txnNode)) {
// 			releaseResources(txn);
			return ERR_DEADLOCK;
		}
#ifdef BRM_VERBOSE
		cerr << " RG: sleeping transaction " << txn << endl;

		set<RGNode *>::iterator sit;
		cerr << " waiting on: " << endl;
		for (sit = txnNode->out.begin(); sit != txnNode->out.end(); sit++) {
			ResourceNode *rn = dynamic_cast<ResourceNode *>(*sit);
			cerr << hex << rn << dec << " " << rn->lbid() << endl;
		}
#endif

		txnNode->sleep(mutex);
#ifdef BRM_VERBOSE
		cerr << " RG: txn " << txn << " is awake" << endl;
#endif
		if (txnNode->dead()) {
			txns.erase(txn);
			delete txnNode;
			return ERR_KILLED;
		}	

		// attempt to grab remaining resources
		connectResources(start, end, txnNode);
	}

	// txn has all requested LBID ranges
	return ERR_OK;
}

void LBIDResourceGraph::releaseResources(VER_t txn)
{
	/*
		get transaction node
		get all inbound nodes
			detach them and wake all txns on the in-edges
			delete the resource nodes
		get all outbound nodes (this can happen if a rollback comes in while blocked)
			detach them
		if txnNode isn't sleeping,
			delete the transaction node
		else 
			mark it dead and wake it
	*/

	TransactionNode *txnNode;
	ResourceNode *rNode;
	map<VER_t, TransactionNode *>::iterator it;
	set<RGNode *>::iterator sit;
	set<RGNode *>::iterator dummy_sit;

	it = txns.find(txn);
	if (it == txns.end())
		return;

	txnNode = (*it).second;
	for (sit = txnNode->in.begin(); sit != txnNode->in.end(); ) {
		rNode = dynamic_cast<ResourceNode *>(*sit);
                dummy_sit = ++sit;
		rNode->wakeAndDetach();
		txnNode->removeInEdge(rNode);
		resources.erase(rNode);
		delete rNode;
                sit = dummy_sit;
	}
	for (sit = txnNode->out.begin(); sit != txnNode->out.end(); )
        {
		rNode = dynamic_cast<ResourceNode *>(*sit);
                dummy_sit = ++sit;
		txnNode->removeOutEdge(rNode);
                sit = dummy_sit;
        }

	if (txnNode->sleeping()) {
		txnNode->die();
		txnNode->wake();
	}
	else {
		txns.erase(txn);
		delete txnNode;
	}
}

void LBIDResourceGraph::releaseResource(LBID_t lbid)
{
	RNodes_t::iterator sit;
	TransactionNode *txnNode;

	for (sit = resources.begin(); sit != resources.end(); sit++)
		if (**sit == lbid)
			break;

	if (sit != resources.end()) {
		(*sit)->wakeAndDetach();

		//should only be one out edge of any resource node
		txnNode = dynamic_cast<TransactionNode *>(*(*sit)->out.begin());
		(*sit)->removeOutEdge(txnNode);
		resources.erase(*sit);
		delete *sit;
	}
}

bool LBIDResourceGraph::DFSStep(RGNode *curNode, uint64_t gray, uint64_t black) const
{
	set<RGNode *>::iterator it;

	if (curNode->color() == gray)
		return true;

	curNode->color(gray);
	for (it = curNode->out.begin(); it != curNode->out.end(); it++) {
		if ((*it)->color() != black)
			if (DFSStep(*it, gray, black))
				return true;
	}
	curNode->color(black);

	return false;
}

bool LBIDResourceGraph::checkDeadlock(TransactionNode &start)
{
	uint64_t gray = ++color, black = ++color;

	return DFSStep(&start, gray, black);
}

}
