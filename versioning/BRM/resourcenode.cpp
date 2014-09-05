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
 * $Id: resourcenode.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#include "transactionnode.h"
#define RESOURCENODE_DLLEXPORT
#include "resourcenode.h"
#undef RESOURCENODE_DLLEXPORT

using namespace std;

namespace BRM
{

ResourceNode::ResourceNode() : _lbid(0)
{
}

ResourceNode::ResourceNode(const ResourceNode &n) : _lbid(n._lbid)
{
}

ResourceNode::ResourceNode(LBID_t s) : _lbid(s)
{
}

ResourceNode::~ResourceNode()
{
}

ResourceNode & ResourceNode::operator=(const ResourceNode &n)
{
	static_cast<RGNode &>(*this) = static_cast<const RGNode &>(n);
	_lbid = n._lbid;
	return *this;
}

bool ResourceNode::operator==(const ResourceNode &n) const
{
	return (_lbid == n._lbid);
}

bool ResourceNode::operator==(LBID_t l) const
{
	return (_lbid == l);
}

bool ResourceNode::operator<(const ResourceNode &n) const
{
	return (_lbid < n._lbid);
}

LBID_t ResourceNode::lbid() const
{
	return _lbid;
}

void ResourceNode::wakeAndDetach()
{
	TransactionNode *txn;
	set<RGNode *>::iterator sit;
	set<RGNode *>::iterator dummy_sit;

	for (sit = in.begin(); sit != in.end(); ) {
		txn = dynamic_cast<TransactionNode *>(*sit);
		txn->wake();
		dummy_sit = ++sit;
		removeInEdge(txn);
		sit = dummy_sit;
	}
	in.clear();
}

}

