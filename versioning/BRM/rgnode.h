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
 * $Id: rgnode.h 1823 2013-01-21 14:13:09Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class RGNode interface.  This is a base class for the nodes in the resource graph.
 */

#ifndef RGNODE_H_
#define RGNODE_H_

#include <set>

#include "brmtypes.h"

#if defined(_MSC_VER) && defined(xxxRGNODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

class RGNode;

class RGNode {
	public:
		EXPORT RGNode();
		EXPORT RGNode(const RGNode &);
		EXPORT virtual ~RGNode();

		EXPORT RGNode& operator=(const RGNode &);

		EXPORT uint64_t color() const;
		EXPORT void color(uint64_t);
		EXPORT void addOutEdge(RGNode *);
		EXPORT void addInEdge(RGNode *);
		EXPORT void removeOutEdge(RGNode *);
		EXPORT void removeInEdge(RGNode *);

		friend class LBIDResourceGraph;

	protected:
		// adjacency lists.  Technically these should be private, but ResourceNode
		// currently uses them to wake connected transactions.  TBD...
		std::set<RGNode *> out;
		std::set<RGNode *> in;

	private:
		uint64_t _color;
};

}

#undef EXPORT

#endif
