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
 * $Id: resourcenode.h 1635 2012-08-03 18:57:10Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class XXX interface
 */

#ifndef RESOURCENODE_H_
#define RESOURCENODE_H_

#include "rgnode.h"

#if defined(_MSC_VER) && defined(xxxRESOURCENODE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace BRM {

class ResourceNode : public RGNode
{
	public:
		EXPORT ResourceNode();
		EXPORT ResourceNode(const ResourceNode &);
		EXPORT ResourceNode(LBID_t);
		EXPORT virtual ~ResourceNode();

		EXPORT ResourceNode& operator=(const ResourceNode &);
		EXPORT bool operator==(const ResourceNode &) const;
		EXPORT bool operator==(LBID_t) const;
		EXPORT bool operator<(const ResourceNode &) const;

		EXPORT void wakeAndDetach();

		EXPORT LBID_t lbid() const;
	private:
		LBID_t _lbid;
};

template<typename T>
struct RNLess {
	bool operator()(const T &x, const T &y) const
	{
		return *x < *y;
	}
};

struct RNHasher {
	size_t operator()(const ResourceNode *x) const
	{
		return x->lbid();
	}
};

struct RNEquals {
	bool operator()(const ResourceNode *x, const ResourceNode *y) const
	{
		return *x == *y;
	}
};

}

#undef EXPORT

#endif
