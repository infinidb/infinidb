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
 * $Id: rgnode.cpp 1823 2013-01-21 14:13:09Z rdempsey $
 *
 ****************************************************************************/

#define RGNODE_DLLEXPORT
#include "rgnode.h"
#undef RGNODE_DLLEXPORT

using namespace std;

namespace BRM {

RGNode::RGNode() : _color(0)
{
}

RGNode::RGNode(const RGNode &n) : out(n.out), in(n.in), _color(n._color)
{
}

RGNode::~RGNode()
{
	set<RGNode *>::iterator it;

	for (it = in.begin(); it != in.end(); ) {
		(*it)->out.erase(this);
		in.erase(it++);
	}
	
	for (it = out.begin(); it != out.end(); ) {
		(*it)->in.erase(this);
		out.erase(it++);
	}

}

RGNode & RGNode::operator=(const RGNode &n)
{
	_color = n._color;
	in = n.in;
	out = n.out;
	return *this;
}

uint64_t RGNode::color() const
{
	return _color;
}

void RGNode::color(uint64_t c)
{
	_color = c;
}

void RGNode::addOutEdge(RGNode *n) 
{
	out.insert(n);
	n->in.insert(this);
}

void RGNode::addInEdge(RGNode *n)
{
	in.insert(n);
	n->out.insert(this);
}

void RGNode::removeOutEdge(RGNode *n) 
{
	out.erase(n);
	n->in.erase(this);
}

void RGNode::removeInEdge(RGNode *n)
{
	in.erase(n);
	n->out.erase(this);
}

}    // namespace
