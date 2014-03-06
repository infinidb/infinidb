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

/*
 * $Id: primitivemsg.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 */

#include <stdexcept>
using namespace std;

#include "calpontselectexecutionplan.h"
using namespace execplan;

#include "primitivemsg.h"
#include "primitivestep.h"
using namespace joblist;

namespace joblist
{
void PrimitiveMsg::send()
{
	throw logic_error("somehow ended up in PrimitiveMsg::send()!");
}

void PrimitiveMsg::buildPrimitiveMessage(ISMPACKETCOMMAND, void*, void*)
{
	throw logic_error("somehow ended up in PrimitiveMsg::buildPrimitiveMessage()!");
}

void PrimitiveMsg::receive()
{
	throw logic_error("somehow ended up in PrimitiveMsg::receive()!");
}

void PrimitiveMsg::sendPrimitiveMessages()
{
	throw logic_error("somehow ended up in PrimitiveMsg::sendPrimitiveMessages()!");
}

void PrimitiveMsg::receivePrimitiveMessages()
{
	throw logic_error("somehow ended up in PrimitiveMsg::receivePrimitiveMessages()!");
}

// Unfortuneately we have 32 bits in the execplan flags, but only 16 that can be sent to
//  PrimProc, so we have to convert them (throwing some away).
uint16_t PrimitiveMsg::planFlagsToPrimFlags(uint32_t planFlags)
{
	uint16_t flags = 0;

	if (planFlags & CalpontSelectExecutionPlan::TRACE_LBIDS)
		flags |= PF_LBID_TRACE;

	if (planFlags & CalpontSelectExecutionPlan::PM_PROFILE)
		flags |= PF_PM_PROF;
	return flags;
}

}

