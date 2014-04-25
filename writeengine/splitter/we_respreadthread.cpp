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

/*******************************************************************************
* $Id$
*
*******************************************************************************/

/*
 * we_respreadthread.cpp
 *
 *  Created on: Oct 18, 2011
 *      Author: bpaul
 */

#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "we_sdhandler.h"
#include "we_respreadthread.h"

namespace WriteEngine
{

WERespReadThread::WERespReadThread(WESDHandler& aSdh):
		fSdh(aSdh)
{
	// ctor
}
WERespReadThread::WERespReadThread(const WERespReadThread& rhs):
		fSdh(rhs.fSdh)
{
	// copy ctor
}
WERespReadThread::~WERespReadThread()
{
	// dtor
}

void WERespReadThread::operator()()
{
	//call datahandler checkForAllPmMsgs()
	fSdh.checkForRespMsgs();
}


} /* namespace WriteEngine */
