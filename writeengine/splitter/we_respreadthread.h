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
 * we_respreadthread.h
 *
 *  Created on: Oct 18, 2011
 *      Author: bpaul
 */

#ifndef WE_RESPREADTHREAD_H_
#define WE_RESPREADTHREAD_H_

namespace WriteEngine
{

class WERespReadThread
{
public:
	WERespReadThread(WESDHandler& aSdh);
	WERespReadThread(const WERespReadThread& rhs);
	virtual ~WERespReadThread();

	void operator()();

private:
	WESDHandler& fSdh;

};

} /* namespace WriteEngine */
#endif /* WE_RESPREADTHREAD_H_ */
