/* Copyright (C) 2013 Calpont Corp.

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
 * we_cpifeederthread.h
 *
 *  Created on: Mar 29, 2012
 *      Author: bpaul
 */

#ifndef WE_CPIFEEDERTHREAD_H_
#define WE_CPIFEEDERTHREAD_H_

#include <queue>

namespace WriteEngine
{

class WEDataLoader;
class WECpiFeederThread;

class WECpiFeederRunner
{
public:
	WECpiFeederRunner(WECpiFeederThread& Ref): fOwner(Ref){ /* ctor */ }
	virtual ~WECpiFeederRunner(){/* dtor */}
	void operator()();

public:
	WECpiFeederThread& fOwner;
};


class WECpiFeederThread
{
public:
	WECpiFeederThread(WEDataLoader& Ref);
	virtual ~WECpiFeederThread();

public:
	void startFeederThread();
	void add2MsgQueue(messageqcpp::ByteStream& Ibs);
	void feedData2Cpi();
	void stopThread();
	bool isMsgQueueEmpty();
	//bool isPushing() { return fPushing; }
	bool isStopped() { return fStopped; }
	int getQueueSize() { return fMsgQueue.size(); }
	bool isContinue();
private:

    WEDataLoader& fOwner;

	boost::condition fFeederCond;
	boost::mutex fMsgQMutex;
    typedef std::queue<messageqcpp::SBS> WEMsgQueue;
    WEMsgQueue fMsgQueue;

    boost::thread *fpThread;
	bool fContinue;
	boost::mutex fContMutex;
	//bool fPushing;
	bool fStopped;


    friend class WEDataLoader;
};

} /* namespace WriteEngine */

#endif /* WE_CPIFEEDERTHREAD_H_ */
