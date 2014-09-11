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

/***********************************************************************
*   $Id$
*
*
***********************************************************************/

#include <unistd.h>
#include <stdexcept>
#include "bppsendthread.h"

using namespace std;
using namespace boost;

#if defined(_MSC_VER) && !defined(_WIN64)
#  ifndef InterlockedAdd
#    define InterlockedAdd64 InterlockedAdd
#    define InterlockedAdd(x, y) ((x) + (y))
#  endif
#endif

namespace primitiveprocessor
{
	
extern uint connectionsPerUM;
	
BPPSendThread::BPPSendThread() : die(false), gotException(false), mainThreadWaiting(false),
	sizeThreshold(100), msgsLeft(-1), waiting(false), sawAllConnections(false),
	fcEnabled(false), currentByteSize(0), maxByteSize(25000000)
{
	runner = boost::thread(Runner_t(this));
}	
	
BPPSendThread::BPPSendThread(uint initMsgsLeft) : die(false), gotException(false),
	mainThreadWaiting(false), sizeThreshold(100), msgsLeft(initMsgsLeft), waiting(false),
	sawAllConnections(false), fcEnabled(false), currentByteSize(0), maxByteSize(25000000)
{
	runner = boost::thread(Runner_t(this));
}

BPPSendThread::~BPPSendThread()
{
	mutex::scoped_lock sl(msgQueueLock);
	mutex::scoped_lock sl2(ackLock);
	die = true;
	queueNotEmpty.notify_one();
	okToSend.notify_one();
	sl.unlock();
	sl2.unlock();
	runner.join();
}

bool BPPSendThread::okToProceed()
{
	// keep the queue size below the 100 msg threshold & below the 25MB mark,
	// but at least 2 msgs so there is always 1 ready to be sent.
	return ((msgQueue.size() < sizeThreshold && currentByteSize < maxByteSize)
		|| msgQueue.size() < 3) && !die;
}

void BPPSendThread::sendResult(const Msg_t &msg, bool newConnection)
{
	if (die)
		return;
	mutex::scoped_lock sl(msgQueueLock);
	if (gotException)
		throw runtime_error(exceptionString);
#ifdef _MSC_VER
	InterlockedAdd64(&currentByteSize, msg.msg->lengthWithHdrOverhead());
#else		
	__sync_add_and_fetch(&currentByteSize, msg.msg->lengthWithHdrOverhead());
#endif
	msgQueue.push(msg);
	if (!sawAllConnections && newConnection) {
		Connection_t ins(msg.sockLock, msg.sock);
		bool inserted = connections_s.insert(ins).second;
		if (inserted) {
			connections_v.push_back(ins);
			if (connections_v.size() == connectionsPerUM) {
				connections_s.clear();
				sawAllConnections = true;
			}
		}
	}
	if (mainThreadWaiting)
		queueNotEmpty.notify_one();
}

void BPPSendThread::sendResults(const vector<Msg_t> &msgs, bool newConnection)
{
	if (die)
		return;
	mutex::scoped_lock sl(msgQueueLock);
	if (gotException)
		throw runtime_error(exceptionString);
	if (!sawAllConnections && newConnection) {
		assert(msgs.size() > 0);
		Connection_t ins(msgs[0].sockLock, msgs[0].sock);
		bool inserted = connections_s.insert(ins).second;
		if (inserted) {
			connections_v.push_back(ins);
			if (connections_v.size() == connectionsPerUM) {
				connections_s.clear();
				sawAllConnections = true;
			}
		}
	}
	for (uint i = 0; i < msgs.size(); i++) {
#ifdef _MSC_VER
		InterlockedAdd64(&currentByteSize, msgs[i].msg->lengthWithHdrOverhead());
#else
		__sync_add_and_fetch(&currentByteSize, msgs[i].msg->lengthWithHdrOverhead());
#endif
		msgQueue.push(msgs[i]);
	}
	if (mainThreadWaiting)
		queueNotEmpty.notify_one();
}

void BPPSendThread::sendMore(int num)
{
	mutex::scoped_lock sl(ackLock);
//	cout << "got an ACK for " << num << " msgsLeft=" << msgsLeft << endl;
	if (num == -1)
		fcEnabled = false;
	else if (num == 0) {
		fcEnabled = true;
		msgsLeft = 0;
	}
	else
#ifdef _MSC_VER
		InterlockedAdd(&msgsLeft, num);
#else
		__sync_add_and_fetch(&msgsLeft, num);
#endif
	if (waiting)
		okToSend.notify_one();
}

bool BPPSendThread::flowControlEnabled()
{
	return fcEnabled;
}

void BPPSendThread::mainLoop()
{
	const uint msgCap = 20;
	boost::scoped_array<Msg_t> msg;
	uint msgCount = 0, i, msgsSent;
	SP_UM_MUTEX lock;
	SP_UM_IOSOCK sock;
	bool doLoadBalancing = false;
	
	msg.reset(new Msg_t[msgCap]);

	while (!die) {
		mutex::scoped_lock sl(msgQueueLock);
		if (msgQueue.empty() && !die) {
			mainThreadWaiting = true;
			queueNotEmpty.wait(sl);
			mainThreadWaiting = false;
			continue;
		}
	
		msgCount = (msgQueue.size() > msgCap ? msgCap : msgQueue.size());
		for (i = 0; i < msgCount; i++) {
			msg[i] = msgQueue.front();
			msgQueue.pop();
		}
		doLoadBalancing = sawAllConnections;
		sl.unlock();

		/* In the send loop below, msgsSent tracks progress on sending the msg array,
		 * i how many msgs are sent by 1 run of the loop, limited by msgCount or msgsLeft. */
		msgsSent = 0;
		while (msgsSent < msgCount && !die) {
			uint64_t bsSize;
			if (msgsLeft == 0 && fcEnabled && !die) {
				mutex::scoped_lock sl2(ackLock);
				while (msgsLeft == 0 && fcEnabled && !die) {
					waiting = true;
					okToSend.wait(sl2);
					waiting = false;
				}
			}
			for (i = 0; msgsSent < msgCount && ((fcEnabled && msgsLeft > 0) || !fcEnabled) && !die;
			  msgsSent++, i++) {
				if (doLoadBalancing) {
					// Bug 4475 move control of sockIndex to batchPrimitiveProcessor
					lock = connections_v[msg[msgsSent].sockIndex].sockLock;
					sock = connections_v[msg[msgsSent].sockIndex].sock;
				}
				else {
					lock = msg[msgsSent].sockLock;
					sock = msg[msgsSent].sock;
				}
				bsSize = msg[msgsSent].msg->lengthWithHdrOverhead();
				try {
					mutex::scoped_lock sl2(*lock);
					sock->write(*msg[msgsSent].msg);
				}
				catch (std::exception &e) {
					sl.lock();
					exceptionString = e.what();
					gotException = true;
					return;
				}
#ifdef _MSC_VER
				InterlockedAdd(&msgsLeft, -1);
				//FIXME: is bsSize always small enough to fit into a signed var?
				int64_t sbsSize = static_cast<int64_t>(bsSize);
				InterlockedAdd64(&currentByteSize, -sbsSize);
#else
				__sync_sub_and_fetch(&msgsLeft, 1);
				__sync_sub_and_fetch(&currentByteSize, bsSize);
#endif			
				msg[msgsSent].msg.reset();
			}
		}
	}
}

void BPPSendThread::abort()
{
	mutex::scoped_lock sl(msgQueueLock);
	mutex::scoped_lock sl2(ackLock);
	die = true;
	queueNotEmpty.notify_one();
	okToSend.notify_one();
	sl.unlock();
	sl2.unlock();
}

bool BPPSendThread::aborted()
{
	return die;
}

}

