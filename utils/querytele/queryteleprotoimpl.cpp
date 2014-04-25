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

#include <queue>
using namespace std;

#define BOOST_DISABLE_ASSERTS
#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>

#include "thrift/transport/TSocket.h"
#include "thrift/transport/TBufferTransports.h"
namespace att=apache::thrift::transport;

#include "thrift/protocol/TBinaryProtocol.h"
namespace atp=apache::thrift::protocol;

#include "atomicops.h"

#include "queryteleserverparms.h"
#include "querytele_types.h"
#include "QueryTeleService.h"

#include "queryteleprotoimpl.h"

namespace
{
const size_t MaxQueueElems=1000;

template <class T>
struct TsTeleQueue
{
	typedef std::queue<T> TeleQueue;

	TeleQueue queue;
	boost::mutex queueMtx;
};

TsTeleQueue<querytele::StepTele> stQueue;
TsTeleQueue<querytele::QueryTele> qtQueue;
TsTeleQueue<querytele::ImportTele> itQueue;

volatile bool isInited=false;
boost::mutex initMux;

boost::shared_ptr<att::TSocket> fSocket;
boost::shared_ptr<att::TBufferedTransport> fTransport;
boost::shared_ptr<atp::TBinaryProtocol> fProtocol;

void TeleConsumer()
{
	bool didSomeWork=false;
	boost::mutex::scoped_lock itlk(itQueue.queueMtx, boost::defer_lock);
	boost::mutex::scoped_lock qtlk(qtQueue.queueMtx, boost::defer_lock);
	boost::mutex::scoped_lock stlk(stQueue.queueMtx, boost::defer_lock);
	querytele::QueryTeleServiceClient client(fProtocol);

	try {
		for (;;)
		{
			didSomeWork=false;

			itlk.lock();
			// Empty the import queue first...
			while (!itQueue.queue.empty())
			{
				querytele::ImportTele itdata = itQueue.queue.front();
				itQueue.queue.pop();
				itlk.unlock();
				try {
					fTransport->open();
					client.postImport(itdata);
					fTransport->close();
				} catch (...) {
					try {
						fTransport->close();
					} catch (...) {
					}
				}
				didSomeWork = true;
				itlk.lock();
			}
			itlk.unlock();

			qtlk.lock();
			// Now empty the query queue...
			while (!qtQueue.queue.empty())
			{
				querytele::QueryTele qtdata = qtQueue.queue.front();
				qtQueue.queue.pop();
				qtlk.unlock();
				try {
					fTransport->open();
					client.postQuery(qtdata);
					fTransport->close();
				} catch (...) {
					try {
						fTransport->close();
					} catch (...) {
					}
				}
				didSomeWork = true;
				qtlk.lock();
			}
			qtlk.unlock();

			stlk.lock();
			// Finally empty the step queue...
			while (!stQueue.queue.empty())
			{
				querytele::StepTele stdata = stQueue.queue.front();
				stQueue.queue.pop();
				stlk.unlock();
				try {
					fTransport->open();
					client.postStep(stdata);
					fTransport->close();
				} catch (...) {
					try {
						fTransport->close();
					} catch (...) {
					}
				}
				didSomeWork = true;
				stlk.lock();
			}
			stlk.unlock();

			if (!didSomeWork)
			{
				usleep(100000);
			}
		}
	} catch (...) {
		//we're probably shutting down, just let this thread die quietly...
	}
}

boost::thread* consThd;

}

namespace querytele
{

QueryTeleProtoImpl::QueryTeleProtoImpl(const QueryTeleServerParms& sp) :
	fServerParms(sp)
{
	if (fServerParms.host.empty() || fServerParms.port == 0) return;

	boost::mutex::scoped_lock lk(initMux);

	atomicops::atomicMb();
	if (isInited) return;

	fSocket.reset(new att::TSocket(fServerParms.host, fServerParms.port));
	fTransport.reset(new att::TBufferedTransport(fSocket));
	fProtocol.reset(new atp::TBinaryProtocol(fTransport));

	consThd = new boost::thread(&TeleConsumer);

	atomicops::atomicMb();
	isInited = true;
}

int QueryTeleProtoImpl::enqStepTele(const StepTele& stdata)
{
	try {
		boost::mutex::scoped_lock lk(stQueue.queueMtx);
		if (stQueue.queue.size() >= MaxQueueElems)
			return -1;
		stQueue.queue.push(stdata);
	} catch (...) {
		return -2;
	}

	return 0;
}

int QueryTeleProtoImpl::enqQueryTele(const QueryTele& qtdata)
{
	try {
		boost::mutex::scoped_lock lk(qtQueue.queueMtx);
		if (qtQueue.queue.size() >= MaxQueueElems)
			return -1;
		qtQueue.queue.push(qtdata);
	} catch (...) {
		return -2;
	}

	return 0;
}

int QueryTeleProtoImpl::enqImportTele(const ImportTele& itdata)
{
	try {
		boost::mutex::scoped_lock lk(itQueue.queueMtx);
		if (itQueue.queue.size() >= MaxQueueElems)
			return -1;
		itQueue.queue.push(itdata);
	} catch (...) {
		return -2;
	}

	return 0;
}

} //namespace querytele

