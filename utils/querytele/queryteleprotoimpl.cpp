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
#include <sstream>
#include <fstream>
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

querytele::StepTele gLastStep;

struct QStats
{
    int qtqueuedrops;
    int stqueuedrops;
    int stqueuedups;
    int itqueuedrops;
    QStats() : 
		qtqueuedrops(0), 
		stqueuedrops(0), 
		stqueuedups(0),
		itqueuedrops(0) { ; }
};

QStats fQStats;

string get_trace_file()
{
    ostringstream oss;
    pid_t pid = getpid();
#ifdef _MSC_VER
    DWORD threadid = GetCurrentThreadId();
#else
    pthread_t threadid = pthread_self();
#endif
    oss << "/tmp/qt-consumer-" << pid << "-" << threadid;

    return oss.str();
}

void log_query(const querytele::QueryTele& qtdata)
{
	ofstream trace(get_trace_file().c_str(), ios::out | ios::app);
	trace << "Query,"
		  <<  qtdata.query_uuid << ","
	      << ","; // skip step uuid
	if (qtdata.msg_type == querytele::QTType::QT_SUMMARY)
		trace << "SUMMARY,";
	else if (qtdata.msg_type == querytele::QTType::QT_START)
		trace << "START,";
	else
		trace << "PROGRESS,";

	trace << ","; // sktp step type

	trace << qtdata.start_time << ",";
	trace << qtdata.end_time << ",";

	trace << qtdata.cache_io << ",";
	trace << qtdata.msg_rcv_cnt << ",";
	trace << qtdata.rows << ",";
	trace << qtdata.max_mem_pct << ",";

	trace << qtdata.query_type << ",";
	trace << qtdata.schema_name << ",";
	trace << qtdata.query << ",";
	trace << qtdata.system_name;
	trace << endl;
	trace.close();
}

const string st2str(enum querytele::StepType::type t)
{       
    switch (t)
    {
    case querytele::StepType::T_HJS: return "HJS";
    case querytele::StepType::T_DSS: return "DSS";
    case querytele::StepType::T_CES: return "CES";
    case querytele::StepType::T_SQS: return "SQS";
    case querytele::StepType::T_TAS: return "TAS";
    case querytele::StepType::T_TNS: return "TNS";
    case querytele::StepType::T_BPS: return "BPS";
    case querytele::StepType::T_TCS: return "TCS";
    case querytele::StepType::T_HVS: return "HVS";
    case querytele::StepType::T_WFS: return "WFS";
    case querytele::StepType::T_SAS: return "SAS";
    case querytele::StepType::T_TUN: return "TUN";
    default: return "INV";
    }
    return "INV";
}

void log_step(const querytele::StepTele& stdata)
{
	ofstream trace(get_trace_file().c_str(), ios::out | ios::app);

	trace << "Step,"
		  << stdata.query_uuid << ","
		  << stdata.step_uuid << ",";
	if (stdata.msg_type == querytele::STType::ST_SUMMARY)
		trace << "SUMMARY,";
	else if (stdata.msg_type == querytele::STType::ST_START)
		trace << "START,";
	else
		trace << "PROGRESS,";
	trace << st2str(stdata.step_type) << ",";

	trace << stdata.start_time << ",";
	trace << stdata.end_time << ",";

	trace << stdata.cache_io << ",";
	trace << stdata.msg_rcv_cnt << ",";
	trace << stdata.rows << ",";

	if (stdata.total_units_of_work > 0)
		trace << stdata.units_of_work_completed*100/stdata.total_units_of_work << ",";
	else
		trace << "-1,";

	trace << ",,,"; // skip qtype, schemo, etc.
	trace << fQStats.stqueuedrops << "," << fQStats.stqueuedups << "," << stQueue.queue.size();
    trace << endl;
	trace.close();
}

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
#ifdef QUERY_TELE_DEBUG
					log_query(qtdata);
#endif
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
			// @bug6088 - Added check for query queue and import queue in while statment below to
			//            keep the step logs from starving the query and import logs.
			while (!stQueue.queue.empty() && qtQueue.queue.empty() && itQueue.queue.empty())
			{
				querytele::StepTele stdata = stQueue.queue.front();
				stQueue.queue.pop();
				stlk.unlock();
				try {
					fTransport->open();
#ifdef QUERY_TELE_DEBUG
					log_step(stdata);
#endif
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
				usleep(50000);
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
		// @bug6088 - Added conditions below to always log progress SUMMARY and START messages to avoid completed queries showing up with progress 0
		//            and no steps.
		if (stQueue.queue.size() >= MaxQueueElems && stdata.msg_type != querytele::STType::ST_SUMMARY && stdata.msg_type != querytele::STType::ST_START)
        { 
            fQStats.stqueuedrops++;    
			return -1;
        }
        if( stdata.step_uuid != gLastStep.step_uuid ||
            stdata.msg_type != gLastStep.msg_type ||
            stdata.step_type != gLastStep.step_type ||
            stdata.total_units_of_work != gLastStep.total_units_of_work ||
            stdata.units_of_work_completed != gLastStep.units_of_work_completed )
        {
			stQueue.queue.push(stdata);
        	gLastStep = stdata;
    	}
		else
		{
			fQStats.stqueuedups++;
		}
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
		{
			fQStats.qtqueuedrops++;
			return -1;
		}
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
		{
			fQStats.itqueuedrops++;
			return -1;
		}
		itQueue.queue.push(itdata);
	} catch (...) {
		return -2;
	}

	return 0;
}

int QueryTeleProtoImpl::waitForQueues()
{
	try {
		boost::mutex::scoped_lock lk(itQueue.queueMtx);
		while (!itQueue.queue.empty())
		{
			lk.unlock();
			usleep(100000);
			lk.lock();
		}
	} catch (...) {
		return -1;
	}

	return 0;
}

} //namespace querytele

