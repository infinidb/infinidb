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

// $Id: jobstep.cpp 7396 2011-02-03 17:54:36Z rdempsey $
#include <iostream>
#include <string>
#include <boost/thread.hpp>
using namespace std;

#include "jobstep.h"
#include "messagelog.h"
#include "messageids.h"
#include "timestamp.h"
using namespace logging;

namespace joblist
{
boost::mutex JobStep::mutex; //=PTHREAD_MUTEX_INITIALIZER;

void JobStep::intToStr( DataList_t& inList, StringBucketDataList& outList )
{
	int it = -1;
	bool more;
	ElementType e;
	int64_t token;
	char buffer[21]; // 19 digits in a 64-bit number + sign + nul
	int n = 0;
	try{
		it = inList.getIterator();
	}catch(exception& ex) {
		cerr << "JobStep::intToStr: caught exception: "
			<< ex.what() << endl;
		throw;
	}catch(...) {
		cerr << "JobStep::intToStr: caught exception" << endl;
		throw;
	}
	
	more = inList.next(it, &e);
	while (more)
	{
		token = e.second;
		n = snprintf(buffer, 21,
#if __LP64__
			"%ld",
#else
			"%lld",
#endif
			token);
		outList.insert(StringElementType(e.first, buffer));
		more = inList.next(it, &e);
	}
	
	return;
}
	
ostream& operator<<(ostream& os, const JobStep* rhs)
{
	os << rhs->toString();
	return os;
}

}

using namespace joblist;

//------------------------------------------------------------------------------
// Log a syslog msg for the start of this specified job step
//------------------------------------------------------------------------------
void JobStep::syslogStartStep (
		uint32_t subSystem,
        const string& stepName) const
{
	LoggingID  logId  ( subSystem, sessionId(), txnId() );
	MessageLog msgLog ( logId );

	Message msgStartStep ( M0030 );
	Message::Args args;
	args.add   ( (uint64_t)statementId() ); // statement id for this job step
	args.add   ( (int)stepId()           ); // step id for this job step
	args.add   ( stepName      );           // step name for this job step
	msgStartStep.format ( args );
	msgLog.logDebugMessage ( msgStartStep );
}

//------------------------------------------------------------------------------
// Log a syslog message for the end of this specified job step
//------------------------------------------------------------------------------
void JobStep::syslogEndStep (
		uint32_t subSystem,
		uint64_t blockedDLInput,
		uint64_t blockedDLOutput,
		uint64_t msgBytesInput,
		uint64_t msgBytesOutput) const
{
	LoggingID  logId  ( subSystem, sessionId(), txnId() );
	MessageLog msgLog ( logId );

	Message msgEndStep ( M0031 );
	Message::Args args;
	args.add   ( (uint64_t)statementId() ); // statement id for this job step
	args.add   ( (int)stepId()           ); // step id for this job step
	args.add   ( blockedDLInput          ); // blocked datalist input (ex: fifo)
	args.add   ( blockedDLOutput         ); // blocked datalist output(ex: fifo)
	args.add   ( msgBytesInput           ); // incoming msg byte count
	args.add   ( msgBytesOutput          ); // outgoing msg byte count
	msgEndStep.format ( args );
	msgLog.logDebugMessage ( msgEndStep );
}

//------------------------------------------------------------------------------
// Log a syslog message for the physical vs cache block I/O counts
//------------------------------------------------------------------------------
void JobStep::syslogReadBlockCounts (
		uint32_t subSystem,
		uint64_t physicalReadCount,
		uint64_t cacheReadCount,
		uint64_t casualPartBlocks) const
{
	LoggingID  logId  ( subSystem, sessionId(), txnId() );
	MessageLog msgLog ( logId );

	Message msgEndStep ( M0032 );
	Message::Args args;
	args.add   ( (uint64_t)statementId() ); // statement id for this job step
	args.add   ( (int)stepId()           ); // step id for this job step
	args.add   ( (int)oid()              ); // step id for this job step
	args.add   ( physicalReadCount       ); // blocked datalist input (ex: fifo)
	args.add   ( cacheReadCount          ); // blocked datalist output(ex: fifo)
	args.add   ( casualPartBlocks        ); // casual partition block hits
	msgEndStep.format ( args );
	msgLog.logDebugMessage ( msgEndStep );
}

//------------------------------------------------------------------------------
// Log a syslog msg for the effective start/end times for this step
// (lastWriteTime denotes when the EndOfInput marker was written out).
//------------------------------------------------------------------------------
void JobStep::syslogProcessingTimes (
		uint32_t subSystem,
		const struct timeval&   firstReadTime,
		const struct timeval&   lastReadTime,
		const struct timeval&   firstWriteTime,
		const struct timeval&   lastWriteTime) const
{
	LoggingID  logId  ( subSystem, sessionId(), txnId() );
	MessageLog msgLog ( logId );
	Message msgStartStep ( M0046 );
	Message::Args args;

	args.add   ( (uint64_t)statementId() ); // statement id for this job step
	args.add   ( (int)stepId()           ); // step id for this job step
	args.add   ( JSTimeStamp::format(firstReadTime) ); // when first DL input element read
	args.add   ( JSTimeStamp::format(lastReadTime) ); // when last DL input element read
	args.add   ( JSTimeStamp::format(firstWriteTime));// when first DL output elem written
	args.add   ( JSTimeStamp::format(lastWriteTime)); // when EndOfInput written to DL out
	msgStartStep.format ( args );
	msgLog.logDebugMessage ( msgStartStep );
}

void JobStep::abort()
{
	die = true;
}
