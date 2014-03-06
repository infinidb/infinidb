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

// $Id: tdriver-dec.cpp 9210 2013-01-21 14:10:42Z rdempsey $
#include <iostream>
#include <cassert>
#include <stdexcept>
using namespace std;

#include <boost/thread.hpp>
using namespace boost;

#include "primitivemsg.h"

#include "bytestream.h"
using namespace messageqcpp;

#include "distributedenginecomm.h"
using namespace joblist;

class TestDistributedEngineComm
{
public:
	TestDistributedEngineComm(DistributedEngineComm* dec) : fDec(dec) { }
	void addDataToOutput(const ByteStream& bs) { fDec->addDataToOutput(bs); }

private:
	DistributedEngineComm* fDec;
};

namespace
{

const ByteStream buildBs(Int16 sessionId, Int16 stepId)
{
	uint32_t len = sizeof(ISMPacketHeader)+2*sizeof(PrimitiveHeader);
	ByteStream::byte* bpr = new ByteStream::byte[len];

	ISMPacketHeader *hdr = reinterpret_cast<ISMPacketHeader*>(bpr);
	PrimitiveHeader* p = reinterpret_cast<PrimitiveHeader*>(hdr+1);

	p->SessionID = sessionId;
	p->StepID = stepId;

	ByteStream bs(bpr, len);
	delete [] bpr;
	return bs;
}

void readBs(const ByteStream& bs, Int16& sessionId, Int16& stepId)
{
	const ISMPacketHeader* hdr = reinterpret_cast<const ISMPacketHeader*>(bs.buf());
	const PrimitiveHeader* p = reinterpret_cast<const PrimitiveHeader*>(hdr+1);
	sessionId = p->SessionID;
	stepId = p->StepID;
	return;
}

class ThdFun1
{
public:
	ThdFun1(DistributedEngineComm* dec, int sessionId, int stepId) :
		fDec(dec), fSessionId(sessionId), fStepId(stepId) { }
	void operator()()
	{
		ByteStream bs = fDec->read(fSessionId, fStepId);
		idbassert(bs.length() == 0);
		return;
	}
private:
	DistributedEngineComm* fDec;
	int fSessionId;
	int fStepId;
};

}

int main(int argc, char** argv)
{
	int leakCheck = 0;
	if (argc > 1 && strcmp(argv[1], "--leakcheck") == 0) leakCheck = 1;

	DistributedEngineComm* dec;

	dec = DistributedEngineComm::instance("./config-dec.xml");

	dec->addSession(12345);
	dec->addStep(12345, 0);
	dec->addStep(12345, 1);
	dec->addStep(12345, 3);
	dec->addStep(12345, 10);

	TestDistributedEngineComm tdec(dec);
	ByteStream bs;

	tdec.addDataToOutput(buildBs(12345, 0));
	tdec.addDataToOutput(buildBs(12345, 1));
	tdec.addDataToOutput(buildBs(12345, 3));
	tdec.addDataToOutput(buildBs(12345, 10));

	Int16 sessionId, stepId;
	bs = dec->read(12345, 10);
	readBs(bs, sessionId, stepId);
	idbassert(sessionId == 12345);
	idbassert(stepId == 10);

	bs = dec->read(12345, 1);
	readBs(bs, sessionId, stepId);
	idbassert(sessionId == 12345);
	idbassert(stepId == 1);

	bs = dec->read(12345, 0);
	readBs(bs, sessionId, stepId);
	idbassert(sessionId == 12345);
	idbassert(stepId == 0);

	bs = dec->read(12345, 3);
	readBs(bs, sessionId, stepId);
	idbassert(sessionId == 12345);
	idbassert(stepId == 3);

	unsigned i;
	bs = buildBs(12345, 1);
	// 1M seems a bit too much for a dev box
	// 500K is about the max
	const unsigned loopMax = 200000 / (leakCheck * 99 + 1);
	for (i = 0; i < loopMax; i++)
	{
		tdec.addDataToOutput(bs);
	}

	for (i = 0; i < loopMax; i++)
	{
		bs = dec->read(12345, 1);
		readBs(bs, sessionId, stepId);
		idbassert(sessionId == 12345);
		idbassert(stepId == 1);
	}

	unsigned throws;
	throws = 0;
	for (i = 0; i < loopMax; i++)
	{
		bs = buildBs(12345, (i % 10));
		//some of these shoud throw since there's only a few steps added
		try
		{
			tdec.addDataToOutput(bs);
		}
		catch (runtime_error& re)
		{
			throws++;
			continue;
		}
	}
	idbassert(throws > 0);

	throws = 0;
	for (i = 0; i < loopMax; i++)
	{
		//some of these shoud throw since there's only a few steps added
		try
		{
			bs = dec->read(12345, (i % 10));
		}
		catch (runtime_error& re)
		{
			throws++;
			continue;
		}
		readBs(bs, sessionId, stepId);
		idbassert(sessionId == 12345);
		idbassert(stepId == (i % 10));
	}
	idbassert(throws > 0);

	ThdFun1 fun1(dec, 12345, 1);
	thread thd1(fun1);

	dec->removeSession(12345);

	thd1.join();

	//delete dec;

	return 0;
}

