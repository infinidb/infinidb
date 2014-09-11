/*
 * FEMsgHandler.cpp
 *
 *  Created on: Mar 2, 2011
 *      Author: pleblanc
 */

#include "femsghandler.h"

using namespace std;
using namespace joblist;
using namespace messageqcpp;

namespace {

class Runner
{
public:
	Runner(FEMsgHandler *f) : target(f) { }
	void operator()() { target->threadFcn(); }
	FEMsgHandler *target;
};

}

FEMsgHandler::FEMsgHandler() : die(false), running(false), sawData(false), sock(NULL)
{
}

FEMsgHandler::FEMsgHandler(boost::shared_ptr<JobList> j, IOSocket *s) :
	die(false), running(false), sawData(false), jl(j)
{
	sock = s;
	assert(sock);
}

FEMsgHandler::~FEMsgHandler()
{
	stop();
	thr.join();
}

void FEMsgHandler::start()
{
	if (!running) {
		running = true;
		thr = boost::thread(Runner(this));
	}
}

void FEMsgHandler::stop()
{
	die = true;
	jl.reset();
}

void FEMsgHandler::setJobList(boost::shared_ptr<JobList> j)
{
	jl = j;
}

void FEMsgHandler::setSocket(IOSocket *i)
{
	sock = i;
	assert(sock);
}

/* Note, the next two fcns strongly depend on ExeMgr's current implementation.  There's a
 * good chance that if ExeMgr's table send loop is changed, these will need to be
 * updated to match.
 */

/* This is currently only called if InetStreamSocket::write() throws, implying
 * a connection error.  It might not make sense in other contexts.
 */
bool FEMsgHandler::aborted()
{
	if (sawData)
		return true;

	boost::mutex::scoped_lock sl(mutex);
	int err;
	int connectionNum = sock->getConnectionNum();

	err = InetStreamSocket::pollConnection(connectionNum, 1000);
	if (err == 1) {
		sawData = true;
		return true;
	}
	return false;
}

void FEMsgHandler::threadFcn()
{
	int err = 0;
	int connectionNum = sock->getConnectionNum();

	/* This waits for the next readable event on sock.  An abort is signaled
	 * by sending something (anything at the moment), then dropping the connection.
	 * This fcn exits on all other events.
	 */
	while (!die && err == 0) {
		boost::mutex::scoped_lock sl(mutex);
		err = InetStreamSocket::pollConnection(connectionNum, 1000);
	}

	if (err == 1)
		sawData = true;   // there's data to read, must be the abort signal
	if (!die && (err == 2 || err == 1)) {
		die = true;
		jl->abort();
		jl.reset();
	}

	running = false;
}

