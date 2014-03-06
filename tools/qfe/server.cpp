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
	Protocol definition:
*/

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <iphlpapi.h>
#include <icmpapi.h>
#include <stdio.h>
#else
#include <poll.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#endif
#include <iostream>
#include <string>
#include <stdexcept>
#include <sstream>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <signal.h>
#include <cerrno>
using namespace std;

#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/tokenizer.hpp>
#include <boost/algorithm/string.hpp>
using namespace boost;

#ifndef _MSC_VER
#include "config.h"
#endif

#include "socktype.h"
#include "parsequery.h"
#include "sendcsep.h"
#include "returnedrows.h"
#include "socketio.h"
#include "ddlstmts.h"

#include "exceptclasses.h" //brings in idbassert_s macro

#include "messagequeue.h"
using namespace messageqcpp;

#include "atomicops.h"

namespace execplan
{
class CalpontSelectExecutionPlan;
}

#define SINGLE_THREADED

namespace qfe
{
string DefaultSchema;
}

namespace
{
using namespace qfe;

enum StmtType
{
	UNKNOWN,
	QUERY,
	CREATE,
	DROP,
	SHOW,
};

volatile uint32_t SystemSID;

void log(const string &s) 
{
	cerr << s << endl;
}

struct QueryMessage
{
	QueryMessage() : isValid(false) { }
	~QueryMessage() { }

	string toString() const;

	bool isValid;
	string queryText;
	string defaultSchema;
};

string QueryMessage::toString() const
{
	ostringstream oss;
	oss << "valid: " << boolalpha << isValid << ", " <<
		"queryText: " << queryText << ", " <<
		"defaultSchema: " << defaultSchema;
	return oss.str();
}

ostream& operator<<(ostream& os, const QueryMessage& rhs)
{
	os << rhs.toString();
	return os;
}

class ThreadFunc
{
public:
	ThreadFunc(SockType fd) : fFd(fd) { }
	~ThreadFunc() { }

	void run();
	void operator()() { run(); }

private:
	ThreadFunc(const ThreadFunc& rhs);
	ThreadFunc& operator=(const ThreadFunc& rhs);

	SockType fFd;
};

bool serverInit()
{
#ifndef _MSC_VER

	setsid();

	//Handle certain signals (we want these to return EINTR so we can throw)
	//SIGPIPE
	//I don't think we'll get any of these from init (except possibly HUP, but that's an indication
	// of bad things anyway)
	//SIGHUP?
	//SIGUSR1?
	//SIGUSR2?
	//SIGPOLL?
	struct sigaction sa;
	memset(&sa, 0, sizeof(struct sigaction));
	sa.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &sa, 0);
	sigaction(SIGHUP, &sa, 0);
	sigaction(SIGUSR1, &sa, 0);
	sigaction(SIGUSR2, &sa, 0);
#ifndef __FreeBSD__
	sigaction(SIGPOLL, &sa, 0);
#endif
#if 0
	int fd;
	close(2);
	fd = open("/tmp/qfe.err",O_CREAT|O_TRUNC|O_WRONLY,0644);
	if (fd >= 0 && fd != 2)
	{
		dup2(fd, 2);
		close(fd);
	}
#endif
#endif
	return true;
}

SockType initListenSock(short portNo)
{
	SockType listenSock=-1;
#ifdef _MSC_VER
	WSAData wsadata;
	const WORD minVersion = MAKEWORD(2, 2);
	WSAStartup(minVersion, &wsadata);
#endif
	listenSock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	idbassert_s(listenSock >= 0, string("socket create error: ") + strerror(errno));
	//if (listenSock < 0) throw runtime_error(string("socket create error: ") + strerror(errno));
#ifndef _MSC_VER
	int optval = 1;
	setsockopt(listenSock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&optval), sizeof(optval));
#endif
	int rc = 0;
	struct sockaddr_in serv_addr;
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = INADDR_ANY;
	serv_addr.sin_port = htons(portNo);
	const int MaxTries = 5 * 60 / 10;
	int tries = 0;
again:
	rc = ::bind(listenSock, (sockaddr*)&serv_addr, sizeof(serv_addr));
	if (rc < 0)
	{
#ifdef _MSC_VER
		int x = WSAGetLastError();
		if (x == WSAEADDRINUSE)
#else
		if (errno == EADDRINUSE)
#endif
		{
			//cerr << "Addr in use..." << endl;
			if (++tries >= MaxTries)
			{
				log("Waited too long for socket to bind...giving up");
				//cerr << "Waited too long for socket to bind...giving up" << endl;
				exit(1);
			}
			sleep(10);
			goto again;
		}
		idbassert_s(0, string("socket bind error: ") + strerror(errno));
		//throw runtime_error(string("socket bind error: ") + strerror(errno));
	}
	rc = listen(listenSock, 16);
	idbassert_s(rc >= 0, string("socket listen error") + strerror(errno));
	//if (rc < 0) throw runtime_error(string("socket listen error") + strerror(errno));

	return listenSock;
}

QueryMessage getNextMsg(SockType fd)
{
	QueryMessage msg;

	try {
		msg.defaultSchema = socketio::readString(fd);
		msg.queryText = socketio::readString(fd);
		msg.isValid = true;
	} catch (runtime_error& rex) {
		cerr << "re reading ctl msg: " << rex.what() << endl;
		msg.queryText = "";
	} catch (...) {
		cerr << "ex reading ctl msg" << endl;
		msg.queryText = "";
	}

	return msg;
}

StmtType guessStatementType(const string& stmt)
{
	typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
	char_separator<char> sep;
	tokenizer tokens(stmt, sep);
	tokenizer::iterator tok_iter = tokens.begin();
	string first_word;
	first_word = *tok_iter;
	algorithm::to_lower(first_word);

	if (first_word == "select")
		return QUERY;
	if (first_word == "create")
		return CREATE;
	if (first_word == "drop")
		return DROP;
	if (first_word == "show")
		return SHOW;

	return UNKNOWN;
}

struct ScopedCleaner
{
	ScopedCleaner(SockType fd=-1) : fFd(fd) { }
#ifdef _MSC_VER
	~ScopedCleaner() { if (fFd >= 0) shutdown(fFd, SHUT_RDWR); closesocket(fFd); }
#else
	~ScopedCleaner() { if (fFd >= 0) shutdown(fFd, SHUT_RDWR); close(fFd); }

#endif
	SockType fFd;
};

void ThreadFunc::run()
{
	QueryMessage m;
	execplan::CalpontSelectExecutionPlan* csep=0;
	MessageQueueClient* msgqcl;

	ScopedCleaner cleaner(fFd);

	uint32_t sid = 1;
	sid = atomicops::atomicInc(&SystemSID);

	try {
		m = getNextMsg(fFd);

		if (m.isValid)
		{
			DefaultSchema = m.defaultSchema;
			StmtType st = guessStatementType(m.queryText);
			switch (st)
			{
			case QUERY:
				csep = parseQuery(m.queryText, sid);
				//sendCSEP takes ownership of the ptr from parseQuery
				msgqcl = sendCSEP(csep);
				//processReturnedRows takes ownership of the ptr from sendCSEP
				processReturnedRows(msgqcl, fFd);
				break;
			case CREATE:
				processCreateStmt(m.queryText, sid);
				break;
			case DROP:
				processDropStmt(m.queryText, sid);
				break;
			case SHOW:
			{
				ostringstream oss;
				oss << "select calpontsys.systable.tablename from calpontsys.systable where "
					"calpontsys.systable.schema='" << m.defaultSchema << "';";
				csep = parseQuery(oss.str(), sid);
				msgqcl = sendCSEP(csep);
				processReturnedRows(msgqcl, fFd);
				break;
			}
			default:
				throw runtime_error("couldn't guess the statement type");
				break;
			}
		}
	} catch (std::exception& ex) {
		socketio::writeString(fFd, ex.what());
		throw; //in a multi-threaded server this will simply cause this thread to exit
	} catch (...) {
		socketio::writeString(fFd, "internal query processing error");
		throw;
	}
}

}

int main(int argc, char** argv)
{
	int c;
	SockType listenSock;
	short portNo;

	portNo = 0;
	char* p = getenv("IDB_QFE_PORT");
	if (p && *p)
		portNo = atoi(p);

	if (portNo <= 0)
		portNo = 9198;
#ifdef _MSC_VER
	listenSock = INVALID_SOCKET;
#else
	listenSock = -1;
#endif
	opterr = 0;

	while ((c = getopt(argc, argv, "p:")) != -1)
		switch (c)
		{
		case 'p':
			portNo = atoi(optarg);
			break;
		case '?':
		default:
			break;
		}

	if (!serverInit())
	{
		log("Could not initialize the QFE Server!");
		cerr << "Could not initialize the QFE Server!" << endl;
		return 1;
	}

	listenSock = initListenSock(portNo);

	SystemSID = 0;

	for (;;)
	{
#ifdef _MSC_VER
		SOCKET querySock = INVALID_SOCKET;
		querySock = accept(listenSock, 0, 0);
		idbassert_s(querySock != INVALID_SOCKET, string("socket accept error: ") + strerror(errno));
#if 0
		uint32_t sndbufsize;
		int sndbufsizelen=4;
		int rc;
		rc = getsockopt(querySock, SOL_SOCKET, SO_SNDBUF, (char*)&sndbufsize, &sndbufsizelen);
		if (rc != SOCKET_ERROR)
		{
			if (sndbufsizelen == 4)
			{
				cerr << "getsockopt(): current SO_SNDBUF = " << sndbufsize << endl;
				sndbufsize = atoi(getenv("SO_SNDBUF"));
				cerr << "setsockopt(): setting SO_SNDBUF = " << sndbufsize << endl;
				rc = setsockopt(querySock, SOL_SOCKET, SO_SNDBUF, (const char*)&sndbufsize, sndbufsizelen);
				if (rc != SOCKET_ERROR)
				{
					getsockopt(querySock, SOL_SOCKET, SO_SNDBUF, (char*)&sndbufsize, &sndbufsizelen);
					cerr << "getsockopt(): new SO_SNDBUF = " << sndbufsize << endl;
				}
				else
				{
					cerr << "setsockopt(): " << WSAGetLastError() << endl;
				}
			}
			else
			{
				cerr << "getsockopt(): expecting 4 bytes, got " << sndbufsizelen << endl;
			}
		}
		else
		{
			cerr << "getsockopt(): " << WSAGetLastError() << endl;
		}
#endif
		uint32_t sndbufsize = 512 * 1024;
		setsockopt(querySock, SOL_SOCKET, SO_SNDBUF, (const char*)&sndbufsize, 4);
#else
		int querySock = -1;
		querySock = accept(listenSock, 0, 0);
		idbassert_s(querySock >= 0, string("socket accept error: ") + strerror(errno));
#endif

		//ThreadFunc now owns querySock and is responsible for cleaning it up
		ThreadFunc tf(querySock);

#ifdef SINGLE_THREADED
		try {
			tf.run();
		} catch (std::exception& ex) {
			cerr << "ThreadFunc run threw an exception: " << ex.what() << endl;
		} catch (...) {
			cerr << "ThreadFunc run threw an exception" << endl;
		}
#else
		thread t(tf);
#endif
	}

	return 0;
}
// vim:ts=4 sw=4:

