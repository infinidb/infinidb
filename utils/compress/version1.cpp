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
#include <pthread.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <poll.h>
#endif
#include <iostream>
#include <stdexcept>
#include <iomanip>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <stdint.h>
#include <cerrno>
#include <sstream>
using namespace std;

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/mapped_region.hpp>
#include <boost/interprocess/sync/scoped_lock.hpp>
#include <boost/interprocess/sync/interprocess_mutex.hpp>
namespace bi=boost::interprocess;

#include <boost/thread.hpp>

#include "installdir.h"
#include "shmkeys.h"
#include <config.h>
#include <exceptclasses.h>

#include "version1.h"

#define OAM_FORKS_DECOMSVR

/*
	Protocol definition:
	On the control socket:
	This server waits for the other end to send it:
		1. The prefix name of the data fifos to open and read/write (string). The incoming,
			compressed data is on "name".c and this server will write uncompressed
			data on "name".u.

	On the data fifos:
	The server then reads the compressed data from the cdata fifo:
		1. The number of bytes of compressed data to read (number)
		2. The compressed data
	then it decompresses it, and writes to the udata fifo:
		1. The number of bytes in the uncompressed stream (number)
		2. The uncompressed data

	strings are sent like this:
		uint32_t string len
		<?> len bytes of the string
	numbers are sent like this:
		uint64_t the number

	This server expects numeric values to be in its native byte order, so
	   the sender needs to send them that way.
*/

namespace
{
short DSPort = 9199;

void log(const string &s) 
{
	logging::MessageLog logger((logging::LoggingID()));
	logging::Message message;
	logging::Message::Args args;

	args.add(s);
	message.format(args);
	logger.logErrorMessage(message);
}

struct ScopedCleaner
{
#ifdef _MSC_VER
	ScopedCleaner() : ctlsock(INVALID_SOCKET),
		cpipeh(INVALID_HANDLE_VALUE),
		upipeh(INVALID_HANDLE_VALUE)
		{ }

	~ScopedCleaner()	
	{
		if (cpipeh != INVALID_HANDLE_VALUE)
			CloseHandle(cpipeh);
		if (upipeh != INVALID_HANDLE_VALUE)
			CloseHandle(upipeh);
		if (ctlsock != INVALID_SOCKET) { 
			shutdown(ctlsock, SHUT_RDWR);
			closesocket(ctlsock);
		}
	}

	SOCKET ctlsock;
	HANDLE cpipeh;
	HANDLE upipeh;
#else
	ScopedCleaner() : fd(-1), ctlsock(-1)
		{ }

	~ScopedCleaner()	
	{
		if (fd >= 0)
			close(fd);
		if (!cpipename.empty())
			unlink(cpipename.c_str());
		if (!upipename.empty())
			unlink(upipename.c_str());
		if (ctlsock >= 0) { 
			shutdown(ctlsock, SHUT_RDWR);
			close(ctlsock);
		}
	}

	int fd;
	int ctlsock;
	string cpipename;
	string upipename;
#endif
};

boost::mutex CtlShmMutex;

struct CtlShmImage
{
	bi::interprocess_mutex controlFifoMutex;
};
CtlShmImage* Ctlshmptr = 0;
bi::shared_memory_object Ctlshmobj;
bi::mapped_region Ctlshmregion;

void initCtlShm()
{
	BRM::ShmKeys shmKeys;
	string DecomShmName(BRM::ShmKeys::keyToName(shmKeys.DECOMSVRMUTEX_SYSVKEY));

	boost::mutex::scoped_lock Ctlshmlk(CtlShmMutex);
	
	if (Ctlshmptr)
		return;

	CtlShmImage* tmpptr = 0;

	try {
		bi::shared_memory_object shm(bi::open_only, DecomShmName.c_str(), bi::read_write);
		bi::mapped_region region(shm, bi::read_write);
		tmpptr = reinterpret_cast<CtlShmImage*>(region.get_address());
		Ctlshmobj.swap(shm);
		Ctlshmregion.swap(region);
	} catch (bi::interprocess_exception&) {
#if BOOST_VERSION < 104500
		bi::shared_memory_object shm(bi::create_only, DecomShmName.c_str(), bi::read_write);
#else
		bi::permissions perms;
		perms.set_unrestricted();
		bi::shared_memory_object shm(bi::create_only, DecomShmName.c_str(), bi::read_write, perms);
#endif
		shm.truncate(sizeof(CtlShmImage));
		bi::mapped_region region(shm, bi::read_write);
		tmpptr = new (region.get_address()) CtlShmImage;
		Ctlshmobj.swap(shm);
		Ctlshmregion.swap(region);
	}
	const string pname("DecomSvr");
	string srvrpath(startup::StartUp::installDir());
	srvrpath += "/bin/" + pname;
#ifndef OAM_FORKS_DECOMSVR
#ifndef _MSC_VER
	int rc;

	rc = fork();
	idbassert_s(rc >= 0, "couldn't fork DecomSvr");
//	if (rc < 0)
//		throw runtime_error("couldn't fork DecomSvr");
//	else if (rc == 0)
	if (rc == 0)
	{
		for (int fd = 0; fd < sysconf(_SC_OPEN_MAX); fd++)
			close(fd);
		open("/dev/null", O_RDONLY);
		open("/dev/null", O_WRONLY);
		open("/dev/null", O_WRONLY);
		execl(srvrpath.c_str(), pname.c_str(), (char*)NULL);
		idbassert_s(0, "couldn't exec DecomSvr");
		//throw runtime_error("couldn't exec DecomSvr");
	}
#else
	srvrpath += ".exe";
	PROCESS_INFORMATION pInfo;
	ZeroMemory(&pInfo, sizeof(pInfo));
	STARTUPINFO sInfo;
	ZeroMemory(&sInfo, sizeof(sInfo));

	idbassert_s(CreateProcess(0, (LPSTR)srvrpath.c_str(), 0, 0, false, DETACHED_PROCESS, 0, 0, &sInfo, &pInfo) != 0,
				"couldn't exec DecomSvr");
	//if (CreateProcess(0, (LPSTR)srvrpath.c_str(), 0, 0, false, 0, 0, 0, &sInfo, &pInfo) == 0)
	//	throw runtime_error("couldn't exec DecomSvr");
	CloseHandle(pInfo.hProcess);

	sleep(5);
#endif
#endif

	char* p = getenv("IDB_DECOMSVR_PORT");
	if (p && *p)
	{
		DSPort = atoi(p);
		if (DSPort <= 0)
			DSPort = 9199;
	}

	Ctlshmptr = tmpptr;
}

void sendn(int fd, const char* p, size_t wanted)
{
	size_t needed = wanted;
	size_t sofar = 0;
	ssize_t rrc = -1;
	pollfd fds[1];
	int en = 0;

	fds[0].fd = fd;
	fds[0].events = POLLOUT;

	while (wanted > sofar)
	{
		fds[0].revents = 0;
		poll(fds, 1, -1);
		errno = 0;
		rrc = send(fd, (p + sofar), needed, 0);
		en = errno;
		if (rrc < 0)
		{
			if (en == EAGAIN || en == EINTR || en == 512)
				continue;
			ostringstream oss;
			oss << "send() returned " << rrc << " (" << strerror(en) << ")";
			idbassert_s(0, oss.str());
			//throw runtime_error(oss.str());
		}
		needed -= rrc;
		sofar += rrc;
	}
}

}

namespace compress
{

namespace v1
{
#ifndef _MSC_VER
void readn(int fd, void* buf, const size_t wanted)
{
	size_t needed = wanted;
	size_t sofar = 0;
	char* p = reinterpret_cast<char*>(buf);
	ssize_t rrc = -1;
	pollfd fds[1];
	int en = 0;
	int prc = 0;
	ostringstream oss;
	unsigned zerocount=0;

	fds[0].fd = fd;
	fds[0].events = POLLIN;

	while (wanted > sofar)
	{
		fds[0].revents = 0;
		errno = 0;
		prc = poll(fds, 1, -1);
		en = errno;
		if (prc <= 0)
		{
			if (en == EAGAIN || en == EINTR || en == 512)
				continue;
			oss << "compress::v1::readn: poll() returned " << prc << " (" << strerror(en) << ")";
			idbassert_s(0, oss.str());
		}
		//Check if there's data to be read
		if ((fds[0].revents & POLLIN) == 0)
		{
			oss << "compress::v1::readn: revents for fd " << fds[0].fd << " was " << fds[0].revents;
			idbassert_s(0, oss.str());
		}
		errno = 0;
		rrc = read(fd, (p + sofar), needed);
		en = errno;
		if (rrc < 0)
		{
			if (en == EAGAIN || en == EINTR || en == 512)
				continue;
			oss << "compress::v1::readn(): read() returned " << rrc << " (" << strerror(en) << ")";
			//this throws logging::IDBExcept()
			idbassert_s(0, oss.str());
		}
		if (rrc == 0)
		{
			ostringstream os;
			zerocount++;
			if (zerocount >= 10)
			{
				os << "compress::v1::readn(): too many zero-length reads!";
				idbassert_s(0, oss.str());
			}
			logging::MessageLog logger((logging::LoggingID()));
			logging::Message message;
			logging::Message::Args args;
			os << "compress::v1::readn(): zero-length read on fd " << fd;
			args.add(os.str());
			message.format(args);
			logger.logWarningMessage(message);
			sleep(1);
		}
		else
			zerocount = 0;
		needed -= rrc;
		sofar += rrc;
	}
}

size_t writen(int fd, const void *data, size_t nbytes)
{
	size_t nleft;
	ssize_t nwritten;
	const char *bufp = (const char *) data;
	nleft = nbytes;

	while (nleft > 0)
	{
		// the O_NONBLOCK flag is not set, this is a blocking I/O.
  		if ((nwritten = ::write(fd, bufp, nleft)) < 0)
		{
			if (errno == EINTR)
				nwritten = 0;
			else {
				// save the error no first
				int e = errno;
				string errorMsg = "v1::writen() error: ";
		 		boost::scoped_array<char> buf(new char[80]);
#if STRERROR_R_CHAR_P
				const char* p;
		 		if ((p = strerror_r(e, buf.get(), 80)) != 0)
					errorMsg += p;
#else
				int p;
				if ((p = strerror_r(e, buf.get(), 80)) == 0)
					errorMsg += buf.get();
#endif
				idbassert_s(0, errorMsg);
				//throw runtime_error(errorMsg);
			}
		}
		nleft -= nwritten;
		bufp += nwritten;
	}
	return nbytes;
}
#endif

bool decompress(const char* in, const uint32_t inLen, unsigned char* out, size_t* ol)
{
	uint32_t u32;
	uint64_t u64;
	ostringstream oss;
	string s;
	string cpipe;
	string upipe;
	ScopedCleaner cleaner;
	int fd = -1;

	if (!Ctlshmptr)
		initCtlShm();

	bi::scoped_lock<bi::interprocess_mutex> cfLock(Ctlshmptr->controlFifoMutex, bi::defer_lock);

#ifndef _MSC_VER
	pthread_t thdid = pthread_self();
#else
	DWORD thdid = GetCurrentThreadId();
#endif

#ifdef _MSC_VER
	oss << "\\\\.\\pipe\\cdatafifo" << thdid;
	s = oss.str();
	cpipe = s + ".c";
	upipe = s + ".u";

	HANDLE cpipeh;
	cpipeh = CreateNamedPipe(cpipe.c_str(), PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,
		1, 0, 8192, 0, 0);
	idbassert_s(cpipeh != INVALID_HANDLE_VALUE, "while creating cdata fifo");
	//if (cpipeh == INVALID_HANDLE_VALUE)
	//	throw runtime_error("while creating cdata fifo");
	cleaner.cpipeh = cpipeh;

	HANDLE upipeh;
	upipeh = CreateNamedPipe(upipe.c_str(), PIPE_ACCESS_INBOUND, PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,
		1, 8192, 0, 0, 0);
	idbassert_s(upipeh != INVALID_HANDLE_VALUE, "while creating udata fifo");
	//if (upipeh == INVALID_HANDLE_VALUE)
	//	throw runtime_error("while creating udata fifo");
	cleaner.upipeh = upipeh;
#else
	oss << "/tmp/cdatafifo" << hex << thdid;
	s = oss.str();
	cpipe = s + ".c";
	upipe = s + ".u";
	cleaner.cpipename = cpipe;
	cleaner.upipename = upipe;
	unlink(cpipe.c_str());
	idbassert_s(mknod(cpipe.c_str(), S_IFIFO|0666, 0) == 0, "while creating cdata fifo");
	//if (mknod(cpipe.c_str(), S_IFIFO|0666, 0) != 0)
	//	throw runtime_error("while creating cdata fifo");
	unlink(upipe.c_str());
	idbassert_s(mknod(upipe.c_str(), S_IFIFO|0666, 0) == 0, "while creating udata fifo");
	//if (mknod(upipe.c_str(), S_IFIFO|0666, 0) != 0)
	//	throw runtime_error("while creating udata fifo");
#endif
	int rc = -1;
	fd = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
#ifdef _MSC_VER
	idbassert_s(fd != INVALID_SOCKET, 
		string("socket create error: ") + strerror(errno));
#else
	idbassert_s(fd >= 0,
		string("socket create error: ") + strerror(errno));
#endif
	cleaner.ctlsock = fd;
	struct sockaddr_in serv_addr;
	struct in_addr la;
	::inet_aton("127.0.0.1", &la);
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = la.s_addr;
	serv_addr.sin_port = htons(DSPort);
	const int MaxTries = 30;
	int tries = 0;

again:
	cfLock.lock();
	rc = ::connect(fd, (struct sockaddr*)&serv_addr, sizeof(serv_addr));
	if (rc < 0)
	{
#ifdef _MSC_VER
		int x = WSAGetLastError();
		if (x == WSAECONNREFUSED)
#else
		if (errno == ECONNREFUSED)
#endif
		{
			idbassert_s(++tries < MaxTries, string("socket connect error: ") + strerror(errno));
			//if (++tries >= MaxTries)
			//	throw runtime_error(string("socket connect error: ") + strerror(errno));
			cfLock.unlock();
			sleep(2);
			goto again;
		}
		idbassert_s(0, string("socket connect error: ") + strerror(errno));
		//throw runtime_error(string("socket connect error: ") + strerror(errno));
	}

	u32 = s.length();

	sendn(fd, reinterpret_cast<const char*>(&u32), 4);

	sendn(fd, s.c_str(), u32);

	shutdown(fd, SHUT_RDWR);
#ifdef _MSC_VER
	closesocket(fd);
	cleaner.ctlsock = INVALID_SOCKET;
#else
	close(fd);
	cleaner.ctlsock = -1;
#endif

	cfLock.unlock();
#ifdef _MSC_VER
	BOOL dwrc;

	dwrc = ConnectNamedPipe(cpipeh, 0);
	idbassert_s(!(dwrc == 0 && GetLastError() != ERROR_PIPE_CONNECTED), "connecting to cpipe");
	//if (dwrc == 0 && GetLastError() != ERROR_PIPE_CONNECTED)
	//	throw runtime_error("connecting to cpipe");

	u64 = static_cast<uint64_t>(inLen);
	idbassert_s(u64 < 8 * 1024 * 1024, "preposterous inLen!");
	//if (!(u64 < 8 * 1024 * 1024))
	//	throw runtime_error("preposterous inLen!");

	DWORD nwrite;
	dwrc = WriteFile(cpipeh, &u64, 8, &nwrite, 0);
	idbassert_s(dwrc != 0 && nwrite == 8, "while writing to cpipe");
	//if (!(dwrc != 0 && nwrite == 8))
	//	throw runtime_error("while writing to cpipe");

	dwrc = WriteFile(cpipeh, in, u64, &nwrite, 0);
	idbassert_s(dwrc != 0 && nwrite == u64, "while writing to cpipe");
	//if (!(dwrc != 0 && nwrite == u64))
	//	throw runtime_error("while writing to cpipe");

	FlushFileBuffers(cpipeh);
	CloseHandle(cpipeh);
	cleaner.cpipeh = INVALID_HANDLE_VALUE;
#else
	ssize_t wrc;
	fd = open(cpipe.c_str(), O_WRONLY);
	idbassert_s(fd >= 0, "while opening data fifo for write");
	//if (fd < 0)
	//	throw runtime_error("while opening data fifo for write");

	cleaner.fd = fd;

	u64 = static_cast<uint64_t>(inLen);
	errno = 0;
	wrc = writen(fd, &u64, 8);
	int err = errno;
	idbassert_s(wrc == 8, string("while writing compressed len to the DS: ") + strerror(err));
//	if (wrc != 8)
//	{
//		ostringstream oss;
//		oss << "while writing compressed len to the DS: " << strerror(err);
//		throw runtime_error(oss.str());
//	}

	wrc = writen(fd, in, u64);
	idbassert_s(wrc == static_cast<ssize_t>(u64), "while writing compressed data to the DS");
//	if (wrc != static_cast<ssize_t>(u64))
//		throw runtime_error("while writing compressed data to the DS");

	close(fd);
	cleaner.fd = -1;
#endif

#ifdef _MSC_VER
	dwrc = ConnectNamedPipe(upipeh, 0);
	idbassert_s(!(dwrc == 0 && GetLastError() != ERROR_PIPE_CONNECTED), "connecting to upipe");
	//if (dwrc == 0 && GetLastError() != ERROR_PIPE_CONNECTED)
	//	throw runtime_error("connecting to upipe");

	DWORD nread;
	dwrc = ReadFile(upipeh, &u64, 8, &nread, 0);
	idbassert_s(dwrc != 0 && nread == 8, "while reading from upipe");
	//if (!(dwrc != 0 && nread == 8))
	//	throw runtime_error("while reading from upipe");

	dwrc = ReadFile(upipeh, out, u64, &nread, 0);
	idbassert_s(dwrc != 0 && nread == u64, "while reading from upipe");
	//if (!(dwrc != 0 && nread == u64))
	//	throw runtime_error("while reading from upipe");

	CloseHandle(upipeh);
	cleaner.upipeh = INVALID_HANDLE_VALUE;
#else
	fd = open(upipe.c_str(), O_RDONLY);
	idbassert_s(fd >= 0, "while opening data fifo for read");
//	if (fd < 0)
//		throw runtime_error("while opening data fifo for read");

	cleaner.fd = fd;

	readn(fd, &u64, 8);
	readn(fd, out, u64);

	close(fd);
	cleaner.fd = -1;
#endif

	*ol = static_cast<size_t>(u64);

	return (u64 != 0);
}

} //namespace v1

} //namespace compress

