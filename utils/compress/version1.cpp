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

#ifdef _MSC_VER
#include "idbregistry.h"
#endif

#include "shmkeys.h"

#include "version1.h"

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

struct ScopedCleaner
{
#ifdef _MSC_VER
	ScopedCleaner() : fd(-1), ctlsock(-1),
		cpipeh(INVALID_HANDLE_VALUE),
		upipeh(INVALID_HANDLE_VALUE)
		{ }

	~ScopedCleaner()	
	{
		if (fd >= 0)
			close(fd);
		if (cpipeh != INVALID_HANDLE_VALUE)
			CloseHandle(cpipeh);
		if (upipeh != INVALID_HANDLE_VALUE)
			CloseHandle(upipeh);
		if (ctlsock >= 0) { 
			shutdown(ctlsock, SHUT_RDWR);
			closesocket(ctlsock);
		}
	}

	int fd;
	int ctlsock;
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
	string srvrpath("/usr/local/Calpont");
	srvrpath += "/bin/" + pname;
#ifndef _MSC_VER
	int rc;

	rc = fork();
	if (rc < 0)
		throw runtime_error("couldn't fork DecomSvr");
	else if (rc == 0)
	{
		for (int fd = 0; fd < sysconf(_SC_OPEN_MAX); fd++)
			close(fd);
		open("/dev/null", O_RDONLY);
		open("/dev/null", O_WRONLY);
		open("/dev/null", O_WRONLY);
		execl(srvrpath.c_str(), pname.c_str(), (char*)NULL);
		throw runtime_error("couldn't exec DecomSvr");
	}
#else
	srvrpath = IDBreadRegistry("");
	srvrpath += "\\bin\\" + pname;
	srvrpath += ".exe";
	PROCESS_INFORMATION pInfo;
	ZeroMemory(&pInfo, sizeof(pInfo));
	STARTUPINFO sInfo;
	ZeroMemory(&sInfo, sizeof(sInfo));

	if (CreateProcess(0, (LPSTR)srvrpath.c_str(), 0, 0, false, 0, 0, 0, &sInfo, &pInfo) == 0)
		throw runtime_error("couldn't exec DecomSvr");
	CloseHandle(pInfo.hProcess);

#endif

	char* p = getenv("IDB_DECOMSVR_PORT");
	if (p && *p)
	{
		DSPort = atoi(p);
		if (DSPort <= 0)
			DSPort = 9199;
	}

	sleep(5);

	Ctlshmptr = tmpptr;
}

}

namespace compress
{

namespace v1
{

bool decompress(const char* in, const uint32_t inLen, unsigned char* out, size_t* ol)
{
	uint32_t u32;
	uint64_t u64;
	ostringstream oss;
	string s;
	string cpipe;
	string upipe;
	ssize_t wrc;
	int thdid = 0;
	ScopedCleaner cleaner;
	int fd = -1;

	if (!Ctlshmptr)
		initCtlShm();

	bi::scoped_lock<bi::interprocess_mutex> cfLock(Ctlshmptr->controlFifoMutex, bi::defer_lock);

#ifndef _MSC_VER
	thdid = pthread_self();
#else
	thdid = GetCurrentThreadId();
#endif

#ifdef _MSC_VER
	oss << "\\\\.\\pipe\\cdatafifo" << thdid;
	s = oss.str();
	cpipe = s + ".c";
	upipe = s + ".u";

	HANDLE cpipeh;
	cpipeh = CreateNamedPipe(cpipe.c_str(), PIPE_ACCESS_OUTBOUND, PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,
		1, 0, 8192, 0, 0);
	if (cpipeh == INVALID_HANDLE_VALUE)
		throw runtime_error("while creating cdata fifo");
	cleaner.cpipeh = cpipeh;

	HANDLE upipeh;
	upipeh = CreateNamedPipe(upipe.c_str(), PIPE_ACCESS_INBOUND, PIPE_TYPE_BYTE|PIPE_READMODE_BYTE|PIPE_WAIT,
		1, 8192, 0, 0, 0);
	if (upipeh == INVALID_HANDLE_VALUE)
		throw runtime_error("while creating udata fifo");
	cleaner.upipeh = upipeh;
#else
	oss << "/tmp/cdatafifo" << thdid;
	s = oss.str();
	cpipe = s + ".c";
	upipe = s + ".u";
	cleaner.cpipename = cpipe;
	cleaner.upipename = upipe;
	unlink(cpipe.c_str());
	if (mknod(cpipe.c_str(), S_IFIFO|0666, 0) != 0)
		throw runtime_error("while creating cdata fifo");
	unlink(upipe.c_str());
	if (mknod(upipe.c_str(), S_IFIFO|0666, 0) != 0)
		throw runtime_error("while creating udata fifo");
#endif
	int rc = -1;
	fd = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	if (fd < 0)
		throw runtime_error(string("socket create error: ") + strerror(errno));
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
			if (++tries >= MaxTries)
				throw runtime_error(string("socket connect error: ") + strerror(errno));
			cfLock.unlock();
			sleep(2);
			goto again;
		}
		throw runtime_error(string("socket connect error: ") + strerror(errno));
	}

	u32 = s.length();

	wrc = send(fd, reinterpret_cast<const char*>(&u32), 4, 0);

	if (wrc != 4)
		throw runtime_error("while writing fifo len to the DS");

	wrc = send(fd, s.c_str(), u32, 0);

	if (wrc != u32)
		throw runtime_error("while writing fifo name to the DS");

	shutdown(fd, SHUT_RDWR);
#ifdef _MSC_VER
	closesocket(fd);
#else
	close(fd);
#endif
	cleaner.ctlsock = -1;

	cfLock.unlock();
#ifdef _MSC_VER
	BOOL dwrc;

	dwrc = ConnectNamedPipe(cpipeh, 0);
	if (dwrc == 0 && GetLastError() != ERROR_PIPE_CONNECTED)
		throw runtime_error("connecting to cpipe");

	u64 = static_cast<uint64_t>(inLen);
	if (!(u64 < 8 * 1024 * 1024))
		throw runtime_error("preposterous inLen!");

	DWORD nwrite;
	dwrc = WriteFile(cpipeh, &u64, 8, &nwrite, 0);
	if (!(dwrc != 0 && nwrite == 8))
		throw runtime_error("while writing to cpipe");

	dwrc = WriteFile(cpipeh, in, u64, &nwrite, 0);
	if (!(dwrc != 0 && nwrite == u64))
		throw runtime_error("while writing to cpipe");

	FlushFileBuffers(cpipeh);
	CloseHandle(cpipeh);
	cleaner.cpipeh = INVALID_HANDLE_VALUE;
#else
	fd = open(cpipe.c_str(), O_WRONLY);
	if (fd < 0)
		throw runtime_error("while opening data fifo for write");

	cleaner.fd = fd;

	u64 = static_cast<uint64_t>(inLen);
	errno = 0;
	wrc = write(fd, &u64, 8);
	int err = errno;
	if (wrc != 8)
	{
		ostringstream oss;
		oss << "while writing compressed len to the DS: " << strerror(err);
		throw runtime_error(oss.str());
	}

	wrc = write(fd, in, u64);
	if (wrc != static_cast<ssize_t>(u64))
		throw runtime_error("while writing compressed data to the DS");

	close(fd);
	cleaner.fd = -1;
#endif

#ifdef _MSC_VER
	dwrc = ConnectNamedPipe(upipeh, 0);
	if (dwrc == 0 && GetLastError() != ERROR_PIPE_CONNECTED)
		throw runtime_error("connecting to upipe");

	DWORD nread;
	dwrc = ReadFile(upipeh, &u64, 8, &nread, 0);
	if (!(dwrc != 0 && nread == 8))
		throw runtime_error("while reading from upipe");

	dwrc = ReadFile(upipeh, out, u64, &nread, 0);
	if (!(dwrc != 0 && nread == u64))
		throw runtime_error("while reading from upipe");

	CloseHandle(upipeh);
	cleaner.upipeh = INVALID_HANDLE_VALUE;
#else
	fd = open(upipe.c_str(), O_RDONLY);
	if (fd < 0)
		throw runtime_error("while opening data fifo for read");

	cleaner.fd = fd;

	read(fd, &u64, 8);
	read(fd, out, u64);

	close(fd);
	cleaner.fd = -1;
#endif

	*ol = static_cast<size_t>(u64);

	return (u64 != 0);
}

} //namespace v1

} //namespace compress

