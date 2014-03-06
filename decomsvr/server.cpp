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

//#define _FILE_OFFSET_BITS 64
//#define _LARGEFILE64_SOURCE

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
#ifndef _MSC_VER
#include <dirent.h>
#endif
using namespace std;

#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#include "quicklz.h"
#ifndef _MSC_VER
#include "config.h"
#endif

//#define SINGLE_THREADED

namespace
{
#ifdef _MSC_VER
typedef SOCKET SockType;
#define SockReadFcn reads
#else
typedef int SockType;
#define SockReadFcn readn
#endif

short PortNo;
SockType ListenSock;

// version 1.1 of the chunk data has a short header
const uint8_t CHUNK_MAGIC1 = 0xff;
const int SIG_OFFSET = 0;
const int CHECKSUM_OFFSET = 1;
const int LEN_OFFSET = 5;
const uint32_t HEADER_SIZE = 9;

const int ERR_OK = 0;
const int ERR_CHECKSUM = -1;
const int ERR_DECOMPRESS = -2;
const int ERR_BADINPUT = -3;

/* version 1.2 of the chunk data changes the hash function used to calculate
 * checksums.  We can no longer use the algorithm used in ver 1.1.  Everything
 * else is the same
 */
const uint8_t CHUNK_MAGIC2 = 0xfe;

// A version of idbassert_s & log() that doesn't require linking the logging lib.
// Things printed to stderr go to /tmp/decomsvr.err
#ifndef __STRING
#define __STRING(x) #x
#endif
#define idbassert_s(x, s) do { \
	if (!(x)) { \
		std::ostringstream os; \
\
		os << __FILE__ << "@" << __LINE__ << ": assertion \'" << __STRING(x) << "\' failed.  Error msg \'" << s << "\'"; \
		std::cerr << os.str() << std::endl; \
		throw runtime_error(os.str()); \
	} \
} while (0)

void log(const string &s) 
{
	cerr << s << endl;
}

struct DecomMessage
{
	DecomMessage() : isValid(false) { }
	~DecomMessage() { }

	string toString() const;

	bool isValid;
	string pipeName;
};

string DecomMessage::toString() const
{
	ostringstream oss;
	oss << "valid: " << boolalpha << isValid << ", " <<
		"pipepfx: " << pipeName;
	return oss.str();
}

ostream& operator<<(ostream& os, const DecomMessage& rhs)
{
	os << rhs.toString();
	return os;
}

class ThreadFunc
{
public:
	ThreadFunc(const DecomMessage& dm) : fDm(dm) { }
	~ThreadFunc() { }

	void operator()();

private:
	//Defaults okay
	//ThreadFunc(const ThreadFunc& rhs);
	//ThreadFunc& operator=(const ThreadFunc& rhs);

	DecomMessage fDm;
};

bool serverInit()
{
#ifndef _MSC_VER

	//Set parent PID to init
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
	int fd;
	close(2);
	fd = open("/tmp/decomsrv.err",O_CREAT|O_TRUNC|O_WRONLY,0644);
	if (fd >= 0 && fd != 2)
	{
		dup2(fd, 2);
		close(fd);
	}
#endif
	return true;
}

bool initCtlFifo()
{
#ifdef _MSC_VER
	WSAData wsadata;
	const WORD minVersion = MAKEWORD(2, 2);
	WSAStartup(minVersion, &wsadata);
#endif
	ListenSock = socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
	idbassert_s(ListenSock >= 0, string("socket create error: ") + strerror(errno));
	//if (ListenSock < 0) throw runtime_error(string("socket create error: ") + strerror(errno));
#ifndef _MSC_VER
	int optval = 1;
	setsockopt(ListenSock, SOL_SOCKET, SO_REUSEADDR, reinterpret_cast<const char*>(&optval), sizeof(optval));
#endif
	int rc = 0;
	struct sockaddr_in serv_addr;
	struct in_addr la;
	inet_aton("127.0.0.1", &la);
	memset(&serv_addr, 0, sizeof(serv_addr));
	serv_addr.sin_family = AF_INET;
	serv_addr.sin_addr.s_addr = la.s_addr;
	serv_addr.sin_port = htons(PortNo);
	const int MaxTries = 5 * 60 / 10;
	int tries = 0;
again:
	rc = ::bind(ListenSock, (sockaddr*)&serv_addr, sizeof(serv_addr));
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
	rc = listen(ListenSock, 16);
	idbassert_s(rc >= 0, string("socket listen error") + strerror(errno));
	//if (rc < 0) throw runtime_error(string("socket listen error") + strerror(errno));

	return true;
}

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
			oss << "decomsvr::readn: poll() returned " << prc << " (" << strerror(en) << ")";
			idbassert_s(0, oss.str());
		}
		//no data on fd
		if ((fds[0].revents & POLLIN) == 0)
		{
			oss << "decomsvr::readn: revents for fd " << fds[0].fd << " was " << fds[0].revents;
			idbassert_s(0, oss.str());
		}
		errno = 0;
		rrc = read(fd, (p + sofar), needed);
		en = errno;
		if (rrc < 0)
		{
			if (en == EAGAIN || en == EINTR || en == 512)
				continue;
			oss << "decomsvr::readn: read() returned " << rrc << " (" << strerror(en) << ")";
			idbassert_s(0, oss.str());
		}
		if (rrc == 0)
		{
			ostringstream os;
			zerocount++;
			if (zerocount >= 10)
			{
				os << "decomsvr::readn(): too many zero-length reads!";
				idbassert_s(0, oss.str());
			}
			os << "decomsvr::readn(): zero-length read on fd " << fd;
			log(os.str());
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
				string errorMsg = "decomsvr: write() error: ";
		 		scoped_array<char> buf(new char[80]);
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
			}
		}
		nleft -= nwritten;
		bufp += nwritten;
	}

	return nbytes;
}
#else
void reads(SOCKET fd, void* buf, const size_t wanted)
{
	size_t needed = wanted;
	size_t sofar = 0;
	char* p = reinterpret_cast<char*>(buf);
	ssize_t rrc = -1;
	pollfd fds[1];
	int en = 0;

	fds[0].fd = fd;
	fds[0].events = POLLIN;

	while (wanted > sofar)
	{
		fds[0].revents = 0;
		poll(fds, 1, -1);
		errno = 0;
		rrc = recv(fd, (p + sofar), (int)needed, 0);
		en = errno;
		if (rrc < 0)
		{
			if (en == EAGAIN || en == EINTR)
				continue;
			ostringstream oss;
			oss << "read() returned " << rrc << " (" << strerror(en) << ")";
			idbassert_s(0, oss.str());
		}
		needed -= rrc;
		sofar += rrc;
	}
}
#endif

uint32_t readNumber32(SockType fd)
{
	uint32_t np;
	SockReadFcn(fd, &np, 4);
	return np;
}

string readString(SockType fd)
{
	string s;
	uint32_t len = readNumber32(fd);
	idbassert_s(len <= 64, "while reading a string len (>64)");
	//if (len > 64)
	//	throw runtime_error("while reading a string len (>64)");
	char* buf = (char*)alloca(len+1); //this should be at most 65 bytes and should always succeed
	SockReadFcn(fd, buf, len);
	buf[len] = 0;
	s = buf;
	return s;
}

DecomMessage getNextMsg(SockType fd)
{
	DecomMessage dm;
	try {
		dm.pipeName = readString(fd);
		dm.isValid = true;
	} catch (runtime_error& rex) {
		cerr << "re reading ctl msg: " << rex.what() << endl;
		dm.pipeName = "";
	} catch (...) {
		cerr << "ex reading ctl msg" << endl;
		dm.pipeName = "";
	}

	return dm;
}

// Murmur3 from code.google.com

uint64_t fmix(uint64_t k)
{
	k ^= k >> 33;
	k *= 0xff51afd7ed558ccdULL;
	k ^= k >> 33;
	k *= 0xc4ceb9fe1a85ec53ULL;
	k ^= k >> 33;

	return k;
}

uint64_t rotl64(uint64_t x, int8_t r)
{
	return (x << r) | (x >> (64 - r));
}

class Hasher128 {
public:
	inline uint64_t operator()(const char *data, uint64_t len) const
	{
		const int nblocks = len / 16;

		uint64_t h1 = 0;
		uint64_t h2 = 0;

		const uint64_t c1 = 0x87c37b91114253d5ULL;
		const uint64_t c2 = 0x4cf5ad432745937fULL;

		//----------
		// body

		const uint64_t * blocks = (const uint64_t *) (data);

		for (int i = 0; i < nblocks; i++) {
			uint64_t k1 = blocks[i * 2 + 0];
			uint64_t k2 = blocks[i * 2 + 1];

			k1 *= c1;
			k1 = rotl64(k1, 31);
			k1 *= c2;
			h1 ^= k1;

			h1 = rotl64(h1, 27);
			h1 += h2;
			h1 = h1 * 5 + 0x52dce729;

			k2 *= c2;
			k2 = rotl64(k2, 33);
			k2 *= c1;
			h2 ^= k2;

			h2 = rotl64(h2, 31);
			h2 += h1;
			h2 = h2 * 5 + 0x38495ab5;
		}

		//----------
		// tail

		const uint8_t * tail = (const uint8_t*) (data + nblocks * 16);

		uint64_t k1 = 0;
		uint64_t k2 = 0;

		switch (len & 15) {
		case 15: k2 ^= uint64_t(tail[14]) << 48;
		case 14: k2 ^= uint64_t(tail[13]) << 40;
		case 13: k2 ^= uint64_t(tail[12]) << 32;
		case 12: k2 ^= uint64_t(tail[11]) << 24;
		case 11: k2 ^= uint64_t(tail[10]) << 16;
		case 10: k2 ^= uint64_t(tail[9]) << 8;
		case 9:	k2 ^= uint64_t(tail[8]) << 0;
			k2 *= c2;
			k2 = rotl64(k2, 33);
			k2 *= c1;
			h2 ^= k2;
		case 8:	k1 ^= uint64_t(tail[7]) << 56;
		case 7:	k1 ^= uint64_t(tail[6]) << 48;
		case 6:	k1 ^= uint64_t(tail[5]) << 40;
		case 5:	k1 ^= uint64_t(tail[4]) << 32;
		case 4:	k1 ^= uint64_t(tail[3]) << 24;
		case 3:	k1 ^= uint64_t(tail[2]) << 16;
		case 2:	k1 ^= uint64_t(tail[1]) << 8;
		case 1:	k1 ^= uint64_t(tail[0]) << 0;
			k1 *= c1;
			k1 = rotl64(k1, 31);
			k1 *= c2;
			h1 ^= k1;
		};

		//----------
		// finalization

		h1 ^= len;
		h2 ^= len;

		h1 += h2;
		h2 += h1;

		h1 = fmix(h1);
		h2 = fmix(h2);

		h1 += h2;
		h2 += h1;

		return h1;
	}
};

int uncompressBlock(const char* in, const size_t inLen, unsigned char* out,
	unsigned int& outLen)
{
	int rc = ERR_OK;
	int qlzrc = 0;

	boost::scoped_ptr<qlz_state_decompress> scratch(new qlz_state_decompress());

	uint32_t realChecksum;
	uint32_t storedChecksum;
	uint32_t storedLen;
	uint8_t storedMagic;
	Hasher128 hasher;

	outLen = 0;

	if (inLen < 1) {
		return ERR_BADINPUT;
	}
	storedMagic = *((uint8_t *) &in[SIG_OFFSET]);

	if (storedMagic == CHUNK_MAGIC1 || storedMagic == CHUNK_MAGIC2) {
		if (inLen < HEADER_SIZE) {
			return ERR_BADINPUT;
		}
		storedChecksum = *((uint32_t *) &in[CHECKSUM_OFFSET]);
		storedLen = *((uint32_t *) (&in[LEN_OFFSET]));
		if (inLen < storedLen + HEADER_SIZE) {
			return ERR_BADINPUT;
		}
		/* We can no longer verify the checksum on ver 1.1 */
		if (storedMagic == CHUNK_MAGIC2) {
			realChecksum = hasher(&in[HEADER_SIZE], storedLen);
			if (storedChecksum != realChecksum) {
				return ERR_CHECKSUM;
			}
		}
		qlzrc = qlz_decompress(&in[HEADER_SIZE], out, scratch.get());
	}
	else
		qlzrc = qlz_decompress(in, out, scratch.get());

	if (qlzrc == 0)
		rc = ERR_DECOMPRESS;
	else
		outLen = qlzrc;

	return rc;
}

struct ScopedCleaner
{
#ifdef _MSC_VER
	ScopedCleaner() : handle(INVALID_HANDLE_VALUE) { }
	~ScopedCleaner() { if (handle != INVALID_HANDLE_VALUE) CloseHandle(handle); }

	HANDLE handle;
#else
	ScopedCleaner() : fd(-1) { }
	~ScopedCleaner() { if (fd >= 0) close(fd); }

	int fd;
#endif
};

void ThreadFunc::operator()()
{
	string cfifo = fDm.pipeName + ".c";
	string ufifo = fDm.pipeName + ".u";
	uint64_t ccount=0;
	ssize_t rrc = -1;
	ScopedCleaner cleaner;
#ifdef _MSC_VER
	HANDLE h;
	h = CreateFile(cfifo.c_str(), GENERIC_READ, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
	idbassert_s(h != INVALID_HANDLE_VALUE, "while opening cpipe");
	//if (!(h != INVALID_HANDLE_VALUE))
	//	throw runtime_error("while opening cpipe");
	cleaner.handle = h;

	DWORD nread;
	BOOL drrc;
	drrc = ReadFile(h, &ccount, 8, &nread, 0);
	idbassert_s(drrc != 0 && nread == 8 && ccount < 8 * 1024 * 1024, "while reading from cpipe");
	//if (!(drrc != 0 && nread == 8 && ccount < 8 * 1024 * 1024))
	//	throw runtime_error("while reading from cpipe");

	scoped_array<char> in(new char[ccount]);

	drrc = ReadFile(h, in.get(), (DWORD)ccount, &nread, 0);
	idbassert_s(drrc != 0 && nread == ccount, "while reading from cpipe");
	//if (!(drrc != 0 && nread == ccount))
	//	throw runtime_error("while reading from cpipe");

	CloseHandle(h);
	cleaner.handle = INVALID_HANDLE_VALUE;
#else
	scoped_array<char> in;
	int fd=-1;
	try
	{
		fd = open(cfifo.c_str(), O_RDONLY);
		idbassert_s(fd >= 0, "when opening data fifo for input");

		cleaner.fd = fd;

		readn(fd, &ccount, 8);

		in.reset(new char[ccount]);

		readn(fd, in.get(), ccount);

		close(fd);
		cleaner.fd = -1;
	}
	catch (std::exception& )
	{
		//This is a protocol error and returning here will clean up resources on
		//the stack unwind.
		return;
	}
#endif
#ifdef _MSC_VER
	h = CreateFile(ufifo.c_str(), GENERIC_WRITE, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
	idbassert_s(h != INVALID_HANDLE_VALUE, "while opening upipe");
	//if (!(h != INVALID_HANDLE_VALUE))
	//	throw runtime_error("while opening upipe");
	cleaner.handle = h;

	uint64_t outlen = 512 * 1024 * 8;
	unsigned int ol = static_cast<unsigned int>(outlen);

	scoped_array<char> out(new char[outlen]);

	int crc = uncompressBlock(in.get(), ccount, reinterpret_cast<unsigned char*>(out.get()), ol);
	if (crc != ERR_OK)
		outlen = 0;
	else
		outlen = ol;

	BOOL dwrc;
	DWORD nwritten;
	dwrc = WriteFile(h, &outlen, 8, &nwritten, 0);
	idbassert_s(dwrc != 0 && nwritten == 8, "while writing to upipe");
	//if (!(dwrc != 0 && nwritten == 8))
	//	throw runtime_error("while writing to upipe");

	dwrc = WriteFile(h, out.get(), (DWORD)outlen, &nwritten, 0);
	idbassert_s(dwrc != 0 && nwritten == outlen, "while writing to upipe");
	//if (!(dwrc != 0 && nwritten == outlen))
	//	throw runtime_error("while writing to upipe");

	FlushFileBuffers(h);
	CloseHandle(h);
	cleaner.handle = INVALID_HANDLE_VALUE;
#else
	scoped_array<char> out;
	try
	{
		fd = open(ufifo.c_str(), O_WRONLY);
		idbassert_s(fd >= 0, "when opening data fifo for output");
		//if (fd < 0)
		//	throw runtime_error("when opening data fifo for output");

		cleaner.fd = fd;

		uint64_t outlen = 512 * 1024 * 8;
		unsigned int ol = outlen;

		out.reset(new char[outlen]);

		int crc = uncompressBlock(in.get(), ccount, reinterpret_cast<unsigned char*>(out.get()), ol);
		if (crc != ERR_OK)
			outlen = 0;
		else
			outlen = ol;

		rrc = writen(fd, &outlen, 8);
		idbassert_s(rrc == 8, "when writing len to data fifo");

		rrc = writen(fd, out.get(), outlen);
		idbassert_s(rrc == (ssize_t)outlen, "when writing data to data fifo");

		close(fd);
		cleaner.fd = -1;
	}
	catch (std::exception& )
	{
		//There was an error writing the uncompressed data back to PrimProc. Cleanup by
		//unwinding the stack
		return;
	}
#endif
}

#ifndef _MSC_VER
void cleanupFifos()
{
	//find all existing fifos and try get rid of them....
	DIR* dirp=0;
	struct dirent* direntp=0;
	char fifoname[PATH_MAX];
	int fd=-1;

	dirp = opendir("/tmp");
	strcpy(fifoname, "/tmp/");

	direntp = readdir(dirp);
	while (direntp != 0)
	{
		if (memcmp(direntp->d_name, "cdatafifo", 9) == 0)
		{
			strcpy(&fifoname[5], direntp->d_name);
			fd = open(fifoname, O_RDONLY|O_NONBLOCK);
			//opening and closing this fifo will cause PP to unblock and retry
			if (fd >= 0)
			{
				close(fd);
			}
			else
			{
				fd = open(fifoname, O_WRONLY|O_NONBLOCK);
				if (fd >= 0)
					close(fd);
			}
		}
		direntp = readdir(dirp);
	}
	closedir(dirp);
}
#endif

}

int main(int argc, char** argv)
{
	int c;

	PortNo = 0;
	char* p = getenv("IDB_DECOMSVR_PORT");
	if (p && *p)
		PortNo = atoi(p);

	if (PortNo <= 0)
		PortNo = 9199;
#ifdef _MSC_VER
	ListenSock = INVALID_SOCKET;
#else
	ListenSock = -1;
#endif
	opterr = 0;

	while ((c = getopt(argc, argv, "p:")) != -1)
		switch (c)
		{
		case 'p':
			PortNo = atoi(optarg);
			break;
		case '?':
		default:
			break;
		}

	if (!serverInit())
	{
		log("Could not initialize the Decompression Server!");
		//cerr << "Could not initialize the Decompression Server!" << endl;
		return 1;
	}

	initCtlFifo();

#ifndef _MSC_VER
	cleanupFifos();
#endif

	DecomMessage m;

	for (;;)
	{
#ifdef _MSC_VER
		SOCKET dataSock = INVALID_SOCKET;
#else
		int dataSock = -1;
#endif
		dataSock = accept(ListenSock, 0, 0);
#ifdef _MSC_VER
		idbassert_s(dataSock != INVALID_SOCKET, string("socket accept error: ") + strerror(errno));
		//if (dataSock == INVALID_SOCKET)
		//	throw runtime_error(string("socket accept error: ") + strerror(errno));
#else
		//if (dataSock < 0)
		idbassert_s(dataSock >= 0, string("socket accept error: ") + strerror(errno));
#endif
		m = getNextMsg(dataSock);
		shutdown(dataSock, SHUT_RDWR);
#ifdef _MSC_VER
		closesocket(dataSock);
#else
		close(dataSock);
#endif
		if (m.isValid)
		{
			ThreadFunc tf(m);
#ifdef SINGLE_THREADED
			tf();
#else
			thread t(tf);
#endif
		}
		else
			idbassert_s(0, "Invalid msg");
			//cerr << "Invalid msg" << endl;
	}

	return 0;
}

