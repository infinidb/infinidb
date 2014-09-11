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
using namespace std;

#include <boost/thread.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
using namespace boost;

#include "quicklz.h"

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
const uint HEADER_SIZE = 9;

const int ERR_OK = 0;
const int ERR_CHECKSUM = -1;
const int ERR_DECOMPRESS = -2;
const int ERR_BADINPUT = -3;

/* version 1.2 of the chunk data changes the hash function used to calculate
 * checksums.  We can no longer use the algorithm used in ver 1.1.  Everything
 * else is the same
 */
const uint8_t CHUNK_MAGIC2 = 0xfe;

struct CompressedDBFileHeader
{
	uint64_t fMagicNumber;
	uint64_t fVersionNum;
	uint64_t fCompressionType;
	uint64_t fHeaderSize;
	uint64_t fBlockCount;
};

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
	if (ListenSock < 0) throw runtime_error(string("socket create error: ") + strerror(errno));
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
				//cerr << "Waited too long for socket to bind...giving up" << endl;
				exit(1);
			}
			sleep(10);
			goto again;
		}
		throw runtime_error(string("socket bind error: ") + strerror(errno));
	}
	rc = listen(ListenSock, 16);
	if (rc < 0) throw runtime_error(string("socket listen error") + strerror(errno));

	return true;
}

void readn(int fd, void* buf, const size_t wanted)
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
		rrc = read(fd, (p + sofar), needed);
		en = errno;
		if (rrc < 0)
		{
			if (en == EAGAIN || en == EINTR || en == 512)
				continue;
			ostringstream oss;
			oss << "read() returned " << rrc << " (" << strerror(en) << ")";
			throw runtime_error(oss.str());
		}
		needed -= rrc;
		sofar += rrc;
	}
}

#ifdef _MSC_VER
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
			throw runtime_error(oss.str());
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
	if (len > 64)
		throw runtime_error("while reading a string len (>64)");
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
	if (!(h != INVALID_HANDLE_VALUE))
		throw runtime_error("while opening cpipe");
	cleaner.handle = h;

	DWORD nread;
	BOOL drrc;
	drrc = ReadFile(h, &ccount, 8, &nread, 0);
	if (!(drrc != 0 && nread == 8 && ccount < 8 * 1024 * 1024))
		throw runtime_error("while reading from cpipe");

	scoped_array<char> in(new char[ccount]);

	drrc = ReadFile(h, in.get(), (DWORD)ccount, &nread, 0);
	if (!(drrc != 0 && nread == ccount))
		throw runtime_error("while reading from cpipe");

	CloseHandle(h);
	cleaner.handle = INVALID_HANDLE_VALUE;
#else
	int fd = open(cfifo.c_str(), O_RDONLY);
	if (fd < 0)
		throw runtime_error("when opening data fifo for input");

	cleaner.fd = fd;

	readn(fd, &ccount, 8);

	scoped_array<char> in(new char[ccount]);

	readn(fd, in.get(), ccount);

	close(fd);
	cleaner.fd = -1;
#endif
#ifdef _MSC_VER
	h = CreateFile(ufifo.c_str(), GENERIC_WRITE, 0, 0, OPEN_EXISTING, FILE_ATTRIBUTE_NORMAL, 0);
	if (!(h != INVALID_HANDLE_VALUE))
		throw runtime_error("while opening upipe");
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
	if (!(dwrc != 0 && nwritten == 8))
		throw runtime_error("while writing to upipe");

	dwrc = WriteFile(h, out.get(), (DWORD)outlen, &nwritten, 0);
	if (!(dwrc != 0 && nwritten == outlen))
		throw runtime_error("while writing to upipe");

	FlushFileBuffers(h);
	CloseHandle(h);
	cleaner.handle = INVALID_HANDLE_VALUE;
#else
	fd = open(ufifo.c_str(), O_WRONLY);
	if (fd < 0)
		throw runtime_error("when opening data fifo for output");

	cleaner.fd = fd;

	uint64_t outlen = 512 * 1024 * 8;
	unsigned int ol = outlen;

	scoped_array<char> out(new char[outlen]);

	int crc = uncompressBlock(in.get(), ccount, reinterpret_cast<unsigned char*>(out.get()), ol);
	if (crc != ERR_OK)
		outlen = 0;
	else
		outlen = ol;

	rrc = write(fd, &outlen, 8);
	if (rrc != 8)
	{
		throw runtime_error("when writing len to data fifo");
	}
	rrc = write(fd, out.get(), outlen);
	if (rrc != (ssize_t)outlen)
	{
		throw runtime_error("when writing data to data fifo");
	}

	close(fd);
	cleaner.fd = -1;
#endif
}

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
		cerr << "Could not initialize the Decompression Server!" << endl;
		return 1;
	}

	initCtlFifo();

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
		if (dataSock == INVALID_SOCKET)
#else
		if (dataSock < 0)
#endif
			throw runtime_error(string("socket accept error: ") + strerror(errno));
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
			cerr << "Invalid msg" << endl;
	}

	return 0;
}

