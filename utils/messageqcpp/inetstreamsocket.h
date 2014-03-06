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

/***********************************************************************
*   $Id: inetstreamsocket.h 3632 2013-03-13 18:08:46Z pleblanc $
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_INETSTREAMSOCKET_H
#define MESSAGEQCPP_INETSTREAMSOCKET_H

#include <ctime>
#include <unistd.h>
#ifndef _MSC_VER
#include <netinet/in.h>
#endif
#include <cstring>

#include "socket.h"
#include "socketparms.h"
#include "bytestream.h"

#if defined(_MSC_VER) && defined(xxxINETSTREAMSOCKET_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

class MessageQTestSuite;

namespace messageqcpp {
class IOSocket;

/// random # marking the beginning of a ByteStream in the stream
const uint32_t BYTESTREAM_MAGIC = 0x14fbc137;
const uint32_t COMPRESSED_BYTESTREAM_MAGIC = 0x14fbc138;

/** An Inet Stream Socket
 *
 */
class InetStreamSocket : public Socket
{
public:
	/** ctor
	 *
	 */
	explicit InetStreamSocket(size_t blocksize=ByteStream::BlockSize);

	/** dtor
	 *
	 */
	virtual ~InetStreamSocket();

	/** copy ctor
	 *
	 */
	InetStreamSocket(const InetStreamSocket& rhs);

	/** assign op
	 *
	 */
	virtual InetStreamSocket& operator=(const InetStreamSocket& rhs);

	/** fSocket mutator
	 *
	 */
	inline virtual void socketParms(const SocketParms& socket);

	/** fSocket accessor
	 *
	 */
	inline virtual const SocketParms socketParms() const;

	/** sockaddr mutator
	 *
	 */
	inline virtual void sa(const sockaddr* sa);

	/** call socket() to get a sd
	 *
	 */
	virtual void open();

	/** close the sd
	 *
	 */
	virtual void close();

	/** test if this socket is open
	 *
	 */
	inline virtual const bool isOpen() const;

	/** read a message from the socket
	 * 
	 * wait for and return a message from the socket. The deafult timeout waits forever. Note that
	 * eventhough struct timespec has nanosecond resolution, this method only has milisecond resolution.
	 * @warning If you specify a timeout, the stream can be corrupted in certain
	 * extreme circumstances.  The circumstance: receiving a portion of the message
	 * followed by a timeout.  If the rest of the message is ever received, it
	 * will be misinterpreted by the following read().  Symptom:  The caller will 
	 * receive an incomplete ByteStream 
	 * (do try-catch around all ">>" operations to detect underflow).  Mitigation:
	 * the caller should not perform another read().  Caller should close the connection.
	 * The behavior will be unpredictable and possibly fatal.
	 * @note A fix is being reviewed but this is low-priority.
	 */
	virtual const SBS read(const struct timespec* timeout=0, bool* isTimeOut = NULL, Stats *stats = NULL) const;

	/** write a message to the socket
	 * 
	 * write a message to the socket
	 */
	virtual void write(const ByteStream& msg, Stats *stats = NULL);
	virtual void write_raw(const ByteStream& msg, Stats *stats = NULL) const;

	/** this version of write takes ownership of the bytestream
	 */
	virtual void write(SBS msg, Stats *stats = NULL);

	/** bind to a port
	 *
	 */
	virtual void bind(const sockaddr* serv_addr);

	/** listen for connections
	 *
	 */
	virtual void listen(int backlog=5);

	/** return an (accepted) IOSocket ready for I/O
	 *
	 */
	virtual const IOSocket accept(const struct timespec* timeout=0);

	/** connect to a server socket
	 *
	 */
	virtual void connect(const sockaddr* serv_addr);

	/** dynamically allocate a copy of this object
	 *
	 */
	virtual Socket* clone() const;

	/** get a string rep of the object
	 *
	 */
	virtual const std::string toString() const;

	/** set the connection timeout (in ms)
	 *
	 */
	virtual void connectionTimeout(const struct ::timespec* timeout) { if (timeout) fConnectionTimeout = *timeout; }

	/** set the connection protocol to be synchronous
	 *
	 */
	virtual void syncProto(bool use) { fSyncProto = use; }

	const int getConnectionNum() const { return fSocketParms.sd(); }

	/* The caller needs to know when/if the remote closes the connection or sends data.
	 * Returns 0 on timeout, 1 if there is data to read, 2 if the connection was dropped.
	 * On error 3 is returned.
	 */
	static int pollConnection(int connectionNum, long msecs);

	/** return the address as a string
	 *
	 */
	virtual const std::string addr2String() const;

	/** compare 2 addresses
	 *
	 */
	virtual const bool isSameAddr(const Socket* rhs) const;

	/** ping an ip address
	 *
	 */
	EXPORT static int ping(const std::string& ipaddr, const struct timespec* timeout=0);

	/*
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

protected:
	static const int KERR_ERESTARTSYS = 512;

	void logIoError(const char* errMsg, int errNum) const;

	/** Empty the stream up to the beginning of the next ByteStream.
	 *
	 * Reads until the beginning of the next ByteStream is found.
	 * @param msecs An optional timeout value.
	 * @param residual Pass in an array of at least 8 bytes, on return it will contain
	 * the first bytes of the stream.
	 * @param reslen On return, it will contain the # of bytes in residual.
	 * @return true if the next byte in the stream is the beginning of a ByteStream,
	 * false otherwise.
	 */
	virtual bool readToMagic(long msecs, bool* isTimeOut, Stats *stats) const;

	void do_write(const ByteStream &msg, uint32_t magic, Stats *stats = NULL) const;
	ssize_t written(int fd, const uint8_t* ptr, size_t nbytes) const;

	SocketParms fSocketParms;	/// The socket parms
	size_t fBlocksize;
	sockaddr_in fSa;

	// how long to wait for a connect() call to complete (in ms)
	struct ::timespec fConnectionTimeout;

	// use sync proto
	bool fSyncProto;

	/// The buffer used to scan for the ByteStream magic in the stream.
	mutable uint32_t fMagicBuffer;

private:
	void doCopy(const InetStreamSocket& rhs);
};

inline const bool InetStreamSocket::isOpen() const { return (fSocketParms.sd() >= 0); }
inline const SocketParms InetStreamSocket::socketParms() const { return fSocketParms; }
inline void InetStreamSocket::socketParms(const SocketParms& socketParms) { fSocketParms = socketParms; }
inline void InetStreamSocket::sa(const sockaddr* sa) { memcpy(&fSa, sa, sizeof(sockaddr_in)); }

/**
 * stream an InetStreamSocket rep to any ostream
 */
inline std::ostream& operator<<(std::ostream& os, const InetStreamSocket& rhs)
{
	os << rhs.toString();
	return os;
}

} //namespace messageqcpp

#undef EXPORT

#endif //MESSAGEQCPP_INETSTREAMSOCKET_H

