/* Copyright (C) 2013 Calpont Corp.

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

/******************************************************************************************
* $Id: messagequeue.h 3632 2013-03-13 18:08:46Z pleblanc $
*
*
******************************************************************************************/
/** @file */
#ifndef MESSAGEQCPP_MESSAGEQUEUE_H
#define MESSAGEQCPP_MESSAGEQUEUE_H
#include <string>
#include <ctime>

#include <sys/types.h>
#ifdef _MSC_VER
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <netinet/in.h>
#endif

#include "serversocket.h"
#include "iosocket.h"
#include "bytestream.h"
#include "logger.h"
namespace config
{
class Config;
}

class MessageQTestSuite;

#if defined(_MSC_VER) && defined(xxxMESSAGEQUEUE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace messageqcpp { 

/**
 * @brief a message queue server
 * This class can recieve (and send) messages
 *
 * MessageQueueServer looks in the config file specified by cfile for an entry like:
 * @code
 * <thisEnd>
 *    <IPAddr>127.0.0.1</IPAddr>
 *    <Port>7601</Port>
 * </thisEnd>
 * @endcode
 * and binds/listens/accepts on the port specified. It ignores the IPAddr setting.
 *
 * Users of this class need to decide whether they want to use it in a single-threaded-
 * compatible (message-at-a-time) mode or in a multi-threaded-compatible (connection-per-
 * thread) mode. For the former, read() and write() are available, and accept() cannot be
 * used. In the latter, the opposite applies.
 */
class MessageQueueServer
{
public:
	/**
	 * @brief construct a server queue for thisEnd
	 * 
	 * construct a server queue for thisEnd. Optionally specify a Config object to use.
	 */
	EXPORT explicit MessageQueueServer(const std::string& thisEnd, config::Config* config=0,
		size_t blocksize=ByteStream::BlockSize, int backlog=5, bool syncProto=true);

	/**
	 * @brief construct a server queue for thisEnd
	 * 
	 * construct a server queue for thisEnd, specifying the name of a config file to use.
	 */
	EXPORT MessageQueueServer(const std::string& thisEnd, const std::string& config,
		size_t blocksize=ByteStream::BlockSize, int backlog=5, bool syncProto=true);

	/**
	 * @brief destructor
	 */
	EXPORT ~MessageQueueServer();
// 
	/**
	 * @brief wait for a connection and return an IOSocket
	 *
	 * This method can be used by a main thread to wait for an incoming connection. The IOSocket
	 * that is returned can be passed to a thread to handle the socket connection. The main thread
	 * is then free to wait again for another connection. The IOSocket is already open and ready for
	 * read() and/or write(). The caller is responsible for calling close() when it is done.
	 */
	EXPORT const IOSocket accept(const struct timespec* timeout=0) const;

	/**
	 * @brief get a mutable pointer to the client IOSocket
	 */
	inline IOSocket& clientSock() const;

	/**
	 * @brief set the sync proto
	 */
	EXPORT void syncProto(bool use);

    /**
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

private:
	/** copy ctor
	 *
	 */
	MessageQueueServer(const MessageQueueServer& rhs);

	/** assign op
	 *
	 */
	MessageQueueServer& operator=(const MessageQueueServer& rhs);

	/** ctor helper
	 *
	 */
	void setup(size_t blocksize, int backlog, bool syncProto);

	std::string fThisEnd;		/// the process name for this process
	struct sockaddr fServ_addr;	/// the addr of the server (may be this process)
	config::Config* fConfig;			/// config file has the IP addrs and port numbers
	mutable ServerSocket fListenSock;	/// the socket the server listens on for new connections
	mutable IOSocket fClientSock;	/// the socket connected to a client

	mutable logging::Logger fLogger;
};

/**
 * @brief a message queue client
 * This class can send (and recieve) messages
 *
 * MessageQueueClient looks in the config file specified by cfile for an entry like:
 * @code
 * <otherEnd>
 *    <IPAddr>127.0.0.1</IPAddr>
 *    <Port>7601</Port>
 * </otherEnd>
 * @endcode
 * and connects to the ipaddr/port specified. It does a "lazy" connect in that it doesn't
 * actually connect until read() is invoked the first time.
 */
class MessageQueueClient
{
public:
	/**
	 * @brief construct a queue to otherEnd
	 * 
	 * construct a queue from this process to otherEnd. Optionally specify a Config object to use.
	 */
	EXPORT explicit MessageQueueClient(const std::string& otherEnd, config::Config* config=0, bool syncProto=true);

	/**
	 * @brief construct a queue to otherEnd
	 * 
	 * construct a queue from this process to otherEnd, specifying the name of a config file to use.
	 */
	EXPORT explicit MessageQueueClient(const std::string& otherEnd, const std::string& config, bool syncProto=true);

	/**
	 * @brief destructor
	 *
	 * calls shutdown() method.
	 */
	EXPORT ~MessageQueueClient();

	/**
	 * @brief read a message from the queue
	 * 
	 * wait for and return a message from otherEnd. The deafult timeout waits forever. Note that
	 * eventhough struct timespec has nanosecond resolution, this method only has milisecond resolution.
	 */
	EXPORT const SBS read(const struct timespec* timeout=0, bool* isTimeOut = NULL, Stats *stats = NULL) const;

	/**
	 * @brief write a message to the queue
	 * 
	 * write a message to otherEnd. If the socket is not open, the timeout parm (in ms) will be used
	 *  to establish a sync connection w/ the server
	 */
	EXPORT void write(const ByteStream& msg, const struct timespec* timeout=0, Stats *stats = NULL) const;

	/**
	 * @brief shutdown the connection to the server
	 *
	 * indicate to the class that the user is done with the socket
	 * and the other class methods won't be used.
	 */
	EXPORT void shutdown();

	/**
	 * @brief connect to the server. Returns true if connection was successful.
	 *
	 * read() and write() automatically connect, but this method can be used to verify a server is listening
	 * before that.
	 */
	EXPORT bool connect() const;
	
	/**
	 * @brief accessors and mutators
	 */    
	EXPORT const sockaddr serv_addr() const { return fServ_addr; }
	EXPORT const std::string otherEnd() const { return fOtherEnd; }
	EXPORT const bool isAvailable() const { return fIsAvailable; }
	EXPORT void isAvailable (const bool isAvailable) { fIsAvailable = isAvailable; }
	EXPORT const std::string moduleName() const {return fModuleName;}
	EXPORT void moduleName(const std::string& moduleName) {fModuleName = moduleName;}

	/**
	 * @brief set the sync proto
	 */
	inline void syncProto(bool use);

	/**
	 * @brief return the address as a string
	 */
	inline const std::string addr2String() const;

	/**
	 * @brief compare the addresses of 2 MessageQueueClient
	 */
	inline const bool isSameAddr(const MessageQueueClient& rhs) const;

	/*
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

private:
	/** copy ctor
	 *
	 */
	MessageQueueClient(const MessageQueueClient& rhs);

	/** assign op
	 *
	 */
	MessageQueueClient& operator=(const MessageQueueClient& rhs);

	/** ctor helper
	 *
	 */
	void setup(bool syncProto);

	std::string fOtherEnd;		/// the process name for this process
	struct sockaddr fServ_addr;	/// the addr of the server (may be this process)
	config::Config* fConfig;			/// config file has the IP addrs and port numbers
	mutable IOSocket fClientSock;	/// the socket to communicate with the server
	mutable logging::Logger fLogger;
	bool fIsAvailable;
	std::string fModuleName;
};

inline IOSocket& MessageQueueServer::clientSock() const { return fClientSock; }
inline const std::string MessageQueueClient::addr2String() const { return fClientSock.addr2String(); }
inline const bool MessageQueueClient::isSameAddr(const MessageQueueClient& rhs) const
	{ return fClientSock.isSameAddr(&rhs.fClientSock); }
inline void MessageQueueClient::syncProto(bool use) { fClientSock.syncProto(use); }

} 

#undef EXPORT

#endif //MESSAGEQCPP_MESSAGEQUEUE_H
// vim:ts=4 sw=4:

