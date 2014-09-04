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

/******************************************************************************
 * $Id$
 *
 *****************************************************************************/

/** @file umsocketselector.h
 * Used in selecting the "next" socket/port when sending a response message
 * to a UM module.  UmSocketSelector is the public API class.  UmModuleIPs
 * is a supporting class of UmSocketSelector, and UmIPSocketConns is in turn
 * a supporting class of UmModuleIPs.
 */

#ifndef UMSOCKETSELECTOR_H__
#define UMSOCKETSELECTOR_H__

#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#include <stdint.h>
typedef boost::uint32_t in_addr_t;
#else
#include <netinet/in.h>
#endif
#include <list>
#include <map>
#include <sstream>
#include <string>
#include <vector>
#include <boost/shared_ptr.hpp>
#include <boost/thread.hpp>

#include "iosocket.h"

using namespace messageqcpp;

namespace primitiveprocessor
{
class UmModuleIPs;
class UmIPSocketConns;

typedef boost::shared_ptr<UmModuleIPs>     SP_UM_MODIPS;
typedef boost::shared_ptr<UmIPSocketConns> SP_UM_IPCONNS;
typedef boost::shared_ptr<IOSocket>		   SP_UM_IOSOCK;
typedef boost::shared_ptr<boost::mutex>	   SP_UM_MUTEX;

//------------------------------------------------------------------------------
/** @brief Public API class used to track and select socket for outgoing msgs.
 *
 * This class maintains a list of UM's and the corresponding IP addresses for
 * each UM.  In addition, a list of socket/port connections are maintained for
 * each IP addresss.  nextIOSocket() can be used to iterate through all the IP
 * addresses and socket/port connections for a given UM, as response messages
 * are sent to the UM.  This action attempts to see that the output network
 * traffic is evenly distributed among all the NIC's for a UM.
 *
 * This class is not entirely thread-safe, as the first call to instance() must
 * be made from a single threaded envirionment as part of initialization. After
 * that, the class is thread-safe through the use of a mutex in UmModuleIPs.
 */
//------------------------------------------------------------------------------
class UmSocketSelector
{
	public:
		typedef std::map<in_addr_t,unsigned int> IpAddressUmMap_t;

		/** @brief Singleton accessor to UmSocketSelector instance.
		 *
		 * This method should be called once from the main thread to perform
		 * initialization from a single threaded environment.
		 */
		static UmSocketSelector* instance();

		/** @brief UmSocketSelector destructor
		 *
		 */
		~UmSocketSelector()
		{ };

		/** @brief Accessor to total number of UM IP's in Calpont.xml.
		 *
		 * @return Number of UM IP addresses read from Calpont.xml.
		 */
		uint32_t ipAddressCount() const;

		/** @brief Add a socket/port connection to the connection list.
		 *
		 * @param ios (in) socket/port connection to be added.
		 * @param writeLock (in) mutex to use when writing to ios.
		 * @return boolean indicating if socket/port connection was added.
		 */
		bool addConnection( const SP_UM_IOSOCK& ios,
				const SP_UM_MUTEX& writeLock );

		/** @brief Delete a socket/port connection from the connection list.
		 *
		 * @param ios (in) socket/port connection to be removed.
		 */
		void delConnection( const IOSocket& ios );

		/** @brief Get the next output IOSocket to use for the specified ios.
		 *
		 * @param ios (in) socket/port connection for the incoming message.
		 * @param outIos (out) socket/port connection to use for the response.
		 * @param writeLock (out) mutex to use when writing to outIos.
		 * @return boolean indicating if operation was successful.
		 */
		bool nextIOSocket( const IOSocket& ios, SP_UM_IOSOCK& outIos,
				SP_UM_MUTEX& writeLock );

		/** @brief toString method used in logging, debugging, etc.
		 *
		 */
		const std::string toString() const;

	private:
		//...Disable default copy constructor and assignment operator by
		//   declaring but not defining
		UmSocketSelector  (const UmSocketSelector& rhs);
		UmSocketSelector& operator=(const UmSocketSelector& rhs);

		UmSocketSelector();
		void loadUMModuleInfo();
		unsigned int findOrAddUm( const std::string& moduleName );

		static UmSocketSelector* fpUmSocketSelector;
		std::vector<SP_UM_MODIPS> fUmModuleIPs; // UM's and their sockets

		// std::map that maps an IP address to an index into fUmModuleIPs
		IpAddressUmMap_t		  fIpAddressUmMap;
};

//------------------------------------------------------------------------------
/** @brief Tracks and selects "next" socket/port for a UM module.
 *
 * This is a supporting class to UmSocketSelector.
 * This class maintains a list of IP addresses (and their corrresponding
 * socket/port connections) for a specific UM module.  UmModuleIPs can
 * be used to iterate through the available IP addresses (and socket/port
 * connections) for a UM module, as response messages are sent out by PrimProc.
 *
 * This class is thread-safe.
 */
//------------------------------------------------------------------------------
class UmModuleIPs
{
	public:
		/** @brief UmModuleIPs constructor.
		 *
		 * @param moduleName (in) UM module name for this UmModuleIPs object.
		 */
		explicit UmModuleIPs ( const std::string& moduleName ) :
			fUmModuleName(moduleName),
			fNextUmIPSocketIdx(NEXT_IP_SOCKET_UNASSIGNED)
		{ }

		/** @brief UmModuleIPs destructor.
		 *
		 */
		~UmModuleIPs ( )
		{ };

		/** @brief Accessor to number of IP's from Calpont.xml for this UM.
		 *
		 * @return Number of IP addresses read from Calpont.xml for this UM.
		 */
		uint32_t ipAddressCount() const
		{ return fUmIPSocketConns.size(); }

		/** @brief Accessor to the module name for this UmModuleIPs object.
		 *
		 * @return UM module name.
		 */
		const std::string& moduleName() const
		{ return fUmModuleName; }

		/** @brief Add an IP address to this UM module.
		 *
		 * @param ip (in) IP address to be added (in network byte order)
		 */
		void addIP         ( in_addr_t ip );

		/** @brief Add specified socket/port to the connection list for this UM.
		 *
		 * @param ioSock (in) socket/port to add to the connection list.
		 * @param writeLock (in) mutex to use when writing to ioSock.
		 * @return boolean indicating if socket/port connection was added.
		 */
		bool addSocketConn ( const SP_UM_IOSOCK& ioSock,
				const SP_UM_MUTEX& writeLock );

		/** @brief Delete specified socket/port from the connection list.
		 *
		 * @param ioSock (in) socket/port to delete from the connection list.
		 */
		void delSocketConn ( const IOSocket& ioSock );

		/** @brief Get the "next" available socket/port for this UM module.
		 *
		 * @param outIos (out) socket/port connection to use for the response.
		 * @param writeLock (out) mutex to use when writing to outIos.
		 * @return bool flag indicating whether a socket/port was available.
		 */
		bool nextIOSocket  ( SP_UM_IOSOCK& outIos, SP_UM_MUTEX& writeLock );

		/** @brief toString method used in logging, debugging, etc.
		 *
		 */
		const std::string toString();

	private:
		void advanceToNextIP();

		static const int32_t	NEXT_IP_SOCKET_UNASSIGNED;
		std::string				fUmModuleName;      // UM module name
		int32_t					fNextUmIPSocketIdx; //index to "next" IP address
		boost::mutex            fUmModuleMutex;

		// collection of IP addresses and their corresponding socket/port conns
		std::vector<SP_UM_IPCONNS> fUmIPSocketConns;
};

//------------------------------------------------------------------------------
/** @brief Tracks and selects "next" socket/port for a UM module IP address.
 *
 * This is a supporting class to UmModuleIPs.
 * This class maintains a list of socket/port connections for a specific UM
 * module IP address.  UmIPSocketConns can be used to iterate through the
 * available connections for an IP address, as response messages are sent out
 * by PrimProc.
 *
 * This class by itself is not thread-safe.  However, UmModulesIPs, which
 * uses UmIPSocketConns, insures thread-safeness.
 */
//------------------------------------------------------------------------------
class UmIPSocketConns
{
	public:
		struct UmIOSocketData
		{
			SP_UM_IOSOCK fSock; // an output IOSocket
			SP_UM_MUTEX	fMutex; // mutex to be use when writing to fSock
		};

		/** @brief UmIPSocketConns constructor.
		 *
		 * @param ip (in) IP address for this UmIPSocketConns object.
		 */
		explicit UmIPSocketConns( in_addr_t ip ) :
			fIpAddress(ip),
			fNextIOSocketIdx(NEXT_IOSOCKET_UNASSIGNED)
		{ }

		/** @brief UmIPSocketConns destructor.
		 *
		 */
		~UmIPSocketConns( )
		{ }

		/** @brief Accessor to the IP address for this UmIPSocketConns object.
		 *
		 * @return IP address (in network byte order).
		 */
		in_addr_t ipAddress ( )
		{ return fIpAddress; }

		/** @brief Accessor to socket/port connection count for this IP address.
		 *
		 * @return socket/port connection count.
		 */
		uint32_t count( )
		{ return fIOSockets.size(); }

		/** @brief Add specified socket/port to the connection list.
		 *
		 * @param ioSock (in) socket/port to add to the connection list.
		 * @param writeLock (in) mutex to use when writing to ioSock.
		 */
		void addSocketConn ( const SP_UM_IOSOCK& ioSock,
				const SP_UM_MUTEX& writeLock );

		/** @brief Delete specified socket/port from the connection list.
		 *
		 * @param ioSock (in) socket/port to delete from the connection list.
		 */
		void delSocketConn ( const IOSocket& ioSock );

		/** @brief Get the "next" available socket/port for this IP address.
		 *
		 * @param outIos (out) socket/port connection to use for the response.
		 * @param writeLock (out) mutex to use when writing to outIos.
		 */
		void nextIOSocket ( SP_UM_IOSOCK& outIos, SP_UM_MUTEX& writeLock );

		/** @brief Convert network byte ordered IP address to string
		 *
		 * @param ipString (out) IP address string;
		 *  ipString must be an array of size INET_ADDRSTRLEN.
		 * @return IP address string (same as ipString)
		 */
		static char* nwToString( in_addr_t addr, char* ipString  );

		/** @brief toString method used in logging, debugging, etc.
		 *
		 */
		const std::string toString() const;

	private:
		static const int32_t	NEXT_IOSOCKET_UNASSIGNED;
		in_addr_t				fIpAddress;  // IP address in network byte order
		int32_t					fNextIOSocketIdx;//index to "next" socket/port
		std::vector<UmIOSocketData> fIOSockets;//socket/port list for fIpAddress
};

}

#endif
