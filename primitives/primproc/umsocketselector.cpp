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

/******************************************************************************
 * $Id$
 *
 *****************************************************************************/

/** @file umsocketselector.cpp
 * Used to iterate through available socket/port connections for a given UM,
 * when sending response messages back to the UM.
 */

#include "umsocketselector.h"
#ifdef _MSC_VER
#define WIN32_LEAN_AND_MEAN
#define NOMINMAX
#include <windows.h>
#include <winsock2.h>
#include <ws2tcpip.h>
#include <stdio.h>
#else
#include <arpa/inet.h>
#include <sys/socket.h>
#endif
#include <iostream>
#include <climits>
#include <sstream>
#include <cstring>
#include <limits>

#include "liboamcpp.h"
using namespace oam;

//#define NDEBUG
#include <cassert>

//#define LOAD_MODULE_DEBUG 1
//#define MOD_CONN_DEBUG    1
//#define SEL_CONN_DEBUG    1

namespace primitiveprocessor
{
	/*static*/ const int32_t UmIPSocketConns::NEXT_IOSOCKET_UNASSIGNED = -1;
	/*static*/ const int32_t UmModuleIPs::NEXT_IP_SOCKET_UNASSIGNED    = -1;
	/*static*/ UmSocketSelector* UmSocketSelector::fpUmSocketSelector  =  0;

//------------------------------------------------------------------------------
// UmSocketSelector methods
//------------------------------------------------------------------------------
// UmSocketSelector Singleton accessor
//------------------------------------------------------------------------------
/* static */ UmSocketSelector*
UmSocketSelector::instance()
{
	if (fpUmSocketSelector == 0)
	{
		fpUmSocketSelector = new UmSocketSelector();
	}

	return fpUmSocketSelector;
}

//------------------------------------------------------------------------------
// UmSocketSelector constructor
//------------------------------------------------------------------------------
UmSocketSelector::UmSocketSelector()
{
	loadUMModuleInfo();
}

//------------------------------------------------------------------------------
// Returns the number of IP addresses defined in the Calpont.xml file
// return - uint32_t; the total number of IP addresses in the Calpont.xml file
//------------------------------------------------------------------------------
uint32_t
UmSocketSelector::ipAddressCount() const
{
	uint32_t ipCount = 0;

	for (unsigned int i=0; i<fUmModuleIPs.size(); ++i)
	{
		ipCount += fUmModuleIPs[i]->ipAddressCount();
	}

	return ipCount;
}

//------------------------------------------------------------------------------
// Loads the UM module information from the Calpont.xml file, so that we have
// the list of IP addresses that are valid for each UM.  Note that this method
// does not insure thread safeness, but that's okay because it is assumed that
// it will only be called once from the first call to instance().
//------------------------------------------------------------------------------
void
UmSocketSelector::loadUMModuleInfo()
{
	Oam					oam;
	ModuleTypeConfig	moduleTypeConfig;
	const std::string	UM_MODTYPE("um");

	oam.getSystemConfig(UM_MODTYPE, moduleTypeConfig);

	int			moduleCount= moduleTypeConfig.ModuleCount;
	std::string	moduleType = moduleTypeConfig.ModuleType;

#ifdef LOAD_MODULE_DEBUG
	std::cout << "ModuleConfig for type: " << UM_MODTYPE         << std::endl;
	std::cout << "ModuleDesc  = " << moduleTypeConfig.ModuleDesc << std::endl;
	std::cout << "ModuleCount = " << moduleCount                 << std::endl;
	std::cout << "RunType     = " << moduleTypeConfig.RunType    << std::endl;
#endif

	if ( moduleCount > 0 )
	{
		//..Loop through the list of UM modules
		for (DeviceNetworkList::iterator iter1 =
				moduleTypeConfig.ModuleNetworkList.begin();
			(iter1 != moduleTypeConfig.ModuleNetworkList.end());
			++iter1)
		{
			std::string moduleName = iter1->DeviceName;

#ifdef LOAD_MODULE_DEBUG
			std::cout << "ModuleName-" << moduleName << std::endl;
#endif

			//..Assign the UM index based on whether it is a new UM or one
			//  we have seen before
			unsigned int umIdx = findOrAddUm( moduleName );

			//..Get the list of IP addresses (NIC's) for this UM module
			for (HostConfigList::iterator iter2 = iter1->hostConfigList.begin();
				(iter2 != iter1->hostConfigList.end());
				++iter2)
			{
				std::string ipAddr = iter2->IPAddr;

#ifdef LOAD_MODULE_DEBUG
				std::cout << "  NIC-" << iter2->NicID <<
					"; host-" << iter2->HostName <<
					"; IP-"   << ipAddr          << std::endl;
#endif

				struct in_addr ip;
				if ( inet_aton(ipAddr.c_str(), &ip ) )
				{
					fIpAddressUmMap[ ip.s_addr ] = umIdx;
					fUmModuleIPs[umIdx]->addIP( ip.s_addr );
				}
				else
				{
					std::cerr << "Invalid IP address in SystemModuleConfig "
						"section: " << ipAddr << std::endl;
				}
			} // loop through the IP addresses for a UM module
		} // loop through the list of UM modules
	} // moduleCount > 0
}

//------------------------------------------------------------------------------
// Search for the specified moduleName, and return the applicable index from
// fUmModuleIPs, if it is found.  Else add the new moduleName.
// return - unsigned int for the index into fUmModuleIPs for moduleName.
//------------------------------------------------------------------------------
unsigned int
UmSocketSelector::findOrAddUm( const std::string& moduleName )
{
	unsigned int umIdx = std::numeric_limits<unsigned int>::max();
	for (unsigned int i=0; i<fUmModuleIPs.size(); ++i)
	{
		if (fUmModuleIPs[i]->moduleName() == moduleName)
		{
			umIdx = i;
			return umIdx;
		}
	}

	//..We have encountered a new UM module we should add to the list
	fUmModuleIPs.push_back( SP_UM_MODIPS(new UmModuleIPs(moduleName)) );
	umIdx = fUmModuleIPs.size() - 1;

	return umIdx;
}

//------------------------------------------------------------------------------
// Add a new socket/port connection.  It will be grouped with other
// socket/port connections belonging to the same UM.
// ios (in)       - socket/port connection to be added
// writeLock (in) - mutex to use when writing to ios.
// return         - boolean indicating whether socket/port connection was added.
//------------------------------------------------------------------------------
bool
UmSocketSelector::addConnection(
	const SP_UM_IOSOCK& ios,
	const SP_UM_MUTEX&  writeLock )
{
	bool bConnAdded = false;

	sockaddr sa = ios->sa();
	const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&sa);
	IpAddressUmMap_t::iterator mapIter =
		fIpAddressUmMap.find ( sinp->sin_addr.s_addr );

	// Add this socket/port connection to the UM connection list it belongs to.
	if ( mapIter != fIpAddressUmMap.end() )
	{
		unsigned int umIdx = mapIter->second;
		bConnAdded = fUmModuleIPs[umIdx]->addSocketConn( ios, writeLock );
	}

	if (!bConnAdded)
	{
#ifdef SEL_CONN_DEBUG
		std::ostringstream oss;
		oss << "No UM/IP match found to add connection " << ios->toString() <<
			std::endl;
		std::cout << oss.str();
#endif
	}

	return bConnAdded;
}

//------------------------------------------------------------------------------
// Delete a socket/port connection from the UM for which it belongs.
// ioSock (in) - socket/port connection to be deleted
//------------------------------------------------------------------------------
void
UmSocketSelector::delConnection( const IOSocket& ios )
{
	sockaddr sa = ios.sa();
	const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&sa);
	IpAddressUmMap_t::iterator mapIter =
		fIpAddressUmMap.find ( sinp->sin_addr.s_addr );

	if ( mapIter != fIpAddressUmMap.end() )
	{
		unsigned int umIdx = mapIter->second;
		fUmModuleIPs[umIdx]->delSocketConn( ios );
	}
}

//------------------------------------------------------------------------------
// Get the next socket/port connection belonging to the same UM as ios.
// The selected socket/port connection will be returned in outIos.  It can
// then be used for sending the next response message back to the applicable
// UM module.
// ios (in)        - socket/port connection where a UM request originated from
// outIos (out)    - socket/port connection to use in sending the
//                   corresponding response
// writelock (out) - mutex lock to be used when writing to outIos
// return          - bool indicating if socket/port connection was assigned to
//                   outIos
//------------------------------------------------------------------------------
bool
UmSocketSelector::nextIOSocket(
	const IOSocket& ios,
	SP_UM_IOSOCK&   outIos,
	SP_UM_MUTEX&    writeLock )
{
	sockaddr sa = ios.sa();
	const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&sa);
	IpAddressUmMap_t::iterator mapIter =
		fIpAddressUmMap.find ( sinp->sin_addr.s_addr );

	if ( mapIter != fIpAddressUmMap.end() )
	{
		unsigned int umIdx = mapIter->second;
		if (fUmModuleIPs[umIdx]->nextIOSocket( outIos, writeLock ))
		{

#ifdef SEL_CONN_DEBUG
			std::ostringstream oss;
			oss << "UM " << fUmModuleIPs[umIdx]->moduleName() <<
				"; in: " << ios.toString() <<
				"; selected out: " << outIos->toString() << std::endl;
			std::cout << oss.str();
#endif

			return true;
		}
	}

	//..This should not happen.  Application is asking for next socket/port for
	//  a connection not in our UM module list.
	return false;
}

//------------------------------------------------------------------------------
// Convert contents to string for logging, debugging, etc.
//------------------------------------------------------------------------------
const std::string
UmSocketSelector::toString() const
{
	std::ostringstream oss;

	oss << "IP Address to UM index map:" << std::endl;
	for (IpAddressUmMap_t::const_iterator mapIter = fIpAddressUmMap.begin();
		(mapIter != fIpAddressUmMap.end());
		++mapIter)
	{
		char ipString[INET_ADDRSTRLEN];
		oss << "  IPAddress: " <<
			UmIPSocketConns::nwToString(mapIter->first, ipString) <<
			" maps to UM: "  << mapIter->second << std::endl;
	}

	for (unsigned int i=0; i<fUmModuleIPs.size(); ++i)
	{
		oss << std::endl << fUmModuleIPs[i]->toString();
	}

	return oss.str();
}

//------------------------------------------------------------------------------
// UmModuleIPs methods
//------------------------------------------------------------------------------
// Add an IP address to be associated with this UM module.
// ip (in) - IP address to associate with this UM module (in network byte order)
//------------------------------------------------------------------------------
void
UmModuleIPs::addIP( in_addr_t ip )
{
	boost::mutex::scoped_lock lock( fUmModuleMutex );

#ifdef MOD_CONN_DEBUG
	std::ostringstream oss;
	char ipString[INET_ADDRSTRLEN];
	oss << "    UM " << fUmModuleName << "; adding IP: " <<
		UmIPSocketConns::nwToString(ip,ipString) << std::endl;
	std::cout << oss.str();
#endif

	fUmIPSocketConns.push_back( SP_UM_IPCONNS(new UmIPSocketConns(ip)) );
}

//------------------------------------------------------------------------------
// Add a new socket/port connection to this UM.  It will be grouped with other
// socket/port connections having the same IP address for this UM.
// ioSock (in)    - socket/port connection to be added
// writeLock (in) - mutex to use when writing to ioSock.
// return         - boolean indicating whether socket/port connection was added.
//------------------------------------------------------------------------------
bool
UmModuleIPs::addSocketConn(
	const SP_UM_IOSOCK& ioSock,
	const SP_UM_MUTEX&  writeLock )
{
	bool bConnAdded = false;

	boost::mutex::scoped_lock lock( fUmModuleMutex );
	for (unsigned int i=0; i<fUmIPSocketConns.size(); ++i)
	{
		sockaddr sa = ioSock->sa();
		const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&sa);
		if (fUmIPSocketConns[i]->ipAddress() == sinp->sin_addr.s_addr)
		{

#ifdef MOD_CONN_DEBUG
			std::ostringstream oss;
			oss << "UM " << fUmModuleName << "; adding connection " <<
				ioSock->toString() << std::endl;
			std::cout << oss.str();
#endif

			fUmIPSocketConns[i]->addSocketConn ( ioSock, writeLock );
			bConnAdded = true;

			//..Initialize fNextUmIPSocketIdx if this is the first socket/port
			//  connection for this UM.
			if ( fNextUmIPSocketIdx == NEXT_IP_SOCKET_UNASSIGNED)
				fNextUmIPSocketIdx = i;
			break;
		}
	}

	return bConnAdded;
}

//------------------------------------------------------------------------------
// Delete a socket/port connection from this UM.
// ioSock (in) - socket/port connection to be deleted
//------------------------------------------------------------------------------
void
UmModuleIPs::delSocketConn( const IOSocket& ioSock )
{
	boost::mutex::scoped_lock lock( fUmModuleMutex );
	for (unsigned int i=0; i<fUmIPSocketConns.size(); ++i)
	{
		sockaddr sa = ioSock.sa();
		const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&sa);
		if (fUmIPSocketConns[i]->ipAddress() == sinp->sin_addr.s_addr)
		{

#ifdef MOD_CONN_DEBUG
			std::ostringstream oss;
			oss << "UM " << fUmModuleName << "; deleting connection "<<
				ioSock.toString() << std::endl;
			std::cout << oss.str();
#endif

			fUmIPSocketConns[i]->delSocketConn ( ioSock );

			//..If we just deleted the last connection for this IP, then ad-
			//  vance fNextUmIPSocketIdx to an IP address having connections.
			if (fUmIPSocketConns[i]->count() == 0)
			{
				advanceToNextIP();
			}
			break;
		}
	}
}

//------------------------------------------------------------------------------
// Get the next socket/port connection for this UM to use in sending a message
// to the applicable UM module.
// outIos (out) - socket/port connection to use in sending next msg
// writelock (out) - mutex lock to be used when writing to outIos
// return          - bool indicating if socket/port connection was assigned to
//                   outIos
//------------------------------------------------------------------------------
bool
UmModuleIPs::nextIOSocket( SP_UM_IOSOCK& outIos, SP_UM_MUTEX& writeLock )
{
	bool found = false;

	boost::mutex::scoped_lock lock( fUmModuleMutex );
	if ((fUmIPSocketConns.size() > 0) &&
		(fNextUmIPSocketIdx != NEXT_IP_SOCKET_UNASSIGNED))
	{
		assert (fNextUmIPSocketIdx < static_cast<int>(fUmIPSocketConns.size()));
		fUmIPSocketConns[fNextUmIPSocketIdx]->nextIOSocket( outIos, writeLock );
		advanceToNextIP();
		found = true;
	}

	return found;
}

//------------------------------------------------------------------------------
// Advance to the "next" available IP (for this UM), skipping over
// any IP's that have 0 socket/port connections.  No mutex locking done, as
// the calling methods will have already locked a mutex.
//------------------------------------------------------------------------------
void
UmModuleIPs::advanceToNextIP()
{
	if ( fUmIPSocketConns.size() > 1 )
	{
		//..Search to end of IP list for an IP having 1 or more connections
		for (int i=fNextUmIPSocketIdx+1;
			i<static_cast<int>(fUmIPSocketConns.size());
			++i)
		{
			if (fUmIPSocketConns[i]->count() > 0)
			{
				fNextUmIPSocketIdx = i;
				return;
			}
		}

		//..Wrap back around to the start of the list, to continue the search
		for (int i=0; i<fNextUmIPSocketIdx; ++i)
		{
			if (fUmIPSocketConns[i]->count() > 0)
			{
				fNextUmIPSocketIdx = i;
				return;
			}
		}

		fNextUmIPSocketIdx = NEXT_IP_SOCKET_UNASSIGNED;
	}
	else // special logic to handle 0 fUmIPSocketConns, or a single empty one
	{
		if ((fUmIPSocketConns.size() == 0) ||
			(fUmIPSocketConns[0]->count() == 0))
		{
			fNextUmIPSocketIdx = NEXT_IP_SOCKET_UNASSIGNED;
		}
	}
}

//------------------------------------------------------------------------------
// Convert contents to string for logging, debugging, etc.
//------------------------------------------------------------------------------
const std::string
UmModuleIPs::toString()
{
	std::ostringstream oss;

	boost::mutex::scoped_lock lock( fUmModuleMutex );
	oss << "UM module name: " << fUmModuleName <<
		"; nextUmIPIdx: " << fNextUmIPSocketIdx << std::endl;
	for (unsigned int i=0; i<fUmIPSocketConns.size(); ++i)
	{
		oss << fUmIPSocketConns[i]->toString();
	}

	return oss.str();
}

//------------------------------------------------------------------------------
// UmIPSocketConns methods
//------------------------------------------------------------------------------
// Add the specified socket/port to the connection list for the IP address
// represented by this UmIPSocketConns object.
// ioSock (in)    - socket/port connection to be added
// writeLock (in) - mutex to use when writing to ioSock
//------------------------------------------------------------------------------
void
UmIPSocketConns::addSocketConn(
	const SP_UM_IOSOCK& ioSock,
	const SP_UM_MUTEX&  writeLock )
{
	UmIOSocketData sockData = {
		SP_UM_IOSOCK(ioSock), SP_UM_MUTEX(writeLock) };
	fIOSockets.push_back( sockData );

	//..Initialize fNextIOSocketIdx when we add first connection
	if (fIOSockets.size() == 1)
		fNextIOSocketIdx = 0;
}

//------------------------------------------------------------------------------
// Delete the specified socket/port from the connection list for the IP address
// referenced by this UmIPSocketConns object.
// ioSock (in) - socket/port connection to be deleted
//
// Not normally a good thing to be using a std::vector if we are going to be
// deleting elements in the middle of the collection.  Very inefficient.
// But this method won't be called often, and we are only dealing with a small
// collection.  Plus we want to use a vector over a list, so that nextIOSocket()
// can benefit from quick random access.
//------------------------------------------------------------------------------
void
UmIPSocketConns::delSocketConn( const IOSocket& ioSock )
{
	for (unsigned int i=0; i<fIOSockets.size(); ++i)
	{
		sockaddr sa1 = fIOSockets[i].fSock->sa();
		const sockaddr_in* sinp1 = reinterpret_cast<const sockaddr_in*>(&sa1);
		sockaddr sa2 = ioSock.sa();
		const sockaddr_in* sinp2 = reinterpret_cast<const sockaddr_in*>(&sa2);
		if (sinp1->sin_port == sinp2->sin_port)
		{
			fIOSockets.erase ( fIOSockets.begin()  + i );

			//..Adjust fNextIOSocketIdx
			//  1a. decrement if fNextIOSocketIdx is after deleted connection
			//  1b. reset to start of vector if we deleted the last connection
			//  2.  reset fNextIOSocketIdx to -1 if we have no more connections
			if (fIOSockets.size() > 0)
			{
				if (fNextIOSocketIdx > static_cast<int>(i))
					fNextIOSocketIdx--;
		
				if ( fNextIOSocketIdx >= static_cast<int>(fIOSockets.size()) )
					fNextIOSocketIdx = 0;
			}
			else
			{
				fNextIOSocketIdx = NEXT_IOSOCKET_UNASSIGNED;
			}

			break;
		}
	}
}

//------------------------------------------------------------------------------
// Get the next socket/port connection for this IP to use in sending a message
// to the applicable UM module.
// outIos (out)    - socket/port connection to use in sending next msg
// writelock (out) - mutex lock to be used when writing to outIos
//------------------------------------------------------------------------------
void
UmIPSocketConns::nextIOSocket ( SP_UM_IOSOCK& outIos, SP_UM_MUTEX& writeLock )
{
	assert (fIOSockets.size() > 0);
	assert (fNextIOSocketIdx != NEXT_IOSOCKET_UNASSIGNED);
	assert (fNextIOSocketIdx  < static_cast<int>(fIOSockets.size()));

	outIos    = fIOSockets[fNextIOSocketIdx].fSock;
	writeLock = fIOSockets[fNextIOSocketIdx].fMutex;

	//..Update "next" index, being sure to wrap around to the start
	//  whenever we reach the end of the vector.
	fNextIOSocketIdx++;
	if (fNextIOSocketIdx >= static_cast<int>(fIOSockets.size()))
		fNextIOSocketIdx = 0;
}

//------------------------------------------------------------------------------
// Convert network byte ordered IP address to a string.
// return - char* is returned with the IP address string.
//------------------------------------------------------------------------------
/* static */ char*
UmIPSocketConns::nwToString( in_addr_t addr, char* ipString )
{
	in_addr addrStruct = { addr };
#ifndef _MSC_VER
	if (!inet_ntop(AF_INET, &addrStruct, ipString, INET_ADDRSTRLEN))
#endif
		strcpy(ipString,"unknown");

	return ipString;
}

//------------------------------------------------------------------------------
// Convert contents to string for logging, debugging, etc.
//------------------------------------------------------------------------------
const std::string
UmIPSocketConns::toString() const
{
	std::ostringstream oss;

	char ipString[INET_ADDRSTRLEN];
	oss << "  IPAddress: " << UmIPSocketConns::nwToString(fIpAddress,ipString)<<
		"; nextIOSocketIdx: " << fNextIOSocketIdx << std::endl;

	for (unsigned int i=0; i<fIOSockets.size(); ++i)
	{
		sockaddr sa = fIOSockets[i].fSock->sa();
		const sockaddr_in* sinp = reinterpret_cast<const sockaddr_in*>(&sa);
		oss << "    port: " << ntohs(sinp->sin_port) <<
			std::endl;
	}

	return oss.str();
}

} // end of primitiveprocessor namespace
