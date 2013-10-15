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
 * Used in selecting "next" port when sending a response to a given IP address
 */

#include "umsocketselector.h"
#include <arpa/inet.h>
#include <iostream>
#include <sstream>
#include "inetstreamsocket.h"

#include "liboamcpp.h"

using namespace oam;

//------------------------------------------------------------------------------
// Test driver for UmSocketSelector
//------------------------------------------------------------------------------
using namespace primitiveprocessor;

int main()
{
	// Calpont.xml file should be configured as follows:
	//   um1: 10.100.4.85 and 10.100.5.85
	//   um2: 10.101.4.85 and 10.101.5.85 
	sockaddr_in sa = { 1, 0, {0} , {' '} };
	char* ips[] = {"10.100.4.85", "10.100.5.85", "10.101.4.85", "10.101.5.85"};

	// These are the IP addresses we use to test runtime connections
	// "not" in the Calpont.xml file.
	sockaddr_in saUnknown = { 1, 0, {0} , {' '} };
	char* ipsUnknown[]={"10.102.1.1", "10.102.2.1", "10.102.3.1", "10.102.4.1"};

	//--------------------------------------------------------------------------
	// Test initialization
	//--------------------------------------------------------------------------
	UmSocketSelector* sockSel = UmSocketSelector::instance();

	std::cout << "IPAddressCount: " << sockSel->ipAddressCount() << std::endl;
	std::cout << std::endl << "----Dump1 after initialization..."  << std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump1 End...................."  << std::endl << std::endl;

	IOSocket sock[4][4];
	for (int i=0; i<4; i++)
	{
		inet_aton(ips[i], &sa.sin_addr);
		for (int j=0; j<4; j++)
		{
			sock[i][j].setSocketImpl(new InetStreamSocket());
			sa.sin_port = htons((i*4)+j);
			sock[i][j].sa( sa );
			sockSel->addConnection( SP_UM_IOSOCK(new IOSocket(sock[i][j])),
									SP_UM_MUTEX( new boost::mutex()) );
		}
	}

	std::cout << std::endl << "----Dump2 after adding 16 connections..." <<
		std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump2 End...................."  << std::endl << std::endl;

	//--------------------------------------------------------------------------
	// Test socket/port selection
	//--------------------------------------------------------------------------
	std::cout << "Test socket/port selection logic..." << std::endl;
	for (unsigned k=0; k<17; k++)
	{
		SP_UM_IOSOCK outIos;
		SP_UM_MUTEX  writeLock;
#if 1
    	if (sockSel->nextIOSocket( sock[0][0], outIos, writeLock ))
	    	std::cout << "next IP: " << inet_ntoa(outIos->sa().sin_addr) <<
				"; port: " << ntohs(outIos->sa().sin_port) << std::endl;
		else
#else
    	if (!sockSel->nextIOSocket( sock[0][0], outIos, writeLock ))
#endif
			std::cout << "no nextIP found for " << sock[0][0] << std::endl;
	}
	for (unsigned k=0; k<7; k++)
	{
		SP_UM_IOSOCK outIos;
		SP_UM_MUTEX  writeLock;
    	if (sockSel->nextIOSocket( sock[2][0], outIos, writeLock ))
	    	std::cout << "next IP: " << inet_ntoa(outIos->sa().sin_addr) <<
				"; port: " << ntohs(outIos->sa().sin_port) << std::endl;
		else
			std::cout << "no nextIP found for " << sock[2][0] << std::endl;
	}
	std::cout << std::endl;
	std::cout << "----Dump3 after selecting 17 connections from IP " << 
		ips[0] << "; and 7 connections from IP " << ips[2] << " ..." <<
		std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump3 End...................."  << std::endl << std::endl;

	//--------------------------------------------------------------------------
	// Test connection deletions
	//--------------------------------------------------------------------------
	for (unsigned k=0; k<4; k++)
	{
		sockSel->delConnection( sock[k][0] );
	}
	std::cout << "----Dump4 after deleting first connection for each IP..." <<
		std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump4 End...................."  << std::endl << std::endl;

	//--------------------------------------------------------------------------
	// Test addition of unknown connections
	//--------------------------------------------------------------------------
	IOSocket sockUnknown[4][4];
	for (int i=0; i<4; i++)
	{
		inet_aton(ipsUnknown[i], &saUnknown.sin_addr);
		for (int j=0; j<4; j++)
		{
			sockUnknown[i][j].setSocketImpl(new InetStreamSocket());
			saUnknown.sin_port = htons((i*4)+j);
			sockUnknown[i][j].sa( saUnknown );
			sockSel->addConnection(
				SP_UM_IOSOCK(new IOSocket(sockUnknown[i][j])),
				SP_UM_MUTEX( new boost::mutex()) );
		}
	}

	std::cout << "----Dump5 after adding connections for unknown IP's..." <<
		std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump5 End...................."  << std::endl << std::endl;

	//--------------------------------------------------------------------------
	// Test resetting of "next" indexes after deleting all sockets from a
	// specific IP address for which the "next" index is pointing.
	//--------------------------------------------------------------------------
	sockSel->delConnection( sock[1][1] );
	sockSel->delConnection( sock[1][2] );
	sockSel->delConnection( sock[1][3] );

	std::cout << "----Dump6 after deleting all connections for IP " << ips[1] <<
		" ..." << std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump6 End...................."  << std::endl << std::endl;

	//--------------------------------------------------------------------------
	// Test socket/port selection for an unknown module
	//--------------------------------------------------------------------------
	std::cout << "Test socket/port selection logic..." << std::endl;
	for (unsigned k=0; k<11; k++)
	{
		SP_UM_IOSOCK outIos;
		SP_UM_MUTEX  writeLock;
    	if (sockSel->nextIOSocket( sockUnknown[2][0], outIos, writeLock ))
    		std::cout << "next IP: " << inet_ntoa(outIos->sa().sin_addr) <<
				"; port: " << ntohs(outIos->sa().sin_port) << std::endl;
		else
			std::cout << "no nextIP found for " << sockUnknown[2][0]<<std::endl;
	}
	std::cout << std::endl;
	std::cout << "----Dump7 after selecting 11 connections for IP " <<
		ipsUnknown[2] << " ..." << std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump7 End...................."  << std::endl << std::endl;

	//--------------------------------------------------------------------------
	// Test deletion of last socket/port connection and resetting if the
	// "next" index that is pointing to it.
	//--------------------------------------------------------------------------
	sockSel->delConnection( sock[3][3] );
	std::cout << "----Dump8 after deleting last connection for IP " <<
		ips[3] << " ..." << std::endl;
	std::cout << sockSel->toString();
	std::cout << "----Dump8 End...................."  << std::endl << std::endl;

	return 0;
}
