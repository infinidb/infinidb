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
 * $Id: multicast.h 3495 2013-01-21 14:09:51Z rdempsey $
 *
 *****************************************************************************/

/** @file 
 * class Multicast interface
 */

#ifndef MULTICAST_H
#define MULTICAST_H

#include "messagequeue.h"

namespace multicast
{

/** @brief  MulticastReceive
  * Wrapper for multicast proto
  */

class MulticastImpl;

class Multicast
{
public:
	/** @brief ctor
	* Base class
	*/
	Multicast();
	
	/** @brief dtor
	*/
	virtual ~Multicast() { destroy(); }

	virtual void destroy() { }

	int PMCount() const { return fPMCount; }
	std::string iFName() const { return fIFName; }
	int portBase() const { return fPortBase; }
	int bufSize() const { return fBufSize; }

private:
	int fPMCount;
	std::string fIFName;
	int fPortBase;
	int fBufSize;

};

class MulticastReceiver: public Multicast
{
public:
	/** @brief ctor
	* 
	*/
	MulticastReceiver();

	~MulticastReceiver();
	
	messageqcpp::SBS receive();

private:
	// not copyable
	MulticastReceiver(const MulticastReceiver& rhs);
	MulticastReceiver& operator=(const MulticastReceiver& rhs);

	messageqcpp::SBS fByteStream;

	MulticastImpl* fPimpl;
};


class MulticastSender : public Multicast
{

public:
	/** @brief ctor
	* 
	*/
	MulticastSender();

	~MulticastSender();

	/** @brief receive 
	* 
	* @param bytestream to send
	*/
	void send(const messageqcpp::ByteStream& bs);

private:
	//Not copyable
	MulticastSender(const MulticastSender& rhs);
	MulticastSender& operator=(const MulticastSender& rhs);

	MulticastImpl* fPimpl;
};

} //namespace

#endif //MULTICAST_H

