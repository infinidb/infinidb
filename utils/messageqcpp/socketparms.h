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
*   $Id: socketparms.h 3495 2013-01-21 14:09:51Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef MESSAGEQCPP_SOCKETPARMS_H
#define MESSAGEQCPP_SOCKETPARMS_H

class MessageQTestSuite;

namespace messageqcpp { 

/** a simple socket parameters class
 *
 */
class SocketParms
{
public:
	/** ctor
	 *
	 */
	explicit SocketParms(int domain=-1, int type=-1, int protocol=-1);

	/** dtor
	 *
	 */
	virtual ~SocketParms();

	/** copy ctor
	 *
	 */
	SocketParms(const SocketParms& rhs);

	/** assign op
	 *
	 */
	SocketParms& operator=(const SocketParms& rhs);

	/** accessor
	 *
	 */
	inline int sd() const;

	/** accessor
	 *
	 */
	inline int domain() const;

	/** accessor
	 *
	 */
	inline int type() const;

	/** accessor
	 *
	 */
	inline int protocol() const;

	/** mutator
	 *
	 */
	inline void sd(int sd);

	/** mutator
	 *
	 */
	inline void domain(int domain);

	/** mutator
	 *
	 */
	inline void type(int type);

	/** mutator
	 *
	 */
	inline void protocol(int protocol);

	/** isOpen test
	 *
	 */
	inline bool isOpen() const;

	/*
	 * allow test suite access to private data for OOB test
	 */
	friend class ::MessageQTestSuite;

private:
	void doCopy(const SocketParms& rhs);

	int fSd;			/// the socket descriptor
	int fDomain;	/// the socket domain
	int fType;		/// the socket type
	int fProtocol;	/// the socket protocol
};

inline int SocketParms::sd() const { return fSd; }
inline int SocketParms::domain() const { return fDomain; }
inline int SocketParms::type() const { return fType; }
inline int SocketParms::protocol() const { return fProtocol; }
inline bool SocketParms::isOpen() const { return (fSd >= 0); }
inline void SocketParms::sd(int sd) { fSd = sd; }
inline void SocketParms::domain(int domain) { fDomain = domain; }
inline void SocketParms::type(int type) { fType = type; }
inline void SocketParms::protocol(int protocol) { fProtocol = protocol; }

} //namespace messageqcpp

#endif //MESSAGEQCPP_SOCKETPARMS_H

