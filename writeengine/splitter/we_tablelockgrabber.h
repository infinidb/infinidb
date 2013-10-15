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

/*******************************************************************************
* $Id$
*
*******************************************************************************/

/*
 * we_tableLockgrabber.h
 *
 *  Created on: Dec 19, 2011
 *      Author: bpaul
 */

#ifndef WE_TABLELOCKGRABBER_H_
#define WE_TABLELOCKGRABBER_H_



namespace WriteEngine
{

class WESDHandler;					// forward deceleration

class WETableLockGrabber
{
public:
	WETableLockGrabber(WESDHandler& Ref): fRef(Ref) { }
	virtual ~WETableLockGrabber() { }

public:

	uint64_t grabTableLock(std::vector<unsigned int> &PmList,
												uint32_t tableOID);

	bool releaseTableLock(uint64_t LockId);

	bool changeTableLockState(uint64_t LockId);


private:
	WESDHandler& fRef;

};


} /* namespace WriteEngine */
#endif /* WE_TABLELOCKGRABBER_H_ */
