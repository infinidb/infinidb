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
 * we_tablelockgrabber.cpp
 *
 *  Created on: Dec 19, 2011
 *      Author: bpaul
 */

#include "brm.h"
#include "brmtypes.h"
using namespace BRM;

#include <iostream>
using namespace std;

#include "we_sdhandler.h"
#include "we_tablelockgrabber.h"



namespace WriteEngine
{

uint64_t WETableLockGrabber::grabTableLock(std::vector<unsigned int> &PmList,
															uint32_t tableOID)
{
	uint64_t aLockId;
	std::string aProcName = "cpimport";
	uint32_t aProcId = getpid();
	int32_t aSessId = -1;
	int32_t aTxnId = -1;



	try
	{
		aLockId = fRef.fDbrm.getTableLock(PmList, tableOID, &aProcName,
							&aProcId, &aSessId, &aTxnId, BRM::LOADING);
	}
	catch (std::exception &e)
	{

		cout << "ERROR: Failed to get Table Lock " << e.what() << endl;
		throw runtime_error(e.what());
	}

	//cout << "lock ID = " << aLockId << endl;
	//if (aLockId == 0)
	//	cout << " existing owner name = " << aProcName << " pid = " <<
	//						aProcId << " session = " << aSessId << endl;

	return aLockId;
}

bool WETableLockGrabber::releaseTableLock(uint64_t LockId)
{
	bool aRet;
	//cout << "releasing lock " << LockId << endl;
	try
	{
		aRet = fRef.fDbrm.releaseTableLock(LockId);
	}
	catch (std::exception &e)
	{
		cout << "caught an exception: " << e.what() << endl;
		throw runtime_error(e.what());
	}
	return aRet;
}

bool WETableLockGrabber::changeTableLockState(uint64_t LockId)
{
	bool aRet;

	//cout << "changing state of lock " << LockId << endl;

	try
	{
		aRet = fRef.fDbrm.changeState(LockId, BRM::CLEANUP);
	}
	catch (std::exception &e)
	{
		cout << "caught an exception: " << e.what() << endl;
		throw runtime_error(e.what());
	}
	return aRet;
}







} /* namespace WriteEngine */
