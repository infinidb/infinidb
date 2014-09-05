/*

   Copyright (C) 2009-2013 Calpont Corporation.

   Use of and access to the Calpont InfiniDB Community software is subject to the
   terms and conditions of the Calpont Open Source License Agreement. Use of and
   access to the Calpont InfiniDB Enterprise software is subject to the terms and
   conditions of the Calpont End User License Agreement.

   This program is distributed in the hope that it will be useful, and unless
   otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
   the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   Please refer to the Calpont Open Source License Agreement and the Calpont End
   User License Agreement for more details.

   You should have received a copy of either the Calpont Open Source License
   Agreement or the Calpont End User License Agreement along with this program; if
   not, it is your responsibility to review the terms and conditions of the proper
   Calpont license agreement by visiting http://www.calpont.com for the Calpont
   InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
   the Calpont InfiniDB Community Calpont Open Source License Agreement.

   Calpont may make changes to these license agreements from time to time. When
   these changes are made, Calpont will make a new copy of the Calpont End User
   License Agreement available at http://www.calpont.com and a new copy of the
   Calpont Open Source License Agreement available at http:///www.infinidb.org.
   You understand and agree that if you use the Program after the date on which
   the license agreement authorizing your use has changed, Calpont will treat your
   use as acceptance of the updated License.

*/

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
