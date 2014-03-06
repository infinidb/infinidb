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

/***************************************************************************
 *
 *   $Id: filerequest.cpp 506 2008-03-14 15:31:55Z jrodriguez $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/


#include "filerequest.h"
#include <stdexcept>

using namespace std;


namespace dbbc {
fileRequest::fileRequest()
{
	fLBID=-1;
	fVer=-1;
	data = NULL;
	init();
	fRqstType = LBIDREQUEST;
}


fileRequest::fileRequest(BRM::LBID_t lbid, BRM::VER_t ver, bool flg) :
	data(NULL), fLBID(lbid), fVer(ver), fFlg(flg)
{
	init();
	fLength=1;
	fRqstType = LBIDREQUEST;
}


fileRequest::fileRequest(BRM::LBID_t lbid, BRM::VER_t ver, bool flg, uint8_t *ptr) :
	fLBID(lbid), fVer(ver), fFlg(flg)
{
	init();
	fLength=1;
	fRqstType = LBIDREQUEST;
	data = ptr;
}


fileRequest::fileRequest(const BRM::InlineLBIDRange& range, const BRM::VER_t ver) :
	data(NULL), fLBID(range.start), fVer(ver), fFlg(false), fLength(range.size), 
	fRqstType(RANGEREQUEST)
{
	init();
	fLength=range.size;
}


fileRequest::fileRequest(const fileRequest& blk)
{
	fLBID=blk.fLBID;
	fVer=blk.fVer;
	fLength=blk.fLength;
	fblksRead=blk.fblksRead;
	fRqstType = blk.fRqstType;
	fRqstStatus=blk.fRqstStatus;
	data = blk.data;
	init();
}


void fileRequest::init() {
	if (pthread_mutex_init(&fFRMutex, NULL)!=0)
		throw runtime_error("mutex initialization failure");
	
	if (pthread_cond_init(&fFRCond, NULL)!=0)
		throw runtime_error("cond var initialization failure");
	
	fFRPredicate=INIT;
	fLength=0;
	fblksRead=0;
	fRqstStatus=0;
}

fileRequest::~fileRequest()
{
	pthread_mutex_destroy(&fFRMutex);
	pthread_cond_destroy(&fFRCond);
}

}
