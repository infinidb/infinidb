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
 *   $Id: filerequest.cpp 2055 2013-02-08 19:09:09Z pleblanc $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/
#include <iostream>
using namespace std;

#include "filerequest.h"

namespace dbbc {

fileRequest::fileRequest() :
	data(0), fLBID(-1), fVer(-1), fFlg(false), fTxn(-1), fRqstType(LBIDREQUEST), fCompType(0),
	cache(true), wasVersioned(false)
{
	init(); //resets fFRPredicate, fLength, fblksRead, fblksLoaded, fRqstStatus
}

fileRequest::fileRequest(BRM::LBID_t lbid, const BRM::QueryContext &ver, bool flg, BRM::VER_t txn, int compType,
  uint8_t *ptr, bool cacheIt) :
	data(ptr), fLBID(lbid), fVer(ver), fFlg(flg), fTxn(txn), fRqstType(LBIDREQUEST), fCompType(compType),
	cache(cacheIt), wasVersioned(false)
{
	init(); //resets fFRPredicate, fLength, fblksRead, fblksLoaded, fRqstStatus
	fLength = 1;
}

fileRequest::fileRequest(const BRM::InlineLBIDRange& range, const BRM::QueryContext &ver, BRM::VER_t txn, int compType) :
	data(0), fLBID(range.start), fVer(ver), fFlg(false), fTxn(txn), fLength(range.size), 
	fRqstType(RANGEREQUEST), fCompType(compType), cache(true), wasVersioned(false)
{
	init(); //resets fFRPredicate, fLength, fblksRead, fblksLoaded, fRqstStatus
	fLength = range.size;
}

fileRequest::fileRequest(const fileRequest& blk)
{
	fLBID = blk.fLBID;
	fVer = blk.fVer;
	fTxn = blk.fTxn;
	fFlg = blk.fFlg;
	fRqstType = blk.fRqstType;
	fRqstStatusString = blk.fRqstStatusString;
	data = blk.data;
	fCompType = blk.fCompType;
	cache = blk.cache;
	wasVersioned = blk.wasVersioned;
	init(); //resets fFRPredicate, fLength, fblksRead, fblksLoaded, fRqstStatus
}

void fileRequest::init()
{
	fFRPredicate = INIT;
	fLength = 0;
	fblksRead = 0;
	fblksLoaded = 0;
	fRqstStatus = SUCCESSFUL;
}

ostream& operator<<(ostream& os, const fileRequest& rhs)
{
	os
		<< "LBID: " << rhs.fLBID
		<< " ver: " << rhs.fVer
		<< " Txn: " << rhs.fTxn
		<< " len: " << rhs.fLength
		<< " read: " << rhs.fblksRead
		<< " load: " << rhs.fblksLoaded
		<< " ct: " << rhs.fCompType
		;
	return os;
}

}

