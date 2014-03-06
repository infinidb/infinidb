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

#ifndef FILEREQUEST_H
#define FILEREQUEST_H

/***************************************************************************
 *
 *   $Id: filerequest.h 658 2008-07-10 14:47:24Z jrodriguez $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include "brmtypes.h"


/**
	@author Jason Rodriguez <jrodriguez@calpont.com>
*/

/**
 * @brief request class for the fileblockrequestqueue and the iomanager
 **/
namespace dbbc {

class fileRequest {

public:

	/**
	 * @brief default ctor
	 **/
	fileRequest();
	
	/**
	 * @brief copy constructor
	 **/
	fileRequest(const fileRequest& blk);
	
	/**
	 * @brief request for the disk block lbid@ver
	 **/
 	fileRequest(BRM::LBID_t lbid, BRM::VER_t ver, bool flg);

	fileRequest(BRM::LBID_t lbid, BRM::VER_t ver, bool flg, uint8_t *ptr);
	/**
	 * @brief request a range of disk blocks
	 **/
	fileRequest(const BRM::InlineLBIDRange& range, const BRM::VER_t ver);

	/**
	 * @brief class dtor
	 **/
    virtual ~fileRequest();

	/**
	 * @brief less-than operator
	 **/
	bool operator< (const fileRequest & rhs) const {
		return fLength < rhs.fLength;
	}

	/**
	 * @brief greater-than operator
	 **/
	bool operator> (const fileRequest & rhs) const {
		return fLength > rhs.fLength;
	}

	/**
	 * @brief equality operator
	 **/
	bool operator== (const fileRequest & rhs) const {
		return fLength == rhs.fLength;
	}
	
	enum request_status_enum {
		SUCCESSFUL,
		FAILED
	};

	enum request_type_enum {
		LBIDREQUEST,
		RANGEREQUEST
	};

	/**
	 * @brief used to manage request processing synchronzation
	 **/
	enum predicate_status_enum {
		INIT,
		SENDING,
		READING,
		COMPLETE,
		STOP
	};

	/**
	 * @brief lbid requested
	 **/
	const BRM::LBID_t 	Lbid() const {return fLBID;}

	/**
	 * @brief version of the lbid requested
	 **/
	const BRM::VER_t 	Ver() const {return fVer;}
	
	/**
	 * @brief VBBM flag of the LBID/Ver
	 **/
	const bool 	Flg() const {return fFlg;}

	/**
	 * @brief number of blocks requested
	 **/
	const uint32_t	BlocksRequested() const {return fLength;}

	/**
	 * @brief setter for blocks requested
	 **/
	void 		BlocksRequested(const int l) {fLength=l;}

	/**
	 * @brief number of blocks read from disk
	 **/
	const uint32_t	BlocksRead() const {return fblksRead;}
	const uint32_t	BlocksLoaded() const {return fblksLoaded;}

	/**
	 * @brief setter for blocks read from disk
	 **/
	void 		BlocksRead(const int l) {fblksRead=l;}
	void 		BlocksLoaded(const int l) {fblksLoaded=l;}

	/**
	 * @brief did the request succeed for fail
	 **/
	int 		RequestStatus() const {return fRqstStatus;}

	/**
	 * @brief setter for the request status
	 **/
	void 		RequestStatus(int s) {fRqstStatus=s;}

	/**
	 * @brief return BLOCK or RANGE requested
	 **/
	int 		RequestType() const {return fRqstType;}

	/**
	 * @brief mutex to control synchronzation of request processing
	 **/
	pthread_mutex_t&	frMutex() const		{return fFRMutex;}

	/**
	 * @brief condition variable. signal when request is complete
	 **/
	pthread_cond_t& 	frCond() const		{return fFRCond;}

	/**
	 * @brief
	 **/
	const enum predicate_status_enum& frPredicate() const {return fFRPredicate;}

	/**
	 * @brief setter for the predicate
	 **/
	void SetPredicate(const enum predicate_status_enum& p) {fFRPredicate=p;}
	
	uint8_t *data;

private:
	
	void 		init();
	BRM::LBID_t fLBID;
	BRM::VER_t fVer;
	bool fFlg;
	mutable pthread_mutex_t fFRMutex;
	mutable pthread_cond_t fFRCond;
	predicate_status_enum fFRPredicate;
	uint32_t fLength; // lbids requested
	uint32_t fblksRead; // lbids read
	uint32_t fblksLoaded; // lbids loaded into cache
	int fRqstStatus;
	enum request_type_enum fRqstType;
};
}
#endif
