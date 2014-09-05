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
 *   $Id: filerequest.h 2055 2013-02-08 19:09:09Z pleblanc $
 *
 *   jrodriguez@calpont.com   *
 *                                                                         *
 ***************************************************************************/

#include <iostream>
#include <string>

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

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
	 * note, useCache tells IOManager to cache the loaded blocks.
	 **/
	fileRequest(BRM::LBID_t lbid, const BRM::QueryContext &ver, bool flg, BRM::VER_t txn, int compType,
	  uint8_t *ptr=0, bool useCache = true);

	/**
	 * @brief request a range of disk blocks
	 **/
	fileRequest(const BRM::InlineLBIDRange& range, const BRM::QueryContext &ver, BRM::VER_t txn, int compType);

	/**
	 * @brief class dtor
	 **/
    virtual ~fileRequest() {};

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
		FAILED,
		BRM_LOOKUP_ERROR,
		FS_EINVAL,
		FS_ENOENT
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
	const BRM::QueryContext 	Ver() const {return fVer;}

	/**
	 * @brief VBBM flag of the LBID/Ver
	 **/
	const bool 	Flg() const {return fFlg;}

	const BRM::VER_t Txn() const { return fTxn;}

	const int CompType() const { return fCompType;}

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
	 * @brief did the request succeed for fail (0-success; else failure)
	 **/
	int 		RequestStatus() const {return fRqstStatus;}

	/**
	 * @brief setter for the request status
	 **/
	void 		RequestStatus(int s) {fRqstStatus=s;}

	void CompType(int compType) { fCompType = compType; }

	/**
	 * @brief if request failed, this is the error message string
	 **/
	std::string	RequestStatusStr() const {return fRqstStatusString;}

	/**
	 * @brief setter for the request status error message string
	 **/
	void 		RequestStatusStr(const std::string& s) {fRqstStatusString=s;}

	/**
	 * @brief return BLOCK or RANGE requested
	 **/
	int 		RequestType() const {return fRqstType;}

	/**
	 * @brief mutex to control synchronzation of request processing
	 **/
	boost::mutex&	frMutex() const		{return fFRMutex;}

	/**
	 * @brief condition variable. signal when request is complete
	 **/
	boost::condition& 	frCond() const		{return fFRCond;}

	/**
	 * @brief
	 **/
	const enum predicate_status_enum& frPredicate() const {return fFRPredicate;}

	/**
	 * @brief setter for the predicate
	 **/
	void SetPredicate(const enum predicate_status_enum& p) {fFRPredicate=p;}

	uint8_t *data;

	friend std::ostream& operator<<(std::ostream& os, const fileRequest& rhs);

	// tells IOManager to cache the loaded blocks or not
	bool useCache() { return cache; }
	void useCache(bool c) { cache = c; }
	
	bool versioned() { return wasVersioned; }
	void versioned(bool b) { wasVersioned = b; }

private:
	void init();

	BRM::LBID_t fLBID;
	BRM::QueryContext fVer;
	bool fFlg;
	BRM::VER_t fTxn;
	mutable boost::mutex fFRMutex;
	mutable boost::condition fFRCond;
	predicate_status_enum fFRPredicate;
	uint32_t fLength; // lbids requested
	uint32_t fblksRead; // lbids read
	uint32_t fblksLoaded; // lbids loaded into cache
	int fRqstStatus;
	std::string fRqstStatusString;
	enum request_type_enum fRqstType;
	int fCompType;
	bool cache;
	bool wasVersioned;
};

}

#endif
// vim:ts=4 sw=4:

