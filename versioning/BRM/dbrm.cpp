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

/*****************************************************************************
 * $Id: dbrm.cpp 1623 2012-07-03 20:17:56Z pleblanc $
 *
 ****************************************************************************/

#include <iostream>
#include <sys/types.h>
#include <vector>
#ifdef __linux__
#include <values.h>
#endif
#include <boost/thread.hpp>
//#define NDEBUG
#include <cassert>

#include "rwlock.h"
#include "mastersegmenttable.h"
#include "extentmap.h"
#include "copylocks.h"
#include "vss.h"
#include "vbbm.h"
#include "socketclosed.h"
#include "configcpp.h"
#include "sessionmanagerserver.h"
#define DBRM_DLLEXPORT
#include "dbrm.h"
#undef DBRM_DLLEXPORT

#ifdef BRM_DEBUG
#define CHECK_EMPTY(x) \
	if (x.length() != 0) \
		throw logic_error("DBRM: got a message of the wrong size");
#else
#define CHECK_EMPTY(x)
#endif

#define DO_ERR_NETWORK \
	delete msgClient; \
	msgClient = NULL; \
	mutex.unlock(); \
	return ERR_NETWORK;

using namespace std;
using namespace messageqcpp;

#ifdef BRM_INFO
 #include "tracer.h"
#endif

namespace BRM {

DBRM::DBRM(bool noBRMinit) throw() : fDebug(false)
{
	if (!noBRMinit) {
		mst.reset(new MasterSegmentTable());
		em.reset(new ExtentMap());
		vss.reset(new VSS());
		vbbm.reset(new VBBM());
		copylocks.reset(new CopyLocks());

		em->setReadOnly();
		vss->setReadOnly();
		vbbm->setReadOnly();
	}
		msgClient = NULL;
		masterName = "DBRM_Controller";
		config = config::Config::makeConfig();
#ifdef BRM_INFO
	fDebug = ("Y" == config->getConfig("DBRM", "Debug"));
#endif
}

DBRM::DBRM(const DBRM& brm)
{
	throw logic_error("DBRM: Don't use the copy constructor.");
}

DBRM::~DBRM() throw()
{
	if (msgClient != NULL)
		msgClient->shutdown();
	delete msgClient;
}

DBRM& DBRM::operator=(const DBRM& brm)
{
	throw logic_error("DBRM: Don't use the = operator.");
}


int DBRM::saveState() throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("saveState()");
#endif
	string prefix = config->getConfig("SystemConfig", "DBRMRoot");
	if (prefix.length() == 0) {
		cerr << "Error: Need a valid Calpont configuation file" << endl;
		exit(1);
	}

	int rc = saveState(prefix);

	return rc;
}

int DBRM::saveState(string filename) throw()
{
#ifdef BRM_INFO
 	if (fDebug)
	{
		TRACER_WRITELATER("saveState(filename)");
		TRACER_ADDSTRINPUT(filename);
		TRACER_WRITE;
	}	
#endif
	string emFilename = filename + "_em";
	string vssFilename = filename + "_vss";
	string vbbmFilename = filename + "_vbbm";
	string clFilename = filename + "_cl";
	bool locked[3] = { false, false, false };
	try {
		vbbm->lock(VBBM::READ);
		locked[0] = true;
		vss->lock(VSS::READ);
		locked[1] = true;
		copylocks->lock(CopyLocks::READ);
		locked[2] = true;

		saveExtentMap(emFilename);
		vbbm->save(vbbmFilename);
		vss->save(vssFilename);
		copylocks->save(clFilename);

		copylocks->release(CopyLocks::READ);
		locked[2] = false;
		vss->release(VSS::READ);
		locked[1] = false;
		vbbm->release(VBBM::READ);
		locked[0] = false;
	}
	catch (exception &e) {
		if (locked[2])
			copylocks->release(CopyLocks::READ);
		if (locked[1])
			vss->release(VSS::READ);
		if (locked[0])
			vbbm->release(VBBM::READ);
		return -1;
	}
	return 0;
}

int DBRM::saveExtentMap(const string &filename) throw() 
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("saveExtentMap");
		TRACER_ADDSTRINPUT(filename);
		TRACER_WRITE;
	}	
#endif

	try {
		em->save(filename);
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		return -1;
	}

	return 0;
}

// @bug 1055+.  New functions added for multiple files per OID enhancement.
int DBRM::lookupLocal(LBID_t lbid, VER_t verid, bool vbFlag, OID_t& oid, 
	uint16_t& dbRoot, uint32_t& partitionNum, uint16_t& segmentNum, uint32_t& fileBlockOffset) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("lookupLocal(lbid,ver,..)");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDINPUT(verid);
		TRACER_ADDBOOLINPUT(vbFlag);
		TRACER_ADDOUTPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDOUTPUT(fileBlockOffset);
		TRACER_WRITE;
	}
	
#endif
	bool locked[2] = {false, false};
	int ret;
	bool tooOld = false;
	
	try {
		if (!vbFlag)
			return em->lookupLocal(lbid, (int&)oid, dbRoot, partitionNum, segmentNum, fileBlockOffset);
		else {
			vbbm->lock(VBBM::READ);
			locked[0] = true;
			ret = vbbm->lookup(lbid, verid, oid, fileBlockOffset);
			vbbm->release(VBBM::READ);
			locked[0] = false;
			if (ret < 0) {
				vss->lock(VSS::READ);
				locked[1] = true;
				tooOld = vss->isTooOld(lbid, verid);
				vss->release(VSS::READ);
				locked[1] = false;
				if (tooOld)
					return ERR_SNAPSHOT_TOO_OLD;
			}
			return ret;
		}
	}
	catch (exception& e) {
		if (locked[1])
			vss->release(VSS::READ);
		if (locked[0])
			vbbm->release(VBBM::READ);
		cerr << e.what() << endl;
		return -1;
	}
}

int DBRM::lookupLocal(OID_t oid, uint32_t partitionNum, uint16_t segmentNum, uint32_t fileBlockOffset, LBID_t& lbid) throw()
{
#ifdef BRM_INFO
        if (fDebug)
        {
                TRACER_WRITELATER("lookupLocal(oid,fbo,..)");
                TRACER_ADDINPUT(oid);
                TRACER_ADDINPUT(partitionNum);
                TRACER_ADDSHORTINPUT(segmentNum);
                TRACER_ADDINPUT(fileBlockOffset);
                TRACER_ADDOUTPUT(lbid);
                TRACER_WRITE;
        }

#endif
        try {
                return em->lookupLocal(oid, partitionNum, segmentNum, fileBlockOffset, lbid);
        }
        catch (exception& e) {
                cerr << e.what() << endl;
                return -1;
        }
}

// @bug 1055-

//------------------------------------------------------------------------------
// Lookup/return starting LBID for the specified OID, partition, segment, and
// file block offset.
//------------------------------------------------------------------------------
int DBRM::lookupLocalStartLbid(OID_t    oid,
                               uint32_t partitionNum,
                               uint16_t segmentNum,
                               uint32_t fileBlockOffset,
                               LBID_t&  lbid) throw()
{
#ifdef BRM_INFO
        if (fDebug)
        {
                TRACER_WRITELATER("lookupLocalStartLbid(oid,fbo,..)");
                TRACER_ADDINPUT(oid);
                TRACER_ADDINPUT(partitionNum);
                TRACER_ADDSHORTINPUT(segmentNum);
                TRACER_ADDINPUT(fileBlockOffset);
                TRACER_ADDOUTPUT(lbid);
                TRACER_WRITE;
        }
#endif
        try {
                return em->lookupLocalStartLbid(oid, partitionNum, segmentNum,
                                               fileBlockOffset, lbid);
        }
        catch (exception& e) {
                cerr << e.what() << endl;
                return -1;
        }
}

int DBRM::lookup(OID_t oid, LBIDRange_v& lbidList) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("lookup(oid,range)");
		TRACER_ADDINPUT(oid);
		TRACER_WRITE;
	}
	
#endif	
	try {
		em->lookup(oid, lbidList);
		return 0;
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return -1;
	}
}

// Casual Partitioning support
int DBRM::markExtentInvalid(const LBID_t lbid) DBRM_THROW
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("markExtentInvalid");
		TRACER_ADDINPUT(lbid);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << MARKEXTENTINVALID << (uint64_t)lbid;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::markExtentsInvalid(const vector<LBID_t> &lbids) DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("markExtentsInvalid");
#endif	
	ByteStream command, response;
	uint8_t err;
	uint32_t size = lbids.size(), i;

	command << MARKMANYEXTENTSINVALID << size;
	for (i = 0; i < size; i++)
		command << (uint64_t) lbids[i];
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::getExtentMaxMin(const LBID_t lbid, int64_t& max, int64_t& min, int32_t& seqNum) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getExtentMaxMin");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDOUTPUT(max);
		TRACER_ADDOUTPUT(min);
		TRACER_ADDOUTPUT(seqNum);
		TRACER_WRITE;
	}
	
#endif

	try {
		int ret=em->getMaxMin(lbid, max, min, seqNum);
		return ret;
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return false;
	}
}

int DBRM::setExtentMaxMin(const LBID_t lbid, const int64_t max, const int64_t min, const int32_t seqNum) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("setExtentMaxMin");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDINPUT(max);
		TRACER_ADDINPUT(min);
		TRACER_ADDINPUT(seqNum);
		TRACER_WRITE;
	}
	
#endif
	ByteStream command, response;
	uint8_t err;

	command << SETEXTENTMAXMIN << (uint64_t)lbid << (uint64_t)max << (uint64_t)min << (uint64_t)seqNum;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;

}

// @bug 1970 - Added function below to set multiple extents casual partition info in one call.
int DBRM::setExtentsMaxMin(const CPInfoList_t &cpInfos) DBRM_THROW
{
	CPInfoList_t::const_iterator it;
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("setExtentsMaxMin");
		for(it = cpInfos.begin(); it != cpInfos.end(); it++)
		{
			TRACER_ADDINPUT(it->firstLbid);
			TRACER_ADDINPUT(it->max);
			TRACER_ADDINPUT(it->min);
			TRACER_ADDINPUT(it->seqNum);
			TRACER_WRITE;
		}
	}
#endif
	ByteStream command, response;
	uint8_t err;

	if (cpInfos.empty())
		return ERR_OK;

	command << SETMANYEXTENTSMAXMIN << (uint32_t)cpInfos.size();
	for(it = cpInfos.begin(); it != cpInfos.end(); it++)
	{
		command << (uint64_t)it->firstLbid << (uint64_t)it->max << (uint64_t)it->min << (uint32_t)it->seqNum;
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// @bug 2117 - Add function to merge Casual Partition info with current extent
// map information.
//------------------------------------------------------------------------------
int DBRM::mergeExtentsMaxMin(const CPInfoMergeList_t &cpInfos) DBRM_THROW
{
	CPInfoMergeList_t::const_iterator it;
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("updateExtentsMaxMin");
		for(it = cpInfos.begin(); it != cpInfos.end(); it++)
		{
			TRACER_ADDINPUT(it->startLbid);
			TRACER_ADDINPUT(it->max);
			TRACER_ADDINPUT(it->min);
			TRACER_ADDINPUT(it->seqNum);
			TRACER_ADDINPUT(it->isChar);
			TRACER_ADDINPUT(it->newExtent);
			TRACER_WRITE;
		}
	}
#endif
	ByteStream command, response;
	uint8_t err;

	command << MERGEMANYEXTENTSMAXMIN << (uint32_t)cpInfos.size();
	for(it = cpInfos.begin(); it != cpInfos.end(); it++)
	{
		command << (uint64_t)it->startLbid <<
				   (uint64_t)it->max       <<
				   (uint64_t)it->min       <<
				   (uint32_t)it->seqNum    <<
				   (uint32_t)it->isChar    <<
                   (uint32_t)it->newExtent;
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::vssLookup(LBID_t lbid, VER_t& verID, VER_t txnID, 
	bool& vbFlag, bool vbOnly) throw()
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("vssLookup");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDINPUT(verID);
		TRACER_ADDINPUT(txnID);
		TRACER_ADDBOOLOUTPUT(vbFlag);
		TRACER_ADDBOOLINPUT(vbOnly);
		TRACER_WRITE;
	}
	
#endif
	if (!vbOnly && vss->isEmpty())
	{
		verID = 0;
		vbFlag = false;
		return -1;
	}

	bool locked = false;

	try {
		int rc = 0;
		vss->lock(VSS::READ);
		locked = true;
		rc = vss->lookup(lbid, verID, txnID, vbFlag, vbOnly);
		vss->release(VSS::READ);
		return rc;
	}
	catch (exception& e) {
		if (locked)
			vss->release(VSS::READ);
		cerr << e.what() << endl;
		return -1;
	}
}

int DBRM::bulkVSSLookup(const std::vector<LBID_t> &lbids, VER_t verID, VER_t txnID,
    std::vector<VSSData> *out) throw()
{
    uint i;
    bool locked = false;
    assert(out);
    try {
        out->resize(lbids.size());
        vss->lock(VSS::READ);
        locked = true;
        if (vss->isEmpty(false)) {
            for (i = 0; i < lbids.size(); i++) {
                VSSData &vd = (*out)[i];
                vd.verID = 0;
                vd.vbFlag = false;
                vd.returnCode = -1;
            }
        }
        else {
            for (i = 0; i < lbids.size(); i++) {
                VSSData &vd = (*out)[i];
                vd.verID = verID;
                vd.returnCode = vss->lookup(lbids[i], vd.verID, txnID, vd.vbFlag, false);
            }
        }
        vss->release(VSS::READ);
        return 0;
    }
    catch (exception& e) {
        cerr << e.what() << endl;
    }
    catch (...) {
        cerr << "bulkVSSLookup: caught an exception" << endl;
    }
    if (locked)
        vss->release(VSS::READ);
    out->clear();
    return -1;
}

int8_t DBRM::send_recv(const ByteStream &in, ByteStream &out) throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("send_recv");
#endif
	bool firstAttempt = true;

	mutex.lock();
	
reconnect:

	if (msgClient == NULL)
		try {
			msgClient = new MessageQueueClient(masterName);
		}
		catch(exception &e) {
			cerr << "class DBRM failed to create a MessageQueueClient: " << 
				e.what() << endl;
			msgClient = NULL;
			mutex.unlock();
			return ERR_FAILURE;
		}
	
	try {
		msgClient->write(in);	
		out = msgClient->read();
	}
	/* If we add a timeout to the read() call, uncomment this clause
	catch (SocketClosed &e) {
		cerr << "DBRM::send_recv: controller node closed the connection" << endl;
		DO_ERR_NETWORK;
	}
	*/
	catch (exception& e) {
		cerr << "DBRM::send_recv caught: " << e.what() << endl;
		if (firstAttempt) {
			firstAttempt = false;
			delete msgClient;
			msgClient = NULL;
			goto reconnect;
		}
		DO_ERR_NETWORK;
	}
	if (out.length() == 0) {
		cerr << "DBRM::send_recv: controller node closed the connection" << endl;
		if (firstAttempt) {
			firstAttempt = false;
			delete msgClient;
			msgClient = NULL;
			sleep(10);
			goto reconnect;
		}	
		DO_ERR_NETWORK;
	}
	mutex.unlock();
	return ERR_OK;
}

//------------------------------------------------------------------------------
// Send a request to create a column extent.
//------------------------------------------------------------------------------
int DBRM::createColumnExtent(OID_t oid,
	u_int32_t  colWidth,
	u_int16_t& dbRoot,
	u_int32_t& partitionNum,
	u_int16_t& segmentNum,
	LBID_t&    lbid,
	int&       allocdSize,
	u_int32_t& startBlockOffset) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	 	TRACER_WRITELATER("createColumnExtent");
		TRACER_ADDINPUT(oid);
	 	TRACER_ADDINPUT(colWidth);
	 	TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdSize);
		TRACER_ADDOUTPUT(startBlockOffset);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t  err;
	uint16_t tmp16;
	uint32_t tmp32;
	uint64_t tmp64;

	command << CREATE_COLUMN_EXTENT << (ByteStream::quadbyte) oid << colWidth << dbRoot <<
               partitionNum;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	try {
		response >> err;
		if (err != 0)
			return (int) err;

		response >> tmp16;
		dbRoot = tmp16;
		response >> tmp32;
		partitionNum = tmp32;
		response >> tmp16;
		segmentNum = tmp16;
		response >> tmp64;
		lbid = (int64_t)tmp64;
		response >> tmp32;
		allocdSize = (int32_t)tmp32;	
		response >> tmp32;
		startBlockOffset = (int32_t)tmp32;
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	CHECK_EMPTY(response);
	return 0;
}

//------------------------------------------------------------------------------
// Send a request to create a dictionary store extent.
//------------------------------------------------------------------------------
int DBRM::createDictStoreExtent(OID_t oid,
	u_int16_t  dbRoot,
	u_int32_t  partitionNum,
	u_int16_t  segmentNum,
	LBID_t&    lbid,
	int&       allocdSize) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	 	TRACER_WRITELATER("createDictStoreExtent");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdSize);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t  err;
	uint32_t tmp32;
	uint64_t tmp64;

	command << CREATE_DICT_STORE_EXTENT << (ByteStream::quadbyte) oid << dbRoot <<
		partitionNum << segmentNum;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	try {
		response >> err;
		if (err != 0)
			return (int) err;

		response >> tmp64;
		lbid = (int64_t)tmp64;
		response >> tmp32;
		allocdSize = (int32_t)tmp32;	
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	CHECK_EMPTY(response);
	return 0;
}

//------------------------------------------------------------------------------
// Send a request to delete a set extents for the specified column OID,
// and to return the extents to the free list.  HWMs for the last stripe of
// extents are updated accordingly.
//------------------------------------------------------------------------------
int DBRM::rollbackColumnExtents(OID_t oid,
	u_int32_t  partitionNum,
	u_int16_t  segmentNum,
	HWM_t      hwm) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("rollbackColumnExtents");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(hwm);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << ROLLBACK_COLUMN_EXTENTS << (ByteStream::quadbyte) oid << partitionNum <<
		segmentNum << hwm;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Send a request to delete a set of extents for the specified dictionary store
// OID, and to return the extents to the free list.  HWMs for the last stripe
// of extents are updated accordingly.
//------------------------------------------------------------------------------
int DBRM::rollbackDictStoreExtents(OID_t oid,
	u_int32_t            partitionNum,
	const vector<HWM_t>& hwms) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("rollbackDictStoreExtents");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << ROLLBACK_DICT_STORE_EXTENTS << (ByteStream::quadbyte) oid << partitionNum;
	serializeVector(command, hwms);
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::deleteEmptyColExtents(const std::vector<ExtentInfo>& extentsInfo) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("deleteEmptyColExtents");
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = extentsInfo.size();
	command << DELETE_EMPTY_COL_EXTENTS;
	command << size;
	for ( unsigned i=0; i<extentsInfo.size();i++)
	{
		command << (ByteStream::quadbyte) extentsInfo[i].oid;
		command << extentsInfo[i].partitionNum;
		command << extentsInfo[i].segmentNum;
		command << extentsInfo[i].dbRoot;
		command << extentsInfo[i].hwm;
	}
	
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::deleteEmptyDictStoreExtents(const std::vector<ExtentInfo>& extentsInfo) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("deleteEmptyDictStoreExtents");
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = extentsInfo.size();
	command << DELETE_EMPTY_DICT_STORE_EXTENTS;
	command << size;
	for ( unsigned i=0; i<extentsInfo.size();i++)
	{
		command << (ByteStream::quadbyte) extentsInfo[i].oid;
		command << extentsInfo[i].partitionNum;
		command << extentsInfo[i].segmentNum;
		command << extentsInfo[i].dbRoot;
		command << extentsInfo[i].hwm;
		command << (uint8_t)extentsInfo[i].newFile;
	}
	
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::deleteOID(OID_t oid) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("deleteOID");
		TRACER_ADDINPUT(oid);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << DELETE_OID << (ByteStream::quadbyte) oid;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::deleteOIDs(const std::vector<OID_t>& oids) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("deleteOIDs");
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = oids.size();
	command << DELETE_OIDS;
	command << size;
	for ( unsigned i=0; i<oids.size();i++)
	{
		command << (ByteStream::quadbyte) oids[i];
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}
	
int DBRM::getHWM(OID_t oid, HWM_t& hwm) throw()
{	

#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getHWM");
		TRACER_ADDINPUT(oid);
		TRACER_ADDOUTPUT(hwm);
		TRACER_WRITE;
	}	
#endif

	try {
		hwm = em->getHWM(oid);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	return ERR_OK;
}

//------------------------------------------------------------------------------
// Return the last local HWM for the specified OID.  The corresponding dbroot,
// partition number, and segment number are returned as well.   This function
// can be used by cpimport for example to find out where the current "end-of-
// data" is, so that cpimport will know where to begin adding new rows.
//------------------------------------------------------------------------------
int DBRM::getLastLocalHWM(OID_t oid, uint16_t& dbRoot, uint32_t& partitionNum,
	uint16_t& segmentNum, HWM_t& hwm) throw()
{	
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getLastLocalHWM");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDOUTPUT(hwm);
		TRACER_WRITE;
	}	
#endif

	try {
		hwm = em->getLastLocalHWM(oid, dbRoot, partitionNum, segmentNum);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	return ERR_OK;
}

//------------------------------------------------------------------------------
// Return the HWM for the specified OID, partition number, and segment number.
// This is used to get the HWM for a particular dictionary segment store file,
// or a specific column segment file.
//------------------------------------------------------------------------------
int DBRM::getLocalHWM(OID_t oid, uint32_t partitionNum, uint16_t segmentNum,
	HWM_t& hwm) throw()
{	
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getLocalHWM");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDOUTPUT(hwm);
		TRACER_WRITE;
	}	
#endif

	try {
		hwm = em->getLocalHWM(oid, partitionNum, segmentNum);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	return ERR_OK;
}

//------------------------------------------------------------------------------
// Set the local HWM for the file referenced by the specified OID, partition
// number, and segment number.
//------------------------------------------------------------------------------
int DBRM::setLocalHWM(OID_t oid, uint32_t partitionNum, uint16_t segmentNum,
	HWM_t hwm) DBRM_THROW
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("setLocalHWM");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(hwm);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << SET_LOCAL_HWM << (ByteStream::quadbyte) oid << partitionNum << segmentNum << hwm;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::bulkSetHWMAndCP(const vector<BulkSetHWMArg> &v, const vector<CPInfo> &setCPDataArgs,
		const vector<CPInfoMerge> &mergeCPDataArgs, VER_t transID) DBRM_THROW
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("bulkSetHWMAndCP");
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << BULK_SET_HWM_AND_CP;
	serializeInlineVector(command, v);
	serializeInlineVector(command, setCPDataArgs);
	serializeInlineVector(command, mergeCPDataArgs);
	command << (uint32_t) transID;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;


}

// dmc-should eventually deprecate
int DBRM::getExtentSize() throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("getExtentSize");
#endif
	return em->getExtentSize();
}

unsigned DBRM::getExtentRows() throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("getExtentRows");
#endif
	return em->getExtentRows();
}

int DBRM::getExtents(int OID, std::vector<struct EMEntry>& entries,
		bool sorted, bool notFoundErr, bool incOutOfService) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("getExtents");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}	
#endif

	try {
		em->getExtents(OID, entries, sorted, notFoundErr, incOutOfService);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return -1;
	}
	return 0;
}

int DBRM::getStartExtent(OID_t oid, uint16_t& dbRoot,
                         uint32_t& partitionNum, bool incOutOfService) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getStartExtent");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_WRITE;
	}	
#endif

    try {
        em->getStartExtent(oid, incOutOfService, dbRoot, partitionNum);
    }
    catch (exception& e) {
        cerr << e.what() << endl;
        return -1;
    }
    return 0;
}

//------------------------------------------------------------------------------
// Delete all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
int DBRM::deletePartition(const std::vector<OID_t>& oids,
	uint32_t partitionNum) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("deletePartition");
		std::ostringstream oss;
		oss << "partitionNum: " << partitionNum << "; OIDS: ";
		std::vector<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = oids.size();

	command << DELETE_PARTITION << partitionNum << size;
	for ( unsigned i=0; i<size; i++)
	{
		command << (ByteStream::quadbyte) oids[i];
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
//------------------------------------------------------------------------------
int DBRM::markPartitionForDeletion(const std::vector<OID_t>& oids,
	uint32_t partitionNum) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markPartitionForDeletion");
		std::ostringstream oss;
		oss << "partitionNum: " << partitionNum << "; OIDS: ";
		std::vector<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = oids.size();

	command << MARK_PARTITION_FOR_DELETION << partitionNum << size;
	for ( unsigned i=0; i<size; i++)
	{
		command << (ByteStream::quadbyte) oids[i];
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) 
//------------------------------------------------------------------------------
int DBRM::markAllPartitionForDeletion(const std::vector<OID_t>& oids) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markAllPartitionForDeletion");
		std::ostringstream oss;
		oss << "OIDS: ";
		std::vector<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = oids.size();

	command << MARK_ALL_PARTITION_FOR_DELETION << size;
	for ( unsigned i=0; i<size; i++)
	{
		command << (ByteStream::quadbyte) oids[i];
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Restore all extents for the specified OID(s) and partition number.
//------------------------------------------------------------------------------
int DBRM::restorePartition(const std::vector<OID_t>& oids,
	uint32_t partitionNum) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("restorePartition");
		std::ostringstream oss;
		oss << "partitionNum: " << partitionNum << "; OIDS: ";
		std::vector<OID_t>::const_iterator it;
		for (it=oids.begin(); it!=oids.end(); ++it)
		{
			oss << (*it) << ", ";
		}
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t size = oids.size();

	command << RESTORE_PARTITION << partitionNum << size;
	for ( unsigned i=0; i<size; i++)
	{
		command << (ByteStream::quadbyte) oids[i];
	}
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Return all the out-of-service partitions for the specified OID.
//------------------------------------------------------------------------------
int DBRM::getOutOfServicePartitions(OID_t oid,
	std::vector<uint32_t>& partitionNums) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getOutOfServicePartitions");
		TRACER_ADDINPUT(oid);
		TRACER_WRITE;
	}	
#endif

    try {
        em->getOutOfServicePartitions(oid, partitionNums);
    }
    catch (exception& e) {
        cerr << e.what() << endl;
        return -1;
    }

    return ERR_OK;
}

int DBRM::writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID,
	u_int32_t vbFBO) DBRM_THROW
{

#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("writeVBEntry");
		TRACER_ADDINPUT(transID);
		TRACER_ADDINPUT(lbid);
		TRACER_ADDINPUT(vbOID);
		TRACER_ADDINPUT(vbFBO);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << WRITE_VB_ENTRY << (uint32_t) transID << (uint64_t) lbid << (uint32_t) vbOID << vbFBO;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}
		
int DBRM::getUncommittedLBIDs(VER_t transID, vector<LBID_t>& lbidList) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getUncommittedLBIDs");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}	
#endif
	bool locked = false;
	
	try {
		vss->lock(VSS::READ);
		locked = true;
		
		vss->getUncommittedLBIDs(transID, lbidList);
		
		vss->release(VSS::READ);
		locked = false;
		return 0;
	}
	catch (exception &e) {
		if (locked)
			vss->release(VSS::READ);
		return -1;
	}
}

// @bug 1509.  New function that returns one LBID per extent touched as part of the transaction.  Used to get a list
// of blocks to use for updating casual partitioning when the transaction is committed.
int DBRM::getUncommittedExtentLBIDs(VER_t transID, vector<LBID_t>& lbidList) throw()
{
#ifdef BRM_INFO
        if (fDebug)
        {
	        TRACER_WRITELATER("getUncommittedExtentLBIDs");
                TRACER_ADDINPUT(transID);
                TRACER_WRITE;
        }
#endif
        bool locked = false;
	vector<LBID_t>::iterator lbidIt;
	typedef pair<int64_t, int64_t> range_t;
	range_t range;
	vector<range_t> ranges;
	vector<range_t>::iterator rangeIt;
        try {
                vss->lock(VSS::READ);
                locked = true;

		// Get a full list of uncommitted LBIDs related to this transactin.
                vss->getUncommittedLBIDs(transID, lbidList);

                vss->release(VSS::READ);
                locked = false;

		if(lbidList.size() > 0) {

			// Sort the vector.
			std::sort<vector<LBID_t>::iterator>(lbidList.begin(), lbidList.end());

			// Get the LBID range for the first block in the list.
			lbidIt = lbidList.begin();
			if (em->lookup(*lbidIt, range.first, range.second) < 0) {
				return -1;
			}
			ranges.push_back(range);

			// Loop through the LBIDs and add the new ranges.
			++lbidIt;
			while(lbidIt != lbidList.end()) {
				if (*lbidIt > range.second) {
		                        if (em->lookup(*lbidIt, range.first, range.second) < 0) {
						return -1;
                        		}
		                        ranges.push_back(range);
				}
				++lbidIt;
			}
			
			// Reset the lbidList and return only the first LBID in each extent that was changed
			// in the transaction.
			lbidList.clear();
			for (rangeIt = ranges.begin(); rangeIt != ranges.end(); rangeIt++) {
				lbidList.push_back(rangeIt->first);
			}			
		}
                return 0;
        }
        catch (exception &e) {
                if (locked)
                        vss->release(VSS::READ);
                return -1;
        }
}

		
int DBRM::beginVBCopy(VER_t transID, const LBIDRange_v& ranges,
	VBRange_v& freeList) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("beginVBCopy");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << BEGIN_VB_COPY << (ByteStream::quadbyte) transID;
	serializeVector<LBIDRange>(command, ranges);
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	try {
		response >> err;
		if (err != 0)
			return err;
		deserializeVector(response, freeList);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return ERR_NETWORK;
	}

	CHECK_EMPTY(response);
	return 0;
}
		
int DBRM::endVBCopy(VER_t transID, const LBIDRange_v& ranges)
	DBRM_THROW
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("endVBCopy");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << END_VB_COPY << (ByteStream::quadbyte) transID;
	serializeVector(command, ranges);
	err = send_recv(command, response);

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}
		
int DBRM::vbCommit(VER_t transID) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("vbCommit");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << VB_COMMIT << (ByteStream::quadbyte) transID;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::vbRollback(VER_t transID, const LBIDRange_v& lbidList) DBRM_THROW
{

#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("vbRollback ");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << VB_ROLLBACK1 << (ByteStream::quadbyte) transID;
	serializeVector(command, lbidList);	
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::vbRollback(VER_t transID, const vector<LBID_t>& lbidList) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("vbRollback");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << VB_ROLLBACK2 << (ByteStream::quadbyte) transID;
	serializeVector(command, lbidList);
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;
	
	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::halt() DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("halt");
#endif
	ByteStream command, response;
	uint8_t err;

	command << HALT;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::resume() DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("resume");
#endif
	ByteStream command, response;
	uint8_t err;

	command << RESUME;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;
	
	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::forceReload() DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("forceReload");
#endif
	ByteStream command, response;
	uint8_t err;

	command << RELOAD;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;
	
	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::setReadOnly(bool b) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("setReadOnly");
		TRACER_ADDBOOLINPUT(b);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << (b ? SETREADONLY : SETREADWRITE);
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;
	
	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::isReadWrite() throw()
{
#ifdef BRM_INFO
  	if (fDebug)  TRACER_WRITENOW("isReadWrite");
#endif
	ByteStream command, response;
	uint8_t err;

	command << GETREADONLY;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;
	
	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	//CHECK_EMPTY(response);
	return (err == 0 ? ERR_OK : ERR_READONLY);
}

int DBRM::flushInodeCaches() DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("flushInodeCaches");
#endif
	ByteStream command, response;
	uint8_t err;

	command << FLUSH_INODE_CACHES;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;
	
	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::dmlLockLBIDRanges(const vector<LBIDRange> &ranges, int txnID)
{
#ifdef BRM_INFO
    if (fDebug) TRACER_WRITENOW("clear");
#endif
    ByteStream command, response;
    uint8_t err;

    command << LOCK_LBID_RANGES;
    serializeVector<LBIDRange>(command, ranges);
    command << (uint32_t) txnID;
    err = send_recv(command, response);
    if (err != ERR_OK)
        return err;

    if (response.length() != 1)
        return ERR_NETWORK;

    response >> err;
    CHECK_EMPTY(response);
    return err;
}

int DBRM::dmlReleaseLBIDRanges(const vector<LBIDRange> &ranges)
{
#ifdef BRM_INFO
    if (fDebug) TRACER_WRITENOW("clear");
#endif
    ByteStream command, response;
    uint8_t err;

    command << RELEASE_LBID_RANGES;
    serializeVector<LBIDRange>(command, ranges);
    err = send_recv(command, response);
    if (err != ERR_OK)
        return err;

    if (response.length() != 1)
        return ERR_NETWORK;

    response >> err;
    CHECK_EMPTY(response);
    return err;
}

int DBRM::clear() DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("clear");
#endif
	ByteStream command, response;
	uint8_t err;

	command << CLEAR;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;
	
	if (response.length() != 1)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::checkConsistency() throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("checkConsistency");
#endif
	bool locked[2] = {false, false};
	
	try {
		em->checkConsistency();
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		return -1;
	}		
	
	try {
		vbbm->lock(VBBM::READ);
		locked[0] = true;
		vss->lock(VSS::READ);
		locked[1] = true;
		vss->checkConsistency(*vbbm, *em);
		vss->release(VSS::READ);
		locked[1] = false;
		vbbm->release(VBBM::READ);
		locked[0] = false;
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		if (locked[1])
			vss->release(VSS::READ);
		if (locked[0])
			vbbm->release(VBBM::READ);
		return -1;
	}
		
	try {
		vbbm->lock(VBBM::READ);
		vbbm->checkConsistency();
		vbbm->release(VBBM::READ);
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		vbbm->release(VBBM::READ);
		return -1;
	}
			
	return 0;
}

int DBRM::getCurrentTxnIDs(set<VER_t> &txnList) throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("getCurrentTxnIDs");
#endif
	bool locked[2] = { false, false };

	try {
		txnList.clear();
		vss->lock(VSS::READ);
		locked[0] = true;
		copylocks->lock(CopyLocks::READ);
		locked[1] = true;
		copylocks->getCurrentTxnIDs(txnList);
		vss->getCurrentTxnIDs(txnList);
		copylocks->release(CopyLocks::READ);
		locked[1] = false;
		vss->release(VSS::READ);
		locked[0] = false;
	}
	catch (exception &e) {
		if (locked[1])
			copylocks->release(CopyLocks::READ);
		if (locked[0])
			vss->release(VSS::READ);
		cerr << e.what() << endl;
		return -1;
	}

	return 0;
}

const execplan::CalpontSystemCatalog::SCN DBRM::verID(void)
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("verID");
#endif
	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << VER_ID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: SessionManager::verID(): network error" << endl;
		return -1;
	}
	
	if (response.length() != 5) {
		cerr << "DBRM: SessionManager::verID(): bad response" << endl;
// 		log("DBRM: SessionManager::verID(): bad response", logging::LOG_TYPE_WARNING);
		return -1;
	}

	response >> err;
	response >> ret;
	CHECK_EMPTY(response);
	return ret;
}

const execplan::CalpontSystemCatalog::SCN DBRM::sysCatVerID(void)
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("sysCatVerID");
#endif
	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << SYSCAT_VER_ID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: SessionManager::sysCatVerID(): network error" << endl;
		return -1;
	}
	
	if (response.length() != 5) {
		cerr << "DBRM: SessionManager::sysCatVerID(): bad response" << endl;
// 		log("DBRM: SessionManager::verID(): bad response", logging::LOG_TYPE_WARNING);
		return -1;
	}

	response >> err;
	response >> ret;
	CHECK_EMPTY(response);
	return ret;
}
const TxnID 
	DBRM::newTxnID(const SessionManagerServer::SID session, bool block, bool isDDL)
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("newTxnID");
		TRACER_ADDINPUT(session);
		TRACER_ADDBOOLINPUT(block);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err, tmp;
	uint32_t tmp32;
	TxnID ret;
	
	command << NEW_TXN_ID << session << (uint8_t) block << (uint8_t)isDDL;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: SessionManager::newTxnID(): network error");
		ret.valid = false;
		return ret;
	}		

	if (response.length() != 6) {
		log("DBRM: SessionManager::newTxnID(): bad response");
		ret.valid = false;
		return ret;
	}
	
	response >> err;
	response >> tmp32;
	ret.id = tmp32;
	response >> tmp;
	ret.valid = (tmp == 0 ? false : true);
	CHECK_EMPTY(response);
	return ret;
}

void DBRM::committed(TxnID& txnid)
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("committed");
		TRACER_ADDINPUT(txnid);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;

	command << COMMITTED << (uint32_t) txnid.id << (uint8_t) txnid.valid;
	err = send_recv(command, response);
	txnid.valid = false;
	if (err != ERR_OK)
		log("DBRM: error: SessionManager::committed() failed");
	else if (response.length() != 1)
		log("DBRM: error: SessionManager::committed() failed (bad response)",
			logging::LOG_TYPE_ERROR);
	response >> err;
	if (err != ERR_OK)
		log("DBRM: error: SessionManager::committed() failed (valid error code)",
			logging::LOG_TYPE_ERROR);

}


void DBRM::rolledback(TxnID& txnid)
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("rolledback");
		TRACER_ADDINPUT(txnid);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err, tmp;

	command << ROLLED_BACK << (uint32_t) txnid.id << (uint8_t) txnid.valid;
	err = send_recv(command, response);
	txnid.valid = false;

	if (err != ERR_OK)
		log("DBRM: error: SessionManager::rolledback() failed (network)");
	else if (response.length() != 1)
		log("DBRM: error: SessionManager::rolledback() failed (bad response)",
			logging::LOG_TYPE_ERROR);
	response >> tmp;
	if (tmp != ERR_OK)
		log("DBRM: error: SessionManager::rolledback() failed (valid error code)",
			logging::LOG_TYPE_ERROR);
}

int DBRM::getUnlockedLBIDs(BlockList_t *list) DBRM_THROW
{
	bool locked = false;

	list->clear();

	try {
		vss->lock(VSS::READ);
		locked = true;
		vss->getUnlockedLBIDs(*list);
		vss->release(VSS::READ);
		locked = false;
		return 0;
	}
	catch(exception &e) {
		if (locked)
			vss->release(VSS::READ);
		cerr << e.what() << endl;
		return -1;		
	}
}


const TxnID DBRM::getTxnID
	(const SessionManagerServer::SID session)
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getTxnID");
		TRACER_ADDINPUT(session);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err, tmp8;
	uint32_t tmp32;
	TxnID ret;

	command << GET_TXN_ID << (uint32_t) session;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::getTxnID() failed (network)", logging::LOG_TYPE_ERROR);
		ret.valid = false;
		return ret;
	}
	
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::getTxnID() failed (got an error)",
			logging::LOG_TYPE_ERROR);
		ret.valid = false;
		return ret;
	}

	response >> tmp32 >> tmp8;
	ret.id = tmp32;
	ret.valid = tmp8;
	return ret;
}
	
const SIDTIDEntry* DBRM::SIDTIDMap(int& len)
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("SIDTIDMap");
		TRACER_ADDOUTPUT(len);
		TRACER_WRITE;
	}	
#endif

	ByteStream  command, response;
	uint8_t err, tmp8;
	uint32_t tmp32;
	int i;
	SIDTIDEntry* ret;
	string tmpstr;

	command << SID_TID_MAP;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::SIDTIDEntry() failed (network)");
		return NULL;
	}
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::SIDTIDEntry() failed (valid error code)",
			logging::LOG_TYPE_ERROR);
		return NULL;
	}
	
	response >> tmp32;
	len = (int) tmp32;

	ret = new SIDTIDEntry[len];
	
	for (i = 0; i < len; i++) {
		response >> tmp32 >> tmp8;
		ret[i].txnid.id = tmp32;
		ret[i].txnid.valid = (tmp8 == 0 ? false : true);
		response >> tmp32;
		ret[i].sessionid = tmp32;
		response >> tmp32;
		ret[i].tableOID = tmp32;
		response >> tmp32;
		ret[i].processID = tmp32;
		response >> tmpstr;
		strncpy(ret[i].processName, tmpstr.c_str(), MAX_PROCNAME-1);
	}

	CHECK_EMPTY(response);
	return ret;
}

char * DBRM::getShmContents(int &len)
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getShmContents");
		TRACER_ADDOUTPUT(len);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
	char *ret;

	command << GET_SHM_CONTENTS;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: warning: SessionManager::getShmContents() failed (network error)");
		return NULL;
	}
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: warning: SessionManager::getShmContents() failed (valid error code)");
		return NULL;
	}
	
	len = response.length();
	ret = new char[len];
	memcpy(ret, response.buf(), len);
	return ret;
}

const uint32_t DBRM::getUnique32()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("getUnique32");
#endif

	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << GET_UNIQUE_UINT32;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: error: SessionManager::getUnique_uint32() failed (network)\n";
		log("DBRM: error: SessionManager::getUnique_uint32() failed (network)", logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: error: SessionManager::getUnique_uint32() failed check the controllernode");
		return 0;
	}

	/* Some jobsteps don't need the connection after this so close it to free up
	resources on the controller node */
	/* Comment the following 4 lines out. The DBRM instance is a singleton so no need to
	remove the client. Plus, it may cause weird network issue when the socket is being
	released and re-established very quickly*/
	//pthread_mutex_lock(&mutex);
	//delete msgClient;
	//msgClient = NULL;
	//pthread_mutex_unlock(&mutex);
	
	response >> err;
	if (err != ERR_OK) {
		cerr << "DBRM: error: SessionManager::getUnique_uint32() failed (got an error)\n";
		log("DBRM: error: SessionManager::getUnique_uint32() failed (got an error)",
			logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: error: SessionManager::getUnique_uint32() failed check the controllernode");
		return 0;
	}
	response >> ret;
// 	cerr << "DBRM returning " << ret << endl;
	return ret;
}

int8_t DBRM::setTableLock (  const OID_t tableOID, const u_int32_t sessionID,  const u_int32_t processID, const std::string processName, bool lock ) 
{

#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("setTableLock");
		TRACER_ADDINPUT(sessionID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
    int8_t	rc = 0;

	command << SET_TABLE_LOCK << (uint32_t) tableOID << (uint32_t) sessionID << (uint32_t) processID << processName << (uint8_t) lock;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		std::ostringstream oss;
		oss << "DBRM: error: SessionManager::setTableLock() tolock is " << lock << " failed (network)";
		log(oss.str(), logging::LOG_TYPE_ERROR);
		return -1;
	}
	
	response >> err;
	if (err != ERR_OK) {
		std::ostringstream oss;
		oss << "DBRM: error: SessionManager::setTableLock() tolock is " << lock << " failed (got an error)";
		log(oss.str(), logging::LOG_TYPE_ERROR);
	}
	else
	{
		response >> err;
	}
	rc = err;
	return rc;
}

int8_t DBRM::updateTableLock (  const OID_t tableOID, u_int32_t&  processID, std::string & processName) 
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("updateTableLock");
		TRACER_ADDINPUT(tableOID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
    int8_t	rc = 0;

	command << UPDATE_TABLE_LOCK << (uint32_t) tableOID << (uint32_t) processID << processName;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		std::ostringstream oss;
		oss << "DBRM: error: SessionManager::updateTableLock() failed (network)";
		log(oss.str(), logging::LOG_TYPE_ERROR);
		return -1;
	}
	
	response >> err;
	if (err != ERR_OK) {
		uint32_t tmp32;
		response >> tmp32;
		processID = tmp32;
		response >> processName;
		std::ostringstream oss;
		oss << "DBRM: error: SessionManager::updateTableLock() failed (got an error)";
		log(oss.str(), logging::LOG_TYPE_ERROR);
	}
	else
	{
		response >> err;
		uint32_t tmp32;
		response >> tmp32;
		processID = tmp32;
		response >> processName;
	}
	rc = err;
	return rc;
}

int8_t DBRM::getTableLockInfo ( const OID_t tableOID, u_int32_t & processID,
	std::string & processName, bool & lockStatus, SessionManagerServer::SID & sid )
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getTableLockInfo");
		TRACER_ADDINPUT(tableOID);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err;
    int8_t	rc = 0;
	u_int32_t tmp32;
	
	command << GET_TABLE_LOCK << (uint32_t) tableOID << (uint32_t) processID << processName << (uint8_t) lockStatus;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::getTableLockInfo() failed (network)", logging::LOG_TYPE_ERROR);
		return -1;
	}
	
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::getTableLockInfo() failed (got an error)",
			logging::LOG_TYPE_ERROR);
		return -1;
	}
	
	response >> err;  //bytestream only take unsigned int8
	rc = err;
	response >>  tmp32;
	processID = tmp32;
	response >> processName;
	response >> err;
	lockStatus = err;
	response >> tmp32;
	sid = tmp32;
	return rc;	
}

void DBRM::getTableLocksInfo (std::vector< SIDTIDEntry> & sidTidEntries)
{
	#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getTableLocksInfo");
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err, size;
	uint32_t tmp32;
	string processName;
	command << GET_TABLE_LOCKS;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::getTableLocksInfo() failed (network)", logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: error: SessionManager::getTableLocksInfo() failed (network)");
	}
	
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::getTableLocksInfo() failed (got an error)",
			logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: error: SessionManager::getTableLocksInfo() failed(got an error)");
	}
	sidTidEntries.clear();
	SIDTIDEntry aEntry;
	response >> size;
	for ( uint8_t i = 0; i < size; i++ )
	{
		response >>  tmp32;
		aEntry.txnid.id = tmp32;
		response >>  err;
		aEntry.txnid.valid = (err != 0);
		response >>  tmp32;
		aEntry.sessionid = tmp32;
		response >>  tmp32;
		aEntry.tableOID = tmp32;
		response >> tmp32;
		aEntry.processID = tmp32;
		response >>  processName;
		memcpy ( aEntry.processName, processName.c_str(), processName.length());
		aEntry.processName[ processName.length() ] = '\0'; // add null terminator
		sidTidEntries.push_back( aEntry );
	}
	
}

bool DBRM::isEMEmpty() throw()
{
	bool res = false;
	try {
		res = em->empty();
	} catch (...) {
		res = false;
	}
	return res;
}

vector<InlineLBIDRange> DBRM::getEMFreeListEntries() throw()
{
	vector<InlineLBIDRange> res;
	try {
		res = em->getFreeListEntries();
	} catch (...) {
		res.clear();
	}
	return res;
}

int DBRM::takeSnapshot() throw ()
{
	ByteStream command, response;
	uint8_t  err;

	command << TAKE_SNAPSHOT;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;
	return 0;
}

bool DBRM::isSystemReady() throw()
{
	try {
#ifdef BRM_INFO
		if (fDebug)
		{
			TRACER_WRITELATER("isSystemReady");
			TRACER_WRITE;
		}	
#endif

		ByteStream command, response;
		uint8_t err;

		command << GET_SYSTEM_STATE;
		err = send_recv(command, response);
		if (err != ERR_OK) {
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::getSystemState() failed (network)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return false;
		}
		
		response >> err;
		if (err != ERR_OK) {
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::getSystemState() failed (got an error)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return false;
		}
		response >> err;
		return (err == SessionManagerServer::SS_READY);
	} catch (...) {
		return false;
	}
}

void DBRM::systemIsReady(SessionManagerServer::SystemState state) throw()
{
	try {
#ifdef BRM_INFO
		if (fDebug)
		{
			TRACER_WRITELATER("isSystemReady");
			TRACER_WRITE;
		}	
#endif

		ByteStream command, response;
		uint8_t err;

		command << SET_SYSTEM_STATE << static_cast<ByteStream::byte>(state);
		err = send_recv(command, response);
		if (err != ERR_OK) {
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::setSystemState() failed (network)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return;
		}
		
		response >> err;
		if (err != ERR_OK) {
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::setSystemState() failed (got an error)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return;
		}
		return;
	} catch (...) {
		return;
	}
}

/* This waits for the lock up to 30 sec.  After 30 sec, the assumption is something
 * bad happened, and this will fix the lock state so that primproc can keep
 * running.  These prevent a non-critical problem anyway.
 */
void DBRM::lockLBIDRange(LBID_t start, uint count)
{
	bool locked = false, lockedRange = false;
	LBIDRange range;
	const uint waitInterval = 50000;  // in usec
	const uint maxRetries = 30000000/waitInterval;  // 30 secs
	uint retries = 0;

	range.start = start;
	range.size = count;

	try {
		copylocks->lock(CopyLocks::WRITE);
		locked = true;
		while (copylocks->isLocked(range) && retries < maxRetries) {
			copylocks->release(CopyLocks::WRITE);
			locked = false;
			usleep(waitInterval);
			retries++;
			copylocks->lock(CopyLocks::WRITE);
			locked = true;
		}
		if (retries >= maxRetries)
			copylocks->forceRelease(range);

		copylocks->lockRange(range, -1);
		lockedRange = true;
		copylocks->confirmChanges();
		copylocks->release(CopyLocks::WRITE);
		locked = false;
	}
	catch (...) {
		if (lockedRange)
			copylocks->releaseRange(range);
		if (locked) {
			copylocks->confirmChanges();
			copylocks->release(CopyLocks::WRITE);
		}
		throw;
	}
}

void DBRM::releaseLBIDRange(LBID_t start, uint count)
{
	bool locked = false;
	LBIDRange range;
	range.start = start;
	range.size = count;

	try {
		copylocks->lock(CopyLocks::WRITE);
		locked = true;
		copylocks->releaseRange(range);
		copylocks->confirmChanges();
		copylocks->release(CopyLocks::WRITE);
		locked = false;
	}
	catch (...) {
		if (locked) {
			copylocks->confirmChanges();
			copylocks->release(CopyLocks::WRITE);
		}
		throw;
	}
}

}   //namespace
