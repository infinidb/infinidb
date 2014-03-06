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
 * $Id: dbrm.cpp 1878 2013-05-02 15:17:12Z dcathey $
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

#include "oamcache.h"
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
using namespace oam;

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

int DBRM::lookupLocal_DBroot(OID_t oid, uint32_t dbroot, uint32_t partitionNum, uint16_t segmentNum,
		uint32_t fileBlockOffset, LBID_t& lbid) throw()
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
                return em->lookupLocal_DBroot(oid, dbroot, partitionNum, segmentNum, fileBlockOffset, lbid);
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
int DBRM::markExtentInvalid(const LBID_t lbid, 
                            execplan::CalpontSystemCatalog::ColDataType colDataType) DBRM_THROW
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

	command << MARKEXTENTINVALID << (uint64_t)lbid << (uint32_t)colDataType;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	CHECK_EMPTY(response);
	return err;
}

int DBRM::markExtentsInvalid(const vector<LBID_t> &lbids,
                             const std::vector<execplan::CalpontSystemCatalog::ColDataType>& colDataTypes) DBRM_THROW
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("markExtentsInvalid");
#endif	
	ByteStream command, response;
	uint8_t err;
	uint32_t size = lbids.size(), i;

	command << MARKMANYEXTENTSINVALID << size;
	for (i = 0; i < size; i++)
    {
		command << (uint64_t) lbids[i];
        command << (uint32_t) colDataTypes[i];
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
	uint8_t err = 0;
	if (cpInfos.size() == 0)
		return err;

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
			TRACER_ADDINPUT(it->type);
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
				   (uint32_t)it->type      <<
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

int DBRM::vssLookup(LBID_t lbid, const QueryContext &verInfo, VER_t txnID, VER_t *outVer,
	bool *vbFlag, bool vbOnly) throw()
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("vssLookup");
		TRACER_ADDINPUT(lbid);
		TRACER_ADDINPUT(verInfo);
		TRACER_ADDINPUT(txnID);
		TRACER_ADDBOOLINPUT(vbOnly);
		TRACER_WRITE;
	}
	
#endif
	if (!vbOnly && vss->isEmpty())
	{
		*outVer = 0;
		*vbFlag = false;
		return -1;
	}

	bool locked = false;

	try {
		int rc = 0;
		vss->lock(VSS::READ);
		locked = true;
		rc = vss->lookup(lbid, verInfo, txnID, outVer, vbFlag, vbOnly);
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

int DBRM::bulkVSSLookup(const std::vector<LBID_t> &lbids, const QueryContext_vss &verInfo,
	VER_t txnID, std::vector<VSSData> *out) 
{
	uint32_t i;
	bool locked = false;
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
				vd.returnCode = vss->lookup(lbids[i], verInfo, txnID, &vd.verID, &vd.vbFlag, false);
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

VER_t DBRM::getCurrentVersion(LBID_t lbid, bool *isLocked) const
{
	bool locked = false;
	VER_t ret = 0;

	try {
		vss->lock(VSS::READ);
		locked = true;
		ret = vss->getCurrentVersion(lbid, isLocked);
		vss->release(VSS::READ);
		locked = false;
	}
	catch(exception &e) {
		cerr << e.what() << endl;
		if (locked)
			vss->release(VSS::READ);
		throw;
	}
	return ret;
}

int DBRM::bulkGetCurrentVersion(const vector<LBID_t> &lbids, vector<VER_t> *versions,
		vector<bool> *isLocked) const
{
	bool locked = false;

	versions->resize(lbids.size());
	if (isLocked != NULL)
		isLocked->resize(lbids.size());
	try {
		vss->lock(VSS::READ);
		locked = true;
		if (isLocked != NULL) {
			bool tmp=false;
			for (uint32_t i = 0; i < lbids.size(); i++) {
				(*versions)[i] = vss->getCurrentVersion(lbids[i], &tmp);
				(*isLocked)[i] = tmp;
			}
		}
		else
			for (uint32_t i = 0; i < lbids.size(); i++)
				(*versions)[i] = vss->getCurrentVersion(lbids[i], NULL);
		vss->release(VSS::READ);
		locked = false;
		return 0;
	}
	catch (exception &e) {
		versions->clear();
		cerr << e.what() << endl;
		if (locked)
			vss->release(VSS::READ);
		return -1;
	}
}


VER_t DBRM::getHighestVerInVB(LBID_t lbid, VER_t max) const
{
	bool locked = false;
	VER_t ret = -1;

	try {
		vss->lock(VSS::READ);
		locked = true;
		ret = vss->getHighestVerInVB(lbid, max);
		vss->release(VSS::READ);
		locked = false;
	}
	catch(exception &e) {
		cerr << e.what() << endl;
		if (locked)
			vss->release(VSS::READ);
		throw;
	}
	return ret;
}

bool DBRM::isVersioned(LBID_t lbid, VER_t ver) const
{
	bool ret = false;
	bool locked = false;

	try {
		vss->lock(VSS::READ);
		locked = true;
		ret = vss->isVersioned(lbid, ver);
		vss->release(VSS::READ);
		locked = false;
	}
	catch(exception &e) {
		cerr << e.what() << endl;
		if (locked)
			vss->release(VSS::READ);
		throw;
	}
	return ret;
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
			return ERR_NETWORK;
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
// Send a request to create a "stripe" of column extents for the specified
// column OIDs and DBRoot.
//------------------------------------------------------------------------------
int DBRM::createStripeColumnExtents(
	const std::vector<CreateStripeColumnExtentsArgIn>& cols,
	uint16_t  dbRoot,
	uint32_t& partitionNum,
	uint16_t& segmentNum,
	std::vector<CreateStripeColumnExtentsArgOut>& extents) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	 	TRACER_WRITELATER("createStripeColumnExtents");
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t  err;
	uint16_t tmp16;
	uint32_t tmp32;

	command << CREATE_STRIPE_COLUMN_EXTENTS;
	serializeInlineVector(command, cols);
	command << dbRoot << partitionNum;

	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	try {
		response >> err;
		if (err != 0)
			return (int) err;

		response >> tmp32;
		partitionNum = tmp32;
		response >> tmp16;
		segmentNum = tmp16;
		deserializeInlineVector(response, extents);
	}
	catch (exception &e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	CHECK_EMPTY(response);
	return 0;
}

//------------------------------------------------------------------------------
// Send a request to create a column extent for the specified OID and DBRoot.
//------------------------------------------------------------------------------
int DBRM::createColumnExtent_DBroot(OID_t oid,
	uint32_t  colWidth,
	uint16_t  dbRoot,
	uint32_t& partitionNum,
	uint16_t& segmentNum,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
	LBID_t&    lbid,
	int&       allocdSize,
	uint32_t& startBlockOffset) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	 	TRACER_WRITELATER("createColumnExtent_DBroot");
		TRACER_ADDINPUT(oid);
	 	TRACER_ADDINPUT(colWidth);
	 	TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdSize);
		TRACER_ADDOUTPUT(startBlockOffset);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t  err;
    uint32_t tmp8 = (uint8_t)colDataType;
	uint16_t tmp16;
	uint32_t tmp32;
	uint64_t tmp64;

	command << CREATE_COLUMN_EXTENT_DBROOT << (ByteStream::quadbyte) oid <<
		colWidth << dbRoot << partitionNum << segmentNum << tmp8;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	try {
		response >> err;
		if (err != 0)
			return (int) err;

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
// Send a request to create a column extent for the exact segment file
// specified by the requested OID, DBRoot, partition, and segment.
//------------------------------------------------------------------------------
int DBRM::createColumnExtentExactFile(OID_t oid,
	uint32_t  colWidth,
	uint16_t  dbRoot,
	uint32_t partitionNum,
	uint16_t segmentNum,
    execplan::CalpontSystemCatalog::ColDataType colDataType,
    LBID_t&    lbid,
	int&       allocdSize,
	uint32_t& startBlockOffset) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
	 	TRACER_WRITELATER("createColumnExtentExactFile");
		TRACER_ADDINPUT(oid);
	 	TRACER_ADDINPUT(colWidth);
	 	TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDINT64OUTPUT(lbid);
		TRACER_ADDOUTPUT(allocdSize);
		TRACER_ADDOUTPUT(startBlockOffset);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t  err;
    uint8_t  tmp8;
	uint16_t tmp16;
	uint32_t tmp32;
	uint64_t tmp64;

    tmp8 = (uint8_t)colDataType;
	command << CREATE_COLUMN_EXTENT_EXACT_FILE << (ByteStream::quadbyte) oid <<
		colWidth << dbRoot << partitionNum << segmentNum << tmp8;
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	try {
		response >> err;
		if (err != 0)
			return (int) err;

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
	uint16_t  dbRoot,
	uint32_t  partitionNum,
	uint16_t  segmentNum,
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
// Send a request to delete a set extents for the specified column OID and
// DBRoot, and to return the extents to the free list.  HWMs for the last
// stripe of extents in the specified DBRoot are updated accordingly.
//------------------------------------------------------------------------------
int DBRM::rollbackColumnExtents_DBroot(OID_t oid,
	bool       bDeleteAll,
	uint16_t  dbRoot,
	uint32_t  partitionNum,
	uint16_t  segmentNum,
	HWM_t      hwm) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("rollbackColumnExtents");
		TRACER_ADDINPUT(oid);
		TRACER_ADDBOOLINPUT(bDeleteAll);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDINPUT(hwm);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << ROLLBACK_COLUMN_EXTENTS_DBROOT << (ByteStream::quadbyte) oid <<
		(uint8_t)bDeleteAll << dbRoot << partitionNum <<
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
// OID and DBRoot, and to return the extents to the free list.  HWMs for the
// last stripe of extents are updated accordingly.
//------------------------------------------------------------------------------
int DBRM::rollbackDictStoreExtents_DBroot(OID_t oid,
	uint16_t            dbRoot,
	uint32_t            partitionNum,
	const vector<uint16_t>& segNums,
	const vector<HWM_t>& hwms) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("rollbackDictStoreExtents");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTINPUT(dbRoot);
		TRACER_ADDINPUT(partitionNum);
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << ROLLBACK_DICT_STORE_EXTENTS_DBROOT <<
		(ByteStream::quadbyte) oid <<
		dbRoot << partitionNum;
	serializeVector(command, segNums);
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

	try {
		deleteAISequence(oid);
	}
	catch (...) { }   // an error here means a network problem, will be caught elsewhere

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

	try {
		for (uint32_t i = 0; i < oids.size(); i++)
			deleteAISequence(oids[i]);
	}
	catch (...) { }   // an error here means a network problem, will be caught elsewhere

	return err;
}
	
//------------------------------------------------------------------------------
// Return the last local HWM for the specified OID and DBroot. The corresponding
// partition number, and segment number are returned as well.  This function
// can be used by cpimport for example to find out where the current "end-of-
// data" is, so that cpimport will know where to begin adding new rows.
// If no available or outOfService extent is found, then bFound is returned
// as false.
//------------------------------------------------------------------------------
int DBRM::getLastHWM_DBroot(int oid, uint16_t dbRoot, uint32_t& partitionNum,
				uint16_t& segmentNum, HWM_t& hwm,
				int& status, bool& bFound) throw()
{	
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getLastHWM_DBroot");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_ADDOUTPUT(partitionNum);
		TRACER_ADDSHORTOUTPUT(segmentNum);
		TRACER_ADDOUTPUT(hwm);
		TRACER_ADDOUTPUT(status);
		TRACER_WRITE;
	}
#endif

	try {
		hwm = em->getLastHWM_DBroot(oid, dbRoot, partitionNum, segmentNum,
			status, bFound);
	}
	catch (exception& e) {
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
	HWM_t& hwm, int& status) throw()
{	
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getLocalHWM");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDOUTPUT(hwm);
		TRACER_ADDOUTPUT(status);
		TRACER_WRITE;
	}	
#endif

	try {
		hwm = em->getLocalHWM(oid, partitionNum, segmentNum, status);
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

int DBRM::bulkSetHWM(const vector<BulkSetHWMArg> &v, VER_t transID) DBRM_THROW
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("bulkSetHWM");
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << BULK_SET_HWM;
	serializeInlineVector(command, v);
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

int DBRM::bulkUpdateDBRoot(const vector<BulkUpdateDBRootArg> &args)
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("bulkUpdateDBRoot");
		TRACER_WRITE;
	}
#endif

	ByteStream command, response;
	uint8_t err;

	command << BULK_UPDATE_DBROOT;
	serializeInlineVector(command, args);
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
// For the specified OID and PM number, this function will return a vector
// of objects carrying HWM info (for the last segment file) and block count
// information about each DBRoot assigned to the specified PM.
//------------------------------------------------------------------------------
int DBRM::getDbRootHWMInfo(OID_t oid, uint16_t pmNumber,
	EmDbRootHWMInfo_v& emDBRootHwmInfos) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getDbRootHWMInfo");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTINPUT(pmNumber);
		TRACER_WRITE;
	}	
#endif

	try {
		em->getDbRootHWMInfo(oid, pmNumber, emDBRootHwmInfos);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}
	
	return ERR_OK;
}

//------------------------------------------------------------------------------
// Return the status or state of the extents in the segment file specified
// by the arguments: oid, partitionNum, and segment Num.
//------------------------------------------------------------------------------
int DBRM::getExtentState(OID_t oid, uint32_t partitionNum,
	uint16_t segmentNum, bool& bFound, int& status) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getExtentState");
		TRACER_ADDINPUT(oid);
		TRACER_ADDINPUT(partitionNum);
		TRACER_ADDSHORTINPUT(segmentNum);
		TRACER_ADDOUTPUT(status);
		TRACER_WRITE;
	}	
#endif
	try {
		em->getExtentState(oid, partitionNum,
			segmentNum, bFound, status);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return ERR_FAILURE;
	}

	return ERR_OK;
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

int DBRM::getExtents_dbroot(int OID, std::vector<struct EMEntry>& entries,
		const uint16_t dbroot) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("getExtents_dbroot");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}	
#endif

	try {
		em->getExtents_dbroot(OID, entries, dbroot);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return -1;
	}
	return 0;
}

//------------------------------------------------------------------------------
// Return the number of extents for the specified OID and DBRoot.
// Any out-of-service extents can optionally be included or excluded.
//------------------------------------------------------------------------------
int DBRM::getExtentCount_dbroot(int OID, uint16_t dbroot,
	bool incOutOfService, uint64_t& numExtents) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
  		TRACER_WRITELATER("getExtentCount_dbroot");
		TRACER_ADDINPUT(OID);
		TRACER_WRITE;
	}	
#endif

	try {
		em->getExtentCount_dbroot(OID, dbroot, incOutOfService, numExtents);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		return -1;
	}
	return 0;
}

//------------------------------------------------------------------------------
// Gets the DBRoot for the specified system catalog OID.
// Function assumes the specified System Catalog OID is fully contained on
// a single DBRoot, as the function only searches for and returns the first
// DBRoot entry that is found in the extent map.
//------------------------------------------------------------------------------
int DBRM::getSysCatDBRoot(OID_t oid, uint16_t& dbRoot) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("getSysCatDBRoot");
		TRACER_ADDINPUT(oid);
		TRACER_ADDSHORTOUTPUT(dbRoot);
		TRACER_WRITE;
	}	
#endif

    try {
        em->getSysCatDBRoot(oid, dbRoot);
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
      const std::set<LogicalPartition>& partitionNums, string& emsg) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("deletePartition");
		std::ostringstream oss;
		oss << "partitionNum: " 
		std::set<LogicalPartition>::const_iterator partIt;
		for (partIt = partitionNums.begin(); partIt != partitionNums.end(); ++partIt)
			oss << (*it) << " "
		oss << "; OIDS: ";
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
	command << DELETE_PARTITION;
	serializeSet<LogicalPartition>(command, partitionNums);
	uint32_t oidSize = oids.size();
	command << oidSize;
	for ( unsigned i=0; i<oidSize; i++)
		command << (ByteStream::quadbyte) oids[i];
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	if (err != 0)
		response >> emsg;

	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Mark all extents as out of service, for the specified OID(s) and partition
// number.
//------------------------------------------------------------------------------
int DBRM::markPartitionForDeletion(const std::vector<OID_t>& oids,
      const std::set<LogicalPartition>& partitionNums, string& emsg) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("markPartitionForDeletion");
		std::ostringstream oss;
		oss << "partitionNum: " 
		std::set<LogicalPartition>::const_iterator partIt;
		for (partIt = partitionNums.begin(); partIt != partitionNums.end(); ++partIt)
			oss << (*it) << " "
		oss << "; OIDS: ";
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
	command << MARK_PARTITION_FOR_DELETION;
	serializeSet<LogicalPartition>(command, partitionNums);
	uint32_t oidSize = oids.size();
	command << oidSize;
	for ( unsigned i=0; i<oidSize; i++)
		command << (ByteStream::quadbyte) oids[i];
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	if (err)
		response >> emsg;
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
      const std::set<LogicalPartition>& partitionNums, string& emsg) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("restorePartition");
		std::ostringstream oss;
		oss << "partitionNum: " 
		std::set<LogicalPartition>::const_iterator partIt;
		for (partIt = partitionNums.begin(); partIt != partitionNums.end(); ++partIt)
			oss << (*it) << " "
		oss << "; OIDS: ";
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

	command << RESTORE_PARTITION;
	serializeSet<LogicalPartition>(command, partitionNums);
	uint32_t oidSize = oids.size();
	command << oidSize;
	for ( unsigned i=0; i<oidSize; i++)
		command << (ByteStream::quadbyte) oids[i];
	
	err = send_recv(command, response);
	if (err != ERR_OK)
		return err;

	if (response.length() == 0)
		return ERR_NETWORK;

	response >> err;
	if (err)
		response >> emsg;

	CHECK_EMPTY(response);
	return err;
}

//------------------------------------------------------------------------------
// Return all the out-of-service partitions for the specified OID.
//------------------------------------------------------------------------------
int DBRM::getOutOfServicePartitions(OID_t oid,
  std::set<LogicalPartition>& partitionNums) throw()
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

//------------------------------------------------------------------------------
// Delete all extents for the specified DBRoot
//------------------------------------------------------------------------------
int DBRM::deleteDBRoot(uint16_t dbroot) DBRM_THROW
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITENOW("deleteDBRoot");
		std::ostringstream oss;
		oss << "DBRoot: " << dbroot;
		TRACER_WRITEDIRECT(oss.str());
	}
#endif

	ByteStream command, response;
	uint8_t err;
	command << DELETE_DBROOT;
	uint32_t q = static_cast<uint32_t>(dbroot);
	command << q;
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
// Does the specified DBRoot have any extents.
// Returns an error if extentmap shared memory is not loaded.
//------------------------------------------------------------------------------
int DBRM::isDBRootEmpty(uint16_t dbroot,
	bool& isEmpty, std::string& errMsg) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
		TRACER_WRITELATER("isDBRootEmpty");
		TRACER_ADDINPUT(dbroot);
		TRACER_WRITE;
	}	
#endif

	errMsg.clear();
	try {
		isEmpty = em->isDBRootEmpty(dbroot);
	}
	catch (exception& e) {
		cerr << e.what() << endl;
		errMsg = e.what();
		return ERR_FAILURE;
	}
	
	return ERR_OK;
}

int DBRM::writeVBEntry(VER_t transID, LBID_t lbid, OID_t vbOID,
	uint32_t vbFBO) DBRM_THROW
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

struct _entry {
	_entry(LBID_t l) : lbid(l) { };
	LBID_t lbid;
	inline bool operator<(const _entry &e) const {
		return ((e.lbid >> 10) < (lbid >> 10));
	}
};

int DBRM::getDBRootsForRollback(VER_t transID, vector<uint16_t> *dbroots) throw()
{
#ifdef BRM_INFO
	if (fDebug)
	{
	  	TRACER_WRITELATER("getDBRootsForRollback");
		TRACER_ADDINPUT(transID);
		TRACER_WRITE;
	}
#endif
	bool locked[2] = {false, false};
	set<OID_t> vbOIDs;
	set<OID_t>::iterator vbIt;
	vector<LBID_t> lbidList;
	uint32_t i, size;
	uint32_t tmp32;
	OID_t vbOID;
	int err;

	set<_entry> lbidPruner;
	set<_entry>::iterator it;

	try {
		vbbm->lock(VBBM::READ);
		locked[0] = true;
		vss->lock(VSS::READ);
		locked[1] = true;

		vss->getUncommittedLBIDs(transID, lbidList);

		// prune the list; will leave at most 1 entry per 1024-lbid range
		for (i = 0, size = lbidList.size(); i < size; i++)
			lbidPruner.insert(_entry(lbidList[i]));

		// get the VB oids
		for (it = lbidPruner.begin(); it != lbidPruner.end(); ++it) {
			err = vbbm->lookup(it->lbid, transID, vbOID, tmp32);
			if (err)   // this error will be caught by DML; more appropriate to handle it there
				continue;
			vbOIDs.insert(vbOID);
		}

		// get the dbroots
		for (vbIt = vbOIDs.begin(); vbIt != vbOIDs.end(); ++vbIt) {
			err = getDBRootOfVBOID(*vbIt);
			if (err) {
				ostringstream os;
				os << "DBRM::getDBRootOfVBOID() returned an error looking for vbOID " << *vbIt;
				log(os.str());
				return ERR_FAILURE;
			}
			dbroots->push_back((uint16_t) err);
		}

		vss->release(VSS::READ);
		locked[1] = false;
		vbbm->release(VBBM::READ);
		locked[0] = false;

		return ERR_OK;
	}
	catch (exception &e) {
		if (locked[0])
			vbbm->release(VBBM::READ);
		if (locked[1])
			vss->release(VSS::READ);
		return -1;
	}

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

		
int DBRM::beginVBCopy(VER_t transID, uint16_t dbRoot, const LBIDRange_v& ranges,
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

	command << BEGIN_VB_COPY << (ByteStream::quadbyte) transID << dbRoot;
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
	ByteStream command, response;
	uint8_t err;

	command << BRM_CLEAR;
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

const QueryContext DBRM::verID()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("verID");
#endif
	ByteStream command, response;
	uint8_t err;
	QueryContext ret;

	command << VER_ID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: SessionManager::verID(): network error" << endl;
		ret.currentScn = -1;
		return ret;
	}

	try {
		response >> err;
		response >> ret;
		CHECK_EMPTY(response);
	}
	catch (exception &e) {
		cerr << "DBRM: SessionManager::verID(): bad response" << endl;
 		log("DBRM: SessionManager::verID(): bad response", logging::LOG_TYPE_WARNING);
		ret.currentScn = -1;
	}
	return ret;
}

const QueryContext DBRM::sysCatVerID()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("sysCatVerID");
#endif
	ByteStream command, response;
	uint8_t err;
	QueryContext ret;

	command << SYSCAT_VER_ID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: SessionManager::sysCatVerID(): network error" << endl;
		ret.currentScn = -1;
		return ret;
	}
	
	try {
		response >> err;
		response >> ret;
		CHECK_EMPTY(response);
	}
	catch (exception &e) {
		cerr << "DBRM: SessionManager::sysCatVerID(): bad response" << endl;
 		log("DBRM: SessionManager::sysCatVerID(): bad response", logging::LOG_TYPE_WARNING);
		ret.currentScn = -1;
	}
	return ret;
}


const TxnID DBRM::newTxnID(const SessionManagerServer::SID session, bool block,
		bool isDDL)
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
	
boost::shared_array<SIDTIDEntry> DBRM::SIDTIDMap(int& len)
{
#ifdef BRM_INFO
 	if (fDebug)
	{
	 	TRACER_WRITELATER("SIDTIDMap");
		TRACER_ADDOUTPUT(len);
		TRACER_WRITE;
	}	
#endif

	ByteStream command, response;
	uint8_t err, tmp8;
	uint32_t tmp32;
	int i;
	boost::shared_array<SIDTIDEntry> ret;

	command << SID_TID_MAP;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::SIDTIDEntry() failed (network)");
		return ret;
	}
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: error: SessionManager::SIDTIDEntry() failed (valid error code)",
			logging::LOG_TYPE_ERROR);
		return ret;
	}
	
	response >> tmp32;
	len = (int) tmp32;
	ret.reset(new SIDTIDEntry[len]);
	
	for (i = 0; i < len; i++) {
		response >> tmp32 >> tmp8;
		ret[i].txnid.id = tmp32;
		ret[i].txnid.valid = (tmp8 == 0 ? false : true);
		response >> tmp32;
		ret[i].sessionid = tmp32;
	}

	CHECK_EMPTY(response);
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
		cerr << "DBRM: getUnique32() failed (network)\n";
		log("DBRM: getUnique32() failed (network)", logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: getUnique32() failed check the controllernode");
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
		cerr << "DBRM: getUnique32() failed (got an error)\n";
		log("DBRM: getUnique32() failed (got an error)",
			logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: getUnique32() failed check the controllernode");
		return 0;
	}
	response >> ret;
// 	cerr << "DBRM returning " << ret << endl;
	return ret;
}

const uint64_t DBRM::getUnique64()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("getUnique64");
#endif

	ByteStream command, response;
	uint8_t err;
	uint64_t ret;

	command << GET_UNIQUE_UINT64;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: getUnique64() failed (network)\n";
		log("DBRM: getUnique64() failed (network)", logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: getUnique64() failed check the controllernode");
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
		cerr << "DBRM: getUnique64() failed (got an error)\n";
		log("DBRM: getUnique64() failed (got an error)",
			logging::LOG_TYPE_ERROR);
		throw runtime_error("DBRM: getUnique64() failed check the controllernode");
		return 0;
	}
	response >> ret;
// 	cerr << "DBRM returning " << ret << endl;
	return ret;
}

void DBRM::sessionmanager_reset()
{
	ByteStream command, response;
	command << SM_RESET;
	send_recv(command, response);
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

int DBRM::getSystemReady() throw()
{
	uint32_t stateFlags;
	if (!getSystemState(stateFlags))
	{
		return -1;
	}

	return (stateFlags & SessionManagerServer::SS_READY);
}

int DBRM::getSystemSuspended() throw()
{
	uint32_t stateFlags;
	if (getSystemState(stateFlags) < 0)
	{
		return -1;
	}

	return (stateFlags & SessionManagerServer::SS_SUSPENDED);
}

int DBRM::getSystemSuspendPending(bool& bRollback) throw()
{
	uint32_t stateFlags;
	if (getSystemState(stateFlags) < 0)
	{
		return -1;
	}
	bRollback = stateFlags & SessionManagerServer::SS_ROLLBACK;

	return (stateFlags & SessionManagerServer::SS_SUSPEND_PENDING);
}

int DBRM::getSystemShutdownPending(bool& bRollback, bool& bForce) throw()
{
	uint32_t stateFlags;
	if (getSystemState(stateFlags) < 0)
	{
		return -1;
	}
	bRollback = stateFlags & SessionManagerServer::SS_ROLLBACK;
	bForce = stateFlags & SessionManagerServer::SS_FORCE;

	return (stateFlags & SessionManagerServer::SS_SHUTDOWN_PENDING);
}

int DBRM::setSystemReady(bool bReady) throw()
{
	if (bReady)
	{
		return setSystemState(SessionManagerServer::SS_READY);
	}
	else
	{
		return clearSystemState(SessionManagerServer::SS_READY);
	}
}

int DBRM::setSystemSuspended(bool bSuspended) throw()
{
	uint32_t stateFlags = 0;

	if (bSuspended)
	{
		if (setSystemState(SessionManagerServer::SS_SUSPENDED) < 0)
		{
			return -1;
		}
	}
	else
	{
		stateFlags = SessionManagerServer::SS_SUSPENDED;
	}
	// In either case, we need to clear the pending and rollback flags
	stateFlags |= SessionManagerServer::SS_SUSPEND_PENDING;
	stateFlags |= SessionManagerServer::SS_ROLLBACK;
	return clearSystemState(stateFlags);
}

int DBRM::setSystemSuspendPending(bool bPending, bool bRollback) throw()
{
	uint32_t stateFlags = SessionManagerServer::SS_SUSPEND_PENDING;
	if (bPending)
	{
		if (bRollback)
		{
			stateFlags |= SessionManagerServer::SS_ROLLBACK;
		}
		return setSystemState(stateFlags);
	}
	else
	{
		stateFlags |= SessionManagerServer::SS_ROLLBACK;
		return clearSystemState(stateFlags);
	}
}

int DBRM::setSystemShutdownPending(bool bPending, bool bRollback, bool bForce) throw()
{
	int rtn = 0;
	uint32_t stateFlags = SessionManagerServer::SS_SHUTDOWN_PENDING;
	if (bPending)
	{
		if (bForce)
		{
			stateFlags |= SessionManagerServer::SS_FORCE;
		}
		else
		if (bRollback)
		{
			stateFlags |= SessionManagerServer::SS_ROLLBACK;
		}
		rtn = setSystemState(stateFlags);
	}
	else
	{
		stateFlags |= SessionManagerServer::SS_ROLLBACK;
		stateFlags |= SessionManagerServer::SS_FORCE;
		rtn = clearSystemState(stateFlags);		// Clears the flags that are turned on in stateFlags
	}

	return rtn;
}

/* Return the shm stateflags
 */
int DBRM::getSystemState(uint32_t& stateFlags) throw()
{
	try 
	{
#ifdef BRM_INFO
		if (fDebug)
		{
			TRACER_WRITELATER("getSystemState");
			TRACER_WRITE;
		}	
#endif
		ByteStream command, response;
		uint8_t err;

		command << GET_SYSTEM_STATE;
		err = send_recv(command, response);
		if (err != ERR_OK) 
		{
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::getSystemState() failed (network)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return -1;
		}

		response >> err;
		if (err != ERR_OK) 
		{
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::getSystemState() failed (got an error)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return -1;
		}

		response >> stateFlags;
		return 1;
	} 
	catch (...) 
	{
	}
	return -1;
}

/* Set the shm stateflags that are set in the parameter
 */
int DBRM::setSystemState(uint32_t stateFlags) throw()
{
	try 
	{
#ifdef BRM_INFO
		if (fDebug)
		{
			TRACER_WRITELATER("setSystemState");
			TRACER_WRITE;
		}	
#endif

		ByteStream command, response;
		uint8_t err;

		command << SET_SYSTEM_STATE << static_cast<ByteStream::quadbyte>(stateFlags);
		err = send_recv(command, response);
		if (err != ERR_OK) 
		{
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::setSystemState() failed (network)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			stateFlags = 0;
			return -1;
		}

		response >> err;
		if (err != ERR_OK) 
		{
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::setSystemState() failed (got an error)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			stateFlags = 0;
			return -1;
		}
		return 1;
	} 
	catch (...) 
	{
	}
	stateFlags = 0;
	return -1;
}

/* Clear the shm stateflags that are set in the parameter
 */
int DBRM::clearSystemState(uint32_t stateFlags) throw()
{
	try 
	{
#ifdef BRM_INFO
		if (fDebug)
		{
			TRACER_WRITELATER("clearSystemState");
			TRACER_WRITE;
		}	
#endif

		ByteStream command, response;
		uint8_t err;

		command << CLEAR_SYSTEM_STATE << static_cast<ByteStream::quadbyte>(stateFlags);
		err = send_recv(command, response);
		if (err != ERR_OK) 
		{
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::clearSystemState() failed (network)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return -1;
		}

		response >> err;
		if (err != ERR_OK) 
		{
			std::ostringstream oss;
			oss << "DBRM: error: SessionManager::clearSystemState() failed (got an error)";
			log(oss.str(), logging::LOG_TYPE_ERROR);
			return -1;
		}
		return 1;
	} 
	catch (...) 
	{
	}
	return -1;
}

/* Ping the controller node. Don't print anything.
 */
bool DBRM::isDBRMReady() throw()
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("isDBRMReady");
#endif
	boost::mutex::scoped_lock scoped(mutex);
	
	try
	{
		for (int attempt = 0; attempt < 2; ++attempt)
		{
			try 
			{
				if (msgClient == NULL)
				{
					msgClient = new MessageQueueClient(masterName);
				}
				if (msgClient->connect())
				{
					return true;
				}
			}
			catch (...) 
			{
			}
			delete msgClient;
			msgClient = NULL;
			sleep(1);
		}
	}
	catch(...)
	{
	}
	return false;
}

/* This waits for the lock up to 30 sec.  After 30 sec, the assumption is something
 * bad happened, and this will fix the lock state so that primproc can keep
 * running.  These prevent a non-critical problem anyway.
 */
void DBRM::lockLBIDRange(LBID_t start, uint32_t count)
{
	bool locked = false, lockedRange = false;
	LBIDRange range;
	const uint32_t waitInterval = 50000;  // in usec
	const uint32_t maxRetries = 30000000/waitInterval;  // 30 secs
	uint32_t retries = 0;

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

void DBRM::releaseLBIDRange(LBID_t start, uint32_t count)
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

/* OID Manager section */

int DBRM::allocOIDs(int num)
{
#ifdef BRM_INFO
  	if (fDebug) TRACER_WRITENOW("allocOID");
#endif
	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << ALLOC_OIDS;
	command << (uint32_t) num;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: OIDManager::allocOIDs(): network error" << endl;
		log("DBRM: OIDManager::allocOIDs(): network error", logging::LOG_TYPE_CRITICAL);
		return -1;
	}

	try {
		response >> err;
		if (err != ERR_OK)
			return -1;
		response >> ret;
		CHECK_EMPTY(response);
		return (int) ret;
	}
	catch (...) {
		log("DBRM: OIDManager::allocOIDs(): bad response", logging::LOG_TYPE_CRITICAL);
		return -1;
	}
}

void DBRM::returnOIDs(int start, int end)
{
	ByteStream command, response;
	uint8_t err;

	command << RETURN_OIDS;
	command << (uint32_t) start;
	command << (uint32_t) end;
	err = send_recv(command, response);
	if (err == ERR_NETWORK) {
		cerr << "DBRM: OIDManager::returnOIDs(): network error" << endl;
		log("DBRM: OIDManager::returnOIDs(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: OIDManager::returnOIDs(): network error");
	}

	try {
		response >> err;
		CHECK_EMPTY(response);
	}
	catch (...) {
		err = ERR_FAILURE;
	}

	if (err != ERR_OK) {
		log("DBRM: OIDManager::returnOIDs() failed", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: OIDManager::returnOIDs() failed");
	}
}

int DBRM::oidm_size()
{
	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << OIDM_SIZE;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: OIDManager::size(): network error" << endl;
		log("DBRM: OIDManager::size(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: OIDManager::size(): network error");
	}

	try {
		response >> err;
		if (err == ERR_OK) {
			response >> ret;
			CHECK_EMPTY(response);
			return ret;
		}
		CHECK_EMPTY(response);
		return -1;
	}
	catch (...) {
		log("DBRM: OIDManager::size(): bad response", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: OIDManager::size(): bad response");
	}
}

int DBRM::allocVBOID(uint32_t dbroot)
{
	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << ALLOC_VBOID << (uint32_t) dbroot;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: OIDManager::allocVBOID(): network error" << endl;
		log("DBRM: OIDManager::allocVBOID(): network error", logging::LOG_TYPE_CRITICAL);
		return -1;
	}

	try {
		response >> err;
		if (err == ERR_OK) {
			response >> ret;
			CHECK_EMPTY(response);
			return ret;
		}
		CHECK_EMPTY(response);
		return -1;
	}
	catch (...) {
		log("DBRM: OIDManager::allocVBOID(): bad response", logging::LOG_TYPE_CRITICAL);
		return -1;
	}
}

int DBRM::getDBRootOfVBOID(uint32_t vbOID)
{
	ByteStream command, response;
	uint8_t err;
	uint32_t ret;

	command << GETDBROOTOFVBOID << (uint32_t) vbOID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		cerr << "DBRM: OIDManager::getDBRootOfVBOID(): network error" << endl;
		log("DBRM: OIDManager::getDBRootOfVBOID(): network error", logging::LOG_TYPE_CRITICAL);
		return -1;
	}

	try {
		response >> err;
		if (err == ERR_OK) {
			response >> ret;
			CHECK_EMPTY(response);
			return (int) ret;
		}
		CHECK_EMPTY(response);
		return -1;
	}
	catch (...) {
		log("DBRM: OIDManager::getDBRootOfVBOID(): bad response", logging::LOG_TYPE_CRITICAL);
		return -1;
	}
}

vector<uint16_t> DBRM::getVBOIDToDBRootMap()
{
	ByteStream command, response;
	uint8_t err;
	vector<uint16_t> ret;

	command << GETVBOIDTODBROOTMAP;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: OIDManager::getVBOIDToDBRootMap(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: OIDManager::getVBOIDToDBRootMap(): network error");
	}

	try {
		response >> err;
		if (err != ERR_OK) {
			log("DBRM: OIDManager::getVBOIDToDBRootMap(): processing error", logging::LOG_TYPE_CRITICAL);
			throw runtime_error("DBRM: OIDManager::getVBOIDToDBRootMap(): processing error");
		}
		deserializeInlineVector<uint16_t>(response, ret);
		CHECK_EMPTY(response);
		return ret;
	}
	catch (...) {
		log("DBRM: OIDManager::getVBOIDToDBRootMap(): bad response", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: OIDManager::getVBOIDToDBRootMap(): bad response");
	}
}

uint64_t DBRM::getTableLock(const vector<uint32_t> &pmList, uint32_t tableOID,
		string *ownerName, uint32_t *ownerPID, int32_t *ownerSessionID, int32_t *ownerTxnID, LockState state)
{
	ByteStream command, response;
	uint8_t err;
	uint64_t ret;
	TableLockInfo tli;
	uint32_t tmp32;
	vector<uint32_t> dbRootsList;
	OamCache * oamcache = OamCache::makeOamCache();
	OamCache::PMDbrootsMap_t pmDbroots = oamcache->getPMToDbrootsMap();
	int moduleId = 0;
	for (uint32_t i = 0; i < pmList.size(); i++)
	{
		moduleId = pmList[i];
		vector<int> dbroots = (*pmDbroots)[moduleId];
		for (uint32_t j = 0; j < dbroots.size(); j++)
			dbRootsList.push_back((uint32_t)dbroots[j]);
	}
	tli.id = 0;
	tli.ownerName = *ownerName;
	tli.ownerPID = *ownerPID;
	tli.ownerSessionID = *ownerSessionID;
	tli.ownerTxnID = *ownerTxnID;
	tli.dbrootList = dbRootsList;
	tli.state = state;
	tli.tableOID = tableOID;
	tli.creationTime = time(NULL);

	command << GET_TABLE_LOCK << tli;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: getTableLock(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getTableLock(): network error");
	}
	response >> err;
	/* TODO: this means a save failure, need a specific exception type */
	if (err != ERR_OK)
		throw runtime_error("Table lock save file failure");
	response >> ret;
	if (ret == 0) {
		response >> *ownerPID;
		response >> *ownerName;
		response >> tmp32;
		*ownerSessionID = tmp32;
		response >> tmp32;
		*ownerTxnID = tmp32;
	}
	idbassert(response.length() == 0);
	return ret;
}

bool DBRM::releaseTableLock(uint64_t id)
{
	ByteStream command, response;
	uint8_t err;

	command << RELEASE_TABLE_LOCK << id;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: releaseTableLock(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: releaseTableLock(): network error");
	}
	response >> err;
	/* TODO: this means a save failure, need a specific exception type */
	if (err != ERR_OK)
		throw runtime_error("Table lock save file failure");
	response >> err;
	idbassert(response.length() == 0);

	return (bool) err;
}

bool DBRM::changeState(uint64_t id, LockState state)
{
	ByteStream command, response;
	uint8_t err;

	command << CHANGE_TABLE_LOCK_STATE << id << (uint32_t) state;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: changeState(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: changeState(): network error");
	}
	response >> err;
	/* TODO: this means a save failure, need a specific exception type */
	if (err != ERR_OK)
		throw runtime_error("Table lock save file failure");
	response >> err;
	idbassert(response.length() == 0);

	return (bool) err;
}

bool DBRM::changeOwner(uint64_t id, const string &ownerName, uint32_t ownerPID, int32_t ownerSessionID,
		int32_t ownerTxnID)
{
	ByteStream command, response;
	uint8_t err;

	command << CHANGE_TABLE_LOCK_OWNER << id << ownerName << ownerPID <<
			(uint32_t) ownerSessionID << (uint32_t) ownerTxnID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: changeOwner(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: changeOwner(): network error");
	}
	response >> err;
	/* TODO: this means a save failure, need a specific exception type */
	if (err != ERR_OK)
		throw runtime_error("Table lock save file failure");
	response >> err;
	idbassert(response.length() == 0);
	return (bool) err;
}

bool DBRM::checkOwner(uint64_t id)
{
	ByteStream command, response;
	uint8_t err;

	command << OWNER_CHECK << id;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: ownerCheck(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: ownerCheck(): network error");
	}
	response >> err;
	/* TODO: this means a save failure, need a specific exception type */
	if (err != ERR_OK)
		throw runtime_error("Table lock save file failure");
	response >> err;
	idbassert(response.length() == 0);
	return (bool) err;  // Return true means the owner is valid
}

vector<TableLockInfo> DBRM::getAllTableLocks()
{
	ByteStream command, response;
	uint8_t err;
	vector<TableLockInfo> ret;

	command << GET_ALL_TABLE_LOCKS;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: getAllTableLocks(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getAllTableLocks(): network error");
	}
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: getAllTableLocks(): processing error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getAllTableLocks(): processing error");
	}
	deserializeVector<TableLockInfo>(response, ret);
	idbassert(response.length() == 0);
	return ret;
}

void DBRM::releaseAllTableLocks()
{
	ByteStream command, response;
	uint8_t err;

	command << RELEASE_ALL_TABLE_LOCKS;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: releaseAllTableLocks(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: releaseAllTableLocks(): network error");
	}
	response >> err;
	idbassert(response.length() == 0);
	if (err != ERR_OK)
		throw runtime_error("DBRM: releaseAllTableLocks(): processing error");
}

bool DBRM::getTableLockInfo(uint64_t id, TableLockInfo *tli)
{
	ByteStream command, response;
	uint8_t err;

	command << GET_TABLE_LOCK_INFO << id;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: getTableLockInfo(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getTableLockInfo(): network error");
	}
	response >> err;
	if (err != ERR_OK)
		throw runtime_error("DBRM: getTableLockInfo() processing error");
	response >> err;
	if (err)
		response >> *tli;
	return (bool) err;
}

void DBRM::startAISequence(uint32_t OID, uint64_t firstNum, uint32_t colWidth,
                           execplan::CalpontSystemCatalog::ColDataType colDataType)
{
	ByteStream command, response;
	uint8_t err;
    uint8_t tmp8 = colDataType;

	command << START_AI_SEQUENCE << OID << firstNum << colWidth << tmp8;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: startAISequence(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: startAISequence(): network error");
	}
	response >> err;
	idbassert(response.length() == 0);
	if (err != ERR_OK) {
		log("DBRM: startAISequence(): processing error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: startAISequence(): processing error");
	}
}

bool DBRM::getAIRange(uint32_t OID, uint32_t count, uint64_t *firstNum)
{
	ByteStream command, response;
	uint8_t err;

	command << GET_AI_RANGE << OID << count;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: getAIRange(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getAIRange(): network error");
	}
	response >> err;
	if (err != ERR_OK) {
		log("DBRM: getAIRange(): processing error",	logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getAIRange(): processing error");
	}
	response >> err;
	if (err == 0)
		return false;
	response >> *firstNum;
	idbassert(response.length() == 0);
	return true;
}

bool DBRM::getAIValue(uint32_t OID, uint64_t *value)
{
	return getAIRange(OID, 0, value);
}

void DBRM::resetAISequence(uint32_t OID, uint64_t value)
{
	ByteStream command, response;
	uint8_t err;

	command << RESET_AI_SEQUENCE << OID << value;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: resetAISequence(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: resetAISequence(): network error");
	}
	response >> err;
	idbassert(response.length() == 0);
	if (err != ERR_OK) {
		log("DBRM: resetAISequence(): processing error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: resetAISequence(): processing error");
	}
}

void DBRM::getAILock(uint32_t OID)
{
	ByteStream command, response;
	uint8_t err;

	command << GET_AI_LOCK << OID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: getAILock(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getAILock(): network error");
	}
	response >> err;
	idbassert(response.length() == 0);
	if (err != ERR_OK) {
		log("DBRM: getAILock(): processing error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: getAILock(): processing error");
	}
}

void DBRM::releaseAILock(uint32_t OID)
{
	ByteStream command, response;
	uint8_t err;

	command << RELEASE_AI_LOCK << OID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM: releaseAILock(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: releaseAILock(): network error");
	}
	response >> err;
	idbassert(response.length() == 0);
	if (err != ERR_OK) {
		log("DBRM: releaseAILock(): processing error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: releaseAILock(): processing error");
	}
}

void DBRM::deleteAISequence(uint32_t OID)
{
	ByteStream command, response;
	uint8_t err;

	command << DELETE_AI_SEQUENCE << OID;
	err = send_recv(command, response);
	if (err != ERR_OK) {
		log("DBRM:deleteAILock(): network error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: deleteAILock(): network error");
	}
	response >> err;
	idbassert(response.length() == 0);
	if (err != ERR_OK) {
		log("DBRM: deleteAILock(): processing error", logging::LOG_TYPE_CRITICAL);
		throw runtime_error("DBRM: deleteAILock(): processing error");
	}
}

void DBRM::invalidateUncommittedExtentLBIDs(execplan::CalpontSystemCatalog::SCN txnid, vector<LBID_t>* plbidList)
{
    // Here we want to minimize the number of calls to dbrm
    // Given that, and the fact that we need to know the column type
    // in order to set the invalid min and max correctly in the extents,
    // We do the following:
    // 1) Maintain a vector of all extents we've looked at.
    // 2) Get the list of uncommitted lbids for the transaction.
    // 3) Look in that list to see if we've already looked at this extent.
    // 4) If not, 
    //    a) lookup the min and max lbid for the extent it belongs to
    //    b) lookup the column oid for that lbid
    //    c) add to the vector of extents
    // 5) Create a list of CPInfo structures with the first lbid and col type of each extent
    // 6) Lookup the column type for each retrieved oid.
    // 7) mark each extent invalid, just like we would during update. This sets the proper
    //    min and max (and set the state to CP_UPDATING.
    // 6) Call setExtentsMaxMin to set the state to CP_INVALID.

	vector<LBID_t> localLBIDList;

    boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
    CPInfoList_t cpInfos;
    CPInfo aInfo;
    int oid;
    uint16_t dbRoot;
    uint32_t partitionNum;
    uint16_t segmentNum;
    uint32_t fileBlockOffset;

    // 2) Get the list of uncommitted lbids for the transaction, if we weren't given one.
    if (plbidList == NULL)
    {
        getUncommittedExtentLBIDs(static_cast<VER_t>(txnid), localLBIDList);
        plbidList = &localLBIDList;
    }
    if (plbidList->size() ==0)
    {
        return; // Nothing to do.
    }
	vector<LBID_t>::const_iterator iter = plbidList->begin();
	vector<LBID_t>::const_iterator end = plbidList->end();
    csc = execplan::CalpontSystemCatalog::makeCalpontSystemCatalog();

    for (; iter != end; ++iter)
    {
        LBID_t lbid = *iter;
        aInfo.firstLbid = lbid;
        // lookup the column oid for that lbid (all we care about is oid here)
        if (em->lookupLocal(lbid, oid, dbRoot, partitionNum, segmentNum, fileBlockOffset) == 0)
        {
            if (execplan::isUnsigned(csc->colType(oid).colDataType))
            {
                aInfo.max = 0;
                aInfo.min = numeric_limits<uint64_t>::max();
            }
            else
            {
                aInfo.max = numeric_limits<int64_t>::min();
                aInfo.min = numeric_limits<int64_t>::max();
            }
        }
        else
        {
            // We have a problem, but we need to put something in. This should never happen.
            aInfo.max = numeric_limits<int64_t>::min();
            aInfo.min = numeric_limits<int64_t>::max();
        }
        aInfo.seqNum = -2;
        cpInfos.push_back(aInfo);
    }

    // Call setExtentsMaxMin to invalidate and set the proper max/min in each extent
    setExtentsMaxMin(cpInfos);
}

}   //namespace
