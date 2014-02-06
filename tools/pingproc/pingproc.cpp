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

// $Id: pingproc.cpp 2101 2013-01-21 14:12:52Z rdempsey $
#include <iostream>
#include <iomanip>
#include <sys/types.h>
#include <unistd.h>
#include <getopt.h>
#include <sstream>
#include <vector>
using namespace std;

#include <boost/thread.hpp>
using namespace boost;

#include "bytestream.h"
using namespace messageqcpp;

#include "distributedenginecomm.h"
#include "primitivemsg.h"
#include "jobstep.h"
#include "batchprimitiveprocessor-jl.h"
using namespace joblist;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "brm.h"
using namespace BRM;

// Global vars
bool debug;
bool thdFcnFailure;

//
// TODO: Why is this namespace here?

namespace
{

void timespec_sub(const struct timespec &tv1,
		  const struct timespec &tv2,
		  struct timespec &diff)
{
	if (tv2.tv_nsec < tv1.tv_nsec) {
	    diff.tv_sec = tv2.tv_sec - tv1.tv_sec - 1;
	    diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec + 1000000000;
	}
	else {
	    diff.tv_sec = tv2.tv_sec - tv1.tv_sec;
	    diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
	}
} // timespec_sub

//
//
class OidOperation {

public:

	enum OpType_t {
		SCAN=0,
		BLOCK=1,
		LOOPBACK=2,
		NONE=3,
		BATCHSCAN = 4,
		BATCHSTEP = 5,
		BATCHFILT = 6
	};

	OidOperation(const OID_t oid, const OpType_t opType, const uint32_t sessionId=0);
	~OidOperation() {};

	void addFilter(const int8_t COP, const int64_t value);

	const OID_t OID() const {return fOid;}
	const OpType_t OpType() const {return fOpType;}

	const CalpontSystemCatalog::ColType& ColumnType() { return fColType;}
	const uint32_t ColumnWidth() const { return fColType.colWidth;}

	const uint32_t FilterCount() const { return fFilterCount;}
	const ByteStream& FilterString() { return fFilterList;}
	const uint32_t SessionId() const { return fSessionId;}
	const uint32_t DataType() const { return fColType.colDataType;}
	const uint32_t BOP() const { return fBOP;}
	void BOP(const uint32_t bop) { if (FilterCount()>=2) fBOP=bop;}
	const uint32_t COP1() const { return fCOP1;}
	void COP1(const uint32_t cop) { fCOP1=cop;}
	const uint32_t COP2() const { return fCOP2;}
	void COP2(const uint32_t cop) { fCOP2=cop;}
	bool isIntegralDataType();
	void setLbidTraceOn();
	void setPMProfileOn();
	bool LbidTrace() {return fLbidTrace;}
	bool PMProfile() {return fPMProfile;}

	void deSerializeFilter(int8_t& COP, int64_t& value);

// private:
	OidOperation(){};
	OID_t fOid;

	ByteStream fFilterList;
	uint32_t fFilterCount;
	OpType_t fOpType;
	CalpontSystemCatalog::ColType fColType;
	uint32_t fSessionId;
	uint32_t fBOP;
	uint32_t fCOP1;
	uint32_t fCOP2;
	bool fLbidTrace;
	bool fPMProfile;
}; // class OidOperation


//
//
OidOperation::OidOperation(const OID_t oid, const OpType_t opType, const uint32_t sessionId)
	:fOid(oid), fFilterList(), fFilterCount(0), fOpType(opType),fSessionId(sessionId), fBOP(BOP_NONE), fCOP1(COMPARE_NIL),fCOP2(COMPARE_NIL), fLbidTrace(false), fPMProfile(false)
{
 	boost::shared_ptr<CalpontSystemCatalog> cat = execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(getpid());
 	fColType = cat->colType(oid);
	fFilterList.reset();
}

void OidOperation::setLbidTraceOn()
{
	fLbidTrace=true;
}

void OidOperation::setPMProfileOn()
{
	fPMProfile=true;
}

void OidOperation::addFilter(const int8_t COP, const int64_t value)
{

	if (fFilterCount==2)
		return;

	fFilterList << (uint8_t) COP;
	// converts to a type of the appropriate width, then bitwise
	// copies into the filter ByteStream
	switch(ColumnWidth()) {
		case 1:
			int8_t tmp8;
			tmp8 = value;
			fFilterList << *((uint8_t *) &tmp8);
			break;
		case 2:
			int16_t tmp16;
			tmp16 = value;
			fFilterList << *((uint16_t *) &tmp16);
			break;
		case 4:
			int32_t tmp32;
			tmp32 = value;
			fFilterList << *((uint32_t *) &tmp32);
			break;
		case 8:
			fFilterList << *((uint64_t *) &value);
			break;
		default:
			ostringstream o;

			o << "addFilter: colType says OID " <<
				" has a width of " << ColumnWidth();
			throw runtime_error(o.str());
	}
	fFilterCount++;
}


void OidOperation::deSerializeFilter(int8_t& COP, int64_t& value)
{

	if (fFilterCount==0)
	{
		COP = COMPARE_NIL;
		return;
	}

	fFilterList >> *(uint8_t*) &COP;

	switch(ColumnWidth()) {
		case 1:
			int8_t tmp8;
			tmp8 = value;
			fFilterList >> *((uint8_t *) &tmp8);
			value = tmp8;
			break;
		case 2:
			int16_t tmp16;
			fFilterList >> *((uint16_t *) &tmp16);
			value = tmp16;
			break;
		case 4:
			int32_t tmp32;
			fFilterList >> *((uint32_t *) &tmp32);
			value = tmp32;
			break;
		case 8:
			fFilterList >> *((uint64_t *) &value);
			break;
		default:
			ostringstream o;

			o << "deSerializeFilter: colType says OID " <<
				" has a width of " << ColumnWidth();
			throw runtime_error(o.str());
	}
	fFilterCount--;
}

typedef vector<OidOperation*> OperationList;

// Only process these column types
//
bool OidOperation::isIntegralDataType()
{
    if (DataType()==CalpontSystemCatalog::BIT ||
    	DataType()==CalpontSystemCatalog::TINYINT ||
    	DataType()==CalpontSystemCatalog::SMALLINT ||
    	DataType()==CalpontSystemCatalog::MEDINT ||
    	DataType()==CalpontSystemCatalog::INT ||
    	DataType()==CalpontSystemCatalog::DATE ||
    	DataType()==CalpontSystemCatalog::BIGINT ||
    	DataType()==CalpontSystemCatalog::DATETIME ||
        DataType()==CalpontSystemCatalog::UTINYINT ||
        DataType()==CalpontSystemCatalog::USMALLINT ||
        DataType()==CalpontSystemCatalog::UMEDINT ||
        DataType()==CalpontSystemCatalog::UINT ||
        DataType()==CalpontSystemCatalog::UBIGINT)
	return true;

	if (DataType()==CalpontSystemCatalog::CHAR && 1 == fColType.colWidth)
		return true;

	return false;
}

const ByteStream formatLoopBackMsg(const uint32_t sessionId, uint32_t uniqueId)
{
	ByteStream primMsg;
	ISMPacketHeader ism;
	memset(&ism, 0, sizeof(ism));
	ism.Command=COL_LOOPBACK;
	ism.Size=sizeof(ism) + sizeof(ColLoopback);
	ism.Type=2;
	primMsg.load((const uint8_t *) &ism, sizeof(ism));
	struct ColLoopback lb;

	memset(&lb, 0, sizeof(lb));

	lb.Hdr.SessionID = sessionId;
	lb.Hdr.StatementID = 0;
	lb.Hdr.TransactionID = sessionId;
	lb.Hdr.VerID = 0;
	lb.Hdr.StepID = sessionId;
	lb.Hdr.UniqueID = uniqueId;
	primMsg.append((const uint8_t *) &lb, sizeof(lb));

	return primMsg;
} // formatLoopBackMsg

const ByteStream formatDictionaryMsg(
								const uint64_t lbid,
								ByteStream& ridList,
								const uint16_t ridCount,
								OidOperation& oidOp)
{
	ByteStream primMsg;
	DictSignatureRequestHeader hdr;
    ISMPacketHeader ism;

    ism.Flags=0; //planFlagsToPrimFlags(fTraceFlags);
    ism.Command=DICT_SIGNATURE;
    ism.Size=sizeof(DictSignatureRequestHeader) + ridList.length();
    ism.Type=2;

    hdr.Hdr.SessionID = oidOp.SessionId();
    hdr.Hdr.StatementID = 0;
    hdr.Hdr.TransactionID = oidOp.SessionId();
    hdr.Hdr.VerID = 0;
    hdr.Hdr.StepID = 0;

    hdr.LBID = lbid;
    hdr.PBID = 0;
    idbassert(ridCount <= 8000);
    hdr.NVALS = ridCount;

    primMsg.load((const uint8_t *) &ism, sizeof(ism));
    primMsg.append((const uint8_t *) &hdr, sizeof(DictSignatureRequestHeader));
    primMsg += ridList;

	return primMsg;
}


const ByteStream formatColStepMsg(
				const uint64_t lbid,
				ByteStream& ridList,
				const uint16_t ridCount,
				OidOperation& oidOp,
				uint32_t uniqueId)
{
	ByteStream primMsg;
	NewColRequestHeader hdr;

	memset(&hdr, 0, sizeof(hdr));

	hdr.ism.Reserve=0;
	hdr.ism.Flags=0;
	if (oidOp.LbidTrace()==true)
		hdr.ism.Flags |= PF_LBID_TRACE;
	if (oidOp.PMProfile()==true)
		hdr.ism.Flags |= PF_PM_PROF;
	hdr.ism.Command=COL_BY_SCAN;
	hdr.ism.Size=sizeof(NewColRequestHeader) + oidOp.FilterString().length() + ridList.length();
	hdr.ism.Type=2;

	hdr.hdr.SessionID = oidOp.SessionId();
	hdr.hdr.StatementID = 0;
	hdr.hdr.TransactionID = oidOp.SessionId();
	hdr.hdr.VerID = 0;
	hdr.hdr.StepID = oidOp.SessionId();
	hdr.hdr.UniqueID = uniqueId;

	hdr.LBID = lbid;
	idbassert(hdr.LBID > 0);
	hdr.PBID = 0;
	hdr.DataSize = oidOp.ColumnWidth();
	hdr.DataType = oidOp.DataType();
	hdr.OutputType = OT_BOTH;
	hdr.BOP = BOP_NONE;
//	hdr.InputFlags = 0;
	hdr.NOPS = oidOp.FilterCount();
	hdr.NVALS = ridCount;
	hdr.sort=0;

	primMsg.load((const uint8_t *) &hdr, sizeof(NewColRequestHeader));

	if (oidOp.FilterCount()>0)
		primMsg += oidOp.FilterString();

	if (ridCount>0)
		primMsg += ridList;

	return primMsg;

} //formatColStepMsg

const ByteStream formatDictionaryScanMsg(const uint64_t lbid,
								const uint16_t count,
								OidOperation& oidOp)
{
	ByteStream primMsg;
    DictTokenByScanRequestHeader hdr;

    hdr.ism.Reserve       = 0;
    hdr.ism.Flags         = 0;
    hdr.ism.Command       = DICT_TOKEN_BY_SCAN_COMPARE;
    hdr.ism.Size          = sizeof(DictTokenByScanRequestHeader) +
                            						oidOp.FilterString().length();
    hdr.ism.Type          = 2;

    hdr.Hdr.SessionID     = oidOp.SessionId();
    hdr.Hdr.StatementID   = 0;
    hdr.Hdr.TransactionID = 0;
    hdr.Hdr.VerID         = 0;
    hdr.Hdr.StepID        = oidOp.SessionId();

    hdr.LBID			= lbid;
    idbassert(hdr.LBID >= 0);
    hdr.PBID              = 0;
    hdr.OutputType        = OT_TOKEN;
    hdr.BOP               = oidOp.BOP();
    hdr.COP1              = oidOp.COP1();
    hdr.COP2              = oidOp.COP2();
    hdr.NVALS             = oidOp.FilterCount();
    hdr.Count             = count;
    idbassert(hdr.Count > 0);

    primMsg.load((const uint8_t *) &hdr.ism, sizeof(ISMPacketHeader));
    primMsg.append((const uint8_t*) &hdr, sizeof(DictTokenByScanRequestHeader));
    primMsg += oidOp.FilterString();

	return primMsg;
}

const ByteStream formatColScanMsg(const uint64_t lbid,
				const uint16_t count,
				OidOperation& oidOp,
				uint32_t uniqueId)
{
	ByteStream primMsg;
	ISMPacketHeader ism;
	ColByScanRangeRequestHeader fMsgHeader;

	memset(&fMsgHeader, 0, sizeof(fMsgHeader));
	memset(&ism, 0, sizeof(ism));

	ism.Reserve=0;
	ism.Flags=0;
	if (oidOp.LbidTrace()==true)
		ism.Flags |= PF_LBID_TRACE;
	if (oidOp.PMProfile()==true)
		ism.Flags |= PF_PM_PROF;
	ism.Command=COL_BY_SCAN_RANGE;
	ism.Size=sizeof(fMsgHeader) + sizeof(ism) + oidOp.FilterString().length();
	ism.Type=2;

	primMsg.load((const uint8_t *) &ism, sizeof(ism));

  	fMsgHeader.LBID = lbid;
	idbassert(fMsgHeader.LBID >= 0);
  	fMsgHeader.PBID = 0;
  	fMsgHeader.DataSize = oidOp.ColumnWidth();
  	fMsgHeader.DataType = oidOp.DataType();
  	fMsgHeader.OutputType = OT_BOTH;
  	fMsgHeader.BOP = oidOp.BOP();
  	fMsgHeader.NOPS = oidOp.FilterCount();
  	fMsgHeader.NVALS = 0;
  	fMsgHeader.Count = count;	//(hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
	idbassert(fMsgHeader.Count > 0);
  	fMsgHeader.Hdr.SessionID = oidOp.SessionId();
  	fMsgHeader.Hdr.StatementID = 0;
  	fMsgHeader.Hdr.TransactionID = oidOp.SessionId();
  	fMsgHeader.Hdr.VerID = 0;
  	fMsgHeader.Hdr.StepID = oidOp.SessionId();
	fMsgHeader.Hdr.UniqueID = uniqueId;

	primMsg.append((const uint8_t *) &fMsgHeader, sizeof(fMsgHeader));

	if (oidOp.FilterCount()>0)
		primMsg+=oidOp.FilterString();

	return primMsg;

} //formatColScanMsg


//
void doBatchOp_scan(OidOperation& OidOp);
void doBatchOp_step(OidOperation& OidOp);
void doBatchOp_filt(OidOperation& OidOp);
void doBatchQueryOp(OperationList& OidOps);
void doColScan(OidOperation& OidOp);
void doColStep(OidOperation& OidOp);
void doDictScan(OidOperation& OidOp);
void doDictStep(OidOperation& OidOp);

// receive the responses from PrimProc
//
struct ThdFcn
{
	void operator()()
	{
		uint64_t totalBytes=0;
		try
		{
			ByteStream ibs;
			if (debug)
				cout << "Waiting on " << fNumMsgs << " messages." << endl;
			for (uint32_t k=0; k<fNumMsgs; k++)
			{
// 				cout << "reading msg #" << k << "...\n";
				ibs = fDec->read(uniqueID);
// 				cout << "got msg #" << k << endl;
				if (debug)
					if (k%10240==0)
					cout
						<< "ThdFcn: read " << fSessionid
						<< " " << k << "/" << fNumMsgs << " "
						<< ibs.length() << "/" << totalBytes
						<< endl;

				if (ibs.length() == 0)
					break;

				totalBytes+=ibs.length();
			}
		}
		catch (exception &e)
		{
			cerr << "read exception: " << e.what() << endl;
			thdFcnFailure = true;
		}

		if (debug)
			cout << totalBytes << " bytes read in "
				<< fNumMsgs << " messages" << endl;

	} // void operator()

	uint32_t fSessionid;
	uint32_t uniqueID;
	DistributedEngineComm* fDec;
	unsigned fNumMsgs;

}; // struct ThdFcn


struct QryThdFcn
{
	void operator()()
	{
		uint64_t totalBytes=0;
		int64_t min;
		int64_t max;
		uint64_t lbid;
		uint32_t cachedIO;
		uint32_t physIO;
		uint32_t touchedBlocks;
		bool validCPData;
		try
		{
			ByteStream ibs;
			ByteStream obs;
			if (debug)
				cout << "Waiting on " << fNumMsgs << " messages." << endl;
			for (uint32_t k=0; k<fNumMsgs; k++)
			{
// 				cout << "reading msg #" << k << "...\n";
				ibs = fDec->read(uniqueID);
// 				cout << "got msg #" << k << endl;
				if (debug)
					if (k%10240==0)
					cout
						<< "QryThdFcn: read " << fSessionid
						<< " " << k << "/" << fNumMsgs << " "
						<< ibs.length() << "/" << totalBytes
						<< " rows: " << fRows << endl;

				if (ibs.length() == 0)
					break;

				totalBytes+=ibs.length();
				fRows += fBpp.getTableBand (ibs, &obs, &validCPData, &lbid, &min, &max, &cachedIO, &physIO, &touchedBlocks );
				fBlockTouched += touchedBlocks;
			}
		}
		catch (exception &e)
		{
			cerr << "read exception: " << e.what() << endl;
			thdFcnFailure = true;
		}

		if (debug)
			cout << totalBytes << " bytes read in "
				<< fNumMsgs << " messages for "
				<< fRows << " rows and " << fBlockTouched << " blocks\n";

	} // void operator()

	QryThdFcn(BatchPrimitiveProcessorJL& bpp, uint64_t& rows, uint32_t& blk): fBpp(bpp), fNumMsgs(0), fRows(rows), fBlockTouched(blk) {}
	BatchPrimitiveProcessorJL& fBpp;
	uint32_t fSessionid;
	DistributedEngineComm* fDec;
	unsigned fNumMsgs;
	uint64_t& fRows;
	uint32_t& fBlockTouched;
	uint32_t uniqueID;

}; // struct ThdFcn


struct BatchScanThr
{
	BatchScanThr(OidOperation& oidOp):fOidOp(oidOp){}

	void operator()()
	{
		doBatchOp_scan(fOidOp);
	} // void operator()

	OidOperation& fOidOp;

}; // struct BatchScanThr

struct BatchStepThr
{
	BatchStepThr(OidOperation& oidOp):fOidOp(oidOp){}

	void operator()()
	{
		doBatchOp_step(fOidOp);
	} // void operator()

	OidOperation& fOidOp;

}; // struct BatchStepThr

struct BatchFiltThr
{
	BatchFiltThr(OidOperation& oidOp):fOidOp(oidOp){}

	void operator()()
	{
		doBatchOp_filt(fOidOp);
	} // void operator()

	OidOperation& fOidOp;

}; // struct BatchFiltThr

struct BatchQueryThr
{
	BatchQueryThr(OperationList& oidOps):fOidOps(oidOps){}

	void operator()()
	{
		doBatchQueryOp(fOidOps);
	} // void operator()

	OperationList& fOidOps;

}; // struct


struct ColStepThr
{
	ColStepThr(OidOperation& oidOp):fOidOp(oidOp){}

	void operator()()
	{
		doColStep(fOidOp);
	} // void operator()

	OidOperation& fOidOp;

}; // struct ColStepThr

struct ColScanThr
{

	ColScanThr(OidOperation& oidOp):fOidOp(oidOp){}

	void operator()()
	{
		doColScan(fOidOp);
	} // void operator()

	OidOperation& fOidOp;

}; // struct ColStepThr

struct DictSigThr
{
}; // DictSigThr

struct DictScanThr
{
}; // DictScanThr

// doColScan
void doDictionaryScan(OidOperation& OidOp) {
}


// doColScan
void doColScan(OidOperation& OidOp) {

if (debug) cout << "beginning doColScan\n";
	BRM::LBIDRange_v lbidRanges;
	HWM_t hwm=0;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
	struct timespec ts1;
	struct timespec ts2;
	struct timespec diff;
	uint32_t uniqueID;

	uint32_t sessionid = getpid();
	uint32_t totalBlks=0;
// 	dec->addSession(sessionid);
// 	dec->addStep(sessionid, sessionid);
	DBRM dbrm;

	uniqueID = dbrm.getUnique32();
	dec->addQueue(uniqueID);

	int err = dbrm.lookup(OidOp.OID(), lbidRanges);
	if (err)
		throw runtime_error("doAColScan: BRM LBID range lookup failure (1)");

	err = dbrm.getHWM(OidOp.OID(), hwm);
	if (err)
		throw runtime_error("doAColScan: BRM HWM lookup failure (3)");

  	LBIDRange_v::iterator it;
  	OID_t tmp;
  	uint32_t fbo;

	ThdFcn f1;
	f1.fSessionid = sessionid;
	f1.uniqueID = uniqueID;
	f1.fDec = dec;
	f1.fNumMsgs=0;
	thdFcnFailure = false;
	uint32_t rangeSize=0;
	ByteStream obs;

	// calculate the expected number of messages
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {
 
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			cerr << "pColScanStep::sendPrimitiveMessages: dbrm.lookup failed for lbid " << lbid << endl;
			abort();
		}

		if (hwm < fbo)
			continue;

		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		totalBlks+=rangeSize;
	} // for

	f1.fNumMsgs=(totalBlks/OidOp.ColumnWidth());
	if (0 < totalBlks % OidOp.ColumnWidth()) ++f1.fNumMsgs;

	idbassert(f1.fNumMsgs);
	if (debug)
		cout
			<< "Scanning OID " << OidOp.OID()
			<< " " << f1.fNumMsgs << " msgs" << endl;

	thread t1(f1);
	clock_gettime(CLOCK_REALTIME, &ts1);
	// send the primitive requests
	int rCount=0;
	totalBlks=0;
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {

		try {

 			BRM::LBID_t lbid = (*it).start;
			if (dbrm.lookup(lbid, 0, false, tmp, fbo))
			{
				cerr << "doAColScan dbrm.lookup failed for lbid " << lbid << endl;
				abort();
			}

			if (hwm < fbo)
				continue;

			rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
  			obs = formatColScanMsg(lbid, rangeSize, OidOp, uniqueID);
			if (obs.length() > 0) {
  				dec->write(obs);
				rCount++;
				if (debug)
					cout << "colScan: " << rCount << "/"
						<< lbidRanges.size() << " sending " << obs.length() << " bytes "
						<< " lbid " << lbid << " sz " << rangeSize << endl;
			}
		}
		catch(exception& e)
			{
				cerr << "catch " << e.what() << endl;
			}
			totalBlks+=rangeSize;

  	} // for (lbidRanges ...

	t1.join();
	clock_gettime(CLOCK_REALTIME, &ts2);
// 	dec->removeSession(sessionid);
	dec->removeQueue(uniqueID);

	timespec_sub(ts1, ts2, diff);

	cout << "ColScan stats OID: " << OidOp.OID()
		<< "\tFilter: " << (int) OidOp.FilterCount()
		<< "\tBlocks: " << (int) totalBlks
		<< "\tElapse: " << diff.tv_sec+(diff.tv_nsec/1000000000.0) << "s";

	float rate = 0;
	rate =totalBlks/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << " Blks/sec : " << rate << endl;

	if (thdFcnFailure)
		cout << "There was a failure in the read thread." << endl;
	cout << endl;

} // doAScan

//
void usage()
{
	cerr <<
		"PingProc operation [filter] [operation] [filter] [reporting]" << endl <<
		"PingProc -c -s <oid> -gt 0 -t <oid> -d " << endl <<
		"\t---- operation flags ----" << endl <<
		"\t--scan -s <scan the oid>" << endl <<
		"\t--block -t <colstep the oid>" << endl <<
		"\t--BatchPrimitiveScan -B <batch scan the oid>" << endl <<
		"\t--BatchPrimitiveStep -Z <batch step the oid>" << endl <<
		"\t--concurrent -c perform each operation in its own thread" << endl <<
		"\t--lbid-trace -lb set lbid trace flag in request" << endl <<
		"\t---- filter flags ----" << endl <<
		"\t--equal -eq <int> equivalency test of values in a block>" << endl <<
		"\t--greater-than -gt  <int> - greater than (>) test of values in a block" << endl <<
		"\t--greater-than-equal -ge <int> greater than or equal to (>=) test of values in a block" << endl <<
		"\t--less-than -lt <int> less than (<) test of values in a block" << endl <<
		"\t--less-than-equal -le <int> less than or equal to (<=) test of values in a block" << endl <<
		"\t--not-equal -ne <int> not equal to (!=) test of values in a block" << endl <<
		"\t--bop <1 or 0> binary operator when 2 comparison filters are present" << endl <<
		"\t---- reporting flags ----" << endl <<
		"\t--debug -d turn debug output on>" << endl <<
		"\t--list <oid> -list <oid> -l <oid> print out all oids and their ranges" << endl <<
		"\t--loopback <count=100000> -p <count> send count loopback requests" << endl <<
		"\t--query -q run batch query for all the oids (enter with -B or -Z)" << endl;

} // usage()

const int64_t getInt(string s) {

	if (s.length()<=0)
		return -1;

	//if (atoll(s.data()) < 0)
	//	return -1;

	return atoll(s.data());

} //getInt

// dictionary
 void doDictionarySig(OidOperation& OidOp) {
} // doDictionarySig

// col step
void doColStep(OidOperation& OidOp) {

	struct timespec ts1;
	struct timespec ts2;
	struct timespec diff;
	DBRM dbrm;
	BRM::LBIDRange_v lbidRanges;
	HWM_t hwm=0;
  	LBIDRange_v::iterator it;
  	OID_t tmp;
  	uint32_t fbo;
  	uint32_t totalBlks=0;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
	ThdFcn f1;

// 	dec->addSession(OidOp.SessionId());
// 	dec->addStep(OidOp.SessionId(), OidOp.SessionId());
	uint32_t uniqueID = dbrm.getUnique32();
	dec->addQueue(uniqueID);

	f1.fSessionid = OidOp.SessionId();
	f1.uniqueID = uniqueID;
	f1.fDec = dec;
	f1.fNumMsgs = 0;
	thdFcnFailure = false;
	ByteStream ridlist;
	uint16_t ridCount = 0; // BLOCK_SIZE/OidOp.ColumnWidth();

	for (uint16_t i=0; i<ridCount; i++)
		ridlist << i;

	int err = dbrm.lookup(OidOp.OID(), lbidRanges);
	if (err)
		throw runtime_error("doAColStep: BRM LBID range lookup failure (1)");

	err = dbrm.getHWM(OidOp.OID(), hwm);
	if (err)
		throw runtime_error("doAColStep: BRM HWM lookup failure (3)");

	uint32_t rangeSize=0;
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			cerr << "pColStep::sendPrimitiveMessages: dbrm.lookup failed for lbid " << lbid << endl;
			abort();
		}
		if (hwm < fbo)
			break; //continue;

		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		totalBlks+=rangeSize;
	} // for

	uint32_t colwidth = OidOp.ColumnWidth();
	f1.fNumMsgs = totalBlks /(colwidth);
	if (0 < totalBlks % colwidth) ++f1.fNumMsgs;

	idbassert(f1.fNumMsgs);
	thread t1(f1);
	ByteStream obs;
	totalBlks=0;
	clock_gettime(CLOCK_REALTIME, &ts1);
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			if (debug)
				cerr << "pColScanStep::sendPrimitiveMessages: dbrm.lookup failed for lbid " << lbid << endl;
			abort();
		}

		if (hwm < fbo)
			break; //continue;

		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		for (unsigned i = 0; i < rangeSize; i++)
		{
			obs += formatColStepMsg(lbid+i, ridlist, ridCount, OidOp, uniqueID);
			if (0 == (i + 1) % colwidth)
			{
				dec->write(obs);
				if (debug && i+1 ==rangeSize)
					cout	<< "colStep: " << i << "/"
						<< rangeSize << " " << obs.length()
						<< " lbid " << lbid + i << endl;
				obs.restart();
			}
		}
		totalBlks+=rangeSize;
	} // for

	if (obs.length())
	{
		dec->write(obs);
		if (debug)
			cout	<< "colStep: last" <<  "/"
				<< rangeSize << " " << obs.length() << endl;
	}
	obs.reset();

	t1.join();  //@bug 849 moved join here and changed output to be like pColScan.
	clock_gettime(CLOCK_REALTIME, &ts2);
	timespec_sub(ts1, ts2, diff);
// 	t1.join();
	cout << "ColStep stats OID: " << OidOp.OID()
		<< "\tFilter: " << (int) OidOp.FilterCount()
		<< "\tBlocks: " << (int) totalBlks
		<< "\tElapse: " << diff.tv_sec+(diff.tv_nsec/1000000000.0) << "s";

	float rate = 0;
	rate = totalBlks/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\tBlks/sec " << rate << endl;

	if (thdFcnFailure)
		cerr << "There was a failure in the read thread." << endl;

	cout << endl;

} // doColStep

void doBatchOp_scan(OidOperation &OidOp)
{
	struct timespec ts1, ts2, diff;
	JobStepAssociation injs, outjs;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
	ThdFcn f1;
	boost::shared_ptr<CalpontSystemCatalog> sysCat = execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(getpid());

	pColScanStep scan(injs, outjs, dec, sysCat, OidOp.fOid, OidOp.fOid,
			  OidOp.fSessionId, 0, OidOp.fSessionId, OidOp.fSessionId, OidOp.fSessionId, rm);

	int32_t filters = OidOp.FilterCount();
	while (OidOp.FilterCount()>0)
	{
		int8_t cop;
		int64_t  value;
		OidOp.deSerializeFilter(cop, value);
		scan.addFilter(cop, value);
	}

	BatchPrimitiveProcessorJL bpp;
	ByteStream bs;
	DBRM dbrm;
	BRM::LBIDRange_v lbidRanges;
	HWM_t hwm=0;
  	LBIDRange_v::iterator it;
  	OID_t tmp;
  	uint32_t fbo;

	uint32_t uniqueID = dbrm.getUnique32();
	bpp.setUniqueID(uniqueID);

	bpp.setSessionID(OidOp.SessionId());
	bpp.setStepID(OidOp.SessionId());
	bpp.addFilterStep(scan);

	cout << "session number = " << OidOp.SessionId() << endl;
// 	dec->addSession(OidOp.SessionId());
// 	dec->addStep(OidOp.SessionId(), OidOp.SessionId());
	dec->addQueue(uniqueID);
	f1.fSessionid = OidOp.SessionId();
	f1.uniqueID = uniqueID;
	f1.fDec = dec;
	thdFcnFailure = false;
	int err = dbrm.lookup(OidOp.OID(), lbidRanges);
	if (err) {
		cerr << "doAColScan: BRM LBID range lookup failure (1)\n";
		throw runtime_error("doAColScan: BRM LBID range lookup failure (1)");
	}

	err = dbrm.getHWM(OidOp.OID(), hwm);
	if (err) {
		cerr << "doAColScan: BRM HWM lookup failure (3)" << endl;
		throw runtime_error("doAColScan: BRM HWM lookup failure (3)");
	}
	f1.fNumMsgs = hwm/OidOp.fColType.colWidth + (hwm % OidOp.fColType.colWidth ? 1 : 0);

	thread t1(f1);

	bpp.createBPP(bs);
	dec->write(bs);
	bs.restart();

	uint32_t rangeSize=0, totalBlks = 0;
	clock_gettime(CLOCK_REALTIME, &ts1);
// 	cout << "BPP scaning\n";
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			cerr << "pColScanStep::sendPrimitiveMessages: dbrm.lookup failed for lbid " << lbid << endl;
			abort();
		}

		if (hwm < fbo)
			continue;

		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		bpp.setLBID(lbid);
		bpp.setCount(rangeSize/OidOp.fColType.colWidth + (rangeSize % OidOp.fColType.colWidth ? 1 : 0));
		bpp.runBPP(bs);
		dec->write(bs);
// 		cout << "sending the BPP\n";
		bpp.reset();
		bs.restart();
		totalBlks += rangeSize;
	}

	t1.join();
	clock_gettime(CLOCK_REALTIME, &ts2);
	timespec_sub(ts1, ts2, diff);
	float rate = 0;
	cout << "ColStep stats OID: " << OidOp.OID()
		<< " " <<(diff.tv_sec+(diff.tv_nsec/1000000000.0)) << "s"
		<< "\tFilters: " << filters
		<< "\tBlocks : " << (int) totalBlks;

	rate = totalBlks/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\tBlks/sec " << rate << endl;

	if (thdFcnFailure)
		cerr << "There was a failure in the read thread." << endl;

	bpp.destroyBPP(bs);
	dec->write(bs);
	cout << endl;

}

void doBatchOp_filt(OidOperation &OidOp)
{
	struct timespec ts1, ts2, diff;
	JobStepAssociation injs, outjs;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
	ThdFcn f1;
	boost::shared_ptr<CalpontSystemCatalog> sysCat = CalpontSystemCatalog::makeCalpontSystemCatalog(getpid());

	ByteStream bs;
	DBRM dbrm;
	BRM::LBIDRange_v lbidRanges;
	HWM_t hwm=0;
  	LBIDRange_v::iterator it;
  	OID_t tmp;
  	uint32_t fbo;
	uint32_t uniqueID;

	BatchPrimitiveProcessorJL bpp;
	uniqueID = dbrm.getUnique32();
	bpp.setUniqueID(uniqueID);
	bpp.setSessionID(OidOp.SessionId());
	bpp.setStepID(OidOp.SessionId());

	pColScanStep scan(injs, outjs, dec, sysCat, OidOp.fOid, OidOp.fOid,
		  OidOp.fSessionId, 0, OidOp.fSessionId, OidOp.fSessionId, OidOp.fSessionId, rm);
	while (OidOp.FilterCount()>0)
	{
		int8_t  cop;
		int64_t value;
		OidOp.deSerializeFilter(cop, value);
		scan.addFilter(cop, value);
	}
	bpp.addFilterStep(scan);

	pColStep step(injs, outjs, dec, sysCat, OidOp.fOid + 1, OidOp.fOid + 1,
		  OidOp.fSessionId, 0, OidOp.fSessionId, OidOp.fSessionId, OidOp.fSessionId, rm);
	while (OidOp.FilterCount()>0)
	{
		int8_t  cop;
		int64_t value;
		OidOp.deSerializeFilter(cop, value);
		step.addFilter(cop, value);
	}
	bpp.addFilterStep(step);

	execplan::CalpontSystemCatalog::ColType colType;
	FilterStep filt(OidOp.fSessionId, OidOp.fSessionId, OidOp.fSessionId, colType);
	filt.setBOP(OidOp.BOP());
	bpp.addFilterStep(filt);

	cout << "session number = " << OidOp.SessionId() << endl;
// 	dec->addSession(OidOp.SessionId());
// 	dec->addStep(OidOp.SessionId(), OidOp.SessionId());
	dec->addQueue(uniqueID);
	f1.fSessionid = OidOp.SessionId();
	f1.uniqueID = uniqueID;
	f1.fDec = dec;
	thdFcnFailure = false;
	int err = dbrm.lookup(OidOp.OID(), lbidRanges);
	if (err) {
		cerr << "doBatchOp_filt: BRM LBID range lookup failure (1)\n";
		throw runtime_error("doAColScan: BRM LBID range lookup failure (1)");
	}

	err = dbrm.getHWM(OidOp.OID(), hwm);
	if (err) {
		cerr << "doBatchOp_filt: BRM HWM lookup failure (2)" << endl;
		throw runtime_error("doBatchOp_filt: BRM HWM lookup failure (2)");
	}
	f1.fNumMsgs = hwm/OidOp.fColType.colWidth + (hwm % OidOp.fColType.colWidth ? 1 : 0);

	thread t1(f1);

	bpp.createBPP(bs);
	dec->write(bs);
	bs.restart();

	uint32_t rangeSize=0, totalBlks = 0;
	clock_gettime(CLOCK_REALTIME, &ts1);
// 	cout << "BPP scaning\n";
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			cerr << "doBatchOp_filt: dbrm.lookup failed for lbid (3)" << lbid << endl;
			abort();
		}

		if (hwm < fbo)
			continue;

		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		bpp.setLBID(lbid);
		bpp.setCount(rangeSize/OidOp.fColType.colWidth + (rangeSize % OidOp.fColType.colWidth ? 1 : 0));
		bpp.runBPP(bs);
		dec->write(bs);
// 		cout << "sending the BPP\n";
		bpp.reset();
		bs.restart();
		totalBlks += rangeSize;
	}

	t1.join();
	clock_gettime(CLOCK_REALTIME, &ts2);
	timespec_sub(ts1, ts2, diff);
	float rate = 0;
	cout << "doBatchOp_filt stats OID: " << OidOp.OID()
		<< " " <<(diff.tv_sec+(diff.tv_nsec/1000000000.0)) << "s"
		<< "\tBlocks : " << (int) totalBlks;

	rate = totalBlks/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\tBlks/sec " << rate << endl;

	if (thdFcnFailure)
		cerr << "There was a failure in the read thread." << endl;

	bpp.destroyBPP(bs);
	dec->write(bs);
	cout << endl;
}


void doBatchQueryOp(OperationList& OidOps)
{
	struct timespec ts1, ts2, diff;

	JobStepAssociation injs, outjs;
	BatchPrimitiveProcessorJL bpp;
	uint64_t rows = 0;
	uint32_t blockTouched = 0;
	DBRM dbrm;

	QryThdFcn f1(bpp, rows, blockTouched);

	OperationList::iterator filterOp = OidOps.begin();
	uint32_t sessionId = (*filterOp)->SessionId();
	uint32_t uniqueID = dbrm.getUnique32();
	bpp.setUniqueID(uniqueID);
	bpp.setSessionID(sessionId);
	bpp.setStepID(sessionId);
	cout << "session number = " << sessionId << endl;


	f1.fSessionid = sessionId;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
// 	dec->addSession(sessionId);
// 	dec->addStep(sessionId, sessionId);
	dec->addQueue(uniqueID);

	f1.fDec = dec;
	f1.uniqueID = uniqueID;
//	boost::shared_ptr<CalpontSystemCatalog> sysCat = execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(getpid());

//first column is made into the first scan filter step including filters
	OID_t scanOid = (*filterOp)->fOid;
	uint32_t scanWidth = (*filterOp)->ColumnWidth();
	uint32_t maxWidth = scanWidth;

	uint32_t pid = getpid();
	pColScanStep scan(injs, outjs, dec, execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(pid), scanOid, scanOid, sessionId, 0, sessionId, sessionId, sessionId, rm);

	uint32_t filterCount = (*filterOp)->FilterCount();
	while ((*filterOp)->FilterCount()>0)
	{
		int8_t cop;
		int64_t  value;
		(*filterOp)->deSerializeFilter(cop, value);
		scan.addFilter(cop, value);
	}
	bpp.addFilterStep(scan);

//Any other columns that are batch scans are added as filter steps, the rest as project steps.
//The last filter step is added as a passthru step into the project list.

	OperationList::iterator listend = OidOps.end();
	for(OperationList::iterator op = OidOps.begin() + 1; op != listend; ++op)
	{
	  pColStep step(injs, outjs, dec, execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(pid), (*op)->fOid, (*op)->fOid, sessionId, 0, sessionId, sessionId, sessionId, rm);

		if ((*op)->OpType()==OidOperation::BATCHSCAN)
		{
			filterCount += (*op)->FilterCount();
			while ((*op)->FilterCount()>0)
			{
				int8_t cop;
				int64_t  value;
				(*op)->deSerializeFilter(cop, value);
				step.addFilter(cop, value);
			}
			filterOp = op;
			bpp.addFilterStep(step);
		}
		else
		{
			bpp.addProjectStep(step);
		}
		if ( (*op)->ColumnWidth() > maxWidth)
			maxWidth = (*op)->ColumnWidth();
	}

	PassThruStep pass(injs, outjs, dec, (*filterOp)->ColumnType(), (*filterOp)->fOid, (*filterOp)->fOid,
			  sessionId, 0, sessionId, sessionId, sessionId, false, rm);
	bpp.addProjectStep(pass);

	ByteStream bs;
	BRM::LBIDRange_v lbidRanges;
	HWM_t hwm=0;
  	LBIDRange_v::iterator it;
  	OID_t tmp;
  	uint32_t fbo;

	thdFcnFailure = false;
	int err = dbrm.lookup(scanOid, lbidRanges);
	if (err) {
		cerr << "doQueryScan: BRM LBID range lookup failure (1)\n";
		throw runtime_error("doQueryScan: BRM LBID range lookup failure (1)");
	}

	err = dbrm.getHWM(scanOid, hwm);
	if (err) {
		cerr << "doQueryScan: BRM HWM lookup failure (3)" << endl;
		throw runtime_error("doQueryScan: BRM HWM lookup failure (3)");
	}
	f1.fNumMsgs = hwm/scanWidth + (hwm % scanWidth ? 1 : 0);

	thread t1(f1);
	uint32_t cnt = dbrm.getExtentSize()/maxWidth;

	bpp.createBPP(bs);
	dec->write(bs);
	bs.restart();
	uint32_t rangeSize=0, totalBlks = 0;
	clock_gettime(CLOCK_REALTIME, &ts1);
// 	cout << "BPP scaning\n";
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
	{
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			cerr << "doBatchQuery dbrm.lookup failed for lbid " << lbid << endl;
			abort();
		}

		if (hwm < fbo)
			continue;


		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		uint32_t totallbid = rangeSize/scanWidth + (0 < rangeSize % scanWidth ? 1 : 0) ;

		while ( 0 < totallbid)
		{
		    if (totallbid < cnt) cnt = totallbid;
		    bpp.setLBID(lbid);
		    bpp.setCount(cnt);
		    bpp.runBPP(bs);
		    dec->write(bs);
//		    cout << "sending the BPP with range cnt " << cnt  << " lbid " << lbid << "\n";
		    bpp.reset();
		    bs.restart();
		    lbid += cnt * scanWidth;
		    totallbid -= cnt;
		}

		for (OperationList::iterator op = OidOps.begin(); op != OidOps.end(); ++op)
		{
		  totalBlks += (uint32_t)(rangeSize * (double)( (double)(*op)->ColumnWidth()/scanWidth));
		}
	}

	t1.join();
	clock_gettime(CLOCK_REALTIME, &ts2);
	timespec_sub(ts1, ts2, diff);

	float rate = 0;
	cout << "QueryScan stats - " << bpp.toString()
		<< "\tElapsed: " <<(diff.tv_sec+(diff.tv_nsec/1000000000.0)) << "s"
		<< "\tFilters: " << filterCount
	     	<< "\tBlocks : " << (int) totalBlks;

	rate = totalBlks/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\tBlks/sec " << rate << endl;
	cout << "\tTouched Blocks: " << blockTouched;
	rate = blockTouched/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\tTouched Blks/sec " << rate << endl;

	cout << "\tRows: " << rows;
	rate = rows/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\t\tRows/sec " << fixed << setprecision(2) << rate << endl;



	if (thdFcnFailure)
		cerr << "There was a failure in the read thread." << endl;

	bpp.destroyBPP(bs);
	dec->write(bs);
	cout << endl;
}

void doBatchOp_step(OidOperation &OidOp)
{
	struct timespec ts1, ts2, diff;
	DBRM dbrm;
	BRM::LBIDRange_v lbidRanges;
	HWM_t hwm=0;
  	LBIDRange_v::iterator it;
  	OID_t tmp;
  	uint32_t fbo;
  	uint32_t totalBlks=0;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
	ThdFcn f1;
	JobStepAssociation injs, outjs;

	boost::shared_ptr<CalpontSystemCatalog> sysCat = execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(getpid());

	pColStep step(injs, outjs, dec, sysCat, OidOp.fOid, OidOp.fOid,
		      OidOp.fSessionId, 0, OidOp.fSessionId, OidOp.fSessionId, OidOp.fSessionId, rm);

	int32_t filters = OidOp.FilterCount();
	while (OidOp.FilterCount()>0)
	{
		int8_t cop;
		int64_t  value;
		OidOp.deSerializeFilter(cop, value);
		step.addFilter(cop, value);
	}

	BatchPrimitiveProcessorJL bpp;
	ElementType et;
	ByteStream obs;
	uint32_t uniqueID = dbrm.getUnique32();
	bpp.setUniqueID(uniqueID);
	bpp.setSessionID(OidOp.SessionId());
	bpp.setStepID(OidOp.SessionId());
	bpp.addFilterStep(step);

// 	dec->addSession(OidOp.SessionId());
// 	dec->addStep(OidOp.SessionId(), OidOp.SessionId());
	dec->addQueue(uniqueID);
	f1.fSessionid = OidOp.SessionId();
	f1.uniqueID = uniqueID;
	f1.fDec = dec;
	f1.fNumMsgs = 0;
	thdFcnFailure = false;
	ByteStream ridlist;
	uint16_t ridCount = 0; // BLOCK_SIZE/OidOp.ColumnWidth();

	for (uint16_t i=0; i<ridCount; i++)
		ridlist << i;

	int err = dbrm.lookup(OidOp.OID(), lbidRanges);
	if (err)
		throw runtime_error("doAColScan: BRM LBID range lookup failure (1)");

	err = dbrm.getHWM(OidOp.OID(), hwm);
	if (err)
		throw runtime_error("doAColScan: BRM HWM lookup failure (3)");

	uint32_t rangeSize=0;
	f1.fNumMsgs=hwm/OidOp.fColType.colWidth + (hwm % OidOp.fColType.colWidth ? 1 : 0);
	thread t1(f1);

	totalBlks=0;
	bpp.createBPP(obs);
	dec->write(obs);
	obs.restart();

	clock_gettime(CLOCK_REALTIME, &ts1);
  	for (it = lbidRanges.begin(); it != lbidRanges.end(); it++) {
		BRM::LBID_t lbid = (*it).start;
		if (dbrm.lookup(lbid, 0, false, tmp, fbo))
		{
			if (debug)
				cerr << "pColScanStep::sendPrimitiveMessages: dbrm.lookup failed for lbid " << lbid << endl;
			abort();
		}

		if (hwm < fbo)
			continue;

		rangeSize = (hwm > (fbo + (*it).size - 1) ? (*it).size : hwm - fbo + 1);
		for (unsigned i = 0; i < rangeSize; i++)
		{
			/* insert all rids for this LBID */
			for (uint j = 0; j < BLOCK_SIZE/OidOp.fColType.colWidth; ++j) {
				et.first = ((fbo+i) * BLOCK_SIZE/OidOp.fColType.colWidth) + j;
				et.second = j;
				bpp.addElementType(et);
			}
			/* If on a logical block boundary, send the primitive */
			if (i % OidOp.fColType.colWidth == (unsigned)OidOp.fColType.colWidth - 1) {
// 				cout << "serializing at extent offset " << i << endl;
				bpp.runBPP(obs);
				dec->write(obs);
				bpp.reset();
				obs.restart();
			}

			if (debug && i+1==rangeSize)
				cout
					<< "colStep: " << i+1 << "/"
					<< rangeSize << " " << obs.length()
					<< " lbid " << lbid+i << endl;
		}
		if (rangeSize % OidOp.fColType.colWidth) {
// 			cout << "serializing last msg\n";
			bpp.runBPP(obs);
			dec->write(obs);
			bpp.reset();
			obs.restart();
		}
		totalBlks+=rangeSize;
	} // for

	t1.join();
	clock_gettime(CLOCK_REALTIME, &ts2);
	timespec_sub(ts1, ts2, diff);
	float rate = 0;
	cout << "ColStep stats OID: " << OidOp.OID()
		<< " " <<(diff.tv_sec+(diff.tv_nsec/1000000000.0)) << "s"
		<< "\tFilters: " << filters
		<< "\tBlocks : " << (int) totalBlks;

	rate = totalBlks/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\tBlks/sec " << rate << endl;

	if (thdFcnFailure)
		cerr << "There was a failure in the read thread." << endl;

	bpp.destroyBPP(obs);
	dec->write(obs);

	cout << endl;

}

//
//
void doListOp(const OID_t o=0)
{

	DBRM dbrm;
	BRM::LBIDRange_v lbidRanges;
  	LBIDRange_v::iterator it;
	OID_t oid=3000;
	HWM_t hwm=0;

	if (o!=0) {
		int err = dbrm.lookup(o, lbidRanges);
		if (err)
			throw runtime_error("doAColScan: BRM LBID range lookup failure (1)");

		err = dbrm.getHWM(o, hwm);
		if (err)
			throw runtime_error("doAColScan: BRM HWM lookup failure (3)");

		cout << "Object ID: " << o << " HWM: " << hwm << endl;
  		for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
			cout << "\tStart: " << (*it).start << " sz: " << (*it).size << endl;
	} else {
		for (; oid < 100000; oid++) {

			int err = dbrm.lookup(oid, lbidRanges);
			if (lbidRanges.size()==0)
				continue;

			if (err)
				throw runtime_error("doAColScan: BRM LBID range lookup failure (1)");

			err = dbrm.getHWM(oid, hwm);
			if (err)
				throw runtime_error("doAColScan: BRM HWM lookup failure (3)");

			cout << "Object ID: " << oid << " HWM: " << hwm << endl;
  			for (it = lbidRanges.begin(); it != lbidRanges.end(); it++)
				cout << "\tStart: " << (*it).start << " sz: " << (*it).size << endl;
			hwm=0;
			lbidRanges.clear();
		} // for (; oid...
	} // else

} // doListOp

//
// do LoopBackOp

void doLoopBack(const uint64_t loopcount)
{

	ByteStream lbMsg;
	struct timespec ts1;
	struct timespec ts2;
	struct timespec diff;
	uint32_t sessionid = getpid();
	DBRM dbrm;
	ResourceManager rm;
	DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
	ThdFcn f1;

// 	dec->addSession(sessionid);
// 	dec->addStep(sessionid, sessionid);
	uint32_t uniqueID = dbrm.getUnique32();
	dec->addQueue(uniqueID);
	f1.fSessionid = sessionid;
	f1.uniqueID = uniqueID;
	f1.fDec = dec;
	f1.fNumMsgs = loopcount;
	thdFcnFailure = false;
	thread t1(f1);
	lbMsg = formatLoopBackMsg(sessionid, uniqueID);

	cout << "Sending " << loopcount << " LOOPBACK requests" << endl;
	clock_gettime(CLOCK_REALTIME, &ts1);
	for (uint64_t i=0; i< loopcount; i++)
	{
	  lbMsg = formatLoopBackMsg(sessionid, uniqueID);
		dec->write(lbMsg);
	}
	clock_gettime(CLOCK_REALTIME, &ts2);
	cout << loopcount << " LOOPBACK msgs sent" << endl;

	t1.join();
	timespec_sub(ts1, ts2, diff);
	cout << "\ttotal runtime: " << (diff.tv_sec
		+(diff.tv_nsec/1000000000.0)) << "s" << endl;

	float rate = 0;
	rate = loopcount/(diff.tv_sec+(diff.tv_nsec/1000000000.0));
	cout << "\t" << rate << " rqsts/s" << endl;
	dec->removeQueue(uniqueID);

} //doLoopBack

} //namespace

/*---------------------------------------------------------------------------
//Command line parameter definition
//
// -o <oid1> -o <oid2> -o . . . <oidN>
// -s <scan the oid>
// -t <colstep the oid>
// -d <turn debug output on>
// -eq -equal <int> < == test of values in a block>
// -gt -greater-than <int> < greater than (>) test of values in a block>
// -ge -greater-than-equal <int> < greater than or equal to (>=) test of values in a block>
// -lt -less-than <int> < less than (<) test of values in a block>
// -le -less-than-equal <int> < less than or equal to (<=) test of values in a block>
// -ne -not-equal <int> < not equal to (!=) test of values in a block>
// -bop <1 or 0> <1 AND 0 OR binary operator when 2 comparison filters are present>
// --list -list -l <optional oid> <print out all oids or the specified oid and their ranges>
// --loopback -loopback <count> send <count> loopback requests
// --concurrent run all jobs currently if set to true. defaults to false
// --lbid-trace -lb turn on lbid tracing
// --pm-profile -pmp turn on pm profiling
-----------------------------------------------------------------------------*/



int main(int argc, char** argv)
{

int64_t eq_val=0;
int64_t gt_val=0;
int64_t ge_val=0;
int64_t lt_val=0;
int64_t le_val=0;
int64_t ne_val=0;
int64_t bop_val=0;
int64_t loopback_count=100000;
int64_t scanOid=0;
int64_t stepOid=0;
bool list=false;
//OID_t listOid=0;
string oidString;
vector<OID_t> oidv;
int ch=0;
bool concurrent_flag=false;
bool lbidtrace_flag=false;
bool pmprofile_flag=false;
bool query_flag=false;


enum CLA_ENUM {
	OID=(int)0,
	SCANOP=(int)1,
	BLOCKOP=(int)2,
	DEBUG=(int)3,
	EQFILTER=(int)4,
	GTFILTER=(int)5,
	GEFILTER=(int)6,
	LTFILTER=(int)7,
	LEFILTER=(int)8,
	NEFILTER=(int)9,
	BOP=(int)10,
	LISTOP=(int)11,
	LOOPBACKOP=(int)12,
	CONCURRENT=(int)13,
	LBIDTRACE=(int)14,
	PMPROFILE=(int)15,
	INVALIDOP=(int)16,
	BATCHSCANOP=(int)17,
	BATCHSTEPOP=(int)18,
	BATCHFILTOP=(int)19,
	QUERYOP=(int)20

};

/**
// longopt struct
struct option {
    const char *name;
    int has_arg;
    int *flag;
    int val;
};
**/

static struct
option long_options[] =
{
//	{const char *name, 		int has_arg, 		int *flag,	int val},
	{"scan", 				required_argument, 	NULL, 		SCANOP },
	{"block", 				required_argument, 	NULL, 		BLOCKOP },
	{"debug", 				no_argument, 		NULL, 		DEBUG },
	{"equal", 				required_argument, 	NULL, 		EQFILTER },
	{"eq", 					required_argument, 	NULL, 		EQFILTER },
	{"greater-than",		required_argument, 	NULL, 		GTFILTER },
	{"gt",					required_argument, 	NULL, 		GTFILTER },
	{"greater-than-equal",	required_argument, 	NULL, 		GEFILTER },
	{"ge",					required_argument, 	NULL, 		GEFILTER },
	{"less-than", 			required_argument, 	NULL, 		LTFILTER },
	{"lt", 					required_argument, 	NULL, 		LTFILTER },
	{"less-than-equal", 	required_argument, 	NULL, 		LEFILTER },
	{"le",					required_argument, 	NULL, 		LEFILTER },
	{"not-equal",			required_argument, 	NULL, 		NEFILTER },
	{"ne",					required_argument, 	NULL, 		NEFILTER },
	{"bop",					optional_argument, 	NULL, 		BOP },
	{"list",				no_argument, 		NULL, 		LISTOP },
	{"loopback",			optional_argument, 	NULL, 		LOOPBACKOP },
	{"concurrent",			no_argument, 		NULL, 		CONCURRENT },
	{"lbid-trace",			no_argument, 		NULL, 		LBIDTRACE },
	{"lb",					no_argument, 		NULL, 		LBIDTRACE },
	{"pm-prof",				no_argument, 		NULL, 		PMPROFILE },
	{"pm-profile",			no_argument, 		NULL, 		PMPROFILE },
	{"pmp",					no_argument, 		NULL, 		PMPROFILE },
	{"batchscan",			required_argument,	NULL,		BATCHSCANOP },
	{"batchstep",			required_argument,	NULL,		BATCHSTEPOP },
	{"batchfilt",			required_argument,	NULL,		BATCHFILTOP },
	{"queryop",			no_argument,		NULL,		QUERYOP },
	{0, 				0, 			0, 		0}
};

OidOperation* currOp=NULL;
OperationList OpList;

if (argc <=1) {
	usage();
}

// process command line arguments
while( (ch = getopt_long_only(argc, argv, "B:Z:F:ds:t:lcqp:", long_options, NULL)) != -1 )
{

	pid_t pidId = getpid();

	switch (ch) {

		case SCANOP:
		case 's':
			if (optarg)
				scanOid=getInt(optarg);
			//cout << "OPT=" << ch << " ARG " << scanOid << endl;
			currOp=NULL;
			if (scanOid>0)
				currOp = new OidOperation(scanOid, OidOperation::SCAN, pidId );
			else {
				cout << "PingProc: scan missing or invalid OID parameter value" << endl;
				break;
			}

			if (currOp && currOp->isIntegralDataType())
				OpList.push_back(currOp);
			else {
				cout << "PingProc cannot process this ColumnType-oid: " << scanOid << endl;
				delete currOp;
				currOp=NULL;
			}
			break;

		case BATCHSCANOP:
		case 'B':
			if (optarg)
				scanOid=getInt(optarg);
			else
				cout << "no optarg\n";
			cout << "OPT=" << ch << " ARG " << scanOid << endl;
			currOp=NULL;
			if (scanOid>0)
				currOp = new OidOperation(scanOid, OidOperation::BATCHSCAN, pidId );
			else {
				cout << "PingProc: batch scan missing or invalid OID parameter value" << endl;
				break;
			}

			if (currOp && currOp->isIntegralDataType())
				OpList.push_back(currOp);
			else {
				cout << "PingProc cannot process this ColumnType-oid: " << scanOid << endl;
				delete currOp;
				currOp=NULL;
			}
			break;

		case BATCHSTEPOP:
		case 'Z':
			if (optarg)
				scanOid=getInt(optarg);
			else
				cout << "no optarg\n";
			cout << "OPT=" << ch << " ARG " << scanOid << endl;
			currOp=NULL;
			if (scanOid>0)
				currOp = new OidOperation(scanOid, OidOperation::BATCHSTEP, pidId );
			else {
				cout << "PingProc: batch step missing or invalid OID parameter value" << endl;
				break;
			}

			if (currOp && currOp->isIntegralDataType())
				OpList.push_back(currOp);
			else {
				cout << "PingProc cannot process this ColumnType-oid: " << scanOid << endl;
				delete currOp;
				currOp=NULL;
			}
			break;

		case BATCHFILTOP:
		case 'F':
			if (optarg)
				scanOid=getInt(optarg);
			else
				cout << "no optarg\n";
			cout << "OPT=" << ch << " ARG " << scanOid << endl;
			currOp=NULL;
			if (scanOid>0)
				currOp = new OidOperation(scanOid, OidOperation::BATCHFILT, pidId );
			else {
				cout << "PingProc: batch filter missing or invalid OID parameter value" << endl;
				break;
			}

			if (currOp && currOp->isIntegralDataType())
				OpList.push_back(currOp);
			else {
				cout << "PingProc cannot process this ColumnType-oid: " << scanOid << endl;
				delete currOp;
				currOp=NULL;
			}
			break;

		case BLOCKOP:
		case 't':
			if (optarg)
				stepOid=getInt(optarg);
			//cout << "OPT=" << ch << " ARG " << stepOid << endl;
			currOp=NULL;
			if (stepOid>0)
				currOp = new OidOperation(stepOid, OidOperation::BLOCK, pidId );
			else {
				cout << "PingProc: step missing or invalid OID parameter value" << endl;
				break;
			}

			if (currOp && currOp->isIntegralDataType())
				OpList.push_back(currOp);
			else {
				cout << "PingProc cannot process this ColumnType-oid: " << stepOid << endl;
				delete currOp;
				currOp=NULL;
			}

			break;

		case DEBUG:
		case 'd':
			//cout << "OPT=" << ch << endl;
			debug=true;
			break;

		case EQFILTER:
			if (optarg)
				eq_val=getInt(optarg);
			else
				eq_val=0;
			cout << "OPT=" << ch << " ARG=" << eq_val << endl;
			if (currOp)
				currOp->addFilter(COMPARE_EQ, eq_val);
			else
				; // TODO: error Processing
			break;

		case GTFILTER:
			if (optarg)
				gt_val=getInt(optarg);
			else
				gt_val=0;
			//cout << "OPT=" << ch << " ARG=" << gt_val << endl;
			if (currOp)
				currOp->addFilter(COMPARE_GT, gt_val);
			break;

		case GEFILTER:
			if (optarg)
				ge_val=getInt(optarg);
			else
				ge_val=0;
			//cout << "OPT=" << ch << " ARG=" << ge_val << endl;
			if (currOp)
				currOp->addFilter(COMPARE_GE, ge_val);
			break;

		case LTFILTER:
			if (optarg)
				lt_val=getInt(optarg);
			else
				lt_val=0;
			//cout << "OPT=" << ch << " ARG=" << lt_val << endl;
			if (currOp)
				currOp->addFilter(COMPARE_LT, lt_val);
			break;

		case LEFILTER:
			if (optarg)
				le_val=getInt(optarg);
			else
				le_val=0;
			//cout << "OPT=" << ch << " ARG=" << le_val << endl;
			if (currOp)
				currOp->addFilter(COMPARE_LE, le_val);
			break;

		case NEFILTER:
		case 'n':
			if (optarg)
				ne_val=getInt(optarg);
			else
				ne_val=0;
			//cout << "OPT=" << ch << " ARG=" << ne_val << endl;
			if (currOp)
				currOp->addFilter(COMPARE_NE, ne_val);
			break;

		case BOP:
		case 'b':
			if (optarg)
				bop_val=getInt(optarg);
			else
				bop_val=1; // assume AND
			//cout << "OPT=" << ch << " ARG=" << bop_val << endl;
			if (currOp)
				currOp->BOP(bop_val?BOP_AND:BOP_OR);
			break;

		case LISTOP:
		case 'l':
			/**
			if (optarg)
				listOid=getInt(optarg);
			else
				listOid=0;
			cout << "OPT=" << ch << " LISTOP " << listOid << endl;
			**/
			list=true;
			break;

		case LOOPBACKOP:
	        case 'p':
			if (optarg)
				loopback_count=getInt(optarg);

			//cout << "OPT=" << ch << " LOOPBACKOP " << loopback_count << endl;
			currOp=NULL;
			currOp = new OidOperation(0, OidOperation::LOOPBACK, pidId );
			OpList.push_back(currOp);
			break;

		case 'c':
		case CONCURRENT:
			concurrent_flag=true;
			//cout << "OPT=" << ch << " CONCURRENT " << concurrent_flag << endl;
			break;

		case LBIDTRACE:
			lbidtrace_flag=true;
			//cout << "OPT=" << ch << " CONCURRENT " << concurrent_flag << endl;
			if (currOp)
				currOp->setLbidTraceOn();
			break;

		case PMPROFILE:
			pmprofile_flag=true;
			//cout << "OPT=" << ch << " CONCURRENT " << concurrent_flag << endl;
			if (currOp)
				currOp->setPMProfileOn();
			break;
		case 'q':
		case QUERYOP:
			query_flag=true;
// 			cout << "OPT=" << ch << " QUERY FLAG " << query_flag << endl;
			break;


		case '?':
		default:
			cout << "optarg " << optarg << endl;
			usage();

	}

	if (list==true)
		break;

} // while

// if list is requested, print the listing and exit
//
vector<struct BatchScanThr*> BatchScanThreads;
vector<struct BatchStepThr*> BatchStepThreads;
vector<struct BatchFiltThr*> BatchFiltThreads;
vector<struct ColScanThr*> ColScanThreads;
vector<struct ColStepThr*> ColStepThreads;
vector<struct DictScanThr*> DictScanThreads;
vector<struct DictSigThr*> DictSigThreads;
vector<thread*> thrArray;
if (query_flag)
{
	cout << "starting batch query thread\n";
	struct BatchQueryThr* qt = new struct BatchQueryThr(OpList);
	thread* t1 = new thread(*qt);
	if (concurrent_flag)
		thrArray.push_back(t1);
	else
		t1->join();
}
else
if (list) {
	doListOp();
} else {

	for (uint i=0; i<OpList.size(); i++) {

		if (OpList[i]->OpType()==OidOperation::LOOPBACK) {
			doLoopBack(loopback_count);
		}

		else if (OpList[i]->OpType()==OidOperation::SCAN) {
			struct ColScanThr* cst = new struct ColScanThr(*OpList[i]);
			ColScanThreads.push_back(cst);
			thread* t1 = new thread(*cst);
			if (concurrent_flag)
				thrArray.push_back(t1);
			else
			{
				t1->join();
				delete t1;
			}
		}

		else if (OpList[i]->OpType()==OidOperation::BLOCK) {
			struct ColStepThr* cst = new struct ColStepThr(*OpList[i]);
			ColStepThreads.push_back(cst);
			thread* t1 = new thread(*cst);
			if (concurrent_flag)
				thrArray.push_back(t1);
			else
			{
				t1->join();
				delete t1;
			}
		}

		else if (OpList[i]->OpType()==OidOperation::BATCHSCAN) {
			cout << "starting batch scan thread\n";
			struct BatchScanThr* cst = new struct BatchScanThr(*OpList[i]);
			BatchScanThreads.push_back(cst);
			thread* t1 = new thread(*cst);
			if (concurrent_flag)
				thrArray.push_back(t1);
			else
			{
				t1->join();
				delete t1;
			}
		}

		else if (OpList[i]->OpType()==OidOperation::BATCHSTEP) {
			cout << "starting batch step thread\n";
			struct BatchStepThr* cst = new struct BatchStepThr(*OpList[i]);
			BatchStepThreads.push_back(cst);
			thread* t1 = new thread(*cst);
			if (concurrent_flag)
				thrArray.push_back(t1);
			else
			{
				t1->join();
				delete t1;
			}
		}

		else if (OpList[i]->OpType()==OidOperation::BATCHFILT) {
			cout << "starting batch filt thread\n";
			struct BatchFiltThr* cst = new struct BatchFiltThr(*OpList[i]);
			BatchFiltThreads.push_back(cst);
			thread* t1 = new thread(*cst);
			if (concurrent_flag)
				thrArray.push_back(t1);
			else
			{
				t1->join();
				delete t1;
			}
			i += 2;
		}
	} // for

} // else


// join threads to main
for (uint i=0; i<thrArray.size(); i++)
	thrArray[i]->join();

// clean up
for(uint i=0; i < OpList.size(); i++)
	delete OpList[i];
OpList.clear();

for(uint i=0; i < BatchScanThreads.size(); i++)
	delete BatchScanThreads[i];
BatchScanThreads.clear();

for(uint i=0; i < BatchStepThreads.size(); i++)
	delete BatchStepThreads[i];
BatchStepThreads.clear();

for(uint i=0; i < BatchFiltThreads.size(); i++)
	delete BatchFiltThreads[i];
BatchFiltThreads.clear();

for(uint i=0; i < ColScanThreads.size(); i++)
	delete ColScanThreads[i];
ColScanThreads.clear();

for(uint i=0; i < ColStepThreads.size(); i++)
	delete ColStepThreads[i];
ColStepThreads.clear();

for(uint i=0; i < DictScanThreads.size(); i++)
	delete DictScanThreads[i];
DictScanThreads.clear();

for(uint i=0; i < DictSigThreads.size(); i++)
	delete DictSigThreads[i];
DictSigThreads.clear();

for (uint i=0; i < thrArray.size(); i++)
	delete thrArray[i];
thrArray.clear();

} //main()

