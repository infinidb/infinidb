/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/***********************************************************************
 *   $Id: dmlpackageprocessor.cpp 9673 2013-07-09 15:59:49Z chao $
 *
 *
 ***********************************************************************/
#define DMLPKGPROC_DLLEXPORT
#include "dmlpackageprocessor.h"
#undef DMLPKGPROC_DLLEXPORT

#include <math.h>
using namespace std;

#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;
#include <boost/tokenizer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;
#include <boost/shared_ptr.hpp>

#include "we_messages.h"
using namespace WriteEngine;
using namespace dmlpackage;
#include "calpontselectexecutionplan.h"
#include "simplecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "columnresult.h"
using namespace execplan;
using namespace logging;
#include "configcpp.h"
using namespace config;
#include "joblistfactory.h"
#include "joblist.h"
#include "distributedenginecomm.h"
using namespace joblist;
#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;
#include "tablelockdata.h"
#include "exceptclasses.h"

namespace
{
using namespace execplan;

const SOP opeq(new Operator("="));
const SOP opne(new Operator("<>"));
const SOP opor(new Operator("or"));
const SOP opand(new Operator("and"));
}

namespace dmlpackageprocessor
{

    DMLPackageProcessor::~DMLPackageProcessor()
    {
		//cout << "In DMLPackageProcessor destructor " << this << endl;
		if (fWEClient)
			delete fWEClient; 
		if (fExeMgr)
			delete fExeMgr;
	}

//@bug 397
void DMLPackageProcessor::cleanString(string& s)
{
    string::size_type pos = s.find_first_not_of(" ");
    //stripe off space and ' or '' at beginning and end       
    if ( pos < s.length() )
    {    
		s = s.substr( pos, s.length()-pos );
		if ( (pos = s.find_last_of(" ")) < s.length())
		{
			s = s.substr(0, pos );           				 
		}
    }
    if  ( s[0] == '\'')
    {
		s = s.substr(1, s.length()-2);
		if  ( s[0] == '\'')
			s = s.substr(1, s.length()-2);
    }
}
#if 0
boost::any DMLPackageProcessor::tokenizeData( execplan::CalpontSystemCatalog::SCN txnID,
	execplan::CalpontSystemCatalog::ColType colType,
	const std::string& data, DMLResult& result, bool isNULL )
{
	SUMMARY_INFO("DMLPackageProcessor::tokenizeData");

	bool retval = true;
	boost::any value;

	if (isNULL)
	{
		WriteEngine::Token nullToken;
		value = nullToken;
	}
	else
	{
		if ( data.length() > (unsigned int)colType.colWidth )
		{
			retval = false;
			// build the logging message
			logging::Message::Args args;
			logging::Message message(6);
			args.add("Insert value is too large for colum ");
			message.format( args );

			result.result = INSERT_ERROR;
			result.message = message;
		}
		else
		{
			//Tokenize the data value
			WriteEngine::DctnryStruct dictStruct;
			dictStruct.dctnryOid = colType.ddn.dictOID;
			//cout << "Dictionary OIDs: " << colType.ddn.treeOID << " " << colType.ddn.listOID << endl;
			WriteEngine::DctnryTuple  dictTuple;
			memcpy(dictTuple.sigValue, data.c_str(), data.length());
			dictTuple.sigSize = data.length();
			int error = NO_ERROR;
			if ( NO_ERROR != (error = fWriteEngine.tokenize( txnID, dictStruct, dictTuple)) )
			{
				retval = false;
				//cout << "Error code from WE: " << error << endl;
				// build the logging message
				logging::Message::Args args;
				logging::Message message(1);
				args.add("Tokenization failed on: ");
				args.add(data);
				args.add("error number: ");
				args.add( error );
				message.format( args );

				result.result = TOKEN_ERROR;
				result.message = message;
			}
			WriteEngine::Token aToken = dictTuple.token;
			value = aToken;
		}
	}
	return value;
}
#endif
void DMLPackageProcessor::getColumnsForTable(uint32_t sessionID, std::string schema,
	std::string table, dmlpackage::ColumnList& colList)
{

	CalpontSystemCatalog::TableName tableName;
	tableName.schema = schema;
	tableName.table = table;

	CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
	CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName, true);

	CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();
	while (rid_iterator != ridList.end())
	{
		CalpontSystemCatalog::ROPair roPair = *rid_iterator;
		DMLColumn* columnPtr = new DMLColumn();
		CalpontSystemCatalog::TableColName tblColName = systemCatalogPtr->colName( roPair.objnum );
		columnPtr->set_Name(tblColName.column);

		colList.push_back(columnPtr);

		++rid_iterator;
	}

}

char* DMLPackageProcessor::strlower(char* in)
{
  char* p = in;
  if (p)
  {
	while (*p)
	{
	  *p = tolower(*p);
	  p++;
	}
  }
  return in;
}

void DMLPackageProcessor::convertRidToColumn(uint64_t& rid, unsigned& dbRoot, unsigned& partition, 
	unsigned& segment, unsigned filesPerColumnPartition, unsigned  extentsPerSegmentFile, unsigned extentRows, 
	unsigned startDBRoot, unsigned dbrootCnt, const unsigned startPartitionNum)
{
	partition = rid / (filesPerColumnPartition * extentsPerSegmentFile * extentRows);
	
	segment = (((rid % ( filesPerColumnPartition * extentsPerSegmentFile * extentRows)) /
		extentRows)) % filesPerColumnPartition;
	
	dbRoot = ((startDBRoot - 1 + segment) % dbrootCnt) + 1;
	
	//Calculate the relative rid for this segment file
	uint64_t relRidInPartition = rid - ((uint64_t)partition * (uint64_t)filesPerColumnPartition *
		(uint64_t)extentsPerSegmentFile * (uint64_t)extentRows);
	idbassert(relRidInPartition <= (uint64_t)filesPerColumnPartition * (uint64_t)extentsPerSegmentFile *
		(uint64_t)extentRows);
	uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
	unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
	uint64_t relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
	rid = relRidInThisExtent +  numExtentsInThisSegPart * extentRows;
}


string DMLPackageProcessor::projectTableErrCodeToMsg(uint ec)
{
	if (ec < 1000) // pre IDB error code
	{
		ErrorCodes ecObj;
		string errMsg("Statement failed.");
		errMsg += ecObj.errorString(ec).substr(150); // substr removes ErrorCodes::fPreamble
		return errMsg;
	}

	// IDB error
	return IDBErrorInfo::instance()->errorMsg(ec);
}

bool DMLPackageProcessor::validateVarbinaryVal( std::string & inStr)
{
	bool invalid = false;
	for (unsigned i=0; i < inStr.length(); i++)
	{
		if (!isxdigit(inStr[i]))
		{
			invalid = true;
			break;
		}
	
	}
	return invalid;
}

int DMLPackageProcessor::commitTransaction(uint64_t uniqueId, BRM::TxnID txnID)
{
	int rc = fDbrm->vbCommit(txnID.id);
	return rc;
}

int DMLPackageProcessor::rollBackTransaction(uint64_t uniqueId, BRM::TxnID txnID, uint32_t sessionID,
	std::string& errorMsg)
{
	std::vector<BRM::LBID_t> lbidList;
	std::vector<BRM::LBIDRange> lbidRangeList;
	BRM::LBIDRange   range;
	int rc = 0;
	//Check BRM status before processing.
	rc = fDbrm->isReadWrite();
	if (rc != 0 )
	{
        std::string brmMsg;
        errorMsg = "Can't read DBRM isReadWrite [ ";
        BRM::errString(rc, brmMsg);
        errorMsg += brmMsg;
        errorMsg += "]";
		return rc;
	}

	ByteStream bytestream;
	fWEClient->addQueue(uniqueId);
	//cout << "adding to queue with id " << uniqueId << endl;
	bytestream << (ByteStream::byte) WE_SVR_ROLLBACK_BLOCKS;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << (uint32_t)txnID.id;
	uint msgRecived = 0;
	try {
		fWEClient->write_to_all(bytestream);
		boost::shared_ptr<messageqcpp::ByteStream> bsIn;
		bsIn.reset(new ByteStream());
		ByteStream::byte tmp8;
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
                errorMsg = "Network error reading WEClient";
				fWEClient->removeQueue(uniqueId);
				//cout << "erroring out remove queue id " << uniqueId << endl;
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
                    char szrc[20];
					*bsIn >> errorMsg;
                    errorMsg += " (WriteEngine returns error ";
                    sprintf(szrc, "%d", rc);
                    errorMsg += szrc;
                    errorMsg += ")";
					fWEClient->removeQueue(uniqueId);
					cout << "erroring out remove queue id " << uniqueId << endl;
					break;
				}
				else
					msgRecived++;						
			}
		}
	}
	catch(std::exception& e)
	{
		rc = NETWORK_ERROR;
		errorMsg = "Network error occured when rolling back blocks";
		errorMsg += e.what();
		fWEClient->removeQueue(uniqueId);
		cout << "erroring out remove queue id " << uniqueId << endl;
		//delete fWEClient;
		return rc;
	}
	catch ( ... )
	{
		rc = NETWORK_ERROR;
		errorMsg = "Unknown exception caught while rolling back transaction.";
		fWEClient->removeQueue(uniqueId);
		cout << "erroring out remove queue id " << uniqueId << endl;
		//delete fWEClient;
		return rc;
	} 
	
	if (rc != 0)
	{
		//delete fWEClient;
		return rc;
	}
	
	fWEClient->removeQueue(uniqueId);
	//delete fWEClient;
//	cout << "success. remove queue id " << uniqueId << endl;
	rc = fDbrm->getUncommittedLBIDs(txnID.id, lbidList);
	if (rc != 0 )
	{
        std::string brmMsg;
        errorMsg = "DBRM getUncommittedLBIDs [ ";
        BRM::errString(rc, brmMsg);
        errorMsg += brmMsg;
        errorMsg += "]";
        return rc;
	}

	for(size_t i = 0; i < lbidList.size(); i++) {
		range.start = lbidList[i];
		range.size = 1;
		lbidRangeList.push_back(range);
	}
	rc =  fDbrm->vbRollback(txnID.id, lbidRangeList);	
	
	if (rc != 0 )
	{
        std::string brmMsg;
        errorMsg = "DBRM vbRollback [ ";
        BRM::errString(rc, brmMsg);
        errorMsg += brmMsg;
        errorMsg += "]";
        return rc;
	}

	return rc;
}

int DMLPackageProcessor::commitBatchAutoOnTransaction(uint64_t uniqueId, BRM::TxnID txnID, const uint32_t tableOid, std::string & errorMsg)
{
	//collect hwm info from all pms and set them here. remove table metadata if all successful
	ByteStream bytestream;
	fWEClient->addQueue(uniqueId);
	bytestream << (ByteStream::byte)WE_SVR_COMMIT_BATCH_AUTO_ON;
	bytestream << uniqueId;
	bytestream << (uint32_t) txnID.id;
	bytestream << tableOid;
	bytestream << fSessionID;
	
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	int rc = 0;
	ByteStream::byte tmp8;
	typedef std::vector<BRM::BulkSetHWMArg> BulkSetHWMArgs;
	std::vector<BulkSetHWMArgs> hwmArgsAllPms;
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			rc = NETWORK_ERROR;
			fWEClient->removeQueue(uniqueId);
			break;
		}			
		else {
			*bsIn >> tmp8;
			rc = tmp8;
			if (rc != 0) {
				*bsIn >> errorMsg;
				fWEClient->removeQueue(uniqueId);
				break;
			}
			else
			{
				//get hwm info
				*bsIn >> errorMsg;
				BulkSetHWMArgs setHWMArgs;
				//cout << "received from WES bytestream length = " <<  bsIn->length() << endl;
				deserializeInlineVector(*(bsIn.get()), setHWMArgs);
				//cout << "get hwm info from WES size " << setHWMArgs.size() << endl;
				hwmArgsAllPms.push_back(setHWMArgs);
				msgRecived++;	
			}
		}
	}
	if (rc != 0)
		return rc;
		
	//set hwm
	std::vector<BRM::BulkSetHWMArg> allHwm;
	BulkSetHWMArgs::const_iterator itor;
	//cout << "total hwmArgsAllPms size " << hwmArgsAllPms.size() << endl;
	for (unsigned i=0; i  < fWEClient->getPmCount(); i++)
	{
		itor = hwmArgsAllPms[i].begin();
		while (itor != hwmArgsAllPms[i].end())
		{
			allHwm.push_back(*itor);
			//cout << "received hwm info: " <<  itor->oid << ":" << itor->hwm << endl;
			itor++;
		}
	}
	//set CP data before hwm.
	
	//cout << "setting hwm allHwm size " << allHwm.size() << endl;
	vector<BRM::LBID_t> lbidList;
	if (idbdatafile::IDBPolicy::useHdfs())
	{
		BRM::LBID_t startLbid;
		for ( unsigned i=0; i < allHwm.size(); i++)
		{
			rc = fDbrm->lookupLocalStartLbid(allHwm[i].oid, allHwm[i].partNum, allHwm[i].segNum, allHwm[i].hwm, startLbid);
			lbidList.push_back(startLbid);
		}
	}
	else
		fDbrm->getUncommittedExtentLBIDs(static_cast<BRM::VER_t>(txnID.id), lbidList);
	vector<BRM::LBID_t>::const_iterator iter = lbidList.begin();
	vector<BRM::LBID_t>::const_iterator end = lbidList.end();
	BRM::CPInfoList_t cpInfos;
	BRM::CPInfo aInfo;
	while (iter != end)
	{
		aInfo.firstLbid = *iter;
		aInfo.max = numeric_limits<int64_t>::min(); // Not used
		aInfo.min = numeric_limits<int64_t>::max(); // Not used
		aInfo.seqNum = -1;
		cpInfos.push_back(aInfo);
		++iter;
	}
	std::vector<BRM::CPInfoMerge>  mergeCPDataArgs;
	rc = fDbrm->bulkSetHWMAndCP(allHwm, cpInfos, mergeCPDataArgs, txnID.id);
	fDbrm->takeSnapshot();
	//Set tablelock to rollforward remove meta files
	
	if (rc != 0)
		return rc;
		
	bool stateChanged = true;
	TablelockData * tablelockData = TablelockData::makeTablelockData(fSessionID);
	uint64_t tablelockId = tablelockData->getTablelockId(tableOid);
	
	try {
		stateChanged = fDbrm->changeState(tablelockId, BRM::CLEANUP);
	}
	catch (std::exception&)
	{
		errorMsg = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
		stateChanged = false;
	}
	
	if (!stateChanged)
		return rc;
		
	bytestream.restart();
	//@Bug 4517 Remove meta data failure doesn't stop tablelock releasing.
	bytestream << (ByteStream::byte)WE_SVR_BATCH_AUTOON_REMOVE_META;
	bytestream << uniqueId;
	bytestream << tableOid;
	msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			fWEClient->removeQueue(uniqueId);
			break;
		}			
		else {
			*bsIn >> tmp8;
			msgRecived++;						
		}
	}
	
	return rc;
		
	
}

int DMLPackageProcessor::rollBackBatchAutoOnTransaction(uint64_t uniqueId, BRM::TxnID txnID, uint32_t sessionID, 
			const uint32_t tableOid, std::string & errorMsg)
{
	//Bulkrollback, rollback blocks, vbrollback, change state, remove meta file
	//cout << "In rollBackBatchAutoOnTransaction" << endl;
	std::vector<BRM::TableLockInfo> tableLocks;
	tableLocks = fDbrm->getAllTableLocks();
	//cout << " Got all tablelocks" << endl;
	unsigned idx=0;
	string ownerName ("DMLProc batchinsert");
	uint64_t tableLockId = 0;
	int rc = 0;
	for (; idx<tableLocks.size(); idx++)
	{
		if ((tableLocks[idx].ownerName == ownerName) && (tableLocks[idx].tableOID == tableOid))
		{
			tableLockId = tableLocks[idx].id;
			break;
		}		
	}
	
	if ((tableLockId == 0) || (tableOid ==0))
	{
		// table is not locked by DMLProc. Could happen if we failed to get lock
		// while inserting. Not an error during rollback, but we don't
		// want to do anything.
		return rc;
	}
	//cout << "sending to WES" << endl;
	ByteStream bytestream;
	fWEClient->addQueue(uniqueId);
	//cout << "adding queue id " << uniqueId << endl;
	bytestream << (ByteStream::byte) WE_SVR_ROLLBACK_BATCH_AUTO_ON;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << tableLockId;
	bytestream << tableOid;
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	ByteStream::byte tmp8;
	//cout << "waiting for reply from WES" << endl;
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			rc = NETWORK_ERROR;
			fWEClient->removeQueue(uniqueId);
			//cout << "erroring out remove queue id " << uniqueId << endl;
			break;
		}			
		else {
			*bsIn >> tmp8;
			rc = tmp8;
			if (rc != 0) {
				*bsIn >> errorMsg;
				fWEClient->removeQueue(uniqueId);
				//cout << "erroring out remove queue id " << uniqueId << endl;
				break;
			}
			else
				msgRecived++;						
		}
	}
	if (rc == 0) //change table lock state
	{
		bool stateChanged = true;
		//cout << "changing tablelock state" << endl;
		try {
			stateChanged = fDbrm->changeState(tableLockId, BRM::CLEANUP);
		}
		catch (std::exception&)
		{
			errorMsg = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
			stateChanged = false;
		}
		if (!stateChanged)
		{
			rc = 1;
		}
	}
	
	if ( rc !=0 )
		return rc;
	bytestream.restart();
	bytestream << (ByteStream::byte)WE_SVR_BATCH_AUTOON_REMOVE_META;
	bytestream << uniqueId;
	bytestream << tableOid;
	msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			fWEClient->removeQueue(uniqueId);
			//cout << "erroring out remove queue id " << uniqueId << endl;
			break;
		}			
		else {
			*bsIn >> tmp8;
			msgRecived++;						
		}
	} 
	fWEClient->removeQueue(uniqueId);				
	return rc;
}

int DMLPackageProcessor::commitBatchAutoOffTransaction(uint64_t uniqueId, BRM::TxnID txnID, const uint32_t tableOid, std::string & errorMsg)
{
	std::vector<BRM::TableLockInfo> tableLocks;
	tableLocks = fDbrm->getAllTableLocks();
	//cout << " Got all tablelocks" << endl;
	unsigned idx=0;
	string ownerName ("DMLProc batchinsert");
	uint64_t tableLockId = 0;
	int rc = 0;
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	ByteStream::byte tmp8;
	for (; idx<tableLocks.size(); idx++)
	{
		if ((tableLocks[idx].ownerName == ownerName) && (tableLocks[idx].tableOID == tableOid))
		{
			tableLockId = tableLocks[idx].id;
			break;
		}		
	}
	
	if ((tableLockId == 0) || (tableOid ==0))
	{
		// table is not locked by DMLProc. Could happen if we failed to get lock
		// while inserting. Not an error during rollback, but we don't
		// want to do anything.
		return rc;
	}
	
	bool stateChanged = true;
		//cout << "changing tablelock state" << endl;
	try {
			stateChanged = fDbrm->changeState(tableLockId, BRM::CLEANUP);
	}
	catch (std::exception&)
	{
			errorMsg = IDBErrorInfo::instance()->errorMsg(ERR_HARD_FAILURE);
			stateChanged = false;
	}
	if (!stateChanged)
	{
		rc = 1;
	}
	if ( rc !=0 )
		return rc;
		
	ByteStream bytestream;
	fWEClient->addQueue(uniqueId);
	bytestream << (ByteStream::byte)WE_SVR_BATCH_AUTOON_REMOVE_META;
	bytestream << uniqueId;
	bytestream << tableOid;
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			fWEClient->removeQueue(uniqueId);
			break;
		}			
		else {
			*bsIn >> tmp8;
			msgRecived++;						
		}
	}
	fWEClient->removeQueue(uniqueId);	
	return rc;
}

int DMLPackageProcessor::rollBackBatchAutoOffTransaction(uint64_t uniqueId, BRM::TxnID txnID, uint32_t sessionID, 
			const uint32_t tableOid, std::string & errorMsg)
{
	ByteStream bytestream;
	fWEClient->addQueue(uniqueId);
	bytestream << (ByteStream::byte) WE_SVR_ROLLBACK_BATCH_AUTO_OFF;
	bytestream << uniqueId;
	bytestream << sessionID;
	bytestream << (uint32_t)txnID.id;
	bytestream << tableOid;
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	int rc = 0;
	ByteStream::byte tmp8;
	
	while (1)
	{
		if (msgRecived == fWEClient->getPmCount())
			break;
		fWEClient->read(uniqueId, bsIn);
		if ( bsIn->length() == 0 ) //read error
		{
			rc = NETWORK_ERROR;
			fWEClient->removeQueue(uniqueId);
			break;
		}			
		else {
			*bsIn >> tmp8;
			rc = tmp8;
			if (rc != 0) {
				*bsIn >> errorMsg;
				fWEClient->removeQueue(uniqueId);
				break;
			}
			else
				msgRecived++;						
		}
	}
					
	return rc;
}

int DMLPackageProcessor::flushDataFiles (int rcIn, std::map<FID,FID> & columnOids, uint64_t uniqueId, BRM::TxnID txnID, uint32_t tableOid)
{
//cout <<"in flushDataFiles" << endl;
	ByteStream bytestream;
	bytestream << (ByteStream::byte) WE_SVR_FLUSH_FILES;
	bytestream << uniqueId;
	bytestream << (uint32_t) rcIn;
	bytestream << (uint32_t)txnID.id;
	bytestream << tableOid;
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	int rc = 0;
	ByteStream::byte tmp8;
	std::string errorMsg;
	try {
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
					*bsIn >> errorMsg;
					break;
				}
				else
					msgRecived++;						
			}
		}
	}
	catch (std::exception&) {
	}

	return rc;
}

int DMLPackageProcessor::endTransaction (uint64_t uniqueId, BRM::TxnID txnID, bool success)
{
//cout <<"in flushDataFiles" << endl;
	ByteStream bytestream;
	bytestream << (ByteStream::byte) WE_END_TRANSACTION;
	bytestream << uniqueId;
	bytestream << (uint32_t)txnID.id;
	bytestream << (ByteStream::byte)success;
	uint msgRecived = 0;
	fWEClient->write_to_all(bytestream);
	boost::shared_ptr<messageqcpp::ByteStream> bsIn;
	bsIn.reset(new ByteStream());
	int rc = 0;
	ByteStream::byte tmp8;
	std::string errorMsg;
	try {
		while (1)
		{
			if (msgRecived == fWEClient->getPmCount())
				break;
			fWEClient->read(uniqueId, bsIn);
			if ( bsIn->length() == 0 ) //read error
			{
				rc = NETWORK_ERROR;
				break;
			}			
			else {
				*bsIn >> tmp8;
				rc = tmp8;
				if (rc != 0) {
					*bsIn >> errorMsg;
					break;
				}
				else
					msgRecived++;						
			}
		}
	}
	catch (std::exception&) {
	}

	return rc;
}
}
// vim:ts=4 sw=4:
