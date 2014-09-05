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

//
// $Id: columncommand.cpp 2057 2013-02-13 17:00:10Z pleblanc $
// C++ Implementation: columncommand
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>
#include <sstream>
#include <map>
#include <cstdlib>
#include <cmath>
using namespace std;

#include "bpp.h"
#include "errorcodes.h"
#include "exceptclasses.h"
#include "primitiveserver.h"
#include "primproc.h"
#include "stats.h"

using namespace messageqcpp;
using namespace rowgroup;

#include "messageids.h"
using namespace logging;

#ifdef _MSC_VER
#define llabs labs
#endif

namespace
{
using namespace primitiveprocessor;

double cotangent(double in)
{
	return (1.0 / tan(in));
}

inline UDFFcnPtr_t ord2Fptr(uint8_t ord)
{
#ifndef _MSC_VER
	if (ord == 0)
		return 0;

	if (ord == 255) return reinterpret_cast<UDFFcnPtr_t>(::llabs);
	if (ord == 254) return reinterpret_cast<UDFFcnPtr_t>(::acos);
	if (ord == 253) return reinterpret_cast<UDFFcnPtr_t>(::asin);
	if (ord == 252) return reinterpret_cast<UDFFcnPtr_t>(::atan);
	if (ord == 251) return reinterpret_cast<UDFFcnPtr_t>(::cos);
	if (ord == 250) return reinterpret_cast<UDFFcnPtr_t>(cotangent);
	if (ord == 249) return reinterpret_cast<UDFFcnPtr_t>(::exp);
	if (ord == 248) return reinterpret_cast<UDFFcnPtr_t>(::log);
#ifdef __linux__
	if (ord == 247) return reinterpret_cast<UDFFcnPtr_t>(::log2);
#else
	if (ord == 247) return reinterpret_cast<UDFFcnPtr_t>(::log);
#endif
	if (ord == 246) return reinterpret_cast<UDFFcnPtr_t>(::log10);
	if (ord == 245) return reinterpret_cast<UDFFcnPtr_t>(::sin);
	if (ord == 244) return reinterpret_cast<UDFFcnPtr_t>(::sqrt);
	if (ord == 243) return reinterpret_cast<UDFFcnPtr_t>(::tan);

	//we'd like to avoid having to lock the map, so we need to use const methods only
	UDFFcnMap_t::const_iterator it = UDFFcnMap.find(ord);
	if (it == UDFFcnMap.end())
		return 0;

	return it->second;
#else
	return 0;
#endif
}
}

namespace primitiveprocessor
{

ColumnCommand::ColumnCommand() :
	Command(COLUMN_COMMAND),
	blockCount(0),
	loadCount(0),
	suppressFilter(false),
	fUdfFuncPtr(0)
	{
	}

ColumnCommand::~ColumnCommand() { }

void ColumnCommand::_execute()
{
// 	cout << "CC: executing" << endl;
	if (_isScan)
		makeScanMsg();
	else if (bpp->ridCount == 0) {   // this would cause a scan
		blockCount += colType.colWidth;
		return;  // a step with no input rids does nothing
	}
	else
		makeStepMsg();
	issuePrimitive();
	processResult();

	// check if feeding a filtercommand
	if (fFilterFeeder != NOT_FEEDER)
		copyRidsForFilterCmd();
}

void ColumnCommand::execute()
{
	if (fFilterFeeder == LEFT_FEEDER)
		values = bpp->fFiltCmdValues[0].get();
	else if (fFilterFeeder == RIGHT_FEEDER)
		values = bpp->fFiltCmdValues[1].get();
	else
		values = bpp->values;
	_execute();
}

void ColumnCommand::execute(int64_t *vals) 
{
	values = vals;
	_execute();
}

void ColumnCommand::makeScanMsg()
{
	/* Finish the NewColRequestHeader. */

	/* XXXPAT: if there is a Command preceeding this one, it's a DictScan feeding tokens
	which need to become filter elements.  Can we handle that with this design?
	Implement that later. */

	primMsg->ism.Size = baseMsgLength;
	primMsg->NVALS = 0;
	primMsg->LBID = lbid;
	primMsg->RidFlags = 0xFF;

// 	cout << "scanning lbid " << lbid << " colwidth = " << primMsg->DataSize << 
// 		" filterCount = " << filterCount << " outputType = " << 
// 		(int) primMsg->OutputType << endl;
}

void ColumnCommand::makeStepMsg()
{
	memcpy(&inputMsg[baseMsgLength], bpp->relRids, bpp->ridCount << 1);
	primMsg->RidFlags = bpp->ridMap;
	primMsg->ism.Size = baseMsgLength +  (bpp->ridCount << 1);
	primMsg->NVALS = bpp->ridCount;
	primMsg->LBID = lbid;
// 	cout << "lbid is " << lbid << endl;
}

void ColumnCommand::issuePrimitive()
{
	int i;
	
	bool wasVersioned;
	uint wasCached;
	uint32_t blocksRead;
	uint32_t resultSize;
	uint8_t _mask;
	uint64_t oidLastLbid=0;
	bool lastBlockReached=false;
	oidLastLbid = getLastLbid();
	uint blocksToLoad = 0;
	BRM::LBID_t *lbids = (BRM::LBID_t *) alloca(8 * sizeof(BRM::LBID_t));
	uint8_t **blockPtrs = (uint8_t **) alloca(8 * sizeof(uint8_t *));	

	_mask = mask;
// 	primMsg->RidFlags = 0xff;   // disables selective block loading
	//cout <<__FILE__ << "::issuePrimitive() o: " << getOID() << " l:" << primMsg->LBID << " ll: " << oidLastLbid << endl;
	
	for (i=0; i < colType.colWidth; ++i, _mask <<= shift) {

		if ((!lastBlockReached && _isScan) || (!_isScan && primMsg->RidFlags & _mask)) {
			lbids[blocksToLoad] = primMsg->LBID + i;
			blockPtrs[blocksToLoad] = &bpp->blockData[i * BLOCK_SIZE];
			blocksToLoad++;
			loadCount++;
		}
		else if (lastBlockReached && _isScan)
		{ // fill remaining blocks with empty values when col scan
			int blockLen = BLOCK_SIZE/colType.colWidth;
		 	ByteStream::octbyte* oPtr=NULL;
		 	ByteStream::quadbyte* qPtr=NULL;
		 	ByteStream::byte* bPtr=NULL;
		 	ByteStream::doublebyte* dPtr=NULL;
			if (colType.colWidth==1)
				bPtr = reinterpret_cast<ByteStream::byte*>(&bpp->blockData[i*BLOCK_SIZE]);
			//@Bug 1812. Added two bytes column handling
			if (colType.colWidth==2)
				dPtr = reinterpret_cast<ByteStream::doublebyte*>(&bpp->blockData[i*BLOCK_SIZE]);
			if (colType.colWidth==4)
				qPtr = reinterpret_cast<ByteStream::quadbyte*>(&bpp->blockData[i*BLOCK_SIZE]);
			if (colType.colWidth==8)
				oPtr = reinterpret_cast<ByteStream::octbyte*>(&bpp->blockData[i*BLOCK_SIZE]);

			for (int idx=0; idx < blockLen; idx++)
			{
				if (bPtr && colType.colWidth==1) {
					ByteStream::byte b = getEmptyRowValue(colType.colDataType, colType.colWidth);
					bPtr[idx] = b;
				}
				//@Bug 1812. Added two bytes column handling
				else if (dPtr && colType.colWidth==2) {
					ByteStream::doublebyte d = getEmptyRowValue(colType.colDataType, colType.colWidth);
					dPtr[idx] = d;
				}
				else if (qPtr && colType.colWidth==4) {
					ByteStream::quadbyte q = getEmptyRowValue(colType.colDataType, colType.colWidth);
					qPtr[idx] = q;
				}
				else if (oPtr && colType.colWidth==8) {
					ByteStream::octbyte o = getEmptyRowValue(colType.colDataType, colType.colWidth);
					oPtr[idx] = o;
				}
			}
				
		}// else

		if ( (primMsg->LBID+i)==oidLastLbid)
			lastBlockReached=true;
		blockCount++;
	} // for
	
	/* Do the load */
	wasCached = primitiveprocessor::loadBlocks(lbids, 
									bpp->versionInfo,
									bpp->txnID,
									colType.compressionType,
									blockPtrs, 
									&blocksRead,
									bpp->LBIDTrace, 
									bpp->sessionID,
									blocksToLoad,
									&wasVersioned,
									willPrefetch(),
									&bpp->vssCache);
	bpp->cachedIO += wasCached;
	bpp->physIO += blocksRead;
	bpp->touchedBlocks += blocksToLoad;

// 	cout << "issuing primitive for LBID " << primMsg->LBID << endl;
	if (!suppressFilter)
		bpp->pp.setParsedColumnFilter(parsedColumnFilter);
	else
		bpp->pp.setParsedColumnFilter(emptyFilter);
	bpp->pp.p_Col(primMsg, outMsg, bpp->outMsgSize, (unsigned int*)&resultSize, fUdfFuncPtr);

	/* Update CP data */
	if (_isScan) {
		bpp->validCPData = (outMsg->ValidMinMax && !wasVersioned);
		//if (wasVersioned && outMsg->ValidMinMax)
		//	cout << "CC: versioning overriding min max data\n";
		bpp->lbidForCP = lbid;
		bpp->maxVal = outMsg->Max;
		bpp->minVal = outMsg->Min;
	}

} // issuePrimitive()

void ColumnCommand::process_OT_BOTH()
{
	uint64_t i, pos;

	bpp->ridCount = outMsg->NVALS;
	bpp->ridMap = outMsg->RidFlags;
// 	cout << "rid Count is " << bpp->ridCount << endl;

	/* this is verbose and repetative to minimize the work per row */
	switch(colType.colWidth) {
		case 8:
			for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i) {
				if (makeAbsRids)
					bpp->absRids[i] = *((uint16_t *) &bpp->outputMsg[pos]) + bpp->baseRid;
				bpp->relRids[i] = *((uint16_t *) &bpp->outputMsg[pos]);
				pos += 2;
				values[i] = *((int64_t *) &bpp->outputMsg[pos]); pos += 8;
			}
			break;
		case 4:
			for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i) {
				if (makeAbsRids)
					bpp->absRids[i] = *((uint16_t *) &bpp->outputMsg[pos]) + bpp->baseRid;
				bpp->relRids[i] = *((uint16_t *) &bpp->outputMsg[pos]);
				pos += 2;
				values[i] = *((int32_t *) &bpp->outputMsg[pos]); pos += 4;
			}
			break;
		case 2:
			for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i) {
				if (makeAbsRids)
					bpp->absRids[i] = *((uint16_t *) &bpp->outputMsg[pos]) + bpp->baseRid;
				bpp->relRids[i] = *((uint16_t *) &bpp->outputMsg[pos]);
				pos += 2;
				values[i] = *((int16_t *) &bpp->outputMsg[pos]); pos += 2;
			}
			break;
		case 1:
			for (i = 0, pos = sizeof(NewColResultHeader); i < outMsg->NVALS; ++i) {
				if (makeAbsRids)
					bpp->absRids[i] = *((uint16_t *) &bpp->outputMsg[pos]) + bpp->baseRid;
				bpp->relRids[i] = *((uint16_t *) &bpp->outputMsg[pos]);
				pos += 2;
				values[i] = *((int8_t *) &bpp->outputMsg[pos++]);
			}
			break;
	}

}

void ColumnCommand::process_OT_RID()
{
	memcpy(bpp->relRids, outMsg + 1, outMsg->NVALS << 1);
	bpp->ridCount = outMsg->NVALS;
	bpp->ridMap = outMsg->RidFlags;
// 	cout << "rid Count is " << bpp->ridCount << endl;
}

void ColumnCommand::process_OT_DATAVALUE()
{
	bpp->ridCount = outMsg->NVALS;
// 	cout << "rid Count is " << bpp->ridCount << endl;
	switch(colType.colWidth) {
		case 8:
		{
			memcpy(values, outMsg + 1, outMsg->NVALS << 3);
// 			cout << "  CC: first value is " << values[0] << endl; 
			break;
		}
		case 4:
		{
			int32_t* arr32 = (int32_t *) (outMsg + 1);
			for (uint64_t i = 0; i < outMsg->NVALS; ++i)
				values[i] = arr32[i];
			break;
		}
		case 2:
		{
			int16_t* arr16 = (int16_t *) (outMsg + 1);
			for (uint64_t i = 0; i < outMsg->NVALS; ++i)
				values[i] = arr16[i];
			break;
		}
		case 1:
		{
			int8_t* arr8 = (int8_t *) (outMsg + 1);
			for (uint64_t i = 0; i < outMsg->NVALS; ++i)
				values[i] = arr8[i];
			break;
		}
	}
}

void ColumnCommand::processResult()
{
	/* Switch on output type, turn pCol output into something useful, store it in
	   the containing BPP */

// 	if (filterCount == 0 && !_isScan)
// 		idbassert(outMsg->NVALS == bpp->ridCount);

	switch (outMsg->OutputType) {
		case OT_BOTH: process_OT_BOTH(); break;
		case OT_RID: process_OT_RID(); break;
		case OT_DATAVALUE: process_OT_DATAVALUE(); break;
		default:
			cout << "outputType = " << outMsg->OutputType << endl;
			throw logic_error("ColumnCommand got a bad OutputType");
	}

	// check if feeding a filtercommand
	if (fFilterFeeder == LEFT_FEEDER)
	{
		bpp->fFiltRidCount[0] = bpp->ridCount;
		for (uint64_t i = 0; i < bpp->ridCount; i++)
			bpp->fFiltCmdRids[0][i] = bpp->relRids[i];
	}
	else if (fFilterFeeder == RIGHT_FEEDER)
	{
		bpp->fFiltRidCount[1] = bpp->ridCount;
		for (uint64_t i = 0; i < bpp->ridCount; i++)
			bpp->fFiltCmdRids[1][i] = bpp->relRids[i];
	}
// 	cout << "processed " << outMsg->NVALS << " rows" << endl;
}

void ColumnCommand::createCommand(ByteStream &bs)
{
	uint8_t tmp8;

	bs >> tmp8;  // eat the Command
	bs >> tmp8;
	_isScan = tmp8;
	bs >> traceFlags;
	bs >> filterString;
#if 0
	cout << "filter string: ";
	for (uint i = 0; i < filterString.length(); ++i)
		cout << (int) filterString.buf()[i] << " ";
	cout << endl;
#endif
	bs >> tmp8;
	colType.colDataType = (execplan::CalpontSystemCatalog::ColDataType) tmp8;
	bs >> tmp8;
	colType.colWidth = tmp8;
	bs >> tmp8;
	colType.scale = tmp8;
	bs >> tmp8;
	colType.compressionType = tmp8;
	bs >> BOP;
	bs >> filterCount;
	bs >> tmp8;  // UDF ordinal number
	fUdfFuncPtr = ord2Fptr(tmp8);
	if (tmp8 != 0 && fUdfFuncPtr == 0) {
		Message::Args args;
		args.add(tmp8);
		mlp->logMessage(logging::M0069, args);
	}
	deserializeInlineVector(bs, lastLbid);
//	cout << "lastLbid count=" << lastLbid.size() << endl;
//	for (uint i = 0; i < lastLbid.size(); i++)
//		cout << "  " << lastLbid[i];


	//cout << "CreateCommand() o:" << getOID() << " lastLbid: " << lastLbid << endl;
	Command::createCommand(bs);

	parsedColumnFilter = bpp->pp.parseColumnFilter(filterString.buf(), colType.colWidth,
		colType.colDataType, filterCount, BOP);

	/* OR hack */
	emptyFilter = bpp->pp.parseColumnFilter(filterString.buf(), colType.colWidth,
		colType.colDataType, 0, BOP);

	/* XXXPAT: for debugging only */
// 	bs >> colType.columnOID;
// 	cout << "got filterCount " << filterCount << endl;
// 	cout << "made a ColumnCommand OID = " << OID << endl;
}

void ColumnCommand::resetCommand(ByteStream &bs)
{
	bs >> lbid;
}

void ColumnCommand::prep(int8_t outputType, bool absRids)
{
	/* make the template NewColRequestHeader */

	baseMsgLength = sizeof(NewColRequestHeader) +
		  (suppressFilter ? 0 : filterString.length());

	if (!inputMsg)
		inputMsg.reset(new uint8_t[baseMsgLength + (LOGICAL_BLOCK_RIDS * 2)]);
	primMsg = (NewColRequestHeader *) inputMsg.get();
	outMsg = (NewColResultHeader *) bpp->outputMsg.get();
	makeAbsRids = absRids;

	primMsg->ism.Interleave = 0;
	primMsg->ism.Flags = 0;
// 	primMsg->ism.Flags = PrimitiveMsg::planFlagsToPrimFlags(traceFlags);	
	primMsg->ism.Command=COL_BY_SCAN;
	primMsg->ism.Size = sizeof(NewColRequestHeader) + (suppressFilter ? 0 : filterString.length());
	primMsg->ism.Type = 2;
	primMsg->hdr.SessionID = bpp->sessionID;
	//primMsg->hdr.StatementID = 0;
	primMsg->hdr.TransactionID = bpp->txnID;
	primMsg->hdr.VerID = bpp->versionInfo.currentScn;
	primMsg->hdr.StepID = bpp->stepID;
	primMsg->DataSize = colType.colWidth;
	primMsg->DataType = colType.colDataType;
	primMsg->CompType = colType.compressionType;
 	primMsg->OutputType = outputType;
	primMsg->BOP = BOP;
	primMsg->NOPS = (suppressFilter ? 0 : filterCount);
	primMsg->sort = 0;
#if 0
 	cout << "filter length is " << filterString.length() << endl;

	cout << "appending filter string: ";
	for (uint i = 0; i < filterString.length(); ++i)
		cout << (int) filterString.buf()[i] << " ";
	cout << endl;
#endif
//  made unnecessary by parsedColumnFilter, just leave empty space as if the filter
//  were still there.
// 	memcpy(primMsg + 1, filterString.buf(), filterString.length());



	switch (colType.colWidth) {
		case 1: shift = 8; mask = 0xFF; break;
		case 2: shift = 4; mask = 0x0F; break;
		case 4: shift = 2; mask = 0x03; break;
		case 8: shift = 1; mask = 0x01; break;
		default:
			cout << "CC: colWidth is " << colType.colWidth << endl;
			throw logic_error("ColumnCommand: bad column width?");
	}

	/* TODO: re-optimize with parameterized output RG */
// 	if (bpp->ot == ROW_GROUP) {
// 		bpp->outputRG.initRow(&r);
// 		rowSize = r.getSize();
// 	}

}

/* Assumes OT_DATAVALUE */
void ColumnCommand::projectResult()
{
	if (primMsg->NVALS != outMsg->NVALS || outMsg->NVALS != bpp->ridCount )
	{
		ostringstream os;
		BRM::DBRM brm;
		BRM::OID_t oid;
		uint16_t l_dbroot;
		uint32_t partNum;
		uint16_t segNum;
		uint32_t fbo;
		brm.lookupLocal(lbid, 0, false, oid, l_dbroot, partNum, segNum, fbo);
		
		os << __FILE__ << " error on projection for oid " << oid << " lbid " << lbid;
		if (primMsg->NVALS != outMsg->NVALS )
			os << ": input rids " << primMsg->NVALS;
		else
			os << ": ridcount " << bpp->ridCount;
		os << ", output rids " << outMsg->NVALS << endl;
		//cout << os.str();
		if (bpp->sessionID & 0x80000000)
			throw NeedToRestartJob(os.str());
		else
			throw PrimitiveColumnProjectResultExcept(os.str());
	}
	idbassert(primMsg->NVALS == outMsg->NVALS);
	idbassert(outMsg->NVALS == bpp->ridCount);
	*bpp->serialized << (uint32_t) (outMsg->NVALS * colType.colWidth);
	bpp->serialized->append((uint8_t *) (outMsg + 1), outMsg->NVALS * colType.colWidth);
}

void ColumnCommand::projectResultRG(RowGroup &rg, uint pos)
{
	uint i, offset;

	/* TODO: reoptimize these away */
	rg.initRow(&r);
	offset = r.getOffset(pos);
	rowSize = r.getSize();

	if (primMsg->NVALS != outMsg->NVALS || outMsg->NVALS != bpp->ridCount)
	{
		ostringstream os;
		BRM::DBRM brm;
		BRM::OID_t oid;
		uint16_t dbroot;
		uint32_t partNum;
		uint16_t segNum;
		uint32_t fbo;
		brm.lookupLocal(lbid, 0, false, oid, dbroot, partNum, segNum, fbo);
		
		os << __FILE__ << " error on projectResultRG for oid " << oid << " lbid " << lbid;
		if (primMsg->NVALS != outMsg->NVALS )
			os << ": input rids " << primMsg->NVALS;
		else
			os << ": ridcount " << bpp->ridCount;
		os << ",  output rids " << outMsg->NVALS;
/*
		BRM::VSSData entry;
		if (bpp->vssCache.find(lbid) != bpp->vssCache.end()) {
			entry = bpp->vssCache[lbid];
			if (entry.returnCode == 0)
				os << "  requested version " << entry.verID;
		}
*/
		os << endl;
		if (bpp->sessionID & 0x80000000)
			throw NeedToRestartJob(os.str());
		else
			throw PrimitiveColumnProjectResultExcept(os.str());
	}

	idbassert(primMsg->NVALS == outMsg->NVALS);
	idbassert(outMsg->NVALS == bpp->ridCount);
	rg.getRow(0, &r);
	switch (colType.colWidth) {
		case 1: {
			uint8_t *val8 = (uint8_t *) (outMsg + 1);
			for (i = 0; i < outMsg->NVALS; ++i) {
				r.setUintField_offset<1>(val8[i], offset);
				r.nextRow(rowSize);
			}
			break;
		}
		case 2: {
			uint16_t *val16 = (uint16_t *) (outMsg + 1);
			for (i = 0; i < outMsg->NVALS; ++i) {
				r.setUintField_offset<2>(val16[i], offset);
				r.nextRow(rowSize);
			}
			break;
		}
		case 4: {
			uint32_t *val32 = (uint32_t *) (outMsg + 1);
			for (i = 0; i < outMsg->NVALS; ++i) {
// 				if (OID > 3000)
// 					cout << "column primitive for oid " << OID << " inserting " << val32[i] << endl;
				r.setUintField_offset<4>(val32[i], offset);
				r.nextRow(rowSize);
			}
			break;
		}
		case 8: {
			uint64_t *val64 = (uint64_t *) (outMsg + 1);
			for (i = 0; i < outMsg->NVALS; ++i) {
				r.setUintField_offset<8>(val64[i], offset);
				r.nextRow(rowSize);
			}
			break;
		}
	}
}

void ColumnCommand::project()
{
	/* bpp->ridCount == 0 would signify a scan operation */
	if (bpp->ridCount == 0) {
		*bpp->serialized << (uint32_t) 0;
		blockCount += colType.colWidth;
		return;
	}
	makeStepMsg();
	issuePrimitive();
	projectResult();
}

void ColumnCommand::projectIntoRowGroup(RowGroup &rg, uint pos)
{
	if (bpp->ridCount == 0) {
		blockCount += colType.colWidth;
		return;
	}
	makeStepMsg();
	issuePrimitive();
	projectResultRG(rg, pos);
}

void ColumnCommand::nextLBID()
{
	lbid += colType.colWidth;
}

SCommand ColumnCommand::duplicate()
{
	SCommand ret;
	ColumnCommand *cc;

	ret.reset(new ColumnCommand());
	cc = (ColumnCommand *) ret.get();
	cc->_isScan = _isScan;
	cc->traceFlags = traceFlags;
	cc->filterString = filterString;
	cc->colType.colDataType = colType.colDataType;
	cc->colType.compressionType = colType.compressionType;
	cc->colType.colWidth = colType.colWidth;
	cc->BOP = BOP;
	cc->filterCount = filterCount;
	cc->fFilterFeeder = fFilterFeeder;
	cc->parsedColumnFilter = parsedColumnFilter;
	cc->suppressFilter = suppressFilter;
	cc->lastLbid = lastLbid;
	cc->r = r;
	cc->rowSize = rowSize;
	cc->Command::duplicate(this);
	return ret;
}

bool ColumnCommand::operator==(const ColumnCommand &cc) const
{
	if (_isScan != cc._isScan)
		return false;
	if (BOP != cc.BOP)
		return false;
	if (filterString != cc.filterString)
		return false;
	if (filterCount != cc.filterCount)
		return false;
	if (makeAbsRids != cc.makeAbsRids)
		return false;
	if (colType.colWidth != cc.colType.colWidth)
		return false;
	if (colType.colDataType != cc.colType.colDataType)
		return false;
	return true;
}

bool ColumnCommand::operator!=(const ColumnCommand &cc) const
{
	return !(*this == cc);
}


ColumnCommand & ColumnCommand::operator=(const ColumnCommand &c)
{
	_isScan = c._isScan;
	traceFlags = c.traceFlags;
	filterString = c.filterString;
	colType.colDataType = c.colType.colDataType;
	colType.compressionType = c.colType.compressionType;
	colType.colWidth = c.colType.colWidth;
	BOP = c.BOP;
	filterCount = c.filterCount;
	fFilterFeeder = c.fFilterFeeder;
	parsedColumnFilter = c.parsedColumnFilter;
	suppressFilter = c.suppressFilter;
	lastLbid = c.lastLbid;
	return *this;
}

bool ColumnCommand::willPrefetch()
{
// 	if (blockCount > 0)
// 		cout << "load rate = " << ((double)loadCount)/((double)blockCount) << endl;

//	if (!((double)loadCount)/((double)blockCount) > bpp->prefetchThreshold)
//		cout << "suppressing prefetch\n";

	//return false;
	return (blockCount == 0 || ((double)loadCount)/((double)blockCount) >
		bpp->prefetchThreshold);
}

void ColumnCommand::disableFilters()
{
	suppressFilter = true;
	prep(primMsg->OutputType, makeAbsRids);
}

void ColumnCommand::enableFilters()
{
	suppressFilter = false;
	prep(primMsg->OutputType, makeAbsRids);
}


/***********************************************************
* DESCRIPTION:
*    Get the value that represents empty row
* PARAMETERS:
*    dataType - data type
*    width - data width in byte
* RETURN:
*    emptyVal - the value of empty row
***********************************************************/
const uint64_t ColumnCommand::getEmptyRowValue( const execplan::CalpontSystemCatalog::ColDataType dataType, const int width ) const
{
    uint64_t emptyVal = 0;
    int offset;

    offset = ( dataType == execplan::CalpontSystemCatalog::VARCHAR )? -1 : 0;
    switch ( dataType )
    {
        case execplan::CalpontSystemCatalog::TINYINT : emptyVal = joblist::TINYINTEMPTYROW; break;
        case execplan::CalpontSystemCatalog::SMALLINT: emptyVal = joblist::SMALLINTEMPTYROW; break;
        case execplan::CalpontSystemCatalog::MEDINT :  
        case execplan::CalpontSystemCatalog::INT :     emptyVal = joblist::INTEMPTYROW; break;
        case execplan::CalpontSystemCatalog::BIGINT :  emptyVal = joblist::BIGINTEMPTYROW; break;

        case execplan::CalpontSystemCatalog::UTINYINT : emptyVal = joblist::UTINYINTEMPTYROW; break;
        case execplan::CalpontSystemCatalog::USMALLINT: emptyVal = joblist::USMALLINTEMPTYROW; break;
        case execplan::CalpontSystemCatalog::UMEDINT :  
        case execplan::CalpontSystemCatalog::UINT :     emptyVal = joblist::UINTEMPTYROW; break;
        case execplan::CalpontSystemCatalog::UBIGINT :  emptyVal = joblist::UBIGINTEMPTYROW; break;

        case execplan::CalpontSystemCatalog::FLOAT :   
        case execplan::CalpontSystemCatalog::UFLOAT :   emptyVal = joblist::FLOATEMPTYROW; break;
        case execplan::CalpontSystemCatalog::DOUBLE :
        case execplan::CalpontSystemCatalog::UDOUBLE :  emptyVal = joblist::DOUBLEEMPTYROW; break;

        case execplan::CalpontSystemCatalog::DECIMAL : 
        case execplan::CalpontSystemCatalog::UDECIMAL : 
            if ( width <= 1 )
                emptyVal = joblist::TINYINTEMPTYROW;
            else if (width <= 2)
                emptyVal = joblist::SMALLINTEMPTYROW;
            else if ( width <= 4 )
                emptyVal = joblist::INTEMPTYROW;
            else
                emptyVal = joblist::BIGINTEMPTYROW;
            break;

        case execplan::CalpontSystemCatalog::CHAR : 
        case execplan::CalpontSystemCatalog::VARCHAR : 
        case execplan::CalpontSystemCatalog::DATE :
        case execplan::CalpontSystemCatalog::DATETIME :
        case execplan::CalpontSystemCatalog::VARBINARY : 
        default:
            emptyVal = joblist::CHAR1EMPTYROW;
            if ( width == (2 + offset) )
                emptyVal = joblist::CHAR2EMPTYROW;
            else
                if ( width >= (3 + offset) && width <= ( 4 + offset ) )
                emptyVal = joblist::CHAR4EMPTYROW;
            else
                if ( width >= (5 + offset)  )
                emptyVal = joblist::CHAR8EMPTYROW;
            break;
    }

    return emptyVal;
}

void ColumnCommand::getLBIDList(uint loopCount, vector<int64_t> *lbids)
{
	int64_t firstLBID = lbid, lastLBID = firstLBID + (loopCount * colType.colWidth) - 1, i;
	
	for (i = firstLBID; i <= lastLBID; i++)
		lbids->push_back(i);
}

const int64_t ColumnCommand::getLastLbid()
{
	if (!_isScan)
		return 0;
	return lastLbid[bpp->dbRoot-1];

#if 0
	/* PL - each dbroot has a different HWM; need to look up the local HWM on start */
	BRM::DBRM dbrm;
	BRM::OID_t oid;
	uint32_t partNum;
	uint16_t segNum;
	uint32_t fbo;

	dbrm.lookupLocal(primMsg->LBID, bpp->versionNum, false, oid, dbRoot, partNum, segNum, fbo);
	gotDBRoot = true;
	cout << "I think I'm on dbroot " << dbRoot << " lbid=" << primMsg->LBID << " ver=" << bpp->versionNum << endl;
	dbRoot--;
	return lastLbid[dbRoot];
#endif
}

}
// vim:ts=4 sw=4:

