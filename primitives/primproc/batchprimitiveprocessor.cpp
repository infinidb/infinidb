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

//
// $Id: batchprimitiveprocessor.cpp 2136 2013-07-24 21:04:30Z pleblanc $
// C++ Implementation: batchprimitiveprocessor
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <stdexcept>
#include <unistd.h>
#include <cstring>
//#define NDEBUG
#include <cassert>
#include <string>
#include <sstream>
#include <set>
using namespace std;

#include <boost/thread.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "bpp.h"
#include "primitiveserver.h"
#include "errorcodes.h"
#include "exceptclasses.h"
#include "pp_logger.h"
#include "funcexpwrapper.h"
#include "fixedallocator.h"
#include "blockcacheclient.h"
#include "MonitorProcMem.h"

#define MAX64 0x7fffffffffffffffLL
#define MIN64 0x8000000000000000LL

using namespace messageqcpp;
using namespace joiner;
using namespace std::tr1;
using namespace rowgroup;
using namespace funcexp;
using namespace logging;
using namespace utils;
using namespace joblist;

namespace primitiveprocessor
{
// these are config parms defined in primitiveserver.cpp, initialized by PrimProc main().
extern uint blocksReadAhead;
extern uint dictBufferSize;
extern uint defaultBufferSize;
extern int fCacheCount;
extern uint connectionsPerUM;
extern int noVB;

BatchPrimitiveProcessor::BatchPrimitiveProcessor() :
	ot(BPS_ELEMENT_TYPE),
	txnID(0),
	sessionID(0),
	stepID(0),
	uniqueID(0),
	count(1),
	baseRid(0),
	ridCount(0),
	needStrValues(false),
	filterCount(0),
	projectCount(0),
	sendRidsAtDelivery(false),
	ridMap(0),
	gotAbsRids(false),
	gotValues(false),
	hasScan(false),
	validCPData(false),
	minVal(MAX64),
	maxVal(MIN64),
	lbidForCP(0),
	busyLoaderCount(0),
	physIO(0),
	cachedIO(0),
	touchedBlocks(0),
	LBIDTrace(false),
	fBusy(false),
	doJoin(false),
	hasFilterStep(false),
	filtOnString(false),
	prefetchThreshold(0),
	hasDictStep(false),
	sockIndex(0),
	endOfJoinerRan(false)
{
	pp.setLogicalBlockMode(true);
	pp.setBlockPtr((int *) blockData);
}

BatchPrimitiveProcessor::BatchPrimitiveProcessor(ByteStream &b, double prefetch,
	boost::shared_ptr<BPPSendThread> bppst) :
	ot(BPS_ELEMENT_TYPE),
	txnID(0),
	sessionID(0),
	stepID(0),
	uniqueID(0),
	count(1),
	baseRid(0),
	ridCount(0),
	needStrValues(false),
	filterCount(0),
	projectCount(0),
	sendRidsAtDelivery(false),
	ridMap(0),
	gotAbsRids(false),
	gotValues(false),
	hasScan(false),
	validCPData(false),
	minVal(MAX64),
	maxVal(MIN64),
	lbidForCP(0),
	busyLoaderCount(0),
	physIO(0),
	cachedIO(0),
	touchedBlocks(0),
	LBIDTrace(false),
	fBusy(false),
	doJoin(false),
	hasFilterStep(false),
	filtOnString(false),
	prefetchThreshold(prefetch),
	hasDictStep(false),
	sockIndex(0),
	endOfJoinerRan(false)
{
	pp.setLogicalBlockMode(true);
	pp.setBlockPtr((int *) blockData);
	sendThread = bppst;
	initBPP(b);
// 	cerr << "made a BPP\n";
}

#if 0
BatchPrimitiveProcessor::BatchPrimitiveProcessor(const BatchPrimitiveProcessor &bpp)
{
	throw logic_error("copy BPP deprecated");
}
#endif

BatchPrimitiveProcessor::~BatchPrimitiveProcessor()
{
	//FIXME: just do a sync fetch
	counterLock.lock(); // need to make sure the loader has exited
	while (busyLoaderCount > 0)
	{
		counterLock.unlock();
		usleep(100000);
		counterLock.lock();
	}
	counterLock.unlock();
}

/**
 * InitBPP Parses the creation messages from BatchPrimitiveProcessor-JL::createBPP()
 * Refer to that fcn for message format info.
 */

void BatchPrimitiveProcessor::initBPP(ByteStream &bs)
{
	uint i;
	uint8_t tmp8;
	Command::CommandType type;

	bs.advance(sizeof(ISMPacketHeader));  // skip the header
	bs >> tmp8;
	ot = static_cast<BPSOutputType>(tmp8);
	bs >> txnID;
	bs >> sessionID;
	bs >> stepID;
	bs >> uniqueID;
	bs >> versionInfo;

	bs >> tmp8;
	needStrValues = tmp8 & NEED_STR_VALUES;
	gotAbsRids = tmp8 & GOT_ABS_RIDS;
	gotValues = tmp8 & GOT_VALUES;
	LBIDTrace = tmp8 & LBID_TRACE;
	sendRidsAtDelivery = tmp8 & SEND_RIDS_AT_DELIVERY;
	doJoin = tmp8 & HAS_JOINER;
	hasRowGroup = tmp8 & HAS_ROWGROUP;
	getTupleJoinRowGroupData = tmp8 & JOIN_ROWGROUP_DATA;

	// This used to signify that there was input row data from previous jobsteps, and
	// it never quite worked right. No need to fix it or update it; all BPP's have started
	// with a scan for years.  Took it out.
	assert(!hasRowGroup);

	bs >> bop;
	bs >> forHJ;

	if (ot == ROW_GROUP) {
		bs >> outputRG;
		//outputRG.setUseStringTable(true);
		bs >> tmp8;
		if (tmp8) {
			fe1.reset(new FuncExpWrapper());
			bs >> *fe1;
			bs >> fe1Input;
		}
		bs >> tmp8;
		if (tmp8) {
			fe2.reset(new FuncExpWrapper());
			bs >> *fe2;
			bs >> fe2Output;
		}
	}

	if (doJoin) {
		objLock.lock();
		if (ot == ROW_GROUP) {
			bs >> joinerCount;
// 			cout << "joinerCount = " << joinerCount << endl;
			joinTypes.reset(new JoinType[joinerCount]);
			tJoiners.reset(new boost::shared_ptr<TJoiner>[joinerCount]);
			_pools.reset(new boost::shared_ptr<utils::SimplePool>[joinerCount]);
			tlJoiners.reset(new boost::shared_ptr<TLJoiner>[joinerCount]);
			tJoinerSizes.reset(new uint[joinerCount]);
			largeSideKeyColumns.reset(new uint[joinerCount]);
			tlLargeSideKeyColumns.reset(new vector<uint>[joinerCount]);
			typelessJoin.reset(new bool[joinerCount]);
			tlKeyLengths.reset(new uint[joinerCount]);
			storedKeyAllocators.reset(new PoolAllocator[joinerCount]);
			joinNullValues.reset(new uint64_t[joinerCount]);
			doMatchNulls.reset(new bool[joinerCount]);
			joinFEFilters.reset(new scoped_ptr<FuncExpWrapper>[joinerCount]);
			hasJoinFEFilters = false;
			hasSmallOuterJoin = false;
			for (i = 0; i < joinerCount; i++) {
				doMatchNulls[i] = false;
				bs >> tJoinerSizes[i];
 				//cout << "joiner size = " << tJoinerSizes[i] << endl;
				bs >> joinTypes[i];
				bs >> tmp8;
				typelessJoin[i] = (bool) tmp8;
				if (joinTypes[i] & WITHFCNEXP) {
					hasJoinFEFilters = true;
					joinFEFilters[i].reset(new FuncExpWrapper());
					bs >> *joinFEFilters[i];
				}
				if (joinTypes[i] & SMALLOUTER)
					hasSmallOuterJoin = true;
				if (!typelessJoin[i]) {
					bs >> joinNullValues[i];
					bs >> largeSideKeyColumns[i];
 					//cout << "large side key is " << largeSideKeyColumns[i] << endl;
					_pools[i].reset(new utils::SimplePool());
					utils::SimpleAllocator<pair<uint64_t const, uint32_t> > alloc(_pools[i]);
					tJoiners[i].reset(new TJoiner(10, TupleJoiner::hasher(), equal_to<uint64_t>(), alloc));
				}
				else {
					deserializeVector<uint>(bs, tlLargeSideKeyColumns[i]);
					bs >> tlKeyLengths[i];
					//storedKeyAllocators[i] = PoolAllocator();
					tlJoiners[i].reset(new TLJoiner(10, TupleJoiner::hasher()));
				}
			}
			if (hasJoinFEFilters) {
				joinFERG.reset(new RowGroup());
				bs >> *joinFERG;
			}
			if (getTupleJoinRowGroupData) {
				deserializeVector(bs, smallSideRGs);
// 				cout << "deserialized " << smallSideRGs.size() << " small-side rowgroups\n";
				idbassert(smallSideRGs.size() == joinerCount);
				smallSideRowLengths.reset(new uint[joinerCount]);
				smallSideRowData.reset(new RGData[joinerCount]);
				smallNullRowData.reset(new RGData[joinerCount]);
				smallNullPointers.reset(new Row::Pointer[joinerCount]);
				ssrdPos.reset(new uint64_t[joinerCount]);
				for (i = 0; i < joinerCount; i++) {
					smallSideRowLengths[i] = smallSideRGs[i].getRowSize();;
					smallSideRowData[i] = RGData(smallSideRGs[i], tJoinerSizes[i]);
//					smallSideRowData[i].reset(new uint8_t[
//					  smallSideRGs[i].getEmptySize() +
//					  (uint64_t) smallSideRowLengths[i] * tJoinerSizes[i]]);
					smallSideRGs[i].setData(&smallSideRowData[i]);
					smallSideRGs[i].resetRowGroup(0);
					ssrdPos[i] = smallSideRGs[i].getEmptySize();
					if (joinTypes[i] & (LARGEOUTER | SEMI | ANTI)) {
						Row smallRow;
						smallSideRGs[i].initRow(&smallRow);
						smallNullRowData[i] = RGData(smallSideRGs[i], 1);
						smallSideRGs[i].setData(&smallNullRowData[i]);
						smallSideRGs[i].getRow(0, &smallRow);
						smallRow.initToNull();
						smallNullPointers[i] = smallRow.getPointer();
						smallSideRGs[i].setData(&smallSideRowData[i]);
					}
				}
				bs >> largeSideRG;
				bs >> joinedRG;
// 				cout << "got the joined Rowgroup: " << joinedRG.toString() << "\n";
			}
		}
		else {
			bs >> tmp8;
			bs >> joinerSize;
			joiner.reset(new Joiner((bool) tmp8));
		}
#ifdef __FreeBSD__
		objLock.unlock();
#endif
	}

	bs >> filterCount;
	filterSteps.resize(filterCount);
//	cout << "deserializing " << filterCount << " filters\n";
	hasScan = false;
	hasPassThru = false;
	for (i = 0; i < filterCount; ++i) {
// 		cout << "deserializing step " << i << endl;
		filterSteps[i] = SCommand(Command::makeCommand(bs, &type, filterSteps));
		if (type == Command::COLUMN_COMMAND) {
			ColumnCommand *col = (ColumnCommand *) filterSteps[i].get();
			if (col->isScan())
				hasScan = true;
			if (bop == BOP_OR)
				col->setScan(true);
		}
		else if (type == Command::FILTER_COMMAND) {
			hasFilterStep = true;
			if (dynamic_cast<StrFilterCmd*>(filterSteps[i].get()) != NULL)
				filtOnString = true;
		}
		else if (type == Command::DICT_STEP || type == Command::RID_TO_STRING)
			hasDictStep = true;
	}
	bs >> projectCount;
//	cout << "deserializing " << projectCount << " projected columns\n\n";
	projectSteps.resize(projectCount);
	for (i = 0; i < projectCount; ++i) {
		projectSteps[i] = SCommand(Command::makeCommand(bs, &type, projectSteps));
		if (type == Command::PASS_THRU)
			hasPassThru = true;
		else if (type == Command::DICT_STEP || type == Command::RID_TO_STRING)
			hasDictStep = true;
	}

	if (ot == ROW_GROUP)
	{
		bs >> tmp8;
		if (tmp8 > 0)
		{
			bs >> fAggregateRG;
			fAggregator.reset(new RowAggregation);
// 			cout << "Made an aggregator\n";
			bs >> *(fAggregator.get());
		}
	}

	initProcessor();
}

/**
 * resetBPP Parses the run messages from BatchPrimitiveProcessor-JL::runBPP()
 * Refer to that fcn for message format info.
 */
void BatchPrimitiveProcessor::resetBPP(ByteStream &bs, const SP_UM_MUTEX& w,
	const SP_UM_IOSOCK& s)
{
	uint i;
	vector<uint64_t> preloads;

	objLock.lock();

	writelock = w;
	sock = s;
	newConnection = true;

	// skip the header, sessionID, stepID, uniqueID, and priority
	bs.advance(sizeof(ISMPacketHeader) + 16);
	bs >> dbRoot;
	bs >> count;
	bs >> ridCount;

	if (gotAbsRids) {
		assert(0);
		memcpy(absRids.get(), bs.buf(), ridCount << 3);
		bs.advance(ridCount << 3);
		/* TODO: this loop isn't always necessary or sensible */
		ridMap = 0;
		baseRid = absRids[0] & 0xffffffffffffe000ULL;
		for (uint i = 0; i < ridCount; i++) {
			relRids[i] = absRids[i] - baseRid;
			ridMap |= 1 << (relRids[i] >> 10);
		}
	}
	else {
		bs >> ridMap;
		bs >> baseRid;
		memcpy(relRids, bs.buf(), ridCount << 1);
		bs.advance(ridCount << 1);
	}

	if (gotValues) {
		memcpy(values, bs.buf(), ridCount << 3);
		bs.advance(ridCount << 3);
	}
	for (i = 0; i < filterCount; ++i) {
		filterSteps[i]->resetCommand(bs);
	}
	for (i = 0; i < projectCount; ++i) {
		projectSteps[i]->resetCommand(bs);
	}

	idbassert(bs.length() == 0);

	/* init vars not part of the BS */
	currentBlockOffset = 0;
	memset(relLBID.get(), 0, sizeof(uint64_t) * (projectCount + 1));
	memset(asyncLoaded.get(), 0, sizeof(bool) * (projectCount + 1));

	buildVSSCache(count);
#ifdef __FreeBSD__
	objLock.unlock();
#endif
}

void BatchPrimitiveProcessor::addToJoiner(ByteStream &bs)
{
	uint32_t count, i, joinerNum, tlIndex, startPos;
	joblist::ElementType *et;
	TypelessData tlLargeKey;
	uint8_t nullFlag;

#pragma pack(push,1)
	struct JoinerElements {
		uint64_t key;
		uint32_t value;
	} *arr;
#pragma pack(pop)

	addToJoinerLock.lock();
	/* skip the header */
	bs.advance(sizeof(ISMPacketHeader) + 3*sizeof(uint32_t));

	bs >> count;
	bs >> startPos;
	if (ot == ROW_GROUP) {
		bs >> joinerNum;
		idbassert(joinerNum < joinerCount);
		arr = (JoinerElements *) bs.buf();
// 		cout << "reading " << count << " elements from the bs, joinerNum is " << joinerNum << "\n";
		for (i = 0; i < count; i++) {
			if (typelessJoin[joinerNum]) {
				bs >> nullFlag;
				if (nullFlag == 0) {
					tlLargeKey.deserialize(bs, storedKeyAllocators[joinerNum]);
					bs >> tlIndex;
					tlJoiners[joinerNum]->insert(pair<TypelessData, uint32_t>(tlLargeKey,
					  tlIndex));
				}
				else
					tJoinerSizes[joinerNum]--;
			}
			else {
				/* A minor optimization: the matchnull logic should only be used with
				 * the jointype specifies it and there's a null value in the small side */
				if (arr[i].key == joinNullValues[joinerNum])
					doMatchNulls[joinerNum] = joinTypes[joinerNum] & MATCHNULLS;
				tJoiners[joinerNum]->insert(pair<const uint64_t, uint32_t>(arr[i].key, arr[i].value));
			}
		}
		if (!typelessJoin[joinerNum])
			bs.advance(count * sizeof(JoinerElements));

		if (getTupleJoinRowGroupData) {
// 			cout << "copying full row data for joiner " << joinerNum << endl;
			/* Need to update this assertion if there's a typeless join.  search
			for nullFlag. */
// 			idbassert(ssrdPos[joinerNum] + (count * smallSideRowLengths[joinerNum]) <=
// 				smallSideRGs[joinerNum].getEmptySize() +
// 				(smallSideRowLengths[joinerNum] * tJoinerSizes[joinerNum]));

			RowGroup &smallSide = smallSideRGs[joinerNum];
			RGData offTheWire;

			// TODO: write an RGData fcn to let it interpret data within a ByteStream to avoid
			// the extra copying.
			offTheWire.deserialize(bs);
			smallSide.setData(&smallSideRowData[joinerNum]);
			smallSide.append(offTheWire, startPos);

			//ssrdPos[joinerNum] += count;

/*  This prints the row data
			smallSideRGs[joinerNum].initRow(&r);
			for (i = 0; i < (tJoinerSizes[joinerNum] * smallSideRowLengths[joinerNum]); i+=r.getSize()) {
				r.setData(&smallSideRowData[joinerNum][i + smallSideRGs[joinerNum].getEmptySize()]);
				cout << " got row: " << r.toString() << endl;
			}
*/
		}
	}
	else {
		et = (joblist::ElementType *) bs.buf();
		for (i = 0; i < count; i++) {
// 			cout << "BPP: adding <" << et[i].first << ", " << et[i].second << "> to Joiner\n";
			joiner->insert(et[i]);
		}
		bs.advance(count << 4);
	}
	idbassert(bs.length() == 0);
	addToJoinerLock.unlock();
}

int BatchPrimitiveProcessor::endOfJoiner()
{
	/* Wait for all joiner elements to be added */
	uint i;

    boost::mutex::scoped_lock scoped(addToJoinerLock);

    if (endOfJoinerRan)
        return 0;

	if (ot == ROW_GROUP)
		for (i = 0; i < joinerCount; i++) {
			if (!typelessJoin[i]) {
				if ((tJoiners[i].get() == NULL || tJoiners[i]->size() !=
				  tJoinerSizes[i]))
					return -1;
			}
			else
				if ((tlJoiners[i].get() == NULL || tlJoiners[i]->size() !=
				  tJoinerSizes[i]))
					return -1;
		}
	else
		if (joiner.get() == NULL || joiner->size() != joinerSize)
			return -1;

    endOfJoinerRan = true;

#ifdef old_version
	addToJoinerLock.lock();
	if (ot == ROW_GROUP)
		for (i = 0; i < joinerCount; i++) {
			if (!typelessJoin[i])
				while ((tJoiners[i].get() == NULL || tJoiners[i]->size() !=
				  tJoinerSizes[i])) {
					addToJoinerLock.unlock();
					usleep(2000);
					addToJoinerLock.lock();
				}
			else
				while ((tlJoiners[i].get() == NULL || tlJoiners[i]->size() !=
				  tJoinerSizes[i])) {
					addToJoinerLock.unlock();
					usleep(2000);
					addToJoinerLock.lock();
				}
		}
	else
		while (joiner.get() == NULL || joiner->size() != joinerSize) {
			addToJoinerLock.unlock();
			usleep(2000);
			addToJoinerLock.lock();
		}
	addToJoinerLock.unlock();
#endif

#ifndef __FreeBSD__
	objLock.unlock();
#endif
    return 0;
}

void BatchPrimitiveProcessor::initProcessor()
{
    uint i, j;

    if (gotAbsRids || needStrValues || hasRowGroup)
        absRids.reset(new uint64_t[LOGICAL_BLOCK_RIDS]);
    if (needStrValues)
        strValues.reset(new string[LOGICAL_BLOCK_RIDS]);
	outMsgSize = defaultBufferSize;
	outputMsg.reset(new uint8_t[outMsgSize]);
	if (ot == ROW_GROUP) {
		// calculate the projection -> outputRG mapping
		projectionMap.reset(new int[projectCount]);
		bool* reserved = (bool*)alloca(outputRG.getColumnCount() * sizeof(bool));
		for (i = 0; i < outputRG.getColumnCount(); i++)
			reserved[i] = false;
		for (i = 0; i < projectCount; i++) {
			for (j = 0; j < outputRG.getColumnCount(); j++)
				if (projectSteps[i]->getTupleKey() == outputRG.getKeys()[j] && !reserved[j]) {
					projectionMap[i] = j;
					reserved[j] = true;
					break;
				}
			if (j == outputRG.getColumnCount())
				projectionMap[i] = -1;
		}
		if (doJoin) {
			outputRG.initRow(&oldRow);
			outputRG.initRow(&newRow);
			tmpKeyAllocators.reset(new FixedAllocator[joinerCount]);
			for (i = 0; i < joinerCount; i++)
				if (typelessJoin[i])
					tmpKeyAllocators[i] = FixedAllocator(tlKeyLengths[i], true);
			tSmallSideMatches.reset(new MatchedData[joinerCount]);
			keyColumnProj.reset(new bool[projectCount]);
			for (i = 0; i < projectCount; i++) {
				keyColumnProj[i] = false;
				for (j = 0; j < joinerCount; j++) {
					if (!typelessJoin[j]) {
						if (projectionMap[i] == (int) largeSideKeyColumns[j]) {
							keyColumnProj[i] = true;
							break;
						}
					}
					else {
						for (uint k = 0; k < tlLargeSideKeyColumns[j].size(); k++) {
							if (projectionMap[i] == (int) tlLargeSideKeyColumns[j][k]) {
								keyColumnProj[i] = true;
								break;
							}
						}
					}
				}
			}
			if (hasJoinFEFilters) {
				joinFERG->initRow(&joinFERow, true);
				joinFERowData.reset(new uint8_t[joinFERow.getSize()]);
				joinFERow.setData(joinFERowData.get());
				joinFEMappings.reset(new shared_array<int>[joinerCount + 1]);
				for (i = 0; i < joinerCount; i++)
					joinFEMappings[i] = makeMapping(smallSideRGs[i], *joinFERG);
				joinFEMappings[joinerCount] = makeMapping(largeSideRG, *joinFERG);
			}
		}
		/*
		Calculate the FE1 -> projection mapping
		Calculate the projection step -> FE1 input mapping
		*/
		if (fe1) {
			fe1ToProjection = makeMapping(fe1Input, outputRG);
			projectForFE1.reset(new int[projectCount]);
			bool* reserved = (bool*)alloca(fe1Input.getColumnCount() * sizeof(bool));
			for (i = 0; i < fe1Input.getColumnCount(); i++)
				reserved[i] = false;
			for (i = 0; i < projectCount; i++) {
				projectForFE1[i] = -1;
				for (j = 0; j < fe1Input.getColumnCount(); j++) {
					if (projectSteps[i]->getTupleKey() == fe1Input.getKeys()[j] && !reserved[j]) {
						reserved[j] = true;
						projectForFE1[i] = j;
						break;
					}
				}
			}
			fe1Input.initRow(&fe1In);
			outputRG.initRow(&fe1Out);
		}
		if (fe2) {
			fe2Input = (doJoin ? &joinedRG : &outputRG);
			fe2Mapping = makeMapping(*fe2Input, fe2Output);
			fe2Input->initRow(&fe2In);
			fe2Output.initRow(&fe2Out);
		}
		if (getTupleJoinRowGroupData) {
			gjrgPlaceHolders.reset(new uint[joinerCount]);
			outputRG.initRow(&largeRow);
			joinedRG.initRow(&joinedRow);
			joinedRG.initRow(&baseJRow, true);
			smallRows.reset(new Row[joinerCount]);
			for (i = 0; i < joinerCount; i++)
				smallSideRGs[i].initRow(&smallRows[i], true);
			baseJRowMem.reset(new uint8_t[baseJRow.getSize()]);
			baseJRow.setData(baseJRowMem.get());
			gjrgMappings.reset(new shared_array<int>[joinerCount + 1]);
			for (i = 0; i < joinerCount; i++)
				gjrgMappings[i] = makeMapping(smallSideRGs[i], joinedRG);
			gjrgMappings[joinerCount] = makeMapping(outputRG, joinedRG);
		}
	}

    // @bug 1051
    if (hasFilterStep)
    {
        for (uint64_t i = 0; i < 2; i++)
        {
            fFiltRidCount[i] = 0;
            fFiltCmdRids[i].reset(new uint16_t[LOGICAL_BLOCK_RIDS]);
            fFiltCmdValues[i].reset(new int64_t[LOGICAL_BLOCK_RIDS]);
            if(filtOnString) fFiltStrValues[i].reset(new string[LOGICAL_BLOCK_RIDS]);
        }
    }

	/* init the Commands */
	if (filterCount > 0) {
		for (i = 0; i < (uint) filterCount - 1; ++i) {
// 			cout << "prepping filter " << i << endl;
			filterSteps[i]->setBatchPrimitiveProcessor(this);
			if (filterSteps[i+1]->getCommandType() == Command::DICT_STEP)
				filterSteps[i]->prep(OT_BOTH, true);
			else if (filterSteps[i]->filterFeeder() != Command::NOT_FEEDER)
				filterSteps[i]->prep(OT_BOTH, false);
			else
				filterSteps[i]->prep(OT_RID, false);
		}
// 		cout << "prepping filter " << i << endl;
		filterSteps[i]->setBatchPrimitiveProcessor(this);
		filterSteps[i]->prep(OT_BOTH, false);
	}
	for (i = 0; i < projectCount; ++i) {
// 		cout << "prepping projection " << i << endl;
		projectSteps[i]->setBatchPrimitiveProcessor(this);
		if (noVB)
            projectSteps[i]->prep(OT_BOTH, false);
        else
            projectSteps[i]->prep(OT_DATAVALUE, false);
		if (0 < filterCount )
		{	//if there is an rtscommand with a passThru, the passThru must make its own absRids
			//unless there is only one project step, then the last filter step can make absRids
			RTSCommand* rts = dynamic_cast<RTSCommand*>(projectSteps[i].get());
			if (rts && rts->isPassThru())
			{
				if (1 == projectCount)
					filterSteps[filterCount - 1]->setMakeAbsRids(true);
				else rts->setAbsNull();
			}
		}
	}

	if (fAggregator.get() != NULL)
	{
		//fAggRowGroupData.reset(new uint8_t[fAggregateRG.getMaxDataSize()]);
		fAggRowGroupData.reinit(fAggregateRG);
		fAggregateRG.setData(&fAggRowGroupData);
		if (doJoin) {
			fAggregator->setInputOutput(fe2 ? fe2Output : joinedRG, &fAggregateRG);
			fAggregator->setJoinRowGroups(&smallSideRGs, &largeSideRG);
		}
		else
			fAggregator->setInputOutput(fe2 ? fe2Output : outputRG, &fAggregateRG);
	}
	minVal = MAX64;
	maxVal = MIN64;

	// @bug 1269, initialize data used by execute() for async loading blocks
	// +1 for the scan filter step with no predicate, if any
	relLBID.reset(new uint64_t[projectCount + 1]);
	asyncLoaded.reset(new bool[projectCount + 1]);
}

void BatchPrimitiveProcessor::executeJoin()
{
	uint newRowCount, i;

	preJoinRidCount = ridCount;
	newRowCount = 0;
	smallSideMatches.clear();
	for (i = 0; i < ridCount; i++) {
		if (joiner->getNewMatches(values[i], &smallSideMatches)) {
			values[newRowCount] = values[i];
			relRids[newRowCount++] = relRids[i];
		}
	}
	ridCount = newRowCount;
}

/* This version does a join on projected rows */
void BatchPrimitiveProcessor::executeTupleJoin()
{
	uint newRowCount = 0, i, j;
	vector<uint32_t> matches;
	uint64_t largeKey;
	TypelessData tlLargeKey;

	preJoinRidCount = ridCount;
	outputRG.getRow(0, &oldRow);
	outputRG.getRow(0, &newRow);

 	//cout << "before join, RG has " << outputRG.getRowCount() << " BPP ridcount= " << ridCount << endl;
	for (i = 0; i < ridCount && !sendThread->aborted(); i++, oldRow.nextRow()) {
		/* Decide whether this large-side row belongs in the output.  The breaks
		 * in the loop mean that it doesn't.
		 *
		 * In English the logic is:
		 * 		Reject the row if there's no match and it's not an anti or an outer join
		 *      or if there is a match and it's an anti join with no filter.
		 * 		If there's an antijoin with a filter nothing can be eliminated at this stage.
		 * 		If there's an antijoin where the join op should match NULL values, and there
		 * 		  are NULL values to match against, but there is no filter, all rows can be eliminated.
		 */

		//cout << "large side row: " << oldRow.toString() << endl;
		for (j = 0; j < joinerCount; j++) {
			bool found;
			if (UNLIKELY(joinTypes[j] & ANTI)) {
				if (joinTypes[j] & WITHFCNEXP)
					continue;
				else if (doMatchNulls[j])
					break;
			}
			if (LIKELY(!typelessJoin[j])) {
				//cout << "not typeless join\n";
				bool isNull;
                uint colIndex = largeSideKeyColumns[j];
                if (oldRow.isUnsigned(colIndex)) 
				    largeKey = oldRow.getUintField(colIndex);
                else
                    largeKey = oldRow.getIntField(colIndex);
                found = (tJoiners[j]->find(largeKey) != tJoiners[j]->end());
				isNull = oldRow.isNullValue(colIndex);
				/* These conditions define when the row is NOT in the result set:
				 *    - if the key is not in the small side, and the join isn't a large-outer or anti join
				 *    - if the key is NULL, and the join isn't anti- or large-outer
				 *    - if it's an anti-join and the key is either in the small side or it's NULL
				 */

				if (((!found || isNull) && !(joinTypes[j] & (LARGEOUTER | ANTI))) ||
				  ((joinTypes[j] & ANTI) && ((isNull && (joinTypes[j] & MATCHNULLS)) || (found && !isNull)))) {
					//cout << " - not in the result set\n";
					break;
				}
				//else
				//	cout << " - in the result set\n";
			}
			else {
				//cout << " typeless join\n";
				// the null values are not sent by UM in typeless case.  null -> !found
				tlLargeKey = makeTypelessKey(oldRow, tlLargeSideKeyColumns[j], tlKeyLengths[j],
				  &tmpKeyAllocators[j]);
				found = tlJoiners[j]->find(tlLargeKey) != tlJoiners[j]->end();
				if ((!found && !(joinTypes[j] & (LARGEOUTER | ANTI))) ||
				  (joinTypes[j] & ANTI)) {

					/* Separated the ANTI join logic for readability.
					 *
					 */
					if (joinTypes[j] & ANTI) {
						if (found)
							break;
						else if (joinTypes[j] & MATCHNULLS) {
							bool hasNull = false;
							for (uint z = 0; z < tlLargeSideKeyColumns[j].size(); z++)
								if (oldRow.isNullValue(tlLargeSideKeyColumns[j][z])) {
									hasNull = true;
									break;
								}
							if (hasNull)  // keys with nulls match everything
								break;
							else
								continue;    // non-null keys not in the small side
											 // are in the result
						}
						else    // signifies a not-exists query
							continue;
					}
					break;
				}
			}
		}
		if (j == joinerCount) {
			for (j = 0; j < joinerCount; j++) {
				uint matchCount;

				/* The result is already known if...
				 *   -- anti-join with no fcnexp
				 *   -- semi-join with no fcnexp and not scalar
				 *
				 * The ANTI join case isn't just a shortcut.  getJoinResults() will produce results
				 * for a different case and generate the wrong result.  Need to fix that, later.
				 */
				if ((joinTypes[j] & (SEMI | ANTI)) && !(joinTypes[j] & WITHFCNEXP) && !(joinTypes[j] & SCALAR)) {
					tSmallSideMatches[j][newRowCount].push_back(-1);
					continue;
				}

				getJoinResults(oldRow, j, tSmallSideMatches[j][newRowCount]);
				matchCount = tSmallSideMatches[j][newRowCount].size();

				if (joinTypes[j] & WITHFCNEXP) {
					vector<uint32_t> newMatches;
					applyMapping(joinFEMappings[joinerCount], oldRow, &joinFERow);
					for (uint k = 0; k < matchCount; k++) {
						if (tSmallSideMatches[j][newRowCount][k] == (uint) -1)
							smallRows[j].setPointer(smallNullPointers[j]);
						else {
							smallSideRGs[j].getRow(tSmallSideMatches[j][newRowCount][k], &smallRows[j]);
							//uint64_t rowOffset = ((uint64_t) tSmallSideMatches[j][newRowCount][k]) *
							//		smallRows[j].getSize() + smallSideRGs[j].getEmptySize();
							//smallRows[j].setData(&smallSideRowData[j][rowOffset]);
						}
						applyMapping(joinFEMappings[j], smallRows[j], &joinFERow);
						if (joinFEFilters[j]->evaluate(&joinFERow)) {
							/* The first match includes it in a SEMI join result and excludes it from an ANTI join
							 * result.  If it's SEMI & SCALAR however, it needs to continue.
							 */
							newMatches.push_back(tSmallSideMatches[j][newRowCount][k]);
							if ((joinTypes[j] & ANTI) || ((joinTypes[j] & (SEMI | SCALAR)) == SEMI))
								break;
						}
					}
					tSmallSideMatches[j][newRowCount].swap(newMatches);
					matchCount = tSmallSideMatches[j][newRowCount].size();
				}

				if (matchCount == 0 && (joinTypes[j] & LARGEOUTER)) {
					tSmallSideMatches[j][newRowCount].push_back(-1);
					matchCount = 1;
				}

				/* Scalar check */
				if ((joinTypes[j] & SCALAR) && matchCount > 1)
					throw scalar_exception();

				/* Reverse the result for anti-join */
				if (joinTypes[j] & ANTI) {
					if (matchCount == 0) {
						tSmallSideMatches[j][newRowCount].push_back(-1);
						matchCount = 1;
					}
					else {
						tSmallSideMatches[j][newRowCount].clear();
						matchCount = 0;
					}
				}

				/* For all join types, no matches here means it's not in the result */
				if (matchCount == 0)
					break;

				/* Pair non-scalar semi-joins with a NULL row */
				if ((joinTypes[j] & SEMI) && !(joinTypes[j] & SCALAR)) {
					tSmallSideMatches[j][newRowCount].clear();
					tSmallSideMatches[j][newRowCount].push_back(-1);
					matchCount = 1;
				}
			}

			/* Finally, copy the row into the output */
			if (j == joinerCount) {
				if (i != newRowCount) {
					values[newRowCount] = values[i];
					relRids[newRowCount] = relRids[i];
					copyRow(oldRow, &newRow);
					//cout << "joined row: " << newRow.toString() << endl;
					//memcpy(newRow.getData(), oldRow.getData(), oldRow.getSize());
				}
				newRowCount++;
				newRow.nextRow();
			}
			//else
			//	cout << "j != joinerCount\n";
		}
	}
	ridCount = newRowCount;
	outputRG.setRowCount(ridCount);

/* prints out the whole result set.
	if (ridCount != 0) {
		cout << "RG rowcount=" << outputRG.getRowCount() << " BPP ridcount=" << ridCount << endl;
		for (i = 0; i < joinerCount; i++) {
			for (j = 0; j < ridCount; j++) {
				cout << "joiner " << i << " has " << tSmallSideMatches[i][j].size() << " entries" << endl;
				cout << "row " << j << ":";
				for (uint k = 0; k < tSmallSideMatches[i][j].size(); k++)
					cout << "  " << tSmallSideMatches[i][j][k];
				cout << endl;
			}
			cout << endl;
		}
	}
*/
}

#ifdef PRIMPROC_STOPWATCH
void BatchPrimitiveProcessor::execute(StopWatch *stopwatch)
#else
void BatchPrimitiveProcessor::execute()
#endif
{
	uint i, j;

	try
	{
#ifdef PRIMPROC_STOPWATCH
stopwatch->start("BatchPrimitiveProcessor::execute first part");
#endif
	// if only one scan step which has no predicate, async load all columns
	if (filterCount == 1 && hasScan) {
		ColumnCommand* col = dynamic_cast<ColumnCommand*>(filterSteps[0].get());

		if ((col != NULL) && (col->getFilterCount() == 0) && (col->getLBID() != 0)) {
			// stored in last pos in relLBID[] and asyncLoaded[]
			uint64_t p = projectCount;
			asyncLoaded[p] = asyncLoaded[p] && (relLBID[p] % blocksReadAhead !=0);
			relLBID[p] += col->getWidth();
			if (!asyncLoaded[p] && col->willPrefetch()) {
				loadBlockAsync(col->getLBID(),
					versionInfo,
					txnID,
					col->getCompType(),
					&cachedIO,
					&physIO,
					LBIDTrace,
					sessionID,
					&counterLock,
					&busyLoaderCount,
					&vssCache);
				asyncLoaded[p] = true;
			}
			asyncLoadProjectColumns();
		}
	}

#ifdef PRIMPROC_STOPWATCH
stopwatch->stop("BatchPrimitiveProcessor::execute first part");
stopwatch->start("BatchPrimitiveProcessor::execute second part");
#endif

	// filters use relrids and values for intermediate results.
	if (bop == BOP_AND)
		for (j = 0; j < filterCount; ++j)
		{
#ifdef PRIMPROC_STOPWATCH
			stopwatch->start("- filterSteps[j]->execute()");
			filterSteps[j]->execute();
			stopwatch->stop("- filterSteps[j]->execute()");
#else
			filterSteps[j]->execute();
#endif
		}
	else {			// BOP_OR

		/* XXXPAT: This is a hacky impl of OR logic.  Each filter is configured to
		be a scan operation on init.  This code runs each independently and
		unions their output ridlists using accumulator.  At the end it turns
		accumulator into a final ridlist for subsequent steps.

		If there's a join or a passthru command in the projection list, the
		values array has to contain values from the last filter step.  In that
		case, the last filter step isn't part of the "OR" filter processing.
		JLF has added it to prep those operations, not to be a filter.

		7/7/09 update: the multiple-table join required relocating the join op.  It's
		no longer necessary to add the loader columncommand to the filter array.
		*/

		bool accumulator[LOGICAL_BLOCK_RIDS];
// 		uint realFilterCount = ((forHJ || hasPassThru) ? filterCount - 1 : filterCount);
		uint realFilterCount = filterCount;

		for (i = 0; i < LOGICAL_BLOCK_RIDS; i++)
			accumulator[i] = false;

		if (!hasScan)  // there are input rids
			for (i = 0; i < ridCount; i++)
				accumulator[relRids[i]] = true;
		ridCount = 0;
		for (i = 0; i < realFilterCount; ++i) {
			filterSteps[i]->execute();
			if (! filterSteps[i]->filterFeeder())
			{
				for (j = 0; j < ridCount; j++)
					accumulator[relRids[j]] = true;
				ridCount = 0;
			}
		}
		for (ridMap = 0, i = 0; i < LOGICAL_BLOCK_RIDS; ++i) {
			if (accumulator[i]) {
				relRids[ridCount] = i;
				ridMap |= 1 << (relRids[ridCount] >> 10);
				++ridCount;
			}
		}
	}

#ifdef PRIMPROC_STOPWATCH
stopwatch->stop("BatchPrimitiveProcessor::execute second part");
stopwatch->start("BatchPrimitiveProcessor::execute third part");
#endif

	if (doJoin && ot != ROW_GROUP)
	{
#ifdef PRIMPROC_STOPWATCH
		stopwatch->start("- executeJoin");
		executeJoin();
		stopwatch->stop("- executeJoin");
#else
		executeJoin();
#endif
	}

	if (projectCount > 0 || ot == ROW_GROUP)
	{
#ifdef PRIMPROC_STOPWATCH
		stopwatch->start("- writeProjectionPreamble");
		writeProjectionPreamble();
		stopwatch->stop("- writeProjectionPreamble");
#else
		writeProjectionPreamble();
#endif
	}

	// async load blocks for project phase, if not alread loaded
	if (ridCount > 0)
	{
#ifdef PRIMPROC_STOPWATCH
		stopwatch->start("- asyncLoadProjectColumns");
		asyncLoadProjectColumns();
		stopwatch->stop("- asyncLoadProjectColumns");
#else
		asyncLoadProjectColumns();
#endif
	}

#ifdef PRIMPROC_STOPWATCH
stopwatch->stop("BatchPrimitiveProcessor::execute third part");
stopwatch->start("BatchPrimitiveProcessor::execute fourth part");
#endif

	// projection commands read relrids and write output directly to a rowgroup
	// or the serialized bytestream
	if (ot != ROW_GROUP)
		for (j = 0; j < projectCount; ++j)
		{
			projectSteps[j]->project();
		}
	else {
		/* Function & Expression group 1 processing
			- project for FE1
			- execute FE1 row by row
			- if return value = true, map input row into the projection RG, adjust ridlist
		*/
#ifdef PRIMPROC_STOPWATCH
		stopwatch->start("- if(ot != ROW_GROUP) else");
#endif
		outputRG.resetRowGroup(baseRid);
		if (fe1) {
			uint newRidCount = 0;
			fe1Input.resetRowGroup(baseRid);
			fe1Input.setRowCount(ridCount);
			fe1Input.getRow(0, &fe1In);
			outputRG.getRow(0, &fe1Out);
			for (j = 0; j < projectCount; j++)
				if (projectForFE1[j] != -1)
					projectSteps[j]->projectIntoRowGroup(fe1Input, projectForFE1[j]);
			for (j = 0; j < ridCount; j++, fe1In.nextRow())
				if (fe1->evaluate(&fe1In)) {
					applyMapping(fe1ToProjection, fe1In, &fe1Out);
					relRids[newRidCount] = relRids[j];
					values[newRidCount++] = values[j];
					fe1Out.nextRow();
				}
			ridCount = newRidCount;
		}
		outputRG.setRowCount(ridCount);
		if (sendRidsAtDelivery) {
			Row r;
			outputRG.initRow(&r);
			outputRG.getRow(0, &r);
			for (j = 0; j < ridCount; ++j) {
				r.setRid(relRids[j]);
				r.nextRow();
			}
		}

		/* 7/7/09 PL: I Changed the projection alg to reduce block touches when there's
		a join.  The key columns get projected first, the join is executed to further
		reduce the ridlist, then the rest of the columns get projected */

		if (!doJoin)
		{
			for (j = 0; j < projectCount; ++j) {
// 				cout << "projectionMap[" << j << "] = " << projectionMap[j] << endl;
				if (projectionMap[j] != -1) {
#ifdef PRIMPROC_STOPWATCH
					stopwatch->start("-- projectIntoRowGroup");
					projectSteps[j]->projectIntoRowGroup(outputRG, projectionMap[j]);
					stopwatch->stop("-- projectIntoRowGroup");
#else
					projectSteps[j]->projectIntoRowGroup(outputRG, projectionMap[j]);
#endif
				}
//				else
//					cout << "   no target found for OID " << projectSteps[j]->getOID() << endl;
			}
		}
		else {
			/* project the key columns.  If there's the filter IN the join, project everything. */
			for (j = 0; j < projectCount; j++)
				if (keyColumnProj[j] || (hasJoinFEFilters && projectionMap[j] != -1))
				{
#ifdef PRIMPROC_STOPWATCH
					stopwatch->start("-- projectIntoRowGroup");
					projectSteps[j]->projectIntoRowGroup(outputRG, projectionMap[j]);
					stopwatch->stop("-- projectIntoRowGroup");
#else
					projectSteps[j]->projectIntoRowGroup(outputRG, projectionMap[j]);
#endif
				}

#ifdef PRIMPROC_STOPWATCH
			stopwatch->start("-- executeTupleJoin()");
			executeTupleJoin();
			stopwatch->stop("-- executeTupleJoin()");
#else
		executeTupleJoin();
#endif

			/* project the non-key columns */
			for (j = 0; j < projectCount; ++j)
			{
				if ((!keyColumnProj[j] && projectionMap[j] != -1) && !hasJoinFEFilters)
				{
#ifdef PRIMPROC_STOPWATCH
					stopwatch->start("-- projectIntoRowGroup");
					projectSteps[j]->projectIntoRowGroup(outputRG, projectionMap[j]);
					stopwatch->stop("-- projectIntoRowGroup");
#else
					projectSteps[j]->projectIntoRowGroup(outputRG, projectionMap[j]);
#endif
				}
			}
		}

		/* The RowGroup is fully joined at this point.
		Add additional RowGroup processing here.
		TODO:  Try to clean up all of the switching */

		if (doJoin && getTupleJoinRowGroupData) {
			bool moreRGs = true;
			ByteStream preamble = *serialized;
			initGJRG();
			while (moreRGs && !sendThread->aborted()) {
				/*
					generate 1 rowgroup (8192 rows max) of joined rows
					if there's an FE2, run it
						-pack results into a new rowgroup
						-if there are < 8192 rows in the new RG, continue
					if there's an agg, run it
					send the result
				*/
				resetGJRG();
				moreRGs = generateJoinedRowGroup(baseJRow);
				*serialized << (uint8_t) !moreRGs;

				if (fe2) {
					/* functionize this -> processFE2()*/
					fe2Output.resetRowGroup(baseRid);
					fe2Output.setDBRoot(dbRoot);
					fe2Output.getRow(0, &fe2Out);
					fe2Input->getRow(0, &fe2In);
					for (j = 0; j < joinedRG.getRowCount(); j++, fe2In.nextRow())
						if (fe2->evaluate(&fe2In)) {
							applyMapping(fe2Mapping, fe2In, &fe2Out);
							fe2Out.setRid(fe2In.getRelRid());
							fe2Output.incRowCount();
							fe2Out.nextRow();
						}
				}
				RowGroup &nextRG = (fe2 ? fe2Output : joinedRG);
				nextRG.setDBRoot(dbRoot);
				if (fAggregator) {

					fAggregator->addRowGroup(&nextRG);

					if ((currentBlockOffset+1) == count && moreRGs == false) {  // @bug4507, 8k
						fAggregator->loadResult(*serialized);                   // @bug4507, 8k
					}                                                           // @bug4507, 8k
					else if (utils::MonitorProcMem::isMemAvailable()) {         // @bug4507, 8k
						fAggregator->loadEmptySet(*serialized);                 // @bug4507, 8k
					}                                                           // @bug4507, 8k
					else {                                                      // @bug4507, 8k
						fAggregator->loadResult(*serialized);                   // @bug4507, 8k
						fAggregator->reset();                                   // @bug4507, 8k
					}                                                           // @bug4507, 8k
				}
				else {
					//cerr <<" * serialzing " << nextRG.toString() << endl;
					nextRG.serializeRGData(*serialized);
					//*serialized << nextRG.getDataSize();
					//serialized->append(nextRG.getData(), nextRG.getDataSize());
				}
				/* send the msg & reinit the BS */
				if (moreRGs) {
					sendResponse();
					serialized.reset(new ByteStream());
					*serialized = preamble;
				}
			}

			if (hasSmallOuterJoin) {
				*serialized << ridCount;
				for (i = 0; i < joinerCount; i++)
					for (j = 0; j < ridCount; ++j)
						serializeInlineVector<uint32_t>(*serialized,
						  tSmallSideMatches[i][j]);
			}
		}

		if (!doJoin && fe2) {
			/* functionize this -> processFE2() */
			fe2Output.resetRowGroup(baseRid);
			fe2Output.getRow(0, &fe2Out);
			fe2Input->getRow(0, &fe2In);
			//cerr << "input row: " << fe2In.toString() << endl;
			for (j = 0; j < outputRG.getRowCount(); j++, fe2In.nextRow()) {
				if (fe2->evaluate(&fe2In)) {
					applyMapping(fe2Mapping, fe2In, &fe2Out);
					//cerr << "   passed. output row: " << fe2Out.toString() << endl;
					fe2Out.setRid (fe2In.getRelRid());
					fe2Output.incRowCount();
					fe2Out.nextRow();
				}
			}
			if (!fAggregator) {
				*serialized << (uint8_t) 1;  // the "count this msg" var
				fe2Output.setDBRoot(dbRoot);
				fe2Output.serializeRGData(*serialized);
				//*serialized << fe2Output.getDataSize();
				//serialized->append(fe2Output.getData(), fe2Output.getDataSize());
			}
		}

		if (!doJoin && fAggregator) {
			*serialized << (uint8_t) 1;  // the "count this msg" var

			RowGroup &toAggregate = (fe2 ? fe2Output : outputRG);
			//toAggregate.convertToInlineDataInPlace();

			if (fe2)
				fe2Output.setDBRoot(dbRoot);
			else
				outputRG.setDBRoot(dbRoot);
			fAggregator->addRowGroup(&toAggregate);
			if ((currentBlockOffset+1) == count) {                    // @bug4507, 8k
				fAggregator->loadResult(*serialized);                 // @bug4507, 8k
			}                                                         // @bug4507, 8k
			else if (utils::MonitorProcMem::isMemAvailable()) {       // @bug4507, 8k
				fAggregator->loadEmptySet(*serialized);               // @bug4507, 8k
			}                                                         // @bug4507, 8k
			else  {                                                   // @bug4507, 8k
				fAggregator->loadResult(*serialized);                 // @bug4507, 8k
				fAggregator->reset();                                 // @bug4507, 8k
			}                                                         // @bug4507, 8k
		}

		if (!getTupleJoinRowGroupData && !fAggregator && !fe2) {
			*serialized << (uint8_t) 1;  // the "count this msg" var
			outputRG.setDBRoot(dbRoot);
			//cerr << "serializing " << outputRG.toString() << endl;
			outputRG.serializeRGData(*serialized);
			//*serialized << outputRG.getDataSize();
			//serialized->append(outputRG.getData(), outputRG.getDataSize());
			if (doJoin) {
				for (i = 0; i < joinerCount; i++) {
					for (j = 0; j < ridCount; ++j) {
						serializeInlineVector<uint32_t>(*serialized,
						  tSmallSideMatches[i][j]);
					}
				}
			}
		}

		// clear small side match vector
		if (doJoin) {
			for (i = 0; i < joinerCount; i++)
				for (j = 0; j < ridCount; ++j)
					tSmallSideMatches[i][j].clear();
		}

#ifdef PRIMPROC_STOPWATCH
		stopwatch->stop("- if(ot != ROW_GROUP) else");
#endif
	}

	if (projectCount > 0 || ot == ROW_GROUP) {
		*serialized << cachedIO;
		cachedIO = 0;
		*serialized << physIO;
		physIO = 0;
		*serialized << touchedBlocks;
		touchedBlocks = 0;
// 		cout << "sent physIO=" << physIO << " cachedIO=" << cachedIO <<
// 			" touchedBlocks=" << touchedBlocks << endl;
	}

#ifdef PRIMPROC_STOPWATCH
stopwatch->stop("BatchPrimitiveProcessor::execute fourth part");
#endif

	}
	catch (logging::QueryDataExcept& qex)
	{
		ostringstream os;
		os << qex.what() << endl;
		writeErrorMsg(os.str(), qex.errorCode());
	}
	catch (logging::DictionaryBufferOverflow &db)
	{
		ostringstream os;
		os << db.what() << endl;
		writeErrorMsg(os.str(), db.errorCode());
	}
	catch (scalar_exception &se)
	{
		writeErrorMsg(IDBErrorInfo::instance()->errorMsg(ERR_MORE_THAN_1_ROW), ERR_MORE_THAN_1_ROW, false);
	}
	catch (NeedToRestartJob &n)
	{
#if 0

		/* This block of code will flush the problematic OIDs from the
		 * cache.  It seems to have no effect on the problem, so it's commented
		 * for now.
		 *
		 * This is currently thrown only on syscat queries.  If we find the problem
		 * in user tables also, we should avoid dropping entire OIDs if possible.
		 *
		 * In local testing there was no need for flushing, because DDL flushes
		 * the syscat constantly.  However, it can take a long time (>10 s) before
		 * that happens.  Doing it locally should make it much more likely only
		 * one restart is necessary.
		 */

		try {
			vector<uint32_t> oids;
			uint32_t oid;
			for (uint i = 0; i < filterCount; i++) {
				oid = filterSteps[i]->getOID();
				if (oid > 0)
					oids.push_back(oid);
			}
			for (uint i = 0; i < projectCount; i++) {
				oid = projectSteps[i]->getOID();
				if (oid > 0)
					oids.push_back(oid);
			}

#if 0
			Logger logger;
			ostringstream os;
			os << "dropping OIDs: ";
			for (int i = 0; i < oids.size(); i++)
				os << oids[i] << " ";
			logger.logMessage(os.str());
#endif
			for (int i = 0; i < fCacheCount; i++) {
				dbbc::blockCacheClient bc(*BRPp[i]);
//				bc.flushCache();
				bc.flushOIDs(&oids[0], oids.size());
			}
		} catch (...) { }   // doesn't matter if this fails, just avoid crashing
#endif

#ifndef __FreeBSD__
		objLock.unlock();
#endif
		throw n;   // need to pass this through to BPPSeeder
	}
	catch (IDBExcept& iex)
	{
    	ostringstream os;
		os << iex.what() << endl;
    	writeErrorMsg(os.str(), iex.errorCode(), true, false);
	}
	catch(const std::exception& ex)
	{
		ostringstream os;
		os <<  ex.what() << endl;
		writeErrorMsg(os.str(), logging::batchPrimitiveProcessorErr);
	}
	catch(...)
	{
		string msg("BatchPrimitiveProcessor caught an unknown exception");
		writeErrorMsg(msg, logging::batchPrimitiveProcessorErr);
	}
}

void BatchPrimitiveProcessor::writeErrorMsg(const string& error, uint16_t errCode, bool logIt, bool critical)
{
	ISMPacketHeader ism;
	PrimitiveHeader ph;

	// we don't need every field of these headers.  Init'ing them anyway
	// makes memory checkers happy.
	memset(&ism, 0, sizeof(ISMPacketHeader));
	memset(&ph, 0, sizeof(PrimitiveHeader));
	ph.SessionID = sessionID;
	ph.StepID = stepID;
	ph.UniqueID = uniqueID;
	ism.Status = errCode;

	serialized.reset(new ByteStream());
	serialized->append((uint8_t *) &ism, sizeof(ism));
	serialized->append((uint8_t *) &ph, sizeof(ph));
	*serialized << error;

	if (logIt) {
		Logger log;
		log.logMessage(error, critical);
	}
}

void BatchPrimitiveProcessor::writeProjectionPreamble()
{
	ISMPacketHeader ism;
	PrimitiveHeader ph;

	// we don't need every field of these headers.  Init'ing them anyway
	// makes memory checkers happy.
	memset(&ism, 0, sizeof(ISMPacketHeader));
	memset(&ph, 0, sizeof(PrimitiveHeader));
	ph.SessionID = sessionID;
	ph.StepID = stepID;
	ph.UniqueID = uniqueID;

	serialized.reset(new ByteStream());
	serialized->append((uint8_t *) &ism, sizeof(ism));
	serialized->append((uint8_t *) &ph, sizeof(ph));

	/* add-ons */
	if (hasScan) {
		if (validCPData) {
			*serialized << (uint8_t) 1;
			*serialized << lbidForCP;
			*serialized << (uint64_t) minVal;
			*serialized << (uint64_t) maxVal;
		}
		else {
			*serialized << (uint8_t) 0;
			*serialized << lbidForCP;
		}
	}

// 	ridsOut += ridCount;
	/* results */

	if (ot != ROW_GROUP) {
		*serialized << ridCount;
		if (sendRidsAtDelivery) {
			*serialized << baseRid;
			serialized->append((uint8_t *) relRids, ridCount << 1);
		}
	}
}

void BatchPrimitiveProcessor::serializeElementTypes()
{

	*serialized << baseRid;
	*serialized << ridCount;
	serialized->append((uint8_t *) relRids, ridCount << 1);
	serialized->append((uint8_t *) values, ridCount << 3);

	/* Send the small side matches if there was a join */
	if (doJoin) {
		uint32_t ssize = smallSideMatches.size();
		*serialized << preJoinRidCount;
		*serialized << (uint32_t) ssize;
		if (ssize > 0)
			serialized->append((uint8_t *) &smallSideMatches[0], ssize << 4);
	}
}

void BatchPrimitiveProcessor::serializeStrings()
{

	*serialized << ridCount;
	serialized->append((uint8_t *) absRids.get(), ridCount << 3);
	for (uint i = 0; i < ridCount; ++i)
		*serialized << strValues[i];
}

void BatchPrimitiveProcessor::sendResponse()
{

	if (sendThread->flowControlEnabled()) {
		// newConnection should be set only for the first result of a batch job
		// it tells sendthread it should consider it for the connection array
		sendThread->sendResult(BPPSendThread::Msg_t(serialized, sock, writelock, sockIndex), newConnection);
		newConnection = false;
	}
	else {
		boost::mutex::scoped_lock lk(*writelock);
		sock->write(*serialized);
	}
	serialized.reset();
}

/* The output of a filter chain is either ELEMENT_TYPE or STRING_ELEMENT_TYPE */
void BatchPrimitiveProcessor::makeResponse()
{
	ISMPacketHeader ism;
	PrimitiveHeader ph;

	// we don't need every field of these headers.  Init'ing them anyway
	// makes memory checkers happy.
	memset(&ism, 0, sizeof(ISMPacketHeader));
	memset(&ph, 0, sizeof(PrimitiveHeader));
	ph.SessionID = sessionID;
	ph.StepID = stepID;
	ph.UniqueID = uniqueID;

	serialized.reset(new ByteStream());
	serialized->append((uint8_t *) &ism, sizeof(ism));
	serialized->append((uint8_t *) &ph, sizeof(ph));

	/* add-ons */
	if (hasScan) {
		if (validCPData) {
			*serialized << (uint8_t) 1;
			*serialized << lbidForCP;
			*serialized << (uint64_t) minVal;
			*serialized << (uint64_t) maxVal;
		}
		else {
			*serialized << (uint8_t) 0;
			*serialized << lbidForCP;
		}
	}

	/* results */
	/* Take the rid and value arrays, munge into OutputType ot */
	switch (ot) {
		case BPS_ELEMENT_TYPE:
			serializeElementTypes(); break;
		case STRING_ELEMENT_TYPE:
			serializeStrings(); break;
		default:
{
ostringstream oss;
oss << "BPP: makeResponse(): Bad output type: " << ot;
throw logic_error(oss.str());
}
			//throw logic_error("BPP: makeResponse(): Bad output type");
	}

	*serialized << cachedIO;
	cachedIO = 0;
	*serialized << physIO;
	physIO = 0;
	*serialized << touchedBlocks;
	touchedBlocks = 0;

// 	cout << "sent physIO=" << physIO << " cachedIO=" << cachedIO <<
// 		" touchedBlocks=" << touchedBlocks << endl;
}

int BatchPrimitiveProcessor::operator()()
{
	if (currentBlockOffset == 0) {
#ifdef PRIMPROC_STOPWATCH   // TODO: needs to be brought up-to-date
		map<pthread_t, logging::StopWatch*>::iterator stopwatchMapIter = stopwatchMap.find(pthread_self());
		logging::StopWatch *stopwatch;
		if(stopwatchMapIter != stopwatchMap.end())
		{
			stopwatch = stopwatchMapIter->second;
		}
		else
		{
			pthread_mutex_lock(&stopwatchMapMutex);
			stopwatch = new logging::StopWatch(stopwatchMap.size());
			stopwatchMap.insert(make_pair(pthread_self(), stopwatch));

			// Create the thread that will show timing results after five seconds of idle time.
			if(!stopwatchThreadCreated)
			{
				pthread_t timerThread;
				int err = pthread_create(&timerThread, NULL, autoFinishStopwatchThread, NULL);
				if(err)
					cout << "Error creating thread to complete Stopwatches." << endl;
				stopwatchThreadCreated = true;
			}
			pthread_mutex_unlock(&stopwatchMapMutex);
		}
		ostringstream oss;
		oss << "BatchPrimitiveProcessor::operator()";
		string msg = oss.str();
		stopwatch->start(msg);
#endif

		idbassert(count > 0);
	}

	if (fAggregator && currentBlockOffset == 0)                     // @bug4507, 8k
		fAggregator->reset();                                       // @bug4507, 8k

	for (; currentBlockOffset < count; currentBlockOffset++) {
		if (!(sessionID & 0x80000000)) {   // can't do this with syscat queries
			if (sendThread->aborted())
				break;
			if (!sendThread->okToProceed()) {
				freeLargeBuffers();
				return -1; // the reschedule error code
			}
		}
		allocLargeBuffers();
		minVal = MAX64;
		maxVal = MIN64;
		validCPData = false;
#ifdef PRIMPROC_STOPWATCH
		stopwatch->start("BPP() execute");
		execute(stopwatch);
		stopwatch->stop("BPP() execute");
#else
		execute();
#endif
		if (projectCount == 0 && ot != ROW_GROUP)
			makeResponse();

		try {
			sendResponse();
		}
		catch (std::exception &e) {
			cerr << "BPP::sendResponse(): " << e.what() << endl;
			break;  // If we make this throw, be sure to do the cleanup at the end
		}
		// Bug 4475: Control outgoing socket so that all messages from a
		// batch go out the same socket
		sockIndex = (sockIndex + 1) % connectionsPerUM;

		nextLBID();

		/* Base RIDs are now a combination of partition#, segment#, extent#, and block#. */
		uint32_t partNum;
		uint16_t segNum;
		uint8_t extentNum;
		uint16_t blockNum;
		rowgroup::getLocationFromRid(baseRid, &partNum, &segNum, &extentNum, &blockNum);
		/*
		cout << "baseRid=" << baseRid << " partNum=" << partNum << " segNum=" << segNum <<
				" extentNum=" << (int) extentNum
				<< " blockNum=" << blockNum << endl;
		*/
		blockNum++;
		baseRid = rowgroup::convertToRid(partNum, segNum, extentNum, blockNum);
		/*
		cout << "-- baseRid=" << baseRid << " partNum=" << partNum << " extentNum=" << (int) extentNum
				<< " blockNum=" << blockNum << endl;
		*/
	}

	vssCache.clear();
#ifndef __FreeBSD__
	objLock.unlock();
#endif
	freeLargeBuffers();
#ifdef PRIMPROC_STOPWATCH
	stopwatch->stop(msg);
#endif
// 	cout << "sent " << count << " responses" << endl;
	fBusy = false;
	return 0;
}

void BatchPrimitiveProcessor::allocLargeBuffers()
{
	if (ot == ROW_GROUP && !outRowGroupData) {
		//outputRG.setUseStringTable(true);
		outRowGroupData.reset(new RGData(outputRG));
		outputRG.setData(outRowGroupData.get());
	}
	if (fe1 && !fe1Data) {
		//fe1Input.setUseStringTable(true);
		fe1Data.reset(new RGData(fe1Input));
		//fe1Data.reset(new uint8_t[fe1Input.getMaxDataSize()]);
		fe1Input.setData(fe1Data.get());
	}
	if (fe2 && !fe2Data) {
		//fe2Output.setUseStringTable(true);
		fe2Data.reset(new RGData(fe2Output));
		fe2Output.setData(fe2Data.get());
	}
	if (getTupleJoinRowGroupData && !joinedRGMem) {
		//joinedRG.setUseStringTable(true);
		joinedRGMem.reset(new RGData(joinedRG));
		joinedRG.setData(joinedRGMem.get());
	}
}

void BatchPrimitiveProcessor::freeLargeBuffers()
{
	/* Get rid of large buffers */
	if (ot == ROW_GROUP && outputRG.getMaxDataSizeWithStrings() > maxIdleBufferSize)
		outRowGroupData.reset();
	if (fe1 && fe1Input.getMaxDataSizeWithStrings() > maxIdleBufferSize)
		fe1Data.reset();
	if (fe2 && fe2Output.getMaxDataSizeWithStrings() > maxIdleBufferSize)
		fe2Data.reset();
	if (getTupleJoinRowGroupData && joinedRG.getMaxDataSizeWithStrings() > maxIdleBufferSize)
		joinedRGMem.reset();
}

void BatchPrimitiveProcessor::nextLBID()
{
	uint i;

	for (i = 0; i < filterCount; i++)
		filterSteps[i]->nextLBID();
	for (i = 0; i < projectCount; i++)
		projectSteps[i]->nextLBID();
}

SBPP BatchPrimitiveProcessor::duplicate()
{
	SBPP bpp;
	uint i;

//	cout << "duplicating a bpp\n";

	bpp.reset(new BatchPrimitiveProcessor());
	bpp->ot = ot;
	bpp->versionInfo = versionInfo;
	bpp->txnID = txnID;
	bpp->sessionID = sessionID;
	bpp->stepID = stepID;
	bpp->uniqueID = uniqueID;
	bpp->needStrValues = needStrValues;
	bpp->gotAbsRids = gotAbsRids;
	bpp->gotValues = gotValues;
	bpp->LBIDTrace = LBIDTrace;
	bpp->hasScan = hasScan;
	bpp->hasFilterStep = hasFilterStep;
	bpp->filtOnString = filtOnString;
	bpp->hasRowGroup = hasRowGroup;
	bpp->getTupleJoinRowGroupData = getTupleJoinRowGroupData;
	bpp->bop = bop;
	bpp->hasPassThru = hasPassThru;
	bpp->forHJ = forHJ;

	if (ot == ROW_GROUP) {
		bpp->outputRG = outputRG;
		if (fe1) {
			bpp->fe1.reset(new FuncExpWrapper(*fe1));
			bpp->fe1Input = fe1Input;
		}
		if (fe2) {
			bpp->fe2.reset(new FuncExpWrapper(*fe2));
			bpp->fe2Output = fe2Output;
		}
	}
	bpp->doJoin = doJoin;
	if (doJoin) {
		bpp->objLock.lock();
		bpp->joinerSize = joinerSize;
		if (ot == ROW_GROUP) {
			/* There are add'l join vars, but only these are necessary for processing
				 a join */
			bpp->tJoinerSizes = tJoinerSizes;
			bpp->joinerCount = joinerCount;
			bpp->joinTypes = joinTypes;
			bpp->largeSideKeyColumns = largeSideKeyColumns;
			bpp->tJoiners = tJoiners;
			bpp->_pools = _pools;
			bpp->typelessJoin = typelessJoin;
			bpp->tlLargeSideKeyColumns = tlLargeSideKeyColumns;
			bpp->tlJoiners = tlJoiners;
			bpp->tlKeyLengths = tlKeyLengths;
			bpp->storedKeyAllocators = storedKeyAllocators;
			bpp->joinNullValues = joinNullValues;
			bpp->doMatchNulls = doMatchNulls;
			bpp->hasJoinFEFilters = hasJoinFEFilters;
			bpp->hasSmallOuterJoin = hasSmallOuterJoin;
			if (hasJoinFEFilters) {
				bpp->joinFERG = joinFERG;
				bpp->joinFEFilters.reset(new scoped_ptr<FuncExpWrapper>[joinerCount]);
				for (i = 0; i < joinerCount; i++)
					if (joinFEFilters[i])
						bpp->joinFEFilters[i].reset(new FuncExpWrapper(*joinFEFilters[i]));
			}
			if (getTupleJoinRowGroupData) {
				bpp->smallSideRGs = smallSideRGs;
				bpp->largeSideRG = largeSideRG;
				bpp->smallSideRowLengths = smallSideRowLengths;
				bpp->smallSideRowData = smallSideRowData;
				bpp->smallNullRowData = smallNullRowData;
				bpp->smallNullPointers = smallNullPointers;
				bpp->joinedRG = joinedRG;
			}
		}
		else
			bpp->joiner = joiner;
#ifdef __FreeBSD__
		bpp->objLock.unlock();
#endif
	}

	bpp->filterCount = filterCount;
	bpp->filterSteps.resize(filterCount);
	for (i = 0; i < filterCount; ++i)
		bpp->filterSteps[i] = filterSteps[i]->duplicate();

	bpp->projectCount = projectCount;
	bpp->projectSteps.resize(projectCount);
	for (i = 0; i < projectCount; ++i)
		bpp->projectSteps[i] = projectSteps[i]->duplicate();

	if (fAggregator.get() != NULL)
	{
		bpp->fAggregateRG = fAggregateRG;
		bpp->fAggregator.reset(new RowAggregation(
			fAggregator->getGroupByCols(), fAggregator->getAggFunctions()));
	}

	bpp->sendRidsAtDelivery = sendRidsAtDelivery;
	bpp->prefetchThreshold = prefetchThreshold;

	bpp->sock = sock;
	bpp->writelock = writelock;
	bpp->hasDictStep = hasDictStep;
	bpp->sendThread = sendThread;
	bpp->newConnection = true;
	bpp->initProcessor();
	return bpp;
}

#if 0
bool BatchPrimitiveProcessor::operator==(const BatchPrimitiveProcessor &bpp) const
{
	uint i;

	if (ot != bpp.ot)
		return false;
	if (versionInfo != bpp.versionInfo)
		return false;
	if (txnID != bpp.txnID)
		return false;
	if (sessionID != bpp.sessionID)
		return false;
	if (stepID != bpp.stepID)
		return false;
	if (uniqueID != bpp.uniqueID)
		return false;
	if (gotValues != bpp.gotValues)
		return false;
	if (gotAbsRids != bpp.gotAbsRids)
		return false;
	if (needStrValues != bpp.needStrValues)
		return false;
	if (filterCount != bpp.filterCount)
		return false;
	if (projectCount != bpp.projectCount)
		return false;
	if (sendRidsAtDelivery != bpp.sendRidsAtDelivery)
		return false;
	if (hasScan != bpp.hasScan)
		return false;
	if (hasFilterStep != bpp.hasFilterStep)
	  return false;
	if (filtOnString != bpp.filtOnString)
	  return false;
	if (doJoin != bpp.doJoin)
		return false;
	if (doJoin)
		/* Join equality test is a bit out of date */
		if (joiner != bpp.joiner || joinerSize != bpp.joinerSize)
			return false;
	for (i = 0; i < filterCount; i++)
		if (*filterSteps[i] != *bpp.filterSteps[i])
			return false;
	for (i = 0; i < projectCount; i++)
		if (*projectSteps[i] != *bpp.projectSteps[i])
			return false;
	return true;
}
#endif


void BatchPrimitiveProcessor::asyncLoadProjectColumns()
{
	// relLBID is the LBID related to the primMsg->LBID,
	// it is used to keep the read ahead boundary for asyncLoads
	// 1. scan driven case: load blocks in # to (# + blocksReadAhead - 1) range,
	//    where # is a multiple of ColScanReadAheadBlocks in Calpont.xml
	// 2. non-scan driven case: load blocks in the logical block.
	//    because 1 logical block per primMsg, asyncLoad only once per message.
	for (uint64_t i = 0; i < projectCount; ++i) {
		// only care about column commands
		ColumnCommand* col = dynamic_cast<ColumnCommand*>(projectSteps[i].get());
		if (col != NULL) {
			asyncLoaded[i] = asyncLoaded[i] && (relLBID[i] % blocksReadAhead !=0);
			relLBID[i] += col->getWidth();
			if (!asyncLoaded[i] && col->willPrefetch()) {
				loadBlockAsync(col->getLBID(),
							versionInfo,
							txnID,
							col->getCompType(),
							&cachedIO,
							&physIO,
							LBIDTrace,
							sessionID,
							&counterLock,
							&busyLoaderCount,
							&vssCache);
				asyncLoaded[i] = true;
			}
		}
	}
}

bool BatchPrimitiveProcessor::generateJoinedRowGroup(rowgroup::Row &baseRow, const uint depth)
{
	Row &smallRow = smallRows[depth];
	const bool lowestLvl = (depth == joinerCount - 1);

	while (gjrgRowNumber < ridCount &&
	  gjrgPlaceHolders[depth] < tSmallSideMatches[depth][gjrgRowNumber].size() &&
	  !gjrgFull) {
		const vector<uint32_t> &results = tSmallSideMatches[depth][gjrgRowNumber];
		const uint size = results.size();

		if (depth == 0) {
			outputRG.getRow(gjrgRowNumber, &largeRow);
			applyMapping(gjrgMappings[joinerCount], largeRow, &baseRow);
			baseRow.setRid(largeRow.getRelRid());
		}

		//cout << "rowNum = " << gjrgRowNumber << " at depth " << depth << " size is " << size << endl;
		for (uint &i = gjrgPlaceHolders[depth]; i < size && !gjrgFull; i++) {
			if (results[i] != (uint) -1) {
				smallSideRGs[depth].getRow(results[i], &smallRow);
				//rowOffset = ((uint64_t) results[i]) * smallRowSize;
				//smallRow.setData(&rowDataAtThisLvl.rowData[rowOffset] + emptySize);
			}
			else
				smallRow.setPointer(smallNullPointers[depth]);

			//cout << "small row: " << smallRow.toString() << endl;
			applyMapping(gjrgMappings[depth], smallRow, &baseRow);
			if (!lowestLvl)
				generateJoinedRowGroup(baseRow, depth + 1);
			else {
				copyRow(baseRow, &joinedRow);
				//memcpy(joinedRow.getData(), baseRow.getData(), joinedRow.getSize());
 				//cerr << "joined row " << joinedRG.getRowCount() << ": " << joinedRow.toString() << endl;
				joinedRow.nextRow();
				joinedRG.incRowCount();
				if (joinedRG.getRowCount() == 8192) {
					i++;
					gjrgFull = true;
				}
			}

			if (gjrgFull)
				break;
		}
		if (depth == 0 && gjrgPlaceHolders[0] == tSmallSideMatches[0][gjrgRowNumber].size()) {
			gjrgPlaceHolders[0] = 0;
			gjrgRowNumber++;
		}
	}
// 	if (depth == 0)
// 		cout << "gjrg returning " << (uint) gjrgFull << endl;
	if (!gjrgFull)
		gjrgPlaceHolders[depth] = 0;
	return gjrgFull;
}

void BatchPrimitiveProcessor::resetGJRG()
{
	gjrgFull = false;
	joinedRG.resetRowGroup(baseRid);
	joinedRG.getRow(0, &joinedRow);
	joinedRG.setDBRoot(dbRoot);
}

void BatchPrimitiveProcessor::initGJRG()
{
	for (uint z = 0; z < joinerCount; z++)
		gjrgPlaceHolders[z] = 0;
	gjrgRowNumber = 0;
}

inline void BatchPrimitiveProcessor::getJoinResults(const Row &r, uint jIndex, vector<uint32_t>& v)
{
	if (!typelessJoin[jIndex]) {
		if (r.isNullValue(largeSideKeyColumns[jIndex])) {
			/* Bug 3524. This matches everything. */
			if (joinTypes[jIndex] & ANTI) {
				TJoiner::iterator it;
				for (it = tJoiners[jIndex]->begin(); it != tJoiners[jIndex]->end(); ++it)
					v.push_back(it->second);
				return;
			}
			else
				return;
		}
        uint64_t largeKey;
        uint colIndex = largeSideKeyColumns[jIndex];
        if (r.isUnsigned(colIndex)) {
		    largeKey = r.getUintField(colIndex);
        }
        else {
		    largeKey = r.getIntField(colIndex);
        }
		pair<TJoiner::iterator, TJoiner::iterator> range = tJoiners[jIndex]->equal_range(largeKey);
		for (; range.first != range.second; ++range.first)
			v.push_back(range.first->second);
		if (doMatchNulls[jIndex]) {   // add the nulls to the match list
			range = tJoiners[jIndex]->equal_range(joinNullValues[jIndex]);
			for (; range.first != range.second; ++range.first)
				v.push_back(range.first->second);
		}
	}
	else {
		/* Bug 3524. Large-side NULL + ANTI join matches everything. */
		if (joinTypes[jIndex] & ANTI) {
			bool hasNullValue = false;
			for (uint i = 0; i < tlLargeSideKeyColumns[jIndex].size(); i++)
				if (r.isNullValue(tlLargeSideKeyColumns[jIndex][i])) {
					hasNullValue = true;
					break;
				}
			if (hasNullValue) {
				TLJoiner::iterator it;
				for (it = tlJoiners[jIndex]->begin(); it != tlJoiners[jIndex]->end(); ++it)
					v.push_back(it->second);
				return;
			}
		}

		TypelessData largeKey = makeTypelessKey(r, tlLargeSideKeyColumns[jIndex],
		  tlKeyLengths[jIndex], &tmpKeyAllocators[jIndex]);
		pair<TLJoiner::iterator, TLJoiner::iterator> range =
		  tlJoiners[jIndex]->equal_range(largeKey);
		for (; range.first != range.second; ++range.first)
			v.push_back(range.first->second);
	}
}

void BatchPrimitiveProcessor::buildVSSCache(uint loopCount)
{
	vector<int64_t> lbidList;
	vector<BRM::VSSData> vssData;
	uint i;
	int rc;

	for (i = 0; i < filterCount; i++)
		filterSteps[i]->getLBIDList(loopCount, &lbidList);
	for (i = 0; i < projectCount; i++)
		projectSteps[i]->getLBIDList(loopCount, &lbidList);

	rc = brm->bulkVSSLookup(lbidList, versionInfo, (int) txnID, &vssData);
	if (rc == 0)
		for (i = 0; i < vssData.size(); i++)
			vssCache.insert(make_pair(lbidList[i], vssData[i]));
//	cout << "buildVSSCache inserted " << vssCache.size() << " elements" << endl;
}

}
// vim:ts=4 sw=4:

