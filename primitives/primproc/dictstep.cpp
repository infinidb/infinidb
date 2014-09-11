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
// $Id: dictstep.cpp 1840 2012-02-17 17:14:00Z rdempsey $
// C++ Implementation: dictstep
//
// Description: 
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include <unistd.h>
#include <algorithm>

#include "bpp.h"
#include "primitiveserver.h"
#include "pp_logger.h"
#include "../linux-port/primitiveprocessor.h"

using namespace std;
using namespace messageqcpp;
using namespace rowgroup;

namespace primitiveprocessor {

extern uint dictBufferSize;
extern bool utf8;

DictStep::DictStep() : Command(DICT_STEP), strValues(NULL), filterCount(0),
		bufferSize(0)
{
}

DictStep::~DictStep()
{
}

DictStep & DictStep::operator=(const DictStep &d)
{
	// Not all fields are copied for the sake of efficiency.
	// Right now, these only need to copy fields deserialized from the create msg.
	BOP = d.BOP;
	fFilterFeeder = d.fFilterFeeder;
	compressionType = d.compressionType;
	filterString = d.filterString;
	hasEqFilter = d.hasEqFilter;
	eqFilter = d.eqFilter;
	eqOp = d.eqOp;
	filterCount = d.filterCount;
	return *this;
}

void DictStep::createCommand(ByteStream &bs)
{
	uint8_t tmp8;

	bs >> tmp8;  // eat the command
	bs >> BOP;
	bs >> tmp8;
	compressionType = tmp8;
	bs >> filterCount;
	bs >> tmp8;
	hasEqFilter = tmp8;
	if (hasEqFilter) {
		string strTmp;

		eqFilter.reset(new primitives::DictEqualityFilter());
		bs >> eqOp;
		//cout << "saw the eqfilter count=" << filterCount << endl;
		for (uint i = 0; i < filterCount; i++) {
			bs >> strTmp;
			//cout << "  " << strTmp << endl;
			eqFilter->insert(strTmp);
		}
	}
	else
		bs >> filterString;

#if 0
	cout << "see " << filterCount << " filters\n";
	DictFilterElement *filters = (DictFilterElement *) filterString.buf();
	for (uint i = 0; i < filterCount; i++) {
		cout << "  COP=" << (int) filters->COP << endl;
		cout << "  len=" << filters->len << endl;
		cout << "  string=" << filters->data << endl;
	}
#endif

	Command::createCommand(bs);
}

void DictStep::resetCommand(ByteStream &bs)
{
}

void DictStep::prep(int8_t outputType, bool makeAbsRids)
{
	// (at most there are 8192 tokens to fetch)
	bufferSize = sizeof(DictInput) + filterString.length() + (8192*sizeof(OldGetSigParams));
	inputMsg.reset(new uint8_t[bufferSize]);
	primMsg = (DictInput *) inputMsg.get();

	primMsg->ism.Reserve = 0;
	primMsg->ism.Flags = 0;
// 	primMsg->ism.Flags = PrimitiveMsg::planFlagsToPrimFlags(traceFlags);	
	primMsg->ism.Command=DICT_SIGNATURE;
	primMsg->ism.Size = bufferSize;
	primMsg->ism.Type = 2;
	primMsg->hdr.SessionID = bpp->sessionID;
	//primMsg->hdr.StatementID = 0;
	primMsg->hdr.TransactionID = bpp->txnID;
	primMsg->hdr.VerID = bpp->versionNum;
	primMsg->hdr.StepID = bpp->stepID;
	primMsg->BOP = BOP;
	primMsg->InputFlags = 1;		//TODO: Use the new p_Dict functionality
	primMsg->OutputType = (eqFilter || filterCount || fFilterFeeder != NOT_FEEDER
			? OT_RID : OT_RID | OT_DATAVALUE);
	primMsg->NOPS = (eqFilter ? 0 : filterCount);
	primMsg->NVALS = 0;

	likeFilter = bpp->pp.makeLikeFilter((DictFilterElement *) filterString.buf(), primMsg->NOPS);
}

void DictStep::issuePrimitive(bool isFilter)
{
	bool wasCached;
	uint32_t blocksRead;

	if (!(primMsg->LBID & 0x8000000000000000LL)) {
 		//cout << "DS issuePrimitive lbid: " << (uint64_t)primMsg->LBID << endl;
		primitiveprocessor::loadBlock(primMsg->LBID,
									bpp->versionNum, 
									bpp->txnID, 
									compressionType,
									bpp->blockData, 
									&wasCached, 
									&blocksRead, 
									bpp->LBIDTrace, 
									bpp->sessionID);
		if (wasCached)
			bpp->cachedIO++;
		bpp->physIO += blocksRead;
		bpp->touchedBlocks++;
	}
	bpp->pp.setLikeFilter(likeFilter);
	bpp->pp.p_Dictionary(primMsg, &result, utf8, isFilter, eqFilter, eqOp);
}

void DictStep::copyResultToTmpSpace(OrderedToken *ot)
{
	uint i;
	uint8_t *pos;
	uint16_t len;
	uint64_t rid64;
	uint16_t rid16;

	assert(primMsg->OutputType & OT_RID);
	DictOutput *header = (DictOutput *) &result[0];

	if (header->NVALS == 0) return;

	pos = &result[sizeof(DictOutput)];
	for (i = 0; i < header->NVALS; i++) {
		rid64 = *((uint64_t *) pos); pos += 8;
		rid16 = rid64 & 0x1fff;
		ot[rid16].inResult = true;
		tmpResultCounter++;

		if (primMsg->OutputType & OT_DATAVALUE) {
			len = *((uint16_t *) pos); pos += 2;
			ot[rid16].str = string((char *) pos, len); pos += len;
			if (rid64 & 0x8000000000000000LL)
				ot[rid16].str = joblist::CPNULLSTRMARK;
		}
	}
}

void DictStep::copyResultToFinalPosition(OrderedToken *ot)
{
	uint i, resultPos = 0;

	for (i = 0; i < inputRidCount; i++) {
		if (ot[i].inResult) {
			bpp->absRids[resultPos] = ot[i].rid;
			bpp->relRids[resultPos] = ot[i].rid & 0x1fff;
			if (primMsg->OutputType & OT_DATAVALUE)
				(*strValues)[resultPos] = ot[i].str;
			resultPos++;
		}
	}
}

void DictStep::processResult()
{
	uint i;
	uint8_t *pos;
	uint16_t len;
	DictOutput *header = (DictOutput *) &result[0];

	if (header->NVALS == 0) return;

	pos = &result[sizeof(DictOutput)];

	for (i = 0; i < header->NVALS; i++, tmpResultCounter++) {
		if (primMsg->OutputType & OT_RID) {
			bpp->absRids[tmpResultCounter] = *((uint64_t *) pos); pos += 8;
			bpp->relRids[tmpResultCounter] = bpp->absRids[tmpResultCounter] & 0x1fff;
		}
		if (primMsg->OutputType & OT_DATAVALUE) {
			len = *((uint16_t *) pos); pos += 2;
			(*strValues)[tmpResultCounter] = string((char *) pos, len); pos += len;
		}
 		//cout << "  stored " << (*strValues)[tmpResultCounter] << endl;
		/* XXXPAT: disclaimer: this is how we do it in DictionaryStep; don't know
			if it's necessary or not yet */
		if ((bpp->absRids[tmpResultCounter] & 0x8000000000000000LL) != 0) {
			if (primMsg->OutputType & OT_DATAVALUE)
				(*strValues)[tmpResultCounter] = joblist::CPNULLSTRMARK.c_str();
			bpp->absRids[tmpResultCounter] &= 0x7FFFFFFFFFFFFFFFLL;
		}
	}
}

void DictStep::projectResult(string *strings)
{
	uint i;
	uint8_t *pos;
	uint16_t len;
	DictOutput *header = (DictOutput *) &result[0];

	if (header->NVALS == 0) return;

	pos = &result[sizeof(DictOutput)];
	//cout << "projectResult() l: " << primMsg->LBID << " NVALS: " << header->NVALS << endl;
	for (i = 0; i < header->NVALS; i++) {
		len = *((uint16_t *) pos); pos += 2;
		strings[tmpResultCounter++] = string((char *) pos, len);
 		//cout << "serialized length is " << len << " string is " << string((char *) pos, len) << endl;
		pos += len;
		totalResultLength += len + 4;
	}
}

void DictStep::execute()
{
    if (fFilterFeeder == LEFT_FEEDER)
		strValues = &(bpp->fFiltStrValues[0]);
    else if (fFilterFeeder == RIGHT_FEEDER)
		strValues = &(bpp->fFiltStrValues[1]);
	else
		strValues = &(bpp->strValues);
	_execute();
}

void DictStep::_execute()
{
	/* Need to loop over bpp->values, issuing a primitive for each LBID */
	uint64_t i;
	int64_t l_lbid;
	OldGetSigParams *pt;
	boost::scoped_array<OrderedToken> newRidList;

	// make the OrderedToken list
	newRidList.reset(new OrderedToken[bpp->ridCount]);
	for (i = 0; i < bpp->ridCount; i++) {
		newRidList[i].rid = bpp->absRids[i];
		newRidList[i].token = bpp->values[i];
		newRidList[i].pos = i;
	}
	sort(&newRidList[0], &newRidList[bpp->ridCount], TokenSorter());

	tmpResultCounter = 0;
	i = 0;
	while (i < bpp->ridCount) {
		l_lbid = ((int64_t) newRidList[i].token) >> 10;
		primMsg->LBID = l_lbid;
		primMsg->NVALS = 0;

		/* When this is used as a filter, the strings can be thrown out.  JLF currently
		 * constructs joblists s.t. only a FilterCommand will use the strings.
		 */
		primMsg->OutputType = (fFilterFeeder == NOT_FEEDER ? OT_RID : OT_RID | OT_DATAVALUE);

		pt = (OldGetSigParams *) (primMsg->tokens);
		while (i < bpp->ridCount && ((((int64_t) newRidList[i].token) >> 10) == l_lbid )) {
			if (UNLIKELY(l_lbid < 0))
				pt[primMsg->NVALS].rid =
				  (fFilterFeeder == NOT_FEEDER ? newRidList[i].rid : i)
					| 0x8000000000000000LL;
			else
				pt[primMsg->NVALS].rid =
				  (fFilterFeeder == NOT_FEEDER ? newRidList[i].rid : i);
			pt[primMsg->NVALS].offsetIndex = newRidList[i].token & 0x3ff;
 			assert(pt[primMsg->NVALS].offsetIndex != 0);
			primMsg->NVALS++;
			i++;
		}

		memcpy(&pt[primMsg->NVALS], filterString.buf(), filterString.length());
		issuePrimitive(true);
		if (fFilterFeeder == NOT_FEEDER)
			processResult();
		else
			copyResultToTmpSpace(newRidList.get());
	}

	inputRidCount = bpp->ridCount;
	bpp->ridCount = tmpResultCounter;

	// check if feeding a filtercommand
    if (fFilterFeeder != NOT_FEEDER) {
    	sort(&newRidList[0], &newRidList[inputRidCount], PosSorter());
    	copyResultToFinalPosition(newRidList.get());
        copyRidsForFilterCmd();
    }
 	//cout << "DS: /_execute()\n";
}

/* This will do the same thing as execute() but put the result in bpp->serialized */
void DictStep::_project()
{
	/* Need to loop over bpp->values, issuing a primitive for each LBID */
	uint i;
	int64_t l_lbid=0;
	OldGetSigParams *pt;
	string tmpStrings[LOGICAL_BLOCK_RIDS];
	boost::scoped_array<OrderedToken> newRidList;

	// make the OrderedToken list
	newRidList.reset(new OrderedToken[bpp->ridCount]);
	for (i = 0; i < bpp->ridCount; i++) {
		newRidList[i].rid = bpp->absRids[i];
		newRidList[i].token = values[i];
		newRidList[i].pos = i;
	}

 	//cout << "DS: _project()\n";
	tmpResultCounter = 0;
	totalResultLength = 0;
	i = 0;
	while (i < bpp->ridCount) {
		l_lbid = ((int64_t) newRidList[i].token) >> 10;
		primMsg->LBID = l_lbid;
		primMsg->NVALS = 0;
		primMsg->OutputType = OT_DATAVALUE;
		pt = (OldGetSigParams *) (primMsg->tokens);
		//@bug 972
		while (i < bpp->ridCount && ((((int64_t) newRidList[i].token) >> 10) == l_lbid)) {
			if (l_lbid < 0)
				pt[primMsg->NVALS].rid = newRidList[i].rid | 0x8000000000000000LL;
			else
				pt[primMsg->NVALS].rid = newRidList[i].rid;
			pt[primMsg->NVALS].offsetIndex = newRidList[i].token & 0x3ff;
			assert(pt[primMsg->NVALS].offsetIndex > 0);
			primMsg->NVALS++;
			i++;
		}
		memcpy(&pt[primMsg->NVALS], filterString.buf(), filterString.length());
		issuePrimitive(false);
		projectResult(tmpStrings);
	}
	assert(tmpResultCounter == bpp->ridCount);
	*bpp->serialized << totalResultLength;
 	//cout << "_project() total length = " << totalResultLength << endl;
	for (i = 0; i < tmpResultCounter; i++) {
 		//cout << "serializing " << tmpStrings[i] << endl;
		*bpp->serialized << tmpStrings[i];
	}
 	//cout << "DS: /_project() l: " << l_lbid << endl;
}

void DictStep::_projectToRG(RowGroup &rg, uint col)
{
	/* Need to loop over bpp->values, issuing a primitive for each LBID */
	uint i;
	int64_t l_lbid=0;
	int64_t o_lbid=0;
	OldGetSigParams *pt;
	string tmpStrings[LOGICAL_BLOCK_RIDS];
	rowgroup::Row r;
	boost::scoped_array<OrderedToken> newRidList;

	// make the OrderedToken list
	newRidList.reset(new OrderedToken[bpp->ridCount]);
	for (i = 0; i < bpp->ridCount; i++) {
		newRidList[i].rid = bpp->absRids[i];
		newRidList[i].token = values[i];
		newRidList[i].pos = i;
	}
	sort(&newRidList[0], &newRidList[bpp->ridCount], TokenSorter());

	tmpResultCounter = 0;
	totalResultLength = 0;
	i = 0;
 	//cout << "DS: projectingToRG rids: " << bpp->ridCount << endl;
	while (i < bpp->ridCount) {
		l_lbid = ((int64_t) newRidList[i].token) >> 10;
		primMsg->LBID = l_lbid;
		primMsg->NVALS = 0;
		primMsg->OutputType = OT_DATAVALUE;
		pt = (OldGetSigParams *) (primMsg->tokens);
		//@bug 972
		//@bug 1821
		while (i<bpp->ridCount && ((((int64_t)newRidList[i].token)>>10) == l_lbid || l_lbid == -1
			|| ((((int64_t)newRidList[i].token)>>10) & 0x8000000000000000LL)) )
		{
			//@bug 1821
			if (newRidList[i].token==0)
			{
				ostringstream oss;
				ostringstream oss2;
				oss << l_lbid;
				logging::Message::Args args;
				args.add("0");
				args.add(oss.str());
				oss2 << newRidList[i].rid;
				args.add(oss2.str());
				primitiveprocessor::mlp->logMessage(logging::M0068, args, true);
				newRidList[i].token=0xfffffffffffffffeLL;
				values[newRidList[i].pos]=0xfffffffffffffffeLL;
			}

			if ((((int64_t)newRidList[i].token)>>10) < 0) {
				pt[primMsg->NVALS].rid = newRidList[i].rid | 0x8000000000000000LL;
			}
			else
			{
				if ((((int64_t)newRidList[i].token)>>10)>0 && o_lbid==0)
					l_lbid=o_lbid=(((int64_t)newRidList[i].token)>>10);

				pt[primMsg->NVALS].rid = newRidList[i].rid;
			}
			pt[primMsg->NVALS].offsetIndex = newRidList[i].token & 0x3ff;
			assert(pt[primMsg->NVALS].offsetIndex > 0);
			primMsg->NVALS++;
// 			pt++;
			i++;
		}
		
		if (((int64_t)primMsg->LBID)<0 && o_lbid>0)
			primMsg->LBID = o_lbid;

		memcpy(&pt[primMsg->NVALS], filterString.buf(), filterString.length());
		issuePrimitive(false);
		projectResult(tmpStrings);
		o_lbid=0;
 		//cout << "DS: project & issue l: " << (int64_t)primMsg->LBID << " NVALS: " << primMsg->NVALS << endl;
	}

 	//cout << "_projectToRG() total length = " << totalResultLength << endl;
	assert(tmpResultCounter == bpp->ridCount);
	rg.initRow(&r);
	rg.getRow(newRidList[0].pos, &r);
	if (rg.getColTypes()[col] != execplan::CalpontSystemCatalog::VARBINARY) {
		for (i = 0; i < tmpResultCounter; i++) {
 			//cout << "serializing " << tmpStrings[i] << endl;
			rg.getRow(newRidList[i].pos, &r);
			r.setStringField(tmpStrings[i], col);
		}
	}
	else {
		for (i = 0; i < tmpResultCounter; i++) {
			rg.getRow(newRidList[i].pos, &r);
			r.setVarBinaryField(tmpStrings[i], col);
		}
	}

 	//cout << "DS: /projectingToRG l: " << (int64_t)primMsg->LBID
	//	<< " len: " << tmpResultCounter
	//	<<  endl;
}

void DictStep::project()
{
	values = bpp->values;
	_project();
}

void DictStep::project(int64_t *vals)
{
	values = vals;
	_project();
}

void DictStep::projectIntoRowGroup(RowGroup &rg, uint col)
{
	values = bpp->values;
	_projectToRG(rg, col);
}

void DictStep::projectIntoRowGroup(RowGroup &rg, int64_t *vals, uint col)
{
	values = vals;
	_projectToRG(rg, col);
}

uint64_t DictStep::getLBID()
{
	return 0;
}

void DictStep::nextLBID()
{
}

SCommand DictStep::duplicate()
{
	SCommand ret;
	DictStep *ds;

	ret.reset(new DictStep());
	ds = (DictStep *) ret.get();
	ds->BOP = BOP;
	ds->fFilterFeeder = fFilterFeeder;
	ds->compressionType = compressionType;
	ds->hasEqFilter = hasEqFilter;
	ds->eqFilter = eqFilter;
	ds->eqOp = eqOp;
	ds->filterString = filterString;
	ds->filterCount = filterCount;
	ds->Command::duplicate(this);
	return ret;
}

bool DictStep::operator==(const DictStep &ds) const
{
	return ((BOP == ds.BOP) &&
		(fFilterFeeder == ds.fFilterFeeder) &&
		(compressionType == ds.compressionType) &&
		(filterString == ds.filterString) &&
		(filterCount == ds.filterCount));
}

bool DictStep::operator!=(const DictStep &ds) const
{
	return !(*this == ds);
}


};  // namespace
