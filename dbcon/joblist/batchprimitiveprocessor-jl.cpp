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
// $Id: batchprimitiveprocessor-jl.cpp 8775 2012-08-01 16:12:11Z dhall $
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

#include <unistd.h>
//#define NDEBUG
#include <cassert>
#include <stdexcept>
#include <iostream>
#include <set>
using namespace std;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "bpp-jl.h"
#include "jlf_common.h"
using namespace messageqcpp;
using namespace rowgroup;
using namespace joiner;

namespace joblist
{

BatchPrimitiveProcessorJL::BatchPrimitiveProcessorJL(const ResourceManager& rm) :
	ot(BPS_ELEMENT_TYPE),
	needToSetLBID(true),
	count(1),
	baseRid(0),
	ridCount(0),
	needStrValues(false),
	filterCount(0),
	projectCount(0),
	needRidsAtDelivery(false),
	ridMap(0),
	sendValues(false),
	sendAbsRids(false),
	_hasScan(false),
	LBIDTrace(false),
	tupleLength(0),
	status(0),
	sendRowGroups(false),
	valueColumn(0),
	sendTupleJoinRowGroupData(false),
	bop(BOP_AND),
	forHJ(false),
	fJoinerChunkSize(rm.getJlJoinerChunkSize()),
	hasSmallOuterJoin(false),
	_priority(1)
{
    PMJoinerCount = 0;
}

BatchPrimitiveProcessorJL::~BatchPrimitiveProcessorJL()
{
}

void BatchPrimitiveProcessorJL::addFilterStep(const pColScanStep &scan)
{
	SCommand cc;

	tableOID = scan.tableOid();
	cc.reset(new ColumnCommandJL(scan));
	cc->setBatchPrimitiveProcessor(this);
	filterSteps.push_back(cc);
	filterCount++;
	_hasScan = true;
	idbassert(sessionID == scan.sessionId());
}

void BatchPrimitiveProcessorJL::addFilterStep(const pColScanStep &scan, vector<BRM::LBID_t> lastScannedLBID)
{
	SCommand cc;

	tableOID = scan.tableOid();
	cc.reset(new ColumnCommandJL(scan, lastScannedLBID));
	cc->setBatchPrimitiveProcessor(this);
	filterSteps.push_back(cc);
	filterCount++;
	_hasScan = true;
	idbassert(sessionID == scan.sessionId());
}

void BatchPrimitiveProcessorJL::addFilterStep(const pColStep &step)
{
	SCommand cc;

	tableOID = step.tableOid();
	cc.reset(new ColumnCommandJL(step));
	cc->setBatchPrimitiveProcessor(this);
	filterSteps.push_back(cc);
	filterCount++;
	idbassert(sessionID == step.sessionId());
}

void BatchPrimitiveProcessorJL::addFilterStep(const pDictionaryStep &step)
{
	SCommand cc;

	tableOID = step.tableOid();
	if (filterCount == 0 && !sendRowGroups) {
		sendAbsRids = true;
		sendValues = true;
		absRids.reset(new uint64_t[8192]);
	}
	cc.reset(new DictStepJL(step));
	cc->setBatchPrimitiveProcessor(this);
	filterSteps.push_back(cc);
	filterCount++;
	needStrValues = true;
	idbassert(sessionID == step.sessionId());
}


void BatchPrimitiveProcessorJL::addFilterStep(const FilterStep& step)
{
	SCommand cc;

	tableOID = step.tableOid();
	cc.reset(new FilterCommandJL(step));
	cc->setBatchPrimitiveProcessor(this);
	filterSteps.push_back(cc);
	filterCount++;
	idbassert(sessionID == step.sessionId());
}

void BatchPrimitiveProcessorJL::addProjectStep(const pColStep &step)
{
	SCommand cc;

	cc.reset(new ColumnCommandJL(step));
	cc->setBatchPrimitiveProcessor(this);
	cc->setTupleKey(step.tupleId());
	projectSteps.push_back(cc);
	colWidths.push_back(cc->getWidth());
	tupleLength += cc->getWidth();
	projectCount++;
	idbassert(sessionID == step.sessionId());
}

void BatchPrimitiveProcessorJL::addProjectStep(const PassThruStep &step)
{
	SCommand cc;

	cc.reset(new PassThruCommandJL(step));
	cc->setBatchPrimitiveProcessor(this);
	cc->setTupleKey(step.tupleId());
	projectSteps.push_back(cc);
	colWidths.push_back(cc->getWidth());
	tupleLength += cc->getWidth();
	projectCount++;
	if (filterCount == 0 && !sendRowGroups)
		sendValues = true;
	idbassert(sessionID == step.sessionId());
}

void BatchPrimitiveProcessorJL::addProjectStep(const pColStep &col,
	const pDictionaryStep &dict)
{
	SCommand cc;

	cc.reset(new RTSCommandJL(col, dict));
	cc->setBatchPrimitiveProcessor(this);
	cc->setTupleKey(dict.tupleId());
	projectSteps.push_back(cc);
	colWidths.push_back(cc->getWidth());
	tupleLength += cc->getWidth();
	projectCount++;
	needStrValues = true;
	idbassert(sessionID == col.sessionId());
	idbassert(sessionID == dict.sessionId());
}

void BatchPrimitiveProcessorJL::addProjectStep(const PassThruStep &p, 
	const pDictionaryStep &dict)
{
	SCommand cc;

	cc.reset(new RTSCommandJL(p, dict));
	cc->setBatchPrimitiveProcessor(this);
	cc->setTupleKey(dict.tupleId());
	projectSteps.push_back(cc);
	colWidths.push_back(cc->getWidth());
	tupleLength += cc->getWidth();
	projectCount++;
	needStrValues = true;
	if (filterCount == 0 && !sendRowGroups) {
		sendValues = true;
		sendAbsRids = true;
		absRids.reset(new uint64_t[8192]);
	}
	idbassert(sessionID == p.sessionId());
	idbassert(sessionID == dict.sessionId());
}

#if 0
void BatchPrimitiveProcessorJL::addDeliveryStep(const DeliveryStep &ds)
{
	idbassert(sessionID == ds.sessionId());

	uint i;

	templateTB = ds.fEmptyTableBand;
	tableOID = ds.fTableOID;
	
	tableColumnCount = templateTB.getColumnCount();
	idbassert(tableColumnCount > 0);
	tablePositions.reset(new int[tableColumnCount]);
	for (i = 0; i < tableColumnCount; i++)
		tablePositions[i] = -1;

	idbassert(projectCount <= projectSteps.size());

	/* In theory, tablePositions maps a column's table position to a projection step 
	 -1 means it's a null column */
	CalpontSystemCatalog::OID oid;
	int idx;
	for (i = 0; i < projectCount; i++)
	{
		oid = static_cast<CalpontSystemCatalog::OID>(projectSteps[i]->getOID());
		if (oid > 0)
		{
			idx = templateTB.find(oid);
			if (idx >= 0)
			{
				tablePositions[idx] = i;
			}
			else
			{
				cout << "BatchPrimitiveProcessorJL::addDeliveryStep(): didn't find OID " << oid <<
					" in tableband at pjstep idx " << i << endl;
			}
		}
		else
		{
			cout << "BatchPrimitiveProcessorJL::addDeliveryStep(): pjstep idx " << i <<
				" doesn't have a valid OID" << endl;
		}
	}
}
#endif

void BatchPrimitiveProcessorJL::addElementType(const ElementType &et, uint dbroot)
{
	uint i;
// 	rowCounter++;

	if (needToSetLBID) {
		needToSetLBID = false;
		for (i = 0; i < filterCount; ++i)
			filterSteps[i]->setLBID(et.first, dbroot);

		for (i = 0; i < projectCount; ++i)
			projectSteps[i]->setLBID(et.first, dbroot);

		baseRid = et.first & 0xffffffffffffe000ULL;
	}
	// TODO: get rid of magics
	if (sendAbsRids)
		absRids[ridCount] = et.first;
	else {
		relRids[ridCount] = et.first & 0x1fff;  // 8192 rows per logical block
		ridMap |= 1 << (relRids[ridCount] >> 10);	// LSB -> 0-1023, MSB -> 7168-8191
	}
	if (sendValues) {
// 		cout << "adding value " << et.second << endl;
		values[ridCount] = et.second;
	}
	ridCount++;
	idbassert(ridCount <= 8192);
}

void BatchPrimitiveProcessorJL::setRowGroupData(const rowgroup::RowGroup &rg)
{
	uint i;
	rowgroup::Row r;

	inputRG.setData(rg.getData());
	inputRG.initRow(&r);
	inputRG.getRow(0, &r);

	if (needToSetLBID) {
		needToSetLBID = false;
		for (i = 0; i < filterCount; ++i)
			filterSteps[i]->setLBID(r.getRid(), 1);

		for (i = 0; i < projectCount; ++i)
			projectSteps[i]->setLBID(r.getRid(), 1);

		baseRid = r.getRid() & 0xffffffffffffe000ULL;
	}
}

void BatchPrimitiveProcessorJL::addElementType(const StringElementType &et, uint dbroot)
{
	if (filterCount == 0)
		throw logic_error("BPPJL::addElementType(StringElementType): doesn't work without filter steps yet");
	addElementType(ElementType(et.first, et.first), dbroot);
}


/**
 * When the output type is ElementType, the messages have the following format:
 *
 * ISMPacketHeader (ignored)
 * PrimitiveHeader (ignored)
 * --
 * If the BPP starts with a scan
 *    casual partitioning data from the scanned column
 * Base Rid for the returned logical block
 * rid count
 * (rid count)x 16-bit rids
 * (rid count)x 64-bit values
 * If there's a join performed by this BPP
 *    pre-Join rid count   (for logging purposes only)
 *    small-side new match count
 *    (match count)x ElementTypes
 * cached IO count
 * physical IO count
 * block touches
 */

void BatchPrimitiveProcessorJL::getElementTypes(ByteStream &in,
	vector<ElementType> *out, bool *validCPData, uint64_t *lbid, int64_t *min,
	int64_t *max, uint32_t *cachedIO, uint32_t *physIO, uint32_t *touchedBlocks,
	uint16_t *preJoinRidCount) const
{
	uint i;
	uint16_t l_count;
	uint64_t l_baseRid;
	uint16_t *rids;
	uint64_t *vals;
	uint8_t *buf;
	uint64_t tmp64;
	uint8_t tmp8;

	/* PM join support */
	uint32_t jCount;
	ElementType *jet;

// 	cout << "get Element Types uniqueID=" << uniqueID << endl;
	/* skip the header */
	idbassert(in.length() > sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
	in.advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));

	if (_hasScan) {
		in >> tmp8;
		*validCPData = (tmp8 != 0);
		if (*validCPData) {
			in >> *lbid;
			in >> tmp64;
			*min = (int64_t) tmp64;
			in >> tmp64;
			*max = (int64_t) tmp64;
		}
		else
			in >> *lbid;
	}

	in >> l_baseRid;
	in >> l_count;
// 	rowsProcessed += l_count;
	idbassert(l_count <= 8192);
	out->resize(l_count);

	buf = (uint8_t *) in.buf();

	rids = (uint16_t *) buf;
	vals = (uint64_t *) (buf + (l_count << 1));
	idbassert(in.length() > (uint) ((l_count << 1) + (l_count << 3)) );
	in.advance((l_count << 1) + (l_count << 3));

	for (i = 0; i < l_count; ++i) {
		(*out)[i].first = rids[i] + l_baseRid;
// 		if (tableOID >= 3000)
// 			idbassert((*out)[i].first > 1023);
		(*out)[i].second = vals[i];
	}

	if (joiner.get() != NULL) {
		in >> *preJoinRidCount;
		in >> jCount;
		idbassert(in.length() > (jCount << 4));
		jet = (ElementType *) in.buf();
		for (i = 0; i < jCount; ++i)
			out->push_back(jet[i]);
		in.advance(jCount << 4);
	}
	else
		*preJoinRidCount = l_count;

	in >> *cachedIO;
	in >> *physIO;
	in >> *touchedBlocks;
// 	cout << "ET: got physIO=" << (int) *physIO << " cachedIO=" << 
// 		(int) *cachedIO << " touchedBlocks=" << (int) *touchedBlocks << endl;
	idbassert(in.length() == 0);
}

/**
 * When the output type is StringElementType the messages have the following format:
 *
 * ISMPacketHeader  (ignored)
 * PrimitiveHeader  (ignored)
 * ---
 * If the BPP starts with a scan
 *     Casual partitioning data from the column scanned
 * Rid count
 * (rid count)x 64-bit absolute rids
 * (rid count)x serialized strings
 * cached IO count
 * physical IO count
 * blocks touched
 */

void BatchPrimitiveProcessorJL::getStringElementTypes(ByteStream &in,
	vector<StringElementType> *out, bool *validCPData, uint64_t *lbid, int64_t *min,
	int64_t *max, uint32_t *cachedIO, uint32_t *physIO, uint32_t *touchedBlocks) const
{
	uint i;
	uint16_t l_count;
	uint64_t *l_absRids;
	uint64_t tmp64;
	uint8_t tmp8;

// 	cout << "get String ETs uniqueID\n";
	/* skip the header */
	in.advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));

	if (_hasScan) {
		in >> tmp8;
		*validCPData = (tmp8 != 0);
		if (*validCPData) {
			in >> *lbid;
			in >> tmp64;
			*min = (int64_t) tmp64;
			in >> tmp64;
			*max = (int64_t) tmp64;
		}
		else
			in >> *lbid;
	}

	in >> l_count;
// 	cout << "parsing " << l_count << " strings\n";
	l_absRids = (uint64_t *) in.buf();
	out->resize(l_count);
	in.advance(l_count << 3);
	for (i = 0; i < l_count; ++i) {
		(*out)[i].first = l_absRids[i];
		in >> (*out)[i].second;
	}
	in >> *cachedIO;
	in >> *physIO;
	in >> *touchedBlocks;
// 	cout << "SET: got physIO=" << (int) *physIO << " cachedIO=" << 
// 		(int) *cachedIO << " touchedBlocks=" << (int) *touchedBlocks << endl;
	idbassert(in.length() == 0);
}

/**
 * When the output type is Tuples, the input message format is the same
 * as when the output type is TableBands
 */

void BatchPrimitiveProcessorJL::getTuples(messageqcpp::ByteStream &in,
	std::vector<TupleType> *out, bool *validCPData, uint64_t *lbid, int64_t *min,
	int64_t *max, uint32_t *cachedIO, uint32_t *physIO, uint32_t *touchedBlocks) const
{
	uint i, j, pos, len;
	uint16_t l_rowCount;
	uint64_t l_baseRid;
	uint16_t *l_relRids;
	uint64_t absRids[8192];
	//const uint8_t* columnData[projectCount];
	const uint8_t** columnData = (const uint8_t**)alloca(projectCount * sizeof(uint8_t*));
	memset(columnData, 0, projectCount * sizeof(uint8_t*));
	const uint8_t *buf;
	//uint32_t colLengths[projectCount];
	uint32_t* colLengths = (uint32_t*)alloca(projectCount * sizeof(uint32_t));
	uint64_t tmp64;
	uint8_t tmp8;

// 	cout << "getTuples msg is " << in.length() << " bytes\n";
	/* skip the header */
	in.advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));

	if (_hasScan) {
		in >> tmp8;
		*validCPData = (tmp8 != 0);
		if (*validCPData) {
			in >> *lbid;
			in >> tmp64;
			*min = (int64_t) tmp64;
			in >> tmp64;
			*max = (int64_t) tmp64;
		}
		else
			in >> *lbid;
	}

	in >> l_rowCount;

// 	cout << "read " << l_rowCount << " rows\n";

	if (needRidsAtDelivery) {
		in >> l_baseRid;
		l_relRids = (uint16_t *) in.buf();
		for (i = 0; i < l_rowCount; i++)
			absRids[i] = l_relRids[i] + l_baseRid;
		in.advance(l_rowCount << 1);
	}
	
	/* Set up pointers to the column data */
	pos = 0;
	buf = in.buf();
	for (i = 0; i < projectCount; i++) {
		colLengths[i] = *((uint32_t *) &buf[pos]);
		pos += 4;
		columnData[i] = &buf[pos];
// 		cout << "column " << i << " is " << colLengths[i] << " long at " << pos << endl;
		pos += colLengths[i];
		idbassert(pos < in.length());
	}
	in.advance(pos);

	out->resize(l_rowCount);
	for (i = 0; i < l_rowCount; i++) {
		(*out)[i].first = absRids[i];
		(*out)[i].second = new char[tupleLength];
		for (j = 0, pos = 0; j < projectCount; j++) {
			idbassert(pos + colWidths[j] <= tupleLength);
			if (projectSteps[j]->getCommandType() == CommandJL::RID_TO_STRING) {
				len = *((uint32_t *) columnData[j]); columnData[j] += 4;
				memcpy(&(*out)[i].second[pos], columnData[j], len);
				pos += len;
				columnData[j] += len;

				// insert padding...
				memset(&(*out)[i].second[pos], 0, colWidths[j] - len);
				pos += colWidths[j] - len;
			}
			else {
				switch(colWidths[j]) {
					case 8: 
						*((uint64_t *) &(*out)[i].second[pos]) = *((uint64_t *) columnData[j]);
						columnData[j] += 8;
						pos += 8;
						break;
					case 4:
						*((uint32_t *) &(*out)[i].second[pos]) = *((uint32_t *) columnData[j]);
						columnData[j] += 4;
						pos += 4;
						break;
					case 2:
						*((uint16_t *) &(*out)[i].second[pos]) = *((uint16_t *) columnData[j]);
						columnData[j] += 2;
						pos += 4;
						break;
					case 1:
						(*out)[i].second[pos] = (char) *columnData[j];
						columnData[j]++;
						pos++;
						break;
					default:
						cout << "BPP::getTuples(): bad column width of " << colWidths[j]
							<< endl;
						throw logic_error("BPP::getTuples(): bad column width");
				}
			}
		}
	}

	in >> *cachedIO;
	in >> *physIO;
	in >> *touchedBlocks;
	idbassert(in.length() == 0);
}

bool BatchPrimitiveProcessorJL::countThisMsg(messageqcpp::ByteStream &in) const
{
	const uint8_t *data = in.buf();
	uint offset = sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader); // skip the headers

	if (_hasScan)
	{
		if (data[offset] != 0)
			offset += 25;  // skip the CP data
		else
			offset += 9;  // skip only the "valid CP data" & LBID bytes
	}
	idbassert(in.length() > offset);

	return (data[offset] != 0);
}

boost::shared_array<uint8_t>
BatchPrimitiveProcessorJL::getRowGroupData(messageqcpp::ByteStream &in,
	bool *validCPData, uint64_t *lbid, int64_t *min, int64_t *max, 
	uint32_t *cachedIO, uint32_t *physIO, uint32_t *touchedBlocks, bool *countThis,
	uint threadID) const
{
	messageqcpp::ByteStream::quadbyte len;
	uint64_t tmp64;
	uint8_t tmp8;
	const uint8_t *buf;
	boost::shared_array<uint8_t> ret;
	uint32_t rowCount;
	rowgroup::RowGroup org = projectionRG;   // can we avoid this assignment?

	if (in.length() == 0) {
		/* make an empty tableband */
		ret.reset(new uint8_t[org.getEmptySize()]);
		org.setData(ret.get());
		org.resetRowGroup(0);
		*cachedIO = 0;
		*physIO = 0;
		*touchedBlocks = 0;
		return ret;
	}

	/* skip the header */
	in.advance(sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));

	if (_hasScan) {
		in >> tmp8;
		*validCPData = (tmp8 != 0);
		if (*validCPData) {
			in >> *lbid;
			in >> tmp64;
			*min = (int64_t) tmp64;
			in >> tmp64;
			*max = (int64_t) tmp64;
		}
		else
			in >> *lbid;
	}

	in >> tmp8;
	*countThis = (tmp8 != 0);
	in >> len;
	/* XXXPAT!  TODO: Make an allocator compatible with shared_ptr for each
	rowgroup, tuned to issue chunks of rg.getMaxSize() bytes.
	shared_ptr for boost 1.33.1 link:
	http://www.boost.org/doc/libs/1_33_1/libs/smart_ptr/shared_ptr.htm
	*/
	ret.reset(new uint8_t[len]);
	buf = in.buf();
	memcpy(ret.get(), buf, len);
	in.advance(len);

	org.setData(ret.get());
	rowCount = org.getRowCount();
//	cout << "rowCount = " << rowCount << endl;
	bool pmSendsMatchesAnyway = (hasSmallOuterJoin && *countThis && PMJoinerCount > 0 &&
			(fe2 || aggregatorPM));

	if (!pmSendsFinalResult() || pmSendsMatchesAnyway) {
		boost::shared_array<vector<uint32_t> > joinResults;
		uint i, j;

		if (pmSendsMatchesAnyway) {
			uint16_t joinRowCount;
			in >> joinRowCount;
			rowCount = joinRowCount;
		}

		for (j = 0; j < PMJoinerCount; j++) {
			/* Reuse the result space if possible */
			joinResults = tJoiners[j]->getPMJoinArrays(threadID);
// 			cout << "deserializing PM join result\n";
			if (joinResults.get() == NULL) {
				joinResults.reset(new vector<uint32_t>[8192]);
				tJoiners[j]->setPMJoinResults(joinResults, threadID);
			}
			for (i = 0; i < rowCount; i++)
				deserializeInlineVector<uint32_t>(in, joinResults[i]);
			if (tJoiners[j]->smallOuterJoin())
				tJoiners[j]->markMatches(threadID, rowCount);
		}
	}
// 	else
// 		cout << "assuming no join or the PM sends joined rows\n";

	if (*countThis) {
// 		cout << "grabbing io stats\n";
		in >> *cachedIO;
		in >> *physIO;
		in >> *touchedBlocks;
	}
	else {
		*cachedIO = 0;
		*physIO = 0;
		*touchedBlocks = 0;
	}

//	if (in.length() != 0)
// 		cout << "there are " << in.length() << " bytes left!\n";
	idbassert(in.length() == 0);
	return ret;
}

boost::shared_array<uint8_t>
BatchPrimitiveProcessorJL::getErrorRowGroupData(uint16_t error) const
{
	boost::shared_array<uint8_t> ret;
	rowgroup::RowGroup rg;

	ret.reset(new uint8_t[rg.getEmptySize()]);
	rg.setData(ret.get());
	rg.resetRowGroup(0);
	rg.setStatus(error);
	return ret;
}

void BatchPrimitiveProcessorJL::reset()
{
	ridCount = 0;
	ridMap = 0;
	needToSetLBID = true;
}

void BatchPrimitiveProcessorJL::setLBID(uint64_t l, uint dbroot)
{
	dbRoot = dbroot;
	filterSteps[0]->setLBID(l, dbroot);
}

void BatchPrimitiveProcessorJL::setLBIDForScan(uint64_t rid, uint dbroot)
{
	uint i;

	baseRid = rid;
// 	cout << "scan set base RID to " << baseRid << endl;

	for (i = 1; i < filterCount; ++i)
		filterSteps[i]->setLBID(rid, dbroot);

	for (i = 0; i < projectCount; ++i) {
// 		cout << "setting lbid for projection #" << i << endl;
		projectSteps[i]->setLBID(rid, dbroot);
	}
}

string BatchPrimitiveProcessorJL::toString() const
{
	ostringstream ret;
	uint i;

	ret << "BatchPrimitiveProcessorJL:" << endl;
	if (!_hasScan) {
		if (sendValues)
			ret << "   -- serializing values" << endl;
		if (sendAbsRids)
			ret << "   -- serializing absolute rids" << endl;
		else
			ret << "   -- serializing relative rids" << endl;
	}
	else
		ret << "   -- scan driven" << endl;
	ret << "   " << (int) filterCount << " filter steps:\n";
	for (i = 0; i < filterCount; i++)
		ret << "      " << filterSteps[i]->toString() << endl;
	ret << "   " << (int) projectCount << " projection steps:\n";
	for (i = 0; i < projectCount; i++)
		ret << "      " << projectSteps[i]->toString() << endl;
	return ret.str();
}

/**
 * The creation messages have the following format:
 *
 * ISMPacketHeader (necessary for DEC and ReadThread classes)
 * ---
 * output type (ElementType, StringElementType, etc)
 * version #
 * transaction #
 * session #  (unnecessary except for debugging)
 * step ID    ( same )
 * unique ID  (uniquely identifies the DEC queue on the UM)
 * 8-bit flags
 * if there's a join...
 *      # of elements in the Joiner object
 *       a flag whether or not to match every element (for inner joins)
 * filter step count
 * (filter count)x serialized Commands
 * projection step count
 * (projection count)x serialized Commands
 */

void BatchPrimitiveProcessorJL::createBPP(ByteStream &bs) const
{
	ISMPacketHeader ism;
	uint i;
	uint8_t flags = 0;

	ism.Command = BATCH_PRIMITIVE_CREATE;

	bs.load((uint8_t *) &ism, sizeof(ism));
	bs << (uint8_t) ot;
	bs << (messageqcpp::ByteStream::quadbyte)versionNum;
	bs << (messageqcpp::ByteStream::quadbyte)txnID;
	bs << (messageqcpp::ByteStream::quadbyte)sessionID;
	bs << (messageqcpp::ByteStream::quadbyte)stepID;
	bs << uniqueID;

	if (needStrValues)
		flags |= NEED_STR_VALUES;
	if (sendAbsRids)
		flags |= GOT_ABS_RIDS;
	if (sendValues)
		flags |= GOT_VALUES;
	if (LBIDTrace)
		flags |= LBID_TRACE;
	if (needRidsAtDelivery)
		flags |= SEND_RIDS_AT_DELIVERY;
	if (joiner.get() != NULL || tJoiners.size() > 0)
		flags |= HAS_JOINER;
	if (sendRowGroups)
		flags |= HAS_ROWGROUP;
	if (sendTupleJoinRowGroupData)
		flags |= JOIN_ROWGROUP_DATA;

	bs << flags;

	bs << bop;
	bs << (uint8_t) (forHJ ? 1 : 0);

	if (sendRowGroups) {
		bs << valueColumn;
		bs << inputRG;
	}

	if (ot == ROW_GROUP) {
		bs << projectionRG;
// 		cout << "BPPJL: projectionRG is:\n" << projectionRG.toString() << endl;

		/* F&E serialization */
		if (fe1) {
			bs << (uint8_t) 1;
			bs << *fe1;
			bs << fe1Input;
		}
		else
			bs << (uint8_t) 0;
		if (fe2) {
			bs << (uint8_t) 1;
			bs << *fe2;
			bs << fe2Output;
		}
		else
			bs << (uint8_t) 0;
	}

	/* if HAS_JOINER, send the init params */
	if (flags & HAS_JOINER) {
		if (ot == ROW_GROUP) {
			idbassert(tJoiners.size() > 0);
			bs << (messageqcpp::ByteStream::quadbyte)PMJoinerCount;
			
			bool atLeastOneFE = false;
#ifdef JLF_DEBUG
			cout << "PMJoinerCount = " << PMJoinerCount << endl;
#endif
			for (i = 0; i < PMJoinerCount; i++) {
				bs << (uint32_t) tJoiners[i]->size();
				bs << tJoiners[i]->getJoinType();
				
				//bs << (uint64_t) tJoiners[i]->smallNullValue();
				
				bs << (uint8_t) tJoiners[i]->isTypelessJoin();
				if (tJoiners[i]->hasFEFilter()) {
					atLeastOneFE = true;
#ifdef JLF_DEBUG
					cout << "serializing join FE object\n";
#endif
					bs << *tJoiners[i]->getFcnExpFilter();
				}
				if (!tJoiners[i]->isTypelessJoin()) {
					bs << (uint64_t) tJoiners[i]->smallNullValue();
					bs << (messageqcpp::ByteStream::quadbyte)tJoiners[i]->getLargeKeyColumn();
 					//cout << "large key column is " << (uint) tJoiners[i]->getLargeKeyColumn() << endl;
				}
				else {
					serializeVector<uint>(bs, tJoiners[i]->getLargeKeyColumns());
					bs << (uint32_t) tJoiners[i]->getKeyLength();
				}
			}
			if (atLeastOneFE)
				bs << joinFERG;
			if (sendTupleJoinRowGroupData) {
#ifdef JLF_DEBUG
				cout << "sending smallside data\n";
#endif
				serializeVector<RowGroup>(bs, smallSideRGs);
				bs << largeSideRG;
				bs << joinedRG;    // TODO: I think we can omit joinedRG if (!(fe2 || aggregatorPM))
// 				cout << "joined RG: " << joinedRG.toString() << endl;
			}
		}
		else {
			bs << (uint8_t) joiner->includeAll();
			bs << (uint32_t) joiner->size();
		}
	}

	bs << filterCount;
	for (i = 0; i < filterCount; ++i) {
// 		cout << "serializing step " << i << endl;
		filterSteps[i]->createCommand(bs);
	}
	bs << projectCount;
	for (i = 0; i < projectCount; ++i)
		projectSteps[i]->createCommand(bs);

	// aggregate step only when output is row group
	if (ot == ROW_GROUP)
	{
		if (aggregatorPM.get() != NULL)
		{
			bs << (uint8_t) 1;
			bs << aggregateRGPM;
			bs << *(aggregatorPM.get());
		}
		else
		{
			bs << (uint8_t) 0;
		}
	}
}

/**
 * The BPP Run messages have the following format:
 *
 * ISMPacketHeader
 *    -- Interleave field is used for DEC interleaving data
 *    -- Size field is used for the relative weight to process the message
 * -----
 * Session ID
 * Step ID
 * Unique ID
 * Sequence #
 * Iteration count
 * Rid count
 * If absolute rids are sent
 *    (rid count)x 64-bit absolute rids
 * else
 *    RID range bitmap
 *    base RID
 *    (rid count)x 16-bit relative rids
 * If values are sent
 *    (rid count)x 64-bit values
 * (filter count)x run msgs for filter Commands
 * (projection count)x run msgs for projection Commands
 */

void BatchPrimitiveProcessorJL::runBPP(ByteStream &bs, uint32_t pmNum)
{
	ISMPacketHeader ism;
	uint i;

/* XXXPAT: BPPJL currently reuses the ism Size fields for other things to
save bandwidth.  They're completely unused otherwise.  We need to throw out all unused
fields of every defined header. */

	bs.restart();

	memset((void*)&ism, 0, sizeof(ism));
	ism.Command = BATCH_PRIMITIVE_RUN;

	// TODO: this causes the code using this ism to send on only one socket, even if more than one socket is defined for each PM.
	ism.Interleave = pmNum;
//	ism.Interleave = pmNum - 1;

	/* ... and the Size field is used for the "weight" of processing a BPP
	  where 1 is one Command on one logical block. */
	ism.Size = count * (filterCount + projectCount);

	bs.append((uint8_t *) &ism, sizeof(ism));

	/* The next 4 vars are for BPPSeeder; BPP itself skips them */
	bs << sessionID;
	bs << stepID;
	bs << uniqueID;
	bs << _priority;

	bs << dbRoot;
	bs << count;

	if (_hasScan)
		idbassert(ridCount == 0);
	else if (!sendRowGroups)
		idbassert(ridCount > 0 && (ridMap != 0 || sendAbsRids));
	else
		idbassert(inputRG.getRowCount() > 0);

	if (sendRowGroups) {
		uint32_t rgSize = inputRG.getDataSize();
		bs << rgSize;
		bs.append(inputRG.getData(), rgSize);
	}
	else {
		bs << ridCount;
		if (sendAbsRids) 
			bs.append((uint8_t *) absRids.get(), ridCount << 3);
		else {
			bs << ridMap;
			bs << baseRid;
			bs.append((uint8_t *) relRids, ridCount << 1);
		}
		if (sendValues)
			bs.append((uint8_t *) values, ridCount << 3);
	}

	for (i = 0; i < filterCount; i++) 
		filterSteps[i]->runCommand(bs);
	for (i = 0; i < projectCount; i++)
		projectSteps[i]->runCommand(bs);
}


void BatchPrimitiveProcessorJL::runErrorBPP(ByteStream &bs)
{
	ISMPacketHeader ism;
	bs.restart();

	memset((void*)&ism, 0, sizeof(ism));
	ism.Command = BATCH_PRIMITIVE_RUN;
	ism.Status = status;
	ism.Size = sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader);

	bs.append((uint8_t *) &ism, sizeof(ism));

	bs << (messageqcpp::ByteStream::quadbyte)sessionID;
	bs << (messageqcpp::ByteStream::quadbyte)stepID;
	bs << uniqueID;
	bs << count;
	bs << ridCount;
}

void BatchPrimitiveProcessorJL::destroyBPP(ByteStream &bs) const
{
	ISMPacketHeader ism;

// 	if (!(sessionID & 0x80000000))
// 		cout << "step ID " << stepID << " added " << rowCounter << " rows, processed " 
// 			<< rowsProcessed << " rows" << endl;

	memset((void*)&ism, 0, sizeof(ism));
	ism.Command = BATCH_PRIMITIVE_DESTROY;

	bs.append((uint8_t *) &ism, sizeof(ism));
	/* XXXPAT: We can get rid of sessionID and stepID;
	it's there for easier debugging only */
	bs << (messageqcpp::ByteStream::quadbyte)sessionID;
	bs << (messageqcpp::ByteStream::quadbyte)stepID;
	bs << uniqueID;
}

void BatchPrimitiveProcessorJL::useJoiners(const vector<boost::shared_ptr<joiner::TupleJoiner> > &j)
{
	pos = 0;
	joinerNum = 0;
	tJoiners = j;

	PMJoinerCount = 0;
	tlKeyLens.reset(new uint[tJoiners.size()]);
	for (uint i = 0; i < tJoiners.size(); i++) {
		if (tJoiners[i]->inPM()) {
			PMJoinerCount++;
			smallSideKeys.push_back(tJoiners[i]->getSmallKeyColumns());
			smallSideRGs.push_back(tJoiners[i]->getSmallRG());
			if (tJoiners[i]->isTypelessJoin())
				tlKeyLens[i] = tJoiners[i]->getKeyLength();
			if (tJoiners[i]->hasFEFilter())
				sendTupleJoinRowGroupData = true;
			if (tJoiners[i]->smallOuterJoin())
				hasSmallOuterJoin = true;
		}
	}
	largeSideRG = tJoiners[0]->getLargeRG();
		
	if (aggregatorPM || fe2) {
		sendTupleJoinRowGroupData = true;
#ifdef JLF_DEBUG
		cout << "will send small side row data\n";
#endif
	}
}

/* This algorithm relies on the joiners being sorted by size atm */
bool BatchPrimitiveProcessorJL::nextTupleJoinerMsg(ByteStream &bs)
{
	uint32_t size = 0, toSend, i, j;
	ISMPacketHeader ism;
	Row r;
	std::vector<uint8_t *> *tSmallSide;
	joiner::TypelessData tlData;
	bool isNull;

	memset((void*)&ism, 0, sizeof(ism));
	tSmallSide = tJoiners[joinerNum]->getSmallSide();
	size = tSmallSide->size();
	if (joinerNum == PMJoinerCount-1 && pos == size) {
		/* last message */
// 		cout << "sending last joiner msg\n";
		ism.Command = BATCH_PRIMITIVE_END_JOINER;
		bs.load((uint8_t *) &ism, sizeof(ism));
		bs << (messageqcpp::ByteStream::quadbyte)sessionID;
		bs << (messageqcpp::ByteStream::quadbyte)stepID;
		bs << uniqueID;
		pos = 0;
		return false;
	}
	else if (pos == size) {
		joinerNum++;
		tSmallSide = tJoiners[joinerNum]->getSmallSide();
		size = tSmallSide->size();
		pos = 0;
	}

	ism.Command = BATCH_PRIMITIVE_ADD_JOINER;
	bs.load((uint8_t *) &ism, sizeof(ism));
	bs << (messageqcpp::ByteStream::quadbyte)sessionID;
	bs << (messageqcpp::ByteStream::quadbyte)stepID;
	bs << uniqueID;

	smallSideRGs[joinerNum].initRow(&r);

	unsigned metasize = 12; //12 = sizeof(struct JoinerElements)
	if (tJoiners[joinerNum]->isTypelessJoin()) metasize = 0;
	unsigned rowunits = fJoinerChunkSize / (r.getSize() + metasize);
	toSend = std::min<unsigned int>(size - pos, rowunits);
	bs << toSend;
	bs << joinerNum;

	if (tJoiners[joinerNum]->isTypelessJoin()) {
		utils::FixedAllocator fa(tlKeyLens[joinerNum], true);
		for (i = pos; i < pos + toSend; i++) {
			r.setData((*tSmallSide)[i]);
			isNull = false;
			for (j = 0; j < smallSideKeys[joinerNum].size(); j++)
				isNull |= r.isNullValue(smallSideKeys[joinerNum][j]);
			bs << (uint8_t) isNull;
			if (!isNull) {
				tlData = makeTypelessKey(r, smallSideKeys[joinerNum],
				  tlKeyLens[joinerNum], &fa);
				tlData.serialize(bs);
				bs << i;
			}
		}
	}
	else {
		#pragma pack(push,1)
		struct JoinerElements {
			int64_t key;
			uint32_t value;
		} *arr;
		#pragma pack(pop)
		bs.needAtLeast(toSend * sizeof(JoinerElements));
		arr = (JoinerElements *) bs.getInputPtr();

		for (i = pos, j = 0; i < pos + toSend; i++, j++) {
			r.setData((*tSmallSide)[i]);
			arr[j].key = r.getIntField(smallSideKeys[joinerNum][0]);
			arr[j].value = i;
// 			cout << "sending " << arr[j].key << ", " << arr[j].value << endl;
		}

		bs.advanceInputPtr(toSend * sizeof(JoinerElements));
	}

	if (sendTupleJoinRowGroupData) {
		uint32_t lpos;
		uint8_t *buf;

		bs.needAtLeast(r.getSize() * toSend);
		buf = (uint8_t *) bs.getInputPtr();
		for (i = pos, lpos = 0; i < pos + toSend; i++, lpos += r.getSize())
			memcpy(&buf[lpos], (*tSmallSide)[i], r.getSize());
		bs.advanceInputPtr(r.getSize() * toSend);
	}

	pos += toSend;
	return true;
}

#if 0
void BatchPrimitiveProcessorJL::setTupleJoinRowGroups(
	const rowgroup::RowGroup &smallRg, const rowgroup::RowGroup &largeRg)
{
	smallSideRG = smallRg;
	largeSideRG = largeRg;
}

void BatchPrimitiveProcessorJL::setSmallSideKeyColumn(uint col)
{
	smallSideKey = col;
}
#endif

void BatchPrimitiveProcessorJL::useJoiner(boost::shared_ptr<joiner::Joiner> j)
{
	pos = 0;
	joiner = j;
}

bool BatchPrimitiveProcessorJL::nextJoinerMsg(ByteStream &bs)
{
	uint32_t size, toSend;
	ISMPacketHeader ism;
	
	memset((void*)&ism, 0, sizeof(ism));
	if (smallSide.get() == NULL)
		smallSide = joiner->getSmallSide();

	size = smallSide->size();
	if (pos == size) {
		/* last message */
		ism.Command = BATCH_PRIMITIVE_END_JOINER;
		bs.load((uint8_t *) &ism, sizeof(ism));
		bs << (messageqcpp::ByteStream::quadbyte)sessionID;
		bs << (messageqcpp::ByteStream::quadbyte)stepID;
		bs << uniqueID;
		pos = 0;
		return false;
	}

	ism.Command = BATCH_PRIMITIVE_ADD_JOINER;
	bs.load((uint8_t *) &ism, sizeof(ism));
	bs << (messageqcpp::ByteStream::quadbyte)sessionID;
	bs << (messageqcpp::ByteStream::quadbyte)stepID;
	bs << uniqueID;

	toSend = (size - pos > 1000000 ? 1000000 : size - pos);
	bs << toSend;
	bs.append((uint8_t *) (&(*smallSide)[pos]), sizeof(ElementType) * toSend);
	pos += toSend;

	return true;
}

void BatchPrimitiveProcessorJL::setProjectionRowGroup(const rowgroup::RowGroup &rg)
{
	ot = ROW_GROUP;
	projectionRG = rg;
}

void BatchPrimitiveProcessorJL::setJoinedRowGroup(const rowgroup::RowGroup &rg)
{
	joinedRG = rg;
}

void BatchPrimitiveProcessorJL::setInputRowGroup(const rowgroup::RowGroup &rg)
{
	sendRowGroups = true;
	sendAbsRids = false;
	sendValues = false;
	inputRG = rg;
}

void BatchPrimitiveProcessorJL::addAggregateStep(const rowgroup::SP_ROWAGG_PM_t& aggpm,
												 const rowgroup::RowGroup &argpm)
{
	aggregatorPM = aggpm;
	aggregateRGPM = argpm;
	if (tJoiners.size() > 0)
		sendTupleJoinRowGroupData = true;
}

/* OR hacks */
void BatchPrimitiveProcessorJL::setBOP(uint op)
{
	bop = op;

	if (op == BOP_OR && filterCount > 1)
	{
		for (int i = 1; i < filterCount; ++i)
		{
			ColumnCommandJL* colcmd = dynamic_cast<ColumnCommandJL*>(filterSteps[i].get());
			if (colcmd != NULL)
				colcmd->scan(false);
		}
	}
}

void BatchPrimitiveProcessorJL::setForHJ(bool b)
{
	forHJ = b;
}

void BatchPrimitiveProcessorJL::setFEGroup1(boost::shared_ptr<funcexp::FuncExpWrapper> fe,
  const RowGroup &input)
{
	fe1 = fe;
	fe1Input = input;
}

void BatchPrimitiveProcessorJL::setFEGroup2(boost::shared_ptr<funcexp::FuncExpWrapper> fe,
  const RowGroup &output)
{
	fe2 = fe;
	fe2Output = output;
	if (tJoiners.size() > 0 && PMJoinerCount > 0)
		sendTupleJoinRowGroupData = true;
}

const string BatchPrimitiveProcessorJL::toMiniString() const
{
	ostringstream oss;
	int i;
	set<string> colset;
	string col;
	for (i = 0; i < filterCount; i++)
	{
		col = filterSteps[i]->getColName();
		// FilterCommandJL has two referenced columns, needs special handling.
		FilterCommandJL* filterCmd = dynamic_cast<FilterCommandJL*>(filterSteps[i].get());
		if (filterCmd == NULL)
		{
			colset.insert(col);
		}
		else
		{
			// is a FilterCommandJL
			size_t sep = col.find(',');
			colset.insert(col.substr(0, sep));
			if (sep != string::npos)
				colset.insert(col.substr(++sep));
		}
	}
	for (i = 0; i < projectCount; i++)
	{
		col = projectSteps[i]->getColName();
		colset.insert(col);
	}
	set<string>::iterator iter = colset.begin();
	oss << '(' << *iter++;
	while (iter != colset.end())
		oss << ',' << *iter++;
	oss << ')';

	return oss.str();
}

void BatchPrimitiveProcessorJL::setJoinFERG(const RowGroup &rg)
{
	joinFERG = rg;
}

void BatchPrimitiveProcessorJL::abortProcessing(ByteStream *bs)
{
	ISMPacketHeader ism;

	memset((void*)&ism, 0, sizeof(ism));
	ism.Command = BATCH_PRIMITIVE_ABORT;

	bs->load((uint8_t *) &ism, sizeof(ism));
	*bs << uniqueID;
}

};
