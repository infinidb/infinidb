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

//
// $Id: batchprimitiveprocessor-jl.h 9705 2013-07-17 20:06:07Z pleblanc $
// C++ Interface: batchprimitiveprocessor
//
// Description:
//
//
// Author: Patrick LeBlanc <pleblanc@calpont.com>, (C) 2008
//
// Copyright: See COPYING file that comes with this distribution
//
//
/** @file */

#ifndef BATCHPRIMITIVEPROCESSORJL_H_
#define BATCHPRIMITIVEPROCESSORJL_H_

#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>

#include "primitivemsg.h"
#include "serializeable.h"
#include "primitivestep.h"
#include "brm.h"
#include "command-jl.h"
#include "resourcemanager.h"
//#include "tableband.h"

namespace joblist
{
const uint LOGICAL_BLOCKS_CONVERTER = 23;  		// 10 + 13.  13 to convert to logical blocks,
												// 10 to convert to groups of 1024 logical blocks

// forward reference
struct JobInfo;

class BatchPrimitiveProcessorJL
{
public:
	/* Constructor used by the JobStep */
	explicit BatchPrimitiveProcessorJL(const ResourceManager& rm);
	~BatchPrimitiveProcessorJL();

	/* Interface used by the JobStep */

	/* Some accessors */
	inline bool hasScan() { return _hasScan; }

	/* For initializing the object */
	inline void setSessionID(uint num) { sessionID = num; }
	inline void setStepID(uint num) { stepID = num; }
	inline void setUniqueID(uint32_t id) { uniqueID = id; }
	inline void setQueryContext(const BRM::QueryContext &qc) { versionInfo = qc; }
	inline void setTxnID(uint num) { txnID = num; }
	inline void setOutputType(BPSOutputType o) { ot = o;
		if (ot == TUPLE || ot == ROW_GROUP) needRidsAtDelivery = true; }
	inline void setNeedRidsAtDelivery(bool b) { needRidsAtDelivery = b; }
	inline void setTraceFlags(uint32_t flags) {
		LBIDTrace = ((flags & execplan::CalpontSelectExecutionPlan::TRACE_LBIDS) != 0);
	}
	inline uint getRidCount() { return ridCount; }
	inline void setThreadCount(uint tc) { threadCount = tc; }

	void addFilterStep(const pColScanStep &, std::vector<BRM::LBID_t> lastScannedLBID);
	void addFilterStep(const pColStep &);
	void addFilterStep(const pDictionaryStep &);
	void addFilterStep(const FilterStep &);
	void addProjectStep(const pColStep &);
	void addProjectStep(const PassThruStep &);
	void addProjectStep(const pColStep &, const pDictionaryStep &);
	void addProjectStep(const PassThruStep &, const pDictionaryStep &);

	void createBPP(messageqcpp::ByteStream &) const;
	void destroyBPP(messageqcpp::ByteStream &) const;

	void useJoiner(boost::shared_ptr<joiner::Joiner>);
	bool nextJoinerMsg(messageqcpp::ByteStream &);

	/* Call this one last */
	// void addDeliveryStep(const DeliveryStep &);

	/* At runtime, feed input here */
	void addElementType(const ElementType &, uint dbroot);
	void addElementType(const StringElementType &, uint dbroot);
	//void setRowGroupData(const rowgroup::RowGroup &);

	void runBPP(messageqcpp::ByteStream &, uint32_t pmNum);
	void abortProcessing(messageqcpp::ByteStream *);

	/* After serializing a BPP object, reset it and it's ready for more input */
	void reset();

	/* The JobStep calls these to initialize a BPP that starts with a column scan */
	void setLBID(uint64_t lbid, const BRM::EMEntry &scannedExtent);
	inline void setCount(uint16_t c) { idbassert(c > 0); count = c; }

	/* Turn a ByteStream into ElementTypes or StringElementTypes */
	void getElementTypes(messageqcpp::ByteStream &in, std::vector<ElementType> *out,
		bool *validCPData, uint64_t *lbid, int64_t *min, int64_t *max, uint32_t *cachedIO, uint32_t *physIO,
		uint32_t *touchedBlocks,
		uint16_t *preJoinRidCount) const;
	void getStringElementTypes(messageqcpp::ByteStream &in,
		std::vector<StringElementType> *out, bool *validCPData, uint64_t *lbid,
		int64_t *min, int64_t *max, uint32_t *cachedIO, uint32_t *physIO,
		uint32_t *touchedBlocks) const;
	/* (returns the row count) */
//	uint getTableBand(messageqcpp::ByteStream &in, messageqcpp::ByteStream *out,
//		bool *validCPData, uint64_t *lbid, int64_t *min, int64_t *max,
//		uint32_t *cachedIO, uint32_t *physIO, uint32_t *touchedBlocks) const;
	void getTuples(messageqcpp::ByteStream &in, std::vector<TupleType> *out,
		bool *validCPData, uint64_t *lbid, int64_t *min, int64_t *max,
		uint32_t *cachedIO,	uint32_t *physIO, uint32_t *touchedBlocks) const;
	void deserializeAggregateResults(messageqcpp::ByteStream *in,
		std::vector<rowgroup::RGData> *out) const;
	void getRowGroupData(messageqcpp::ByteStream &in, std::vector<rowgroup::RGData> *out,
		bool *validCPData, uint64_t *lbid, int64_t *min, int64_t *max,
		uint32_t *cachedIO,	uint32_t *physIO, uint32_t *touchedBlocks, bool *countThis,
		uint threadID) const;
	void deserializeAggregateResult(messageqcpp::ByteStream *in, 
		std::vector<rowgroup::RGData> *out) const;
	bool countThisMsg(messageqcpp::ByteStream &in) const;

	void setStatus(uint16_t s) {  status = s; }
	uint16_t  getStatus() const { return status; }
	void runErrorBPP(messageqcpp::ByteStream &);
//	uint getErrorTableBand(uint16_t error, messageqcpp::ByteStream *out);
//	boost::shared_array<uint8_t> getErrorRowGroupData(uint16_t error) const;
	rowgroup::RGData getErrorRowGroupData(uint16_t error) const;

	// @bug 1098
	std::vector<SCommand>& getFilterSteps() { return filterSteps; }
	std::vector<SCommand>& getProjectSteps() { return projectSteps; }

	std::string toString() const;

	/* RowGroup additions */
	void setProjectionRowGroup(const rowgroup::RowGroup &rg);
	void setInputRowGroup(const rowgroup::RowGroup &rg);

	/* Aggregation */
	void addAggregateStep(const rowgroup::SP_ROWAGG_PM_t&, const rowgroup::RowGroup&);
	void setJoinedRowGroup(const rowgroup::RowGroup &rg);

	/* Tuple hashjoin */
	void useJoiners(const std::vector<boost::shared_ptr<joiner::TupleJoiner> > &);
	bool nextTupleJoinerMsg(messageqcpp::ByteStream &);
// 	void setSmallSideKeyColumn(uint col);

	/* OR hacks */
	void setBOP(uint op);   // BOP_AND or BOP_OR, default is BOP_AND
	void setForHJ(bool b);  // default is false

	/* self-join */
	void jobInfo(const JobInfo* jobInfo) { fJobInfo = jobInfo; }

	/* Function & Expression support */
	void setFEGroup1(boost::shared_ptr<funcexp::FuncExpWrapper>,
	  const rowgroup::RowGroup &input);
	void setFEGroup2(boost::shared_ptr<funcexp::FuncExpWrapper>,
	  const rowgroup::RowGroup &output);
	void setJoinFERG(const rowgroup::RowGroup &rg);

	/* This fcn determines whether or not the containing TBPS obj will process results
	from a join or put the RG data right in the output datalist. */
	bool pmSendsFinalResult() const
	{
		return ((sendTupleJoinRowGroupData && tJoiners.size() > 0 && PMJoinerCount > 0) || tJoiners.size() == 0);
	}
	const std::string toMiniString() const;

	void priority(uint p) { _priority = p; };
	uint priority() { return _priority; }

	void deliverStringTableRowGroup(bool b);

private:
	//void setLBIDForScan(uint64_t rid, uint dbroot);

	BPSOutputType ot;

	bool needToSetLBID;

	BRM::QueryContext versionInfo;
	uint txnID;
	uint sessionID;
	uint stepID;
	uint32_t uniqueID;

	// # of times to loop over the command arrays
	// ...  This is 1, except when the first command is a scan, in which case
	// this single BPP object produces count responses.
	uint16_t count;

	/* XXXPAT: tradeoff here.  Memory wasted by statically allocating all of these
		arrays on the UM (most aren't used) vs more dynamic allocation
		on the PM */

	uint64_t baseRid;	// first abs RID of the logical block

	uint16_t relRids[LOGICAL_BLOCK_RIDS];
	boost::scoped_array<uint64_t> absRids;
	uint64_t values[LOGICAL_BLOCK_RIDS];
	uint16_t ridCount;
	bool needStrValues;

	std::vector<SCommand> filterSteps;
	std::vector<SCommand> projectSteps;
	//@bug 1136
	uint16_t filterCount;
	uint16_t projectCount;
	bool needRidsAtDelivery;
	uint8_t ridMap;

//	TableBand templateTB;
	uint32_t tableOID;
	boost::scoped_array<int> tablePositions;
	uint tableColumnCount;
	bool sendValues;
	bool sendAbsRids;
	bool _hasScan;
	bool LBIDTrace;

	/* for tuple return type */
	std::vector<uint16_t> colWidths;
	uint tupleLength;
// 		uint rowCounter;    // for debugging
// 		uint rowsProcessed;
	uint16_t status;

	/* for Joiner serialization */
	uint32_t pos, joinerNum;
	boost::shared_ptr<joiner::Joiner> joiner;
	boost::shared_ptr<std::vector<ElementType> > smallSide;

	/* for RowGroup return type */
	rowgroup::RowGroup inputRG, projectionRG;
	bool sendRowGroups;
	uint32_t valueColumn;

	/* for PM Aggregation */
	rowgroup::RowGroup joinedRG;
	rowgroup::SP_ROWAGG_PM_t aggregatorPM;
	rowgroup::RowGroup aggregateRGPM;

	/* UM portion of the PM join alg */
	std::vector<boost::shared_ptr<joiner::TupleJoiner> > tJoiners;
	std::vector<rowgroup::RowGroup> smallSideRGs;
	rowgroup::RowGroup largeSideRG;
	std::vector<std::vector<uint> > smallSideKeys;
	boost::scoped_array<uint> tlKeyLens;
	bool sendTupleJoinRowGroupData;
	uint PMJoinerCount;

	/* OR hack */
	uint8_t bop;   // BOP_AND or BOP_OR
	bool    forHJ; // indicate if feeding a hashjoin, doJoin does not cover smallside

	/* Self-join */
	const JobInfo* fJobInfo;

	/* Functions & Expressions support */
	boost::shared_ptr<funcexp::FuncExpWrapper> fe1, fe2;
	rowgroup::RowGroup fe1Input, fe2Output;
	rowgroup::RowGroup joinFERG;

	mutable boost::scoped_array<rowgroup::RowGroup> primprocRG;   // the format of the data received from PrimProc
	uint threadCount;

	unsigned fJoinerChunkSize;
	uint dbRoot;
	bool hasSmallOuterJoin;

	uint _priority;

	friend class CommandJL;
	friend class ColumnCommandJL;
	friend class PassThruCommandJL;
};

}

#endif
// vim:ts=4 sw=4:

