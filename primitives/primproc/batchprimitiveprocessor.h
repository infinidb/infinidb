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
// $Id: batchprimitiveprocessor.h 1915 2012-08-01 15:54:40Z dhall $
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

#ifndef BATCHPRIMITIVEPROCESSOR_H_
#define BATCHPRIMITIVEPROCESSOR_H_

#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/scoped_ptr.hpp>
#ifndef _MSC_VER
#include <tr1/unordered_map>
#else
#include <unordered_map>
#endif
#include <boost/thread.hpp>

#include "errorcodes.h"
#include "serializeable.h"
#include "messagequeue.h"
#include "primitiveprocessor.h"
#include "command.h"
#include "umsocketselector.h"
#include "tuplejoiner.h"
#include "rowgroup.h"
#include "rowaggregation.h"
#include "funcexpwrapper.h"
#include "bppsendthread.h"

namespace primitiveprocessor
{
typedef std::tr1::unordered_map<int64_t, BRM::VSSData> VSSCache;
};

#include "primitiveserver.h"

namespace primitiveprocessor
{

typedef boost::shared_ptr<BatchPrimitiveProcessor> SBPP;

class scalar_exception : public std::exception { 
	const char * what() const throw() { return "Not a scalar subquery."; }
};

class NeedToRestartJob : public std::runtime_error
{
	public:
	NeedToRestartJob() : std::runtime_error("NeedToRestartJob") { }
	NeedToRestartJob(const std::string &s) :
		std::runtime_error(s) { }
};


class BatchPrimitiveProcessor
{
	public:
		BatchPrimitiveProcessor(messageqcpp::ByteStream &, double prefetchThresh,
			boost::shared_ptr<BPPSendThread>);

		~BatchPrimitiveProcessor();

		/* Interface used by primproc */
		void initBPP(messageqcpp::ByteStream &);
		void resetBPP(messageqcpp::ByteStream &, const SP_UM_MUTEX& wLock, const SP_UM_IOSOCK& outputSock);
		void addToJoiner(messageqcpp::ByteStream &);
		void endOfJoiner();
		int operator()();
		void setLBIDForScan(uint64_t rid);

		/* Duplicate() returns a deep copy of this object as it was init'd by initBPP.
			It's thread-safe wrt resetBPP. */
		SBPP duplicate();
		bool operator==(const BatchPrimitiveProcessor &) const;
		bool operator!=(const BatchPrimitiveProcessor &) const;

		inline uint getSessionID() { return sessionID; }
		inline uint getStepID() { return stepID; }
		inline uint32_t getUniqueID() { return uniqueID; }
		inline bool     busy()       { return fBusy; }
		inline void     busy(bool b) { fBusy = b; }

		uint16_t FilterCount() const {return filterCount;}
		uint16_t ProjectCount() const {return projectCount;}
		uint32_t PhysIOCount() const { return physIO;}
		uint32_t CachedIOCount() const { return cachedIO;}
		uint32_t BlocksTouchedCount() const { return touchedBlocks;}

		void setError(const std::string& error, logging::ErrorCodeValues errorCode) {}

	private:
		BatchPrimitiveProcessor();
		BatchPrimitiveProcessor(const BatchPrimitiveProcessor &);
		BatchPrimitiveProcessor& operator=(const BatchPrimitiveProcessor &);

		void initProcessor();
#ifdef PRIMPROC_STOPWATCH
		void execute(logging::StopWatch *stopwatch);
#else
		void execute();
#endif
		void writeProjectionPreamble();
		void makeResponse();
		void sendResponse();

		/* Used by scan operations to increment the LBIDs in successive steps */
		void nextLBID();

		/* these send relative rids, should this be abs rids? */
		void serializeElementTypes();
		void serializeStrings();

		void asyncLoadProjectColumns();
		void writeErrorMsg(const std::string& error, uint16_t errCode, bool logIt = true, bool critical = true);

		BPSOutputType ot;

		uint versionNum;
		uint txnID;
		uint sessionID;
		uint stepID;
		uint32_t uniqueID;

		// # of times to loop over the command arrays
		// ...  This is 1, except when the first command is a scan, in which case
		// this single BPP object produces count responses.
		uint16_t count;
		uint64_t baseRid;		// first rid of the logical block

		uint16_t relRids[LOGICAL_BLOCK_RIDS];
		int64_t  values[LOGICAL_BLOCK_RIDS];
		boost::scoped_array<uint64_t> absRids;
		boost::scoped_array<std::string> strValues;
		uint16_t ridCount;
		bool needStrValues;

		/* Common space for primitive data */
		static const uint BUFFER_SIZE = 65536;
		uint8_t blockData[BLOCK_SIZE * 8];
		boost::scoped_array<uint8_t> outputMsg;
		uint outMsgSize;

		std::vector<SCommand> filterSteps;
		std::vector<SCommand> projectSteps;
		//@bug 1136
		uint16_t filterCount;
		uint16_t projectCount;
		bool sendRidsAtDelivery;
		uint8_t ridMap;
		bool gotAbsRids;
		bool gotValues;

		bool hasScan;
		bool validCPData;
		int64_t minVal, maxVal;    // CP data from a scanned column
		uint64_t lbidForCP;

		// IO counters
		boost::mutex counterLock;
		uint busyLoaderCount;

		uint32_t physIO, cachedIO, touchedBlocks;

		SP_UM_IOSOCK sock;
		messageqcpp::SBS serialized;
		SP_UM_MUTEX  writelock;

		boost::mutex objLock;
		bool LBIDTrace;
		bool fBusy;

		/* Join support TODO: Make join ops a seperate Command class. */
		boost::shared_ptr<joiner::Joiner> joiner;
		std::vector<joblist::ElementType> smallSideMatches;
		bool doJoin;
		uint32_t joinerSize;
		uint16_t preJoinRidCount;
		boost::mutex addToJoinerLock;
		void executeJoin();

// 		uint ridsIn, ridsOut;

		//@bug 1051 FilterStep on PM
		bool hasFilterStep;
		bool filtOnString;
		boost::scoped_array<uint16_t> fFiltCmdRids[2];
		boost::scoped_array<int64_t> fFiltCmdValues[2];
		boost::scoped_array<std::string> fFiltStrValues[2];
		uint64_t fFiltRidCount[2];

		// query density threshold for prefetch & async loading
		double prefetchThreshold;
		
		/* RowGroup support */
		rowgroup::RowGroup inputRG, outputRG;
		boost::scoped_array<uint8_t> inRowGroupData, outRowGroupData;
		boost::shared_array<int> rgMap;  // maps input cols to output cols
		boost::shared_array<int> projectionMap;  // maps the projection steps to the output RG
		bool hasRowGroup;
		uint32_t valueColumn;
		void copyInputRGColumns();
		void initFromRowGroup();

		/* Rowgroups + join */
		typedef std::tr1::unordered_multimap<uint64_t, uint32_t,
				joiner::TupleJoiner::hasher, std::equal_to<uint64_t>,
				utils::SimpleAllocator<std::pair<const uint64_t, uint32_t> > > TJoiner;
	
		typedef std::tr1::unordered_multimap<joiner::TypelessData,
				uint32_t, joiner::TupleJoiner::hasher> TLJoiner;

		bool generateJoinedRowGroup(rowgroup::Row &baseRow, const uint depth = 0);
		/* generateJoinedRowGroup helper fcns & vars */
		void initGJRG();   // called once after joining
		void resetGJRG();   // called after every rowgroup returned by generateJRG
		boost::scoped_array<uint> gjrgPlaceHolders;
		uint gjrgRowNumber;
		bool gjrgFull;
		rowgroup::Row largeRow, joinedRow, baseJRow;
		boost::scoped_array<uint8_t> baseJRowMem;
		boost::scoped_array<uint8_t> joinedRGMem;
		boost::scoped_array<rowgroup::Row> smallRows;
		boost::shared_array<boost::shared_array<int> > gjrgMappings;

		boost::shared_array<boost::shared_ptr<TJoiner> > tJoiners;
		typedef std::vector<uint32_t> MatchedData[LOGICAL_BLOCK_RIDS];
		boost::shared_array<MatchedData> tSmallSideMatches;
		void executeTupleJoin();
		bool getTupleJoinRowGroupData;
		std::vector<rowgroup::RowGroup> smallSideRGs;
		rowgroup::RowGroup largeSideRG;
		boost::shared_array<boost::shared_array<uint8_t> > smallSideRowData;
		boost::shared_array<boost::shared_array<uint8_t> > smallNullRowData;
		boost::shared_array<uint64_t> ssrdPos;  // this keeps track of position when building smallSideRowData
		boost::shared_array<uint> smallSideRowLengths;
		boost::shared_array<joblist::JoinType> joinTypes;
		uint joinerCount;
		boost::shared_array<uint> tJoinerSizes;
		// LSKC[i] = the column in outputRG joiner i uses as its key column
		boost::shared_array<uint> largeSideKeyColumns;
		// KCPP[i] = true means a joiner uses projection step i as a key column
		boost::shared_array<bool> keyColumnProj;
		rowgroup::Row oldRow, newRow;  // used by executeTupleJoin()
		boost::shared_array<uint64_t> joinNullValues;
		boost::shared_array<bool> doMatchNulls;
		boost::scoped_array<boost::scoped_ptr<funcexp::FuncExpWrapper> > joinFEFilters;
		bool hasJoinFEFilters;
		bool hasSmallOuterJoin;

		/* extra typeless join vars & fcns*/
		boost::shared_array<bool> typelessJoin;
		boost::shared_array<std::vector<uint> > tlLargeSideKeyColumns;
		boost::shared_array<boost::shared_ptr<TLJoiner> > tlJoiners;
		boost::shared_array<uint> tlKeyLengths;
		inline void getJoinResults(const rowgroup::Row &r, uint jIndex, std::vector<uint32_t>& v);
		// these allocators hold the memory for the keys stored in tlJoiners
		boost::shared_array<utils::FixedAllocator> storedKeyAllocators;
		// these allocators hold the memory for the large side keys which are short-lived
		boost::scoped_array<utils::FixedAllocator> tmpKeyAllocators;

		/* PM Aggregation */
		rowgroup::RowGroup joinedRG;  // if there's a join, the rows are formatted with this
		rowgroup::SP_ROWAGG_PM_t fAggregator;
		rowgroup::RowGroup fAggregateRG;
		boost::scoped_array<uint8_t> fAggRowGroupData;
		boost::shared_array<boost::shared_ptr<utils::SimplePool> > _pools;

		/* OR hacks */
		uint8_t bop;   // BOP_AND or BOP_OR
		bool hasPassThru;
		uint8_t forHJ;

		boost::scoped_ptr<funcexp::FuncExpWrapper> fe1, fe2;
		rowgroup::RowGroup fe1Input, fe2Output, *fe2Input;
		// note, joinFERG is only for metadata, and is shared between BPPs
		boost::shared_ptr<rowgroup::RowGroup> joinFERG;
		boost::scoped_array<uint8_t> fe1Data, fe2Data, joinFERowData;
		boost::shared_array<int> projectForFE1;
		boost::shared_array<int> fe1ToProjection, fe2Mapping;   // RG mappings
		boost::scoped_array<boost::shared_array<int> > joinFEMappings;
		rowgroup::Row fe1In, fe1Out, fe2In, fe2Out, joinFERow;

		bool hasDictStep;

		primitives::PrimitiveProcessor pp;
		
		/* VSS cache members */
		VSSCache vssCache;
		void buildVSSCache(uint loopCount);
		
		/* To support limited DEC queues on the PM */		
		boost::shared_ptr<BPPSendThread> sendThread;
		bool newConnection;   // to support the load balancing code in sendThread
		
		/* To support reentrancy */
		uint currentBlockOffset;
		boost::scoped_array<uint64_t> relLBID;
		boost::scoped_array<bool> asyncLoaded;
		
		/* To support a smaller memory footprint when idle */
		static const uint64_t maxIdleBufferSize = 16*1024*1024;  // arbitrary
		void allocLargeBuffers();
		void freeLargeBuffers();

		/* To ensure all packets of an LBID go out the same socket */
		int sockIndex;

		friend class Command;
		friend class ColumnCommand;
		friend class DictStep;
		friend class PassThruCommand;
		friend class RTSCommand;
		friend class FilterCommand;
		friend class ScaledFilterCmd;
		friend class StrFilterCmd;
};

}

#endif
