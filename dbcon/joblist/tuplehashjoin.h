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

//  $Id: tuplehashjoin.h 9655 2013-06-25 23:08:13Z xlou $


#ifndef TUPLEHASHJOIN_H_
#define TUPLEHASHJOIN_H_

#include "jobstep.h"
#include "calpontsystemcatalog.h"
#include "hasher.h"
#include "tuplejoiner.h"
#include <boost/shared_ptr.hpp>
#include <map>
#include <string>
#include <vector>
#include <utility>

namespace joblist
{
class BatchPrimitive;
class ResourceManager;
class TupleBPS;

class TupleHashJoinStep : public JobStep, public TupleDeliveryStep
{
public:
	/**
	 * @param
	 */
	TupleHashJoinStep(const JobInfo& jobInfo);
	virtual ~TupleHashJoinStep();

	void setLargeSideBPS(BatchPrimitive*);
	void setLargeSideStepsOut(const std::vector<SJSTEP>& largeSideSteps);
	void setSmallSideStepsOut(const std::vector<SJSTEP>& smallSideSteps);

	/* mandatory JobStep interface */
	void run();
	void join();
	const std::string toString() const;

	/* These tableOID accessors can go away soon */
	execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOID2; }
	execplan::CalpontSystemCatalog::OID tableOid1() const { return fTableOID1; }
	execplan::CalpontSystemCatalog::OID tableOid2() const { return fTableOID2; }
	void tableOid1(execplan::CalpontSystemCatalog::OID tableOid1)
		{ fTableOID1 = tableOid1; }
	void tableOid2(execplan::CalpontSystemCatalog::OID tableOid2)
		{ fTableOID2 = tableOid2; }

	std::string alias1() const { return fAlias1; }
	void alias1(const std::string& alias) { fAlias1 = alias; }
	std::string alias2() const { return fAlias2; }
	void alias2(const std::string& alias) { fAlias = fAlias2 = alias; }

	std::string view1() const { return fView1; }
	void view1(const std::string& vw) { fView1 = vw; }
	std::string view2() const { return fView2; }
	void view2(const std::string& vw) { fView = fView2 = vw; }

	std::string schema1() const { return fSchema1; }
	void schema1(const std::string& s) { fSchema1 = s; }
	std::string schema2() const { return fSchema2; }
	void schema2(const std::string& s) { fSchema = fSchema2 = s; }

	int32_t sequence1() const { return fSequence1; }
	void sequence1(int32_t seq) { fSequence1 = seq; }
	int32_t sequence2() const { return fSequence2; }
	void sequence2(int32_t seq) { fSequence2 = seq; }

	const execplan::ReturnedColumn* column1() const { return fColumn1; }
	void column1(const execplan::ReturnedColumn* pos) { fColumn1 = pos; }
	const execplan::ReturnedColumn* column2() const { return fColumn2; }
	void column2(const execplan::ReturnedColumn* pos) { fColumn2 = pos; }

	int correlatedSide() const { return fCorrelatedSide; }
	void correlatedSide(int c) { fCorrelatedSide = c; }

	uint64_t tupleId() const  { return fTupleId2; }
	uint64_t tupleId1() const  { return fTupleId1; }
	uint64_t tupleId2() const  { return fTupleId2; }
	void tupleId1(uint64_t id) { fTupleId1 = id; }
	void tupleId2(uint64_t id) { fTupleId2 = id; }

	void addSmallSideRG(const std::vector<rowgroup::RowGroup>& rgs,
						const std::vector<std::string> &tableNames);
	void addJoinKeyIndex(const std::vector<JoinType>& jt,
						 const std::vector<bool>& typeless,
						 const std::vector<std::vector<uint32_t> >& smallkeys,
						 const std::vector<std::vector<uint32_t> >& largekeys);

	void configSmallSideRG(const std::vector<rowgroup::RowGroup>& rgs,
						   const std::vector<std::string> &tableNames);
	void configLargeSideRG(const rowgroup::RowGroup &rg);

	void configJoinKeyIndex(const std::vector<JoinType>& jt,
							const std::vector<bool>& typeless,
							const std::vector<std::vector<uint32_t> >& smallkeys,
							const std::vector<std::vector<uint32_t> >& largekeys);

	void setOutputRowGroup(const rowgroup::RowGroup &rg);

	uint32_t nextBand(messageqcpp::ByteStream &bs);

	const rowgroup::RowGroup& getOutputRowGroup() const { return outputRG; }
	const rowgroup::RowGroup &getSmallRowGroup() const { return smallRGs[0]; }
	const std::vector<rowgroup::RowGroup> &getSmallRowGroups() const { return smallRGs; }
	const rowgroup::RowGroup& getLargeRowGroup() const { return largeRG; }
	const uint32_t getSmallKey() const { return smallSideKeys[0][0]; }
	const std::vector<std::vector<uint32_t> >& getSmallKeys() const { return smallSideKeys; }
	const std::vector<std::vector<uint32_t> >& getLargeKeys() const { return largeSideKeys; }

	/* Some compat fcns to get rid of later */
	void oid1(execplan::CalpontSystemCatalog::OID oid) { fOid1 = oid; }
	execplan::CalpontSystemCatalog::OID oid1() const { return fOid1; }
	void oid2(execplan::CalpontSystemCatalog::OID oid) { fOid2 = oid; }
	execplan::CalpontSystemCatalog::OID oid2() const { return fOid2; }
	void dictOid1(execplan::CalpontSystemCatalog::OID oid) { fDictOid1 = oid; }
	execplan::CalpontSystemCatalog::OID dictOid1() const { return fDictOid1; }
	void dictOid2(execplan::CalpontSystemCatalog::OID oid) { fDictOid2 = oid; }
	execplan::CalpontSystemCatalog::OID dictOid2() const { return fDictOid2; }

	/* The replacements.  I don't think there's a need for setters or vars.
		OIDs are already in the rowgroups. */
	// s - sth table pair; k - kth key in compound join, 0 for non-compand join
	execplan::CalpontSystemCatalog::OID smallSideKeyOID(uint32_t s, uint32_t k) const;
	execplan::CalpontSystemCatalog::OID largeSideKeyOID(uint32_t s, uint32_t k) const;

	void deliveryStep(const SJSTEP& ds) { fDeliveryStep = ds; }

	/* Iteration 18 mods */
	void setLargeSideDLIndex(uint32_t i) { largeSideIndex = i; }

	/* obsolete, need to update JLF */
	void setJoinType(JoinType jt) { joinType = jt; }
	JoinType getJoinType() const { return joinType; }


	/* Functions & Expressions interface */
	/* Cross-table Functions & Expressions in where clause */
	void addFcnExpGroup2(const boost::shared_ptr<execplan::ParseTree>& fe);
	bool hasFcnExpGroup2() { return (fe2 != NULL); }

	/* Functions & Expressions in select and groupby clause */
	void setFcnExpGroup3(const std::vector<execplan::SRCP>& fe);
	void setFE23Output(const rowgroup::RowGroup& rg);

	/* result rowgroup */
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	void  deliverStringTableRowGroup(bool b);
	bool  deliverStringTableRowGroup() const;

	// joinId
	void joinId(int64_t id) { fJoinId = id; }
	int64_t joinId() const { return fJoinId; }

	/* semi-join support */
	void addJoinFilter(boost::shared_ptr<execplan::ParseTree>, uint32_t index);
	bool hasJoinFilter() const { return (fe.size() > 0); }
	bool hasJoinFilter(uint32_t index) const;
	void setJoinFilterInputRG(const rowgroup::RowGroup &rg);

	/* UM Join logic */
	rowgroup::RGData joinOneRG(rowgroup::RGData input);

	virtual bool stringTableFriendly() { return true; }

	uint32_t tokenJoin() const { return fTokenJoin; }
	void tokenJoin(uint32_t k) { fTokenJoin = k;    }

private:
	TupleHashJoinStep();
	TupleHashJoinStep(const TupleHashJoinStep &);
	TupleHashJoinStep & operator=(const TupleHashJoinStep &);

	void errorLogging(const std::string& msg, int err) const;
	void startAdjoiningSteps();

	void formatMiniStats(uint32_t index);

	RowGroupDL *largeDL, *outputDL;
	std::vector<RowGroupDL *> smallDLs;
	std::vector<uint32_t> smallIts;
	uint32_t largeIt;

	JoinType joinType;   // deprecated
	std::vector<JoinType> joinTypes;
	execplan::CalpontSystemCatalog::OID fTableOID1;
	execplan::CalpontSystemCatalog::OID fTableOID2;
	execplan::CalpontSystemCatalog::OID fOid1;
	execplan::CalpontSystemCatalog::OID fOid2;

	// v-table string join
	execplan::CalpontSystemCatalog::OID fDictOid1;
	execplan::CalpontSystemCatalog::OID fDictOid2;

	std::string fAlias1;
	std::string fAlias2;

	std::string fView1;
	std::string fView2;

	std::string fSchema1;
	std::string fSchema2;

	int32_t fSequence1;
	int32_t fSequence2;

	// @bug3398, add tuple id to steps
	uint64_t fTupleId1;
	uint64_t fTupleId2;

	// @bug3524
	// for NOT IN subquery where correlate join in subquery is treated as additional comparison.
	// These simple columns are for converting join to expression.
	// DON'T delete, they owned by exec plan.
	const execplan::ReturnedColumn* fColumn1;
	const execplan::ReturnedColumn* fColumn2;

	int fCorrelatedSide;

	std::vector<bool> typelessJoin;     // the size of the vector is # of small side
	std::vector<std::vector<uint32_t> > largeSideKeys;
	std::vector<std::vector<uint32_t> > smallSideKeys;

	ResourceManager& resourceManager;
	volatile uint64_t totalUMMemoryUsage;

	struct JoinerSorter {
		inline bool operator()(const boost::shared_ptr<joiner::TupleJoiner> &j1,
		  const boost::shared_ptr<joiner::TupleJoiner> &j2) const
			{ return *j1 < *j2; }
	};
	std::vector<boost::shared_ptr<joiner::TupleJoiner> > joiners;

	boost::scoped_array<std::vector<rowgroup::RGData> > rgData;
	TupleBPS* largeBPS;
	rowgroup::RowGroup largeRG, outputRG;
	std::vector<rowgroup::RowGroup> smallRGs;
	uint64_t pmMemLimit;
	uint64_t rgDataSize;

	void hjRunner();
	void smallRunnerFcn(uint32_t index);

	struct HJRunner {
		HJRunner(TupleHashJoinStep *hj) : HJ(hj) { }
		void operator()() { HJ->hjRunner(); }
		TupleHashJoinStep *HJ;
	};
	struct SmallRunner {
		SmallRunner(TupleHashJoinStep *hj, uint32_t i) :HJ(hj), index(i) { }
		void operator()() { HJ->smallRunnerFcn(index); }
		TupleHashJoinStep *HJ;
		uint32_t index;
	};

	boost::shared_ptr<boost::thread> mainRunner;
	std::vector<boost::shared_ptr<boost::thread> > smallRunners;

	// for notify TupleAggregateStep PM hashjoin
	// Ideally, hashjoin and delivery communicate with RowGroupDL,
	// they don't need to know each other.
	// Due to dynamic PM/UM hashjoin selection and support PM aggregation,
	// delivery step need to know if raw or partially aggregated to process.
	SJSTEP fDeliveryStep;

	// temporary hack to make sure JobList only calls run, join once
	boost::mutex jlLock;
	bool runRan, joinRan;

	/* Iteration 18 mods */
	uint32_t largeSideIndex;
	bool joinIsTooBig;

	/* Functions & Expressions support */
	boost::shared_ptr<funcexp::FuncExpWrapper> fe2;
	std::vector<uint32_t> fe2TableDeps;
	rowgroup::RowGroup fe2Output;
	bool runFE2onPM;

	// Support Mixed Join Type
	int64_t fJoinId;

	/* Semi-join support */
	std::vector<int> feIndexes;
	std::vector<boost::shared_ptr<funcexp::FuncExpWrapper> > fe;
	rowgroup::RowGroup joinFilterRG;

	/* Casual Partitioning forwarding */
	void forwardCPData();
	uint32_t uniqueLimit;

	/* UM Join support.  Most of this code is ported from the UM join code in tuple-bps.cpp.
	 * They should be kept in sync as much as possible. */
	struct JoinRunner {
		JoinRunner(TupleHashJoinStep *hj, uint32_t i) : HJ(hj), index(i) { }
		void operator()() { HJ->joinRunnerFcn(index); }
		TupleHashJoinStep *HJ;
		uint32_t index;
	};
	void joinRunnerFcn(uint32_t index);
	void startJoinThreads();
	void generateJoinResultSet(const std::vector<std::vector<rowgroup::Row::Pointer> > &joinerOutput,
	  rowgroup::Row &baseRow, const boost::shared_array<boost::shared_array<int> > &mappings,
	  const uint32_t depth, rowgroup::RowGroup &outputRG, rowgroup::RGData &rgData,
	  std::vector<rowgroup::RGData> *outputData,
	  const boost::shared_array<rowgroup::Row> &smallRows, rowgroup::Row &joinedRow);
	void grabSomeWork(std::vector<rowgroup::RGData> *work);
	void sendResult(const std::vector<rowgroup::RGData> &res);
	void processFE2(rowgroup::RowGroup &input, rowgroup::RowGroup &output, rowgroup::Row &inRow,
	  rowgroup::Row &outRow, std::vector<rowgroup::RGData> *rgData,
	  funcexp::FuncExpWrapper* local_fe);
	void joinOneRG(uint32_t threadID, std::vector<rowgroup::RGData> *out,
	  rowgroup::RowGroup &inputRG, rowgroup::RowGroup &joinOutput, rowgroup::Row &largeSideRow,
	  rowgroup::Row &joinFERow, rowgroup::Row &joinedRow, rowgroup::Row &baseRow,
	  std::vector<std::vector<rowgroup::Row::Pointer> > &joinMatches,
	  boost::shared_array<rowgroup::Row> &smallRowTemplates);
	void finishSmallOuterJoin();
	void makeDupList(const rowgroup::RowGroup &rg);
	void processDupList(uint32_t threadID, rowgroup::RowGroup &ingrp,
		std::vector<rowgroup::RGData> *rowData);

	boost::scoped_array<boost::shared_ptr<boost::thread> > joinRunners;
	boost::mutex inputDLLock, outputDLLock;
	boost::shared_array<boost::shared_array<int> > columnMappings, fergMappings;
	boost::shared_array<int> fe2Mapping;
	uint32_t joinThreadCount;
	boost::scoped_array<boost::scoped_array<uint8_t> > smallNullMemory;
	uint64_t outputIt;
	bool moreInput;
	std::vector<std::pair<uint32_t, uint32_t> > dupList;
	boost::scoped_array<rowgroup::Row> dupRows;
	std::vector<std::string> smallTableNames;
	bool isExeMgr;
	uint32_t lastSmallOuterJoiner;

	//@bug5958 & 6117, stores the table key for identify token join
	uint32_t fTokenJoin;

	// moved from base class JobStep
	boost::mutex* fStatsMutexPtr;
};

}


#endif
// vim:ts=4 sw=4:
