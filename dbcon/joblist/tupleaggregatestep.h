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

//  $Id: tupleaggregatestep.h 8808 2012-08-13 16:59:37Z zzhu $


#ifndef JOBLIST_TUPLEAGGREGATESTEP_H
#define JOBLIST_TUPLEAGGREGATESTEP_H

#include "jobstep.h"
#include "rowaggregation.h"

#include <boost/thread.hpp>


namespace joblist
{

/** @brief class TupleAggregateStep
 *
 */
class TupleAggregateStep : public JobStep, public TupleDeliveryStep
{
public:
	/** @brief TupleAggregateStep constructor
	 * @param in the inputAssociation pointer
	 * @param out the outputAssociation pointer
	 */
	TupleAggregateStep(
			const JobStepAssociation& inputJobStepAssociation,
			const JobStepAssociation& outputJobStepAssociation,
			execplan::CalpontSystemCatalog* syscat,
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t statementId,
			const rowgroup::SP_ROWAGG_UM_t&,
			const rowgroup::RowGroup&,
			const rowgroup::RowGroup&,
			ResourceManager *);

	/** @brief TupleAggregateStep destructor
	 */
   ~TupleAggregateStep();

	/** @brief virtual void Run method
	 */
	void run();
	void join();

	/** @brief virtual JobStepAssociation * inputAssociation
	 *
	 * @returns JobStepAssociation *
	 */
	const JobStepAssociation& inputAssociation() const
	{
		return fInputJobStepAssociation;
	}
	void inputAssociation(const JobStepAssociation& inputAssociation)
	{
		fInputJobStepAssociation = inputAssociation;
	}
	/** @brief virtual JobStepAssociation * outputAssociation
	 *
	 * @returns JobStepAssocation *
	 */
	const JobStepAssociation& outputAssociation() const
	{
		return fOutputJobStepAssociation;
	}
	void outputAssociation(const JobStepAssociation& outputAssociation)
	{
		fOutputJobStepAssociation = outputAssociation;
	}

	void stepId(uint16_t stepId) { fStepId = stepId; }
	uint16_t stepId() const { return fStepId; }
	uint32_t sessionId()   const { return fSessionId; }
	uint32_t txnId()       const { return fTxnId; }
	uint32_t statementId() const { return fStatementId; }

	const std::string toString() const;

	void setOutputRowGroup(const rowgroup::RowGroup&);
	const rowgroup::RowGroup& getOutputRowGroup() const;
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	uint nextBand(messageqcpp::ByteStream &bs);
	uint nextBand_singleThread(messageqcpp::ByteStream &bs);
	bool setPmHJAggregation(JobStep* step);
	void savePmHJData(rowgroup::SP_ROWAGG_t&, rowgroup::SP_ROWAGG_t&, rowgroup::RowGroup&);
	void logger(const SPJL& logger) { fLogger = logger; }

	bool umOnly() const { return fUmOnly; }
	void umOnly(bool b) { fUmOnly = b; }

	void configDeliveredRowGroup(const JobInfo&);
	void setIsDelivery(bool b) { fDelivery = b; }
	void setEidMap(std::map<int, int>& m) { fIndexEidMap = m; }

	static SJSTEP prepAggregate(SJSTEP&, JobInfo&);

	// for multi-thread variables
	void initializeMultiThread();

private:
	static void prep1PhaseDistinctAggregate(
		JobInfo&, std::vector<rowgroup::RowGroup>&, std::vector<rowgroup::SP_ROWAGG_t>&);
	static void prep1PhaseAggregate(
		JobInfo&, std::vector<rowgroup::RowGroup>&, std::vector<rowgroup::SP_ROWAGG_t>&);
	static void prep2PhasesAggregate(
		JobInfo&, std::vector<rowgroup::RowGroup>&, std::vector<rowgroup::SP_ROWAGG_t>&);
	static void prep2PhasesDistinctAggregate(
		JobInfo&, std::vector<rowgroup::RowGroup>&, std::vector<rowgroup::SP_ROWAGG_t>&);

	void prepExpressionOnAggregate(rowgroup::SP_ROWAGG_UM_t&, JobInfo&);
	void addConstangAggregate(std::vector<rowgroup::ConstantAggData>&);

	void doAggregate();
	void doAggregate_singleThread();
	uint64_t doThreadedAggregate(messageqcpp::ByteStream &bs, RowGroupDL* dlp);
	void aggregateRowGroups();
	void threadedAggregateRowGroups(uint8_t threadID);
	void doThreadedSecondPhaseAggregate(uint8_t threadID);
	bool nextDeliveredRowGroup();
	void pruneAuxColumns();
	void formatMiniStats();
	void printCalTrace();

	JobStepAssociation fInputJobStepAssociation;
	JobStepAssociation fOutputJobStepAssociation;
	execplan::CalpontSystemCatalog *fCatalog;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint16_t fStepId;
	uint32_t fStatementId;
	uint64_t fRowsReturned;
	bool     fDoneAggregate;
	bool     fEndOfResult;
	SPJL     fLogger;

	rowgroup::SP_ROWAGG_UM_t fAggregator;
	rowgroup::RowGroup fRowGroupOut;
	rowgroup::RowGroup fRowGroupDelivered;
	boost::shared_array<uint8_t> fRowGroupData;

	// for setting aggregate column eid in delivered rowgroup
	std::map<int, int> fIndexEidMap;

	// data from RowGroupDL
	rowgroup::RowGroup fRowGroupIn;

	// for PM HashJoin
	// PM hashjoin is selected at runtime, prepare for it anyway.
	rowgroup::SP_ROWAGG_UM_t fAggregatorUM;
	rowgroup::SP_ROWAGG_PM_t fAggregatorPM;
	rowgroup::RowGroup fRowGroupPMHJ;

	// for Union
	class Aggregator
	{
	public:
		Aggregator(TupleAggregateStep* step) : fStep(step) { }
		void operator()() { fStep->doAggregate(); }

		TupleAggregateStep* fStep;
	};

	class ThreadedAggregator
	{
		public:
			ThreadedAggregator(TupleAggregateStep* step, uint8_t threadID) :
				fStep(step),
				fThreadID(threadID)
			{}
			void operator()() { fStep->threadedAggregateRowGroups(fThreadID); }

			TupleAggregateStep* fStep;
			uint8_t fThreadID;
	};

	class ThreadedSecondPhaseAggregator
	{
		public:
			ThreadedSecondPhaseAggregator(TupleAggregateStep* step, uint8_t threadID) :
				fStep(step),
				fThreadID(threadID)
			{
			}
			void operator()() {fStep->doThreadedSecondPhaseAggregate(fThreadID);}
			TupleAggregateStep* fStep;
			uint8_t fThreadID;
	};

	boost::scoped_ptr<boost::thread> fRunner;
	bool fDelivery;
	bool fUmOnly;
	ResourceManager *rm;

	// multi-threaded
	uint fNumOfThreads;
	uint fNumOfBuckets;
	uint fNumOfRowGroups;
	uint64_t fBucketMask;

	boost::mutex mutex;
	std::vector<boost::mutex*> agg_mutex;
	std::vector<boost::shared_array<uint8_t> > fRowGroupDatas;
	std::vector<rowgroup::SP_ROWAGG_UM_t> fAggregators;
	std::vector<rowgroup::RowGroup> fRowGroupIns;
	vector<rowgroup::RowGroup> fRowGroupOuts;
	std::vector<std::vector<boost::shared_array<uint8_t> > > fRowGroupsDeliveredData;
	bool fIsMultiThread;
	int fInputIter; // iterator
	boost::scoped_array<uint64_t> memUsage;
};


} // namespace

#endif  // JOBLIST_TUPLEAGGREGATESTEP_H

// vim:ts=4 sw=4:
