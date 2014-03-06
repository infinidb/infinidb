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

//  $Id: tupleaggregatestep.h 9732 2013-08-02 15:56:15Z pleblanc $


#ifndef JOBLIST_TUPLEAGGREGATESTEP_H
#define JOBLIST_TUPLEAGGREGATESTEP_H

#include "jobstep.h"
#include "rowaggregation.h"

#include <boost/thread.hpp>


namespace joblist
{

// forward reference
struct JobInfo;

/** @brief class TupleAggregateStep
 *
 */
class TupleAggregateStep : public JobStep, public TupleDeliveryStep
{
public:
	/** @brief TupleAggregateStep constructor
	 */
	TupleAggregateStep(
			const rowgroup::SP_ROWAGG_UM_t&,
			const rowgroup::RowGroup&,
			const rowgroup::RowGroup&,
			const JobInfo&);

	/** @brief TupleAggregateStep destructor
	 */
   ~TupleAggregateStep();

	/** @brief virtual void Run method
	 */
	void run();
	void join();

	const std::string toString() const;

	void setOutputRowGroup(const rowgroup::RowGroup&);
	const rowgroup::RowGroup& getOutputRowGroup() const;
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	void  deliverStringTableRowGroup(bool b);
	bool  deliverStringTableRowGroup() const;
	uint32_t nextBand(messageqcpp::ByteStream &bs);
	uint32_t nextBand_singleThread(messageqcpp::ByteStream &bs);
	bool setPmHJAggregation(JobStep* step);
	void savePmHJData(rowgroup::SP_ROWAGG_t&, rowgroup::SP_ROWAGG_t&, rowgroup::RowGroup&);

	bool umOnly() const { return fUmOnly; }
	void umOnly(bool b) { fUmOnly = b; }

	void configDeliveredRowGroup(const JobInfo&);
	//void setEidMap(std::map<int, int>& m) { fIndexEidMap = m; }

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
	void threadedAggregateRowGroups(uint32_t threadID);
	void doThreadedSecondPhaseAggregate(uint32_t threadID);
	bool nextDeliveredRowGroup();
	void pruneAuxColumns();
	void formatMiniStats();
	void printCalTrace();

	boost::shared_ptr<execplan::CalpontSystemCatalog>fCatalog;
	uint64_t fRowsReturned;
	bool     fDoneAggregate;
	bool     fEndOfResult;

	rowgroup::SP_ROWAGG_UM_t fAggregator;
	rowgroup::RowGroup fRowGroupOut;
	rowgroup::RowGroup fRowGroupDelivered;
	rowgroup::RGData fRowGroupData;

	// for setting aggregate column eid in delivered rowgroup
	//std::map<int, int> fIndexEidMap;

	// data from RowGroupDL
	rowgroup::RowGroup fRowGroupIn;

	// for PM HashJoin
	// PM hashjoin is selected at runtime, prepare for it anyway.
	rowgroup::SP_ROWAGG_UM_t fAggregatorUM;
	rowgroup::SP_ROWAGG_PM_t fAggregatorPM;
	rowgroup::RowGroup fRowGroupPMHJ;

	// for run thread (first added for union)
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
			ThreadedAggregator(TupleAggregateStep* step, uint32_t threadID) :
				fStep(step),
				fThreadID(threadID)
			{}
			void operator()() { fStep->threadedAggregateRowGroups(fThreadID); }

			TupleAggregateStep* fStep;
			uint32_t fThreadID;
	};

	class ThreadedSecondPhaseAggregator
	{
		public:
			ThreadedSecondPhaseAggregator(TupleAggregateStep* step, uint32_t threadID, uint32_t bucketsPerThread) :
				fStep(step),
				fThreadID(threadID),
				bucketCount(bucketsPerThread)
			{
			}
			void operator()() {
				for (uint32_t i = 0; i < bucketCount; i++)
					fStep->doThreadedSecondPhaseAggregate(fThreadID+i);
			}
			TupleAggregateStep* fStep;
			uint32_t fThreadID;
			uint32_t bucketCount;
	};

	boost::scoped_ptr<boost::thread> fRunner;
	bool fUmOnly;
	ResourceManager& fRm;

	// multi-threaded
	uint32_t fNumOfThreads;
	uint32_t fNumOfBuckets;
	uint32_t fNumOfRowGroups;
	uint32_t fBucketNum;

	boost::mutex fMutex;
	std::vector<boost::mutex*> fAgg_mutex;
	std::vector<rowgroup::RGData > fRowGroupDatas;
	std::vector<rowgroup::SP_ROWAGG_UM_t> fAggregators;
	std::vector<rowgroup::RowGroup> fRowGroupIns;
	vector<rowgroup::RowGroup> fRowGroupOuts;
	std::vector<std::vector<rowgroup::RGData> > fRowGroupsDeliveredData;
	bool fIsMultiThread;
	int fInputIter; // iterator
	boost::scoped_array<uint64_t> fMemUsage;
	vector<boost::shared_ptr<boost::thread> > fFirstPhaseRunners;
	uint32_t fFirstPhaseThreadCount;
};


} // namespace

#endif  // JOBLIST_TUPLEAGGREGATESTEP_H

// vim:ts=4 sw=4:
