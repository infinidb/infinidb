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

#include "jobstep.h"
#include "tuplehashjoin.h"
#include "joinpartition.h"
#include "prioritythreadpool.h"

#ifndef DISKJOINSTEP_H
#define DISKJOINSTEP_H

namespace joblist {

class DiskJoinStep : public JobStep
{
	public:
		DiskJoinStep();
		DiskJoinStep(TupleHashJoinStep *, int djsIndex, int joinerIndex, bool lastOne);
		virtual ~DiskJoinStep();

		void run();
		void join();
		const std::string toString() const;

		void loadExistingData(std::vector<rowgroup::RGData> &data);
		uint32_t getIterationCount() { return largeIterationCount; }

	protected:
	private:
		boost::shared_ptr<joiner::JoinPartition> jp;
		rowgroup::RowGroup largeRG, smallRG, outputRG, joinFERG;
		std::vector<uint32_t> largeKeyCols, smallKeyCols;
		boost::shared_ptr<RowGroupDL> largeDL, outputDL;
		RowGroupDL *smallDL;

		boost::shared_array<int> LOMapping, SOMapping, SjoinFEMapping, LjoinFEMapping;
		TupleHashJoinStep *thjs;
		boost::shared_ptr<boost::thread> runner;
		boost::shared_ptr<funcexp::FuncExpWrapper> fe;
		bool typeless;
		JoinType joinType;
		boost::shared_ptr<joiner::TupleJoiner> joiner;     // the same instance THJS uses

		/* main thread, started by JobStep::run() */
		void mainRunner();
		struct Runner {
			Runner(DiskJoinStep *d) : djs(d) { }
			void operator()() { djs->mainRunner(); }
			DiskJoinStep *djs;
		};

		void smallReader();
		void largeReader();
		int largeIt;
		bool lastLargeIteration;
		uint32_t largeIterationCount;

		boost::shared_ptr<boost::thread> mainThread;

		/* Loader structs */
		struct LoaderOutput {
			std::vector<rowgroup::RGData> smallData;
			uint64_t partitionID;
			joiner::JoinPartition *jp;
		};
		boost::shared_ptr<joblist::FIFO<boost::shared_ptr<LoaderOutput> > > loadFIFO;

		struct Loader {
			Loader(DiskJoinStep *d) : djs(d) { }
			void operator()() { djs->loadFcn(); }
			DiskJoinStep *djs;
		};
		void loadFcn();

		boost::shared_ptr<boost::thread> loadThread;

		/* Builder structs */
		struct BuilderOutput {
			boost::shared_ptr<joiner::TupleJoiner> tupleJoiner;
			std::vector<rowgroup::RGData> smallData;
			uint64_t partitionID;
			joiner::JoinPartition *jp;
		};

		boost::shared_ptr<joblist::FIFO<boost::shared_ptr<BuilderOutput> > > buildFIFO;

		struct Builder {
			Builder(DiskJoinStep *d) : djs(d) { }
			void operator()() { djs->buildFcn(); }
			DiskJoinStep *djs;
		};
		void buildFcn();
		boost::shared_ptr<boost::thread> buildThread;

		/* Joining structs */
		struct Joiner {
			Joiner(DiskJoinStep *d) : djs(d) { }
			void operator()() { djs->joinFcn(); }
			DiskJoinStep *djs;
		};
		void joinFcn();
		boost::shared_ptr<boost::thread> joinThread;

		// limits & usage
		boost::shared_ptr<int64_t> smallUsage;
		int64_t smallLimit;
		int64_t largeLimit;
		uint64_t partitionSize;

		void reportStats();

		uint32_t joinerIndex;
		bool closedOutput;
};

}

#endif // DISKJOINSTEP_H
