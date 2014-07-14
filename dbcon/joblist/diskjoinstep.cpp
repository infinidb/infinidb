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


#include "diskjoinstep.h"
#include "exceptclasses.h"
#include "resourcemanager.h"

using namespace std;
using namespace rowgroup;
using namespace joiner;
using namespace logging;

namespace joblist {

DiskJoinStep::DiskJoinStep() { }

DiskJoinStep::DiskJoinStep(TupleHashJoinStep *t, int djsIndex, int joinIndex, bool lastOne) : JobStep(*t), thjs(t),
	joinerIndex(joinIndex), closedOutput(false)
{
	/*
		grab all relevant vars from THJS
		make largeRG and outputRG
		make the RG mappings
		init a JoinPartition
		load the existing RGData into JoinPartition
	*/

	largeRG = thjs->largeRG + thjs->outputRG;
	if (lastOne)
		outputRG = thjs->outputRG;
	else
		outputRG = largeRG;
	smallRG = thjs->smallRGs[joinerIndex];
	largeKeyCols = thjs->largeSideKeys[joinerIndex];
	smallKeyCols = thjs->smallSideKeys[joinerIndex];

	/* Should not be necessary if we can use THJS's logic to do the join */
	fe = thjs->getJoinFilter(joinerIndex);
	if (fe) {
		joinFERG = thjs->joinFilterRG;
		SjoinFEMapping = makeMapping(smallRG, joinFERG);
		LjoinFEMapping = makeMapping(largeRG, joinFERG);
	}
	joiner = thjs->djsJoiners[djsIndex];
	joinType = joiner->getJoinType();
	typeless = joiner->isTypelessJoin();
	joiner->clearData();
	joiner->setInUM();

	LOMapping = makeMapping(largeRG, outputRG);
	SOMapping = makeMapping(smallRG, outputRG);

	/* Link up */
	largeDL = thjs->fifos[djsIndex];
	outputDL = thjs->fifos[djsIndex+1];
	smallDL = thjs->smallDLs[joinerIndex];

	smallUsage = thjs->djsSmallUsage;
	smallLimit = thjs->djsSmallLimit;
	largeLimit = thjs->djsLargeLimit;
	partitionSize = thjs->djsPartitionSize;

	if (smallLimit == 0)
		smallLimit = numeric_limits<int64_t>::max();
	if (largeLimit == 0)
		largeLimit = numeric_limits<int64_t>::max();

	try {
		uint64_t totalUMMemory = thjs->resourceManager.getConfiguredUMMemLimit();
		jp.reset(new JoinPartition(largeRG, smallRG, smallKeyCols, largeKeyCols, typeless,
				(joinType & ANTI) && (joinType & MATCHNULLS), (bool) fe, totalUMMemory, partitionSize));
	}
	catch(IDBExcept &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(e.errorCode()));
			status(e.errorCode());
		}
		cout << "DJS(): " << e.what() << endl;
		abort();
		throw;
	}
	catch (exception &e) {
		if (status() == 0) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_UNKNOWN_ERROR));
			status(ERR_DBJ_UNKNOWN_ERROR);
		}
		abort();
		cout << "DJS(): " << e.what() << endl;
		throw;
	}
	largeIterationCount = 0;
	lastLargeIteration = false;
	fMiniInfo.clear();
	fExtendedInfo.clear();
}

DiskJoinStep::~DiskJoinStep()
{
	abort();
	if (mainThread) {
		mainThread->join();
		mainThread.reset();
	}
	if (jp)
		atomicops::atomicSub(smallUsage.get(), jp->getSmallSideDiskUsage());
}

void DiskJoinStep::loadExistingData(vector<RGData> &data)
{
	int64_t memUsage;
	uint32_t i;

	for (i = 0; i < data.size() && !cancelled(); i++) {
		memUsage = jp->insertSmallSideRGData(data[i]);
		atomicops::atomicAdd(smallUsage.get(), memUsage);
	}
}

void DiskJoinStep::run()
{
	mainThread.reset(new boost::thread(Runner(this)));
}

void DiskJoinStep::join()
{
	if (mainThread)
		mainThread->join();
	if (jp) {
		atomicops::atomicSub(smallUsage.get(), jp->getSmallSideDiskUsage());
		//int64_t memUsage;
		//memUsage = atomicops::atomicSub(smallUsage.get(), jp->getSmallSideDiskUsage());
		//cout << "join(): small side usage was: " << jp->getSmallSideDiskUsage() << " final shared mem usage = " << memUsage << endl;
		jp.reset();
	}
}

void DiskJoinStep::smallReader()
{
	RGData rgData;
	bool more = true;
	int64_t memUsage = 0, combinedMemUsage = 0;
	int rowCount = 0;
	RowGroup l_smallRG = smallRG;

	try {
		while (more && !cancelled()) {
			more = smallDL->next(0, &rgData);
			if (more) {
				l_smallRG.setData(&rgData);
				rowCount += l_smallRG.getRowCount();
				memUsage = jp->insertSmallSideRGData(rgData);
				combinedMemUsage = atomicops::atomicAdd(smallUsage.get(), memUsage);
				//cout << "memusage = " << memUsage << " total = " << combinedMemUsage << endl;
				if (combinedMemUsage > smallLimit) {
					errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_DISK_USAGE_LIMIT));
					status(ERR_DBJ_DISK_USAGE_LIMIT);
					cout << "DJS small reader: exceeded disk space limit" << endl;
					abort();
				}
			}
		}

		//cout << "(" << joinerIndex << ") read the small side data, combined mem usage= " << combinedMemUsage << " rowcount = " << rowCount << endl;
		if (!cancelled()) {
			memUsage = jp->doneInsertingSmallData();
			combinedMemUsage = atomicops::atomicAdd(smallUsage.get(), memUsage);
			//cout << "2memusage = " << memUsage << " total = " << combinedMemUsage << endl;
			if (combinedMemUsage > smallLimit) {
				errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_DISK_USAGE_LIMIT));
				status(ERR_DBJ_DISK_USAGE_LIMIT);
				cout << "DJS small reader: exceeded disk space limit" << endl;
				abort();
			}
		}
	}
	catch(IDBExcept &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(e.errorCode()));
			status(e.errorCode());
		}
		cout << "DJS small reader: " << e.what() << endl;
		abort();
	}
	catch(exception &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_UNKNOWN_ERROR));
			status(ERR_DBJ_UNKNOWN_ERROR);
		}
		cout << "DJS small reader: " << e.what() << endl;
		abort();
	}

	while (more)
		more = smallDL->next(0, &rgData);
}

void DiskJoinStep::largeReader()
{
	RGData rgData;
	bool more = true;
	int64_t largeSize = 0;
	int rowCount = 0;
	RowGroup l_largeRG = largeRG;

	largeIterationCount++;
	//cout << "iteration " << largeIterationCount << " largeLimit=" << largeLimit << endl;

	try {
		while (more && !cancelled() && largeSize < largeLimit) {
			more = largeDL->next(largeIt, &rgData);
			if (more) {
				l_largeRG.setData(&rgData);
				rowCount += l_largeRG.getRowCount();
				//cout << "large side raw data: " << largeRG.toString() << endl;

				largeSize += jp->insertLargeSideRGData(rgData);
			}
		}

		jp->doneInsertingLargeData();

		//cout << "(" << joinerIndex << ") read the large side data rowcount = " << rowCount << endl;
		if (!more)
			lastLargeIteration = true;
	}
	catch(IDBExcept &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(e.errorCode()));
			status(e.errorCode());
		}
		cout << "DJS large reader: " << e.what() << endl;
		abort();
	}
	catch(exception &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_UNKNOWN_ERROR));
			status(ERR_DBJ_UNKNOWN_ERROR);
		}
		cout << "DJS large reader: " << e.what() << endl;
		abort();
	}

	if (cancelled())
		while (more)
			more = largeDL->next(largeIt, &rgData);
}


void DiskJoinStep::loadFcn()
{
	boost::shared_ptr<LoaderOutput> out;
	bool ret;

	try {
		do {
			out.reset(new LoaderOutput());
			ret = jp->getNextPartition(&out->smallData, &out->partitionID, &out->jp);
			if (ret) {
				//cout << "loaded partition " << out->partitionID << " smallData = " << out->smallData.size() << endl;
				loadFIFO->insert(out);
			}
		} while (ret && !cancelled());
	}
	catch(IDBExcept &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(e.errorCode()));
			status(e.errorCode());
		}
		cout << "DJS loader: " << e.what() << endl;
		abort();
	}
	catch(exception &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_UNKNOWN_ERROR));
			status(ERR_DBJ_UNKNOWN_ERROR);
		}
		cout << "DJS loader: " << e.what() << endl;
		abort();
	}
	loadFIFO->endOfInput();
}

void DiskJoinStep::buildFcn()
{
	boost::shared_ptr<LoaderOutput> in;
	boost::shared_ptr<BuilderOutput> out;
	bool more = true;
	int it = loadFIFO->getIterator();
	int i, j;
	Row smallRow;
	RowGroup l_smallRG = smallRG;

	l_smallRG.initRow(&smallRow);

	while (1) {
		//cout << "getting a partition from the loader" << endl;
		more = loadFIFO->next(it, &in);
		if (!more || cancelled())
			goto out;

		out.reset(new BuilderOutput());
		out->smallData = in->smallData;
		out->partitionID = in->partitionID;
		out->jp = in->jp;
		out->tupleJoiner = joiner->copyForDiskJoin();

		//cout << "building a tuplejoiner" << endl;
		for (i = 0; i < (int) in->smallData.size(); i++) {
			l_smallRG.setData(&in->smallData[i]);
			l_smallRG.getRow(0, &smallRow);
			for (j = 0; j < (int) l_smallRG.getRowCount(); j++, smallRow.nextRow())
				out->tupleJoiner->insert(smallRow, (largeIterationCount == 1));
		}
		out->tupleJoiner->doneInserting();
		buildFIFO->insert(out);
	}

out:
	while (more)
		more = loadFIFO->next(it, &in);
	buildFIFO->endOfInput();
}

void DiskJoinStep::joinFcn()
{

	/* This function mostly serves as an adapter between the
	input data and the joinOneRG() fcn in THJS.  */

	boost::shared_ptr<BuilderOutput> in;
	bool more = true;
	int it = buildFIFO->getIterator();
	int i, j;
	vector<RGData> joinResults;
	RowGroup l_largeRG = largeRG, l_smallRG = smallRG;
	RowGroup l_outputRG = outputRG;
	Row l_largeRow;
	Row l_joinFERow, l_outputRow, baseRow;
	vector<vector<Row::Pointer> > joinMatches;
	boost::shared_array<Row> smallRowTemplates(new Row[1]);
	vector<boost::shared_ptr<TupleJoiner> > joiners;
	boost::shared_array<boost::shared_array<int> > colMappings, fergMappings;
	boost::scoped_array<boost::scoped_array<uint8_t > > smallNullMem;
	boost::scoped_array<uint8_t> joinFEMem;
	Row smallNullRow;

	boost::scoped_array<uint8_t> baseRowMem;

	if (joiner->hasFEFilter()) {
		joinFERG.initRow(&l_joinFERow, true);
		joinFEMem.reset(new uint8_t[l_joinFERow.getSize()]);
		l_joinFERow.setData(joinFEMem.get());
	}
	outputRG.initRow(&l_outputRow);
	outputRG.initRow(&baseRow, true);

	largeRG.initRow(&l_largeRow);

	baseRowMem.reset(new uint8_t[baseRow.getSize()]);
	baseRow.setData(baseRowMem.get());
	joinMatches.push_back(vector<Row::Pointer>());
	smallRG.initRow(&smallRowTemplates[0]);
	joiners.resize(1);

	colMappings.reset(new boost::shared_array<int>[2]);
	colMappings[0] = SOMapping;
	colMappings[1] = LOMapping;
	if (fe) {
		fergMappings.reset(new boost::shared_array<int>[2]);
		fergMappings[0] = SjoinFEMapping;
		fergMappings[1] = LjoinFEMapping;
	}

	l_smallRG.initRow(&smallNullRow, true);
	smallNullMem.reset(new boost::scoped_array<uint8_t>[1]);
	smallNullMem[0].reset(new uint8_t[smallNullRow.getSize()]);
	smallNullRow.setData(smallNullMem[0].get());
	smallNullRow.initToNull();

	while (1) {
		more = buildFIFO->next(it, &in);
		if (!more || cancelled())
			goto out;

		joiners[0] = in->tupleJoiner;
		boost::shared_ptr<RGData> largeData;
		largeData = in->jp->getNextLargeRGData();
		while (largeData) {
			l_largeRG.setData(largeData.get());
			thjs->joinOneRG(0, &joinResults, l_largeRG, l_outputRG, l_largeRow, l_joinFERow,
							l_outputRow, baseRow, joinMatches, smallRowTemplates,
							&joiners, &colMappings, &fergMappings, &smallNullMem);

			for (j = 0; j < (int) joinResults.size(); j++) {
				//l_outputRG.setData(&joinResults[j]);
				//cout << "got joined output " << l_outputRG.toString() << endl;
				outputDL->insert(joinResults[j]);
			}
			joinResults.clear();
			largeData = in->jp->getNextLargeRGData();
		}

		if (joinType & SMALLOUTER) {
			if (!lastLargeIteration) {

				/* TODO: an optimization would be to detect whether any new rows were marked and if not
				   suppress the save operation */
				vector<Row::Pointer> unmatched;
				in->tupleJoiner->getUnmarkedRows(&unmatched);
				//cout << "***** saving partition " << in->partitionID << " unmarked count=" << unmatched.size() << " total count="
				//	<< in->tupleJoiner->size() << " vector size=" << in->smallData.size() <<  endl;
				in->jp->saveSmallSidePartition(in->smallData);
			}
			else {

				//cout << "finishing small-outer output" << endl;
				vector<Row::Pointer> unmatched;
				RGData rgData(l_outputRG);
				Row outputRow;

				l_outputRG.setData(&rgData);
				l_outputRG.resetRowGroup(0);
				l_outputRG.initRow(&outputRow);
				l_outputRG.getRow(0, &outputRow);

				l_largeRG.initRow(&l_largeRow, true);
				boost::scoped_array<uint8_t> largeNullMem(new uint8_t[l_largeRow.getSize()]);
				l_largeRow.setData(largeNullMem.get());
				l_largeRow.initToNull();

				in->tupleJoiner->getUnmarkedRows(&unmatched);
				//cout << " small-outer count=" << unmatched.size() << endl;
				for (i = 0; i < (int) unmatched.size(); i++) {
					smallRowTemplates[0].setData(unmatched[i]);
					applyMapping(LOMapping, l_largeRow, &outputRow);
					applyMapping(SOMapping, smallRowTemplates[0], &outputRow);
					l_outputRG.incRowCount();
					if (l_outputRG.getRowCount() == 8192) {
						outputDL->insert(rgData);
						//cout << "inserting a full RG" << endl;
						rgData.reinit(l_outputRG);
						l_outputRG.setData(&rgData);
						l_outputRG.resetRowGroup(0);
						l_outputRG.getRow(0, &outputRow);
					}
					else
						outputRow.nextRow();
				}
				if (l_outputRG.getRowCount() > 0) {
					//cout << "inserting an rg with " << l_outputRG.getRowCount() << endl;
					outputDL->insert(rgData);
				}
			}
		}
	}

out:
	while (more)
		more = buildFIFO->next(it, &in);
	if (lastLargeIteration || cancelled()) {
		reportStats();
		outputDL->endOfInput();
		closedOutput = true;
	}
}

void DiskJoinStep::mainRunner()
{
	/*
		Read from smallDL, insert into small side
		Read from largeDL, insert into large side
		Start the processing threads
	*/
	try {
		smallReader();
		largeIt = largeDL->getIterator();
		while (!lastLargeIteration && !cancelled()) {
			//cout << "large iteration " << largeIterationCount << endl;
			jp->initForLargeSideFeed();
			largeReader();
			//cout << "done reading iteration " << largeIterationCount-1 << endl;

			jp->initForProcessing();
			if (cancelled())
				break;

			loadFIFO.reset(new FIFO<boost::shared_ptr<LoaderOutput> >(1, 1));   // double buffering should be good enough
			buildFIFO.reset(new FIFO<boost::shared_ptr<BuilderOutput> >(1, 1));

			loadThread.reset(new boost::thread(Loader(this)));
			buildThread.reset(new boost::thread(Builder(this)));
			joinThread.reset(new boost::thread(Joiner(this)));
			loadThread->join();
			buildThread->join();
			joinThread->join();
		}

		// make sure we drain all inputs
		if (cancelled()) {
			jp->initForLargeSideFeed();
			largeReader();
		}
		if (!closedOutput) {
			outputDL->endOfInput();
			closedOutput = true;
		}
	}
	catch (IDBExcept &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(e.errorCode()));
			status(e.errorCode());
		}
		cout << "DJS(): " << e.what() << endl;
		abort();
	}
	catch (exception &e) {
		if (!status()) {
			errorMessage(IDBErrorInfo::instance()->errorMsg(ERR_DBJ_UNKNOWN_ERROR));
			status(ERR_DBJ_UNKNOWN_ERROR);
		}
		cout << "DJS(): " << e.what() << endl;
		abort();
	}
}

const string DiskJoinStep::toString() const
{
	return "DiskJoinStep\n";
}

void DiskJoinStep::reportStats()
{
	ostringstream os1, os2;

	os1 << "DiskJoinStep: joined (large) " << alias() << " to (small) " << joiner->getTableName() <<". Processing stages: " << largeIterationCount <<
		", disk usage small/large: " << jp->getMaxSmallSize() << "/" << jp->getMaxLargeSize() <<
		", total bytes read/written: " << jp->getBytesRead() << "/" << jp->getBytesWritten() << endl;
	fExtendedInfo = os1.str();

	/* TODO: Can this report anything more useful in miniInfo? */
	int64_t bytesToReport = jp->getBytesRead() + jp->getBytesWritten();
	char units;
	if (bytesToReport > (1 << 30)) {
		bytesToReport >>= 30;
		units = 'G';
	}
	else if (bytesToReport > (1 << 20)) {
		bytesToReport >>= 20;
		units = 'M';
	}
	else if (bytesToReport > (1 << 10)) {
		bytesToReport >>= 10;
		units = 'K';
	}
	else units = ' ';

	os2 << "DJS UM " << alias() << "-" << joiner->getTableName() << " - - " << bytesToReport <<
		units << " - - -------- -\n";

	fMiniInfo = os2.str();

	if (traceOn())
		logEnd(os1.str().c_str());
}


}
