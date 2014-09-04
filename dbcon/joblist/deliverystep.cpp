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

/*
 * $Id: deliverystep.cpp 8476 2012-04-25 22:28:15Z xlou $
 */

#include <iostream>
#include <set>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
using namespace std;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "jobstep.h"
#include "tableband.h"
using namespace joblist;

#include "bytestream.h"
using namespace messageqcpp;

struct ObjNumColPos
{
	bool operator<(const ObjNumColPos& rhs) const
	{
		return colPosition < rhs.colPosition;
	}
	CalpontSystemCatalog::OID objnum;
	int colPosition;
};

namespace
{

}
//std::fstream derr;
namespace joblist
{

DeliveryStep::~DeliveryStep()
{	
	if (initialized) {
		uint i;

// 		cerr << "DeliveryStep::join()\n";

		mutex.lock(); //pthread_mutex_lock(&mutex);
		die = true;
		nextBandReady.notify_all(); //pthread_cond_broadcast(&nextBandReady);
		mutex.unlock(); //pthread_mutex_unlock(&mutex);

// 		cerr << "joining " << columnCount << " threads" << endl;
		for (i = 0; i < threads.size(); i++) {
			threads[i]->join();
// 			cerr << "joined " << i << endl;
			delete threads[i];
		}

		//pthread_mutex_destroy(&mutex);
		//pthread_cond_destroy(&nextBandReady);
		//pthread_cond_destroy(&allDone);
	}

}

/* Don't use this; it's only for the unit tester & benchmark */
DeliveryStep::DeliveryStep(const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	uint colWidth) :
		fInputJobStepAssociation(inputJobStepAssociation),
		fOutputJobStepAssociation(outputJobStepAssociation)
{
	uint i;
 	FifoDataList *dlp;
	StringFifoDataList *sdlp;

	initialized = true;
	die = false;
	bandCount = 0;
	doneCounter = 0;
	fRowsDelivered = 0;

	//pthread_mutex_init(&mutex, NULL);
	//pthread_cond_init(&nextBandReady, NULL);
	//pthread_cond_init(&allDone, NULL);

	columnCount = fInputJobStepAssociation.outSize();
	iterators.reset(new uint[columnCount]);
	colWidths.reset(new uint[columnCount]);
	fTableOID = 9000;
	fTableBand.tableOID(fTableOID);
	returnColumnCount = columnCount;
	columnData.reset(new ByteStream[returnColumnCount]);

	for (i = 0; i < columnCount; i++) {
		dlp = fInputJobStepAssociation.outAt(i)->fifoDL();
		if (dlp != NULL) 
			fTableBand.addNullColumn(dlp->OID());
		else {
			sdlp = fInputJobStepAssociation.outAt(i)->stringDL();
			fTableBand.addNullColumn(sdlp->OID());
		}
	}
	fEmptyTableBand = fTableBand;

	for (i = 0; i < columnCount; i++) {
		dlp = fInputJobStepAssociation.outAt(i)->fifoDL();
		if (dlp != NULL) {
			iterators[i] = dlp->getIterator();
			colWidths[i] = colWidth;
			threads.push_back(new boost::thread(Demuxer(this, i, 0, true)));
		}
		else {
			sdlp = fInputJobStepAssociation.outAt(i)->stringDL();
			iterators[i] = sdlp->getIterator();
			colWidths[i] = 0;
			threads.push_back(new boost::thread(Demuxer(this, i, 1, true)));
		}
	}
	fExtendedInfo = "DS: Not yet...";
}

// This step delivers rows for a table a band at a time
DeliveryStep::DeliveryStep(const JobStepAssociation& inputJobStepAssociation,
	const JobStepAssociation& outputJobStepAssociation,
	CalpontSystemCatalog::TableName tableName,
	CalpontSystemCatalog* cat,
	uint32_t sessionId,
	uint32_t txnId,
	uint32_t statementId,
	uint32_t flushInterval) :
		fInputJobStepAssociation(inputJobStepAssociation),
		fOutputJobStepAssociation(outputJobStepAssociation),
		fSessionId(sessionId),
		fTxnId(txnId),
		fStepId(0),
		fStatementId(statementId),
		fFlushInterval(flushInterval),
		fRowsDelivered(0)
{
	// We want to setup the table with all null columns in column position order
	fTableName = tableName;
	CalpontSystemCatalog::ROPair p = cat->tableRID(tableName);
	fTableOID = p.objnum;
	catalog = cat;

	CalpontSystemCatalog::RIDList ridList = cat->columnRIDs(tableName, true);

	// now put them into column position sorted order via a set
	CalpontSystemCatalog::RIDList::iterator iter = ridList.begin();
	CalpontSystemCatalog::RIDList::iterator end = ridList.end();
	typedef set<ObjNumColPos> ObjNumColPosSet;
	ObjNumColPosSet colPosMap;
	while (iter != end)
	{
		ObjNumColPos cp;
		cp.objnum = iter->objnum;
		// get the col pos for this col
		CalpontSystemCatalog::ColType ct = cat->colType(cp.objnum);
		if ( ct.colDataType == CalpontSystemCatalog::VARCHAR )
		{
			ct.colWidth++; 
		}
		//If this is a dictionary column, fudge the numbers...
		if (ct.colWidth > 8 || ct.colDataType == CalpontSystemCatalog::VARBINARY)
		{
			cp.objnum = ct.ddn.dictOID;
		}
		cp.colPosition = ct.colPosition;
		colPosMap.insert(cp);
		++iter;
	}

	fTableBand.tableOID(fTableOID);
	ObjNumColPosSet::iterator iter2 = colPosMap.begin();
	ObjNumColPosSet::iterator end2 = colPosMap.end();
	returnColumnCount = 0;
	while (iter2 != end2)
	{
		fTableBand.addNullColumn(iter2->objnum);
		++iter2;
		++returnColumnCount;
	}
	// keep a convenient "empty" band around
	fEmptyTableBand = fTableBand;
	columnData.reset(new ByteStream[returnColumnCount]);  // != columnCount if there are duplicates

	fState = 0;
	initialized = false;
	fExtendedInfo = "DS: Not yet...";
}

void DeliveryStep::run()
{

}

void DeliveryStep::join()
{

}

void DeliveryStep::initialize(bool bs)
{
	if (traceOn())
	{
		syslogStartStep(16,               // exemgr subsystem
			std::string("DeliveryStep")); // step name
	}

	uint i;
 	FifoDataList *dlp;
	StringFifoDataList *sdlp;
	initialized = true;
		
	if (fTableOID >= 3000 && traceOn())
	{
		ostringstream logStr;
		logStr << "first call of nextBand() for table OID " << fTableOID << endl;
		logEnd(logStr.str().c_str());
	}
	die = false;
	bandCount = 0;
	doneCounter = 0;

	//pthread_mutex_init(&mutex, NULL);
	//pthread_cond_init(&nextBandReady, NULL);
	//pthread_cond_init(&allDone, NULL);

	columnCount = fInputJobStepAssociation.outSize();
	iterators.reset(new uint[columnCount]);
	colWidths.reset(new uint[columnCount]);
	for (i = 0; i < columnCount; i++) {
		dlp = fInputJobStepAssociation.outAt(i)->fifoDL();
		if (dlp != NULL) {
			iterators[i] = dlp->getIterator();
			colWidths[i] = catalog->colType(dlp->OID()).colWidth;
			if ( catalog->colType(dlp->OID()).colDataType == CalpontSystemCatalog::VARCHAR )
				++colWidths[i];
			else if ( catalog->colType(dlp->OID()).colDataType == CalpontSystemCatalog::VARBINARY )
				colWidths[i] += 2;
// 			cout << "colWidths[" << i << "] = " << colWidths[i] << endl;
			threads.push_back(new boost::thread(Demuxer(this, i, 0, bs)));
		}
		else {
			sdlp = fInputJobStepAssociation.outAt(i)->stringDL();
			iterators[i] = sdlp->getIterator();
			colWidths[i] = 0;
			threads.push_back(new boost::thread(Demuxer(this, i, 1, bs)));
		}
	}
}

const string DeliveryStep::toString() const
{
	ostringstream oss;
	size_t outSize = fInputJobStepAssociation.outSize();
	oss << "DeliverStep tb:" << fTableOID;
	oss << " in:";
	for (unsigned i = 0; i < outSize; i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
	return oss.str();
}

void DeliveryStep::fillByteStream(ByteStream &bs)
{
	ByteStream::octbyte ob;
	uint i;

	ob = fTableOID;
	bs << ob;
	ob = returnColumnCount;
	bs << ob;
	ob = rowCount;
	bs << ob;
	bs << (uint64_t)0; //tableband fStatus

	bs << (uint8_t) false;   // a new "got rids" var which DS doesn't use
	
	for (i = 0; i < returnColumnCount; ++i) {
		if (columnData[i].length() > 0) {
			bs += columnData[i];
			columnData[i].restart();
		}
		else {
			// Make the null columns serialize themselves.
			fEmptyTableBand.getColumns()[i]->serialize(bs);
		}
	}
}

uint DeliveryStep::nextBand(ByteStream &bs)
{
	if (!initialized)
		initialize(true);

	if (traceOn() && (dlTimes.FirstReadTime().tv_sec == 0))
		dlTimes.setFirstReadTime();

	mutex.lock(); //pthread_mutex_lock(&mutex);
	bandCount++;
	doneCounter = 0;
	nextBandReady.notify_all(); //pthread_cond_broadcast(&nextBandReady);
	while (doneCounter < columnCount)
		allDone.wait(mutex); //pthread_cond_wait(&allDone, &mutex);
	mutex.unlock(); //pthread_mutex_unlock(&mutex);

	fillByteStream(bs);
	if (rowCount == 0)
		columnData.reset();

	if (fTableOID >= 3000)
	{
		if(fRowsDelivered == 0 && traceOn()) 
		{
			ostringstream logStr;
			logStr  << "DeliveryStep::nextBand: delivering initial band for " << fTableName
					<< " (" << fTableOID << ") w/ " << rowCount << " rows" << endl;
			logEnd(logStr.str().c_str());
		}

		fRowsDelivered += rowCount;
		if(rowCount == 0)
		{
			//...Construct timestamp using ctime_r() instead of ctime() not
			//...necessarily due to re-entrancy, but because we want to strip
			//...the newline ('\n') off the end of the formatted string.
			time_t t = time(0);
			char timeString[50];
			ctime_r(&t, timeString);
			timeString[strlen(timeString)-1 ] = '\0';

			FifoDataList* pFifo    = 0;
			uint totalBlockedReadCount  = 0;
			uint totalBlockedWriteCount = 0;

			//...Sum up the blocked FIFO reads for all input associations
			size_t inDlCnt  = fInputJobStepAssociation.outSize();
			for (size_t iDataList=0; iDataList<inDlCnt; iDataList++)
			{
				pFifo = fInputJobStepAssociation.outAt(iDataList)->fifoDL();
				if (pFifo)
					totalBlockedReadCount += pFifo->blockedReadCount();
			}

			//...Sum up the blocked FIFO writes for all output associations
			size_t outDlCnt = fOutputJobStepAssociation.outSize();
			for (size_t iDataList=0; iDataList<outDlCnt; iDataList++)
			{
				pFifo = fOutputJobStepAssociation.outAt(iDataList)->fifoDL();
				if (pFifo)
					totalBlockedWriteCount += pFifo->blockedWriteCount();
			}

			if (traceOn())
			{
				//...Print job step completion information
				ostringstream logStr;
				logStr << "DeliveryStep::nextBand: delivered last band for " << fTableName << " ("
					<< fTableOID << ") - " << fRowsDelivered << " total rows delivered" << endl <<
					"DeliveryStep for " << fTableName <<
					" finished at "     << timeString <<
					"; BlockedFifoIn/Out-"  << totalBlockedReadCount <<
					"/" << totalBlockedWriteCount << endl;
				logEnd(logStr.str().c_str());

				dlTimes.setLastReadTime();

				syslogProcessingTimes(16,    // exemgr subsystem
					dlTimes.FirstReadTime(), // first datalist read
					dlTimes.LastReadTime(),  // last  datalist read
					dlTimes.FirstReadTime(), // first write n/a, use first read
					dlTimes.LastReadTime()); // last  write n/a, use last  read
				syslogEndStep(16, // exemgr subsystem
					0,            // no blocked datalist input  to report
					0);           // no blocked datalist output to report
			}
		}
 	}
	return rowCount;
}
	
const TableBand DeliveryStep::nextBand()
{
	
	if (!initialized)
		initialize(false);

	fTableBand = fEmptyTableBand;
	
	if (traceOn() && (dlTimes.FirstReadTime().tv_sec == 0))
		dlTimes.setFirstReadTime();

	mutex.lock(); //pthread_mutex_lock(&mutex);
	bandCount++;
	doneCounter = 0;
	nextBandReady.notify_all(); //pthread_cond_broadcast(&nextBandReady);
	while (doneCounter < columnCount)
		allDone.wait(mutex); //pthread_cond_wait(&allDone, &mutex);
	mutex.unlock(); //pthread_mutex_unlock(&mutex);

	if (fTableOID >= 3000)
	{
		if(fRowsDelivered == 0 && traceOn()) 
		{
			ostringstream logStr;
			logStr  << "DeliveryStep::nextBand: delivering initial band for " << fTableName
					<< " (" << fTableOID << ") w/ " << fTableBand.getRowCount() << " rows" << endl;
			logEnd(logStr.str().c_str());
		}

		fRowsDelivered += fTableBand.getRowCount();
		if(fTableBand.getRowCount() == 0)
		{
			//...Construct timestamp using ctime_r() instead of ctime() not
			//...necessarily due to re-entrancy, but because we want to strip
			//...the newline ('\n') off the end of the formatted string.
			time_t t = time(0);
			char timeString[50];
			ctime_r(&t, timeString);
			timeString[strlen(timeString)-1 ] = '\0';

			FifoDataList* pFifo    = 0;
			uint totalBlockedReadCount  = 0;
			uint totalBlockedWriteCount = 0;

			//...Sum up the blocked FIFO reads for all input associations
			size_t inDlCnt  = fInputJobStepAssociation.outSize();
			for (size_t iDataList=0; iDataList<inDlCnt; iDataList++)
			{
				pFifo = fInputJobStepAssociation.outAt(iDataList)->fifoDL();
				if (pFifo)
					totalBlockedReadCount += pFifo->blockedReadCount();
			}

			//...Sum up the blocked FIFO writes for all output associations
			size_t outDlCnt = fOutputJobStepAssociation.outSize();
			for (size_t iDataList=0; iDataList<outDlCnt; iDataList++)
			{
				pFifo = fOutputJobStepAssociation.outAt(iDataList)->fifoDL();
				if (pFifo)
					totalBlockedWriteCount += pFifo->blockedWriteCount();
			}

			if (traceOn())
			{
				//...Print job step completion information
				ostringstream logStr;
				logStr << "DeliveryStep::nextBand: delivered last band for " << fTableName <<
					" (" << fTableOID << ") - " << fRowsDelivered <<
					" total rows delivered" << endl <<
					"DeliveryStep for " << fTableName <<
					" finished at "     << timeString <<
					"; BlockedFifoIn/Out-"  << totalBlockedReadCount <<
					"/" << totalBlockedWriteCount << endl;
				logEnd(logStr.str().c_str());

				dlTimes.setLastReadTime();

				syslogProcessingTimes(16,    // exemgr subsystem
					dlTimes.FirstReadTime(), // first datalist read
					dlTimes.LastReadTime(),  // last  datalist read
					dlTimes.FirstReadTime(), // first write n/a, use first read
					dlTimes.LastReadTime()); // last  write n/a, use last  read
				syslogEndStep(16, // exemgr subsystem
					0,            // no blocked datalist input  to report
					0);           // no blocked datalist output to report
			}
		}
 	}	
	
	return fTableBand;
}

void DeliveryStep::fillBSFromNumericalColumn(uint index)
{
	FifoDataList *fifo;
// 	uint it, i;
	uint it;
	int bsIndex;
	uint64_t bandNumber;
	UintRowGroup rg;
	messageqcpp::ByteStream::octbyte bsRowCount;
	messageqcpp::ByteStream::octbyte oid;
	messageqcpp::ByteStream::byte nullFlag;
	messageqcpp::ByteStream::byte columnType;

	bool firstColumn = (index == 0);

	fifo = fInputJobStepAssociation.outAt(index)->fifoDL();
	it = iterators[index];
	bsIndex = fEmptyTableBand.find(fifo->OID());
	if (bsIndex < 0)  {
		cout << "fill ByteStream throwing\n";
		throw logic_error("DeliveryStep: fEmptyTableBand is unaware of the given OID");
	}
	bandNumber = 0;

	mutex.lock(); //pthread_mutex_lock(&mutex);

	while (1) {
		while (bandNumber == bandCount && !die) 
			nextBandReady.wait(mutex); //pthread_cond_wait(&nextBandReady, &mutex);
		bandNumber = bandCount;
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
		if (die) 
			return;

		// fill in column BS
		oid = fifo->OID();
		columnData[bsIndex] << oid;
		//Bug 836
		switch (colWidths[index]) {
			case 5:
			case 6:
			case 7: {colWidths[index] = 8;}
			case 8: columnType = TableColumn::UINT64; break;
			case 3: { colWidths[index] = 4;	}
			case 4: columnType = TableColumn::UINT32; break;
			case 2: columnType = TableColumn::UINT16; break;
			case 1: columnType = TableColumn::UINT8; break;
			default: 
				cout << "DS: throwing!\n" << endl;
				throw logic_error("DeliveryStep: Bad column width!");
		}
		columnData[bsIndex] << columnType;

		nullFlag = 0;
		columnData[bsIndex] << nullFlag;
		rg.count = 0;
		fifo->next(it, &rg);
		bsRowCount = rg.count;
		columnData[bsIndex] << bsRowCount;
//  		cout << "DS: got an RG, oid = " << oid << " appending "<< 
// 			rg.count * colWidths[index] << " count = " << rg.count << endl;

		/* When this is called, pColStep implicitly knows it's called and
		puts packed data in the ElementType array.  Here we just need to
		copy it into the ByteStream for this column.  
		TableColumn::unserialize() will parse both formats */
		columnData[bsIndex].append((uint8_t *) &rg.et[0], rg.count * colWidths[index]);
		mutex.lock(); //pthread_mutex_lock(&mutex);
		if (firstColumn) 
			rowCount = rg.count;
		if (++doneCounter == columnCount)
			allDone.notify_one(); //pthread_cond_signal(&allDone);
	}
}

void DeliveryStep::fillBSFromStringColumn(uint index)
{
	StringFifoDataList *fifo;
	uint it, i;
	int bsIndex;
	uint64_t bandNumber;
	StringRowGroup rg;
	messageqcpp::ByteStream::octbyte bsRowCount;
	messageqcpp::ByteStream::octbyte oid;
	messageqcpp::ByteStream::byte nullFlag;
	messageqcpp::ByteStream::byte columnType;

	bool firstColumn = (index == 0);

	fifo = fInputJobStepAssociation.outAt(index)->stringDL();
	it = iterators[index];
	bsIndex = fEmptyTableBand.find(fifo->OID());
	if (bsIndex < 0)
		throw logic_error("DeliveryStep: fEmptyTableBand is unaware of the given OID");
	bandNumber = 0;

	mutex.lock(); //pthread_mutex_lock(&mutex);

	while (1) {
		while (bandNumber == bandCount && !die)
			nextBandReady.wait(mutex); //pthread_cond_wait(&nextBandReady, &mutex);
		bandNumber = bandCount;
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
		if (die)
			return;

		// fill in column BS
		oid = fifo->OID();
		columnData[bsIndex] << oid;
		columnType = TableColumn::STRING;
		columnData[bsIndex] << columnType;
		nullFlag = 0;
		columnData[bsIndex] << nullFlag;

		rg.count = 0;
		fifo->next(it, &rg);
		bsRowCount = rg.count;
// 		cout << "DS (S): got a group count = " << rg.count << endl;
		columnData[bsIndex] << bsRowCount;
		for (i = 0; i < rg.count; ++i) {
			columnData[bsIndex] << rg.et[i].second;
//  			cout << "adding " << rg.et[i].second << " at " << i << endl;
		}

		mutex.lock(); //pthread_mutex_lock(&mutex);
		if (firstColumn) 
			rowCount = rg.count;
		if (++doneCounter == columnCount) 
			allDone.notify_one(); //pthread_cond_signal(&allDone);
	}
}


void DeliveryStep::readNumericalColumn(uint index)
{
	FifoDataList *fifo;
	uint it;
	uint64_t bandNumber;

	bool firstColumn = (index == 0);

	fifo = fInputJobStepAssociation.outAt(index)->fifoDL();
	it = iterators[index];
	bandNumber = 0;

	mutex.lock(); //pthread_mutex_lock(&mutex);
	while (1) {
		while (bandNumber == bandCount && !die)
			nextBandReady.wait(mutex); //pthread_cond_wait(&nextBandReady, &mutex);
		bandNumber = bandCount;
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
		if (die)
			return;

		fTableBand.addColumn(fifo, it, fFlushInterval, firstColumn, false);

		mutex.lock(); //pthread_mutex_lock(&mutex);
		if (++doneCounter == columnCount)
			allDone.notify_one(); //pthread_cond_signal(&allDone);
	}
	
}

void DeliveryStep::readStringColumn(uint index)
{
	StringElementType e;
	StringFifoDataList *fifo;
	uint it;
	uint64_t bandNumber;


	fifo = fInputJobStepAssociation.outAt(index)->stringDL();
	it = iterators[index];
	bandNumber = 0;

	bool firstColumn = (index == 0);
	
	mutex.lock(); //pthread_mutex_lock(&mutex);
	while (1) {
		while (bandNumber == bandCount && !die)
			nextBandReady.wait(mutex); //pthread_cond_wait(&nextBandReady, &mutex);
		bandNumber = bandCount;
		mutex.unlock(); //pthread_mutex_unlock(&mutex);
		if (die) 
			return;

		fTableBand.addColumn(fifo, it, fFlushInterval, firstColumn, false);

		mutex.lock(); //pthread_mutex_lock(&mutex);
		if (++doneCounter == columnCount)
			allDone.notify_one(); //pthread_cond_signal(&allDone);
	}
	
}

}
