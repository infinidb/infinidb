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

/*****************************************************************************
 * $Id: tdriver-deliver.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 ****************************************************************************/

#define BENCHMARK

#include "joblist.h"
#include "jobstep.h"
#include "distributedenginecomm.h"
#include "calpontsystemcatalog.h"
#include "tableband.h"
#include <sys/timeb.h>
#include <iostream>
#include <pthread.h>
#include <boost/scoped_ptr.hpp>

#include "stopwatch.cpp"

#include "bytestream.h"
using namespace messageqcpp;

#include "brm.h"
using namespace BRM;

using namespace std;
using namespace joblist;
using namespace execplan;


string schema("tpch100");//dmc
string table("lineitem"); //create table d_step(c1 number);
string strtable("d_strstep"); //create table d_strstep(c1 char(10));
string colstable("lineitem"); //create table d_colsstep(c1 number, c2 number, c3number, c4 number, c5 number, c6 number, c7 number, c8 number, c9 number,);

int dataSize = 100 * 1000 * 1000; // 8 MB
int bandSize = 8192 * 8;  // 8 K
int fifoSize = 128;
int startingOid = 3416;  // Oid for lineitem.l_orderkey on your database.
// uint32_t flushInterval = 16384; // interval used in flushing table bands
uint32_t flushInterval = 16384; // interval used in flushing table bands
uint32_t columns;

Stopwatch timer;

//******************************************************************************
//******************************************************************************
// Start of Class to manage table band projection in a separate thread
// (modeled after a similar class in the exemgr directory)

#include <queue>

namespace
{

class BSQueueMgr
{
    public:
        /** @brief BSQueueMgr constructor.
         *
         * @param joblist      (in) JobList where table bands reside
         * @param tableOID     (in) OID of the table to be projected
         * @param maxQueueSize (in) Max # of table bands to be queued up
         */
        explicit BSQueueMgr(
            DeliveryStep *d,
            int   maxQueueSize=1);

        /** @brief TableBandQueueMgr destructor.
         *
         */
        ~BSQueueMgr( );

        /** @brief Main processing loop that projects the table bands.
         *
         */
        void project();

        /** @brief Called by second thread to acquire projected table band
         *
         * Acquires the next table band that has been projected, so that
         * the calling thread can serialize and forward the table band to
         * the next step.
         *
         * @return Next projected table band. Row count of 0 marks end of data
         */
        messageqcpp::ByteStream* getNextByteStream(uint32_t &rowCount);

    private:
        //Disable these by declaring but not defining
        BSQueueMgr (const BSQueueMgr& rhs);
        BSQueueMgr& operator=(const BSQueueMgr& rhs);

		struct QueueElement {
			messageqcpp::ByteStream *bs;
			uint32_t rowCount;
		};

        DeliveryStep *ds;
        unsigned int        fMaxQueueSize;

        //...Internal queue used to save table bands as they are projected.
        std::queue<QueueElement> fBSQueue;

        //...Mutex to protect our internal table band queue, and the
        //...condition variables used to signal when a table band has
        //...been added to, or removed from the queue.     
        pthread_mutex_t     fMutex;
        pthread_cond_t      fBSAdded;
        pthread_cond_t      fBSRemoved;
};

BSQueueMgr::BSQueueMgr (
	DeliveryStep *d,
	int                                 maxQueueSize ) :
		ds     (d),
		fMaxQueueSize(maxQueueSize)
{
	pthread_mutex_init ( &fMutex, 0 ); 
	pthread_cond_init  ( &fBSAdded,   0 );
	pthread_cond_init  ( &fBSRemoved, 0 );
}

//------------------------------------------------------------------------------
// BSQueueMgr destructor
//------------------------------------------------------------------------------
BSQueueMgr::~BSQueueMgr ( )
{
	pthread_mutex_destroy( &fMutex);
	pthread_cond_destroy ( &fBSAdded );
	pthread_cond_destroy ( &fBSRemoved );
}

//------------------------------------------------------------------------------
// Contains loop to project the table bands.  They are stored into our internal
// queue, until we reach the queue size limit, in which case, we wait for the
// consumer to work off some of the table bands.  This will in turn free us up
// to continue adding projected table bands, until we reach a table band with
// a row count of 0, which denotes the end of the table.
//------------------------------------------------------------------------------
void BSQueueMgr::project ( )
{
	bool moreData = true;

	//...Stay in loop to project table bands, until we reach a table band
	//...having a row count of 0.

	while ( moreData )
	{
		uint32_t rowCount;
		QueueElement qe;
		
		qe.bs = new ByteStream;

		rowCount = ds->nextBand(*(qe.bs));
		qe.rowCount = rowCount;
	
		pthread_mutex_lock( &fMutex );

		//...Wait for room in our queue before adding this table band
		while (fBSQueue.size() >= fMaxQueueSize )
		{
			pthread_cond_wait( &fBSRemoved, &fMutex );
		}

		fBSQueue.push( qe );

		if ( rowCount == 0 )
			moreData = false;

		pthread_cond_broadcast( &fBSAdded );
			
		pthread_mutex_unlock( &fMutex );
	}
}

//------------------------------------------------------------------------------
// Extract a projected table band from our internal queue of table bands.
// Returns next projected table band.  A row count of 0, marks the table
// band as being the last.
//------------------------------------------------------------------------------
ByteStream * BSQueueMgr::getNextByteStream(uint32_t &rowCount)
{
	QueueElement qe;

	pthread_mutex_lock( &fMutex );
	//...Wait for a table band to be added to our queue if the queue is empty
	while ( fBSQueue.size() < 1 )
	{
		pthread_cond_wait( &fBSAdded, &fMutex );
	}

	qe = fBSQueue.front( );
	fBSQueue.pop( );
	pthread_cond_broadcast(&fBSRemoved);

	pthread_mutex_unlock( &fMutex );
	
	rowCount = qe.rowCount;
	return qe.bs;
}

struct BSProjectThread
{
    BSProjectThread ( BSQueueMgr* pMgr ) :
        fBSQueueMgr ( pMgr ) { }

    BSQueueMgr* fBSQueueMgr;

    void operator()()
    {
        fBSQueueMgr->project();
    }
};


//
// A class that manages projection of table bands for a specific table
//
// This class manages table band projections for a specified table OID.
// The projections can be performed in a separate thread, allowing the
// serialization of the resulting table band(s) to occur concurrently.
// The projected table band(s) are saved into an internal queue, with the
// size of the queue being controlled through a constructor argument.
//
class TableBandQueueMgr
{
	public:
		//
		// TableBandQueueMgr constructor.
		//
		// pDStep       (in) Delivery step where table bands reside
		// maxQueueSize (in) Max # of table bands to be queued up
		//
		explicit TableBandQueueMgr(
			DeliveryStep* pDStep,
			int   maxQueueSize=1) :
				fDStep       (pDStep),
				fMaxQueueSize(maxQueueSize)
		{
			pthread_mutex_init ( &fMutex, 0 ); 
			pthread_cond_init  ( &fTableBandAdded,   0 );
			pthread_cond_init  ( &fTableBandRemoved, 0 );
		}

		// TableBandQueueMgr destructor.
		~TableBandQueueMgr( )
		{
			pthread_mutex_destroy( &fMutex);
			pthread_cond_destroy ( &fTableBandAdded );
			pthread_cond_destroy ( &fTableBandRemoved );
		}

		// Main processing loop that projects the table bands.
		void project();

		// Called by second thread to acquire projected table band
		//
		// Acquires the next table band that has been projected, so that
		// the calling thread can serialize and forward the table band to
		// the next step.
		//
		// Returns next projected table band. Row count of 0 marks end of data
		joblist::TableBand* getNextTableBand();

	private:
		// Disable these by declaring but not defining
		TableBandQueueMgr (const TableBandQueueMgr& rhs);
		TableBandQueueMgr& operator=(const TableBandQueueMgr& rhs);

		DeliveryStep*       fDStep;
		unsigned int        fMaxQueueSize;

		// Internal queue used to save table bands as they are projected.
		std::queue<joblist::TableBand*> fTblBandQueue;

		// Mutex to protect our internal table band queue, and the
		// condition variables used to signal when a table band has
		// been added to, or removed from the queue.     
		pthread_mutex_t     fMutex;
		pthread_cond_t      fTableBandAdded;
		pthread_cond_t      fTableBandRemoved;
};

//
// Contains loop to project the table bands.  They are stored into our internal
// queue, until we reach the queue size limit, in which case, we wait for the
// consumer to work off some of the table bands.  This will in turn free us up
// to continue adding projected table bands, until we reach a table band with
// a row count of 0, which denotes the end of the table.
//
void TableBandQueueMgr::project ( )
{
	bool moreData = true;

	//...Stay in loop to project table bands, until we reach a table band
	//...having a row count of 0.

	while ( moreData )
	{
		joblist::TableBand* pTblBand = new joblist::TableBand;
		*pTblBand = fDStep->nextBand( );

		pthread_mutex_lock( &fMutex );

		//...Wait for room in our queue before adding this table band
		while ( fTblBandQueue.size( ) >= fMaxQueueSize )
		{
			pthread_cond_wait( &fTableBandRemoved, &fMutex );
		}

		fTblBandQueue.push( pTblBand );

		if ( pTblBand->getRowCount( ) == 0 )
			moreData = false;

		pthread_cond_broadcast( &fTableBandAdded );
			
		pthread_mutex_unlock( &fMutex );
	}
}

//
// Extract a projected table band from our internal queue of table bands.
// Returns next projected table band.  A row count of 0, marks the table
// band as being the last.
//
joblist::TableBand* TableBandQueueMgr::getNextTableBand()
{
	joblist::TableBand* pTblBand = 0;

	pthread_mutex_lock( &fMutex );

	//...Wait for a table band to be added to our queue if the queue is empty
	while ( fTblBandQueue.size() < 1 )
	{
		pthread_cond_wait( &fTableBandAdded, &fMutex );
	}

	pTblBand = fTblBandQueue.front( );
	fTblBandQueue.pop( );

	//...If the row count is 0, there is no need to notify our producing
	//...thread, since that means we have reached the end of data.
	if ( pTblBand->getRowCount( ) != 0 )
		pthread_cond_broadcast( &fTableBandRemoved );

	pthread_mutex_unlock( &fMutex );

	return pTblBand;
}

} // end of namespace

// End of Class to manage table band projection in a separate thread
//******************************************************************************
//******************************************************************************

//------------------------------------------------------------------------------
// Drives thread to project table bands for delivery.
//------------------------------------------------------------------------------
void* projectThreadWrapper( void* pThreadData )
{
		TableBandQueueMgr* tableBandMgr = (TableBandQueueMgr*)pThreadData;

		//cout << "Starting thread to project columns..." << endl;
		tableBandMgr->project();
		//cout << "Finished thread to project columns..." << endl;

		return 0;
}

//------------------------------------------------------------------------------
// Perform column projection and serialization for the given delivery step
//------------------------------------------------------------------------------
void runStep(DeliveryStep& dstep, const string& message)
{
		string nextBandMsg  (message );
		nextBandMsg      += " - nextBand()";
		string serializeMsg (message );
		serializeMsg     += " - serialize()";
		int nextBandCount = 0;

		ByteStream bs;
		TableBand  tb;

//...Perform table band projection and serialization in succession
#if 0
		while (1)
		{
			// timer.start(nextBandMsg);
			tb = dstep.nextBand();
			nextBandCount++;
			// timer.stop (nextBandMsg);

			// timer.start(serializeMsg);
			bs.reset();
			tb.serialize(bs);                
			// timer.stop( serializeMsg);

			if (tb.getRowCount() == 0)
				break;
		}
//...Perform table band projection and serialization in parallel
#else
		string thrCreateMsg (message );
		thrCreateMsg     += " - serialize-thrCreate";
		string thrJoinMsg   (message );
		thrJoinMsg       += " - serialize-thrJoin";
		string serializeWaitMsg(message );
		serializeWaitMsg += " - serialize-Wait";

		//...Would prefer to record this label in projectThreadWrapper, but
		//...Stopwatch is not threadsafe, so safer to put here in main thread,
		//...where the other Stopwatch times are recorded.  Note that this
		//...time will overlap the other timestamps we are recording.
		// timer.start(nextBandMsg);

		//...Start a second thread that will allow us to perform
		//...table projections in parallel with band serialization
		// timer.start(thrCreateMsg);
		TableBandQueueMgr tableBandMgr(&dstep,1);
		pthread_t projectionThread;
		pthread_create(&projectionThread, 0, 
			projectThreadWrapper, &tableBandMgr );
		// timer.stop (thrCreateMsg);

		while (1)
		{
			//...The amount of time we spend waiting will help tell us how
			//...much extra time is being spent constructing the table bands
			// timer.start(serializeWaitMsg);
			boost::scoped_ptr<TableBand> band(tableBandMgr.getNextTableBand());
			nextBandCount++;
			// timer.stop (serializeWaitMsg);

			// timer.start(serializeMsg);
			bs.reset();
			band->serialize(bs);                
			// timer.stop( serializeMsg);

			if (band->getRowCount() == 0)
				break;
		}
		// timer.stop(nextBandMsg);

		// timer.start(thrJoinMsg);
		pthread_join(projectionThread, 0);
		// timer.stop (thrJoinMsg);
#endif
		cout << nextBandCount << " table bands delivered" << endl;
}

//------------------------------------------------------------------------------
// Add elements to a BandedDL (outdated version, replaced by FIFO version)
//------------------------------------------------------------------------------
void addElements(BandedDL<ElementType>* dl1)
{
		ElementType e;

		for (int i = 1; i <= dataSize; i++) 
		{
			e.first = i;
			e.second = i;
			dl1->insert(e);
		}
		dl1->endOfInput();
}

//------------------------------------------------------------------------------
// Add string elements to a FifoDL
//------------------------------------------------------------------------------
void addElements(FIFO<StringElementType>* dl1)
{
		StringElementType e;
		for (int i = 1; i <= dataSize; i++) 
		{
			e.first = i;
			e.second = strtable;
			dl1->insert(e);
		}
		dl1->endOfInput();
}

//------------------------------------------------------------------------------
// Add numeric elements to a FifoDL
//------------------------------------------------------------------------------
void addElements(FIFO<UintRowGroup>* dl1)
{
		ElementType e;
		int wrapCount = 0;
		UintRowGroup rg;
		for (int i = 1; i <= dataSize; i++) 
		{
			e.first = i;
			e.second = i;

			rg.et[wrapCount] = e;
			wrapCount++;
			if(wrapCount == 8192 || i == dataSize)
			{
				rg.count = wrapCount;
				dl1->insert(rg);
				wrapCount = 0;
			}
		}
		dl1->endOfInput();
}

//------------------------------------------------------------------------------
// Test delivery of a single numeric column
//------------------------------------------------------------------------------
void deliveryStep_1()  //number column
{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;

		ResourceManager rm;
		dec = DistributedEngineComm::instance(rm);
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog(1);

		DBRM dbrm;	
		uint32_t uniqueID = dbrm.getUnique32();
		dec->addQueue(uniqueID);

		JobStepAssociation inJs;

		AnyDataListSPtr spdl1(new AnyDataList());
		FIFO<UintRowGroup>* dl1 = new FIFO<UintRowGroup>(1,100);
		addElements(dl1);

		spdl1->fifoDL(dl1);

		inJs.outAdd(spdl1);

		DeliveryStep dstep(inJs, JobStepAssociation(),
			make_table(schema, table),
			cat, 1, 1, 1, flushInterval);

		runStep(dstep, string("deliveryStep_1"));

		dec->removeQueue(uniqueID);
}

//------------------------------------------------------------------------------
// Test delivery of a single string column
//------------------------------------------------------------------------------
/*
void deliveryStep_2()   //string column
{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;

		dec = DistributedEngineComm::instance();
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog(1);

		dec->addSession(12345);
		dec->addStep(12345, 0);

		JobStepAssociation inJs;

		AnyDataListSPtr spdl1(new AnyDataList());
		FIFO<StringElementType>* dl1 = new FIFO<StringElementType>(1,100);
		
		addElements(dl1);
		StringElementType e;

		spdl1->stringDL(dl1);

		inJs.outAdd(spdl1);

		DeliveryStep dstep(inJs, JobStepAssociation(),
			make_table(schema, strtable),
			cat, bandSize, 1, 1, 1, flushInterval);

		runStep(dstep, string("deliveryStep_2"));

		dec->removeSession(12345);
}
*/

//------------------------------------------------------------------------------
// Drives thread to add elements to the specified FIFO.
//------------------------------------------------------------------------------
void* addElementsThreadWrapper( void* pThreadData )
{
		FIFO<UintRowGroup>* dl1 = (FIFO<UintRowGroup>*)pThreadData;

		//cout << "Starting thread to add elements for column " <<
		//	dl1->OID() << endl;
		addElements(dl1);
		//cout << "Finished thread to add elements for column " <<
		//	dl1->OID() << endl;

		return 0;
}

//------------------------------------------------------------------------------
// Test delivery of multiple (numCols) numeric columns
//------------------------------------------------------------------------------
void deliverTest(int numCols)   
{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ResourceManager rm;
		dec = DistributedEngineComm::instance(rm);
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog(1);

		// Get the oid for the first column in the table - usually lineitem.l_orderkey.
		CalpontSystemCatalog::TableName tableName = make_table(schema, colstable);
		CalpontSystemCatalog::ROPair p = cat->tableRID(tableName);
		startingOid = p.objnum + 1;
		
		int startingOid = 3416;  // Oid for lineitem.l_orderkey on your database.

		DBRM dbrm;	
		uint32_t uniqueID = dbrm.getUnique32();
		dec->addQueue(uniqueID);

		JobStepAssociation inJs;

		stringstream ss;
		pthread_t threads[ numCols ];

		for (int i = 0; i < numCols; ++i)
		{
			AnyDataListSPtr spdl1(new AnyDataList());

			// Make FIFO big enough to contain the elements for a flush interval
			FIFO<UintRowGroup>* dl1 = new FIFO<UintRowGroup>(1, fifoSize);
//			BandedDL<ElementType>* dl1 = new BandedDL<ElementType>(1);

			dl1->OID(i+startingOid);	// lineitem first col object id	
			spdl1->fifoDL(dl1);
//			spdl1->bandedDL(dl1);
			inJs.outAdd(spdl1);

			pthread_create(&threads[i], 0, addElementsThreadWrapper, dl1 );
		}
		DeliveryStep dstep(inJs, JobStepAssociation(), tableName,
			cat, 12345, 1, 1, flushInterval);

		ss << "DeliverStep test for " << numCols;

		string message = ss.str();

		runStep(dstep, ss.str());

		for (int i = 0; i < numCols; ++i)
		{
			pthread_join(threads[i], 0);
		}

		dec->removeQueue(uniqueID);

}

//------------------------------------------------------------------------------
// Perform testcases for a 1 column table, a 2 column table, etc, up to
// a table having "maxCols" columns.
//------------------------------------------------------------------------------
void testSizes()   
{

	// Prompt for schema.
	cout << "Enter Schema or Enter for " << schema << ":  ";
	string tmpSchema;
	getline(cin, tmpSchema);
	if(tmpSchema.length() > 0)
	{
		schema = tmpSchema;
	}

	// Prompt for table.
	cout << "Enter Table or Enter for " << colstable << ":  ";
	string tmpTable;
	getline(cin, tmpTable);
	if(tmpTable.length() > 0)
	{
		colstable = tmpTable;
	}

	timer.start("Total");
	int maxCols = 9;
	cout << endl;
	for(int i = 7; i <= maxCols; i++) {
		cout << endl << "Running test " << i << " of " << maxCols << endl;
		stringstream ss;
		ss << "Delivery test for " << dataSize << " rows and " << i << " columns";
		timer.start(ss.str());
		deliverTest(i);
		timer.stop(ss.str());
	}
	timer.stop("Total");
	timer.finish();
}

void *nextBandBenchProducer(void *arg)
{
	FIFO<UintRowGroup>* dl1 = (FIFO<UintRowGroup>*) arg;
	UintRowGroup rg;
	uint64_t *arr;
	uint32_t i;

	arr = (uint64_t*) rg.et;
	for (i = 0; i < 8192; ++i)
		arr[i] = i;
	rg.count = 8192;

	for (i = 1; i <= dataSize/8192; i++) {
//		cout << "inserting set " << i << endl;
		dl1->insert(rg);
	}

	dl1->endOfInput();
	return NULL;
}

void nextBandBenchmark()
{
	ByteStream bs;
	pthread_t threads[columns];
	uint32_t i, rowCount = 1;
	JobStepAssociation inJs;

	for (i = 0; i < columns; ++i) {
		AnyDataListSPtr spdl1(new AnyDataList());

		FIFO<UintRowGroup>* dl1 = new FIFO<UintRowGroup>(1, fifoSize);

		dl1->OID(i);	// lineitem first col object id	
		spdl1->fifoDL(dl1);
		inJs.outAdd(spdl1);

		pthread_create(&threads[i], 0, nextBandBenchProducer, dl1 );
		cout << "started thread " << i << endl;
	}

	DeliveryStep ds(inJs, JobStepAssociation(), 8);
	stringstream ss;

	ss << "nextBandBenchmark with " << columns << " columns\n";

	timer.start(ss.str());
	while (rowCount != 0) {
//		cout << "getting a BS\n";
		rowCount = ds.nextBand(bs);
		bs.restart();
//		cout << "got a BS\n";
	}
	timer.stop(ss.str());
	for (i = 0; i < columns; ++i)
		pthread_join(threads[i], NULL);
}

void queuedBSBenchmark(int queueLength)
{
    ByteStream *bs;
    pthread_t threads[columns];
    uint32_t i, rowCount = 1;
    JobStepAssociation inJs;

    for (i = 0; i < columns; ++i) {
        AnyDataListSPtr spdl1(new AnyDataList());

        FIFO<UintRowGroup>* dl1 = new FIFO<UintRowGroup>(1, fifoSize);

        dl1->OID(i);    // lineitem first col object id
        spdl1->fifoDL(dl1);
        inJs.outAdd(spdl1);

        pthread_create(&threads[i], 0, nextBandBenchProducer, dl1 );
        cout << "started thread " << i << endl;
    }

    DeliveryStep ds(inJs, JobStepAssociation(), 8);
	BSQueueMgr bsq(&ds, queueLength);
	stringstream ss;
	ss << "queuedBSBenchmark with " << columns << " columns and " << queueLength << " queue length\n";
	
    timer.start(ss.str());
	boost::thread(BSProjectThread(&bsq));
    while (rowCount != 0) {
        bs = bsq.getNextByteStream(rowCount);
		delete bs;
    }
    timer.stop(ss.str());
    for (i = 0; i < columns; ++i)
        pthread_join(threads[i], NULL);
}



//------------------------------------------------------------------------------
// Main entry point
//------------------------------------------------------------------------------
int main( int argc, char **argv)
{
  if (argc > 1)
    columns = atoi(argv[1]); // override default number of rows
  else 
	columns = 10;

	while (columns > 0) {

//   testSizes();
		nextBandBenchmark();

		queuedBSBenchmark(10);
		queuedBSBenchmark(9);
		queuedBSBenchmark(8);
		queuedBSBenchmark(5);
		queuedBSBenchmark(4);
		queuedBSBenchmark(1);
		timer.finish();
		columns--;
	}
	return 0;
}

