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

/***********************************************************************
*   $Id: tdriver-bru.cpp 8436 2012-04-04 18:18:21Z rdempsey $
*
*
***********************************************************************/
#include <iostream>
#include <sstream>
#include <fstream>
#include <iomanip>
#include <vector>
#include <set>

#include <pthread.h>
#include <time.h>
#include <sys/time.h>

#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/thread/mutex.hpp>

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "bucketdl.h"
#include "elementtype.h"
#include "stopwatch.cpp"

#include "bucketreuse.h"


// #undef CPPUNIT_ASSERT
// #define CPPUNIT_ASSERT(x)

using namespace std;
using namespace boost;
using namespace joblist;

//Stopwatch timer;

//------------------------------------------------------------------------------
// TestDriver class derived from CppUnit
//------------------------------------------------------------------------------

class BucketReUseDriver : public CppUnit::TestFixture
{

CPPUNIT_TEST_SUITE(BucketReUseDriver);
CPPUNIT_TEST(parseConfig);
CPPUNIT_TEST(createFiles);
CPPUNIT_TEST(reuseFiles);
CPPUNIT_TEST(newversion);
CPPUNIT_TEST(concurrent);
CPPUNIT_TEST(concurrent_newversion);
CPPUNIT_TEST(concurrent_race);
CPPUNIT_TEST_SUITE_END();

private:
public:
	//--------------------------------------------------------------------------
	// setup method run prior to each unit test, inherited from base
	//--------------------------------------------------------------------------
	void setUp() { clock_gettime(CLOCK_REALTIME, &ts); }

	//--------------------------------------------------------------------------
	// validate results from a unit test, inherited from base
	//--------------------------------------------------------------------------
	void validateResults() {}

	//--------------------------------------------------------------------------
	// test functions
	//--------------------------------------------------------------------------
	void parseConfig();
	void createFiles();
	void reuseFiles();
	void newversion();
	void concurrent();
	void concurrent_newversion();
	void concurrent_race();

private:
	//--------------------------------------------------------------------------
	// initialize method
	// oid : reference to the column OID used for test
	//--------------------------------------------------------------------------
	void initControl(execplan::CalpontSystemCatalog::OID& oid);

	//--------------------------------------------------------------------------
	// validate results
	// files: the filenames to be verified if exist
	// exist: expect if the files are created or deleted
	//--------------------------------------------------------------------------
	void validateFileExist(set<string>& files, bool exist);

	static void *insertThread(void*);
	static void *readThread(void*);

	static void *scanThread(void*);
	static void *reuseThread(void*);
	static void *raceThread(void*);

	struct ThreadArg
	{
		uint64_t id;                              // thread id
		uint64_t version;                         // db version
		uint64_t buckets;                         // max bucket numbers
		uint64_t elements;                        // max number of elem per bucket
		uint64_t total;                           // total number of elements
		execplan::CalpontSystemCatalog::OID oid;  // column OID
		BucketDataList* dl;                       // datalist

		// for sync threads
		bool* flag;
		pthread_mutex_t* mutex;
		pthread_cond_t*  cond;

		// for file status check
		set<string>*  files;

		ThreadArg() : id(0), version(0), buckets(0), elements(0), total(0), dl(NULL),
						flag(NULL), mutex(NULL), cond(NULL), files(NULL) {}
	};

	static const string column;

public:
	static struct timespec ts;
};

CPPUNIT_TEST_SUITE_REGISTRATION(BucketReUseDriver);

const string BucketReUseDriver::column = "tpch.lineitem.l_orderkey";
struct timespec BucketReUseDriver::ts;

//------------------------------------------------------------------------------
// main entry point
//------------------------------------------------------------------------------
int main(int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}

struct timespec atTime();
ostream& operator<<(ostream& os, const struct timespec& t);

void BucketReUseDriver::parseConfig()
{
	cout << "ut: parseConfig start...\n" << endl;

	// before run this test, make sure "tpch.lineitem.l_orderkey" is in the Calpont.xml
	BucketReuseManager* control = BucketReuseManager::instance();
	ResourceManager rm;
	BucketReuseManager::instance()->startup(rm);

	// list all the predicates if any listed in the Calpont.xml file
	for (BucketReuseMap::iterator it = control->fControlMap.begin();
									it != control->fControlMap.end(); it++)
		cout << *(it->second);
	cout << endl;

	size_t schemap = column.find_first_of(".");
	size_t columnp = column.find_last_of(".");
	CPPUNIT_ASSERT(schemap != string::npos && columnp != string::npos);

	execplan::CalpontSystemCatalog::TableColName columnName;
	columnName.schema = column.substr(0, schemap);
	columnName.table  = column.substr(schemap+1, columnp-schemap-1);
	columnName.column = column.substr(columnp+1);

	string filter = "allrows";

	CPPUNIT_ASSERT(control->fControlMap.find(BucketFileKey(columnName, filter)) !=
					control->fControlMap.end());

	cout << "ut: parseConfig done!\n" << endl;
}


void BucketReUseDriver::createFiles()
{
	cout << "ut: createFiles start...\n" << endl;

	ThreadArg arg;
	arg.id = 1;
	arg.version = 1;
	arg.buckets = 2;
	arg.elements = 2;
	arg.total = 1000000;

	execplan::CalpontSystemCatalog::OID oid;
	initControl(oid);
	arg.oid = oid;

	set<string> files;
	arg.files = &files;

	pthread_t t;
	pthread_create(&t, NULL, scanThread, &arg);
	pthread_join(t, NULL);

	validateFileExist(files, true);

	cout << "ut: createFiles done!\n" << endl;
}


void BucketReUseDriver::reuseFiles()
{
	cout << "ut: reuseFiles start...\n" << endl;

	ThreadArg arg1;
	arg1.id = 1;
	arg1.version = 1;
	arg1.buckets = 2;
	arg1.elements = 2;
	arg1.total = 1000000;

	execplan::CalpontSystemCatalog::OID oid;
	initControl(oid);
	arg1.oid = oid;

	set<string> files1;
	arg1.files = &files1;

	// create the files
	pthread_t t1;
	pthread_create(&t1, NULL, scanThread, &arg1);
	pthread_join(t1, NULL);

	validateFileExist(files1, true);

	// use new datalist, reuse case
	ThreadArg arg2 = arg1;
	arg2.id = 2;

	set<string> files2;
	arg2.files = &files2;

	pthread_t t2;
	pthread_create(&t2, NULL, reuseThread, &arg2);
	pthread_join(t2, NULL);

	validateFileExist(files2, true);

	cout << "ut: reuseFiles done!\n" << endl;
}


void BucketReUseDriver::newversion()
{
	cout << "ut: newversion start...\n" << endl;

	ThreadArg arg1;
	arg1.id = 1;
	arg1.version = 1;
	arg1.buckets = 2;
	arg1.elements = 2;
	arg1.total = 1000000;

	execplan::CalpontSystemCatalog::OID oid;
	initControl(oid);
	arg1.oid = oid;

	set<string> files1;
	arg1.files = &files1;

	// create the files
	pthread_t t1;
	pthread_create(&t1, NULL, scanThread, &arg1);
	pthread_join(t1, NULL);

	validateFileExist(files1, true);

	// new version
	ThreadArg arg2 = arg1;
	arg2.id = 2;
	arg2.version = 2;

	set<string> files2;
	arg2.files = &files2;

	pthread_t t2;
	pthread_create(&t2, NULL, scanThread, &arg2);
	pthread_join(t2, NULL);

	pthread_yield();

	validateFileExist(files1, false);
	validateFileExist(files2, true);

	// read from the new files with new datalist
	ThreadArg arg3 = arg2;
	arg3.id = 3;
	arg3.dl = NULL;

	set<string> files3;
	arg3.files = &files3;

	pthread_t t3;
	pthread_create(&t3, NULL, reuseThread, &arg3);
	pthread_join(t3, NULL);

	validateFileExist(files3, true);

	cout << "ut: newversion done!\n" << endl;
}

void BucketReUseDriver::concurrent()
{
	cout << "ut: concurrent start...\n" << endl;

	ThreadArg arg1;
	arg1.id = 1;
	arg1.version = 1;
	arg1.buckets = 2;
	arg1.elements = 2;
	arg1.total = 1000000;

	execplan::CalpontSystemCatalog::OID oid;
	initControl(oid);
	arg1.oid = oid;

	set<string> files1;
	arg1.files = &files1;

	// create the files
	pthread_t t1;
	pthread_create(&t1, NULL, scanThread, &arg1);

	sleep(1);

	// reuse case, current thread
	ThreadArg arg2 = arg1;
	arg2.id = 2;
	set<string> files2;
	arg2.files = &files2;

	pthread_t t2;
	pthread_create(&t2, NULL, reuseThread, &arg2);

	pthread_join(t1, NULL);
	validateFileExist(files1, true);

	pthread_join(t2, NULL);
	validateFileExist(files2, true);

	cout << "ut: concurrent done!\n" << endl;
}

void BucketReUseDriver::concurrent_newversion()
{
	cout << "ut: concurrent_newversion start...\n" << endl;

	bool flag = false;
	pthread_mutex_t mutex;
	pthread_mutex_init(&mutex, 0);
	pthread_cond_t  cond;
	pthread_cond_init(&cond, 0);

	ThreadArg arg1;
	arg1.id = 1;
	arg1.version = 1;
	arg1.buckets = 2;
	arg1.elements = 2;
	arg1.total = 1000000;

	execplan::CalpontSystemCatalog::OID oid;
	initControl(oid);
	arg1.oid = oid;

	set<string> files1;
	arg1.files = &files1;

	// create the files
	pthread_t t1;
	pthread_create(&t1, NULL, scanThread, &arg1);

	// reuse case, current thread
	ThreadArg arg2 = arg1;
	arg2.id = 2;
	set<string> files2;
	arg2.files = &files2;

	sleep(1);

	pthread_t t2;
	pthread_create(&t2, NULL, reuseThread, &arg2);

	// new version
	ThreadArg arg3 = arg1;
	arg3.id = 3;
	arg3.version = 3;
	arg3.flag = &flag;
	arg3.mutex = &mutex;
	arg3.cond = &cond;
	set<string> files3;
	arg3.files = &files3;

	pthread_t t3;
	pthread_create(&t3, NULL, scanThread, &arg3);

	sleep(3);

	pthread_mutex_lock(&mutex);
	flag = true;
	pthread_cond_broadcast(&cond);
	pthread_mutex_unlock(&mutex);

	pthread_join(t1, NULL);
	pthread_join(t2, NULL);
	pthread_join(t3, NULL);

	// let cleanup thread do its job
	pthread_yield();

	validateFileExist(files1, false);
	validateFileExist(files2, false);
	validateFileExist(files3, true);

	pthread_mutex_destroy(&mutex);
	pthread_cond_destroy(&cond);

	cout << "ut: concurrent_newversion done!\n" << endl;
}

void BucketReUseDriver::concurrent_race()
{
	cout << "ut: concurrent_race start...\n" << endl;

	ThreadArg arg1;
	arg1.id = 1;
	arg1.version = 1;
	arg1.buckets = 2;
	arg1.elements = 2;
	arg1.total = 2000000;

	execplan::CalpontSystemCatalog::OID oid;
	initControl(oid);
	arg1.oid = oid;

	set<string> files1;
	arg1.files = &files1;

	ThreadArg arg2 = arg1;
	arg2.id = 2;
	set<string> files2;
	arg2.files = &files2;

	// start the version 1 threads
	pthread_t t1;
	pthread_t t2;
	pthread_create(&t1, NULL, raceThread, &arg1);
	pthread_create(&t2, NULL, raceThread, &arg2);

	// let the version 1 threads register
	sleep(1);

	ThreadArg arg3 = arg1;
	arg3.id = 3;
	arg3.version = 4;
	arg3.total = 1000000;
	set<string> files3;
	arg3.files = &files3;

	ThreadArg arg4 = arg3;
	arg4.id = 4;
	set<string> files4;
	arg4.files = &files4;

	// start the version 4 threads
	pthread_t t3;
	pthread_t t4;
	pthread_create(&t3, NULL, raceThread, &arg3);
	pthread_create(&t4, NULL, raceThread, &arg4);

	pthread_join(t3, NULL);
	pthread_join(t4, NULL);
	validateFileExist(files1, true);
	validateFileExist(files4, true);
	
	pthread_join(t1, NULL);
	pthread_join(t2, NULL);
	validateFileExist(files1, false);
	validateFileExist(files4, true);

	cout << "ut: concurrent_race done!\n" << endl;
}

void BucketReUseDriver::initControl(execplan::CalpontSystemCatalog::OID& oid)
{
	// make sure the column for testing is in the map, "column" is a class variable
	// to test with another column, only one place to change
	// -- to validate the config parsing, run parseConfig --
	config::Config* cf = config::Config::makeConfig();
	vector<string> columns;
	cf->getConfig("HashBucketReuse", "Predicate", columns);

	bool found = true;
	for (vector<string>::iterator it = columns.begin(); it != columns.end(); ++it)
	{
		if (it->compare(0, column.size(), column) == 0)
		{
			found = true;
			break;
		}
	}

	string filter = "allrows";
	BucketReuseManager* control = BucketReuseManager::instance();
	if (found == false)
	{
		cout << "tpch.lineitem.l_orderkey is not in the Calpont.xml!)" << endl;
		cout << "insert it to countinue unit test" << endl;

		size_t schemap = column.find_first_of(".");
		size_t columnp = column.find_last_of(".");
		CPPUNIT_ASSERT(schemap != string::npos && columnp != string::npos);

		execplan::CalpontSystemCatalog::TableColName tcn;
		tcn.schema = column.substr(0, schemap);
		tcn.table  = column.substr(schemap+1, columnp-schemap-1);
		tcn.column = column.substr(columnp+1);

		control->fConfigMap.insert(pair<string, BucketFileKey>(column, BucketFileKey(tcn, filter)));
	}

	ResourceManager rm;
	// now  start the BucketReuseManager
	BucketReuseManager::instance()->startup(rm);

	// get the oid for registration
	size_t schemap = column.find_first_of(".");
	size_t columnp = column.find_last_of(".");
	CPPUNIT_ASSERT(schemap != string::npos && columnp != string::npos);

	execplan::CalpontSystemCatalog::TableColName columnName;
	columnName.schema = column.substr(0, schemap);
	columnName.table  = column.substr(schemap+1, columnp-schemap-1);
	columnName.column = column.substr(columnp+1);

	CPPUNIT_ASSERT(control->fControlMap.find(BucketFileKey(columnName, filter)) !=
					control->fControlMap.end());
}


void BucketReUseDriver::validateFileExist(set<string>& files, bool exist)
{
	cout << "\ncheck if files exist or not:" << endl;
	for (set<string>::iterator i = files.begin(); i != files.end(); i++)
	{
		filesystem::path p(i->c_str());
		cout << (*i) << "-- ";

		if (exist)
		{
			CPPUNIT_ASSERT(filesystem::exists(p));
			cout << "OK" << endl;
		}
		else
		{
			CPPUNIT_ASSERT(!filesystem::exists(p));
			cout << "GONE" << endl;
		}
	}
	cout << endl;
}


void *BucketReUseDriver::insertThread(void * arg)
{
	ThreadArg* a = reinterpret_cast<ThreadArg*>(arg);
	BucketDataList* dl = a->dl;
	CPPUNIT_ASSERT(dl != NULL);

	cout << "thread " << a->id << " start at " << atTime() << endl;

	BucketReuseControlEntry* entry = dl->reuseControl();
	for (uint64_t i = 0; i < a->buckets; i++)
	{
		stringstream ss;
		ss << entry->baseName() << "." << i;
		filesystem::path p(ss.str().c_str());
		a->files->insert(ss.str());
	}

	ElementType e;
	for (uint64_t i = 0; i < a->total; i++)
	{
		e.first = i;
		e.second = i * 10 + a->version;  // include the version in values
		dl->insert(e);
	}

	cout << "thread[" << a->id << "] last element inserted at " << atTime() << endl;
	dl->endOfInput();

	cout << "thread " << a->id << " finished at " << atTime() << endl;

	return NULL;
}


void *BucketReUseDriver::readThread(void* arg)
{
	ThreadArg* a = reinterpret_cast<ThreadArg*>(arg);
	BucketDataList* dl = a->dl;
	CPPUNIT_ASSERT(dl != NULL);

	cout << "thread " << a->id << " start at " << atTime() << endl;

	BucketReuseControlEntry* entry = dl->reuseControl();
	for (uint64_t i = 0; i < a->buckets; i++)
	{
		stringstream ss;
		ss << entry->baseName() << "." << i;
		filesystem::path p(ss.str().c_str());
		a->files->insert(ss.str());
	}

	ElementType e;
	uint64_t min=0xffffffff, max=0, count=0;
	uint64_t k[a->buckets];  // count of each bucket
	bool firstRead = true;
	for (uint64_t i = 0; i < a->buckets; i++)
	{
		k[i] = 0;
		uint64_t it = dl->getIterator(i);
		while (dl->next(i, it, &e))
		{
			if (firstRead)
			{
				cout << "thread[" << a->id << "] first read at " << atTime() << endl;
				firstRead = false;
			}

			if (e.second < min) min = e.second;
			if (e.second > max) max = e.second;

			// output the first 10 of each bucket or last 10 of the datalist
			//if (count < 10 || (a->total - count) < 10 || k[i] < 10)
			if (count < 2 || (a->total - count) < 2 || k[i] < 2)
				cout << "thread[" << a->id << "] bucket:" << i
					 << " e(" << e.first << ", " << e.second << ")" << endl;

			count++;
			k[i]++;
		}
	}

	cout << "\nthread[" << a->id << "] element: count = " << count
		 << ", min/max = " << min << "/" << max << ", elements in each bucket: ";
	for (uint64_t i = 0; i < a->buckets; i++)
		cout << k[i] << " ";
	cout << endl;

	cout << "thread " << a->id << " finished at " << atTime() << endl;

	return NULL;
}


void *BucketReUseDriver::scanThread(void* arg)
{
	ThreadArg* a = reinterpret_cast<ThreadArg*>(arg);
	BucketDataList* dl = a->dl;
	CPPUNIT_ASSERT(dl == NULL);

	if (a->cond != NULL)
	{
		pthread_mutex_lock(a->mutex);
		while (*(a->flag) != true)
			pthread_cond_wait(a->cond, a->mutex);
		pthread_mutex_unlock(a->mutex);
	}

	cout << "thread " << a->id << " start at " << atTime() << endl;

	string dummy;
	bool scan = false;
	execplan::CalpontSystemCatalog *c =
						execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(0x80000000);
	execplan::CalpontSystemCatalog::TableColName tcn = c->colName(a->oid);
	BucketReuseControlEntry* entry = BucketReuseManager::instance()->userRegister(
										tcn, dummy, a->version, a->buckets, scan);
	CPPUNIT_ASSERT(scan == true);

	ResourceManager rm;
	dl = new BucketDataList(a->buckets, 1, a->elements, rm);
	dl->setElementMode(1);
	dl->reuseControl(entry, !scan);

	ThreadArg arg1 = *a;
	arg1.id *= 10;
	arg1.dl = dl;

	pthread_t t1;
	pthread_create(&t1, NULL, insertThread, &arg1);

	ThreadArg arg2 = arg1;
	arg2.id += 1;
	pthread_t t2;
	pthread_create(&t2, NULL, readThread, &arg2);

	pthread_join(t1, NULL);
	pthread_join(t2, NULL);

	delete dl;

	cout << "thread " << a->id << " finished at " << atTime() << endl;

	return NULL;
}


void *BucketReUseDriver::reuseThread(void* arg)
{
	ThreadArg* a = reinterpret_cast<ThreadArg*>(arg);
	BucketDataList* dl = a->dl;
	CPPUNIT_ASSERT(dl == NULL);

	if (a->cond != NULL)
	{
		pthread_mutex_lock(a->mutex);
		while (*(a->flag) != true)
			pthread_cond_wait(a->cond, a->mutex);
		pthread_mutex_unlock(a->mutex);
	}

	cout << "thread " << a->id << " start at " << atTime() << endl;

	string dummy;
	bool scan = true;
	execplan::CalpontSystemCatalog *c =
						execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(0x80000000);
	execplan::CalpontSystemCatalog::TableColName tcn = c->colName(a->oid);
	BucketReuseControlEntry* entry = BucketReuseManager::instance()->userRegister(
										tcn, dummy, a->version, a->buckets, scan);
	CPPUNIT_ASSERT(scan == false);

	ResourceManager rm;
	dl = new BucketDataList(a->buckets, 1, a->elements, rm);
	dl->setElementMode(1);
	dl->reuseControl(entry, !scan);

	ThreadArg arg1 = *a;
	arg1.id *= 10;
	arg1.dl = dl;

	pthread_t t1;
	pthread_create(&t1, NULL, readThread, &arg1);

	if (entry->fileStatus() == BucketReuseControlEntry::progress_c)
	{
		boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
		dl->reuseControl()->stateChange().wait(lock);
	}
	else
	{
		CPPUNIT_ASSERT((entry->fileStatus() == BucketReuseControlEntry::using_c) ||
						(entry->fileStatus() == BucketReuseControlEntry::ready_c));
	}

	// the bucket files are ready
	dl->restoreBucketInformation();
	dl->endOfInput();

	pthread_join(t1, NULL);

	delete dl;

	cout << "thread " << a->id << " finished at " << atTime() << endl;

	return NULL;
}


void *BucketReUseDriver::raceThread(void* arg)
{
	ThreadArg* a = reinterpret_cast<ThreadArg*>(arg);
	BucketDataList* dl = a->dl;
	CPPUNIT_ASSERT(dl == NULL);

	if (a->cond != NULL)
	{
		pthread_mutex_lock(a->mutex);
		while (*(a->flag) != true)
			pthread_cond_wait(a->cond, a->mutex);
		pthread_mutex_unlock(a->mutex);
	}

	cout << "thread " << a->id << " start at " << atTime() << endl;

	string dummy;
	bool scan = true;
	ResourceManager rm;
	BucketReuseControlEntry* entry = NULL;
	{
		boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
		execplan::CalpontSystemCatalog *c =
						execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(0x80000000);
		execplan::CalpontSystemCatalog::TableColName tcn = c->colName(a->oid);
		entry = BucketReuseManager::instance()->userRegister(
										tcn, dummy, a->version, a->buckets, scan);

		dl = new BucketDataList(a->buckets, 1, a->elements, rm);
		dl->setElementMode(1);
		dl->reuseControl(entry, !scan);
	}

	if (scan == true)
	{
		CPPUNIT_ASSERT(entry->fileStatus() == BucketReuseControlEntry::progress_c);

		ThreadArg arg1 = *a;
		arg1.id *= 10;
		arg1.dl = dl;

		pthread_t t1;
		pthread_create(&t1, NULL, insertThread, &arg1);

		ThreadArg arg2 = arg1;
		arg2.id += 1;
		pthread_t t2;
		pthread_create(&t2, NULL, readThread, &arg2);

		pthread_join(t1, NULL);
		pthread_join(t2, NULL);
	}
	else
	{
		ThreadArg arg1 = *a;
		arg1.id *= 10;
		arg1.dl = dl;

		pthread_t t1;
		pthread_create(&t1, NULL, readThread, &arg1);

		if (entry->fileStatus() == BucketReuseControlEntry::progress_c)
		{
			boost::mutex::scoped_lock lock(BucketReuseManager::instance()->getMutex());
			dl->reuseControl()->stateChange().wait(lock);
		}
		else
		{
			CPPUNIT_ASSERT((entry->fileStatus() == BucketReuseControlEntry::using_c) ||
							(entry->fileStatus() == BucketReuseControlEntry::ready_c));
		}

		// the bucket files are ready
		dl->restoreBucketInformation();
		dl->endOfInput();

		pthread_join(t1, NULL);
	}

	delete dl;

	cout << "thread " << a->id << " finished at " << atTime() << endl;

	return NULL;
}


timespec atTime()
{
	timespec ts, ts1, ts2;
	ts1 = BucketReUseDriver::ts;
	clock_gettime(CLOCK_REALTIME, &ts2);

	if (ts2.tv_nsec < ts1.tv_nsec)
	{
		ts.tv_sec  = ts2.tv_sec - ts1.tv_sec - 1;
		ts.tv_nsec = ts2.tv_nsec + 1000000000 - ts1.tv_nsec;
	}
	else
	{
		ts.tv_sec  = ts2.tv_sec - ts1.tv_sec;
		ts.tv_nsec = ts2.tv_nsec - ts1.tv_nsec;
	}
	return ts;
}

ostream& operator<<(ostream& os, const struct timespec& t)
{
	os << t.tv_sec << "." << setw(9) << setfill('0') << t.tv_nsec << "s";
	return os;
}

