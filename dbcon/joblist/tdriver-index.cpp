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
 * $Id: tdriver-index.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 ****************************************************************************/
/*
 * Brief description of the file contents
 *
 * More detailed description
 */	

#include <iostream>

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "joblist.h"
#include "jobstep.h"
#include "pidxwalk.h"
#include "pidxlist.h"
#include "distributedenginecomm.h"
#include "calpontsystemcatalog.h"

using namespace std;
using namespace joblist;
using namespace execplan;


CalpontSystemCatalog::TableColName testcol = { "tpch", "orders", "o_orderkey"};
CalpontSystemCatalog::TableColName largecol = { "tpch", "lineitem", "l_orderkey"};

class JobStepDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(JobStepDriver);

// CPPUNIT_TEST(indexTest_ss1);
// CPPUNIT_TEST(indexTest_ss2);
// CPPUNIT_TEST(indexTest_many);
// CPPUNIT_TEST(indexTest_many2);
CPPUNIT_TEST(indexTest_lists);

CPPUNIT_TEST_SUITE_END();


private:
     	int getIndexOID(const CalpontSystemCatalog::TableColName&  col, boost::shared_ptr<CalpontSystemCatalog> cat)
	{	
		const CalpontSystemCatalog::IndexNameList iNames = cat->colValueSysindexCol(col);
		if (0 == iNames.size())
		{
			cout << "No index for " << col << endl;
			return -1;
		}
		CalpontSystemCatalog::IndexOID ixoid = cat->lookupIndexNbr(*iNames.begin());
		return ixoid.objnum;
	}

public:
	// 1 search string
    	void indexTest_ss1() 
	{
		ResourceManager rm;
		DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
		boost::shared_ptr<CalpontSystemCatalog> cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation walkJs;

		AnyDataListSPtr spdlw(new AnyDataList());
		BandedDL<ElementType>* dlw = new BandedDL<ElementType>(1, rm);
		spdlw->bandedDL(dlw);
		walkJs.outAdd(spdlw);

 		int oid = getIndexOID(testcol, cat); //returns 3154 
		if (0 > oid) return;
	
		pIdxWalk step0(inJs, walkJs, dec, cat, oid, 12345, 999, 7, 0, 0, 0, 0);

		step0.addSearchStr(COMPARE_EQ, 3);

		step0.run();

		step0.join();


		ElementType e;

		JobStepAssociation outJs;
		AnyDataListSPtr spdlo(new AnyDataList());
		BandedDL<ElementType>* dlo = new BandedDL<ElementType>(1, rm);
		spdlo->bandedDL(dlo);
		outJs.outAdd(spdlo);


		pIdxList step1(walkJs, outJs, dec, cat, 12345, 999, 7, 0, 0, 0, 0);
		step1.run();
		step1.join();

		int it = dlo->getIterator();
		int i = 1;
		while  (dlo->next(it, &e) ) 
		{
			cout << i++ << " <ss1 Rid:  " << (int)e.first << ">\n";
		}


	}
		// 2 search strings
    	void indexTest_ss2() 
	{
		ResourceManager rm;
		DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
		boost::shared_ptr<CalpontSystemCatalog> cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation walkJs;

		AnyDataListSPtr spdlw(new AnyDataList());
		BandedDL<ElementType>* dlw = new BandedDL<ElementType>(1, rm);
		spdlw->bandedDL(dlw);	


		walkJs.outAdd(spdlw);

 		int oid = getIndexOID(testcol, cat); //returns 3154 
		if (0 > oid) return;

		pIdxWalk step0(inJs, walkJs, dec, cat, oid, 12345, 999, 7, 0, 0, 0, 0);

		step0.addSearchStr(COMPARE_GT, 3);
		step0.addSearchStr(COMPARE_LT, 60);
		step0.setBOP(BOP_AND);

		step0.run();

		step0.join();

		JobStepAssociation outJs;

		AnyDataListSPtr spdlo(new AnyDataList());
		BandedDL<ElementType>* dlo = new BandedDL<ElementType>(1, rm);
		spdlo->bandedDL(dlo);
		outJs.outAdd(spdlo);

		pIdxList step1(walkJs, outJs, dec, cat, 12345, 999, 7, 0, 0, 0, 0);

		step1.run();
		step1.join();

		int it = dlo->getIterator();
		int i = 1;
		ElementType e;
		while  (dlo->next(it, &e) ) 
		{
			cout << i++ << " <ss2 Rid:  " << (int)e.first << ">\n";
		}
	}
		// input list of tokens
    	void indexTest_many() 
	{
		ResourceManager rm;
		DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
		boost::shared_ptr<CalpontSystemCatalog> cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

// 		dec->addSession(12345);
// 		dec->addStep(12345, 0);

		JobStepAssociation inJs;
		AnyDataListSPtr spdli(new AnyDataList());
		BandedDL<ElementType>* dli = new BandedDL<ElementType>(1, rm);
		spdli->bandedDL(dli);
		ElementType e;
		for (e.second = 1; e.second < 100; ++e.second) 
		{
			if (0 == e.second % 3 )   
				dli->insert(e);
		}
		dli->endOfInput();
		inJs.outAdd(spdli);

		JobStepAssociation walkJs;

		AnyDataListSPtr spdlw(new AnyDataList());
		BandedDL<ElementType>* dlw = new BandedDL<ElementType>(1, rm);
		spdlw->bandedDL(dlw);
		walkJs.outAdd(spdlw);

 		int oid = getIndexOID(testcol, cat); //returns 3154 
		if (0 > oid) return;


		pIdxWalk step0(inJs, walkJs, dec, cat, oid, 12345, 999, 7, 0, 0, 0, 0);

		step0.run();
		step0.join();

		JobStepAssociation outJs;

		AnyDataListSPtr spdlo(new AnyDataList());
		BandedDL<ElementType>* dlo = new BandedDL<ElementType>(1, rm);
		spdlo->bandedDL(dlo);
		outJs.outAdd(spdlo);

		pIdxList step1(walkJs, outJs, dec, cat, 12345, 999, 7, 0, 0, 0, 0);

		step1.run();
		step1.join();
// 		dec->removeSession(12345);

		int it = dlo->getIterator();
		int i = 1;
		while  (dlo->next(it, &e) ) 
		{
			cout << i++ << " <many Rid:  " << (int)e.first << ">\n";
		}
	}
	// 2 tokens; should use search string
   	void indexTest_many2() 
	{
		ResourceManager rm;
		DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
		boost::shared_ptr<CalpontSystemCatalog> cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

// 		dec->addSession(12345);
// 		dec->addStep(12345, 0);

		JobStepAssociation inJs;
		AnyDataListSPtr spdli(new AnyDataList());
		BandedDL<ElementType>* dli = new BandedDL<ElementType>(1, rm);
		spdli->bandedDL(dli);
		ElementType e;
		e.second = 3;
		dli->insert(e);
		e.second = 32;
		dli->insert(e);

		dli->endOfInput();
		inJs.outAdd(spdli);

		JobStepAssociation walkJs;

		AnyDataListSPtr spdlw(new AnyDataList());
		BandedDL<ElementType>* dlw = new BandedDL<ElementType>(1, rm);
		spdlw->bandedDL(dlw);
		walkJs.outAdd(spdlw);

 		int oid = getIndexOID(testcol, cat); //returns 3154 
		if (0 > oid) return;

		pIdxWalk step0(inJs, walkJs, dec, cat, oid, 12345, 999, 7, 0, 0, 0, 0);

		step0.run();
		step0.join();

		JobStepAssociation outJs;

		AnyDataListSPtr spdlo(new AnyDataList());
		BandedDL<ElementType>* dlo = new BandedDL<ElementType>(1, rm);
		spdlo->bandedDL(dlo);
		outJs.outAdd(spdlo);

		pIdxList step1(walkJs, outJs, dec, cat, 12345, 999, 7, 0, 0, 0, 0);

		step1.run();
		step1.join();
// 		dec->removeSession(12345);

		int it = dlo->getIterator();
		int i = 1;
		while  (dlo->next(it, &e) ) 
		{
			cout << i++ << " <many2 Rid:  " << (int)e.first << ">\n";
		}
	}
		//Send enough data so that index list must send it back to primitives
    	void indexTest_lists() 
	{
		ResourceManager rm;
		DistributedEngineComm* dec = DistributedEngineComm::instance(rm);
		boost::shared_ptr<CalpontSystemCatalog> cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation walkJs;

		AnyDataListSPtr spdlw(new AnyDataList());
		BandedDL<ElementType>* dlw = new BandedDL<ElementType>(1, rm);
		spdlw->bandedDL(dlw);
		walkJs.outAdd(spdlw);

 		int oid = getIndexOID(largecol, cat); //returns 3152 
		if (0 > oid) return;


		pIdxWalk step0(inJs, walkJs, dec, cat, oid, 12345, 999, 7, 0, 0, 0, 0);

		step0.addSearchStr(COMPARE_GT, 3);

		step0.run();

		step0.join();

		JobStepAssociation outJs;
		AnyDataListSPtr spdlo(new AnyDataList());
		BandedDL<ElementType>* dlo = new BandedDL<ElementType>(1, rm);
		spdlo->bandedDL(dlo);
		outJs.outAdd(spdlo);

		pIdxList step1(walkJs, outJs, dec, cat, 12345, 999, 7, 0, 0, 0, 0);

		step1.run();
		step1.join();
		//cout << "lists returned " <<  dlo->size() << " values.\n";
	}


};

CPPUNIT_TEST_SUITE_REGISTRATION(JobStepDriver);


int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}
