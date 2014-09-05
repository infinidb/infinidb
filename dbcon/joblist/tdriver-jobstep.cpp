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
 * $Id: tdriver-jobstep.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 ****************************************************************************/

#include <iostream>

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "joblist.h"
#include "jobstep.h"
#include "distributedenginecomm.h"
#include "calpontsystemcatalog.h"
#include "zdl.h"

using namespace std;
using namespace joblist;
using namespace execplan;

uint64_t count = 1000000;
const uint64_t ZDL_VEC_SIZE = 4096;

class JobStepDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(JobStepDriver);

/* These rely on Patrick's DB */
// CPPUNIT_TEST(pColScan_1);
// CPPUNIT_TEST(pColStep_1);
// CPPUNIT_TEST(pColStep_2);
// CPPUNIT_TEST(pColStep_as_ProjectionStep_1);

// CPPUNIT_TEST(pnljoin_1);	// value list, no rid list, no reduction step
// CPPUNIT_TEST(pnljoin_2);	// value list, w/rid list, no reduction step
// CPPUNIT_TEST(pnljoin_3);	// value list + rid list + reduction step

CPPUNIT_TEST(reduceStep_1);	// ElementType
CPPUNIT_TEST(reduceStep_2);	// StringElementType
//CPPUNIT_TEST(reduceStep_3); // DoubleElementType
//CPPUNIT_TEST(reduceStep_4); // reduceStep_1 with BucketDLs as inputs

CPPUNIT_TEST(unionStep_1);
//CPPUNIT_TEST(unionStep_2);
//CPPUNIT_TEST(unionStep_3);

CPPUNIT_TEST_SUITE_END();

ResourceManager fRm;

private:
public:

    void pColScan_1() 
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;

		AnyDataListSPtr spdl1(new AnyDataList());
		BandedDL<ElementType>* dl1 = new BandedDL<ElementType>(1, fRm);
		spdl1->bandedDL(dl1);
		outJs.outAdd(spdl1);

		pColScanStep step0(inJs, outJs, dec, cat, 1003, 1000, 12345, 999, 7, 0, 0, fRm);
		int8_t cop;
		int64_t filterValue;
		cop = COMPARE_GE;
		filterValue = 3010;
		step0.addFilter(cop, filterValue);
		cop = COMPARE_LE;
		filterValue = 3318;
		step0.addFilter(cop, filterValue);
		step0.setBOP(BOP_AND);
		inJs = outJs;

		step0.run();

		step0.join();

 		DeliveryStep step1(inJs, outJs, make_table("CALPONTSYS", "SYSTABLE"), cat, 10000, 0, 0, 0);
		inJs = outJs;

		step1.run();

		step1.join();
	}

	void pColStep_1()
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		int i, it;
		bool more;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;
		
		AnyDataListSPtr spdl1(new AnyDataList());
		BandedDL<ElementType>* dl1 = new BandedDL<ElementType>(1, fRm);
		spdl1->bandedDL(dl1);
		inJs.outAdd(spdl1);

		AnyDataListSPtr spdl2(new AnyDataList());
		BandedDL<ElementType>* dl2 = new BandedDL<ElementType>(1, fRm);
		spdl2->bandedDL(dl2);
		outJs.outAdd(spdl2);

		for (i = 10; i < 15; i++) {
			e.first = i;
			dl1->insert(e);
		}
		dl1->endOfInput();

		pColStep p(inJs, outJs, dec, cat, 1003, 1000, 12346, 11, 11, 1, 0, fRm);

		p.setRidList(dl1);    // JSA should do this
		p.run();
		p.join();

		it = dl2->getIterator();
		for (more = dl2->next(it, &e), i = 0; more; more = dl2->next(it, &e), i++)
#ifdef DEBUG
			cout << "<rid = " << e.first << ", value = " << e.second << ">" << endl;
#else
			;  // walk the list silently
#endif
		CPPUNIT_ASSERT(i == 5);
	}

	/* make sure it issues multiple primitive msgs correctly */
	void pColStep_2() 
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		int i, it;
		bool more;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;
		
		AnyDataListSPtr spdl1(new AnyDataList());
		BandedDL<ElementType>* dl1 = new BandedDL<ElementType>(1, fRm);
		spdl1->bandedDL(dl1);
		inJs.outAdd(spdl1);

		AnyDataListSPtr spdl2(new AnyDataList());
		BandedDL<ElementType>* dl2 = new BandedDL<ElementType>(1, fRm);
		spdl2->bandedDL(dl2);
		outJs.outAdd(spdl2);

		for (i = 10; i < 10000; i++) {
			if (i % 2 == 0) {  // make it sparse
				e.first = i;
				dl1->insert(e);
			}
		}
		dl1->endOfInput();

		pColStep p(inJs, outJs, dec, cat, 1003, 1000, 12347, 11, 11, 1, 0, fRm);

		p.setRidList(dl1);    // JSA should do this
		p.run();
		p.join();

		it = dl2->getIterator();
		for (more = dl2->next(it, &e), i = 0; more; more = dl2->next(it, &e), i++)
#ifdef DEBUG
			cout << "<rid = " << e.first << ", value = " << e.second << ">" << endl;
#else
			;  // walk the list silently
#endif
		CPPUNIT_ASSERT(i == 6);
	}

	void pColStep_as_ProjectionStep_1()
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		int i, it;
		bool more;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;
		
		AnyDataListSPtr spdl1(new AnyDataList());
		BandedDL<ElementType>* dl1 = new BandedDL<ElementType>(1, fRm);
		spdl1->bandedDL(dl1);
		inJs.outAdd(spdl1);

		AnyDataListSPtr spdl2(new AnyDataList());
		BandedDL<ElementType>* dl2 = new BandedDL<ElementType>(1, fRm);
		spdl2->bandedDL(dl2);
		outJs.outAdd(spdl2);

		for (i = 1; i <= 21; i++) {
			e.first = i;
			dl1->insert(e);
		}
		dl1->endOfInput();

		// flushInterval = 8
		pColStep p(inJs, outJs, dec, cat, 1003, 1000, 12348, 11, 11, 1, 2, fRm);

		p.setRidList(dl1);    // JSA should do this
		p.run();
		p.join();

		it = dl2->getIterator();
		for (more = dl2->next(it, &e), i = 0; more; more = dl2->next(it, &e), i++)
#ifdef DEBUG
			cout << "<rid = " << e.first << ", value = " << e.second << ">" << endl;
#else
			;  // walk the list silently
#endif
// 		CPPUNIT_ASSERT(i == 5);
	}

	void reduceStep_1()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		WSDL<ElementType>  *outDL;
		ElementType e;
		unsigned i;
		int it;
		bool more;



		ZDL<ElementType>* inDL = new ZDL<ElementType>(1, fRm);
		ZDL<ElementType>* dDL = new ZDL<ElementType>(1, fRm);
		outDL = new WSDL<ElementType>(1, 100000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->zonedDL(inDL);
		driverADL->zonedDL(dDL);
		outputADL->workingSetDL(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);

// 		cout << "making input DataList" << endl;
   		vector <ElementType> vec1;
  		vector <ElementType> vec2;

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = i+1;
			vec1.push_back(e);
		        if (vec1.size() >= ZDL_VEC_SIZE)
			{
				inDL->insert(vec1);
				vec1.clear();
			}
// 			inDL->insert(e);
		}
		if (!vec1.empty())
			inDL->insert(vec1);
		inDL->endOfInput();

// 		cout << "making driver DataList" << endl;

		for (i = 0; i < ::count; i+=2) {
			e.first = i;
			e.second = i+1;
			vec2.push_back(e);
		        if (vec2.size() >= ZDL_VEC_SIZE)
			{
				dDL->insert(vec2);
				vec2.clear();
			}

// 			dDL->insert(e);
		}
		if (!vec2.empty())
			dDL->insert(vec2);
		dDL->endOfInput();

// 		cout << "reducing" << endl;

		ReduceStep rs(in, out, 5, 1, 0, 0, 0, 0);
		rs.run();
		rs.join();

		it = outDL->getIterator();
		more = outDL->next(it, &e);
		i = 0;
		while (more) {
//  			cout << i << ": first: " << e.first << " second: " << e.second << endl;
			CPPUNIT_ASSERT(e.first < ::count);
			CPPUNIT_ASSERT(e.first % 2 == 0); 
			more = outDL->next(it, &e);
			i++;
		}

	}

	void reduceStep_2()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		WSDL<StringElementType> *outDL;
		StringElementType e;
		unsigned i;
		int it;
		bool more;

// 		inDL = new WSDL<StringElementType>(1, 10000, fRm);
// 		dDL = new WSDL<StringElementType>(1, 10000, fRm);
		ZDL<StringElementType>* inDL = new ZDL<StringElementType>(1, fRm);
		ZDL<StringElementType>* dDL = new ZDL<StringElementType>(1, fRm);

		outDL = new WSDL<StringElementType>(1, 10000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->stringZonedDL(inDL);
		driverADL->stringZonedDL(dDL);
		outputADL->strDataList(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);
   		vector <StringElementType> vec1;
  		vector <StringElementType> vec2;

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = string("blahblahblahblahblah");
			vec1.push_back(e);
		        if (vec1.size() >= ZDL_VEC_SIZE)
			{
				inDL->insert(vec1);
				vec1.clear();
			}
			if (0 == i % 2)
			{
				e.second = string("blahblahblah");
				vec2.push_back(e);
				if (vec2.size() >= ZDL_VEC_SIZE)
				{
					dDL->insert(vec2);
					vec2.clear();
				}
			}
// 			inDL->insert(e);
		}
		if (!vec1.empty())
			inDL->insert(vec1);
		inDL->endOfInput();

/*		for (i = 0; i < ::count; i+=2) {
			e.first = i;
			e.second = string("blahblahblah");
			dDL->insert(e);
		}*/
		if (!vec2.empty())
			dDL->insert(vec2);

		dDL->endOfInput();

		ReduceStep rs(in, out, 5, 1, 0, 0, 0, 0);
		rs.run();
		rs.join();

		it = outDL->getIterator();
		more = outDL->next(it, &e);
		i = 0;
		while (more) {
//    			cout << i << ": first: " << e.first << " second: " << e.second << endl;
			CPPUNIT_ASSERT(e.first < ::count);
			CPPUNIT_ASSERT(e.first % 2 == 0); 
			more = outDL->next(it, &e);
			i++;
		}
	}

	void reduceStep_3()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		WSDL<DoubleElementType> *inDL, *dDL, *outDL;
		DoubleElementType e;
		unsigned i;	
		int it;
		bool more;

		inDL = new WSDL<DoubleElementType>(1, 100000, fRm);
		dDL = new WSDL<DoubleElementType>(1, 100000, fRm);
		outDL = new WSDL<DoubleElementType>(1, 100000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->doubleDL(inDL);
		driverADL->doubleDL(dDL);
		outputADL->doubleDL(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = ((double) i) + 0.1;
			inDL->insert(e);
		}
		inDL->endOfInput();

		for (i = 0; i < ::count; i+=2) {
			e.first = i;
			e.second = ((double) i) + 0.1;
			dDL->insert(e);
		}
		dDL->endOfInput();

		ReduceStep rs(in, out, 5, 1, 0, 0, 0, 0);
		rs.run();
		rs.join();

		it = outDL->getIterator();
		more = outDL->next(it, &e);
		i = 0;
		while (more) {
//    			cout << i << ": first: " << e.first << " second: " << e.second << endl;
			CPPUNIT_ASSERT(e.first < ::count);
			CPPUNIT_ASSERT(e.first % 2 == 0); 
			more = outDL->next(it, &e);
			i++;
		}
	}

void reduceStep_4()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		BucketDL<ElementType> *inDL, *dDL;
		WSDL<ElementType> *outDL;
		ElementType e;
		unsigned i;
		int it;
		bool more;

		inDL = new BucketDL<ElementType>(10, 1, 100000, fRm);
		dDL = new BucketDL<ElementType>(10, 1, 100000, fRm);
		outDL = new WSDL<ElementType>(1, 100000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->bucketDL(inDL);
		driverADL->bucketDL(dDL);
		outputADL->workingSetDL(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);

// 		cout << "making input DataList" << endl;

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = i+1;
			inDL->insert(e);
		}
		inDL->endOfInput();

// 		cout << "making driver DataList" << endl;

		for (i = 0; i < ::count; i+=2) {
			e.first = i;
			e.second = i+1;
			dDL->insert(e);
		}
		dDL->endOfInput();

// 		cout << "reducing" << endl;

		ReduceStep rs(in, out, 5, 1, 0, 0, 0, 0);
		rs.run();
		rs.join();

		it = outDL->getIterator();
		more = outDL->next(it, &e);
		i = 0;
		while (more) {
//  			cout << i << ": first: " << e.first << " second: " << e.second << endl;
			CPPUNIT_ASSERT(e.first < ::count);
			CPPUNIT_ASSERT(e.first % 2 == 0); 
			more = outDL->next(it, &e);
			i++;
		}
	}

	void pnljoin_1()
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		int it;
		bool more;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;
		
		AnyDataListSPtr spdl1(new AnyDataList());
		WSDL<ElementType>* dl1 = new WSDL<ElementType>(1, 100, fRm);
		spdl1->workingSetDL(dl1);
		inJs.outAdd(spdl1);

		AnyDataListSPtr spdl2(new AnyDataList());
		WSDL<ElementType>* dl2 = new WSDL<ElementType>(1, 100, fRm);
		spdl2->workingSetDL(dl2);
		outJs.outAdd(spdl2);

		/* These values are unique to Pat's DB files unfortunately. */
		/* Fill in the value list */
		dl1->insert(ElementType(1, 3179));  // row 10 in the target
		dl1->insert(ElementType(2, 3191));	// row 12
		dl1->insert(ElementType(3, 3207));	// row 14
		dl1->insert(ElementType(4, 3318));	// row 20  OOO
		dl1->insert(ElementType(5, 3242));	// row 16
		dl1->insert(ElementType(6, 3289));	// row 18
		dl1->insert(ElementType(7, 3191));  // duplicate value of row 14

		dl1->endOfInput();
		dl1->OID(1003);

		PNLJoin joiner(inJs, outJs, dec, cat, 12349, 1000, 1, 0, fRm);

		joiner.run();
		joiner.join();

		it = dl2->getIterator();
		more = dl2->next(it, &e);
		while (more) {
#ifdef DEBUG
			cout << "first: " << e.first << " second: " << e.second << endl;
#endif
			more = dl2->next(it, &e);
		}
	}
	void pnljoin_2()
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		int i, it;
		bool more;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;
		
		AnyDataListSPtr spdl1(new AnyDataList());
		WSDL<ElementType>* valueList = new WSDL<ElementType>(1, 100, fRm);
		spdl1->workingSetDL(valueList);
		inJs.outAdd(spdl1);

		AnyDataListSPtr spdl2(new AnyDataList());
		AnyDataListSPtr spdl3(new AnyDataList());
		WSDL<ElementType>* colResults = new WSDL<ElementType>(1, 100, fRm);
		WSDL<ElementType>* inputRidList = new WSDL<ElementType>(1, 100, fRm);
		spdl2->workingSetDL(colResults);
		spdl3->workingSetDL(inputRidList);
		outJs.outAdd(spdl2);
		inJs.outAdd(spdl3);


		/* These values are unique to Pat's DB files unfortunately. */
		/* Fill in the value list */
		valueList->insert(ElementType(1, 3179));  // row 10 in the target
		valueList->insert(ElementType(2, 3191));	// row 12
		valueList->insert(ElementType(3, 3207));	// row 14
		valueList->insert(ElementType(4, 3318));	// row 20  OOO
		valueList->insert(ElementType(5, 3242));	// row 16
		valueList->insert(ElementType(6, 3289));	// row 18
		valueList->insert(ElementType(7, 3191));  // duplicate value of row 14

		valueList->endOfInput();
		valueList->OID(1003);
	
		// supply a ridlist with row 16 missing; make sure it's missing in the result
		for (i = 0; i < 25; i++)
			if (i != 16)
				inputRidList->insert(ElementType(i, i));
		inputRidList->endOfInput();		

		PNLJoin joiner(inJs, outJs, dec, cat, 12349, 1000, 1, 0, fRm);

		joiner.run();
		joiner.join();

		it = colResults->getIterator();
		more = colResults->next(it, &e);
		while (more) {
#ifdef DEBUG
			cout << "first: " << e.first << " second: " << e.second << endl;
#endif
			more = colResults->next(it, &e);
		}
	}

	void pnljoin_3()
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		int i, it;
		bool more;

		dec = DistributedEngineComm::instance(fRm);
	// 	dec = DistributedEngineComm::instance("./config-dec.xml");
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		AnyDataListSPtr spdl1(new AnyDataList());
		AnyDataListSPtr spdl3(new AnyDataList());
		WSDL<ElementType>* valueList = new WSDL<ElementType>(1, 100, fRm);
		WSDL<ElementType>* inputRidList = new WSDL<ElementType>(1, 100, fRm);
		spdl1->workingSetDL(valueList);
		spdl3->workingSetDL(inputRidList);

		JobStepAssociation inJs;
		JobStepAssociation outJs;

		inJs.outAdd(spdl1);
		inJs.outAdd(spdl3);

		AnyDataListSPtr spdl2(new AnyDataList());
		AnyDataListSPtr spdl4(new AnyDataList());
		WSDL<ElementType>* colResults = new WSDL<ElementType>(1, 100, fRm);
		WSDL<ElementType>* reducedRidList = new WSDL<ElementType>(1, 100, fRm);
		spdl2->workingSetDL(colResults);
		spdl4->workingSetDL(reducedRidList);
		outJs.outAdd(spdl2);
		outJs.outAdd(spdl4);

		/* These values are unique to Pat's DB files unfortunately. */
		/* Fill in the value list */
		valueList->insert(ElementType(10, 3179));  // row 10 in the target
		valueList->insert(ElementType(12, 3191));	// row 12
		valueList->insert(ElementType(14541513, 3207));	// row 14 - on output should be in colResults, not in reducedridlist
		valueList->insert(ElementType(20, 3318));	// row 20  OOO
		valueList->insert(ElementType(16, 3242));	// row 16
		valueList->insert(ElementType(18, 3289));	// row 18

		// XXXPAT: Duplicates here can end up in the reducedRidList.  Technically it's
		// correct, but do we want that or not?
// 		valueList->insert(ElementType(12, 3191));  // duplicate value of row 12

		valueList->endOfInput();
		valueList->OID(1003);
	
		// supply a ridlist with row 16 missing; make sure it's missing in the result
		for (i = 0; i < 25; i++)
			if (i != 16)
				inputRidList->insert(ElementType(i, i));
		inputRidList->endOfInput();		

		PNLJoin joiner(inJs, outJs, dec, cat, 12349, 1000, 1, 0, fRm);

		joiner.run();
		joiner.join();

		it = colResults->getIterator();
		more = colResults->next(it, &e);
#ifdef DEBUG
		cout << "ColResults:" << endl;
#endif
		while (more) {
#ifdef DEBUG
			cout << "   first: " << e.first << " second: " << e.second << endl;
#endif
			more = colResults->next(it, &e);
		}

		it = reducedRidList->getIterator();
		more = reducedRidList->next(it, &e);
#ifdef DEBUG
		cout << "Reduced Rid List:" << endl;
#endif
		while (more) {
#ifdef DEBUG
			cout << "   first: " << e.first << " second: " << e.second << endl;
#endif
			more = reducedRidList->next(it, &e);
		}

	}

	void unionStep_1()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		WSDL<ElementType> *outDL;
		set<ElementType> s;
		set<ElementType>::iterator sIt;
		ElementType e;
		unsigned i;
		int it;
		bool more;

		ZDL<ElementType>* inDL = new ZDL<ElementType>(1, fRm);
		ZDL<ElementType>* dDL = new ZDL<ElementType>(1, fRm);

		outDL = new WSDL<ElementType>(1, 100000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->zonedDL(inDL);
		driverADL->zonedDL(dDL);

		outputADL->workingSetDL(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);

//  		cout << "making input DataList" << endl;
   		vector <ElementType> vec1;
  		vector <ElementType> vec2;

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = i+1;
			vec1.push_back(e);
		        if (vec1.size() >= ZDL_VEC_SIZE)
			{
				inDL->insert(vec1);
				vec1.clear();
			}
// 			inDL->insert(e);
		}
		if (!vec1.empty())
			inDL->insert(vec1);
		inDL->endOfInput();

// 		cout << "making driver DataList" << endl;

		for (i = 0; i < ::count; i+=2) {
			e.first = i;
			e.second = i+1;
			vec2.push_back(e);
		        if (vec2.size() >= ZDL_VEC_SIZE)
			{
				dDL->insert(vec2);
				vec2.clear();
			}

// 			dDL->insert(e);
		}
		if (!vec2.empty())
			dDL->insert(vec2);
		dDL->endOfInput();

//  		cout << "unionizing" << endl;

		UnionStep rs(in, out, 50, 5, 1, 0, 0, 0);
		rs.run();
		rs.join();

		CPPUNIT_ASSERT(outDL->totalSize() == inDL->totalSize());

		it = outDL->getIterator();
		for (i = 0, more = outDL->next(it, &e) ; more; more = outDL->next(it, &e), i++)
			s.insert(e);
		CPPUNIT_ASSERT(outDL->totalSize() == i);
		CPPUNIT_ASSERT(i == ::count);		

		CPPUNIT_ASSERT(s.size() == outDL->totalSize());  // verifies no duplicates in outDL
		for (i = 0, sIt = s.begin(); sIt != s.end(); sIt++, i++)
			CPPUNIT_ASSERT(sIt->first == i);
		CPPUNIT_ASSERT(i == ::count);	// verifies they all exist.
	}	

	void unionStep_2()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		WSDL<ElementType> *inDL, *dDL, *outDL;
		set<ElementType> s;
		set<ElementType>::iterator sIt;
		ElementType e;
		unsigned i;
		int it;
		bool more;

		inDL = new WSDL<ElementType>(1, 100000, fRm);
		dDL = new WSDL<ElementType>(1, 100000, fRm);
		outDL = new WSDL<ElementType>(1, 100000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->workingSetDL(inDL);
		driverADL->workingSetDL(dDL);
		outputADL->workingSetDL(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);

//  		cout << "making input DataList" << endl;

		for (i = 0; i < ::count; i+=2) {
			e.first = i;
			e.second = i+1;
			inDL->insert(e);
		}
		inDL->endOfInput();

//  		cout << "making driver DataList" << endl;

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = i+1;
			dDL->insert(e);
		}
		dDL->endOfInput();

//  		cout << "unionizing" << endl;

		UnionStep rs(in, out, 50, 5, 1, 0, 0, 0);
		rs.run();
		rs.join();

		CPPUNIT_ASSERT(outDL->totalSize() == dDL->totalSize());

		it = outDL->getIterator();
		for (i = 0, more = outDL->next(it, &e) ; more; more = outDL->next(it, &e), i++)
			s.insert(e);
		CPPUNIT_ASSERT(outDL->totalSize() == i);
		CPPUNIT_ASSERT(i == ::count);		

		CPPUNIT_ASSERT(s.size() == outDL->totalSize());  // verifies no duplicates in outDL
		for (i = 0, sIt = s.begin(); sIt != s.end(); sIt++, i++)
			CPPUNIT_ASSERT(sIt->first == i);
		CPPUNIT_ASSERT(i == ::count);	// verifies they all exist.
	}	

	void unionStep_3()
	{
		JobStepAssociation in, out;
		AnyDataList *inputADL, *driverADL, *outputADL;
		AnyDataListSPtr inputSPtr, driverSPtr, outputSPtr;
		WSDL<ElementType> *outDL;
		BucketDL<ElementType> *inDL, *dDL;
		set<ElementType> s;
		set<ElementType>::iterator sIt;
		ElementType e;
		unsigned i;
		int it;
		bool more;

		inDL = new BucketDL<ElementType>(10, 1, 100000, fRm);
		dDL = new BucketDL<ElementType>(10, 1, 100000, fRm);
		outDL = new WSDL<ElementType>(1, 100000, fRm);
		inputADL = new AnyDataList();
		driverADL = new AnyDataList();
		outputADL = new AnyDataList();
		inputADL->bucketDL(inDL);
		driverADL->bucketDL(dDL);
		outputADL->workingSetDL(outDL);
		inputSPtr.reset(inputADL);
		driverSPtr.reset(driverADL);
		outputSPtr.reset(outputADL);

		in.outAdd(inputSPtr);
		in.outAdd(driverSPtr);
		out.outAdd(outputSPtr);

//  		cout << "making input DataList" << endl;

		for (i = 0; i < ::count; i++) {
			e.first = i;
			e.second = i+1;
			inDL->insert(e);
		}
		inDL->endOfInput();

//  		cout << "making driver DataList" << endl;

		for (; i < ::count * 2; i++) {
			e.first = i;
			e.second = i+1;
			dDL->insert(e);
		}
		dDL->endOfInput();

//  		cout << "unionizing" << endl;

		UnionStep rs(in, out, 50, 5, 1, 0, 0, 0);
		rs.run();
		rs.join();

		CPPUNIT_ASSERT(outDL->totalSize() == 2 * ::count);

		it = outDL->getIterator();
		for (i = 0, more = outDL->next(it, &e) ; more; more = outDL->next(it, &e), i++)
			s.insert(e);
		CPPUNIT_ASSERT(outDL->totalSize() == i);
		CPPUNIT_ASSERT(i == 2 * ::count);		

		CPPUNIT_ASSERT(s.size() == outDL->totalSize());  // verifies no duplicates in outDL
		for (i = 0, sIt = s.begin(); sIt != s.end(); sIt++, i++)
			CPPUNIT_ASSERT(sIt->first == i);
		CPPUNIT_ASSERT(i == 2 * ::count);	// verifies they all exist.
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
