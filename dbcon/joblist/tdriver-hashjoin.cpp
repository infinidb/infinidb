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

// $Id: tdriver-hashjoin.cpp 9210 2013-01-21 14:10:42Z rdempsey $
//
// C++ Implementation: testhasjoin
//
// Description: Test driver for HashJoin class. To be used for construction only.
//
//
// Author: Jason Rodriguez <jrodriguez@calpont.com>
//
// Copyright: See COPYING file that comes with this distribution
//
//
//	Calpont (C) 2007
//

#include <iostream>

#include <cppunit/extensions/HelperMacros.h>
#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

#include "elementtype.h"
#include "wsdl.h"
#include "bucketdl.h"
#include "bdlwrapper.h"
#include "largehashjoin.h"
#include <limits.h>
#include <sys/types.h>
#include <unistd.h>

using namespace std;
using namespace joblist;
using namespace execplan;


class HashJoinTestDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(HashJoinTestDriver);

CPPUNIT_TEST(HashJoin_1);
CPPUNIT_TEST(HashJoin_2);
CPPUNIT_TEST(HashJoin_3);
CPPUNIT_TEST(HashJoin_4);
CPPUNIT_TEST(HashJoin_5);

CPPUNIT_TEST(LeftOuterJoin_1);
CPPUNIT_TEST(LeftOuterJoin_2);
CPPUNIT_TEST(LeftOuterJoin_3);
CPPUNIT_TEST(LeftOuterJoin_4);
CPPUNIT_TEST(RightOuterJoin_1);
CPPUNIT_TEST(RightOuterJoin_2);

CPPUNIT_TEST_SUITE_END();

private:

	uint32_t elementCount( BucketDL < ElementType>* dl ) const
	{
		int sz = 0;

		if (dl==NULL)
			return 0;

		for (uint32_t i=0;i<dl->bucketCount();i++)
			sz+=dl->size(i);

		return sz;

	}
  JSTimeStamp  fTs;
  ResourceManager fRm;

public:

	void HashJoin_1()
	{
		uint64_t maxElems=32655;
		int maxBuckets=8;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);

		A.insert(ElementType(1025,1));
		A.insert(ElementType(1026,2));
		A.insert(ElementType(1027,3));
		A.insert(ElementType(1028,4));

		B.insert(ElementType(1034,4));
		B.insert(ElementType(1035,4));
		B.insert(ElementType(1036,2));
		B.insert(ElementType(1037,4));
		B.insert(ElementType(1041,1));
		B.insert(ElementType(1042,2));
		B.insert(ElementType(1043,3));
		B.insert(ElementType(1044,4));
		B.insert(ElementType(1045,2));
		B.insert(ElementType(1046,3));
		B.insert(ElementType(1047,3));
		B.insert(ElementType(1048,1));
		B.insert(ElementType(1049,5));

		A.endOfInput();
		B.endOfInput();

		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, INNER, &fTs);
	
		hj->performJoin();

		int csize = elementCount(&C);
		int dsize = elementCount(&D);

		//cout << "A " << setA.size()
	//		<< " B " << setB.size()
	//		<< " C " << csize
	//		<< " D " << dsize
	//		<< endl;

		CPPUNIT_ASSERT(csize==4);
		CPPUNIT_ASSERT(dsize==12);

	} // HashJoin_1

	void HashJoin_2()
	{
		uint64_t maxElems=100;
		int maxBuckets=8;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		// create A
        for (uint64_t idx=0; idx < maxElems ; idx++)
        {
            A.insert(ElementType(idx, idx));
            B.insert(ElementType(idx+maxElems, idx));
        }


		A.endOfInput();
		B.endOfInput();

		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, INNER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);

		//cout << "T2 sz " << csize << " " << dsize << endl;
		CPPUNIT_ASSERT(csize==maxElems);
		CPPUNIT_ASSERT(dsize==maxElems);
		CPPUNIT_ASSERT(dsize==csize);

	} // HashJoin_2

	void HashJoin_3()
	{
		uint64_t maxElems=100;
		int maxBuckets=8;
		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		// create A
        for (uint64_t idx=0; idx < maxElems ; idx++)
        {
            A.insert(ElementType(idx, idx));
            B.insert(ElementType(idx+maxElems, (-1)*idx));
        }

		A.endOfInput();
		B.endOfInput();

		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, INNER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);

		// both sets containt zero
		//cout << "T3 sz " << csize << " " << dsize << endl;

		CPPUNIT_ASSERT(csize==1);
		CPPUNIT_ASSERT(dsize==1);
		CPPUNIT_ASSERT(dsize==csize);
	} // HashJoin_3

	void HashJoin_4()
	{
		uint64_t maxElems=32655;
		int maxBuckets=128;
		int setAMin=0;
		int setAMax=10000;
		int setARange = (setAMax-setAMin)+1;
		int setBMin=0;
		int setBMax=10000;
		int setBRange = (setBMax-setBMin)+1;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		setARange=0;
		setBRange=0;
		// create A
  		srand(time(0)*getpid());
		long stime = clock();
        for (uint64_t idx=0; idx < maxElems ; idx++)
        {
            uint64_t aVal = (rand()%setAMax)+1;
            uint64_t bVal = (rand()%setAMax)+1;
            A.insert(ElementType(idx, aVal) );
            B.insert(ElementType(idx+maxElems, bVal) );
        }

		A.endOfInput();
		B.endOfInput();
		long etime = clock();

		//cout << "Build time " << (float)(etime - stime)/(float)CLOCKS_PER_SEC << endl;
		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, INNER, &fTs);
	
		stime = clock();
		hj->performJoin();
		etime = clock();
		//cout << "Join time " << (float)(etime - stime)/(float)CLOCKS_PER_SEC << endl;

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);

		//cout << "T4 sz " << csize << " " << dsize << endl;

		// TODO: determine what values to test
		CPPUNIT_ASSERT(csize>=1);
		CPPUNIT_ASSERT(dsize>=1);

	} // HashJoin_4

	void HashJoin_5()
	{
		uint64_t maxElems=32655;
		int maxBuckets=128;
		const uint64_t modValue=10;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		// create A
        for (uint64_t idx=1; idx < maxElems ; idx++)
        {
            uint64_t aVal = idx;
            uint64_t bVal = idx;

			if (bVal%modValue!=0)
				bVal*=(-1);	
            A.insert(ElementType(idx, aVal) );
            B.insert(ElementType(idx+maxElems, bVal) );
        }

		A.endOfInput();
		B.endOfInput();
		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, INNER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);

		//cout << "T5 sz " << csize << " " << dsize << endl;

		// TODO: determine what values to test
		CPPUNIT_ASSERT(csize==(uint64_t)(maxElems/modValue));
		CPPUNIT_ASSERT(dsize==(uint64_t)(maxElems/modValue));

	} // HashJoin_5

	void LeftOuterJoin_1()
	{

		// Outer left join such as A (+) = B.
		// All of Bs should be returned with matching As.
		uint64_t maxElems=32655;
		int maxBuckets=8;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);

		A.insert(ElementType(1025,1));
		A.insert(ElementType(1026,2));
		A.insert(ElementType(1027,3));
		A.insert(ElementType(1028,4));
		A.insert(ElementType(1029,99));

		B.insert(ElementType(1034,4));
		B.insert(ElementType(1035,4));
		B.insert(ElementType(1036,2));
		B.insert(ElementType(1037,4));
		B.insert(ElementType(1041,1));
		B.insert(ElementType(1042,2));
		B.insert(ElementType(1043,3));
		B.insert(ElementType(1044,4));
		B.insert(ElementType(1045,2));
		B.insert(ElementType(1046,3));
		B.insert(ElementType(1047,3));
		B.insert(ElementType(1048,1));
		B.insert(ElementType(1049,5));

		A.endOfInput();
		B.endOfInput();

		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, LEFTOUTER, &fTs);
	
		hj->performJoin();

		int csize = elementCount(&C);
		int dsize = elementCount(&D);

		//cout << "A " << setA.size()
	//		<< " B " << setB.size()
	//		<< " C " << csize
	//		<< " D " << dsize
	//		<< endl;

		CPPUNIT_ASSERT(csize==4);
		CPPUNIT_ASSERT(dsize==13);

	} // HashJoin_1


	// Inserts 1,000,000 values in A.
	// Inserts 666,668 values in B half of which match A.
	// Peforms left outer join A (+) = B.
	// Asserts that we returned 333,334 A values and all 666,668 B values.
	void LeftOuterJoin_2()
	{
		uint64_t maxElems=1000 * 1000;
		int maxBuckets=128;
		int setAMin=0;
		int setAMax=10000;
		int setARange = (setAMax-setAMin)+1;
		int setBMin=0;
		int setBMax=10000;
		int setBRange = (setBMax-setBMin)+1;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		setARange=0;
		setBRange=0;

		ElementType el;
		ElementType el2;
		for (uint64_t idx=0; idx < maxElems ; idx++)
		{
			el.first = idx;
			el.second = idx;
			el2.first = idx + maxElems;
			el2.second = idx;
			A.insert(el);
			if(idx%3 == 0) {
				B.insert(el2);
				el2.second = el2.first;
				B.insert(el2);
			}
		}

		A.endOfInput();
		B.endOfInput();

		//cout << "Build time " << (float)(etime - stime)/(float)CLOCKS_PER_SEC << endl;
		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, LEFTOUTER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);
// cout << "csize=" << csize << "; dsize=" << dsize << endl;
		CPPUNIT_ASSERT(csize == 333334);
		CPPUNIT_ASSERT(dsize == 666668);

	} 

	// Inserts 0 values in A.
	// Inserts 666,668 values in B half of which match A.
	// Peforms left outer join A (+) = B.
	// Asserts that we returned 0 A values and all 666,668 B values.
	void LeftOuterJoin_3()
	{
		uint64_t maxElems=1000 * 1000;
		int maxBuckets=128;
		int setAMin=0;
		int setAMax=10000;
		int setARange = (setAMax-setAMin)+1;
		int setBMin=0;
		int setBMax=10000;
		int setBRange = (setBMax-setBMin)+1;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		setARange=0;
		setBRange=0;

		ElementType el;
		ElementType el2;
		for (uint64_t idx=0; idx < maxElems ; idx++)
		{
			el.first = idx;
			el.second = idx;
			el2.first = idx + maxElems;
			el2.second = idx;
			// A.insert(el);
			if(idx%3 == 0) {
				B.insert(el2);
				el2.second = el2.first;
				B.insert(el2);
			}
		}

		A.endOfInput();
		B.endOfInput();

		//cout << "Build time " << (float)(etime - stime)/(float)CLOCKS_PER_SEC << endl;
		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, LEFTOUTER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);
// cout << "csize=" << csize << "; dsize=" << dsize << endl;
		CPPUNIT_ASSERT(csize == 0);
		CPPUNIT_ASSERT(dsize == 666668);

	} 

	// Inserts 1,000,000 values in A.
	// Inserts 0 values in B half of which match A.
	// Peforms left outer join A (+) = B.
	// Asserts that we returned 0 A values and all 0 B values.
	void LeftOuterJoin_4()
	{
		uint64_t maxElems=1000 * 1000;
		int maxBuckets=128;
		int setAMin=0;
		int setAMax=10000;
		int setARange = (setAMax-setAMin)+1;
		int setBMin=0;
		int setBMax=10000;
		int setBRange = (setBMax-setBMin)+1;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		setARange=0;
		setBRange=0;

		ElementType el;
		ElementType el2;
		for (uint64_t idx=0; idx < maxElems ; idx++)
		{
			el.first = idx;
			el.second = idx;
			el2.first = idx + maxElems;
			el2.second = idx;
			A.insert(el);
			/*
			if(idx%3 == 0) {
				B.insert(el2);
				el2.second = el2.first;
				B.insert(el2);
			}
			*/
		}

		A.endOfInput();
		B.endOfInput();

		//cout << "Build time " << (float)(etime - stime)/(float)CLOCKS_PER_SEC << endl;
		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, LEFTOUTER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);
// cout << "csize=" << csize << "; dsize=" << dsize << endl;
		CPPUNIT_ASSERT(csize == 0);
		CPPUNIT_ASSERT(dsize == 0);

	} 

	void RightOuterJoin_1()
	{

		// Outer left join such as A (+) = B.
		// All of Bs should be returned with matching As.
		uint64_t maxElems=32655;
		int maxBuckets=8;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);

		A.insert(ElementType(1025,1));
		A.insert(ElementType(1026,2));
		A.insert(ElementType(1027,3));
		A.insert(ElementType(1028,4));
		A.insert(ElementType(1029,99));

		B.insert(ElementType(1034,4));
		B.insert(ElementType(1035,4));
		B.insert(ElementType(1036,2));
		B.insert(ElementType(1037,4));
		B.insert(ElementType(1041,1));
		B.insert(ElementType(1042,2));
		B.insert(ElementType(1043,3));
		B.insert(ElementType(1044,4));
		B.insert(ElementType(1045,2));
		B.insert(ElementType(1046,3));
		B.insert(ElementType(1047,3));
		B.insert(ElementType(1048,1));
		B.insert(ElementType(1049,5));

		A.endOfInput();
		B.endOfInput();

		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, RIGHTOUTER, &fTs);
	
		hj->performJoin();

		int csize = elementCount(&C);
		int dsize = elementCount(&D);

		//cout << "A " << setA.size()
	//		<< " B " << setB.size()
	//		<< " C " << csize
	//		<< " D " << dsize
	//		<< endl;

		CPPUNIT_ASSERT(csize==5);
		CPPUNIT_ASSERT(dsize==12);

	} 

	// Inserts 1,000,000 values in A.
	// Inserts 666,668 values in B half of which match A.
	// Peforms right outer join A = B.(+)
	// Asserts that we returned 1,000,000 A values and all 333,334 B values.
	void RightOuterJoin_2()
	{
		uint64_t maxElems=1000 * 1000;
		int maxBuckets=128;
		int setAMin=0;
		int setAMax=10000;
		int setARange = (setAMax-setAMin)+1;
		int setBMin=0;
		int setBMax=10000;
		int setBRange = (setBMax-setBMin)+1;

		BucketDL< ElementType > A(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > B(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > C(maxBuckets, 1, maxElems/maxBuckets, fRm);
		BucketDL< ElementType > D(maxBuckets, 1, maxElems/maxBuckets, fRm);
		A.setHashMode(1);
		B.setHashMode(1);
		setARange=0;
		setBRange=0;

		ElementType el;
		ElementType el2;
		for (uint64_t idx=0; idx < maxElems ; idx++)
		{
			el.first = idx;
			el.second = idx;
			el2.first = idx + maxElems;
			el2.second = idx;
			A.insert(el);
			if(idx%3 == 0) {
				B.insert(el2);
				el2.second = el2.first;
				B.insert(el2);
			}
		}

		A.endOfInput();
		B.endOfInput();

		//cout << "Build time " << (float)(etime - stime)/(float)CLOCKS_PER_SEC << endl;
		BDLWrapper< ElementType > setA(&A);
		BDLWrapper< ElementType > setB(&B);
		DataList< ElementType >* resultA(&C);
		DataList< ElementType >* resultB(&D);
	
		HashJoin<ElementType>* hj = new HashJoin<ElementType>(setA, setB, resultA, resultB, RIGHTOUTER, &fTs);
	
		hj->performJoin();

		uint64_t csize = elementCount(&C);
		uint64_t dsize = elementCount(&D);
// cout << "csize=" << csize << "; dsize=" << dsize << endl;
		CPPUNIT_ASSERT(csize == 1000000);
		CPPUNIT_ASSERT(dsize == 333334);

	} 


}; //

CPPUNIT_TEST_SUITE_REGISTRATION(HashJoinTestDriver);

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();

  runner.addTest( registry.makeTest() );

  bool wasSuccessful = runner.run( "", false );

  return (wasSuccessful ? 0 : 1);
}
