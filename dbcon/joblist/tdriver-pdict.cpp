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
 * $Id: tdriver-pdict.cpp 9210 2013-01-21 14:10:42Z rdempsey $
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
#include "we_type.h"
#include "dbrm.h"

using namespace std;
using namespace joblist;
using namespace execplan;

class JobStepDriver : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE(JobStepDriver);

CPPUNIT_TEST(pDictStep_1);

CPPUNIT_TEST_SUITE_END();

private:
public:

	void pDictStep_1()
	{
		DistributedEngineComm* dec;
		boost::shared_ptr<CalpontSystemCatalog> cat;
		ElementType e;
		uint32_t i, it;
		bool more;
		BRM::DBRM dbrm;
		const uint32_t dictOID=2064;
		ResourceManager rm;
		dec = DistributedEngineComm::instance(rm);
		cat = CalpontSystemCatalog::makeCalpontSystemCatalog();

		JobStepAssociation inJs;
		JobStepAssociation outJs;
	
		// create request list	
		AnyDataListSPtr spdl1(new AnyDataList());
		FifoDataList* dl1 = new FifoDataList(1, 128);
		spdl1->fifoDL(dl1);
		inJs.outAdd(spdl1);

		AnyDataListSPtr spdl2(new AnyDataList());
		StringFifoDataList* dl2 = new StringFifoDataList(1, 128);
		spdl2->stringDL(dl2);
		outJs.outAdd(spdl2);

		int64_t lbid;
		int err = dbrm.lookup(dictOID, 0, lbid);
		CPPUNIT_ASSERT(err==0);
		
		// populate the element pair
		UintRowGroup rw;

		const uint32_t tcount=10;
		for (i = 1; i <= tcount; i++) {
			WriteEngine::Token token; // rid
			token.op=i; // index of sig in block
			token.fbo=lbid; // lbid of sig block to search
			token.spare = 0;
			// cast for ElementType second value
			uint64_t *u = reinterpret_cast<uint64_t*>(&token);
			CPPUNIT_ASSERT(u);
			rw.et[rw.count].first = i;
			rw.et[rw.count++].second = *u;
			if (rw.count == rw.ElementsPerGroup)
			{
				dl1->insert(rw);
			}

		}
		if (rw.count > 0)
			dl1->insert(rw);
		// close input set
		dl1->endOfInput();

		pDictionaryStep p(inJs, outJs, dec, cat, dictOID, 1000, 12346, 32000, 32000, 1, 0, rm);
		p.dec(dec);
		p.run();
		p.join();
// 		StringElementType s;
		it = dl2->getIterator();
		// dump the result set
		StringRowGroup s;

				
		for (more = dl2->next(it, &s), i = 0; more; more = dl2->next(it, &s), i++) 
		{

		for (uint64_t i = 0; i < s.count; ++i )
			cout << i << " <rid = " << s.et[i].first << ", value = " << s.et[i].second << ">" << endl;
		}

		CPPUNIT_ASSERT(s.count == tcount);

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
