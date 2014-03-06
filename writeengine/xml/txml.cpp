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

#include <stdio.h>
#include <string>
#include <stdexcept>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "we_xmltag.h"
#include "we_xmljob.h"

using namespace WriteEngine;

class XmlTest : public CppUnit::TestFixture {


CPPUNIT_TEST_SUITE( XmlTest );


CPPUNIT_TEST( test1 );

// XML basic testing
CPPUNIT_TEST( testBasicXMLRead );
/*CPPUNIT_TEST( testAddTreeNode );

// Index tree testing
CPPUNIT_TEST( testTreeGetTestbitValue );
*/
CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

   void test1() {
   }
   void testBasicXMLRead() {
      int      rc;
      XMLJob   myJob;

      rc = myJob.loadJobXmlFile( "../test/bulk/job/Job_127.xml" );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      
      myJob.printJobInfo();
   }

};

CPPUNIT_TEST_SUITE_REGISTRATION( XmlTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


