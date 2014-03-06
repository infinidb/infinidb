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

/***************************************************************************
 *   wweeks@calpont.com   *
 *                                                                         *
 ***************************************************************************/

using namespace std;
#include <iostream>
#include <cstdlib>
#include <sys/types.h>
#include <unistd.h>
#include "sessionmanager.h"

#include <cppunit/extensions/HelperMacros.h>

using namespace execplan;

int maxNewTxns=1000;
int maxTxns = 1000;

class ExecPlanTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ExecPlanTest );

CPPUNIT_TEST_SUITE_END();

private:
public:

    void setUp() {
    }
    
    void tearDown() {
    }
}; // test suite

CPPUNIT_TEST_SUITE_REGISTRATION( ExecPlanTest);

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main(int argc, char *argv[])
{

  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}
