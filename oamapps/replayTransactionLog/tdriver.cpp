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
