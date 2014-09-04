/* $Id: tdriver.cpp 2 2006-08-30 13:29:53Z rdempsey $ */

#include <cstring>
#include <cppunit/extensions/HelperMacros.h>

class ResourceMonitorTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( ResourceMonitorTest );

CPPUNIT_TEST( test1 );

CPPUNIT_TEST_SUITE_END();

private:

public:
	void setUp() {
	}

	void tearDown() {
	}

	void test1() {
		//system ("./resourceMonitor 0");

	}

}; 

CPPUNIT_TEST_SUITE_REGISTRATION( ResourceMonitorTest );

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

