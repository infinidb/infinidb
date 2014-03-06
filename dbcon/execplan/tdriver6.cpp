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

#include <string>
#include <stdexcept>
#include <typeinfo>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include<sstream>
#include<exception>
#include<iostream>
#include <unistd.h>

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "calpontselectexecutionplan.h"
#include "simplefilter.h"
#include "simplecolumn.h"
#include "expressionparser.h"
#include "constantcolumn.h"
#include "treenode.h"
#include "operator.h"
#include "arithmeticcolumn.h"
#include "aggregatecolumn.h"
#include "existsfilter.h"

using namespace execplan;

class TPCH_EXECPLAN : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( TPCH_EXECPLAN );

CPPUNIT_TEST( Q1 );

CPPUNIT_TEST_SUITE_END();

private:
public:
   
    void setUp() {
    }
    
    void tearDown() {
    }
    
    void Q1() {
        string sql = "\
        select\
        	sum(l_extendedprice * l_discount) as revenue\
        from\
        	lineitem\
        where\
	        l_shipdate >= date ':1'\
	        and l_shipdate < date ':1' + interval '1' year\
	        and l_discount between :2 - 0.01 and :2 + 0.01\
	        and l_quantity < :3;";
        	        

        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        
        ArithmeticColumn *c1 = new ArithmeticColumn("sum(tpch.lineitem.l_extendedprice * tpch.lineitem.l_discount)");
        c1->alias("revenue");
        returnedColumnList.push_back(c1);                
        
        csep.returnedCols(returnedColumnList);
        
        // Filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        SimpleFilter *f1 = new SimpleFilter (new Operator(">="),
                                             new SimpleColumn("tpch.lineitem.l_shipdate"),
                                             new ArithmeticColumn("date (':1')"));
        filterTokenList.push_back(f1);
        filterTokenList.push_back( new Operator ("and"));
        
        SimpleFilter *f2 = new SimpleFilter (new Operator("<"),
                                             new SimpleColumn("tpch.lineitem.l_shipdate"),
                                             new ArithmeticColumn("date (':1') + interval('1', year)"));
        filterTokenList.push_back(f2);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f3 = new SimpleFilter (new Operator(">="),
                                             new SimpleColumn("tpch.lineitem.l_discount"),
                                             new ArithmeticColumn("':2' - 0.01"));
        filterTokenList.push_back(f3);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f4 = new SimpleFilter (new Operator("<="),
                                             new SimpleColumn("tpch.lineitem.l_discount"),
                                             new ArithmeticColumn("':2' + 0.01"));
        filterTokenList.push_back(f4);
        filterTokenList.push_back( new Operator ("and"));
                
        SimpleFilter *f5 = new SimpleFilter (new Operator("<"),
                                             new SimpleColumn("tpch.lineitem.l_quantity"),
                                             new ConstantColumn(":3"));
        filterTokenList.push_back(f5);        
                
        csep.filterTokenList(filterTokenList);     
        
        ParseTree *pt = const_cast<ParseTree*>(csep.filters());
        pt->drawTree ("q6.dot");
        
        cout << csep;
    }  

    
}; 

CPPUNIT_TEST_SUITE_REGISTRATION( TPCH_EXECPLAN );

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


