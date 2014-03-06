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
#include "treenodeimpl.h"
#include "operator.h"
#include "arithmeticcolumn.h"
#include "aggregatecolumn.h"
#include "existsfilter.h"
#include "selectfilter.h"

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
    	ps_partkey,\
    	sum(ps_supplycost * ps_availqty) as value\
    from\
    	partsupp,\
    	supplier,\
    	nation\
    where\
    	ps_suppkey = s_suppkey\
    	and s_nationkey = n_nationkey\
    	and n_name = ':1'\
    group by\
    	ps_partkey having\
    		sum(ps_supplycost * ps_availqty) > (\
    			select\
    				sum(ps_supplycost * ps_availqty) * :2\
    			from\
    				partsupp,\
    				supplier,\
    				nation\
    			where\
    				ps_suppkey = s_suppkey\
    				and s_nationkey = n_nationkey\
    				and n_name = ':1'\
    		)\
    order by\
    	value desc;";
        
        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
             
        SimpleColumn *c1 = new SimpleColumn("tpch.partsupp.ps_partkey");
        returnedColumnList.push_back(c1);
        
        ArithmeticColumn *c2 = new ArithmeticColumn("sum(tpch.partsupp.ps_supplycost*tpch.partsupp.ps_availqty)");
        c2->alias ("value");
        returnedColumnList.push_back(c2);
                        
        csep.returnedCols(returnedColumnList);
        
        // Where filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;        
        SimpleFilter *sf1 = new SimpleFilter( new Operator("="),
                                              new SimpleColumn("tpch.partsupp.ps_suppkey"),
                                              new SimpleColumn("tpch.supplier.s_suppkey") );
        filterTokenList.push_back(sf1);
        filterTokenList.push_back( new Operator ("and"));
        
        SimpleFilter *sf2 = new SimpleFilter( new Operator("="),
                                              new SimpleColumn("tpch.supplier.s_nationkey"),
                                              new SimpleColumn("tpch.nation.n_nationkey") );
        filterTokenList.push_back(sf2);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *sf3 = new SimpleFilter( new Operator("="),
                                             new SimpleColumn ("tpch.nation.n_name"),
                                             new ConstantColumn (":1"));
        filterTokenList.push_back(sf3);                                             
        csep.filterTokenList(filterTokenList);  

        // Group by
   	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
        SimpleColumn *g1 = new SimpleColumn ("tpch.partsupp.ps_partkey");
        groupByList.push_back(g1);
        
        // Having
   	    CalpontSelectExecutionPlan::FilterTokenList havingTokenList;
   	    
   	    ArithmeticColumn *hc = new ArithmeticColumn ("sum(tpch.partsupp.ps_supplycost*tpch.partsupp.ps_availqty)");

   	    // sub select in having
        CalpontSelectExecutionPlan *subsep = 
           new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::HAVING);   	    
        
        // subselect returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList subReturnedColList;
        ArithmeticColumn *sc1 = new ArithmeticColumn("sum(tpch.partsupp.ps_supplycost*tpch.partsupp.ps_availqty)*':2'");
        subReturnedColList.push_back(sc1);
             
        subsep->returnedCols(subReturnedColList);
        
        // subselect filters
        CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList;
        
        SimpleFilter *subsf1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.partsupp.ps_suppkey"),
                                             new SimpleColumn("tpch.supplier.s_suppkey"));
        subFilterTokenList.push_back(subsf1);
                                             
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *subsf2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_nationkey"),
                                             new SimpleColumn("tpch.nation.n_nationkey"));
        subFilterTokenList.push_back(subsf2);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *subsf3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.nation.n_name"),
                                             new ConstantColumn(":1"));
        subFilterTokenList.push_back(subsf3);
                    
        subsep->filterTokenList(subFilterTokenList);  
        
        SelectFilter *sef = new SelectFilter (hc, new Operator (">"), subsep);
        havingTokenList.push_back (sef);
        
        csep.havingTokenList (havingTokenList);
       
        //ParseTree* pt = const_cast<ParseTree*>(subsep->filters());
        //pt->drawTree("q7.dot");   
	    
        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    ArithmeticColumn *o1 = new ArithmeticColumn(*c2);
	    orderByList.push_back(o1);
	      
        csep.orderByCols(orderByList);
        
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


