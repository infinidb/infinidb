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
	        n_name,\
	        sum(l_extendedprice * (1 - l_discount)) as revenue\
        from\
        	customer,\
        	orders,\
        	lineitem,\
        	supplier,\
        	nation,\
        	region\
        where\
        	c_custkey = o_custkey\
        	and l_orderkey = o_orderkey\
        	and l_suppkey = s_suppkey\
        	and c_nationkey = s_nationkey\
        	and s_nationkey = n_nationkey\
        	and n_regionkey = r_regionkey\
        	and r_name = ':1'\
        	and o_orderdate >= date ':2'\
        	and o_orderdate < date ':2' + interval '1' year\
        group by\
        	n_name\
        order by\
        	revenue desc;";
        	        

        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
                
        SimpleColumn *c1 = new SimpleColumn("tpch.nation.n_name");
        returnedColumnList.push_back(c1);
        
        ArithmeticColumn *c2 = new ArithmeticColumn("sum(tpch.lineitem.l_extendedprice * (1 - tpch.lineitem.l_discount))");
        c2->alias("revenue");
        returnedColumnList.push_back(c2);                
        
        csep.returnedCols(returnedColumnList);
        
        // Filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        SimpleFilter *f1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.customer.c_custkey"),
                                             new SimpleColumn("tpch.orders.o_custkey"));
        filterTokenList.push_back(f1);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.lineitem.l_orderkey"),
                                             new SimpleColumn("tpch.orders.o_orderkey"));
        filterTokenList.push_back(f2);
        filterTokenList.push_back( new Operator ("and"));
        
        SimpleFilter *f3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.lineitem.l_suppkey"),
                                             new SimpleColumn("tpch.supplier.s_suppkey"));
        filterTokenList.push_back(f3);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f4 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.customer.c_nationkey"),
                                             new SimpleColumn("tpch.supplier.s_nationkey"));
        filterTokenList.push_back(f4);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f5 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_nstionkey"),
                                             new SimpleColumn("tpch.nation.n_nationkey"));
        filterTokenList.push_back(f5);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f6 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.region.r_name"),
                                             new ConstantColumn(":1"));
        filterTokenList.push_back(f6);
        filterTokenList.push_back( new Operator ("and"));
        SimpleFilter *f7 = new SimpleFilter (new Operator(">="),
                                             new SimpleColumn("tpch.orders.o_orderdate"),
                                             new ArithmeticColumn("date(':2')"));
        filterTokenList.push_back(f7);
        filterTokenList.push_back( new Operator ("and"));

        SimpleFilter *f8 = new SimpleFilter (new Operator("<"),
                                             new SimpleColumn("tpch.orders.o_orderdate"),
                                             new ArithmeticColumn("date(':2') + interval ('1', year)"));
        filterTokenList.push_back(f8);
                
        csep.filterTokenList(filterTokenList);     
        
        ParseTree *pt = const_cast<ParseTree*>(csep.filters());
        pt->drawTree ("q5.dot");
        
        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
        groupByList.push_back(c1->clone());        
 
        csep.groupByCols (groupByList);    

        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    ArithmeticColumn *o1 = new ArithmeticColumn(*c2);
	    o1->asc(false);
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


