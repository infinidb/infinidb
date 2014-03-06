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
        	l_returnflag,\
        	l_linestatus,\
        	sum(l_quantity) as sum_qty,\
        	sum(l_extendedprice) as sum_base_price,\
        	sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,\
        	sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,\
        	avg(l_quantity) as avg_qty,\
        	avg(l_extendedprice) as avg_price,\
        	avg(l_discount) as avg_disc,\
        	count(*) as count_order\
        from\
        	lineitem\
        where\
        	l_shipdate <= date '1998-12-01' - interval ':1' day (3)\
        group by\
        	l_returnflag,\
        	l_linestatus\
        order by\
	        l_returnflag,\
	        l_linestatus;";
	        

        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
                
        SimpleColumn *c1 = new SimpleColumn("tpch.lineitem.l_returnflag");
        returnedColumnList.push_back(c1);
        
        SimpleColumn *c2 = new SimpleColumn("tpch.lineitem.l_linestatus");
        returnedColumnList.push_back(c2);
        
        ArithmeticColumn *c3 = new ArithmeticColumn("sum(tpch.lineitem.l_quantity)");
        c3->alias("sum_qty");
        returnedColumnList.push_back(c3);        
        
        ArithmeticColumn *c4 = new ArithmeticColumn("sum(tpch.lineitem.l_extendedprice)");
        c4->alias("sum_base_price");
        returnedColumnList.push_back(c4);
        
        ArithmeticColumn *c5 = new ArithmeticColumn("sum(tpch.lineitem.l_extendedprice * (1 - tpch.lineitem.l_discount))");
        c5->alias("sum_disc_price");
        returnedColumnList.push_back(c5);
        
        ArithmeticColumn *c6 = new ArithmeticColumn("avg(tpch.lineitem.l_quantity)");
        c6->alias("avg_qty");
        returnedColumnList.push_back(c6);
        
        ArithmeticColumn *c7 = new ArithmeticColumn("avg(tpch.lineitem.l_extendedprice)");
        c6->alias("avg_price");
        returnedColumnList.push_back(c7);
        
        ArithmeticColumn *c8 = new ArithmeticColumn("avg(tpch.lineitem.l_discount)");
        c8->alias("avg_disc");
        returnedColumnList.push_back(c8);
        
        // count(*) -> count(ALL)
        ArithmeticColumn *c9 = new ArithmeticColumn("count(ALL)");
        c9->alias("count_order");
        returnedColumnList.push_back(c9);
        
        csep.returnedCols(returnedColumnList);
        
        // Filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        SimpleFilter *f1 = new SimpleFilter (new Operator("<="),
                                             new SimpleColumn("tpch.lineitem.l_shipdate"),
                                             new ArithmeticColumn("date('1998-12-01')"));
        filterTokenList.push_back(f1);
        csep.filterTokenList(filterTokenList);     
        
        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
        SimpleColumn *g1 = new SimpleColumn(*c1);
        groupByList.push_back(g1);
        
        SimpleColumn *g2 = new SimpleColumn(*c2);
        groupByList.push_back(g2);
            
        csep.groupByCols (groupByList);    

        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    SimpleColumn *o1 = new SimpleColumn(*g1);
	    orderByList.push_back(o1);
	    
	    SimpleColumn *o2 = new SimpleColumn(*g2);
	    orderByList.push_back(o2);	    
        
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


