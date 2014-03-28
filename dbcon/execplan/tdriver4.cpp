/* Copyright (C) 2013 Calpont Corp.

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
	    o_orderpriority,\
	    count(*) as order_count\
    from\
    	orders\
    where\
    	o_orderdate >= date ':1'\
    	and o_orderdate < date ':1' + interval '3' month\
    	and exists (\
    		select\
    			*\
    		from\
    			lineitem\
    		where\
    			l_orderkey = o_orderkey\
    			and l_commitdate < l_receiptdate\
    	)\
    group by\
    	o_orderpriority\
    order by\
    	o_orderpriority;";

        
        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
                
        SimpleColumn *c1 = new SimpleColumn("tpch.orders.o_orderpriority");
        returnedColumnList.push_back(c1);
        
        ArithmeticColumn *c2 = new ArithmeticColumn ("count(ALL)");
        c2->alias("order_count");                 
        returnedColumnList.push_back(c2);          
               
        csep.returnedCols(returnedColumnList);
        
        // Filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        SimpleFilter *f1 = new SimpleFilter (new Operator(">="),
                                             new SimpleColumn("tpch.orders.o_orderdate"),
                                             new ConstantColumn("1998-05-01"));
        filterTokenList.push_back(f1);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f2 = new SimpleFilter (new Operator("<"),
                                             new SimpleColumn("tpch.orders.o_orderdate"),
                                             new ConstantColumn("1999-05-01"));
        filterTokenList.push_back(f2);
        filterTokenList.push_back(new Operator("and"));
        
        // subselect for exists
        CalpontSelectExecutionPlan *subcsep = 
           new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);        
        // select * (lineitem)
        CalpontSelectExecutionPlan::ReturnedColumnList subReturnedColList;
        SimpleColumn *sc1 = new SimpleColumn ("tpch.lineitem.l_orderkey");
        SimpleColumn *sc2 = new SimpleColumn ("tpch.lineitem.l_partkey");
        SimpleColumn *sc3 = new SimpleColumn ("tpch.lineitem.l_suppkey");
        SimpleColumn *sc4 = new SimpleColumn ("tpch.lineitem.l_linenumber");
        SimpleColumn *sc5 = new SimpleColumn ("tpch.lineitem.l_extendedprice");
        SimpleColumn *sc6 = new SimpleColumn ("tpch.lineitem.l_tax");
        SimpleColumn *sc7 = new SimpleColumn ("tpch.lineitem.l_returnflag");
        SimpleColumn *sc8 = new SimpleColumn ("tpch.lineitem.l_linestatus");
        SimpleColumn *sc9 = new SimpleColumn ("tpch.lineitem.l_commitdate");
        SimpleColumn *sc10 = new SimpleColumn ("tpch.lineitem.l_receiptdate");
        SimpleColumn *sc11 = new SimpleColumn ("tpch.lineitem.l_shipinstruct");
        SimpleColumn *sc12 = new SimpleColumn ("tpch.lineitem.l_shipmode");
        SimpleColumn *sc13 = new SimpleColumn ("tpch.lineitem.l_comment");

        subReturnedColList.push_back(sc1);
        subReturnedColList.push_back(sc2);
        subReturnedColList.push_back(sc3);
        subReturnedColList.push_back(sc4);
        subReturnedColList.push_back(sc5);
        subReturnedColList.push_back(sc6);
        subReturnedColList.push_back(sc7);
        subReturnedColList.push_back(sc8);
        subReturnedColList.push_back(sc9);
        subReturnedColList.push_back(sc10);
        subReturnedColList.push_back(sc11);
        subReturnedColList.push_back(sc12);
        subReturnedColList.push_back(sc13);
 
        subcsep->returnedCols(subReturnedColList);
 
        // filters of subselect
        CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList;
        SimpleFilter *ssf1 = new SimpleFilter (
                                new Operator("="),
                                new SimpleColumn("tpch.lineitem.l_orderkey"),
                                new SimpleColumn("tpch.orders.o_orderkey"));
        subFilterTokenList.push_back (ssf1);
        subFilterTokenList.push_back (new Operator("and"));
        
        SimpleFilter *ssf2 = new SimpleFilter (
                                new Operator("<"),
                                new SimpleColumn("tpch.lineitem.l_commitdate"),
                                new SimpleColumn("tpch.lineitem.l_receiptdate"));
        subFilterTokenList.push_back (ssf2);
        subcsep->filterTokenList(subFilterTokenList);   
 
        // back to parent select exist filter       
        ExistsFilter *exists = new ExistsFilter (subcsep);
        filterTokenList.push_back (exists);
        csep.filterTokenList(filterTokenList);   
        
        ParseTree* pt = const_cast<ParseTree*>(csep.filters());
        pt->drawTree("q4.dot");   

        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;        
        SimpleColumn *g1 = new SimpleColumn(*c1);
        groupByList.push_back(g1);       
        
        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
        SimpleColumn *o1 = new SimpleColumn(*c1);
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


