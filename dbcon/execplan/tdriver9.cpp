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
	nation,\
	o_year,\
	sum(amount) as sum_profit\
from\
	(\
		select\
			n_name as nation,\
			extract(year from o_orderdate) as o_year,\
			l_extendedprice * (1 - l_discount) - ps_supplycost * l_quantity as amount\
		from\
			part,\
			supplier,\
			lineitem,\
			partsupp,\
			orders,\
			nation\
		where\
			s_suppkey = l_suppkey\
			and ps_suppkey = l_suppkey\
			and ps_partkey = l_partkey\
			and p_partkey = l_partkey\
			and o_orderkey = l_orderkey\
			and s_nationkey = n_nationkey\
			and p_name like '%:1%'\
	) as profit\
group by\
	nation,\
	o_year\
order by\
	nation,\
	o_year desc;";
        
        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        
        // these columns are from the temp table of FROM clause. 
        // I hereby give schema name "calpont", table name "FROMTABLE",
        SimpleColumn *c1 = new SimpleColumn("tpch.nation.n_name");
        c1->tableAlias("profit");
        c1->alias("nation");
        returnedColumnList.push_back(c1);        
        ArithmeticColumn *c2 = new ArithmeticColumn("extract(year from tpch.orders.o_orderdate)");
        c2->tableAlias("profit");
        c2->tableAlias("o_year");
        returnedColumnList.push_back(c2);       
        ArithmeticColumn *c3 = new ArithmeticColumn("sum(amount)");
        c3->alias("sum_profit");
        returnedColumnList.push_back(c3);
        
        csep.returnedCols(returnedColumnList);
     
        // subselect in FROM clause      
        CalpontSelectExecutionPlan *subsep = 
           new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::FROM);
        
        // subselect returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList subReturnedColList;
        SimpleColumn *sc1 = new SimpleColumn("tpch.nation.n_name");
        sc1->alias("nation");
        sc1->tableAlias("profit");
        subReturnedColList.push_back(sc1);
        
        ArithmeticColumn *sc2 = new ArithmeticColumn("extract(year from tpch.orders.o_orderdate)");
        sc2->alias ("o_year");
        sc2->tableAlias("profit");
        subReturnedColList.push_back(sc2);
              
        ArithmeticColumn *sc3 = new ArithmeticColumn(
        "tpch.lineitem.l_extendeprice * (1-tpch.lineitem.l_discount)- tpch.partsupp.ps_supplycost * tpch.lineitem.l_quantity");
        sc3->alias("amount");
        sc3->tableAlias("shipping");
        subReturnedColList.push_back(sc3);
        
        subsep->returnedCols(subReturnedColList);
        
        // subselect filters
        CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList;
        
        SimpleFilter *sf1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_suppkey"),
                                             new SimpleColumn("tpch.lineitem.l_suppkey"));
        subFilterTokenList.push_back(sf1);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.partsupp.ps_suppkey"),
                                             new SimpleColumn("tpch.lineitem.l_suppkey"));
        subFilterTokenList.push_back(sf2);
                                             
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.partsupp.ps_partkey"),
                                             new SimpleColumn("tpch.lineitem.l_partkey"));
        subFilterTokenList.push_back(sf3);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf4 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.part.p_partkey"),
                                             new SimpleColumn("tpch.lineitem.l_partkey"));
        subFilterTokenList.push_back(sf4);
        subFilterTokenList.push_back(new Operator("and"));        
        
        SimpleFilter *sf5 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.order.o_orderkey"),
                                             new SimpleColumn("tpch.lineitem.l_orderkey"));
        subFilterTokenList.push_back(sf5);
        subFilterTokenList.push_back(new Operator("and"));          

        SimpleFilter *sf6 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_nationkey"),
                                             new SimpleColumn("tpch.nation.n_nationkey"));
        subFilterTokenList.push_back(sf6);
        subFilterTokenList.push_back(new Operator("and"));

        SimpleFilter *sf7 = new SimpleFilter ( new Operator("like"),
                                              new SimpleColumn("tpch.part.p_name"),
                                              new ConstantColumn ("%:1%"));
        subFilterTokenList.push_back(sf7);

        subsep->filterTokenList(subFilterTokenList);  
        subsep->tableAlias("profit");
                
        CalpontSelectExecutionPlan::SelectList fromSubSelectList;
        fromSubSelectList.push_back(subsep);
        csep.subSelects(fromSubSelectList);
       
        ParseTree* pt = const_cast<ParseTree*>(subsep->filters());
        pt->drawTree("q9.dot");   
        
        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
	    SimpleColumn *g1 = new SimpleColumn (*c1);
	    groupByList.push_back (g1);	
	    //ArithmeticColumn *g2 = new ArithmeticColumn (*c2);
	    groupByList.push_back (c2->clone());	    
	    csep.groupByCols(groupByList);
	    
        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    SimpleColumn *o1 = new SimpleColumn(*c1);
	    orderByList.push_back(o1);
	    ArithmeticColumn *o2 = c2->clone();
	    o2->asc(false);
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


