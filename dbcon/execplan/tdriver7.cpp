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
	    supp_nation,\
	    cust_nation,\
	    l_year,\
	    sum(volume) as revenue\
    from\
	(\
		select\
			n1.n_name as supp_nation,\
			n2.n_name as cust_nation,\
			extract(year from l_shipdate) as l_year,\
			l_extendedprice * (1 - l_discount) as volume\
		from\
			supplier,\
			lineitem,\
			orders,\
			customer,\
			nation n1,\
			nation n2\
		where\
			s_suppkey = l_suppkey\
			and o_orderkey = l_orderkey\
			and c_custkey = o_custkey\
			and s_nationkey = n1.n_nationkey\
			and c_nationkey = n2.n_nationkey\
			and (\
				(n1.n_name = ':1' and n2.n_name = ':2')\
				or (n1.n_name = ':2' and n2.n_name = ':1')\
			)\
			and l_shipdate between date '1995-01-01' and date '1996-12-31'\
	    ) as shipping\
    group by\
	    supp_nation,\
	    cust_nation,\
	    l_year\
    order by\
	    supp_nation,\
	    cust_nation,\
	    l_year;";
        
        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        
        // these columns are from the temp table of FROM clause. 
        // I hereby give schema name "calpont", table name "FROMTABLE",
        // and alias name "shipping"        
        SimpleColumn *c1 = new SimpleColumn("calpont.FROMTABLE.supp_nation");
        c1->tableAlias ("shipping");
        returnedColumnList.push_back(c1);
        
        SimpleColumn *c2 = new SimpleColumn("calpont.FROMTABLE.cust_nation");
        c2->tableAlias ("shipping");
        returnedColumnList.push_back(c2);
        
        SimpleColumn *c3 = new SimpleColumn("tpch.lineitem.l_year");
        returnedColumnList.push_back(c3);        
        
        ArithmeticColumn *c4 = new ArithmeticColumn("sum(volumn)");
        c4->alias("revenue");
        returnedColumnList.push_back(c4);
                  
        csep.returnedCols(returnedColumnList);
        
        // from subselect                                                       
        CalpontSelectExecutionPlan *subsep = 
           new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::FROM);
        
        // subselect returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList subReturnedColList;
        SimpleColumn *sc1 = new SimpleColumn("tpch.nation.n_name");
        sc1->alias ("supp_nation");
        sc1->tableAlias ("n1");
        subReturnedColList.push_back(sc1);
        
        SimpleColumn *sc2 = new SimpleColumn("tpch.nation.n_name");
        sc1->alias ("cust_nation");
        sc1->tableAlias ("n2");
        subReturnedColList.push_back(sc2);
        
        ArithmeticColumn *sc3 = new ArithmeticColumn("extract(year from tpch.lineitem.l_shipdate)");
        sc3->alias("l_year");
        subReturnedColList.push_back(sc3);
        
        ArithmeticColumn *sc4 = new ArithmeticColumn("tpch.lineitem.l_extendeprice * (1-tpch.lineitem.l_discount)");
        sc3->alias("volume");
        subReturnedColList.push_back(sc4);
        
        subsep->returnedCols(subReturnedColList);
        
        // subselect filters
        CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList;
        
        SimpleFilter *sf1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_suppkey"),
                                             new SimpleColumn("tpch.lineitem.l_suppkey"));
        subFilterTokenList.push_back(sf1);
                                             
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.orders.o_orderkey"),
                                             new SimpleColumn("tpch.lineitem.l_orderkey"));
        subFilterTokenList.push_back(sf2);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.customer.c_custkey"),
                                             new SimpleColumn("tpch.orders.o_custkey"));
        subFilterTokenList.push_back(sf3);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleColumn *n1 = new SimpleColumn ("tpch.nation.n_nationkey");
        n1->tableAlias("n1");
        SimpleFilter *sf4 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_nationkey"),
                                             n1);
        subFilterTokenList.push_back(sf4);
        subFilterTokenList.push_back(new Operator("and"));        
        
        SimpleColumn *n2 = new SimpleColumn ("tpch.nation.n_nationkey");
        n2->tableAlias("n2");
        SimpleFilter *sf5 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.customer.c_nationkey"),
                                             n2);
        subFilterTokenList.push_back(sf5);
        subFilterTokenList.push_back(new Operator("and")); 
        
        // ((n1.n_name = ':1' and n2.n_name = ':2')
		//	or (n1.n_name = ':2' and n2.n_name = ':1'))      
        subFilterTokenList.push_back(new Operator("("));        
        subFilterTokenList.push_back(new Operator("("));        

        SimpleColumn *n1_name = new SimpleColumn("tpch.nation.n_name");
        n1_name->tableAlias ("n1");
        SimpleFilter *sf6 = new SimpleFilter ( new Operator("="),
                                              n1_name,
                                              new ConstantColumn (":1"));
        subFilterTokenList.push_back(sf6);
        subFilterTokenList.push_back(new Operator("and"));

        SimpleColumn *n2_name = new SimpleColumn("tpch.nation.n_name");
        n1_name->tableAlias ("n2");
        SimpleFilter *sf7 = new SimpleFilter ( new Operator("="),
                                              n2_name,
                                              new ConstantColumn (":2"));
        subFilterTokenList.push_back(sf7); 
        subFilterTokenList.push_back(new Operator (")"));                                     
        subFilterTokenList.push_back(new Operator("or"));
        subFilterTokenList.push_back(new Operator("("));        
        
        SimpleFilter *sf8 = new SimpleFilter ( new Operator("="),
                                              new SimpleColumn(*n1_name),
                                              new ConstantColumn (":2"));
        subFilterTokenList.push_back(sf8);
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf9 = new SimpleFilter ( new Operator("="),
                                              new SimpleColumn(*n2_name),
                                              new ConstantColumn (":1"));   
        subFilterTokenList.push_back(sf9);
        subFilterTokenList.push_back(new Operator (")"));                                     
        subFilterTokenList.push_back(new Operator (")"));                                     
                                                                                         
        
        subFilterTokenList.push_back(new Operator("and"));      
        
        SimpleFilter *sf10 = new SimpleFilter (new Operator (">="),
                                               new SimpleColumn ("tpch.lineitem.l_shipdate"),
                                               new ConstantColumn ("1995-01-01")); 
        subFilterTokenList.push_back(sf10);
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf11 = new SimpleFilter (new Operator ("<="),
                                               new SimpleColumn ("tpch.lineitem.l_shipdate"),
                                               new ConstantColumn ("1995-01-06"));                                                         
        subFilterTokenList.push_back(sf11);
            
        subsep->filterTokenList(subFilterTokenList);  
        
        // end of subselect in FROM. push FROM subselect to selectList
        // NOTE: only FROM subselect needs to be pushed into selectList.
        // Subselects in WHERE or HAVING clause are in where or having
        // filter parse tree. It may make more sense to change the member
        // fSelectList of CSEP class to fFromSubSelect (type CSEP*)
        CalpontSelectExecutionPlan::SelectList fromSubSelectList;
        fromSubSelectList.push_back(subsep);
        csep.subSelects(fromSubSelectList);
       
        ParseTree* pt = const_cast<ParseTree*>(subsep->filters());
        pt->drawTree("q7.dot");   
        
        // Group by
	    CalpontSelectExecutionPlan::GroupByColumnList groupByList;
	    SimpleColumn *g1 = new SimpleColumn (*c1);
	    groupByList.push_back (g1);
	    
	    SimpleColumn *g2 = new SimpleColumn (*c2);
	    groupByList.push_back (g2);
	    
	    SimpleColumn *g3 = new SimpleColumn (*c3);
	    groupByList.push_back (g3);
	    
	    csep.groupByCols(groupByList);
	    
        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    SimpleColumn *o1 = new SimpleColumn(*c1);
	    orderByList.push_back(o1);
	    
	    SimpleColumn *o2 = new SimpleColumn(*c2);
	    orderByList.push_back(o2);	   
	    
	    SimpleColumn *o3 = new SimpleColumn(*c3);
	    orderByList.push_back(o3); 
        
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


