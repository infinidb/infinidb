/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
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
            s_acctbal,\
            s_name,\
            n_name,\
            p_partkey, \
            p_mfgr,\
            s_address,\
            s_phone,\
            s_comment\
    from\
            part,\
            supplier,\
            partsupp,\
            nation,\
            region\
    where\
            p_partkey = ps_partkey\
            and s_suppkey = ps_suppkey\
            and p_size = :1\
            and p_type like '%:2'\
            and s_nationkey = n_nationkey\
            and n_regionkey = r_regionkey\
            and r_name = ':3'\
            and ps_supplycost = (\
                    select\
                            min(ps_supplycost)\
                    from\
                            partsupp,\
                            supplier,\
                            nation,\
                            region\
                    where\
                            p_partkey = ps_partkey\
                            and s_suppkey = ps_suppkey\
                            and s_nationkey = n_nationkey\
                            and n_regionkey = r_regionkey\
                            and r_name = ':3'\
            )\
    order by\
            s_acctbal desc,\
            n_name,\
            s_name,\
            p_partkey;";
        
        CalpontSelectExecutionPlan csep;
               
        // Returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
                
        SimpleColumn *c1 = new SimpleColumn("tpch.supplier.s_acctbal");
        returnedColumnList.push_back(c1);
        
        SimpleColumn *c2 = new SimpleColumn("tpch.supplier.s_name");
        returnedColumnList.push_back(c2);
        
        SimpleColumn *c3 = new SimpleColumn("tpch.nation.n_name");
        returnedColumnList.push_back(c3);        
        
        SimpleColumn *c4 = new SimpleColumn("tpch.part.p_partkey");
        returnedColumnList.push_back(c4);
        
        SimpleColumn *c5 = new SimpleColumn("tpch.part.p_mfgr");
        returnedColumnList.push_back(c5);
        
        SimpleColumn *c6 = new SimpleColumn("tpch.supplier.s_address");
        returnedColumnList.push_back(c6);
        
        SimpleColumn *c7 = new SimpleColumn("tpch.supplier.s_phone");
        returnedColumnList.push_back(c7);
        
        SimpleColumn *c8 = new SimpleColumn("tpch.supplier.s_comment");
        returnedColumnList.push_back(c8);
            
        csep.returnedCols(returnedColumnList);
        
        // Filters
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        SimpleFilter *f1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.part.p_partkey"),
                                             new SimpleColumn("tpch.partsupp.ps_partkey"));
        filterTokenList.push_back(f1);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_suppkey"),
                                             new SimpleColumn("tpch.partsupp.ps_suppkey"));
        filterTokenList.push_back(f2);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.part.p_size"),
                                             new ConstantColumn(1));
        filterTokenList.push_back(f3);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f4 = new SimpleFilter (new Operator("like"),
                                             new SimpleColumn("tpch.part.p_type"),
                                             new ConstantColumn(":2"));
        filterTokenList.push_back(f4);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f5 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_nationkey"),
                                             new SimpleColumn("tpch.nation.n_nationkey"));
        filterTokenList.push_back(f5);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f6 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.nation.n_regionkey"),
                                             new SimpleColumn("tpch.region.r_regionkey"));
        filterTokenList.push_back(f6);
        filterTokenList.push_back(new Operator("and"));
        SimpleFilter *f7 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.region.r_name"),
                                             new ConstantColumn(":3"));
        filterTokenList.push_back(f7);
        filterTokenList.push_back(new Operator("and"));                                             
        
        // sub select execution plan for select filter
        CalpontSelectExecutionPlan *subsep = 
           new CalpontSelectExecutionPlan(CalpontSelectExecutionPlan::WHERE);
        
        // subselect returned columns
        CalpontSelectExecutionPlan::ReturnedColumnList subReturnedColList;
        ArithmeticColumn *sc1 = new ArithmeticColumn("min(tpch.partsupp.ps_supplycost)");
        subReturnedColList.push_back(sc1);
        subsep->returnedCols(subReturnedColList);
        
        // subselect filters
        CalpontSelectExecutionPlan::FilterTokenList subFilterTokenList;
        
        SimpleFilter *sf1 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.part.p_partkey"),
                                             new SimpleColumn("tpch.partsupp.ps_partkey"));
        subFilterTokenList.push_back(sf1);
                                             
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf2 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.part.p_partkey"),
                                             new SimpleColumn("tpch.partsupp.ps_partkey"));
        subFilterTokenList.push_back(sf2);
        subFilterTokenList.push_back(new Operator("and"));
        
        SimpleFilter *sf3 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.supplier.s_nationkey"),
                                             new SimpleColumn("tpch.nation.n_nationkey"));
        subFilterTokenList.push_back(sf3);
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf4 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.nation.n_regionkey"),
                                             new SimpleColumn("tpch.region.r_regionkey"));
        subFilterTokenList.push_back(sf4);
        subFilterTokenList.push_back(new Operator("and"));
        SimpleFilter *sf5 = new SimpleFilter (new Operator("="),
                                             new SimpleColumn("tpch.region.r_name"),
                                             new ConstantColumn(":3"));
        subFilterTokenList.push_back(sf5);
        
        subsep->filterTokenList(subFilterTokenList);  
        
        // end of sub select
        SelectFilter *f8 = new SelectFilter (new SimpleColumn("tpch.partsupp.ps_supplycost"),
                                             new Operator("="),
                                             subsep);
                                                
        filterTokenList.push_back(f8);
        csep.filterTokenList(filterTokenList);   
        
        //ParseTree* pt = const_cast<ParseTree*>(csep.filters());
        //pt->drawTree("q2.dot");   

        // Order by                                                
	    CalpontSelectExecutionPlan::OrderByColumnList orderByList;
	    SimpleColumn *o1 = new SimpleColumn(*c1);
	    o1->asc(false);
	    orderByList.push_back(o1);
	    
	    SimpleColumn *o2 = new SimpleColumn(*c3);
	    orderByList.push_back(o2);	   
	    
	    SimpleColumn *o3 = new SimpleColumn(*c2);
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


