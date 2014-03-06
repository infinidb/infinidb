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

#include "../../myhdr.h"
#include <boost/timer.hpp>

#include <stdio.h>
#include <string>
#include <stdexcept>
using namespace std;

#include <cppunit/extensions/HelperMacros.h>

#include <sstream>
#include <exception>
#include <iostream>
#include <unistd.h>
#include <sys/types.h>
#include <errno.h>

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "ddlpkg.h"
#include "sqlparser.h"
using namespace ddlpackage;

#include "createindexprocessor.h"
#include "altertableprocessor.h"
#include "createtableprocessor.h"

using namespace ddlpackageprocessor;

#include "writeengine.h"
#include "we_colop.h"
using namespace WriteEngine;

#include "distributedenginecomm.h"
using namespace joblist;

#include "messagelog.h"

class PopulateIndexTest
{
public:

  PopulateIndexTest(DistributedEngineComm* ec) : fEC(ec) { }
  DistributedEngineComm *fEC;

  void test_createindex()
  {
    cout << "Begining create index test ... " << endl;
    std::string sqlbuf = "CREATE  INDEX test1_idx ON tpch.nation (n_nationkey)";
    cout << sqlbuf << endl;
 
    SqlParser parser;
    parser.Parse(sqlbuf.c_str());
    if (parser.Good())
      {
	const ParseTree &ptree = parser.GetParseTree();

        cout << "Parser succeeded." << endl;
        cout << ptree.fList.size() << " " << "SQL statements" << endl;
        cout << ptree.fSqlText << endl;

	try
	  {
	    CreateIndexProcessor processor;
	    processor.setDebugLevel(CreateIndexProcessor::VERBOSE);
	    SqlStatement &stmt = *ptree.fList[0];
	    CreateIndexProcessor::DDLResult result;
	    DISPLAY(stmt.fSessionID);

	    result = processor.processPackage(dynamic_cast<CreateIndexStatement&>(stmt));

	    std::cout << "return: " << result.result << std::endl;
	  }
	catch(...)
	  {
	    throw;
	  }
      }
  }

  void test_createuniqueindex()
  {
    cout << "Begining create unique index test ..." << endl;
    std::string sqlbuf = "CREATE UNIQUE INDEX test2_idx ON tpch.nation (n_name)";
    cout << sqlbuf << endl;

    SqlParser parser;
    parser.Parse(sqlbuf.c_str());
    if (parser.Good())
      {
	const ParseTree &ptree = parser.GetParseTree();
	cout << ptree.fSqlText << endl;

	try
	  {
	    CreateIndexProcessor processor;
	    processor.setDebugLevel(CreateIndexProcessor::VERBOSE);

	    SqlStatement &stmt = *ptree.fList[0];
	    CreateIndexProcessor::DDLResult result;

	    result = processor.processPackage(dynamic_cast<CreateIndexStatement&>(stmt));
	    std::cout << "return: " << result.result << std::endl;
	  }
	catch(...)
	  {
	    throw;
	  }
      }
  }

  void test_createindextest(std::string& sqlbuf)
  {
    cout << "Begining create index test ..." << endl;
     cout << sqlbuf << endl;

    SqlParser parser;
    parser.Parse(sqlbuf.c_str());
    if (parser.Good())
      {
	const ParseTree &ptree = parser.GetParseTree();
	cout << ptree.fSqlText << endl;

	try
	  {
	    CreateIndexProcessor processor;
	    processor.setDebugLevel(CreateIndexProcessor::VERBOSE);

	    SqlStatement &stmt = *ptree.fList[0];
	    CreateIndexProcessor::DDLResult result = processor.processPackage(dynamic_cast<CreateIndexStatement&>(stmt));
	    std::cout << "return: " << result.result << std::endl;
	  }
	catch(...)
	  {
	    throw;
	  }
      }
  }


    void test_createtabletest(const string& sqlbuf)
    {
	cout << "Begining create table test: " << sqlbuf << endl;
        SqlParser parser;
        parser.Parse(sqlbuf.c_str());
        if (parser.Good())
        {
            const ParseTree &ptree = parser.GetParseTree();
	    cout << ptree.fSqlText << endl;
            try
            {
                CreateTableProcessor processor;
                processor.setDebugLevel(CreateTableProcessor::VERBOSE);

                SqlStatement &stmt = *ptree.fList[0];
                CreateTableProcessor::DDLResult result;

                result  = processor.processPackage(dynamic_cast<CreateTableStatement&>(stmt));
                std::cout << "return: " << result.result << std::endl;
            }
            catch(...)
            {
                throw;
            }
        }
    }

  void test_altertable_addtableconstraint(std::string& sqlbuf)
  {
    cout << "Begining Alter Table add table constraint test ... " << endl;
    cout << sqlbuf << endl;

    SqlParser parser;
    parser.Parse(sqlbuf.c_str());
    if (parser.Good())
      {
	const ParseTree &ptree = parser.GetParseTree();
	cout << ptree.fSqlText << endl;
	try
	  {
	    AlterTableProcessor processor;
	    processor.setDebugLevel(AlterTableProcessor::VERBOSE);

	    SqlStatement &stmt = *ptree.fList[0];
	    AlterTableProcessor::DDLResult result;
		
	    result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
	    std::cout << "return: " << result.result << std::endl;
	  }
	catch(...)
	  {
	    throw;
	  }
      }
  }

  void test_altertable_addtablenullconstraint()
  {
//sql syntax error?  (Does not build index test.)
    cout << "Begining Alter Table add table not null constraint test ... " << endl;
    std::string sqlbuf = "ALTER TABLE tpch.region add CONSTRAINT not null(r_regionkey);";
    cout << sqlbuf << endl;

    SqlParser parser;
    parser.Parse(sqlbuf.c_str());
    if (parser.Good())
      {
	const ParseTree &ptree = parser.GetParseTree();
	cout << ptree.fSqlText << endl;
	try
	  {
	    AlterTableProcessor processor;
	    processor.setDebugLevel(AlterTableProcessor::VERBOSE);

	    SqlStatement &stmt = *ptree.fList[0];
	    AlterTableProcessor::DDLResult result;

	    result = processor.processPackage(dynamic_cast<AlterTableStatement&>(stmt));
	    std::cout << "return: " << result.result << std::endl;
	  }
	catch(...)
	  {
	    throw;
	  }
      }
  }
 

};


int main( int argc, char **argv)
{
  int DoAll = 0;
  int Do1 = 0;
  int Do2 = 0;
  int Do3 = 0;
  int Do4 = 0;
  int Do5 = 0;
  int Do6 = 0;
  int Do7 = 0;
  int Do8 = 0;
  int Do9 = 0;
  int Do10 = 0;
  int Do11 = 0;
  int Do12 = 0;
  int Do13 = 0;
  int Do14 = 0;
  int Do15 = 0;
  int Do16 = 0;
  int Do17 = 0;
  int Do18 = 0;
  int Do19 = 0;
  int Do20 = 0;
  int Do21 = 0;
  int Do22 = 0;
  int Do23 = 0;
  int Do24 = 0;
  int Do25 = 0;
  int Do26 = 0;
  int Do27 = 0;
  int Do28 = 0;
  int Do29 = 0;
  int Do30 = 0;
  int Do31 = 0;
  int Do32 = 0;
  int Do33 = 0;
  int Do34 = 0;
  int Do35 = 0;
  int Do36 = 0;
  
  cout << "Driver Test starting with " << argc << " parameters." << endl;

  for (int i=0; i<argc; i++)
    {
      cout << "Arg " << i << ": " << argv[i] << endl;
    }

  if (argc > 1)
    {
      if (strcmp(argv[1],"All") == 0) DoAll = 1;
      else if (strcmp(argv[1],"t1") == 0) Do1 = 1;
      else if (strcmp(argv[1],"t2") == 0) Do2 = 1;
      else if (strcmp(argv[1],"t3") == 0) Do3 = 1;
      else if (strcmp(argv[1],"t4") == 0) Do4 = 1;
      else if (strcmp(argv[1],"t5") == 0) Do5 = 1;
      else if (strcmp(argv[1],"t6") == 0) Do6 = 1;
      else if (strcmp(argv[1],"t7") == 0) Do7 = 1;
      else if (strcmp(argv[1],"t8") == 0) Do8 = 1;
      else if (strcmp(argv[1],"t9") == 0) Do9 = 1;
      else if (strcmp(argv[1],"t10") == 0) Do10 = 1;
      else if (strcmp(argv[1],"t11") == 0) Do11= 1;
      else if (strcmp(argv[1],"t12") == 0) Do12= 1;
      else if (strcmp(argv[1],"t13") == 0) Do13= 1;
      else if (strcmp(argv[1],"t14") == 0) Do14= 1;
      else if (strcmp(argv[1],"t15") == 0) Do15= 1;
      else if (strcmp(argv[1],"t16") == 0) Do16= 1;
      else if (strcmp(argv[1],"t17") == 0) Do17= 1;
      else if (strcmp(argv[1],"t18") == 0) Do18= 1;
      else if (strcmp(argv[1],"t19") == 0) Do19= 1;
      else if (strcmp(argv[1],"t20") == 0) Do20= 1;
      else if (strcmp(argv[1],"t21") == 0) Do21= 1;
      else if (strcmp(argv[1],"t22") == 0) Do22= 1;
      else if (strcmp(argv[1],"t23") == 0) Do23= 1;
      else if (strcmp(argv[1],"t24") == 0) Do24= 1;
      else if (strcmp(argv[1],"t25") == 0) Do25= 1;
      else if (strcmp(argv[1],"t26") == 0) Do26= 1;
      else if (strcmp(argv[1],"t27") == 0) Do27= 1;
      else if (strcmp(argv[1],"t28") == 0) Do28= 1;
      else if (strcmp(argv[1],"t29") == 0) Do29= 1;
      else if (strcmp(argv[1],"t30") == 0) Do30= 1;
      else if (strcmp(argv[1],"t31") == 0) Do31= 1;
      else if (strcmp(argv[1],"t32") == 0) Do32= 1;
      else if (strcmp(argv[1],"t33") == 0) Do33= 1;
      else if (strcmp(argv[1],"t34") == 0) Do34= 1;
      else if (strcmp(argv[1],"t35") == 0) Do35= 1;
      else if (strcmp(argv[1],"t36") == 0) Do35= 1;
 

    }
  PopulateIndexTest pit(DistributedEngineComm::instance());
  boost::timer theTimer;

  if (DoAll)
    {
      cout << "Starting all tests" << endl;
    
      pit.test_createindex();
      pit.test_createuniqueindex();
      std::string altsql = "ALTER TABLE tpch.region add CONSTRAINT test1_cstr unique(r_name);";
      pit.test_altertable_addtableconstraint(altsql);
      pit.test_altertable_addtablenullconstraint();
   
      cout << "Finished all tests" << endl;
    }
   else if (Do1)
    {    
      pit.test_createindex();
    
      cout << "Finished create index test" << endl;
    }
   else if (Do2)
    {    
      pit.test_createuniqueindex();
    
      cout << "Finished create unique index test" << endl;
    }
   else if (Do3)
    {
      std::string sqlbuf = "ALTER TABLE tpch.region add CONSTRAINT test1r5_cstr unique(r_name);";
      pit.test_altertable_addtableconstraint(sqlbuf);    
      cout << "Finished add table constraint test" << endl;
    }
   else if (Do4)
    {
      std::string sql("CREATE INDEX test4_idx ON tpch.part (p_size)");
      pit.test_createindextest(sql);
    
      cout << "Finished " << sql << endl;
    }
   else if (Do5)
    {
      std::string sql("CREATE INDEX test5_idx ON tpch.part (p_name)");
      pit.test_createindextest(sql);
    
      cout << "Finished " << sql << endl;
    }
   else if (Do6)
    {
      std::string sql("CREATE INDEX test6_idx ON tpch.orders (o_orderkey, o_custkey)");
      pit.test_createindextest(sql);
    
      cout << "Finished " << sql << endl;
    }
   else if (Do7)
    {
      std::string sqlbuf = "ALTER TABLE tpch.supplier add CONSTRAINT tests2_cstr unique(s_name);";
      pit.test_altertable_addtableconstraint(sqlbuf);    
      cout << "Finished add table constraint test" << endl;
    }
   else if (Do8)
    {
      std::string sqlbuf = "ALTER TABLE tpch.partsupp add CONSTRAINT testps1_cstr unique(ps_partkey);";
      pit.test_altertable_addtableconstraint(sqlbuf);    
      cout << "Finished add table constraint test: should fail ps_partkey is not unique" << endl;
    }
    else if (Do9)
    {
      std::string sql("CREATE INDEX test7_idx ON tpch.customer (c_custkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }
    else if (Do10)
    {
      std::string sql("CREATE INDEX test8_idx ON tpch.supplier(s_phone)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }
    else if (Do11)
    {
      std::string sql("CREATE INDEX test9_idx ON tpch.part (p_retailprice)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }
    else if (Do12)
    {
      std::string sql("CREATE INDEX test10_idx ON tpch.customer (c_acctbal)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }
    else if (Do13)
    {
      std::string sql("CREATE UNIQUE INDEX test11_idx ON tpch.orders (o_clerk)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test: should fail" << endl;
    }
    else if (Do14)
    {
      std::string sql("CREATE INDEX test12_idx ON tpch.lineitem (l_returnflag)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }
    else if (Do15)
    {
      std::string sql("CREATE INDEX test13_idx ON tpch.lineitem (l_linestatus)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }    
    else if (Do16)
    {
      std::string sql("CREATE INDEX multi_4idx ON tpch.region (r_regionkey, r_name)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }    
    else if (Do17)
    {
      std::string sql("CREATE INDEX multi_5idx ON tpch.orders (o_orderkey, o_orderstatus)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }       
    else if (Do18)
    {
      std::string sql("CREATE UNIQUE INDEX orderkey_idx ON tpch.orders (o_orderkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }       
    else if (Do19)
    {
      std::string sql("CREATE UNIQUE INDEX partkey_idx ON tpch.part (p_partkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }    
    else if (Do20)
    {
      std::string sql("CREATE INDEX lorderkey_idx ON tpch.lineitem (l_orderkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }     
    else if (Do21)
    {
      std::string sql("CREATE INDEX lpartkey1_idx ON tpch.lineitem (l_partkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }  
    else if (Do22)
    {
      std::string sql("CREATE INDEX suppkey1_idx ON tpch.lineitem (l_suppkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }                     
     else if (Do23)
    {
      std::string sql("CREATE INDEX n_regionkey_id ON tpch.nation (n_regionkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }                    
    else if (Do24)
    {
      std::string sql("CREATE INDEX multi_cust_idx ON tpch.customer (c_name, c_address, c_phone, c_mktsegment)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test: " <<  sql << endl;
    }                 
    else if (Do25)
    {
      std::string sql("CREATE INDEX multi_part_no_idx ON tpch.part (p_name, p_mfgr, p_brand, p_container, p_size)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test: " <<  sql << endl;
    }               
    else if (Do26)
    {
      std::string sql("CREATE INDEX o_date_idx ON tpch.orders (o_orderdate)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test: " <<  sql << endl;
    }                   
    else if (Do27)
    {
      std::string sql("CREATE INDEX multi_order_idx ON tpch.orders (o_orderkey, o_orderstatus, o_orderdate)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test: " <<  sql << endl;
    }                   
    else if (Do28)
    {
      string sql("create table tpch.tablea( c1 integer, c2 char);");
      pit.test_createtabletest(sql);
      std::string sqlbuf = "ALTER TABLE tpch.tablea add CONSTRAINT tablea_cstr1 unique(c2);";
      pit.test_altertable_addtableconstraint(sqlbuf);    
      cout << "Finished add table constraint test" << endl;

    }                   
    else if (Do29)
    {
      std::string sqlbuf = "ALTER TABLE tpch.nation add CONSTRAINT testn1_cstr unique(n_regionkey);";
      pit.test_altertable_addtableconstraint(sqlbuf);    
      cout << "Finished add table constraint test: should fail n_regionkey is not unique" << endl;
    }    
    else if (Do30)
    {
      std::string sql("CREATE UNIQUE INDEX multicstr_1idx ON tpch.region (r_regionkey, r_name)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }    
    else if (Do31)
    {
      std::string sql("CREATE UNIQUE INDEX multicsto_1idx ON tpch.orders (o_orderkey, o_orderstatus)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }           
    else if (Do32)
    {
      std::string sql("CREATE UNIQUE INDEX multicstn_1idx ON tpch.nation (n_nationkey, n_regionkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }           
     else if (Do33)
    {
      std::string sql("CREATE UNIQUE INDEX multicstps_1idx ON tpch.partsupp (ps_partkey, ps_suppkey)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test" << endl;
    }           
     else if (Do34)
    {
      std::string sql("CREATE UNIQUE INDEX multicsto_2idx ON tpch.orders (o_orderstatus, o_orderpriority)");
      pit.test_createindextest(sql);
      cout << "Finished add table index test: should fail" << endl;
    }           
    else if (Do35)
    {
      std::string sql("ALTER TABLE tpch.nation add CONSTRAINT testn2_cstr unique(n_nationkey);");
      pit.test_altertable_addtableconstraint(sql);    
      cout << "Finished add table constraint test" << endl;
    }           
    else if (Do36)
    {
      pit.test_altertable_addtablenullconstraint();
    
      cout << "Finished add table not null constraint test" << endl;
    }
  else 
    {
      cout << "No Test Selected!" << endl << endl;

      cout << "All" << endl;
      cout << "t1" << endl;
      cout << "t2" << endl;
      cout << "t3" << endl;
      cout << "t4" << endl;
      cout << endl;
    }
    cout << "Create index test took :" << theTimer.elapsed() << " seconds to complete." << endl;


}

