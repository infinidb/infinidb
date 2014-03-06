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

/******************************************************************************************
* $Id: tpchpopulate.cpp 2101 2013-01-21 14:12:52Z rdempsey $
*
******************************************************************************************/

#include <boost/timer.hpp>

#include "dbbuilder.h"
#include "tpchpopulate.h"


using namespace dmlpackageprocessor;
using namespace dmlpackage;
using namespace std;

void TpchPopulate::populateFromFile(std::string tableName, string& fileName)
{

    cout << endl;
    cout << "Populating " << tableName << endl;
    cout << "---------------------------------------" << endl;

    std::string dmlStatement;
    ifstream ifdml(fileName.c_str(), ios::in);
    if (ifdml)
    {
        while (ifdml.good())
        {
            char line[256];
            line[0] = '\0';;
            ifdml.getline(line, 256);
            //if ( line[0] == '\0' )
            if ( strlen(line) == 0 )
            	continue;
            dmlStatement = "insert into ";
            dmlStatement += tableName;
            dmlStatement += " values(";
            dmlStatement += line;
            dmlStatement += ");";

            insert(dmlStatement);
        }

    }
    else
    {
        perror(fileName.c_str());
    }

    cout << endl;
    cout << "Finished Populating " << tableName << endl;
}

void TpchPopulate::populate_part()
{

    std::string dmlStatement = "insert into tpch.part values('1', 'goldenrod lace spring peru powder', 'Manufac#1', 'Brand#13', 'PROMO BURNISHED COPPER', '7', 'JUMBO PKG', '901.00', 'final deposits s' );";
    insert(dmlStatement);

    dmlStatement = "insert into tpch.part values(2, 'blush rosy metallic lemon navajo', 'Manufac#1', 'Brand#13','LARGE BRUSHED BRASS', 1, 'LG CASE', 902.00, 'final platelets hang f');";
    insert(dmlStatement);

    dmlStatement = "insert into tpch.part values(3, 'dark green antique puff wheat', 'Manufac#4', 'Brand#42', 'STANDARD POLISHED BRASS', 21, 'WRAP CASE', 903.00, 'unusual excuses ac');";
    insert(dmlStatement);

    dmlStatement = "insert into tpch.part values(4, 'chocolate metallic smoke ghost drab', 'Manufac#3', 'Brand#34', 'SMALL PLATED BRASS', 14, 'MED DRUM', 904.00, 'ironi');";
    insert(dmlStatement);

    dmlStatement = "insert into tpch.part values(5, 'forest blush chiffon thistle chocolate', 'Manufac#3', 'Brand#32', 'STANDARD POLISHED TIN', 15, 'SM PKG', 905.00, 'pending, spe');";
    insert(dmlStatement);
    
    cout << endl;
    cout << "Commiting inserting to tpch.part table ..." << endl;
    std::string command("COMMIT;");
    VendorDMLStatement dml_command(command, 1);
    CalpontDMLPackage* dmlCommandPkgPtr = CalpontDMLFactory::makeCalpontDMLPackage(dml_command);

    DMLPackageProcessor* pkgProcPtr = DMLPackageProcessorFactory::makePackageProcessor(  DML_COMMAND, *dmlCommandPkgPtr );

    DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *dmlCommandPkgPtr );

    if ( DMLPackageProcessor::NO_ERROR != result.result )
    {
       	cout << "Command process failed!" << endl;
    }
    delete pkgProcPtr;
    delete dmlCommandPkgPtr; 

    cout << endl;
    cout << "Finished Populating tpch.part table" << endl;
}

void TpchPopulate::populate_customer()
{
    cout << endl;
    cout << "Populating tpch.customer table" << endl;
    cout << "---------------------------------------" << endl;

    std::string dmlStatement = "insert into tpch.customer values(1,'Customer#000000001', 'IVhzIApeRb ot,c,E',15, '25-989-741-2988',711.56, 'BUILDING', 'regular, regular platelets are fluffily according to the even attainments. blithely iron');";

    insert(dmlStatement);

    cout << endl;
    cout << "Finished Populating tpch.customer table" << endl;
}

void TpchPopulate::populate_tpch()
{
	std::string tableName;
	std::string filePath;
    cout << endl;
    cout << "Populating region table" << endl;
    cout << "---------------------------------------" << endl;  
    tableName = "tpch.region";
    filePath = "region.tbl";      
    populateFromFile(tableName, filePath );
    
    cout << "Populating nation table" << endl;
    cout << "---------------------------------------" << endl;   
    tableName = "tpch.nation";
    filePath = "nation.tbl";     
    populateFromFile(tableName, filePath );
    
    cout << "Populating customer table" << endl;
    cout << "---------------------------------------" << endl;   
    tableName = "tpch.customer";
    filePath = "customer.tbl";  
    populateFromFile(tableName, filePath );
    
    cout << "Populating orders table" << endl;
    cout << "---------------------------------------" << endl; 
    tableName = "tpch.orders";
    filePath = "orders.tbl";   
    populateFromFile(tableName, filePath );
    
    cout << "Populating part table" << endl;
    cout << "---------------------------------------" << endl;  
    tableName = "part.nation";
    filePath = "part.tbl";  
    populateFromFile(tableName, filePath );
    
    cout << "Populating supplier table" << endl;
    cout << "---------------------------------------" << endl;  
    tableName = "tpch.supplier";
    filePath = "supplier.tbl"; 
    populateFromFile(tableName, filePath );
    
    cout << "Populating partsupp table" << endl;
    cout << "---------------------------------------" << endl;  
    tableName = "tpch.partsupp";
    filePath = "partsupp.tbl";  
    populateFromFile(tableName, filePath );
    
    cout << "Populating lineitem table" << endl;
    cout << "---------------------------------------" << endl;  
    tableName = "tpch.lineitem";
    filePath = "lineitem.tbl";   
    populateFromFile(tableName, filePath );
        
    cout << endl;
    cout << "Finished Populating TPCH tables." << endl;

}
void TpchPopulate::insert(string insertStmt)
{

    cout << endl;
    cout << insertStmt << endl;
    cout << "---------------------------------------" << endl;

	boost::timer theTimer;
    VendorDMLStatement dmlStmt(insertStmt, 1);
    CalpontDMLPackage* dmlPkgPtr = CalpontDMLFactory::makeCalpontDMLPackage(dmlStmt);

    DMLPackageProcessor* pkgProcPtr = DMLPackageProcessorFactory::makePackageProcessor(  DML_INSERT, *dmlPkgPtr );

    //pkgProcPtr->setDebugLevel(DMLPackageProcessor::VERBOSE);
    DMLPackageProcessor::DMLResult result = pkgProcPtr->processPackage( *dmlPkgPtr );
    if ( DMLPackageProcessor::NO_ERROR != result.result )
    {
        cout << "Insert failed!" << endl;
    }
    else
    {
        cout << "Insert successful" << endl;
    }

	cout << "Insert took :" << theTimer.elapsed() << " seconds to complete." << endl;

    delete pkgProcPtr;
    delete dmlPkgPtr;

}

