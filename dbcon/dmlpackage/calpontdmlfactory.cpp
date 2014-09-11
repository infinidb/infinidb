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

/***********************************************************************
 *   $Id: calpontdmlfactory.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *
 ***********************************************************************/

#define CALPONTDMLFACTORY_DLLEXPORT
#include "calpontdmlfactory.h"
#undef CALPONTDMLFACTORY_DLLEXPORT
#include "insertdmlpackage.h"
#include "updatedmlpackage.h"
#include "deletedmlpackage.h"
#include "commanddmlpackage.h"
#include "dmlparser.h"

#undef DECIMAL
#undef DELETE
#undef IN
#include "dml-gram.h"

#include <stdexcept>
#include <iostream>
using namespace std;
namespace dmlpackage
{
boost::mutex CalpontDMLFactory::fParserLock;

dmlpackage::CalpontDMLPackage* CalpontDMLFactory::makeCalpontDMLPackage(dmlpackage::VendorDMLStatement& vpackage,
        std::string defaultSchema /*= ""*/)
{
    int retval = 1;
    CalpontDMLPackage* packagePtr = 0;
    try
    {
        std::string dmlStatement = vpackage.get_DMLStatement();
		//@Bug 2680. DMLParser is not thread safe.
		boost::mutex::scoped_lock lk(fParserLock);
        DMLParser parser;
        if (defaultSchema.size())
		{
            parser.setDefaultSchema(defaultSchema);
		}
        parser.parse(dmlStatement.c_str());
		
        if (parser.good())
        {

            const ParseTree &ptree = parser.getParseTree();
            SqlStatement* statementPtr = ptree[0];

#ifdef DML_PACKAGE_DEBUG
            //std::cout << ptree;
            //std::cout << statementPtr->getQueryString();
            //std::cout << endl;
#endif

            int dmlStatementType = statementPtr->getStatementType();

            switch (dmlStatementType)
            {
                case DML_INSERT:
                    packagePtr = new InsertDMLPackage(statementPtr->getSchemaName(), statementPtr->getTableName(),
                                                      ptree.fSqlText, vpackage.get_SessionID() );
		    packagePtr->set_SQLStatement(dmlStatement);
                    retval = packagePtr->buildFromSqlStatement(*statementPtr);
                    break;

                case DML_UPDATE:
                    packagePtr = new UpdateDMLPackage(statementPtr->getSchemaName(), statementPtr->getTableName(),
                                                      ptree.fSqlText, vpackage.get_SessionID() );
		    packagePtr->set_SQLStatement(dmlStatement);
                    retval = packagePtr->buildFromSqlStatement(*statementPtr);
                    break;

                case DML_DELETE:
                    packagePtr = new DeleteDMLPackage(statementPtr->getSchemaName(), statementPtr->getTableName(),
                                                      ptree.fSqlText, vpackage.get_SessionID() );
		    packagePtr->set_SQLStatement(dmlStatement);
                    retval = packagePtr->buildFromSqlStatement(*statementPtr);
                    break;

                case DML_COMMAND:
                    packagePtr = new CommandDMLPackage(ptree.fSqlText, vpackage.get_SessionID());
                    retval = packagePtr->buildFromSqlStatement(*statementPtr);
                    break;

                default:
                    cerr << "makeCalpontDMLPackage: invalid statement type" << endl;
                    break;

            }

        }
    }
    catch (std::exception& ex)
    {
        cerr << "makeCalpontDMLPackage:" << ex.what() << endl;
    }
    catch (...)
    {
        cerr << "makeCalpontDMLPackage: caught unknown exception!" << endl;
    }

    return packagePtr;

}

dmlpackage::CalpontDMLPackage* CalpontDMLFactory::makeCalpontDMLPackageFromBuffer(dmlpackage::VendorDMLStatement& vpackage)
{
    int retval = 1;
    CalpontDMLPackage* packagePtr = 0;
    try
    {
        int dmlStatementType = vpackage.get_DMLStatementType();
        switch (dmlStatementType)
        {
            case DML_INSERT:
                packagePtr = new InsertDMLPackage(vpackage.get_SchemaName(), vpackage.get_TableName(), vpackage.get_DMLStatement(), vpackage.get_SessionID());
                retval = packagePtr->buildFromBuffer(vpackage.get_DataBuffer
                                                     (),vpackage.get_Columns(), vpackage.get_Rows());
                break;
            case DML_UPDATE:
                packagePtr = new UpdateDMLPackage(vpackage.get_SchemaName(),
                                                  vpackage.get_TableName(),vpackage.get_DMLStatement(), vpackage.get_SessionID());
                retval = packagePtr->buildFromBuffer(vpackage.get_DataBuffer
                                                     (),vpackage.get_Columns(), vpackage.get_Rows());
                break;
            case DML_DELETE:
                packagePtr = new DeleteDMLPackage(vpackage.get_SchemaName(),
                                                  vpackage.get_TableName(),vpackage.get_DMLStatement(), vpackage.get_SessionID());
                retval = packagePtr->buildFromBuffer(vpackage.get_DataBuffer
                                                     (),vpackage.get_Columns(), vpackage.get_Rows());
                break;
            case DML_COMMAND:
                packagePtr = new CommandDMLPackage(vpackage.get_DMLStatement(), vpackage.get_SessionID() );

                break;
            default:
                cerr << "makeCalpontDMLPackage: invalid statement type" << endl;
                break;
        }
    }
    catch (std::exception& ex)
    {
        cerr << "makeCalpontDMLPackage:" << ex.what() << endl;
    }
    catch (...)
    {
        cerr << "makeCalpontDMLPackage: caught unknown exception!" << endl;
    }
    return packagePtr;
}

dmlpackage::CalpontDMLPackage* CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlpackage::VendorDMLStatement& vpackage)
{
	int retval = 1;
    CalpontDMLPackage* packagePtr = 0;
	try
    {
        int dmlStatementType = vpackage.get_DMLStatementType();
        switch (dmlStatementType)
        {
            case DML_INSERT:
                packagePtr = new InsertDMLPackage(vpackage.get_SchemaName(), vpackage.get_TableName(), vpackage.get_DMLStatement(), vpackage.get_SessionID());
                retval = packagePtr->buildFromMysqlBuffer(vpackage.get_ColNames(), vpackage.get_values(), vpackage.get_Columns(), vpackage.get_Rows());
                break;
			case  DML_COMMAND:
                packagePtr = new CommandDMLPackage(vpackage.get_DMLStatement(), vpackage.get_SessionID() );
				break;
			case DML_DELETE:
                packagePtr = new DeleteDMLPackage(vpackage.get_SchemaName(), vpackage.get_TableName(),
                                                      vpackage.get_DMLStatement(), vpackage.get_SessionID() );
                retval = packagePtr->buildFromMysqlBuffer(vpackage.get_ColNames(), vpackage.get_values(), vpackage.get_Columns(), vpackage.get_Rows());
                break;
			default:
                cerr << "makeCalpontDMLPackage: invalid statement type" << endl;
                break;
		}
	}
    catch (std::exception& ex)
    {
        cerr << "makeCalpontDMLPackage:" << ex.what() << endl;
    }
    catch (...)
    {
        cerr << "makeCalpontDMLPackage: caught unknown exception!" << endl;
    }
	return packagePtr;
}

dmlpackage::CalpontDMLPackage* CalpontDMLFactory::makeCalpontUpdatePackageFromMysqlBuffer(dmlpackage::VendorDMLStatement& vpackage, dmlpackage::UpdateSqlStatement& updateStmt)
{
	CalpontDMLPackage* packagePtr = new UpdateDMLPackage((updateStmt.fNamePtr)->fSchema, (updateStmt.fNamePtr)->fName,
                                                      vpackage.get_DMLStatement(), vpackage.get_SessionID() );
	UpdateDMLPackage* updatePkgPtr = dynamic_cast<UpdateDMLPackage*>(packagePtr);
    updatePkgPtr->buildUpdateFromMysqlBuffer(updateStmt);
	packagePtr = dynamic_cast<CalpontDMLPackage*>(updatePkgPtr);
	return packagePtr;
}
}                                                 //namespace dmlpackage
