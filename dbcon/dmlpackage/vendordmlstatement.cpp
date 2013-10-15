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

/***********************************************************************
 *   $Id: vendordmlstatement.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/

#define VENDORDMLSTATEMENT_DLLEXPORT
#include "vendordmlstatement.h"
#undef VENDORDMLSTATEMENT_DLLEXPORT

using namespace std;

namespace dmlpackage
{

    VendorDMLStatement::VendorDMLStatement(std::string dmlstatement, int sessionID)
        :fDMLStatement(dmlstatement),fSessionID(sessionID), fLogging(true),fLogending(true)
        {}

	VendorDMLStatement::VendorDMLStatement(std::string dmlstatement, int stmttype, int sessionID)
        :fDMLStatement(dmlstatement), fDMLStatementType(stmttype), fSessionID(sessionID),fLogging(true),fLogending(true)
        {}
		
    VendorDMLStatement::VendorDMLStatement(std::string dmlstatement, int stmttype,
        std::string tName, std::string schema,
        int rows, int columns, std::string buf,
        int sessionID)
        :fDMLStatement(dmlstatement), fDMLStatementType(stmttype),
        fTableName(tName), fSchema(schema), fRows(rows), fColumns(columns),
        fDataBuffer(buf), fSessionID(sessionID), fLogging(true),fLogending(true)
        {}

	VendorDMLStatement::VendorDMLStatement(std::string dmlstatement, int stmttype, std::string tName, std::string schema, int rows, int columns, 
				ColNameList& colNameList, TableValuesMap& tableValuesMap, int sessionID)
		:fDMLStatement(dmlstatement), fDMLStatementType(stmttype),
        fTableName(tName), fSchema(schema), fRows(rows), fColumns(columns),
        fColNameList(colNameList), fTableValuesMap(tableValuesMap), fSessionID(sessionID), fLogging(true),fLogending(true)
        {}
		
    VendorDMLStatement::~VendorDMLStatement()
        {}
}
