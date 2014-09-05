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
*   $Id: insertpackageprocessor.h 9302 2013-03-07 16:06:59Z chao $
*
*
***********************************************************************/
/** @file */

#ifndef INSERTPACKAGEPROCESSOR_H
#define INSERTPACKAGEPROCESSOR_H
#include <string>
#include <vector>
#include <boost/any.hpp>
#include "dmlpackageprocessor.h"
#include "dmltable.h"
#include "dataconvert.h"
#include "we_chunkmanager.h"

#if defined(_MSC_VER) && defined(INSERTPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{
/** @brief concrete implementation of a DMLPackageProcessor.
  * Specifically for interacting with the Write Engine to
  * process INSERT dml statements.
  */
class InsertPackageProcessor : public DMLPackageProcessor
{

public:
	InsertPackageProcessor(BRM::DBRM* aDbrm) : DMLPackageProcessor(aDbrm) {
	}
    /** @brief process an InsertDMLPackage
      *
      * @param cpackage the InsertDMLPackage to process
      */
    EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);

protected:

private:
    /**	@brief add any columns that were not supplied in the insert statement
      *
      * @param schema the schema name
      * @param table  the table name
      * @param rows the lists of rows add any missing columns
      * @param result the result of the operation
      */
    bool fixupColumns( u_int32_t sessionID, const std::string& schema, const std::string& table,
                       const dmlpackage::RowList& rows, DMLResult& result );
    //u_int32_t fSessionID;

};

}

#undef EXPORT

#endif //INSERTPACKAGEPROCESSOR_H

