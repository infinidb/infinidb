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
 *   $Id: deletepackageprocessor.h 9302 2013-03-07 16:06:59Z chao $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef DELETEPACKAGEPROCESSOR_H
#define DELETEPACKAGEPROCESSOR_H
#include <string>
#include <iostream>
#include <vector>
#include "dmlpackageprocessor.h"
#include "dmltable.h"
#include <vector>
#include "joblist.h"

#if defined(_MSC_VER) && defined(DMLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{
/** @brief concrete implementation of a DMLPackageProcessor.
 * Specifically for interacting with the Write Engine to
 * process DELETE dml statements.
 */
class DeletePackageProcessor : public DMLPackageProcessor
{

public:

    DeletePackageProcessor(BRM::DBRM* aDbrm, uint32_t sid) : DMLPackageProcessor(aDbrm, sid){}
    /** @brief process a DeleteDMLPackage
      *
      * @param cpackage the delete dml package to process
      */
   EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);

protected:

private:

	 /** @brief delete a row
      *
      * @param txnID the transaction id
      * @param tablePtr a pointer to the table that is being operated on
      * @param rowIDList upon return containts the row ids of the rows deleted
      * @param colOldValuesList upon return contains the values the were delete
      * @param result upon return will containt the result of the operation
    bool deleteRows(execplan::CalpontSystemCatalog::SCN txnID, dmlpackage::DMLTable* tablePtr,
                   WriteEngine::RIDList& rowIDList, WriteEngine::ColValueList& colOldValuesList,
                   DMLResult& result);
	  */
    bool processRowgroup(messageqcpp::ByteStream & aRowGroup, DMLResult& result, const uint64_t uniqueId, dmlpackage::CalpontDMLPackage& cpackage, std::map<unsigned, bool>& pmStateDel, 
						bool isMeta = false, uint32_t dbroot=1);


    /** @brief add all rows if we have no filter for the delete
      *
      * @param tablePtr a pointer to the table that is being operated on
      */
   uint64_t fixUpRows(dmlpackage::CalpontDMLPackage& cpackage, DMLResult& result, const uint64_t uniqueId, const uint32_t tableOid);
   bool receiveAll(DMLResult& result, const uint64_t uniqueId, std::vector<int>& fPMs, std::map<unsigned, bool>& pmStateDel, const uint32_t tableOid);
   
	//bandListsByExtent bandListsMap;

};

} // namespace dmlpackageprocessor

#undef EXPORT

#endif  // DELETEPACKAGEPROCESSOR_H
