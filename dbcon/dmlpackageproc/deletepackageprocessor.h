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
 *   $Id: deletepackageprocessor.h 7409 2011-02-08 14:38:50Z rdempsey $
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

#if defined(_MSC_VER) && defined(DELETEPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{

	struct deleteExtentInfo 
{
	uint16_t dbRoot;
	uint32_t partition;
	uint16_t segment;
};
typedef struct deleteExtentInfo delextentInfo;
typedef std::vector<uint64_t> RidList;
typedef struct deleteExtentInfo delextentInfo;

struct delextentInfoCompare // lt operator
{
    bool operator()(const delextentInfo& lhs, const delextentInfo& rhs) const
    {
        if( lhs.dbRoot < rhs.dbRoot ) {
          return true;
        }
        if(lhs.dbRoot==rhs.dbRoot && lhs.partition < rhs.partition ) {
          return true;
        }
        if(lhs.dbRoot==rhs.dbRoot && lhs.partition==rhs.partition && lhs.segment < rhs.segment ) {
          return true;
        }

        return false;

    } // operator
}; // struct

//typedef std::vector<joblist::TableBand> tableBands;
//typedef std::vector<uint64_t> rids;
typedef std::map< delextentInfo, dmlpackageprocessor::rids, delextentInfoCompare > ridsListsByExtent;
/** @brief concrete implementation of a DMLPackageProcessor.
 * Specifically for interacting with the Write Engine to
 * process DELETE dml statements.
 */
class DeletePackageProcessor : public DMLPackageProcessor
{

public:

    DeletePackageProcessor() : DMLPackageProcessor(), fMaxDeleteRows(5000000) {}
    /** @brief process a DeleteDMLPackage
      *
      * @param cpackage the delete dml package to process
      */
   EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);
   void setMaxDeleteRows(uint64_t maxDeleteRows) { fMaxDeleteRows = maxDeleteRows; }
   typedef std::vector<void *> VoidValuesList;

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
    bool deleteRows(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const std::string& schema,
		    const std::string& table,
		    std::vector<WriteEngine::RIDList>& rowidLists,
		    std::vector<void *>& colOldValuesList, DMLResult& result, ridsListsByExtent& ridsListsMap);


    /** @brief add all rows if we have no filter for the delete
      *
      * @param tablePtr a pointer to the table that is being operated on
      */
    bool fixUpRows(u_int32_t sessionID, dmlpackage::CalpontDMLPackage& cpackage, const std::string& schema, 
			const std::string& table, std::vector<dicStrValues>& dicStrValCols,
			bool & firstCall, joblist::SJLP & jbl, ridsListsByExtent& ridsListsMap, DMLResult& result);

    void clearVoidValuesList(VoidValuesList& valuesList);

    uint64_t fMaxDeleteRows; 
   
	//bandListsByExtent bandListsMap;

};

} // namespace dmlpackageprocessor

#undef EXPORT

#endif  // DELETEPACKAGEPROCESSOR_H
