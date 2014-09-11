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
 *   $Id: updatepackageprocessor.h 7535 2011-03-09 21:36:04Z chao $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef UPDATEPACKAGEPROCESSOR_H
#define UPDATEPACKAGEPROCESSOR_H
#include <string>
#include "dmlpackageprocessor.h"
#include "dataconvert.h"
#include <vector>
#include "joblist.h"

#if defined(_MSC_VER) && defined(UPDATEPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{
struct extentInfo 
{
	uint16_t dbRoot;
	uint32_t partition;
	uint16_t segment;
};
typedef struct extentInfo extentInfo;
typedef std::vector<uint64_t> RidList;
typedef std::vector<std::string> ColValues;
struct extentInfoCompare // lt operator
{
    bool operator()(const extentInfo& lhs, const extentInfo& rhs) const
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

//typedef std::vector<uint64_t> rids;
typedef std::map< extentInfo, dmlpackageprocessor::rids, extentInfoCompare > ridsUpdateListsByExtent;
typedef std::map< extentInfo, std::vector<ColValues>, extentInfoCompare > valueListsByExtent;
/** @brief concrete implementation of a DMLPackageProcessor.
 * Specifically for interacting with the Write Engine to
 * process UPDATE dml statements.
 */
class UpdatePackageProcessor : public DMLPackageProcessor
{

public:
	UpdatePackageProcessor() : DMLPackageProcessor(), fMaxUpdateRows(5000000) {}
    /** @brief process an UpdateDMLPackage
     *
     * @param cpackage the UpdateDMLPackage to process
     */
    EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);
	void setMaxUpdateRows(uint64_t maxUpdateRows) { fMaxUpdateRows = maxUpdateRows; }
	typedef std::vector<void *> VoidValuesList;
	
protected:

private:
    /** @brief update one or more rows with constant values
     *
	 * @param sessionID the session id of the is session
     * @param txnID the transaction id for the current transaction
	 * @param schema the schema name
     * @param table  the table name
     * @param rows the list of rows
	 * @param rowidLists on return contains the row id(s) of the updated rows for each extent
	 * @param colValuesList on return contains the values to be updated to
	 * @param colOldValuesList on return contains the values prior to the update
	 * @param result the result of the operation
	 * @param colNameList the column names for updating columns
	 * @param colTypes the column types for updating columns
	 * @param extentsinfo the information for each extent
	 * @param ridsListsMap ridlist for all extents
     * @return true on success, false on error
     */
    bool updateRows(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const std::string& schema,
                    const std::string& table, const dmlpackage::RowList& rows, std::vector<WriteEngine::RIDList>& rowidLists,
                    WriteEngine::ColValueList& colValuesList,
                    std::vector<void *>& colOldValuesList, 
                    DMLResult& result, 
                    std::vector<std::string>& colNameList,
                    std::vector<execplan::CalpontSystemCatalog::ColType>& colTypes,
					std::vector<extentInfo>& extentsinfo,
					ridsUpdateListsByExtent& ridsListsMap, long long & nextVal);

    /** @brief update one or more rows for column=column
     *
	 * @param sessionID the session id of the is session
     * @param txnID the transaction id for the current transaction
     * @param schema the schema name
     * @param table  the table name
     * @param rows the list of rows
	 * @param rowsProcessed the rows have been updated
	 * @param colsValues the values for the updating columns
	 * @param result the result of the operation
	 * @param ridsListsMap ridlist for all extents
     * @return true on success, false on error
     */
    bool updateRowsValues(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const std::string& schema,
                    const std::string& table, const dmlpackage::RowList& rows, uint64_t & rowsProcessed,
                    valueListsByExtent& valueLists, 
                    DMLResult& result, 
					ridsUpdateListsByExtent& ridsListsMap, long long & nextVal, execplan::CalpontSystemCatalog::ColType & autoColType);
    /** @brief add all rows to the update  for update with constants
     *
	 * @param sessionID the session id of the is session
	 * @param cpackage the serialized calpontdmlpackage
     * @param schema the schema name
     * @param table the table name
     * @param rows the list of rows
	 * @param firstCall indicate whether the jobsteps need to be executed 
	 * @param jbl the joblist built by joblistfactory
	 * @param extentsinfo the information for each extent
	 * @param ridsListsMap ridlist for all extents
	 * @param result the result of the operation
     * @return ture when this is the last batch, otherwise return false.
     */
    bool fixUpRows(u_int32_t sessionID, dmlpackage::CalpontDMLPackage& cpackage, const std::string& schema, const std::string& table, 
				dmlpackage::RowList& rows, bool & firstCall, joblist::SJLP & jbl, std::vector<extentInfo>& extentsinfo, ridsUpdateListsByExtent& ridsListsMap, DMLResult& result);
	
    /** @brief add all rows to the update  for update column=column
     *
	 * @param sessionID the session id of the is session
	 * @param cpackage the serialized calpontdmlpackage
     * @param schema the schema name
     * @param table the table name
     * @param colsValues the values for updating columns
	 * @param firstCall indicate whether the jobsteps need to be executed 
	 * @param jbl the joblist built by joblistfactory
	 * @param extentsinfo the information for each extent
	 * @param ridsListsMap ridlist for all extents
	 * @param result the result of the operation
     * @return ture when this is the last batch, otherwise return false.
     */
    bool fixUpRowsValues(u_int32_t sessionID, dmlpackage::CalpontDMLPackage& cpackage, const std::string& schema, const std::string& table, 
				valueListsByExtent& valueLists,
				bool & firstCall, joblist::SJLP & jbl, std::vector<extentInfo>& extentsinfo, ridsUpdateListsByExtent& ridsListsMap, DMLResult& result);
	
	void clearVoidValuesList(VoidValuesList& valuesList);
	uint64_t fMaxUpdateRows; 
};

}

#undef EXPORT

#endif                                            //UPDATEPACKAGEPROCESSOR_H
