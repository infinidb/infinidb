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
 *   $Id: ddlindexpopulator.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef DDLINDEXPOPULATOR_H

#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>

#include "ddlpackageprocessor.h"
#include "calpontsystemcatalog.h"
#include "sessionmanager.h"
#include "ddlpkg.h"
#include "messageobj.h"
#include "we_type.h"
#include "writeengine.h"
#include "columnresult.h"
#include <boost/any.hpp>
#include <boost/tuple/tuple.hpp>


#include "joblistfactory.h"

namespace joblist {
class DistributedEngineComm;
}

namespace ddlpackageprocessor
{

/** @brief Populate an new Index 
 * implementation of a DDLPopulator
 */
class DDLIndexPopulator 
{

public:
  /** @brief constructor
   *
   */
  DDLIndexPopulator(WriteEngine::WriteEngineWrapper* writeEngine, 
		    execplan::SessionManager* sessionManager, 
		    uint32_t sessionID, 
		    execplan::CalpontSystemCatalog::SCN txnID, 
		    DDLPackageProcessor::DDLResult& result, 
		    const DDLPackageProcessor::IndexOID& idxOID, 
		    const ddlpackage::ColumnNameList& colNames, 
		    const ddlpackage::QualifiedName& table,
		    const ddlpackage::DDL_CONSTRAINTS constraint,
		    const DDLPackageProcessor::DebugLevel debug):
    fWriteEngine(writeEngine), fSessionManager(sessionManager),
    fSessionID(sessionID), fTxnID(txnID), fResult(result),
    fIdxOID(idxOID), fColNames(colNames), fTable(table), fDebugLevel(debug),
    fEC(0), fRidList(), fIdxStructList(), fIdxValueList(), 
 /*   fTOKENSIZE(sizeof(WriteEngine::Token) ) {}*/
    fConstraint(constraint), fUniqueColResultList() {}
 

    /** @brief destructor
     */
    virtual ~DDLIndexPopulator() { };
	
	
    /** @brief Is it required to debug
     */
    const bool isDebug( const DDLPackageProcessor::DebugLevel level ) const { return level <= fDebugLevel; }
	
    /** @brief Get debug level
     */
    const DDLPackageProcessor::DebugLevel getDebugLevel() const { return fDebugLevel; }
	
	
    /** @brief set distributedEngineComm pointer ( for
     * loading index).
     */
    void setEngineComm(joblist::DistributedEngineComm* ec) { fEC = ec; }
    void setIdxOID(const DDLPackageProcessor::IndexOID& idxOID) { fIdxOID = idxOID; }
	
    DDLPackageProcessor::DDLResult  getResult() const { return fResult; } 


    /** @brief add data to the index from the statement
     *
     * populate the newly made index with data in the index columns.
     * returns if there was an error.
     */
    bool populateIndex(DDLPackageProcessor::DDLResult& result);
	/** @brief returns if dictionary type
     *
     * determines if coltype is dictionary type based on type and size
     */
	bool isDictionaryType(const execplan::CalpontSystemCatalog::ColType& ctype);

    void setConstraint(ddlpackage::DDL_CONSTRAINTS constraint);

 protected:
 
    /** @brief make the structures to update the index
     *
     * builds and executes a query to retrieve all the data from the index columns
     * to make the structures required by WriteEngine::updateIndex
     * returns if there is data and no error
     */
    bool makeIndexStructs();

    /** @brief make the IdxStruct
     *
     * Fills in the values from the column result and calpont system catalog for
     * the WriteEngine::IdxStruct
     */
    execplan::CalpontSystemCatalog::ColType makeIdxStruct(const execplan::ColumnResult* cr, size_t cols, boost::shared_ptr<execplan::CalpontSystemCatalog> csc );
	
    /** @brief add the column result data to the value list
     *
     * Check contraints on each data item and adds it to a tuple list.
     * Adds the rid to the rid list if it is the first column (not been added)
     * Adds the completed tuple list to the fValueList
     */
    void addColumnData(const execplan::ColumnResult* cr, const execplan::CalpontSystemCatalog::ColType ctype, int added);

    /** @brief insert data into index.
     *
     * updates the index with the data using the appropriate write engine method 
     * based on multi column. Sets result to error if there is one.
     */
    void insertIndex();	 
       

 private: 
     DDLIndexPopulator(const DDLIndexPopulator& );
     void operator=(const DDLIndexPopulator& );
    /** @brief makes Calpont Select Execution Plan
     *
     * builds csep to select data from all columns from fColNames
     */
    void makeCsep(execplan::CalpontSelectExecutionPlan&  csep);

    /** @brief return if ColumnResult string type
     *
     * Uses same logic as ColumnResult from type to return getStringData (true)
     * or getData (false).
     */

    bool isStringType(int type) const
    {
  	return (type == execplan::CalpontSystemCatalog::CHAR 
		|| type == execplan::CalpontSystemCatalog::VARCHAR
		|| type == execplan::CalpontSystemCatalog::FLOAT
		|| type == execplan::CalpontSystemCatalog::DOUBLE
        || type == execplan::CalpontSystemCatalog::UFLOAT
        || type == execplan::CalpontSystemCatalog::UDOUBLE );
    }

    /** @brief converts column result data
     *
     * Converts or tokenizes data, depending on column type
     */
    void convertColData(const execplan::ColumnResult* cr, int idx,  const execplan::CalpontSystemCatalog::ColType& cType, WriteEngine::IdxTuple& tuple);

    /** @brief converts non token data to its original type
     */
    boost::any  convertData(const execplan::CalpontSystemCatalog::ColType&  colType, const execplan::ColumnResult* cr, int idx );
  
     /** @brief returns token for string data
     *
     * There are two methods, the one using the rid is a workaround.
     * Use the method that passes string data when WriteEngine::tokenize is
     * able to return an existing token for a string.  The rid method reads
     * the column file directly.
     */    
    boost::any tokenizeData( const execplan::CalpontSystemCatalog::ColType& colType, const std::string& data );

    boost::any tokenizeData( WriteEngine::RID rid  );

     /** @brief convert token data
     *
     * Indexes will use the first 8 bytes of a token type value instead
     * of a token.
     */    

    boost::any convertTokenData( const std::string& data );

     /** @brief opens the column file for the oid
     *
     * This method is needed only as long as the class is using the rid
     * tokenizeData method.  The fColumnFile and this method are no longer 
     * needed when the WriteEngine::tokenize method can be used.
     */     
    //bool  openColumnFile(WriteEngine::OID oid);

    /** @brief returns if data violated its constraint
     *
     * checks data according to contraint in coltype and sets result to error
     * if constraint violated.  Returns if no error.
     */
    bool checkConstraints(const WriteEngine::IdxTuple& data, const execplan::CalpontSystemCatalog::ColType& ctype, int i, int column); 

    /** @brief returns if data not null
     *
     * Returns false if data is null and sets result to error
     */
    bool checkNotNull(const WriteEngine::IdxTuple& data, const execplan::CalpontSystemCatalog::ColType& ctype);

    /** @brief returns if data is not unique
     *
     * Returns false if data row is found more than once in columns and sets result to error
     */
    bool checkUnique(int  i, const execplan::CalpontSystemCatalog::ColType& colType );

    bool checkCheck( const WriteEngine::IdxTuple& data, const execplan::CalpontSystemCatalog::ColType& ctype) const  { return true; }

    bool isUnique() { return ddlpackage::DDL_PRIMARY_KEY == fConstraint || ddlpackage::DDL_UNIQUE == fConstraint; }
    /** @brief logs error and message
     *
     * Updates result with message and sets it to CREATE_ERROR
     */
    void logError(const std::string& msg, int error = 0); 

    bool compareToken(const WriteEngine::Token& first, const WriteEngine::Token& second) const
    {
      return (first.op == second.op && first.fbo == second.fbo && first.spare == second.spare);
    }    

    WriteEngine::WriteEngineWrapper*	fWriteEngine;
    execplan::SessionManager*        	fSessionManager;
    uint32_t 				fSessionID;
    execplan::CalpontSystemCatalog::SCN fTxnID;
    DDLPackageProcessor::DDLResult	fResult;
    DDLPackageProcessor::IndexOID    	fIdxOID;
    ddlpackage::ColumnNameList	 	fColNames;
    ddlpackage::QualifiedName		fTable;
    DDLPackageProcessor::DebugLevel 	fDebugLevel; 
    joblist::DistributedEngineComm*    	fEC;	    
    WriteEngine::RIDList  	       	fRidList;
    WriteEngine::IdxStructList         	fIdxStructList;
    WriteEngine::IdxValueList		fIdxValueList;

    ddlpackage::DDL_CONSTRAINTS		fConstraint;
    std::vector<execplan::ColumnResult*> fUniqueColResultList;
    std::vector<int> fOidList;
    std::ifstream 			fColumnFile;
    static const int			fTOKENSIZE = 8;

    struct DDLNullValueForType : DDLPackageProcessor
    {
      DDLNullValueForType(const execplan::CalpontSystemCatalog::ColType& ctype)
	: DDLPackageProcessor(), fType(ctype) {}
      boost::any operator()(execplan::CalpontSystemCatalog::ColType& ctype)
      {
	return getNullValueForType(fType);
      }
      const execplan::CalpontSystemCatalog::ColType& fType;
    };

};
 
}
#endif //DDLPINDEXPOPULATOR_H
