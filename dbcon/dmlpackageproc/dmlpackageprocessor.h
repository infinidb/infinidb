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
 *   $Id: dmlpackageprocessor.h 9673 2013-07-09 15:59:49Z chao $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef DMLPACKAGEPROCESSOR_H
#define DMLPACKAGEPROCESSOR_H
#include <stdexcept>                              
#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <boost/any.hpp>
#include "calpontdmlpackage.h"
#include "calpontsystemcatalog.h"
#include "we_type.h"
#include "writeengine.h"
#include "messageobj.h"
#include "sessionmanager.h"
#include "distributedenginecomm.h"
#include "brmtypes.h"
#include "we_clients.h"
#include "liboamcpp.h"
#include "oamcache.h"
#include "querystats.h"
#include "clientrotator.h"

#if defined(_MSC_VER) && defined(DMLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

//#define IDB_DML_DEBUG
namespace dmlpackageprocessor
{
typedef std::vector<std::string> dicStrValues;
#define SUMMARY_INFO( message ) \
        if ( isDebug(SUMMARY) ) \
        { \
            std::cerr << message << std::endl; \
        }

#define DETAIL_INFO( message ) \
        if ( isDebug(DETAIL) ) \
        { \
            std::cerr << message << std::endl; \
        }

#define VERBOSE_INFO( message ) \
        if ( isDebug(VERBOSE) ) \
        { \
            std::cerr << message << std::endl; \
        }

typedef std::vector<uint64_t> rids;

/** @brief abstract class that the defines the general interface and
 * implemetation of a DMLPackageProcessor
 */
class DMLPackageProcessor
{

public:
    /** @brief Result code
     */
    enum  ResultCode
    {
        NO_ERROR, INSERT_ERROR, NETWORK_ERROR, NOTNULL_VIOLATION,
        CHECK_VIOLATION, DELETE_ERROR, UPDATE_ERROR, INDEX_UPDATE_ERROR,
        COMMAND_ERROR, TOKEN_ERROR, NOT_ACCEPTING_PACKAGES, DEAD_LOCK_ERROR, REFERENCE_VIOLATION,
        IDBRANGE_WARNING, VB_OVERFLOW_ERROR, ACTIVE_TRANSACTION_ERROR, TABLE_LOCK_ERROR, JOB_ERROR, JOB_CANCELED
    };

    enum DebugLevel                   /** @brief Debug level type enumeration */
    {
        NONE                    = 0,  /** @brief No debug info */
        SUMMARY                 = 1,  /** @brief Summary level debug info */
        DETAIL                  = 2,  /** @brief A little detail debug info */
        VERBOSE                 = 3,  /** @brief Detailed debug info */
    };

    /** @brief the result of dml operations
     */
    struct DMLResult
    {
        /** @brief the result code
         */
        ResultCode result;
        /** @brief the error message if result != NO_ERROR
         */
        logging::Message message;
        /** @brief the rowCount
         */
        long long rowCount; 
        std::string tableLockInfo;
        // query stats;
        std::string queryStats;
        std::string extendedStats;
        std::string miniStats;
        querystats::QueryStats stats;
        	
        DMLResult():result(NO_ERROR),rowCount(0){};
    };
    /** @brief a structure to hold a date
     */
    struct Date
    {
    unsigned spare  : 6;
    unsigned day    : 6;
    unsigned month  : 4;
    unsigned year   : 16;
		// NULL column value = 0xFFFFFFFE
        Date( )   { year = 0xFFFF; month = 0xF; day = 0x3F; spare = 0x3E;}
    };
    /** @brief a structure to hold a datetime
     */
    struct dateTime
    {
    unsigned msecond : 20;
    unsigned second  : 6;
    unsigned minute  : 6;
    unsigned hour    : 6;
    unsigned day     : 6;
    unsigned month   : 4;
    unsigned year    : 16;
		// NULL column value = 0xFFFFFFFFFFFFFFFE
        dateTime( )   { year = 0xFFFF; month = 0xF; day = 0x3F; hour = 0x3F; minute = 0x3F; second = 0x3F;
			msecond = 0xFFFFE; }
    };
	
    /** @brief ctor
     */ 
    DMLPackageProcessor(BRM::DBRM* aDbrm) : fEC(0), DMLLoggingId(21), fRollbackPending(false), fDebugLevel(NONE)
	{
		try {
		fWEClient = new WriteEngine::WEClients(WriteEngine::WEClients::DMLPROC);
		//std::cout << "In DMLPackageProcessor constructor " << this << std::endl;
		fPMCount = fWEClient->getPmCount();
		}
		catch (...)
		{
			std::cout << "Cannot make connection to WES" << std::endl;
		}
		
		oam::OamCache * oamCache = oam::OamCache::makeOamCache();
		fDbRootPMMap = oamCache->getDBRootToPMMap();
		fDbrm = aDbrm;
		fExeMgr =  new execplan::ClientRotator(1, "ExeMgr"); 
		fExeMgr->connect(0.005);
	}



    /** @brief destructor
     */
    EXPORT virtual ~DMLPackageProcessor();

    /** @brief Is it required to debug
     */
    inline const bool isDebug( const DebugLevel level ) const { return level <= fDebugLevel; }

    /**
     * @brief Get debug level
     */
    inline const DebugLevel getDebugLevel() const { return fDebugLevel; }
    //int rollBackTransaction(u_int64_t uniqueId, u_int32_t txnID, u_int32_t sessionID, std::string & errorMsg);
    /**
     * @brief Set debug level
     */
    inline void  setDebugLevel( const DebugLevel level ) { fDebugLevel = level; }

    /**
     * @brief Set the Distributed Engine Comm object
     */
    inline void setEngineComm(joblist::DistributedEngineComm* ec) { fEC = ec; }
    
    /** @brief process the dml package
     *
     * @param cpackage the CalpontDMLPackage to process
     */
    virtual DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage) = 0;
	
	inline void setRM ( joblist::ResourceManager* frm) { fRM = frm; };

	EXPORT int rollBackTransaction(uint64_t uniqueId, BRM::TxnID txnID, uint32_t sessionID, std::string & errorMsg);
	
	EXPORT int rollBackBatchAutoOnTransaction(u_int64_t uniqueId, BRM::TxnID txnID, u_int32_t sessionID, const u_int32_t tableOid, std::string & errorMsg);
    /**
     * @brief convert a columns data, represnted as a string, to it's native
     * data type
     *
     * @param type the columns database type
     * @param data the columns string representation of it's data
     */
    //static boost::any convertColumnData( execplan::CalpontSystemCatalog::ColType colType,
//                                  const std::string& dataOrig );
    /**
     * @brief convert a columns data from native format to a string
     *
     * @param type the columns database type
     * @param data the columns string representation of it's data
     */
    //static std::string dateToString( int  datevalue );         
    /** @brief validate numerical string
     *
     * @param result the result structure
     * @param data
     */

    //static bool  numer_value( std::string data );
    /** @brief check whether the given year is leap year
     *
     * @param year
     */
    //static bool isLeapYear ( int year);

    /** @brief check whether the given date valid
     *
     * @param year, month, day
     */
    //static bool isDateValid ( int day, int month, int year);

    /** @brief check whether the given datetime valid
     *
     * @param hour, minute, second, microSecond
     */
    //static bool isDateTimeValid ( int hour, int minute, int second, int microSecond);

    /** @brief convert month from string to integer
     *
     * @param month
     */
    //static int convertMonth (std::string month);

    /** @brief tokenize date or datetime string
     *
     * @param data
     */
    //static void tokenTime (std::string data, std::vector<std::string>& dataList);

    /** @brief Access the rollback pending flag
     */
    bool getRollbackPending() {return fRollbackPending;}

    /** @brief Set the rollback pending flag
     */
    void setRollbackPending(bool rollback) {fRollbackPending = rollback;}

protected:
    /** @brief update the indexes on the target table
     *
     * @param schema the schema name
     * @param table  the table name
     * @param rows the list of rows
     * @param result the result structure
     * @param updateFlag 0: delete, 1: update, 2 insert
     * @param colNameList the updated column names, only valid for update SQL statement
     */
    bool updateIndexes( u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const std::string& schema,
                        const std::string& table,const dmlpackage::RowList& rows, DMLResult& result,
                        std::vector<WriteEngine::RID> ridList,
                        WriteEngine::ColValueList& colValuesList, std::vector<std::string>& colNameList, const char updateFlag,
                        std::vector<dicStrValues>& dicStrValCols );

    /** @brief delete the indexes on the target table
     *
     * @param schema the schema name
     * @param table  the table name
     * @param rows the list of rows
     * @param result the result structure
     */
    bool deleteIndexes( const std::string& schema, const std::string& table,
                        const dmlpackage::RowList& rows, DMLResult& result );

    /** @brief validate that none of the columns in the supplied row(s) violate
     * any defined constraints
     *
     * @param schema the schema name
     * @param table  the table name
     * @param rows the lists of rows to check column constraints on
     * @param result the result structure
     */
    bool violatesConstraints( u_int32_t sessionID, const std::string& schema, const std::string& table,
                              const dmlpackage::RowList& rows, DMLResult& result );

    /**	@brief validate that the column does not violate a unique constraint
     *
     * @param column the column to validate
     * @param result the result structure
     */
    bool violatesUniqueConstraint( const dmlpackage::RowList& rows,
        const execplan::CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        unsigned int sessionID,
        DMLResult& result);
        
    /** @brief validate that the column does not violate a check constraint
     *
     * @param column the column to validate
     * @param result the result structure
     */
    bool violatesCheckConstraint( const dmlpackage::RowList& rows,
        const execplan::CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        unsigned int sessionID, unsigned int colOffset,
        DMLResult& result );

    /**	@brief validate that the column does not violate a not null constraint
     *
     *  @param type the columns database type
     *	@param column the column to validate
     *  @param result the result structure
     */
    bool violatesNotNullConstraint(  const dmlpackage::RowList& rows, unsigned int colOffset,
                                     DMLResult& result );
                                     
     /** @brief validate that the column does not violate a reference (foreign key) constraint
     *
     *	@param rows the row set to validate
     *  @param colOffset the offset of this column in the row
     *  @param constraintInfo the column constraint infomation
     *  @param result the result structure
     */
    bool violatesReferenceConstraint(  const dmlpackage::RowList& rows, 
                                       unsigned int colOffset,
                                       const execplan::CalpontSystemCatalog::ConstraintInfo& constraintInfo,  
                                       DMLResult& result ); 
       
    /** @brief validate that non of the rows deleted from this table violate a reference
     *   (foreign key) constraint of the reference table.
     *
     * @param schema the schema name
     * @param table  the table name
     * @param rows the lists of rows to check column constraints on
     * @param result the result structure
     */   
	 bool violatesPKRefConnstraint ( u_int32_t sessionID,
                                     const std::string& schema,
                                     const std::string& table,
                                     const dmlpackage::RowList& rows,
                                     const WriteEngine::ColValueList& oldValueList,
                                     DMLResult& result );
                                 
    bool violatesPKRefConnstraint ( u_int32_t sessionID, 
                                     const std::string& schema, 
                                     const std::string& table,
                                     std::vector<WriteEngine::RID>& rowIDList, 
                                     std::vector<void *>& oldValueList, 
                                     DMLResult& result );                       

     /** @brief validate that the rows deleted does not violate a reference (foreign key) constraint
     *
     *	@param rows the row set to validate
     *  @param colOffset the offset of this column in the row
     *  @param constraintInfo the column constraint infomation
     *  @param result the result structure
     */
    bool violatesReferenceConstraint_PK( const WriteEngine::ColValueList& oldValueList, 
                                          const execplan::CalpontSystemCatalog::ColType& colType, 
                                          unsigned int colOffset,
                                          const execplan::CalpontSystemCatalog::ConstraintInfo& constraintInfo,  
                                          DMLResult& result );  

    bool violatesReferenceConstraint_PK(  std::vector<void *>& oldValueList,
                                          const int totalRows,
                                          const execplan::CalpontSystemCatalog::ColType& colType,
                                          unsigned int colOffset,
                                          const execplan::CalpontSystemCatalog::ConstraintInfo& constraintInfo,
                                          DMLResult& result );

     /** @brief validate that none of the columns in the update row(s) violate
     * any reference constraints of the foreign key table
     *
     * @param schema the schema name
     * @param table  the table name
     * @param rows the lists of rows to check column constraints on
     * @param result the result structure
     */
    bool violatesUpdtRefConstraints( u_int32_t sessionID, 
                                     const std::string& schema, 
                                     const std::string& table,
                                     const dmlpackage::RowList& rows, 
                                     DMLResult& result );


    /** @brief validate that the column does not violate a reference (foreign key) constraint
     *
     *	@param rows the row set to validate
     *  @param colOffset the offset of this column in the row
     *  @param constraintInfo the column constraint infomation
     *  @param result the result structure
     */                                 
    bool violatesReferenceConstraint_updt(  const dmlpackage::RowList& rows, 
                                            unsigned int colOffset,
                                            const execplan::CalpontSystemCatalog::ConstraintInfo& constraintInfo,  
                                            DMLResult& result );            

        
    /** @brief get the column list for the supplied table
     *
     * @param schema the schema name
     * @param table the table name
     * @param colList ColumnList to fill with the columns for the supplied table
     */
    void getColumnsForTable( u_int32_t sessionID, std::string schema, std::string table, dmlpackage::ColumnList& colList );
	
	/** @brief convert absolute rid to relative rid in a segement file
     *
     * @param rid  in:the absolute rid  out:  relative rid in a segement file
     * @param dbRoot,partition, segment   the extent information obtained from rid
     * @param filesPerColumnPartition,extentRows, extentsPerSegmentFile   the extent map parameters
     * @param startDBRoot the dbroot this table starts
     * @param dbrootCnt the number of dbroot in db
     */
    void convertRidToColumn(u_int64_t& rid, unsigned& dbRoot, unsigned& partition, 
                                                    unsigned& segment, unsigned filesPerColumnPartition, 
                                                    unsigned  extentsPerSegmentFile, unsigned extentRows, 
                                                    unsigned startDBRoot, unsigned dbrootCnt,
                                                    const unsigned startPartitionNum );

    inline bool isDictCol ( execplan::CalpontSystemCatalog::ColType colType )
    {
            if (((colType.colDataType == execplan::CalpontSystemCatalog::CHAR) && (colType.colWidth > 8)) 
       || ((colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR) && (colType.colWidth > 7)) 
       || ((colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL) && (colType.precision > 18))
               || (colType.colDataType == execplan::CalpontSystemCatalog::VARBINARY)) 
             {
                    return true;
             }
             else
                    return false;
    }


    /** @brief convert an error code to a string
     *
     * @param   ec in:the error code received
     * @returns error string
     */
    std::string projectTableErrCodeToMsg(uint ec);
    
//    bool validateNextValue(execplan::CalpontSystemCatalog::ColType colType, int64_t value, bool & offByOne);
    
    bool validateVarbinaryVal( std::string & inStr);
    int commitTransaction(u_int64_t uniqueId, BRM::TxnID txnID);
    int commitBatchAutoOnTransaction(u_int64_t uniqueId, BRM::TxnID txnID, const u_int32_t tableOid, std::string & errorMsg);
    int commitBatchAutoOffTransaction(u_int64_t uniqueId, BRM::TxnID txnID, const u_int32_t tableOid, std::string & errorMsg);
    int rollBackBatchAutoOffTransaction(u_int64_t uniqueId, BRM::TxnID txnID, u_int32_t sessionID, const u_int32_t tableOid, std::string & errorMsg);
    int flushDataFiles (int rc, std::map<u_int32_t,u_int32_t> & columnOids, u_int64_t uniqueId, BRM::TxnID txnID, u_int32_t tableOid);
	int endTransaction (uint64_t uniqueId, BRM::TxnID txnID, bool success);
    
    /** @brief the Session Manager interface
     */
    execplan::SessionManager fSessionManager;
    joblist::DistributedEngineComm *fEC;
    joblist::ResourceManager* fRM;
    char* strlower(char* in);
    u_int32_t fSessionID;
    const unsigned DMLLoggingId;
    uint fPMCount;
    WriteEngine::WEClients* fWEClient;
    BRM::DBRM* fDbrm;
    boost::shared_ptr<std::map<int, int> > fDbRootPMMap;
    oam::Oam fOam;
    bool fRollbackPending;         // When set, any derived object should stop what it's doing and cleanup in preparation for a Rollback
	execplan::ClientRotator* fExeMgr;

private:
    
   /** @brief clean beginning and ending glitches and spaces from string
      *
      * @param s string to be cleaned
      */
    void cleanString(std::string& s);

    DebugLevel fDebugLevel;           // internal use debug level

};

/** @brief helper template function to do safe from string to type conversions
 *
 */
template <class T>
bool from_string(T& t,
                 const std::string& s,
                 std::ios_base& (*f)(std::ios_base&))
{
    std::istringstream iss(s);
    return !(iss >> f >> t).fail();
}

}

#undef EXPORT

#endif                                            //DMLPACKAGEPROCESSOR_H
