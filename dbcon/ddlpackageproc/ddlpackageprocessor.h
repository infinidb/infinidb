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
 *   $Id: ddlpackageprocessor.h 9303 2013-03-07 16:07:12Z chao $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef DDLPACKAGEPROCESSOR_H
#define DDLPACKAGEPROCESSOR_H
#include <string>
#include <stdexcept>
#include <sstream>
#include <iostream>
#include <stdint.h>

#include <boost/any.hpp>
#include <boost/tuple/tuple.hpp>

#include "calpontsystemcatalog.h"
#include "objectidmanager.h"
#include "sessionmanager.h"
#include "brmtypes.h"
#include "ddlpkg.h"
#include "messageobj.h"
#include "we_type.h"
#include "we_define.h"
#include "writeengine.h"
#include "columnresult.h"
#include "we_clients.h"
#include "liboamcpp.h"
#if defined(_MSC_VER) && defined(DDLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif
//#define IDB_DDL_DEBUG
namespace ddlpackageprocessor
{

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

/** @brief base class that defines the common interface and
 * implementation of a DDLPacakageProcessor
 */
class DDLPackageProcessor
{

public:

    /** @brief Result code
     */
    enum ResultCode { NO_ERROR, CREATE_ERROR,  ALTER_ERROR, DROP_ERROR, TRUNC_ERROR,
                      TOKENIZATION_ERROR, NOT_ACCEPTING_PACKAGES, PK_NOTNULL_ERROR, WARNING, USER_ERROR, NETWORK_ERROR, PARTITION_WARNING,
                      WARN_NO_PARTITION};

    enum DebugLevel {                          /** @brief Debug level type enumeration */
        NONE                    = 0,           /** @brief No debug info */
        SUMMARY                 = 1,           /** @brief Summary level debug info */
        DETAIL                  = 2,           /** @brief A little detail debug info */
        VERBOSE                 = 3,           /** @brief Detailed debug info */
    };

	enum LogFileType { DROPTABLE_LOG, DROPPART_LOG, TRUNCATE_LOG};
	typedef std::vector<execplan::CalpontSystemCatalog::OID> OidList;
	typedef std::set<BRM::LogicalPartition> PartitionNums;
	struct LogInfo 
	{
		LogFileType fileType;
		OidList oids;
		PartitionNums partitionNums;
	};
	typedef std::map<execplan::CalpontSystemCatalog::OID, LogInfo> TableLogInfo;
	
    /** @brief the result of dml operations
     */
    struct DDLResult
    {
        /** @brief the result code
         */
        ResultCode result;
        /** @brief the error message if result != NO_ERROR
         */
        logging::Message message;
    };

    /** @brief a structure to hold ddlcolumn attributes
     */
    struct DDLColumn
    {
        execplan::CalpontSystemCatalog::OID oid;
        execplan::CalpontSystemCatalog::ColType colType;
        execplan:: CalpontSystemCatalog::TableColName tableColName;
    };

    /** @brief a list of DDLColumns
     */
    typedef std::vector<DDLColumn> ColumnList;

    /** @brief a strcuture to hold index object
     */
    struct IndexOID
    {
        int listOID;
        int treeOID;
    };

    /** @brief a vector of index object ids
     */
    typedef std::vector<IndexOID> IndexOIDList;

    /** @brief a structure to hold a dictionary's
      *  object ids
      */
    struct DictOID
    {
        int dictOID;
        int listOID;
        int treeOID;
		int colWidth;
		int compressionType;
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
        EXPORT Date( )   { year = 0xFFFF; month = 0xF; day = 0x3F; spare = 0x3E;}
    };
/*
    struct Date
    {
    int year   : 16;
    int month  : 4;
    int day    : 6;
    int spare  : 6;
        Date( )   { year = 0; month = 0; day = 0; spare = 0;}
    }; */
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
        EXPORT dateTime( )   { year = 0xFFFF; month = 0xF; day = 0x3F; hour = 0x3F; minute = 0x3F; second = 0x3F;
			msecond = 0xFFFFE; }
    };
/*
    struct dateTime
    {
    int year    : 16;
    int month   : 4;
    int day     : 6;
    int hour    : 6;
    int minute  : 6;
    int second  : 6;
    int msecond : 20;
        dateTime( )   { year = 0; month = 0; day = 0; hour = 0; minute = 0; second = 0; msecond = 0; }
    }
    ; */
    /** @brief a vector of dictionary object ids
      */
    typedef std::vector<DictOID> DictionaryOIDList;
    
    /** the type of a list of ColumnResult as returned from getSysData
      */
    typedef std::vector <execplan::ColumnResult*> NJLSysDataVector;
    struct NJLSysDataList
    {
        NJLSysDataVector sysDataVec;
        EXPORT NJLSysDataList(){};
        EXPORT ~NJLSysDataList();
        NJLSysDataVector::const_iterator begin() {return sysDataVec.begin();}
        NJLSysDataVector::const_iterator end() {return sysDataVec.end();}
        void push_back(execplan::ColumnResult* cr) {sysDataVec.push_back(cr);}
        unsigned int size() {return static_cast<unsigned int>(sysDataVec.size());}
		int findColumn(const execplan::CalpontSystemCatalog::OID& columnOID) 
	{
		for(uint i = 0; i < sysDataVec.size(); i++) {
			if(sysDataVec[i]->ColumnOID() == columnOID) {
				return i;
			}
		}
		return -1;
	}
    };
    

    /** @brief constructor
      */
    DDLPackageProcessor(BRM::DBRM* aDbrm) : fStartingColOID(0), fDDLLoggingId(23), fDebugLevel( NONE )
	{
		fWEClient = new WriteEngine::WEClients(WriteEngine::WEClients::DDLPROC);
		fPMCount = fWEClient->getPmCount();
		fDbrm = aDbrm;
		//std::cout << "in DDLPackageProcessor constructor " << this << std::endl;
	}

    /** @brief destructor
      */
    EXPORT virtual ~DDLPackageProcessor();


    /** @brief Is it required to debug
      */
    const bool isDebug( const DebugLevel level ) const { return level <= fDebugLevel; }

    /** @brief Get debug level
      */
    const DebugLevel getDebugLevel() const { return fDebugLevel; }

    /** @brief Set debug level
      */
    void  setDebugLevel( const DebugLevel level ) { fDebugLevel = level; }

    /** @brief Get index oid that was allocated during index creation
      */
    IndexOID getIndexOID() const { return fIdxOID; }

    /** @brief Get starting column oid that was allocated during table
      * creation.
      */
    int getStartingColumnOID() const { return fStartingColOID; }
    
    /** @brief access and mutator of fPKName */
    const std::string PKName() const {return fPKName;}
    void PKName (const std::string PKName) {fPKName = PKName;}
		/**  @brief Flush primproc cache for associated lbids.
     *
     *  @param oidList the list of OIDs for
     *  which the lbids for those files will be removed
     */
    EXPORT void flushPrimprocCache( std::vector<execplan::CalpontSystemCatalog::OID>& oidList );
							
	 /**  @brief remove the physical files
     *
     *  @param txnID the transaction id
     *  @param oidList the list of OIDs for
     *  which the files should be removed
     */
    EXPORT void removeFiles(const uint64_t uniqueId, std::vector<execplan::CalpontSystemCatalog::OID>& oidList);
	
	EXPORT void createFiles(execplan::CalpontSystemCatalog::TableName aTableName, const int useDBRoot, const uint64_t uniqueId, const uint32_t numOids);
	
	/**  @brief remove the physical files for the specified partition
     *
     *  @param oidList the list of OIDs for
     *  which the files should be removed
	 *  @param partition number
     */
    EXPORT void removePartitionFiles(std::vector<execplan::CalpontSystemCatalog::OID>& oidList, 
    	                               const PartitionNums& partitions,
    	                               uint64_t uniqueId);

	/**  @brief remove the extents from extent map
     *
     *  @param txnID the transaction id
     *  @param result the result of the operation
     *  @param oidList the list of OIDs for
     *  which the extents should be removed
     */
    EXPORT void removeExtents(std::vector<execplan::CalpontSystemCatalog::OID>& oidList);


	 /**  @brief create and open log file to log a table information
     *
     *  @param tableOid the oid of the table
	 *  @param tableName the shcema, table name 
     */
    EXPORT void createWriteDropLogFile(execplan::CalpontSystemCatalog::OID tableOid,  
		uint64_t uniqueId, std::vector<execplan::CalpontSystemCatalog::OID>& oidList);

	/**  @brief create and open log file to log a table partition information
     *
     *  @param tableOid the oid of the table
	 *  @param tableName the shcema, table name 
	 *  @param partition the partition number to be dropped
     */
    EXPORT void createWritePartitionLogFile(execplan::CalpontSystemCatalog::OID tableOid, 
    	                                     const PartitionNums& partitionNums, 
											 std::vector<execplan::CalpontSystemCatalog::OID>& oidList,
											 uint64_t uniqueId);

    EXPORT void createOpenTruncateTableLogFile(execplan::CalpontSystemCatalog::OID tableOid, execplan::CalpontSystemCatalog::TableName tableName);
											
	/**  @brief create and open log file to log a truncae table information
     *
     *  @param tableOid the oid of the table
	 *  @param tableName the shcema, table name 
     */
    EXPORT void createWriteTruncateTableLogFile(execplan::CalpontSystemCatalog::OID tableOid, uint64_t uniqueId, std::vector<execplan::CalpontSystemCatalog::OID>& oidList);
	

	/**  @brief delete log file
     * 
     */
    EXPORT void deleteLogFile(LogFileType fileType, execplan::CalpontSystemCatalog::OID tableOid, uint64_t uniqueId);
	
	/**  @brief fetch log file infomation
     * 
     */
    EXPORT void fetchLogFile(TableLogInfo & tableLogInfos, uint64_t uniqueId);

	BRM::TxnID fTxnid;
	
protected:
    /** @brief get a list of DDLColumns for the given schema.table
     *
     * @param schema the schema the table belongs to
     * @param table the table name
     * @param colList will contain the list of columns on return
     */
    EXPORT void getColumnsForTable( uint32_t sessionID, std::string schema,std::string table,
                            ColumnList& colList );

    /** @brief convert parsed ddl data type to a system catalog data type
     *
     * @param dateType the parsed ddl data type
     */
    execplan::CalpontSystemCatalog::ColDataType convertDataType(int dataType);

    /** @brief get the null representation for the given column type
     *
     * @param colType the column type
     */
    boost::any getNullValueForType(const execplan::CalpontSystemCatalog::ColType& colType);

    /** @brief return a tokenized value for the supplied data value
      *
      * @param result the result of the operation
      * @param colType the column type
      * @param data the value to tokenize
      */
    boost::any tokenizeData(execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                            const execplan::CalpontSystemCatalog::ColType& colType,
                            const boost::any& data );

    /** @brief does the supplied constraint type require an index
      *
      * @param type the constraint type
      */
    bool isIndexConstraint(ddlpackage::DDL_CONSTRAINTS type);

    /** @brief get the char code for the constraint type
      *
      * @param type the constraint type
      */
    char getConstraintCode(ddlpackage::DDL_CONSTRAINTS type);

    /** @brief get the column refrences for the given table constraint
      *
      * @param tableConstraint the table constraint
      * @param columns on return will contain the list of columns
      */
    void getColumnReferences(ddlpackage::TableConstraintDef& tableConstraint,
                             ddlpackage::ColumnNameList& columns);

    /** @brief build a table constraint name
     *
     * @param schema the schema the table belongs to
     * @param table the name of the table
     * @param constraintNumber the constraint number
     * @param type the constraint type 
     */
    std::string buildTableConstraintName(int oid,
                                         ddlpackage::DDL_CONSTRAINTS type);

    /** @brief build a column constraint name
      *
      * @param schema the schema the table belongs to
      * @param table the name of the table
      * @param column the column name
      * @param type the constraint type 
      */
    std::string buildColumnConstraintName(const std::string& schema,
                                          const std::string& table,
                                          const std::string& column,
                                          ddlpackage::DDL_CONSTRAINTS type);


    /** @brief write the tables meta data to the SYSTABLE table
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param tableDef the table definition
     */
    void writeSysTableMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                               ddlpackage::TableDef& tableDef, uint32_t tableWithAutoi=0);

    /** @brief write the table columns meta data to the SYSCOLUMN table
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param tableDefCols the table columns definition
     * @param qualifiedName the name of catalog, schema, object names
     */
    void writeSysColumnMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                                ddlpackage::ColumnDefList& tableDefCols,
                                ddlpackage::QualifiedName& qualifiedName, int colpos, bool alterFlag=false );

    /** @brief write the table constraint meta data to the SYSCONSTRAINT table
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param constraintList the table constrain list
     * @param qualifiedName the name of catalog, schema, object names
     */
 //   void writeTableSysConstraintMetaData(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
 //                                       ddlpackage::TableConstraintDefList& constraintList, ddlpackage::QualifiedName&
 //                                        qualifiedName, bool alterFlag=false );
    /** @brief write the table constraint meta data to the SYSCONSTRAINTCOL table
      * 
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param constraintList the table constrain list
      * @param qualifiedName the name of catalog, schema, object names
      */
 //   void writeTableSysConstraintColMetaData(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
 //                                           const DDLResult& result, ddlpackage::TableConstraintDefList& constraintList,
 //                                           ddlpackage::QualifiedName& qualifiedName, bool alterFlag=false );

    /** @brief write the column constraint meta data to the SYSCONTRAINT table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param constraintList the table constrain list
      * @param qualifiedName the name of catalog, schema, object names
      */
//    void writeColumnSysConstraintMetaData(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
//                                          ddlpackage::ColumnDefList& tableDefCols,
//                                          ddlpackage::QualifiedName& qualifiedName );

    /** @brief write the column constraint meta data to the SYSCONTRAINTCOL table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableDef the table definition
      */
//    void writeColumnSysConstraintColMetaData(u_int32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
//            const DDLResult& result, ddlpackage::ColumnDefList& tableDefCols,
 //           ddlpackage::QualifiedName& qualifiedName);


    /** @brief write the index meta data to the SYSINDEX table
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param tableDef the table definiton
     * @param consDef the table constraint
     * @param indexName name of the index
     */
    void writeSysIndexMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                               ddlpackage::QualifiedName& qualifiedName,
                               ddlpackage::DDL_CONSTRAINTS type,
                               std::string& indexName, bool multicol, bool alterFlag=false);

    /** @brief write the index meta data to the SYSINDEXCOL table
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param tableDef the table definiton
     * @param constraintCols the list of columns in this index
     * @param indexName name of the index
     */
    void writeSysIndexColMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                                  ddlpackage::QualifiedName& qualifiedName,
                                  ddlpackage::ColumnNameList& constraintCols,
                                  std::string& indexName, bool alterFlag=false);

    /** @brief remove all indexes for the supplied table from the SYSINDEX table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableName the qualified name of the table
      */
    void removeSysIndexMetaDataForTable(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
                                        DDLResult& result, ddlpackage::QualifiedName& tableName);

    /** @brief remove all index columns for the supplied table from the SYSINDEXCOL table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableName the qualified name of the table
      */
    void removeSysIndexColMetaDataForTable(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
                                           DDLResult& result, ddlpackage::QualifiedName& tableName);

    /** @brief remove an index from the SYSINDEX table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param indexName the qualified name of the index
      */
    void removeSysIndexMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                                ddlpackage::QualifiedName& indexName);

    /** @brief remove index columns from the SYSINDEXCOL table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param indexName the qualified name of the index
      */
    void removeSysIndexColMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                                   ddlpackage::QualifiedName& indexName);

    /** @brief remove the table meta data from the SYSTABLE table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableName the qualified name of the table to remove
      */
    void removeSysTableMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                                ddlpackage::QualifiedName& tableName);

    /** @brief remove the column meta data from the SYSCOLUMN table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableName the qualified name of the table whose columns 
      * are to be removed
      */
    void removeSysColMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                              ddlpackage::QualifiedName& tableName);

    /** @brief remove the column meta data from the SYSCOLUMN table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param columnInfo the qualified name of the column 
      * to be removed
      */
    void removeColSysColMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                              ddlpackage::QualifiedName& columnInfo);
                              
    /**  @brief remove the constraint meta data from the SYSCONSTRAINT table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableName the qualified name of the table whose constraints
      * are to be removed
      */
    void removeSysContraintMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                                    ddlpackage::QualifiedName& tableName);
     
    /**  @brief remove the constraint meta data from the SYSCONSTRAINT table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param indexName the index name to be removed
      */                                
    void removeSysIndexMetaDataForIndex(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
        DDLResult& result,
        execplan::CalpontSystemCatalog::IndexNameList& indexNameList);                                

    /**  @brief remove the column constraint meta data from the SYSCONSTRAINTCOL table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param tableName the qualified name of the table whose column constraints
      * are to be removed
      */
    void removeSysConstraintColMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                                        ddlpackage::QualifiedName& tableName);
      /**  @brief remove the column constraint meta data from the SYSCONSTRAINT table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param constrintNames the names of the constraints
      * are to be removed
      */                                    
    void removeSysContraintMetaDataForConstraint(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID,
        DDLResult& result,
        execplan::CalpontSystemCatalog::IndexNameList& constrintNames);                                    
    /**  @brief remove the column constraint meta data from the SYSCONSTRAINTCOL table
      *
      * @param txnID the transaction id
      * @param result the result of the operation
      * @param columnInfo the qualified name of the column whose constraints
      * are to be removed
      */
    void removeColSysConstraintColMetaData(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                                        ddlpackage::QualifiedName& columnInfo);

    /** @brief create the physical dictionary files
     * 
     * @param txnID the transaction id
     * @param result the result of the operation
     */
    void createDictionaryFiles(execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result, const int useDBRoot);

    /** @brief create the physical column files
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param tableDefCols the table column definition list
     */
    void createColumnFiles(execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                           ddlpackage::ColumnDefList& tableDefCols, const int useDBRoot, const uint32_t partitionNum=0);

    /** @brief create the physical index files
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     */
    void createIndexFiles(execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result);

    /**  @brief update the SYSCOLUMN table
     *
     *  @param txnID the transaction id
     *  @param result the result of the operation
     *  @param ridList the list of OIDs for
     *  which the column file should be removed
     */                           
    void updateSyscolumns( execplan::CalpontSystemCatalog::SCN txnID,
        DDLResult& result, WriteEngine::RIDList& ridList, 
        WriteEngine::ColValueList& colValuesList,
        WriteEngine::ColValueList& colOldValuesList);
        
    /** @brief remove the physical index files
     *
     * @param txnID the transaction id
     * @param result the result of the operation
     * @param idxOIDList the list of OIDs for
     * which the index files should be removed
     */
    void removeIndexFiles(execplan::CalpontSystemCatalog::SCN txnID, DDLResult& result,
                          execplan::CalpontSystemCatalog::IndexOIDList& idxOIDList);


    /** @brief return the OIDs used by the database objects
     *
     * @param ridList the list of column OIDs to return
     * @param dictOIDList the list of dictionary OIDs to return
     */
    void returnOIDs(execplan::CalpontSystemCatalog::RIDList& ridList,
                    execplan::CalpontSystemCatalog::DictOIDList& dictOIDList);
   
    /**
      * @brief convert a columns data, represnted as a string, to it's native
      * data type
      *
      * @param type the columns database type
      * @param data the columns string representation of it's data
      */
    //boost::any convertColumnData( execplan::CalpontSystemCatalog::ColType colType,
    //                             const std::string& data );

    /**
      * @brief Find the given column in the given system catalog table and
      * return it as a DDLColumn object
      *
      * @param systableName the table to find the column in
      * @param colName the name of the column to find in the table
      * @param sysCol on success the returned sysCol object
      */
    void findColumnData(uint32_t sessionID, execplan::CalpontSystemCatalog::TableName& systableName,
                        const std::string& colName, DDLColumn& sysCol );

    /** @brief remove the supplied row from the supplied system catalog table
      *
      * @param result the result of the operation
      * @param sysCatalogTableName the qualified name of the system catalog table
      * @param rid the id of the row to remove
      */
    void removeRowFromSysCatalog(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                                 ddlpackage::QualifiedName& sysCatalogTableName, WriteEngine::RID& rid);
    
    /** @brief validate reference constraint for altering existing table
      *
      * @param sessionID session ID
      * @param result the result of the operation
      * @param tcn the column which has the foreign key constraint
      * @param refIndexName the index name of the referenced primary key constraint
      * @return true if violation
      */    
    bool referenceConstraintViolation(uint32_t sessionID, 
                                      DDLResult& result,
                                      execplan::CalpontSystemCatalog::TableColName tcn, 
                                      execplan::CalpontSystemCatalog::IndexName refIndexName);
    
    /** @brief validate PK constraint (not null part) for altering existing table
      *
      * @param sessionID session ID
      * @param result the result of the operation
      * @param qualifiedName schema.table name
      * @param constraintCols the columns associated with the primary key
      * @return true if violation
      */
    bool PKConstraintViolation(uint32_t sessionID, 
                               DDLResult& result,
                               ddlpackage::QualifiedName& qualifiedName,
                               ddlpackage::ColumnNameList& constraintCols);
    /** @brief validate check constraint for altering existing table
      *
      * @param sessionID session ID
      * @param result the result of the operation
      * @param qualifiedName schema.table name
      * @param checkConstraint the constraint text string
      * @return true if violation
      */                           
    bool checkConstraintViolation(uint32_t sessionID, 
                                                   DDLResult& result,
                                                   ddlpackage::QualifiedName& qualifiedName,
                                                   std::string& checkConstraint);
                               
    
    /** @brief remove the supplied rows from the supplied system catalog table
      *
      * @param result the result of the operation
      * @param sysCatalogTableName the qualified name of the system catalog table
      * @param colRidList the list of row ids to remove
      */
    void removeRowsFromSysCatalog(uint32_t sessionID, execplan::CalpontSystemCatalog::SCN txnID, const DDLResult& result,
                                  ddlpackage::QualifiedName& sysCatalogTableName,
                                  execplan::CalpontSystemCatalog::RIDList& colRidList);


    WriteEngine::WriteEngineWrapper fWriteEngine;
	
	BRM::DBRM* fDbrm;
   
    execplan::SessionManager fSessionManager;
	uint fPMCount;
	WriteEngine::WEClients* fWEClient;


    IndexOIDList fIndexOIDList;
    DictionaryOIDList fDictionaryOIDList;

    // store oids used during table and index creation
    // for external reference
    int fTableOID;
    int fStartingColOID;
    int fColumnNum;
    IndexOID fIdxOID;
	std::ofstream	       fDDLLogFile; 
	std::string            fDDLLogFileName;
	
    std::string fPKName;    // primary key name supplied by Oracle. Oracle will issue
                            // two separate DDL statements for a create table with primary
                            // key DDL, with the 1st one being create index and the 2nd
                            // one being create table. This PK name will be stored here
                            // when creatindexprocessor gets the create index statement. This
                            // is to make sure Calpont use the same system primary key name as Oracle
    unsigned const fDDLLoggingId;
	
	//std::ofstream	       fDDLLogFile; 
	//std::string            fDDLLogFileName;
    
	/** @brief convert absolute rid to relative rid in a segement file
     *
     * @param rid  in:the absolute rid  out:  relative rid in a segement file
     * @param dbRoot,partition, segment   the extent information obtained from rid
     * @param filesPerColumnPartition,extentRows, extentsPerSegmentFile   the extent map parameters
	 * @param startDBRoot the dbroot this table starts
	 * @param dbrootCnt the number of dbroot in db
	 */
	void convertRidToColumn(uint64_t& rid, unsigned& dbRoot, unsigned& partition, 
							unsigned& segment, unsigned filesPerColumnPartition, 
							unsigned  extentsPerSegmentFile, unsigned extentRows, 
							unsigned startDBRoot, unsigned dbrootCnt);
							
	int rollBackTransaction(uint64_t uniqueId, BRM::TxnID txnID, uint32_t sessionID);
	int commitTransaction(uint64_t uniqueId, BRM::TxnID txnID);
					   
private:
   /** @brief clean beginning and ending glitches and spaces from string
      *
      * @param s string to be cleaned
      */
    void cleanString(std::string& s);
	//std::string            fDDLLogFileName;
    DebugLevel fDebugLevel; // internal use debug level
	
	
	
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

#endif //DDLPACKAGEPROCESSOR_H
// vim:ts=4 sw=4:

