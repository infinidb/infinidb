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

/*
 * $Id: ha_calpont_dml.cpp 9711 2013-07-23 21:01:27Z chao $
 */

#include <string>
#include <iostream>
#include <stack>
#ifdef _MSC_VER
#include <unordered_map>
#include <unordered_set>
#else
#include <tr1/unordered_map>
#include <tr1/unordered_set>
#endif
#include <fstream>
#include <sstream>
#include <cerrno>
#include <cstring>
using namespace std;

#include <boost/shared_ptr.hpp>
using namespace boost;

#include "idb_mysql.h"

#define NEED_CALPONT_INTERFACE
#include "ha_calpont_impl.h"

#include "ha_calpont_impl_if.h"
using namespace cal_impl_if;

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "dataconvert.h"
using namespace dataconvert;

#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "calpontsystemcatalog.h"

#include "resourcemanager.h"
using namespace joblist;
//#include "stopwatch.h"
//using namespace logging;

#include "dbrm.h"

namespace
{
ResourceManager rm;
uint64_t fBatchInsertGroupRows = rm.getRowsPerBatch();
bool useHdfs = rm.useHdfs();
//convenience fcn
inline uint32_t tid2sid(const uint32_t tid)
{
	return execplan::CalpontSystemCatalog::idb_tid2sid(tid);
}

//StopWatch timer;
int buildBuffer(uchar* buf, string& buffer, int& columns, TABLE* table)
{
    char attribute_buffer[1024];
    String attribute(attribute_buffer, sizeof(attribute_buffer),
                     &my_charset_bin);

    std::string cols = " (";
    std::string vals = " values (";
    columns = 0;

    for (Field** field = table->field; *field; field++)
    {
        const char* ptr;
        const char* end_ptr;

        if((*field)->is_null())
            ptr = end_ptr =0;
        else
        {
            bitmap_set_bit(table->read_set, (*field)->field_index);
            (*field)->val_str(&attribute, &attribute);
            ptr = attribute.ptr();
            end_ptr = attribute.length() + ptr;
        }

        if (columns > 0)
        {
            cols.append(",");
            vals.append(",");
        }
        columns++;

        cols.append((*field)->field_name);
        if (ptr == end_ptr)
        {
            vals.append ("NULL");
        }
        else
        {

            if ( (*field)->type() == MYSQL_TYPE_VARCHAR ||
                    /*FIXME: (*field)->type() == MYSQL_TYPE_VARBINARY || */
                    (*field)->type() == MYSQL_TYPE_VAR_STRING ||
                    (*field)->type() == MYSQL_TYPE_STRING || 
                    (*field)->type() == MYSQL_TYPE_DATE ||
                    (*field)->type() == MYSQL_TYPE_DATETIME )
                vals.append("'");
            while (ptr < end_ptr)
            {

                if (*ptr == '\r')
                {
                    ptr++;
                }
                else if (*ptr == '\n')
                {
                    ptr++;
                }
				else if (*ptr == '\'' )
				{
					//@Bug 1820. Replace apostrophe with strange character to pass parser.
					vals += '\252';
					ptr++;
				}
                else
                    vals += *ptr++;
            }
            if ( (*field)->type() == MYSQL_TYPE_VARCHAR ||
                    /*FIXME: (*field)->type() == MYSQL_TYPE_VARBINARY || */
                    (*field)->type() == MYSQL_TYPE_VAR_STRING ||
                    (*field)->type() == MYSQL_TYPE_STRING || 
                    (*field)->type() == MYSQL_TYPE_DATE ||
                    (*field)->type() == MYSQL_TYPE_DATETIME )
                vals.append("'");
        }
    }

    if(columns)
    {
        cols.append(") ");
        vals.append(") ");
        buffer = "INSERT INTO ";
        buffer.append(table->s->table_name.str);
        buffer.append(cols);
        buffer.append(vals);
    }
    return columns;
}

uint32_t buildValueList (TABLE* table, cal_connection_info& ci )
{
	char attribute_buffer[1024];
    String attribute(attribute_buffer, sizeof(attribute_buffer),
                     &my_charset_bin);
	uint32_t size=0;
	int columnPos = 0;
    double dbval;
	for (Field** field = table->field; *field; field++)
    {
        if((*field)->is_null())
        {
          ci.tableValuesMap[columnPos].push_back (""); //currently, empty string is treated as null.
        }
        else
        {
          bitmap_set_bit(table->read_set, (*field)->field_index);
          // @bug 3798 get real value for float/double type
          if ((*field)->result_type() == REAL_RESULT)
          {
            dbval = (*field)->val_real();
            //int maxlen = (*field)->max_display_length();
            const unsigned maxlen = 1024+1+1+1; //1 for leading zero, 1 for dp, 1 for null
            char buf[maxlen];            
            memset(buf, 0, maxlen);
            snprintf(buf, maxlen, "%.1024f", dbval);
            ci.tableValuesMap[columnPos].push_back(buf);
          }
          else
          {
            //fetch different data type
            (*field)->val_str(&attribute, &attribute);
            if (attribute.length() == 0)
            {
              ci.tableValuesMap[columnPos].push_back (""); //currently, empty string is treated as null.
            }
            else
            {
              string val(attribute.ptr(),attribute.length());
              ci.tableValuesMap[columnPos].push_back(val);
            }
	        }
        }
		
		ci.colNameList.push_back((*field)->field_name);

		columnPos++;
    }
	size = ci.tableValuesMap[0].size();
	return size;
}

int ProcessCommandStatement(THD *thd, string& dmlStatement, cal_connection_info& ci, std::string schema="")
{
	int rc = 0;

	ulong sessionID = tid2sid(thd->thread_id);
	
	CalpontDMLPackage* pDMLPackage; 
	//@Bug 2721 and 2722. Log the statement before issuing commit/rollback
	if ( dmlStatement == "LOGGING" )
	{	
		VendorDMLStatement cmdStmt(idb_mysql_query_str(thd), DML_COMMAND, sessionID);
		cmdStmt.set_Logging( false );
		pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
		pDMLPackage->set_Logging( false );
		pDMLPackage->set_SchemaName( schema );
	}
    else
	{
		VendorDMLStatement cmdStmt(dmlStatement, DML_COMMAND, sessionID);
		pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
	}
	
	pDMLPackage->setTableOid (ci.tableOid);
	if (!ci.singleInsert)
	{
		pDMLPackage->set_isBatchInsert(true);
	}
		
	if (!(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))	
		pDMLPackage->set_isAutocommitOn(true);
		
	if (useHdfs)
		pDMLPackage->set_isAutocommitOn(true);
			
    ByteStream bytestream;
    bytestream << static_cast<uint32_t>(sessionID);
    
    pDMLPackage->write(bytestream);
    delete pDMLPackage;
	
    ByteStream::byte b = 0;
	string errormsg;
	ByteStream::octbyte rows;
    try
    {
        ci.dmlProc->write(bytestream);
        bytestream = ci.dmlProc->read();
		if ( bytestream.length() == 0 )
        {
            rc = 1;
			thd->killed = THD::KILL_QUERY;
            thd->main_da.can_overwrite_status = true;

            thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Lost connection to DMLProc [1]");

        }
		else {
        	bytestream >> b;
			bytestream >> rows;
			bytestream >> errormsg;
		}
    }
    catch (runtime_error&)
    {
		rc =1 ;
		thd->killed = THD::KILL_QUERY;
		thd->main_da.can_overwrite_status = true;

        thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Lost connection to DMLProc [2]");
    }
    catch (...)
    {
		rc = 1;
		thd->killed = THD::KILL_QUERY;
		thd->main_da.can_overwrite_status = true;

        thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Caught unknown error");
    }

	if (( b !=0 ) && (!thd->main_da.is_set()))
	{
		rc = 1;
		thd->killed = THD::KILL_QUERY;
		thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR,  errormsg.c_str());
	}
	delete ci.dmlProc;
	ci.dmlProc = NULL;
    return rc;
}

int doProcessInsertValues ( TABLE* table, uint32_t size, cal_connection_info& ci, bool lastBatch = false )
{
		THD *thd = current_thd;
		uint32_t sessionID = tid2sid(thd->thread_id);
		
		int rc = 0;
		
		VendorDMLStatement dmlStmts(idb_mysql_query_str(thd), DML_INSERT, table->s->table_name.str,
                table->s->db.str, size, ci.colNameList.size(), ci.colNameList,
                ci.tableValuesMap, sessionID);

			CalpontDMLPackage* pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(dmlStmts);
		//@Bug 2466 Move the clean up earlier to avoid the second insert in another session to get the data
		ci.tableValuesMap.clear();
		ci.colNameList.clear();
		if (!pDMLPackage)
		{
			rc = -1;
			string emsg("Calpont DML package cannot build. ");
			thd->main_da.can_overwrite_status = true;
			thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, emsg.c_str());
			return rc;
		}
		
		//@Bug 2721 and 2722 log batch insert statement only once in the bebug file.
		if (( ( ci.isLoaddataInfile ) || ((ci.rowsHaveInserted + size) < ci.bulkInsertRows ) ) && (ci.rowsHaveInserted > 0))
		{
			pDMLPackage->set_Logging( false );
			pDMLPackage->set_Logending( false );
		}
		else if (( ( ci.isLoaddataInfile ) || ((ci.rowsHaveInserted + size) < ci.bulkInsertRows ) ) && (ci.rowsHaveInserted == 0))
		{
			pDMLPackage->set_Logging( true );
			pDMLPackage->set_Logending( false );
		}
			
		if ( ci.singleInsert )
		{
			pDMLPackage->set_Logging( true );
			pDMLPackage->set_Logending( true );
		}
		
		if ( !ci.singleInsert )
		{
			pDMLPackage->set_isBatchInsert( true );
		}
		pDMLPackage->setTableOid (ci.tableOid);
		if (lastBatch)
		{
			pDMLPackage->set_Logending( true );
			
		}
		
		if (lastBatch && (ci.rowsHaveInserted>0))
			pDMLPackage->set_Logging( false );
			
		std::string name = table->s->table_name.str;
		pDMLPackage->set_TableName(name);
		name = table->s->db.str;
		pDMLPackage->set_SchemaName(name);
		
		if (thd->lex->sql_command == SQLCOM_INSERT_SELECT)
			pDMLPackage->set_isInsertSelect(true);
			
		//Carry session autocommit info in the pkg to use in DMLProc
		//cout << "Thread options = "  << thd->options << " and  OPTION_NOT_AUTOCOMMIT:OPTION_BEGIN = " << OPTION_NOT_AUTOCOMMIT << ":" << OPTION_BEGIN << endl;
		if (!(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN)))	
		{
			//cout << "autocommit is on" << endl;
			pDMLPackage->set_isAutocommitOn(true);
		}
		else if (useHdfs)
		{
			pDMLPackage->set_isAutocommitOn(true);
		}
		ByteStream bytestream, bytestreamRcv;
		bytestream << sessionID;
   
		pDMLPackage->write(bytestream);
		delete pDMLPackage;
		
		ByteStream::byte b = 0;
		string errormsg;
		ByteStream::octbyte rows;
		try
		{
			ci.dmlProc->write(bytestream);
			bytestreamRcv = ci.dmlProc->read();
			if ( bytestreamRcv.length() == 0 )
			{	
				//check if it is first batch and DMLProc restarted. Only this case, get a new client and resend
				if (ci.rowsHaveInserted == 0)
				{
					delete ci.dmlProc;
					ci.dmlProc = new MessageQueueClient("DMLProc");
					try
					{
						ci.dmlProc->write(bytestream);
						bytestreamRcv = ci.dmlProc->read();
						if ( bytestreamRcv.length() == 0 )
						{	
							rc = -1;
							b=1;
							errormsg = "Lost connection to DMLProc [3]";
						}
						else
						{
							bytestreamRcv >> b;
							bytestreamRcv >> rows;
							bytestreamRcv >> errormsg;
							rc = b;
						}
					}
					catch (runtime_error&)
					{
						rc = -1;
						thd->main_da.can_overwrite_status = true;
						errormsg = "Lost connection to DMLProc [4]";
						b = 1;
					}
				}
			}
			else
			{
				bytestreamRcv >> b;
				bytestreamRcv >> rows;
				bytestreamRcv >> errormsg;
				rc = b;
			}
		}
		catch (std::exception& rex)
		{
			//check if it is first batch and DMLProc restarted. Only this case, get a new client and resend
			if (ci.rowsHaveInserted == 0)
			{
				delete ci.dmlProc;
				ci.dmlProc = new MessageQueueClient("DMLProc");
				try
				{
					ci.dmlProc->write(bytestream);
					bytestreamRcv = ci.dmlProc->read();
					if ( bytestreamRcv.length() == 0 )
					{	
						rc = -1;
						b=1;
						errormsg = string("Lost connection to DMLProc after getting a new client [1:") + rex.what() + "]";
					}
					else
					{
						bytestreamRcv >> b;
						bytestreamRcv >> rows;
						bytestreamRcv >> errormsg;
						rc = b;
					}
				}
				catch (std::exception& rrex)
				{
					rc = -1;
					thd->main_da.can_overwrite_status = true;
					errormsg = string("Lost connection to DMLProc after getting a new client [2:") + rex.what() + " then " + rrex.what() + "]";
					b = 1;
				}	
			}
			else //really lost connection
			{
				rc = -1;
				thd->main_da.can_overwrite_status = true;
				errormsg = string("Lost connection to DMLProc really [1:") + rex.what() + "]";
				b = 1;
			}
		}
		catch (...)
		{
			rc = -1;
			thd->main_da.can_overwrite_status = true;

			errormsg = "Unknown error caught";
			b = 1;
		}
		
		
		if ((b != 0) && (b != dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING))
		{
			thd->main_da.can_overwrite_status = true;

			thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, errormsg.c_str());

		}
		if ( b == dmlpackageprocessor::DMLPackageProcessor::IDBRANGE_WARNING )
		{
			rc = 0;
			push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, 9999, errormsg.c_str());
		}
		
		if ( rc != 0 )
			ci.rc = rc;
			
		if ( b == dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR )
			rc = b;
		
		return rc;
}

}

int ha_calpont_impl_write_last_batch(TABLE* table, cal_connection_info& ci, bool abort)
{
		int rc = 0;
		THD *thd = current_thd;
		std::string command;
		uint32_t size =  ci.tableValuesMap[0].size();
		//@Bug 2468. Add a logging statement command
		command = "COMMIT";
		std::string schema;
		schema = table->s->db.str;
		//@Bug 6112. if no row to be insert and no rows have been inserted, no need to send to DMLProc
		if ((size == 0) && (ci.rowsHaveInserted == 0))
			return rc;
		//@Bug 2715 Check the saved error code.
		//@Bug 4516 always send the last package to allow DMLProc receive all messages from WES
		if (( ci.rc != 0 ) || abort )
		{
			if (abort) //@Bug 5285. abort is different from error, dmlproc only clean up when erroring out
				rc = doProcessInsertValues( table, size , ci, true);		
			
			//@Bug 2722 Log the statement into datamod log
			//@Bug 4605 if error, rollback and no need to check whether the session is autocommit off 
			
			command = "ROLLBACK";
			rc = ProcessCommandStatement ( thd, command, ci, schema );
			rc = ci.rc;
			ci.rc = 0;
			if (size > 0 )
			{
				ci.tableValuesMap.clear();
				ci.colNameList.clear();
			}
			return rc;		
		}
		else
		{
			rc = doProcessInsertValues( table, size , ci, true);
		}
		
		if ( abort )
		{
			rc = 1;
			thd->main_da.can_overwrite_status = true;
			std::string errormsg = "statement is aborted.";
			thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, errormsg.c_str());
		}
		
		if ( rc == dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR  )
			return rc;
		
		//@Bug 4605
		if ( (rc == 0) && !abort && (!(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))))
		{
				ci.rowsHaveInserted += size;
				command = "COMMIT";
				ProcessCommandStatement ( thd, command, ci, schema );
		}
		else if (useHdfs)
		{
			ci.rowsHaveInserted += size;
			command = "COMMIT";
			ProcessCommandStatement ( thd, command, ci, schema );
		}
		else if (( rc != 0) || abort )
		{
			command = "ROLLBACK";
			ProcessCommandStatement ( thd, command, ci, schema );
		}
		else 
		{
			return rc;
		}
		
		return rc;

}

int ha_calpont_impl_write_row_(uchar *buf, TABLE* table, cal_connection_info& ci, ha_rows& rowsInserted)
{
    int rc = 0;
	//timer.start( "buildValueList");
	ci.colNameList.clear();
	THD *thd = current_thd;
	uint32_t size = 0;
	std::string schema;
	schema = table->s->db.str;
	//@Bug 2086 Added syntax check for '\0'
	try
	{
		size = buildValueList ( table, ci );
	}
	catch (runtime_error& rex)
	{
		rc = 1;
		ci.rc = rc; //@Bug 2790 Save the error infomation.
		thd->main_da.can_overwrite_status = true;
		thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, rex.what());
		return rc;
	}
	//timer.stop( "buildValueList");
	if ( ci.singleInsert   // Single insert 
	    || (( ci.bulkInsertRows > 0 ) && (( ( ci.rowsHaveInserted + size) >= ci.bulkInsertRows ) || ( size >= fBatchInsertGroupRows )) ) 
		//Insert with mutilple value case: processed batch by batch. Last batch is sent also.
		|| (( ci.bulkInsertRows == 0 ) && ( size >= fBatchInsertGroupRows ) )  ) // Load data in file is processed batch by batch
	{
		//timer.start( "DMLProc takes");
		//cout <<" sending a batch to DMLProc ... The size is " << size << "  the current bulkInsertRows = " <<  ci.bulkInsertRows << endl;
		//Build dmlpackage
		if (( ci.bulkInsertRows > 0 ) && ( ( ci.rowsHaveInserted + size) >= ci.bulkInsertRows ))
		{
			rc = doProcessInsertValues( table, size, ci, true );
		}
		else
		{
			rc = doProcessInsertValues( table, size, ci );
		}
		
		if ( rc == 0 )
			rowsInserted = size;
		else
			ci.rc = rc;
		//@Bug 2481. The current active transaction needs to be released if autocommit is on 
		//@Bug 2438 Added a check for batchinsert's last batch to send commit if autocommit is on
		std::string command;
		if ( ci.singleInsert  || ( (ci.bulkInsertRows > 0 ) && (( ci.rowsHaveInserted + size) >= ci.bulkInsertRows ) ) )
		{
			if ( thd->killed > 0 )
			{
				command = "ROLLBACK";
				ProcessCommandStatement ( thd, command, ci, schema );
			}
			else if (rc != dmlpackageprocessor::DMLPackageProcessor::ACTIVE_TRANSACTION_ERROR)
			{
				//@Bug 4605
				if ( rc != 0 )
				{
					command = "ROLLBACK";
					ProcessCommandStatement ( thd, command, ci, schema );
				}
				else if (( rc == 0 ) && (!(thd->options & (OPTION_NOT_AUTOCOMMIT | OPTION_BEGIN))))
				{
					command = "COMMIT";
					ProcessCommandStatement ( thd, command, ci, schema );
				}
				else if (useHdfs)
				{
					command = "COMMIT";
					ProcessCommandStatement ( thd, command, ci, schema );
				}
			}
		}
		//timer.stop( "DMLProc takes");
		//timer.finish();
		return rc;

	}
	else
	{
		return rc;
	}	
}

 std::string  ha_calpont_impl_viewtablelock( cal_impl_if::cal_connection_info& ci, execplan::CalpontSystemCatalog::TableName& tablename)
 {
	THD *thd = current_thd;
	ulong sessionID = tid2sid(thd->thread_id);
	CalpontDMLPackage* pDMLPackage; 
	std::string dmlStatement( "VIEWTABLELOCK" );
	VendorDMLStatement cmdStmt(dmlStatement, DML_COMMAND, sessionID);
	pDMLPackage = CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer(cmdStmt);
	pDMLPackage->set_SchemaName (tablename.schema);
	pDMLPackage->set_TableName (tablename.table);
    	
    ByteStream bytestream;
    bytestream << static_cast<uint32_t>(sessionID);
    pDMLPackage->write(bytestream);
    delete pDMLPackage;
	
    ByteStream::byte b = 0;
	ByteStream::octbyte rows;
	std::string errorMsg;
	std::string tableLockInfo;
	//int dmlRowCount = 0;

    try
    {
        ci.dmlProc->write(bytestream);
        bytestream = ci.dmlProc->read();
		if ( bytestream.length() == 0 )
        {
            thd->main_da.can_overwrite_status = true;

            thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Lost connection to DMLProc [5]");

        }
		else
		{
        	bytestream >> b;
			bytestream >> rows;
			bytestream >> errorMsg;
			bytestream >> tableLockInfo;
		}
			
    }
    catch (runtime_error&)
    {
		thd->main_da.can_overwrite_status = true;

        thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Lost connection to DMLProc [6]");
    }
    catch (...)
    {
		thd->main_da.can_overwrite_status = true;

        thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, "Caught unknown error");
    }
	if ( b != 0 )
		tableLockInfo = errorMsg;
	
    return tableLockInfo;
 
 }
 
//------------------------------------------------------------------------------
// Clear the table lock associated with the specified table lock id.
// Any bulk rollback that is pending will be applied before the table
// lock is released.
//------------------------------------------------------------------------------
 std::string  ha_calpont_impl_cleartablelock(
	cal_impl_if::cal_connection_info& ci,
	uint64_t tableLockID)
 {
	execplan::CalpontSystemCatalog::TableName tblName;
	THD *thd        = current_thd;
	ulong sessionID = tid2sid(thd->thread_id);
	std::string tableLockInfo;
	BRM::TableLockInfo lockInfo;

	// Perform preliminary setup.  CalpontDMLPackage expects schema and table
	// name to be provided, so we get the table OID for the specified table
	// lock, and then get the table name for the applicable table OID.
	std::string prelimTask;
	try
	{
		BRM::DBRM brm;
		prelimTask = "getting table locks from BRM.";
		bool getLockInfo = brm.getTableLockInfo(tableLockID, &lockInfo);
		if (!getLockInfo)
		{
			tableLockInfo = "No table lock found for specified table lock ID";
			return tableLockInfo;
		}

		boost::shared_ptr<execplan::CalpontSystemCatalog> csc =
			execplan::CalpontSystemCatalog::makeCalpontSystemCatalog(sessionID);
		csc->identity(execplan::CalpontSystemCatalog::FE);

		prelimTask = "getting table name from system catalog.";
		tblName    = csc->tableName( lockInfo.tableOID );
	}
	catch (std::exception& ex)
	{
		std::string eMsg(ex.what());
		eMsg += " Error ";
		eMsg += prelimTask;

		thd->main_da.can_overwrite_status = true;

		thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, eMsg.c_str());
		return tableLockInfo;
	}
	catch (...)
	{
		std::string eMsg(" Error ");
		eMsg += prelimTask;

		thd->main_da.can_overwrite_status = true;

		thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR, eMsg.c_str());
		return tableLockInfo;
	}

	std::string dmlStatement( "CLEARTABLELOCK" );
	VendorDMLStatement cmdStmt(dmlStatement, DML_COMMAND, sessionID);
	CalpontDMLPackage* pDMLPackage =
		CalpontDMLFactory::makeCalpontDMLPackageFromMysqlBuffer( cmdStmt );
	pDMLPackage->set_SchemaName(tblName.schema);
	pDMLPackage->set_TableName (tblName.table );

	// Table lock ID is passed in the SQL statement attribute
	std::ostringstream lockIDString;
	lockIDString << tableLockID;
	pDMLPackage->set_SQLStatement( lockIDString.str() );

    ByteStream bytestream;
    bytestream << static_cast<uint32_t>(sessionID);
    pDMLPackage->write(bytestream);
    delete pDMLPackage;
	
    ByteStream::byte b = 0;
	ByteStream::octbyte rows;
	std::string errorMsg;

    try
    {
        ci.dmlProc->write(bytestream);
        bytestream = ci.dmlProc->read();
		if ( bytestream.length() == 0 )
        {
            thd->main_da.can_overwrite_status = true;

            thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR,
				"Lost connection to DMLProc [7]");
        }
		else
		{
        	bytestream >> b;
			bytestream >> rows;
			bytestream >> errorMsg;
			bytestream >> tableLockInfo;
		}
			
    }
    catch (runtime_error&)
    {
		thd->main_da.can_overwrite_status = true;

        thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR,
			"Lost connection to DMLProc [8]");
    }
    catch (...)
    {
		thd->main_da.can_overwrite_status = true;

        thd->main_da.set_error_status(thd, HA_ERR_INTERNAL_ERROR,
			"Caught unknown error");
    }
	//@Bug 2606. Send error message back to sql session
	if ( b != 0 )
		tableLockInfo = errorMsg;

    return tableLockInfo;
 }

int ha_calpont_impl_commit_ (handlerton *hton, THD *thd, bool all, cal_connection_info& ci )
{
	int rc = 0;
	if (thd->infinidb_vtable.vtable_state == THD::INFINIDB_ALTER_VTABLE ||
		  thd->infinidb_vtable.vtable_state == THD::INFINIDB_SELECT_VTABLE )
		return rc;
	if (thd->slave_thread) return 0;
	std::string command("COMMIT");
#ifdef INFINIDB_DEBUG
	cout << "COMMIT" << endl;
#endif
	rc = ProcessCommandStatement(thd, command, ci);
	return rc;
}

int ha_calpont_impl_rollback_ (handlerton *hton, THD *thd, bool all, cal_connection_info& ci)
{
	int rc = 0;
#ifdef INFINIDB_DEBUG
	cout << "ROLLBACK" << endl;
#endif
	if (useHdfs)
	{
		string msg = string("Some non-transactional changed tables couldn't be rolled back");
	//	cout << "Some non-transactional changed tables couldn't be rolled back" << endl;
		push_warning(thd, MYSQL_ERROR::WARN_LEVEL_WARN, 1196, msg.c_str());
		return rc;
	}
		
	std::string command("ROLLBACK");
	rc = ProcessCommandStatement(thd, command, ci);
	return rc;
}

int ha_calpont_impl_close_connection_ (handlerton *hton, THD *thd, cal_connection_info& ci )
{
	int rc = 0;
#ifdef INFINIDB_DEBUG
	cout << "Close connection session ID " << thd->thread_id << endl;
#endif
	if ( !ci.dmlProc )
	{
		return rc;
	}
	std::string command("CLEANUP");
	rc = ProcessCommandStatement(thd, command, ci);
	// @bug 1622. remove calpontsystemcatalog and close the socket when session quit.
	// @info when Calpont process a select query, an alter table phase is involved in
	// the vtable design, which will auto start a transaction. when autocommit on (by default), rollback is automically called
	// when session quit. rollback can also be called by user explicitly to rollback
	// a transaction. Under either situation, system catalog cache for this session should 
	// be removed
	return rc;
}

// vim:ts=4 sw=4:

