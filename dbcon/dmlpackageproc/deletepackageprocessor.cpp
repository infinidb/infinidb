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
 *   $Id: deletepackageprocessor.cpp 8796 2012-08-08 14:10:49Z chao $
 *
 *
 ***********************************************************************/

#include <iostream>
using namespace std;
#define DELETEPKGPROC_DLLEXPORT
#include "deletepackageprocessor.h"
#undef DELETEPKGPROC_DLLEXPORT
#include "writeengine.h"
#include "joblistfactory.h"
#include "messagelog.h"
#include "dataconvert.h"
#include "simplecolumn.h"
#include "messagelog.h"
#include "sqllogger.h"
#include "stopwatch.h"
#include "dbrm.h"
#include "idberrorinfo.h"
#include "errorids.h"
#include "rowgroup.h"
#include "bytestream.h"
#include "columnresult.h"

using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace dataconvert;
using namespace joblist;
using namespace BRM;
using namespace rowgroup;
using namespace messageqcpp;

namespace dmlpackageprocessor
{
  //bandListsByExtent bandListsMap;
  DMLPackageProcessor::DMLResult
  DeletePackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
  {
    SUMMARY_INFO("DeletePackageProcessor::processPackage");

    VoidValuesList colOldValuesList;
    DMLResult result;
    result.result = NO_ERROR;
    BRM::TxnID txnid;
    fSessionID = cpackage.get_SessionID();
	//StopWatch timer;
    VERBOSE_INFO("DeletePackageProcessor is processing CalpontDMLPackage ...");
	joblist::SJLP jbl;
    try
    {
      // set-up the transaction
      txnid.id  = cpackage.get_TxnID();
	  txnid.valid = true;

      SQLLogger sqlLogger(cpackage.get_SQLStatement(), DMLLoggingId, fSessionID, txnid.id);

      // get the table object from the package
      DMLTable* tablePtr =  cpackage.get_Table();
      if ( 0 != tablePtr )
      {
        // get the row(s) from the table
        //RowList rows = tablePtr->get_RowList();
        /*            if (rows.size() == 0)
                    {
                        SUMMARY_INFO("No row to delete!");
                        return result;
                    }
        */
        std::string schemaName = tablePtr->get_SchemaName();
        std::string tableName = tablePtr->get_TableName();
		//@bug 3440. For compressed table, get a table lock
		CalpontSystemCatalog * csc = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
		CalpontSystemCatalog::TableName aTableName;
		CalpontSystemCatalog::OID oid;
		CalpontSystemCatalog::ColType colType;
		aTableName.table = tableName;
		aTableName.schema = schemaName;
		CalpontSystemCatalog::RIDList colRidList = csc->columnRIDs (aTableName, true);
		bool isCompressedTable = false;
		for (unsigned int j = 0; j < colRidList.size(); j++)
		{
			oid = colRidList[j].objnum;
			colType = csc->colType(oid);

			if (colType.compressionType != 0)
			{
				isCompressedTable = true;
				break;
			}
		}

		if (isCompressedTable) //check table lock
		{
			execplan::CalpontSystemCatalog::ROPair roPair;
			roPair = csc->tableRID(aTableName);

			u_int32_t  processID = 0;
			std::string  processName("DMLProc");
			int rc = 0;
			int i = 0;
			rc = fSessionManager.setTableLock(roPair.objnum, fSessionID, processID, processName, true);

			if ( rc  == BRM::ERR_TABLE_LOCKED_ALREADY )
			{
				int waitPeriod = 10;
				int sleepTime = 100; // sleep 100 milliseconds between checks
				int numTries = 10;  // try 10 times per second
				waitPeriod = Config::getWaitPeriod();
            	numTries = 	waitPeriod * 10;
				struct timespec rm_ts;

				rm_ts.tv_sec = sleepTime/1000;
				rm_ts.tv_nsec = sleepTime%1000 *1000000;

            	for (; i < numTries; i++)
            	{
#ifdef _MSC_VER
					Sleep(rm_ts.tv_sec * 1000);
#else
					struct timespec abs_ts;
            		do
					{
						abs_ts.tv_sec = rm_ts.tv_sec;
						abs_ts.tv_nsec = rm_ts.tv_nsec;
					}
					while(nanosleep(&abs_ts,&rm_ts) < 0);
#endif

					rc = fSessionManager.setTableLock(roPair.objnum, fSessionID, processID, processName, true);

					if (rc == 0)
						break;
				}

				if (i >= numTries) //error out
				{
					result.result = UPDATE_ERROR;
					result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_TABLE_LOCKED));
					return result;
				}

			}
			else if ( rc  == BRM::ERR_FAILURE)
			{
				logging::Message::Args args;
				logging::Message message(1);
				args.add("Delete Failed due to BRM failure");
				result.result = DELETE_ERROR;
				result.message = message;
				return result;
			}

		}

	std::vector<dicStrValues> dicStrValCols;
	std::vector<WriteEngine::RIDList> rowIDLists;

	bool done = false;
	bool firstCall = true;
	ridsListsByExtent ridsListsMap;
	while (! done)
        {

        // Add all rows if there is no filter supplied
		//timer.start("fixUpRows");
        done = fixUpRows( cpackage.get_SessionID(), cpackage, schemaName, tableName, dicStrValCols, firstCall, jbl, ridsListsMap, result );
		if ( result.result != 0 )
			throw std::runtime_error(result.message.msg());
		//timer.stop("fixUpRows");
		std::vector<std::string> colNameList;
		//timer.start("deleteRows");
	 if ( deleteRows( cpackage.get_SessionID(), txnid.id, schemaName, tableName, rowIDLists, colOldValuesList, result, ridsListsMap ) )
        {
			//timer.stop("deleteRows");
			for ( unsigned i = 0; i < rowIDLists.size(); i++ )
			{
				result.rowCount += rowIDLists[i].size();
			}
        }
        else
        {
			ostringstream oss;
			oss << "Deletion of one or more rows failed. " << result.message.msg();
			SUMMARY_INFO(oss.str());
			throw std::runtime_error(oss.str());
        }

	rowIDLists.clear();
	clearVoidValuesList(colOldValuesList);
      }

      }

      // Log the DML statement.
      logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_SQLStatement(), cpackage.get_SchemaName());
    }
    catch (exception& ex)
    {
      cerr << "DeletePackageProcessor::processPackage: " << ex.what() << endl;
      logging::Message::Args args;
      logging::Message message(2);
      args.add("Delete Failed: ");
      args.add( ex.what() );
      message.format( args );
	  if ( result.result != VB_OVERFLOW_ERROR )
	  {
		result.result = DELETE_ERROR;
		result.message = message;
	  }

    }
    catch (...)
    {
      cerr << "DeletePackageProcessor::processPackage: caught unknown exception!" << endl;
      logging::Message::Args args;
      logging::Message message(6);
      args.add( "Delete Failed: ");
      args.add( "encountered unknown exception" );
      args.add(result.message.msg());
      args.add("");
      message.format( args );

      result.result = DELETE_ERROR;
      result.message = message;
    }
	//timer.finish();
	//@Bug 1886,2870 Flush VM cache only once per statement.
	//WriteEngineWrapper writeEngine;
	fWriteEngine.flushVMCache();
	std::map<u_int32_t,u_int32_t> oids;
	//@Bug 4563. Always flush
	fWriteEngine.flushDataFiles(0, oids);

    VERBOSE_INFO("Finished Processing Delete DML Package");
    return result;
  }


void DeletePackageProcessor::clearVoidValuesList(VoidValuesList& valuesList)
{
    int nCols = valuesList.size();
    for(int i=0; i < nCols; i++)
    {
     	if( valuesList[i]!=NULL ) free(valuesList[i]);
    }
    valuesList.clear();
}



  bool DeletePackageProcessor::deleteRows(u_int32_t sessionID, CalpontSystemCatalog::SCN txnID, const std::string& schema,
					  const std::string& table,
                                          std::vector<WriteEngine::RIDList>& rowIDLists,
                                          std::vector<void *>& colOldValuesList,
                                          DMLResult& result,
										  ridsListsByExtent& ridsListsMap)
  {
    SUMMARY_INFO("DeletePackageProcessor::deleteRow");

    VERBOSE_INFO("DeletePackageProcessor is deleting Row(s)");

    int retval = NO_ERROR;

    // get the row(s) from the table
    //RowList rows = tablePtr->get_RowList();

    CalpontSystemCatalog::TableColName tableColName;
    tableColName.table = table;
    tableColName.schema = schema;
    CalpontSystemCatalog* systemCatalogPtr =
      CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
    WriteEngine::ColStructList colStructs;
	std::vector<WriteEngine::ColStructList> colExtentsStruct;
    //Build column structure list

      dmlpackage::ColumnList columns;
	 getColumnsForTable(sessionID, schema,table,columns);
      dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();
      while ( column_iterator != columns.end() )
      {
        DMLColumn* columnPtr = *column_iterator;
        tableColName.column  = columnPtr->get_Name();

        CalpontSystemCatalog::ROPair roPair = systemCatalogPtr->columnRID( tableColName );

        CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID( tableColName );

        CalpontSystemCatalog::ColType colType;
        colType = systemCatalogPtr->colType( oid );

        WriteEngine::ColStruct colStruct;
        colStruct.dataOid = roPair.objnum;
        colStruct.tokenFlag = false;
		colStruct.fCompressionType = colType.compressionType;
        if (colType.colWidth > 8)         //token
        {
          colStruct.colWidth = 8;
          colStruct.tokenFlag = true;
        }
        else
        {
          colStruct.colWidth = colType.colWidth;
        }

        colStruct.colDataType = (WriteEngine::ColDataType)colType.colDataType;

        colStructs.push_back( colStruct );

        ++column_iterator;
      }

		//build colExtentsStruct
		ridsListsByExtent::const_iterator itor;
		for ( itor = ridsListsMap.begin(); itor != ridsListsMap.end(); itor++ )
		{

			for ( unsigned j=0; j < colStructs.size(); j++ )
			{
				colStructs[j].fColPartition = (itor->first).partition;
				colStructs[j].fColSegment = (itor->first).segment;
				colStructs[j].fColDbRoot = (itor->first).dbRoot;
			}
			colExtentsStruct.push_back( colStructs );
			rowIDLists.push_back( itor->second );
		}
		long long  rowCount = 0;
		for ( unsigned i = 0; i < rowIDLists.size(); i++ )
		{
			//cout << "row count is " << rowIDLists[i].size() << endl;
			rowCount += rowIDLists[i].size();
		}

		bool rtn = true;
		if ( rowCount == 0 )
			return rtn;
		//Call write engine to delete the row(s)
		//WriteEngineWrapper writeEngine;

    	retval = fWriteEngine.deleteRow( txnID, colExtentsStruct, colOldValuesList, rowIDLists );

    	if (retval != NO_ERROR)
    	{
      		rtn = false;

      		// build the logging message
      		logging::Message::Args args;
      		logging::Message message(9);
      		args.add("Delete failure on ");
      		args.add(tableColName.table);
      		args.add(" table with error: ");
			WErrorCodes ec;
      		args.add( ec.errorString(retval) );
      		message.format( args );

      		if ( retval == ERR_BRM_DEAD_LOCK )
        	{
        		result.result = DEAD_LOCK_ERROR;
				result.message = message;

        	}
			else if ( retval == ERR_BRM_VB_OVERFLOW )
			{
				result.result = VB_OVERFLOW_ERROR;
				result.message = Message(IDBErrorInfo::instance()->errorMsg(ERR_VERSIONBUFFER_OVERFLOW));
			}
        	else
        	{
        		result.result = DELETE_ERROR;
				result.message = message;
        	}
			logging::LoggingID lid(21);
			logging::MessageLog ml(lid);
			ml.logCriticalMessage( message );
    	}
	else
	{
    	VERBOSE_INFO("DeletePackageProcessor is finished deleting Row(s)");
    }
    return rtn;
  }

  bool DeletePackageProcessor::fixUpRows(u_int32_t sessionID, dmlpackage::CalpontDMLPackage& cpackage, const std::string& schema, const std::string& table,
				std::vector<dicStrValues>& dicStrValCols, bool & firstCall, joblist::SJLP & jbl, ridsListsByExtent& ridsListsMap, DMLResult& result)
  {
    dmlpackage::Row* rowPtr = new dmlpackage::Row();
    dmlpackage::ColumnList colList;
	WriteEngine::RIDList rowIDList;
    getColumnsForTable(sessionID, schema,table,colList);
    CalpontSystemCatalog * csc = CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
	config::Config* cf = config::Config::makeConfig();
	unsigned filesPerColumnPartition = 32;
	unsigned extentsPerSegmentFile = 4;
	unsigned dbrootCnt = 2;
	unsigned extentRows = 0x800000;
	string fpc = cf->getConfig("ExtentMap", "FilesPerColumnPartition");
	if ( fpc.length() != 0 )
		filesPerColumnPartition = cf->uFromText(fpc);
	string epsf = cf->getConfig("ExtentMap", "ExtentsPerSegmentFile");
	if ( epsf.length() != 0 )
		extentsPerSegmentFile = cf->uFromText(epsf);
	string dbct = cf->getConfig("SystemConfig", "DBRootCount");
	if ( dbct.length() != 0 )
		dbrootCnt = cf->uFromText(dbct);

	DBRM brm;
	extentRows = brm.getExtentRows();
	unsigned dbRoot = 0;
	unsigned partition = 0;
	unsigned segment = 0;

    bool done  = true;
	delextentInfo aExtentinfo;
	uint curRow = 0;
	uint64_t batchRows = 0;
	uint64_t rid;
	CalpontSystemCatalog::ColType ct;
	uint16_t startDBRoot;
	uint32_t partitionNum;

	RowGroup rowGroup;
	ridsListsMap.clear();

    if (colList.size())
    {
      rowPtr->set_RowID(0);
      dmlpackage::ColumnList::const_iterator iter = colList.begin();
      while (iter != colList.end())
      {
        DMLColumn* columnPtr = *iter;
        rowPtr->get_ColumnList().push_back(columnPtr);
        ++iter;
      }
    }

	CalpontSystemCatalog::TableName tableName;
	CalpontSystemCatalog::RIDList colRidList;
	CalpontSystemCatalog::OID oid;
	TableBand band;
	tableName.table = table;
	tableName.schema = schema;
	colRidList = csc->columnRIDs (tableName, true);
	vector<ColumnResult*>::const_iterator it;

    if(cpackage.HasFilter())
    {
		// do the following because we have a filter on the delete statement that needs to be reconciled
		execplan::CalpontSelectExecutionPlan *plan = cpackage.get_ExecutionPlan();
		//cout << " plan is : " << *plan << endl;
		execplan::SRCP  retunedCol = plan->columnMap().begin()->second;
		ByteStream bs;
		SimpleColumn* sc =  dynamic_cast<SimpleColumn*>(retunedCol.get());
		if ( sc )
		{
			ct = csc->colType( sc->oid() );
			oid = ct.ddn.dictOID;
		}
		else
		{
			oid = 0;
		}

		if((0 != plan) && (0 != fEC))
		{
			if ( firstCall )
			{
				//plan->traceFlags(1);
				jbl = joblist::JobListFactory::makeJobList(plan, *fRM, true); // true: tuple
				if (jbl->status() == 0)
				{
					TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
					//assert(tjlp);
					rowGroup = tjlp->getOutputRowGroup();
					if ((jbl->putEngineComm(fEC) != 0) || (jbl->doQuery() != 0))
					{
						result.result = JOB_ERROR;
						return true;
					}
				}
				else
				{
					result.result = JOB_ERROR;
					return true;
				}
				firstCall = false;
			}
			else
			{
				TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
				//assert(tjlp);
				rowGroup = tjlp->getOutputRowGroup();
			}

			CalpontSystemCatalog::OID tableOid = csc->tableRID(execplan::make_table(tableName.schema, tableName.table)).objnum;
			vector<ColumnResult*>::const_iterator it;
			brm.getStartExtent( colRidList[0].objnum, startDBRoot, partitionNum );

			rowgroup::Row row;
			rowGroup.initRow(&row);
			do {
				bs.restart();
				//assert( jbl.get());
				curRow = jbl->projectTable(tableOid, bs);
				batchRows += curRow;
				rowGroup.setData((bs.buf()));
				uint err = rowGroup.getStatus();
				if ( err != 0 )
				{
					result.result = UPDATE_ERROR;
					result.message = Message(projectTableErrCodeToMsg(err));
					return true;
				}
				if ( curRow == 0 ) //No more rows to fetch
				{
					jbl.reset();
					bs.reset();
					done = true;
					return done;
				}

				if (rowGroup.getBaseRid() == (uint64_t) (-1 & ~0x1fff))
				{
					continue;  // @bug4247, not valid row ids, may from small side outer
				}

				rowGroup.getRow( 0, &row );
				rid = row.getRid();
				convertRidToColumn ( rid, dbRoot, partition, segment, filesPerColumnPartition,
						extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt, partitionNum );

				aExtentinfo.dbRoot = dbRoot;
				aExtentinfo.partition = partition;
				aExtentinfo.segment = segment;

				uint32_t rowsThisRowgroup = rowGroup.getRowCount();
				for ( unsigned i = 0; i < rowsThisRowgroup; i++ )
				{
					rowGroup.getRow( i, &row );
					rid = row.getRid();
					//cout << "rid is "<< rid << endl;
					convertRidToColumn ( rid, dbRoot, partition, segment, filesPerColumnPartition,
						extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt, partitionNum );
					ridsListsMap[aExtentinfo].push_back( rid );
				}

				if ( batchRows >= fMaxDeleteRows )
				{
					done = false;
					//cout << " sending a batch " << endl;
					return done;
				}

			} while( 0 != curRow );
		}

    }
    else
    {

			/* Here we build a simple execution plan to scan all the columns which has index and at the same time,
			it is dictionary. Other wise just scan the first column of the table with no filter to effectively get back all the rids for the table */
			execplan::CalpontSelectExecutionPlan csep;
			SessionManager sm;
			BRM::TxnID txnID;
			//TableBand band;
			ByteStream bs;

			txnID = sm.getTxnID(sessionID);
			CalpontSystemCatalog::SCN verID;
			verID = sm.verID();

			csep.txnID(txnID.id);
			csep.verID(verID);
			csep.sessionID(cpackage.get_SessionID());

			CalpontSelectExecutionPlan::ReturnedColumnList colList;
			CalpontSelectExecutionPlan::ColumnMap colMap;
			CalpontSelectExecutionPlan::TableList tbList;

			CalpontSystemCatalog::TableAliasName tn = make_aliastable(schema, table, table);
			tbList.push_back(tn);
			csep.tableList(tbList);

			DMLColumn* columnPtr = rowPtr->get_ColumnList().at(0);
			std::string fullSchema = schema+"."+table+"."+columnPtr->get_Name();
			SimpleColumn* sc = new SimpleColumn(fullSchema, cpackage.get_SessionID());
			sc->tableAlias( table );
			execplan::SRCP srcp(sc);
			colList.push_back(srcp);
			colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(fullSchema, srcp));
			oid = sc->oid();
			// @bug 1367
			CalpontSystemCatalog::ColType colType = csc->colType(oid);
			if (colType.colWidth > 8 ||
				(colType.colDataType == CalpontSystemCatalog::VARCHAR && colType.colWidth > 7 ) )
				oid = colType.ddn.dictOID;

			csep.columnMapNonStatic (colMap);
			csep.returnedCols (colList);
			//cout << csep << endl;

			// so now that we have a defined execution plan, go run it and get the ridlist back into rows
			if((0 != fEC))
			{
				if ( firstCall )
				{
					jbl = joblist::JobListFactory::makeJobList(&csep, *fRM, true); // true: tuple
					if (jbl->status() == 0)
					{
						TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
						//assert(tjlp);
						rowGroup = tjlp->getOutputRowGroup();
						if ((jbl->putEngineComm(fEC) != 0) || (jbl->doQuery() != 0))
						{
							//cout << "get error from doQuery " << endl;
							result.result = JOB_ERROR;
							return true;
						}
					}
					else
					{
						result.result = JOB_ERROR;
						return true;
					}
					firstCall = false;

				}
				else
				{
					TupleJobList* tjlp = dynamic_cast<TupleJobList*>(jbl.get());
					//assert(tjlp);
					rowGroup = tjlp->getOutputRowGroup();
				}

				CalpontSystemCatalog::OID tableoid = csc->tableRID(execplan::make_table(tableName.schema, tableName.table)).objnum;

				//vector<ColumnResult*>::const_iterator it;
				brm.getStartExtent( colRidList[0].objnum, startDBRoot, partitionNum );
				rowgroup::Row row;
				rowGroup.initRow(&row);
				do {
					bs.reset();
					//assert( jbl.get());
					curRow = jbl->projectTable(tableoid, bs);
					rowGroup.setData((bs.buf()));
					uint err = rowGroup.getStatus();
					if ( err != 0 )
					{
						result.result = UPDATE_ERROR;
						result.message = Message(projectTableErrCodeToMsg(err));
						return true;
					}
					batchRows += curRow;
					if ( curRow == 0 ) //No more rows to fetch
					{
						jbl.reset();
						bs.reset();
						done = true;
						return done;
					}

					if (rowGroup.getBaseRid() == (uint64_t) (-1 & ~0x1fff))
					{
						continue;  // @bug4247, not valid row ids, may from small side outer
					}

					rowGroup.getRow( 0, &row );
					rid = row.getRid();
					convertRidToColumn ( rid, dbRoot, partition, segment, filesPerColumnPartition,
						extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt, partitionNum );

					aExtentinfo.dbRoot = dbRoot;
					aExtentinfo.partition = partition;
					aExtentinfo.segment = segment;
					uint32_t rowsThisRowgroup = rowGroup.getRowCount();
					for ( unsigned i = 0; i < rowsThisRowgroup; i++ )
					{
						rowGroup.getRow( i, &row );
						rid = row.getRid();
						convertRidToColumn(rid, dbRoot, partition, segment, filesPerColumnPartition,
						extentsPerSegmentFile, extentRows, startDBRoot, dbrootCnt, partitionNum );
						ridsListsMap[aExtentinfo].push_back( rid );
					}

					if ( batchRows >= fMaxDeleteRows )
					{
						done = false;
						return done;
					}
				} while( 0 != curRow );
			}
	    //}
	}

	return done;
  }
} // namespace dmlpackageprocessor
