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
 *   $Id: commandpackageprocessor.cpp 8434 2012-04-03 18:31:24Z dcathey $
 *
 *
 ***********************************************************************/
#include <iostream>
#include <sstream>
#include <boost/scoped_ptr.hpp>

#define COMMANDPKGPROC_DLLEXPORT
#include "commandpackageprocessor.h"
#undef COMMANDPKGPROC_DLLEXPORT
#include "messagelog.h"
#include "dbrm.h"
#include "sqllogger.h"
#include "autoincrementdata.h"

using namespace std;
using namespace WriteEngine;
using namespace dmlpackage;
using namespace execplan;
using namespace logging;
using namespace boost;
using namespace BRM;

namespace dmlpackageprocessor
{

DMLPackageProcessor::DMLResult
CommandPackageProcessor::processPackage(dmlpackage::CalpontDMLPackage& cpackage)
{
    SUMMARY_INFO("CommandPackageProcessor::processPackage");

    DMLResult result;
    result.result = NO_ERROR;

    VERBOSE_INFO("Processing Command DML Package...");
	std::string stmt = cpackage.get_DMLStatement();
	boost::algorithm::to_upper(stmt);
	trim(stmt);
	fSessionID = cpackage.get_SessionID();
    try
    {
        // set-up the transaction
		if ( (stmt == "COMMIT") || (stmt == "ROLLBACK") )
		{
			BRM::TxnID txnid = fSessionManager.getTxnID(cpackage.get_SessionID());
			SQLLogger sqlLogger(stmt, DMLLoggingId, cpackage.get_SessionID(), txnid.id);
			if ((txnid.valid))
			{
				vector<LBID_t> lbidList;
				scoped_ptr<DBRM> dbrmp(new DBRM());
				dbrmp->getUncommittedExtentLBIDs(static_cast<VER_t>(txnid.id), lbidList);
	
				if (stmt == "COMMIT")
				{
					logging::logCommand(cpackage.get_SessionID(), txnid.id, "COMMIT;");
					
					//update syscolumn for the next value 
					AutoincrementData *autoincrementData = AutoincrementData::makeAutoincrementData(fSessionID);
					AutoincrementData::OIDNextValue  nextValMap = autoincrementData->getOidNextValueMap();
					if (!nextValMap.empty())
					{				
						int weRc = fWriteEngine.updateNextValue (nextValMap,fSessionID);
						//@Bug 3557. Clean cache after commit.
						AutoincrementData::removeAutoincrementData(cpackage.get_SessionID());
						if (weRc != WriteEngine::NO_ERROR)
						{
							WErrorCodes   ec;
							ostringstream oss;
							oss << "updateNextValue failed: " << ec.errorString(weRc);
							throw std::runtime_error(oss.str());
						}
						CalpontSystemCatalog *systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
						systemCatalogPtr->updateColinfoCache(nextValMap);
					}
					vector<LBID_t>::const_iterator iter = lbidList.begin();
					vector<LBID_t>::const_iterator end = lbidList.end();
					CPInfoList_t cpInfos;
					CPInfo aInfo;
					while (iter != end)
					{
						aInfo.firstLbid = *iter;
						aInfo.max = numeric_limits<int64_t>::min();
						aInfo.min = numeric_limits<int64_t>::max();
						aInfo.seqNum = -1;
						cpInfos.push_back(aInfo);
						++iter;
					}
					std::vector<BRM::BulkSetHWMArg> allHwm;
					std::vector<CPInfoMerge>  mergeCPDataArgs;
					int brmRc = dbrmp->bulkSetHWMAndCP(allHwm, cpInfos, mergeCPDataArgs, txnid.id);
					if (brmRc != 0)
					{
						string errMsg;
						BRM::errString(brmRc, errMsg);
						throw std::runtime_error(errMsg);
					}
					fSessionManager.committed( txnid );
				}
				else if (stmt == "ROLLBACK")
				{
					int weRc = fWriteEngine.rollbackTran( txnid.id, cpackage.get_SessionID() );
					logging::logCommand(cpackage.get_SessionID(), txnid.id, "ROLLBACK;");
					AutoincrementData::removeAutoincrementData(cpackage.get_SessionID());
					if (weRc != WriteEngine::NO_ERROR)
					{
						WErrorCodes   ec;
						ostringstream oss;
						oss << "ROLLBACK failed: " << ec.errorString(weRc);
						weRc = dbrmp->setReadOnly(true);
						throw std::runtime_error(oss.str());
					}
					vector<LBID_t>::const_iterator iter = lbidList.begin();
					vector<LBID_t>::const_iterator end = lbidList.end();
					CPInfoList_t cpInfos;
					CPInfo aInfo;
					while (iter != end)
					{
						aInfo.firstLbid = *iter;
						aInfo.max = numeric_limits<int64_t>::min();
						aInfo.min = numeric_limits<int64_t>::max();
						aInfo.seqNum = -1;
						cpInfos.push_back(aInfo);
						++iter;
					}
					std::vector<BRM::BulkSetHWMArg> allHwm;
					std::vector<CPInfoMerge>  mergeCPDataArgs;
					int brmRc = dbrmp->bulkSetHWMAndCP(allHwm, cpInfos, mergeCPDataArgs, 0);
					if (brmRc != 0)
					{
						string errMsg;
						BRM::errString(brmRc, errMsg);
						throw std::runtime_error(errMsg);
					}
					fSessionManager.rolledback( txnid );
				}
			}
		}
		else if (stmt == "CLEANUP")
		{
			execplan::CalpontSystemCatalog::removeCalpontSystemCatalog
				  (cpackage.get_SessionID());
			execplan::CalpontSystemCatalog::removeCalpontSystemCatalog
				  (cpackage.get_SessionID() | 0x80000000);
			AutoincrementData::removeAutoincrementData(cpackage.get_SessionID());
		}
		else if (stmt == "VIEWTABLELOCK")
		{
			CalpontSystemCatalog *systemCatalogPtr =
				CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(fSessionID);
			CalpontSystemCatalog::TableName tableName;
			tableName.schema =  cpackage.get_SchemaName();
			tableName.table =  cpackage.get_TableName();
			execplan::CalpontSystemCatalog::ROPair roPair;
	
			roPair = systemCatalogPtr->tableRID( tableName );
			u_int32_t  processID;
			std::string  processName;
			bool  lockStatus; 
			u_int32_t  sid;
			fSessionManager.getTableLockInfo( roPair.objnum, processID,
				processName, lockStatus, sid );
		
			string DMLProcProcessName("DMLProc");
			ostringstream os;
			if ( lockStatus )
			{
				os <<  " table " << tableName.schema << "." << tableName.table
					<< " is locked by  \n" << "   ProcessName = " << processName
					<< "    ProcessID = " <<  processID ;
//os  << " [sid: " << sid << "]";
				if ((processName == DMLProcProcessName) && (sid == 0))
				{
					os << " (DMLProc startup adopted this lock from "
						"an aborted bulk load)";
				}
				result.tableLockInfo = os.str();
			}
			else
				result.tableLockInfo =  " table " + tableName.schema + "." +
					tableName.table + " is not locked." ;
		}
		else if (stmt == "CLEARTABLELOCK")
		{
			CalpontSystemCatalog *systemCatalogPtr =
				CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
			systemCatalogPtr->identity(CalpontSystemCatalog::EC);
			systemCatalogPtr->sessionID(fSessionID);
			CalpontSystemCatalog::TableName tableName;
			tableName.schema =  cpackage.get_SchemaName();
			tableName.table =  cpackage.get_TableName();
			execplan::CalpontSystemCatalog::ROPair roPair;
			roPair = systemCatalogPtr->tableRID( tableName );
			u_int32_t  processID;
			std::string  processName;
			bool  lockStatus; 
			u_int32_t  sid;
			int rc = fSessionManager.getTableLockInfo( roPair.objnum, processID,
				processName, lockStatus, sid );
			if ( lockStatus )
			{
				DBRM dbrm;
				u_int32_t  processID = ::getpid();
			    std::string  processName = "DMLProc";
				rc = dbrm.updateTableLock( roPair.objnum, processID, processName) ;
				if ( rc != 0 )
				{
					ostringstream os;
					os << " table " << tableName.schema << "." << tableName.table << " is still locked by an active process.";
					result.tableLockInfo = os.str();
				}
				else
				{
					
					std::string rollbackErrMsg;
					const std::string APPLNAME("cleartablelock SQL cmd");
					rc = fWriteEngine.bulkRollback( roPair.objnum,
						tableName.toString(),
						APPLNAME, false, false, rollbackErrMsg );
					if ( rc == 0 )
						result.tableLockInfo = " Lock for table " + tableName.schema + "." + tableName.table + " is cleared." ;
					else
						result.tableLockInfo =  "Lock for table " + tableName.schema + "." + tableName.table + " cannot be cleared.  " + rollbackErrMsg;
				}
			}
			else
				result.tableLockInfo =  " table " + tableName.schema + "." + tableName.table + " is not locked." ;
		}
		else if ( !cpackage.get_Logging())
		{
			BRM::TxnID txnid = fSessionManager.getTxnID(cpackage.get_SessionID());
			logging::logDML(cpackage.get_SessionID(), txnid.id, cpackage.get_DMLStatement()+ ";", cpackage.get_SchemaName());
			SQLLogger sqlLogger(cpackage.get_DMLStatement(), DMLLoggingId, fSessionID, txnid.id);
			//cout << "commandpackageprocessor Logging " << cpackage.get_DMLStatement()+ ";" << endl;
		}
        else
        {
            std::string err = "Unknown command.";
            SUMMARY_INFO(err);
            throw std::runtime_error(err);
        }

    }
	catch ( logging::IDBExcept& noTable) //@Bug 2606 catch no table found exception
	{
		cerr << "CommandPackageProcessor::processPackage: " << noTable.what() << endl;

        result.result = COMMAND_ERROR;
        result.message = Message(noTable.what());
	}
    catch (std::exception& ex)
    {
        cerr << "CommandPackageProcessor::processPackage: " << ex.what() << endl;

        logging::Message::Args args;
        logging::Message message(1);
        args.add( ex.what() );
        args.add("");
        args.add("");
        message.format( args );

        result.result = COMMAND_ERROR;
        result.message = message;
    }
    catch (...)
    {
        cerr << "CommandPackageProcessor::processPackage: caught unknown exception!" << endl;
        logging::Message::Args args;
        logging::Message message(1);
        args.add( "Command Failed: ");
        args.add( "encountered unkown exception" );
        args.add("");
        args.add("");
        message.format( args );

        result.result = COMMAND_ERROR;
        result.message = message;
    }

    VERBOSE_INFO("Finished processing Command DML Package");
    return result;
}

}                                                 // namespace dmlpackageprocessor
