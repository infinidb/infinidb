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
 *   $Id: ddlindexpopulator.cpp 7455 2011-02-18 12:59:55Z rdempsey $
 *
 *
 ***********************************************************************/
#include <sys/types.h>
#include <sys/ipc.h>

#include "ddlindexpopulator.h"

#include "messagelog.h"
#include "dataconvert.h"
#include "joblist.h"
#include "calpontselectexecutionplan.h"
#include "distributedenginecomm.h"
#include "simplecolumn.h"
#include "resourcemanager.h"
#include "columnresult.h"

#include <boost/any.hpp>
using namespace boost;
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;

using namespace WriteEngine;
using namespace logging;
using namespace resultset;
using namespace joblist;

using namespace std;
using namespace execplan;
using namespace ddlpackage;
using namespace messageqcpp;

namespace ddlpackageprocessor
{

  bool DDLIndexPopulator::populateIndex(DDLPackageProcessor::DDLResult& result)
  {
    if (makeIndexStructs() )
      insertIndex();
    result = fResult;
    return NO_ERROR != fResult.result;
  }


  bool DDLIndexPopulator::makeIndexStructs( )
  {
    CalpontSelectExecutionPlan csep;
    makeCsep(csep);
    ResourceManager rm;
    if (! fEC) 
    {
		fEC = DistributedEngineComm::instance(rm);
		fEC->Open();
    }

    SJLP jbl = joblist::JobListFactory::makeJobList(&csep, rm);

    CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
    csc->identity(CalpontSystemCatalog::EC);

    jbl->putEngineComm(fEC);
    /*
    ResultManager * result = jbl->GetResultManager();
    result->setRunning(1);
    jbl->Execute();	*/
    jbl->doQuery();
    
    CalpontSystemCatalog::TableName tableName;
    tableName.schema = fTable.fSchema;
    tableName.table = fTable.fName;
    
    CalpontSystemCatalog::OID tableOid = (csc->tableRID ( tableName )).objnum;
    CalpontSystemCatalog::NJLSysDataList sysDataList;
    for (;;)
	{
		TableBand band;
		band = jbl->projectTable(tableOid);
		if (band.getRowCount() == 0)
		{
			// No more bands, table is done
			break;
		}
		band.convertToSysDataList(sysDataList, csc);
		break;
	}

    //size_t cnt = fColNames.size(); 
    size_t i = 0;
    vector<ColumnResult*>::const_iterator it;
    vector<int>::const_iterator oid_iter;
    for (it = sysDataList.begin(); it != sysDataList.end(); it++)
    {
		if (isUnique())
	    	fUniqueColResultList.push_back(*it);
	    for ( oid_iter = fOidList.begin(); oid_iter != fOidList.end(); oid_iter++ )
	    {
	    	if ( (*it)->ColumnOID() == *oid_iter )
	    	{
				CalpontSystemCatalog::ColType coltype = makeIdxStruct(*it, fColNames.size(), csc);
				addColumnData(*it, coltype, i);
			}
		}
			i++;
    } 

    return (fIdxValueList.size() && NO_ERROR == fResult.result );		

  }



  void DDLIndexPopulator::makeCsep(CalpontSelectExecutionPlan&  csep)
  {

    csep.sessionID(fSessionID);

    csep.txnID(fTxnID);
    csep.verID(fSessionManager->verID());

    CalpontSelectExecutionPlan::ReturnedColumnList colList;
    CalpontSelectExecutionPlan::ColumnMap colMap;
 	CalpontSystemCatalog::TableColName tableColName;
 	CalpontSystemCatalog::OID oid;
 	tableColName.schema = fTable.fSchema;
 	tableColName.table = fTable.fName;
 	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
    string tableName(fTable.fSchema + "." + fTable.fName + ".");

    ColumnNameList::const_iterator cend = fColNames.end();
    for (ColumnNameList::const_iterator cname = fColNames.begin(); cname != cend; ++cname)
    {
	string fullColName(tableName + *cname);
        SRCP srcp(new SimpleColumn (fullColName, fSessionID));
	colList.push_back(srcp);
	tableColName.column = *cname; 
	oid = csc->lookupOID( tableColName );
	fOidList.push_back( oid );
	colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(fullColName, srcp));
    }
    csep.columnMap (colMap);
    csep.returnedCols (colList);
  }

	   
 CalpontSystemCatalog::ColType DDLIndexPopulator::makeIdxStruct(const ColumnResult* cr, size_t cols, CalpontSystemCatalog* csc )
  {
    IdxStruct idx;
    idx.treeOid = fIdxOID.treeOID;
    idx.listOid = fIdxOID.listOID;
    idx.multiColFlag = cols > 1;
    CalpontSystemCatalog::ColType coltype = csc->colType(cr->ColumnOID());
    idx.idxDataType = static_cast<WriteEngine::ColDataType>(coltype.colDataType);
 
    if (isDictionaryType(coltype) )
    {
	idx.idxWidth = fTOKENSIZE;
	idx.idxType = WR_CHAR;
    }//@bug 410: index sizes are either 1, 4 or 8
    else if (isCharType(coltype))
    {
	if (1 == coltype.colWidth) idx.idxWidth = 1;
	else idx.idxWidth = (coltype.colWidth > 4) ? 8 : 4;
	idx.idxType = WR_CHAR;
    }
    else 
	idx.idxWidth = coltype.colWidth;

    fIdxStructList.push_back(idx);
    return coltype;
  }

  void DDLIndexPopulator::addColumnData(const execplan::ColumnResult* cr, const CalpontSystemCatalog::ColType colType, int added)
  {
    WriteEngine::IdxTupleList  tupleList;
    WriteEngine::IdxTuple tuple; 

    for(int i=0;i < cr->dataCount(); ++i)
    {	
	
	WriteEngine::IdxTuple tuple ; 
	convertColData( cr, i,  colType, tuple);

	if (checkConstraints( tuple, colType, i, added))
	{
	    tupleList.push_back(tuple);
	    if (! added )
	      fRidList.push_back(cr->GetRid(i));
	}
	else
	  break;
    }
    if (tupleList.size())
    	fIdxValueList.push_back(tupleList);
  }



  void DDLIndexPopulator::convertColData(const execplan::ColumnResult* cr, int idx,  const CalpontSystemCatalog::ColType& colType, WriteEngine::IdxTuple& tuple)
  {
    if (isDictionaryType(colType))
    {
/*	tuple.data = tokenizeData ( colType, cr->GetStringData(idx) );*/
/*	tuple.data = tokenizeData ( cr->GetRid(idx) );*/
	tuple.data = convertTokenData(cr->GetStringData(idx));
    }
    else tuple.data = convertData( colType, cr, idx);
  }

  boost::any DDLIndexPopulator::convertTokenData( const std::string& data )
  {
    string strData((size_t)fTOKENSIZE < data.length() ? data.substr(0, fTOKENSIZE) : data);
    return  strData;
 }

#if 0
// Disabled this function as it is currently not used.
// If we decide to use, we should check on the usage of fileop.getFileName().
// With iteration 17, the more common version of this getFileName() takes a
// partition and segment number in addition to an OID.  openColumnFile
// should perhaps be changed to use this updated version of getFileName().
  bool DDLIndexPopulator::openColumnFile(WriteEngine::OID oid)
  {
    FileOp fileOp;
    char fileName[WriteEngine::FILE_NAME_SIZE];
    if (WriteEngine::NO_ERROR == fileOp.getFileName(oid, fileName) )
    {	
	fColumnFile.open(fileName);
	return true;
    }
    else
    {	
	logError("Could not get column file name for data");
	return false;
    }
  }
#endif

// Workaround to get original column token and not "retokenize" the string value
  boost::any DDLIndexPopulator::tokenizeData( WriteEngine::RID rid )
  {
    int64_t byteOffset = rid * fTOKENSIZE;
    ByteStream::byte inbuf[fTOKENSIZE];
    fColumnFile.seekg(byteOffset, ios::beg);
    fColumnFile.read(reinterpret_cast<char*>(inbuf), fTOKENSIZE);
	
    WriteEngine::Token token;
    memcpy(&token, inbuf, fTOKENSIZE);
    return token;
  }


  boost::any DDLIndexPopulator::tokenizeData( const execplan::CalpontSystemCatalog::ColType& colType, const std::string& data )
  {
    WriteEngine::DctnryTuple  dictTuple;

    if ( data.length() > (unsigned int)colType.colWidth )
    {
	logError("Insert value is too large for column");
    }
    else
    {
        WriteEngine::DctnryStruct dictStruct;
	dictStruct.treeOid = colType.ddn.treeOID;
	dictStruct.listOid = colType.ddn.listOID;
	dictStruct.dctnryOid = colType.ddn.dictOID;
	memcpy(dictTuple.sigValue, data.c_str(), data.length());
	dictTuple.sigSize = data.length();
	int error = NO_ERROR;
	if ( NO_ERROR != (error = fWriteEngine->tokenize( fTxnID, dictStruct, dictTuple)) )
	{
	    logError("Tokenization failed", error);
	}
    }
    return dictTuple.token;
  }



  boost::any   DDLIndexPopulator::convertData(const CalpontSystemCatalog::ColType&  colType, const execplan::ColumnResult* cr, int idx )
  {
    uint64_t data = cr->GetData(idx);  
    switch( colType.colDataType )
    {
      case CalpontSystemCatalog::BIT: 
      case execplan::CalpontSystemCatalog::TINYINT:    return  *reinterpret_cast<char*>(&data);
      case execplan::CalpontSystemCatalog::SMALLINT:   return  *reinterpret_cast<short*>(&data);
      case execplan::CalpontSystemCatalog::DATE:	// @bug 375
      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::INT:        return  *reinterpret_cast<int*>(&data);
      case execplan::CalpontSystemCatalog::DATETIME: 	// @bug 375
      case execplan::CalpontSystemCatalog::BIGINT:     return  *reinterpret_cast<long long*>(&data);
      case execplan::CalpontSystemCatalog::DECIMAL:  
      {
	  if (colType.colWidth <= CalpontSystemCatalog::FOUR_BYTE) return  *reinterpret_cast<short*>(&data);

	  else if (colType.colWidth <= 9)              return  *reinterpret_cast<int*>(&data);

	  else                                         return  *reinterpret_cast<long long*>(&data);
       }

      case execplan::CalpontSystemCatalog::FLOAT:      return  *reinterpret_cast<float*>(&data);
      case execplan::CalpontSystemCatalog::DOUBLE:     return  *reinterpret_cast<double*>(&data);
      case execplan::CalpontSystemCatalog::CHAR:      
      case execplan::CalpontSystemCatalog::VARCHAR:   
      {
	  string  strData(cr->GetStringData(idx) );  
	  return  *reinterpret_cast<string*>(&strData);
      }
      default: break;
    }
    logError("Invalid column type");
    throw std::runtime_error("Invalid data");

    return *reinterpret_cast<long long*>(&data);
     
  }


  void DDLIndexPopulator::insertIndex( )
  {
// @bug 359 use bulk load build
	int rc = (1 < fIdxStructList.size()) ?
	(void)0
	:  (void)0;
    if (rc)
      logError("Error inserting index values", rc );

  }

  bool DDLIndexPopulator::isDictionaryType(const CalpontSystemCatalog::ColType& colType)
  {
    return ( (CalpontSystemCatalog::CHAR == colType.colDataType && 8 < colType.colWidth ) 
	     || (CalpontSystemCatalog::VARCHAR == colType.colDataType  &&  7 < colType.colWidth ) 
	     || (CalpontSystemCatalog::DECIMAL == colType.colDataType  &&  18 < colType.precision ));

  }

  bool DDLIndexPopulator::checkConstraints( const IdxTuple& data, const CalpontSystemCatalog::ColType& ctype, int i, int column)
  {

    switch( fConstraint )
    {
      case DDL_INVALID_CONSTRAINT:
	return true;

      case DDL_UNIQUE:
      case DDL_PRIMARY_KEY:
	if ((size_t)column + 1 < fColNames.size() )
		return true;
	return checkUnique( i, ctype );

      case DDL_NOT_NULL:
	return checkNotNull( data, ctype );

      case DDL_CHECK:
	return checkCheck( data, ctype );

      default:
	return true; //?
    }

  }

// Check if the row of data at idx is already in fUniqueColResultList 

  bool DDLIndexPopulator::checkUnique( int idx, const CalpontSystemCatalog::ColType& colType ) 
  {
    if (0 == idx)
	return true;
    //Get row of data as each column result data at idx
    size_t indexSize = fColNames.size();
    vector <uint64_t> rowIntData(indexSize);
    vector <string> rowStrData(indexSize);

    for (size_t i = 0; i < indexSize; ++i) 
    { 
      //if  ( isStringType(fUniqueColResultList[i]->columnType()) )
      if  ( isStringType(colType.colDataType) )
	  rowStrData[i] = fUniqueColResultList[i]->GetStringData(idx);
      else
	  rowIntData[i] = fUniqueColResultList[i]->GetData(idx);
    }
	//check if each value in the idx row is equal to each value in a previous row
	// i is the row; j is the column.
    bool unique = true;
    for (int i = 0; i < idx && unique; ++i)
    {
	bool equal = true;
	for (size_t j = 0; j < indexSize && equal; ++j)
	{
	  if ( isStringType(colType.colDataType) )
	  {
	    equal = fUniqueColResultList[j]->GetStringData(i) == rowStrData[j];
          }
	  else
          {
	    equal = (static_cast<uint64_t>(fUniqueColResultList[j]->GetData(i)) == rowIntData[j]);
          }
	}
	unique = ! equal;
    }
    if (! unique)
    {
	stringstream ss;
    	ss << idx;   
	logError("Unique Constraint violated on row: " + ss.str() );
    }
    return unique;
  }


  bool DDLIndexPopulator::checkNotNull(const IdxTuple& data, const CalpontSystemCatalog::ColType& colType)
  {

    any nullvalue = DDLNullValueForType(colType);
    bool isNull = false;

    switch( colType.colDataType )
    {
      case CalpontSystemCatalog::BIT: 
	break;

      case execplan::CalpontSystemCatalog::TINYINT:
	isNull = any_cast<char>(data.data) == any_cast<char>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::SMALLINT:
	isNull = any_cast<short>(data.data) == any_cast<short>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::MEDINT:
      case execplan::CalpontSystemCatalog::INT:
	isNull = any_cast<int>(data.data) == any_cast<int>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::BIGINT:
	isNull = any_cast<long long>(data.data) == any_cast<long long>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::DECIMAL:
      {
	  if (colType.colWidth <= CalpontSystemCatalog::FOUR_BYTE)
	    isNull = any_cast<short>(data.data) == any_cast<short>(nullvalue);
	  else if (colType.colWidth <= 9)
	    isNull =  any_cast<int>(data.data) == any_cast<int>(nullvalue);
	  else if (colType.colWidth <= 18)
	    isNull =  any_cast<long long>(data.data) == any_cast<long long>(nullvalue);
	  else
	    isNull =  compareToken(any_cast<WriteEngine::Token>(data.data), any_cast<WriteEngine::Token>(nullvalue));
	  break;
       }
      case execplan::CalpontSystemCatalog::FLOAT:
	isNull =  any_cast<float>(data.data) == any_cast<float>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::DOUBLE:
	isNull =  any_cast<double>(data.data) == any_cast<double>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::DATE:
	isNull =  any_cast<int>(data.data) == any_cast<int>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::DATETIME:
	isNull =  any_cast<long long>(data.data) == any_cast<long long>(nullvalue);
	break;

      case execplan::CalpontSystemCatalog::CHAR:
      {
	  if (colType.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
	    isNull =  any_cast<string>(data.data) == any_cast<string>(nullvalue);
	  else if (colType.colWidth == execplan::CalpontSystemCatalog::TWO_BYTE)
	    isNull =  any_cast<string>(data.data) == any_cast<string>(nullvalue); 		
	  else if (colType.colWidth <= execplan::CalpontSystemCatalog::FOUR_BYTE)
	    isNull =  any_cast<string>(data.data) == any_cast<string>(nullvalue);		
	  else
	    isNull =  compareToken(any_cast<WriteEngine::Token>(data.data), any_cast<WriteEngine::Token>(nullvalue));
	  break;

      }
      case execplan::CalpontSystemCatalog::VARCHAR:
      {
	  if (colType.colWidth == execplan::CalpontSystemCatalog::ONE_BYTE)
	    isNull =  any_cast<string>(data.data) == any_cast<string>(nullvalue);
	  else if (colType.colWidth < execplan::CalpontSystemCatalog::FOUR_BYTE)
	    isNull =  any_cast<string>(data.data) == any_cast<string>(nullvalue);
	  else
	    isNull =  compareToken(any_cast<WriteEngine::Token>(data.data), any_cast<WriteEngine::Token>(nullvalue));
	  break;
      }
      default:
	throw std::runtime_error("getNullValueForType: unkown column data type");
    }
    if (isNull)
      logError("Null value not allowed in index");
    return ! isNull; 

  }

  void  DDLIndexPopulator::logError(const string& msg, int error) 
  {

    Message::Args args;
    Message message(9);
    args.add((string)__FILE__ + ": ");
    args.add(msg);
    if (error)
    {
	args.add("Error number: ");
	args.add(error);
    }

    message.format( args );

    fResult.result = DDLPackageProcessor::CREATE_ERROR;
    fResult.message = message;
  }


} //namespace






