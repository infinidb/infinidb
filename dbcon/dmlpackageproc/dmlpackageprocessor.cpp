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
 *   $Id: dmlpackageprocessor.cpp 7720 2011-05-20 14:28:14Z chao $
 *
 *
 ***********************************************************************/
#define DMLPKGPROC_DLLEXPORT
#include "dmlpackageprocessor.h"
#undef DMLPKGPROC_DLLEXPORT
#include <math.h>
using namespace std;
using namespace WriteEngine;
using namespace dmlpackage;
#include "calpontselectexecutionplan.h"
#include "simplecolumn.h"
#include "constantcolumn.h"
#include "simplefilter.h"
#include "constantfilter.h"
#include "columnresult.h"
using namespace execplan;
using namespace logging;
#include "configcpp.h"
using namespace config;
#include "joblistfactory.h"
#include "joblist.h"
#include "distributedenginecomm.h"
using namespace joblist;
#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;
#include <boost/algorithm/string/case_conv.hpp>
using namespace boost::algorithm;
#include <boost/tokenizer.hpp>
#include <boost/date_time/gregorian/gregorian.hpp>
using namespace boost::gregorian;

namespace
{
using namespace execplan;

const SOP opeq(new Operator("="));
const SOP opne(new Operator("<>"));
const SOP opor(new Operator("or"));
const SOP opand(new Operator("and"));
const int MAX_INT = numeric_limits<int32_t>::max();
const short MAX_TINYINT = numeric_limits<int8_t>::max(); //127;
const short MAX_SMALLINT = numeric_limits<int16_t>::max(); //32767;
const long long MAX_BIGINT = numeric_limits<int64_t>::max();//9223372036854775807LL
}

namespace dmlpackageprocessor
{

    DMLPackageProcessor::~DMLPackageProcessor()
        {}

#if 0
    bool DMLPackageProcessor::updateIndexes( u_int32_t sessionID, CalpontSystemCatalog::SCN txnID, const std::string& schema,
        const std::string& table, const dmlpackage::RowList& rows,
        DMLResult& result, std::vector<WriteEngine::RID> ridList,
        WriteEngine::ColValueList& colValuesList, std::vector<std::string>& colNameList, const char updateFlag,
        std::vector<dicStrValues>& dicStrValCols )
    {
        SUMMARY_INFO("DMLPackageProcessor::updateIndexes");

        bool retval = true;

        VERBOSE_INFO("Updating Index(s)...");
		if ( ridList.size() == 0 )
		{
			VERBOSE_INFO("No need to update Index(s)");
			return retval;
		}
        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
	systemCatalogPtr->identity(CalpontSystemCatalog::EC);

        CalpontSystemCatalog::TableColName tableColName;
        CalpontSystemCatalog::TableName tableName;
        tableColName.table = table;
        tableColName.schema = schema;
        tableName.table = table;
        tableName.schema = schema;
        WriteEngineWrapper writeEngine;
        WriteEngine::IdxStructList idxStructList;
        WriteEngine::ColValueList::const_iterator value_iter = colValuesList.begin();
        WriteEngine::ColTupleList::const_iterator tuple_iter;
        WriteEngine::RIDList ridLists;
        std::vector<WriteEngine::IdxTupleList> idxValueList;
        CalpontSystemCatalog::IndexNameList indexNameList = systemCatalogPtr->indexNames( tableName );
        int rc = NO_ERROR;
        //check whether this table has any indexed column
        if ( indexNameList.size() > 0 )
        {
        	CalpontSystemCatalog::IndexNameList::const_iterator indexName_iter = indexNameList.begin();
        	while ( indexName_iter != indexNameList.end() )
        	{
        		CalpontSystemCatalog::IndexName indexName = *indexName_iter;
        		CalpontSystemCatalog::TableColNameList tableColumns = systemCatalogPtr->indexColNames( indexName );
        		CalpontSystemCatalog::TableColNameList::const_iterator colNames_iter = tableColumns.begin();
        		Row* rowPtr = rows.at(0);
        		dmlpackage::ColumnList columns = rowPtr->get_ColumnList();
        		dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();
        
        		CalpontSystemCatalog::IndexOID idxoid =  systemCatalogPtr->lookupIndexNbr( indexName );
        		WriteEngine::IdxStruct idxstruct;
                idxstruct.treeOid = idxoid.objnum;
                idxstruct.listOid = idxoid.listOID;
                WriteEngine::IdxTupleList tupleList;
                vector<std::string>::const_iterator col_iter;
                std::string colValue;
                int dictColCount = 0;
                bool needUpdate = true;
				//check whether the updated columns has index. 
        		while ( colNames_iter != tableColumns.end() )
        		{
        			tableColName = *colNames_iter;
        			if ( colNameList.size() > 0 )
        			{
        				col_iter = colNameList.begin();
        				while ( col_iter != colNameList.end())
        				{
        						
        					if ( *col_iter != tableColName.column )
        					{
        						needUpdate = false;
        						++col_iter;
        						continue;
        					}
        					else
        					{
        						needUpdate = true;
        						++col_iter;
        						continue;
        					}
        				}
        			}
        			else 
        				break;
        			colNames_iter++;
        		}
        		
        		if ( !needUpdate ) //No need do anything about index, go to next
        		{
        			continue;
        		}
        		colNames_iter = tableColumns.begin();
        		if ( tableColumns.size() > 1 ) //multicolumn index
        		{
        			//collect the column structure for each column belong to the index
        			WriteEngine::IdxStructList multiIdxStructList;
        			std::vector<WriteEngine::IdxTupleList> multiIdxValueList;
                    WriteEngine::RIDList multiRidLists;
				
        			while ( colNames_iter != tableColumns.end() )
        			{
        				CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID( tableColName );

                		CalpontSystemCatalog::ColType colType;
                		colType = systemCatalogPtr->colType( oid );               		
                		idxstruct.tokenFlag = false;
                		WriteEngine::IdxTupleList atupleList;
                		int colLength = colType.colWidth;
                		if (((colType.colDataType == execplan::CalpontSystemCatalog::CHAR) && (colType.colWidth > 8)) 
               				|| ((colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR) && (colType.colWidth > 7)) 
               				|| ((colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL) && (colType.precision > 18)))       //token
                		{
                			colLength = 8;
                			if ( updateFlag == 0 ) //delete
                			{
                				dicStrValues aColDict = dicStrValCols[dictColCount];
                				for ( unsigned int k = 0; k < aColDict.size(); k++ )
                				{
                					colValue = aColDict[k].substr(0, 8);               					
                					WriteEngine::IdxTuple aTuple;
                					aTuple.data = colValue;
                    				atupleList.push_back(aTuple); 
                				}
                				dictColCount++;               				
                			}
                			else if ( updateFlag == 2 ) //insert
                			{          			            		               		 	                    	               		
                				const DMLColumn* columnPtr = rowPtr->get_ColumnAt( colType.colPosition );
        						colValue = columnPtr->get_Data();        						
                				WriteEngine::IdxTuple aTuple;
                				aTuple.data = colValue;
                    			atupleList.push_back(aTuple); 
                			}
                			else //update
                			{
                				const DMLColumn* columnPtr = rowPtr->get_ColumnAt( colType.colPosition );
                				if ( columnPtr != 0 )
                				{               					
                					colValue = columnPtr->get_Data();        						
                					WriteEngine::IdxTuple aTuple;
                					aTuple.data = colValue;
                    				atupleList.push_back(aTuple);
                				}
                				else
                				{
                					dicStrValues aColDict = dicStrValCols[dictColCount];
                					for ( unsigned int k = 0; k < aColDict.size(); k++ )
                					{
                						colValue = aColDict[k].substr(0, 8);               					
                						WriteEngine::IdxTuple aTuple;
                						aTuple.data = colValue;
                    					atupleList.push_back(aTuple); 
                					}
                					dictColCount++;  
                				}
                			}
                    	}
                    	else
                    	{
                    		//WriteEngine::IdxTupleList atupleList;
                			WriteEngine::IdxTuple aTuple;
                		
                			value_iter = colValuesList.begin();
                			int i = 0;
                			while ( value_iter != colValuesList.end() )
                			{
                				if ( i == colType.colPosition )
                				{
                					tuple_iter = (*value_iter).begin();
                					while (tuple_iter != (*value_iter).end())
                					{                				
                    					aTuple.data = (*tuple_iter).data;
  	                  					//@bug 410: index char width must be 4 or 8 if actual width between 2 and 7
										if ( (colType.colDataType == execplan::CalpontSystemCatalog::CHAR || colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR ) && 1 < colType.colWidth ) 
										{
											colLength = colType.colWidth <= 4 ? 4 : 8;
											memset(&aTuple.data + colType.colWidth, '\0', colLength - colType.colWidth);
										}			
                  						atupleList.push_back(aTuple);                    			
                    					++tuple_iter;                    			
                					}
                				}
                				++value_iter;
                				++i;
                         	}                 	
                			
                    	}
                		multiIdxValueList.push_back(atupleList);
						idxstruct.idxWidth = colLength;
                		idxstruct.idxDataType = (WriteEngine::ColDataType)colType.colDataType;
                		idxstruct.multiColFlag = true;   
                		multiIdxStructList.push_back(idxstruct);
                        multiRidLists.push_back(ridList[0]);                		         	
                		
        				++colNames_iter; 	
        			}
        			
        			if (updateFlag && multiIdxValueList.size())
        			{
        				//writeEngine.setDebugLevel(WriteEngine::DEBUG_3);
        				rc = NO_ERROR;
            			rc = writeEngine.updateMultiColIndexRec(txnID, multiIdxStructList, multiIdxValueList, ridList);
        			}
        			else if (multiIdxValueList.size())
        			{
        				
        				rc = NO_ERROR;
            			rc = writeEngine.deleteMultiColIndexRec(txnID, multiIdxStructList, multiIdxValueList, ridList);
        			}       			
        		}
        		
        		else
        		{
        			//collect the column structure for the index column
        			while ( colNames_iter != tableColumns.end() )
        			{
        				tableColName = *colNames_iter;
        				if ( colNameList.size() > 0 )
        				{
        					col_iter = colNameList.begin();
        					while ( col_iter != colNameList.end())
        					{
        						if ( *col_iter == tableColName.column )
        						{
        							break;
        						}
        						++col_iter;
        					}
        					if ( col_iter == colNameList.end() )
        					{
        						++colNames_iter;
        						continue;
        					}
        				}
        				CalpontSystemCatalog::OID oid = systemCatalogPtr->lookupOID( tableColName );

                		CalpontSystemCatalog::ColType colType;
                		colType = systemCatalogPtr->colType( oid );               		
                		idxstruct.tokenFlag = false;
                		
						idxstruct.idxWidth = colType.colWidth;
                		idxstruct.idxDataType = (WriteEngine::ColDataType)colType.colDataType;
                		idxstruct.multiColFlag = false;   
 //@bug 410               		idxStructList.push_back(idxstruct); 
                		
                		// Build value list    
                		if (((colType.colDataType == execplan::CalpontSystemCatalog::CHAR) && (colType.colWidth > 8)) 
               				|| ((colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR) && (colType.colWidth > 7)) 
               				|| ((colType.colDataType == execplan::CalpontSystemCatalog::DECIMAL) && (colType.precision > 18)))       //token
                		{  
							//@bug 410
							idxstruct.idxWidth = 8;  
                			WriteEngine::IdxTupleList atupleList;   
                			if ( updateFlag == 0 ) //delete
                			{
                				dicStrValues aColDict = dicStrValCols[dictColCount];
                				for ( unsigned int k = 0; k < aColDict.size(); k++ )
                				{
                					if ( aColDict[k] == "\376\377\377\377\377\377\377\377" )
                					{
                						colValue = "\377\377\377\377\377\377\377\376";
                					}
                					else
                					{
                						colValue = aColDict[k].substr(0, 8);
                					}               					
                					WriteEngine::IdxTuple aTuple;
                					aTuple.data = colValue;
                    				atupleList.push_back(aTuple); 
                				}
                				dictColCount++;               				
                			}
                			else if ( updateFlag == 2 ) //insert
                			{          			            		               		 	                    	               		
                				const DMLColumn* columnPtr = rowPtr->get_ColumnAt( colType.colPosition );
                				if ( columnPtr->get_isnull())
                				{
                					colValue = "\377\377\377\377\377\377\377\376";
                				}
                				else
                				{
        							colValue = columnPtr->get_Data();   
        						}     						
                				WriteEngine::IdxTuple aTuple;
                				aTuple.data = colValue;
                    			atupleList.push_back(aTuple); 
                			}
                			else //update
                			{
                				const DMLColumn* columnPtr = rowPtr->get_ColumnAt( colType.colPosition );
                				if ( columnPtr != 0 )
                				{   
                					if ( columnPtr->get_isnull())
                					{
                						colValue = "\377\377\377\377\377\377\377\376";
                					}
                					else 
                					{           					
                						colValue = (columnPtr->get_Data()).substr(0,8);   
                					}     						
                					WriteEngine::IdxTuple aTuple;
                					aTuple.data = colValue;
                    				atupleList.push_back(aTuple);
                				}
                				else
                				{
                					dicStrValues aColDict = dicStrValCols[dictColCount];
                					for ( unsigned int k = 0; k < aColDict.size(); k++ )
                					{
                						colValue = aColDict[k].substr(0, 8);               					
                						WriteEngine::IdxTuple aTuple;
                						aTuple.data = colValue;
                    					atupleList.push_back(aTuple); 
                					}
                					dictColCount++;  
                				}
                				
                			}
                			              	                  	
                			idxValueList.push_back(atupleList); 
                		}
                		else
                		{
             				value_iter = colValuesList.begin();
                			int j = 0;
                			while ( value_iter != colValuesList.end() )
                			{
                				if ( j == colType.colPosition )
                				{
                					tuple_iter = (*value_iter).begin();
                					while (tuple_iter != (*value_iter).end())
                					{     
                						WriteEngine::IdxTuple atuple;           				
                    					atuple.data = (*tuple_iter).data;
                    					//tupleList.push_back(atuple); 
	                  				//@bug 410: index char width must be 4 or 8 if actual width between 2 and 7
										if ( (colType.colDataType == execplan::CalpontSystemCatalog::CHAR || colType.colDataType == execplan::CalpontSystemCatalog::VARCHAR ) && 1 < colType.colWidth ) 
										{
											idxstruct.idxWidth = colType.colWidth <= 4 ? 4 : 8;
											memset(&atuple.data + colType.colWidth, '\0', idxstruct.idxWidth - colType.colWidth);
										}	
										
                    					tupleList.push_back(atuple);		
                    					++tuple_iter;                    			
                					}
                				}
                				++value_iter;
                				++j;
                         	}                 	                  	
                			idxValueList.push_back(tupleList);
                		}
                		idxStructList.push_back(idxstruct);  //@bug 410, allow readjust width
               			colNames_iter++;    				 	
        			}
        		}
        		
        		++indexName_iter ;
        	}
        	
        	if ((updateFlag != 0 ) && idxValueList.size())
        	{
        		//writeEngine.setDebugLevel(WriteEngine::DEBUG_3);
            	rc = writeEngine.updateIndexRec(txnID, idxStructList, idxValueList, ridList);
        	}
        	else if (idxValueList.size())
        	{
        		//writeEngine.setDebugLevel(WriteEngine::DEBUG_3);
            	rc = writeEngine.deleteIndexRec(txnID, idxStructList, idxValueList, ridList);
        	}       			
        		
        } 
        else
        {
        	VERBOSE_INFO("No need to update Index(s)");
        }
        
        if (rc != NO_ERROR)
        {
            retval = false;
        }

        if ( retval == false )
        {
            logging::Message::Args args;
            logging::Message message(5);
            args.add("index update error: ");
            args.add( tableColName.column );

            message.format( args );

            result.result = INDEX_UPDATE_ERROR;
            result.message = message;
        }

        VERBOSE_INFO("Finished Updating Index(s)");
        return retval;                
    }

    bool DMLPackageProcessor::deleteIndexes( const std::string& schema,
        const std::string& table,
        const dmlpackage::RowList& rows,
        DMLResult& result )
    {
        bool retval = true;

        return retval;
    }

    bool DMLPackageProcessor::violatesConstraints( u_int32_t sessionID, 
            const std::string& schema, 
            const std::string& table,
            const dmlpackage::RowList& rows, 
            DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesCheckConstraint");

        VERBOSE_INFO("Checking Constraints...");

        bool constraintViolation = false;
        
        // only check the first row meta data and validate all rows
        RowList::const_iterator row_iterator = rows.begin();

        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );

        CalpontSystemCatalog::TableColName tableColName;
        tableColName.table = table;
        tableColName.schema = schema;

        Row* rowPtr = *row_iterator;

	    dmlpackage::ColumnList columns = rowPtr->get_ColumnList();

	    dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();
		unsigned int colOffset = 0;
        while( column_iterator != columns.end() && false == constraintViolation )
        {
            DMLColumn* columnPtr = *column_iterator;

            // see if this column has any constraints
            // no need to get coltype. only need to get constraintType
            tableColName.column  = columnPtr->get_Name();
            CalpontSystemCatalog::IndexNameList indexNameList =  systemCatalogPtr->colValueSysconstraint( tableColName, true );
            
            CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
            for (indexIter = indexNameList.begin(); indexIter != indexNameList.end(); indexIter++)
            {
                CalpontSystemCatalog::ConstraintInfo constraintInfo = systemCatalogPtr->constraintInfo((*indexIter));
                switch( constraintInfo.constraintType )
                {
                    case CalpontSystemCatalog::NO_CONSTRAINT:
                        break;

                    case CalpontSystemCatalog::UNIQUE_CONSTRAINT:
                        constraintViolation = violatesUniqueConstraint( rows, constraintInfo, sessionID, result );
                        break;

                    case CalpontSystemCatalog::NOTNULL_CONSTRAINT:
                        constraintViolation = violatesNotNullConstraint( rows, colOffset, result );
                        break;

                    case CalpontSystemCatalog::CHECK_CONSTRAINT:
                        constraintViolation = violatesCheckConstraint( rows, constraintInfo, sessionID, colOffset, result );
                        break;
                    case CalpontSystemCatalog::REFERENCE_CONSTRAINT:
                        constraintViolation = violatesReferenceConstraint( rows, colOffset, constraintInfo, result);
                        break;
                    case CalpontSystemCatalog::PRIMARYKEY_CONSTRAINT:
					{
						if ( !(constraintViolation = violatesNotNullConstraint( rows, colOffset, result )))
						{
							
                        	constraintViolation = violatesUniqueConstraint( rows, constraintInfo, sessionID, result );
                        }
                        
                    }
                        break;
                    default:
                        break;
                }
                
                if ( constraintViolation )
                {
                	VERBOSE_INFO("Finished Checking Constraints...");
        			return constraintViolation;
        		}
            }
			
            ++column_iterator;
            ++colOffset;
        }

        VERBOSE_INFO("Finished Checking Constraints...");
        return constraintViolation;

    }

    bool DMLPackageProcessor::violatesUniqueConstraint( const dmlpackage::RowList& rows,
        const CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        unsigned int sessionID,
        DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesUniqueConstraint");

        bool violatesConstraint = false;

        CalpontSystemCatalog::TableColNameList tableColNames;
        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
            
        tableColNames = systemCatalogPtr->indexColNames( constraintInfo.constraintName );
        //Check whether the columns are related to the changed column
        Row* rowPtr = rows.at(0);
        dmlpackage::ColumnList columns = rowPtr->get_ColumnList();
        dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();
        bool needCheck = false;
        std::string columnName;
        CalpontSystemCatalog::TableColNameList::const_iterator colNamesIter;
        for ( colNamesIter = tableColNames.begin(); colNamesIter != tableColNames.end(); colNamesIter++ )
        { 
        	CalpontSystemCatalog::TableColName tableColName = *colNamesIter;
        	while ( column_iterator != columns.end() )
        	{
            	DMLColumn* columnPtr = *column_iterator;
            	columnName = columnPtr->get_Name();
            	boost::algorithm::to_lower(columnName);
            	if ( columnName == tableColName.column )
            	{
            		needCheck = true;
            		break;
            	}
            	column_iterator++;
            }
        }
        
        if ( !needCheck )
        {
        	return violatesConstraint;
        }
        // construct execution plan to validate UNIQUE constraint
        // select col1 from tablename where col1 = value1 and col2 = value2 and col3 = value3 ... )
        CalpontSelectExecutionPlan csep;          
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        CalpontSelectExecutionPlan::ColumnMap colMap; 

        string selColName = tableColNames[0].schema+"."+tableColNames[0].table+"."+tableColNames[0].column;
        SimpleColumn* selectCol = new SimpleColumn(selColName, sessionID);
        
        SRCP srcp(selectCol);
        colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(selColName, srcp));
        csep.columnMapNonStatic(colMap);
        CalpontSystemCatalog::OID selColOID = selectCol->oid();
        
        ConstantFilter *cf = new ConstantFilter();
        SSC ssc(selectCol->clone());
        cf->col(ssc);                       
       
        unsigned int i=1;
        std::string colValue;
        CalpontSystemCatalog::ROPair roPair;
        CalpontSystemCatalog::ColType colType;
        for ( colNamesIter = tableColNames.begin(); colNamesIter != tableColNames.end(); colNamesIter++ )
        {
        	CalpontSystemCatalog::TableColName tableColName = *colNamesIter;
        	//Find the column position
        	roPair = systemCatalogPtr->columnRID( tableColName );
        	colType = systemCatalogPtr->colType ( roPair.objnum);
        	const DMLColumn* columnPtr = rowPtr->get_ColumnAt( colType.colPosition );
        	if ( columnPtr->get_isnull() )
        	{
        		//@BUG 398:TODO: need to consider multi column case
        		return violatesConstraint;
        		colValue = columnPtr->get_Data();
        		SimpleColumn* col = new SimpleColumn(tableColName.schema+"."+tableColName.table+"."+tableColName.column, sessionID);
    			SimpleFilter *filter = new SimpleFilter (opeq,
                                         col->clone(),
                                         new ConstantColumn(colValue, ConstantColumn::NULLDATA));
    			filterTokenList.push_back(filter);
    			if ( i < tableColNames.size())
    			{
    				filterTokenList.push_back(new Operator("and"));
    			}
    			i++;
        	}
        	else
        	{
        		colValue = columnPtr->get_Data();
        		SimpleColumn* col = new SimpleColumn(tableColName.schema+"."+tableColName.table+"."+tableColName.column, sessionID);
    			SimpleFilter *filter = new SimpleFilter (opeq,
                                         col->clone(),
                                         new ConstantColumn(colValue, ConstantColumn::LITERAL));
    			filterTokenList.push_back(filter);
    			if ( i < tableColNames.size())
    			{
    				filterTokenList.push_back(new Operator("and"));
    			}
    			i++;
    		}
    	}
    
        csep.filterTokenList(filterTokenList); 
        CalpontSystemCatalog::NJLSysDataList valueList;
	
	executePlan (csep, valueList,
		systemCatalogPtr->tableRID(execplan::make_table(tableColNames[0].schema, tableColNames[0].table)).objnum);  
        unsigned int dataCount = 0;
    
        vector<ColumnResult*>::const_iterator it;
        for (it = valueList.begin(); it != valueList.end(); it++)
        {
            if ((*it)->ColumnOID() == selColOID)
            {
                dataCount += (unsigned int) (*it)->dataCount();
            }
        }
        if ( dataCount > 1)
            violatesConstraint = true;
        
        
        if ( violatesConstraint )
        {
            logging::Message::Args args;
            logging::Message message(3);
            args.add("unique constraint violation: ");
            //args.add(column.get_Name());
            message.format(args);

            result.result = UNIQUE_VIOLATION;
            result.message = message;
        }
        return violatesConstraint;
    }
#endif
	//@bug 397
	void DMLPackageProcessor::cleanString(string& s)
	{
	    string::size_type pos = s.find_first_not_of(" ");
            //stripe off space and ' or '' at beginning and end       
            if ( pos < s.length() )
            {    
            	s = s.substr( pos, s.length()-pos );
            	if ( (pos = s.find_last_of(" ")) < s.length())
            	{
            		s = s.substr(0, pos );           				 
            	}
            				 
            }
            if  ( s[0] == '\'')
            {
            	s = s.substr(1, s.length()-2);
            	if  ( s[0] == '\'')
            		s = s.substr(1, s.length()-2);
            }
	}


#if 0
    bool DMLPackageProcessor::violatesCheckConstraint( const dmlpackage::RowList& rows,
        const CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        unsigned int sessionID, unsigned int colOffset, 
        DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesCheckConstraint");

        bool violatesConstraint = false;
        
       	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
	csc->identity(CalpontSystemCatalog::EC);        		
        
        //@Bug 397. If the insert or update value is null, there is no need to check
        RowList::const_iterator row_iterator = rows.begin();
        while (row_iterator != rows.end())
        {
            Row* rowPtr = *row_iterator;
	        dmlpackage::ColumnList columns = rowPtr->get_ColumnList();
	        dmlpackage::DMLColumn* column = columns[colOffset];
            if (column->get_isnull() )
            {
            	return 	violatesConstraint;
            }
            ++row_iterator;
         }
        std::string checkText( constraintInfo.constraintText );
        std::string colName;
        std::string op;
        std::vector<std::string> valuesList; 
        string::size_type pos1;
        string::size_type pos2;
        string::size_type pos3;
        //Replace "/n" with " " if not in quotes
        pos1 = checkText.find( "\n" );
        while( (pos1 < checkText.length() ) && ( pos1 < ( pos2 = checkText.find_first_of( "'" ))))
		{
			checkText.replace( pos1, 1, " ");
			pos1 = checkText.find( "\n", pos1 + 2 );
		}
        boost::algorithm::to_lower( checkText );
        //Parse checkText to get column name, check value and operators       
        typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
        if (( (pos1 = checkText.find( " in {" )) < checkText.length() || ( pos1 = checkText.find( " in  {" ) )< checkText.length()
			 || ( pos1 = checkText.find( " in   {" ) ) < checkText.length() || ( pos1 = checkText.find( " in{" )) < checkText.length()) 
			 && ( pos1 < ( pos2 = checkText.find_first_of( "'" ))))
		{
			//check NOT IN
        	bool notFlag = false;
        	std::string tmpStr(checkText.substr(0, pos1));
        	if (( pos2 = tmpStr.find("not")) < tmpStr.length() )
        	{
        		pos1 = pos2;
        		notFlag = true;
        	}
			//IN constraint
			colName = constraintInfo.constraintText.substr(0, pos1) ;
			if ( (pos2 = colName.find_first_not_of(" ")) < colName.length() )
			{
				//strip off space
				colName = colName.substr( pos2, colName.length()-pos2 );
				 if ( (pos2 = colName.find_last_of(" ")) < colName.length())
				 {
				 	colName = colName.substr(0, pos2 );
				 	boost::algorithm::to_lower(colName);
				 }
				 
			}
			
			checkText = constraintInfo.constraintText.substr ( colName.length() + 1, checkText.length() - colName.length() - 1);
			pos2 = checkText.find_first_of( "{" );
			pos1 = checkText.find_last_of( "}" );
			
			//Only inside the curly bracket part
			checkText = checkText.substr( pos2+1, pos1-pos2-1 );
			// try to replace the comma in '' in order not to confuse the tokenizer
			pos2 = 0;
			pos1 = 0;
        	while (pos2 < checkText.length() && pos1 != string::npos)
        	{
            	pos1 = checkText.find_first_of("'", pos2);
            	pos2 = checkText.find_first_of("'", pos1+1);
            	pos3 = checkText.find_first_of(",", pos1);
            	if (pos3 < pos2)
                	checkText.replace(pos3, 1, "^");
            	pos2++;
        	}
        	
			boost::char_separator<char> sep1(",");
        	tokenizer tokens1(checkText, sep1);
        	tokenizer::iterator tok_iter = tokens1.begin();
            std::string value;
            valuesList.clear();
            //Get all values inside curly bracket
        	for (; tok_iter != tokens1.end(); ++tok_iter)
        	{
        		value = *tok_iter;
        		// recover '^' to ','
        		for (string::size_type i = 0; i < value.length(); i++)
            		if (value[i] == '^') value[i] = ',';
			cleanString(value);
        		valuesList.push_back( value );
        	}	
			/*select column1 from table where column1 <> value1 AND column1 <> value2 ...
			Build plan to check constraint violation	*/
			CalpontSelectExecutionPlan csep;          
        	CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        	CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        	CalpontSelectExecutionPlan::ColumnMap colMap; 
        
        	string selColName = constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName;
        	SimpleColumn* selectCol = new SimpleColumn(selColName, sessionID);
        
                SRCP srcp(selectCol);
        	colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(selColName, srcp));
        	csep.columnMapNonStatic(colMap);
        	CalpontSystemCatalog::OID selColOID = selectCol->oid();

        	ConstantFilter *cf = new ConstantFilter();
                SSC ssc(selectCol->clone());
        	cf->col(ssc);  
        	if ( notFlag )  		
        		cf->op(opor);
        	else
        		cf->op(opand);
        		
            for ( unsigned int  i = 0; i < valuesList.size(); i++ )
            {
            
       			//SimpleColumn* col = new SimpleColumn(constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName, sessionID);
    			if ( notFlag)
    			{
    				SimpleFilter *filter = new SimpleFilter (opeq,
    														selectCol->clone(),
    				                                         new ConstantColumn(valuesList[i], ConstantColumn::LITERAL));
    				cf->pushFilter( filter );
    				
    			}
    			else
    			{
    				SimpleFilter *filter = new SimpleFilter (opne,
                                         selectCol->clone(),
                                         new ConstantColumn(valuesList[i], ConstantColumn::LITERAL));
    				cf->pushFilter(filter);				
    			}
    		}
            filterTokenList.push_back(cf);
            
        	csep.filterTokenList(filterTokenList); 
        	CalpontSystemCatalog::NJLSysDataList valueList;
        	executePlan (csep, valueList,
			csc->tableRID(execplan::make_table(constraintInfo.constraintName.schema, constraintInfo.constraintName.table)).objnum);  
        	unsigned int dataCount = 0;
    
        	vector<ColumnResult*>::const_iterator it;
        	for (it = valueList.begin(); it != valueList.end(); it++)
        	{
            	if ((*it)->ColumnOID() == selColOID)
            	{
                	dataCount += (unsigned int) (*it)->dataCount();
            	}
        	}
        	if ( dataCount > 0)
            	violatesConstraint = true;
		}
		
		else if ( ((pos1 = checkText.find_first_of( ">=<" ) ) < checkText.length() ) && 
				( pos1 < ( pos2 = checkText.find_first_of( "'" )) ) )
		{
			
			//Get column name
			colName = constraintInfo.constraintText.substr(0, pos1) ;
			if ( (pos2 = colName.find_first_not_of(" ")) < colName.length() )
			{
				//strip off space
				colName = colName.substr( pos2, colName.length()-pos2 );
				 if ( (pos2 = colName.find_last_of(" ")) < colName.length())
				 {
				 	colName = colName.substr(0, pos2 );
				 	boost::algorithm::to_lower(colName);
				 }
				 
			}
			 //Get operator
			op =  constraintInfo.constraintText.substr(pos1,2) ;
			if ( (op[1] != '=')  && (op[1] != '>') )
			{
				op = op.substr(0,1);
			}
			
			//Get value
			std::string value;
            valuesList.clear();
            value = constraintInfo.constraintText.substr(pos1+op.length(), constraintInfo.constraintText.length() - pos1 - op.length());
            //stripe off space and ' or '' at beginning and end       
//             if ( (pos2 = value.find_first_not_of(" ")) < value.length() )
//             {    
//             	value = value.substr( pos2, value.length()-pos2 );
//             	if ( (pos2 = value.find_last_of(" ")) < value.length())
//             	{
//             		value = value.substr(0, pos2 );           				 
//             	}
//             				 
//             }
//             if  ( value[0] == '\'')
//             {
//             	value = value.substr(1, value.length()-2);
//             	if  ( value[0] == '\'')
//             		value = value.substr(1, value.length()-2);
//             }
            cleanString(value);
            valuesList.push_back(value);
            //Convert operator
            if (op.compare(">") == 0)
            {
            	op = "<=";
            }
			else if (op.compare("<") == 0)
            {
            	op = ">=";
            }
            else if (op.compare(">=") == 0)
            {
            	op = "<";
            }
			else if (op.compare("<=") == 0)
            {
            	op = ">";
            }
            else if (op.compare("=") == 0)
            {
            	op = "<>";
            }
            else
            {
            	op = "=";
            }
            //Build plan
			CalpontSelectExecutionPlan csep;          
        	CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        	CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        	CalpontSelectExecutionPlan::ColumnMap colMap; 
        
        	string selColName = constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName;
        	SimpleColumn* selectCol = new SimpleColumn(selColName, sessionID);
        
                SRCP srcp(selectCol);
        	colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(selColName, srcp));
        	csep.columnMapNonStatic(colMap);
        	CalpontSystemCatalog::OID selColOID = selectCol->oid();
        
        	ConstantFilter *cf = new ConstantFilter();
                SSC ssc(selectCol->clone());
        	cf->col(ssc);                                   
            
       		//SimpleColumn* col = new SimpleColumn(constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName, sessionID);
                SOP sop(new Operator(op));
    		SimpleFilter *filter = new SimpleFilter (sop,
                                         selectCol->clone(),
                                         new ConstantColumn(value, ConstantColumn::LITERAL));
    		cf->pushFilter(filter);
    		filterTokenList.push_back(cf);
    		
    
        	csep.filterTokenList(filterTokenList); 
        	CalpontSystemCatalog::NJLSysDataList valueList;

        	executePlan (csep, valueList,
			csc->tableRID(execplan::make_table(constraintInfo.constraintName.schema, constraintInfo.constraintName.table)).objnum);  


        	unsigned int dataCount = 0;
    
        	vector<ColumnResult*>::const_iterator it;
        	for (it = valueList.begin(); it != valueList.end(); it++)
        	{
            	if ((*it)->ColumnOID() == selColOID)
            	{
                	dataCount += (unsigned int) (*it)->dataCount();
            	}
        	}
        	if ( dataCount > 0)
            	violatesConstraint = true;
        	
		}      
		else if ( ((pos1 = checkText.find( " is " ) ) < checkText.length() ) && 
				( pos1 < ( pos2 = checkText.find_first_of( "'" )) ) )
		{
			//Get column name
			colName = constraintInfo.constraintText.substr(0, pos1) ;
			if ( (pos2 = colName.find_first_not_of(" ")) < colName.length() )
			{
				//strip off space
				colName = colName.substr( pos2, colName.length()-pos2 );
				if ( (pos2 = colName.find_last_of(" ")) < colName.length())
				{
					colName = colName.substr(0, pos2 );
					boost::algorithm::to_lower(colName);
				}
							 
			}
		
			// check whether the op is "null" or "not null"
			std::string tmpStr(checkText.substr(pos1+3, checkText.length() - pos1 - 3) );
			if ( (pos2 = tmpStr.find("not " )) < tmpStr.length() )
			{
				op = "IS";			
			}
			else
			{
				op = "IS NOT";
			}
			//Build plan
			CalpontSelectExecutionPlan csep;          
			CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
			CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
			CalpontSelectExecutionPlan::ColumnMap colMap; 
			        
			string selColName = constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName;
			SimpleColumn* selectCol = new SimpleColumn(selColName, sessionID);

                        SRCP srcp(selectCol);
			colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(selColName, srcp));
			csep.columnMapNonStatic(colMap);
			CalpontSystemCatalog::OID selColOID = selectCol->oid();
			        
			ConstantFilter *cf = new ConstantFilter();
                        SSC ssc(selectCol->clone());
			cf->col(ssc);                                   
			            
			//SimpleColumn* col = new SimpleColumn(constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName, sessionID);
            SOP sop(new Operator(op));
			SimpleFilter *filter = new SimpleFilter (sop,
													selectCol->clone(),
			                                         new ConstantColumn("null", ConstantColumn::NULLDATA));
			cf->pushFilter(filter);
			filterTokenList.push_back(cf);
			    		
			    
			csep.filterTokenList(filterTokenList); 
			CalpontSystemCatalog::NJLSysDataList valueList;
			executePlan (csep, valueList,
				csc->tableRID(execplan::make_table(constraintInfo.constraintName.schema, constraintInfo.constraintName.table)).objnum);  
			unsigned int dataCount = 0;
			    
			vector<ColumnResult*>::const_iterator it;
			for (it = valueList.begin(); it != valueList.end(); it++)
			{
			   if ((*it)->ColumnOID() == selColOID)
			   {
			       dataCount += (unsigned int) (*it)->dataCount();			           
			   }
			}
			   if ( dataCount > 0)
			      violatesConstraint = true;
			
		}
		else if ( ((pos1 = checkText.find( " between " ) ) < checkText.length() ) && 
        				( pos1 < ( pos2 = checkText.find_first_of( "'" )) ) )
        {
        	//Get column name
        	colName = checkText.substr(0, pos1) ;
        	if ( (pos2 = colName.find_first_not_of(" ")) < colName.length() )
        	{
        		//strip off space
        		colName = colName.substr( pos2, colName.length()-pos2 );
        		if ( (pos2 = colName.find_last_of(" ")) < colName.length())
        		{
        			colName = colName.substr(0, pos2 );        						
        		}
        								 
        	}
        	
        	//Get values
        	pos2 = checkText.find( " and ");  
        	std::string value1( constraintInfo.constraintText.substr( pos1+8,pos2-pos1-8 ) );
        	std::string value2( constraintInfo.constraintText.substr(pos2+4, constraintInfo.constraintText.length()-pos2 -4) );
        	//stripe off space and ' or '' at beginning and end       
//         	if ( (pos2 = value1.find_first_not_of(" ")) < value1.length() )
//         	{    
//         	   value1 = value1.substr( pos2, value1.length()-pos2 );
//         	   if ( (pos2 = value1.find_last_of(" ")) < value1.length())
//         	   {
//         	      value1 = value1.substr(0, pos2 );           				 
//         	   }
//         	            				 
//         	}
//         	if  ( value1[0] == '\'')
//         	{
//         	   value1 = value1.substr(1, value1.length()-2);
//         	   if  ( value1[0] == '\'')
//         	      value1 = value1.substr(1, value1.length()-2);
//         	}
//         	if ( (pos2 = value2.find_first_not_of(" ")) < value2.length() )
//         	{    
//         	    value2 = value2.substr( pos2, value2.length()-pos2 );
//         	    if ( (pos2 = value2.find_last_of(" ")) < value2.length())
//         	    {
//         	        value2 = value2.substr(0, pos2 );           				 
//         	    }
//         	        	            				 
//         	}
//         	if  ( value2[0] == '\'')
//         	{
//         	    value2 = value2.substr(1, value2.length()-2);
//         	    if  ( value2[0] == '\'')
//         	       value2 = value2.substr(1, value2.length()-2);
//         	}
		cleanString(value1);
		cleanString(value2);
        	
        	std::string op1( "<");
        	std::string op2( ">");
        	
        	//Build plan
        	CalpontSelectExecutionPlan csep;          
        	CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        	CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        	CalpontSelectExecutionPlan::ColumnMap colMap; 
        				        
        	string selColName = constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName;
        	SimpleColumn* selectCol = new SimpleColumn(selColName, sessionID);
        				        
                SRCP srcp(selectCol->clone());
        	colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(selColName, srcp));
        	csep.columnMapNonStatic(colMap);
        	CalpontSystemCatalog::OID selColOID = selectCol->oid();
        				        
        	ConstantFilter *cf = new ConstantFilter();
                SSC ssc(selectCol);
        	cf->col(ssc);   
        	cf->op(opor);
        	
        	//SimpleColumn* col = new SimpleColumn(constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName, sessionID);
            SOP sop(new Operator(op1));
        	SimpleFilter *filter = new SimpleFilter (sop,
        												selectCol->clone(),
        				                                 new ConstantColumn(value1, ConstantColumn::LITERAL));
        	cf->pushFilter(filter);
        	
        
        	//SimpleColumn* col1 = new SimpleColumn(constraintInfo.constraintName.schema+"."+constraintInfo.constraintName.table+"."+colName, sessionID);
            sop.reset(new Operator(op2));
        	SimpleFilter *filter1 = new SimpleFilter (sop,
        												selectCol->clone(),
        	        				                     new ConstantColumn(value2, ConstantColumn::LITERAL));
        	cf->pushFilter(filter1);
        	filterTokenList.push_back(cf);			    
        	csep.filterTokenList(filterTokenList); 
        	
        	CalpontSystemCatalog::NJLSysDataList valueList;
		executePlan (csep, valueList,
			csc->tableRID(execplan::make_table(constraintInfo.constraintName.schema, constraintInfo.constraintName.table)).objnum);          	
        	unsigned int dataCount = 0;
        				    
        	vector<ColumnResult*>::const_iterator it;
        	for (it = valueList.begin(); it != valueList.end(); it++)
        	{
        		if ((*it)->ColumnOID() == selColOID)
        		{
        			dataCount += (unsigned int) (*it)->dataCount();		       
        		}
        	}
        	if ( dataCount > 0)
        		violatesConstraint = true;
        }
		else
		{
			violatesConstraint = false;
		}
        
        if ( violatesConstraint )
        {
            logging::Message::Args args;
            logging::Message message(9);
            args.add("check constraint violation: ");
            args.add( constraintInfo.constraintName.index );
            args.add( "  ");
            args.add( constraintInfo.constraintText );
            message.format(args);

            result.result = CHECK_VIOLATION;
            result.message = message;
        }

        return violatesConstraint;
    }

    bool DMLPackageProcessor::violatesNotNullConstraint( const dmlpackage::RowList& rows, unsigned int colOffset, DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesNotNullConstraint");

        bool violatesConstraint = false;

        RowList::const_iterator row_iterator = rows.begin();
        while (row_iterator != rows.end() && violatesConstraint == false)
        {
            Row* rowPtr = *row_iterator;
	        dmlpackage::ColumnList columns = rowPtr->get_ColumnList();
	        dmlpackage::DMLColumn* column = columns[colOffset];
            if (column->get_isnull() )
            {
                // build the logging message
                logging::Message::Args args;
                logging::Message message(2);
                args.add("not null constraint violation: ");
                args.add(column->get_Name());
                message.format( args );
    
                result.result = NOTNULL_VIOLATION;
                result.message = message;
    
                violatesConstraint = true;
                break;
            }
            row_iterator++;
        }

        return violatesConstraint;
    }
    
    bool DMLPackageProcessor::violatesReferenceConstraint( const dmlpackage::RowList& rows,
        unsigned int colOffset, 
        const CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        DMLResult& result )
    {
        bool violatesConstraint = false;
        RowList::const_iterator row_iterator = rows.begin();

        map<string, int> distDataMap;

        while (row_iterator != rows.end() && violatesConstraint == false)
        {
            Row* rowPtr = *row_iterator;
	        dmlpackage::ColumnList columns = rowPtr->get_ColumnList();
	        dmlpackage::DMLColumn* column = columns[colOffset];
            
            // get distinct insert/update data list
            if (!column->get_isnull())
                distDataMap.insert(map<string, int>::value_type(column->get_Data(),0));
            ++ row_iterator;
        }
        
        if (distDataMap.size() == 0)
        {
            return violatesConstraint; // not violated
        }
    
        // get referenced column name
        CalpontSystemCatalog::IndexName indexName;
        indexName.schema = constraintInfo.referenceSchema;
        indexName.table = constraintInfo.referenceTable;
        indexName.index = constraintInfo.referencePKName;
        
        CalpontSystemCatalog *cat = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
        cat->identity(CalpontSystemCatalog::EC);
        
        CalpontSystemCatalog::TableColNameList tableColNameList = cat->indexColNames(indexName);
        if (tableColNameList.size() == 0)
        {
            logging::Message::Args args;
            logging::Message message(6);
            args.add("No referenced col found for referenced constraint name");
            message.format( args );
            result.result = REFERENCE_VIOLATION;
            result.message = message;
            violatesConstraint = true;
            return violatesConstraint;
        }
        
        // only support 1-1 mapping for reference constraint
        //string currColName = tableColName.schema+"."+tableColName.table+"."+tableColName.column;
        string refColName = tableColNameList[0].schema+"."+tableColNameList[0].table+"."+tableColNameList[0].column;
        
        // construct execution plan to validate reference constraint
        // select referenced_col from referenced_tb where referenced_col in (dist curr_col_val list)
        CalpontSelectExecutionPlan csep;          
        CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
        CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
        CalpontSelectExecutionPlan::ColumnMap colMap;  
        
        SimpleColumn* refCol = new SimpleColumn(refColName, fSessionID);
        
        SRCP srcp(refCol);
        colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(refColName, srcp));
        csep.columnMapNonStatic(colMap);
        CalpontSystemCatalog::OID refColOID = refCol->oid();
        
        ConstantFilter *cf = new ConstantFilter();
        SSC ssc(refCol->clone());
        cf->col(ssc);
        cf->op(opor);
        
        map<string, int>::const_iterator dataIter = distDataMap.begin();
        for (; dataIter != distDataMap.end(); dataIter++)
        {
            SimpleFilter *sf = NULL;
                sf = new SimpleFilter(opeq,
                                  refCol->clone(),
                                  new ConstantColumn((*dataIter).first, ConstantColumn::LITERAL)); // the datatype chould be NUM as well. but joblist does not care this field. 
            cf->pushFilter(sf);                                                
        }
        filterTokenList.push_back(cf);
        csep.filterTokenList(filterTokenList); 
        
        CalpontSystemCatalog::NJLSysDataList valueList;
	executePlan (csep, valueList,
		cat->tableRID(execplan::make_table(tableColNameList[0].schema, tableColNameList[0].table)).objnum);          
        unsigned int dataCount = 0;
    
        vector<ColumnResult*>::const_iterator it;
        for (it = valueList.begin(); it != valueList.end(); it++)
        {
            if ((*it)->ColumnOID() == refColOID)
            {
                // if the returned data size != distDataMap size, not validated
                dataCount += (unsigned int) (*it)->dataCount();
            }
        }
        if (distDataMap.size() != dataCount || valueList.size() == 0)
            violatesConstraint = true;
        if (violatesConstraint)
        {
            logging::Message::Args args;
            logging::Message message(5);
            args.add("Reference constraint violation: ");
            args.add( refColName );
            args.add ("--Transaction rolling back.");
            message.format( args );
            result.result = REFERENCE_VIOLATION;
            result.message = message;
        }
        return violatesConstraint;
    }

 bool DMLPackageProcessor::violatesPKRefConnstraint( u_int32_t sessionID,
            const std::string& schema,
            const std::string& table,
            const dmlpackage::RowList& rows,
            const WriteEngine::ColValueList& oldValueList,
            DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesDelRefConnstraint");

        VERBOSE_INFO("Checking Reference Constraints for Delete...");

        bool constraintViolation = false;

        // only check the first row meta data and validate all rows
        RowList::const_iterator row_iterator = rows.begin();
        if (row_iterator == rows.end()) return constraintViolation;

        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
        systemCatalogPtr->identity(CalpontSystemCatalog::EC);

        CalpontSystemCatalog::TableColName tableColName;
        tableColName.table = table;
        tableColName.schema = schema;

        Row* rowPtr = *row_iterator;

            dmlpackage::ColumnList columns = rowPtr->get_ColumnList();

            dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();

        while( column_iterator != columns.end() && false == constraintViolation )
        {
            DMLColumn* columnPtr = *column_iterator;

            // see if this column has any constraints
            // no need to get coltype. only need to get constraintType
            tableColName.column  = columnPtr->get_Name();
            CalpontSystemCatalog::IndexNameList indexNameList =  systemCatalogPtr->colValueSysconstraint( tableColName );
            CalpontSystemCatalog::OID colOID = systemCatalogPtr->lookupOID(tableColName);
            CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(colOID);

            unsigned int colOffset = 0;

            CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
            for (indexIter = indexNameList.begin(); indexIter != indexNameList.end() && constraintViolation == false; indexIter++)
            {
                CalpontSystemCatalog::ConstraintInfo constraintInfo = systemCatalogPtr->constraintInfo((*indexIter));
                if (constraintInfo.constraintType == CalpontSystemCatalog::PRIMARYKEY_CONSTRAINT)
 constraintViolation =
                        violatesReferenceConstraint_PK(oldValueList, colType, colOffset, constraintInfo, result);                            
            }

            ++column_iterator;
            ++colOffset;
        }

        VERBOSE_INFO("Finished Checking Constraints...");
        return constraintViolation;

    }

    bool DMLPackageProcessor::violatesPKRefConnstraint( u_int32_t sessionID, 
            const std::string& schema, 
            const std::string& table,
		std::vector<WriteEngine::RID>& rowIDList,
            std::vector<void *>& oldValueList, 
            DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesDelRefConnstraint");

        VERBOSE_INFO("Checking Reference Constraints for Delete...");

        bool constraintViolation = false;
        
        // only check the first row meta data and validate all rows
        if (rowIDList.size()==0) return constraintViolation;

        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
        systemCatalogPtr->identity(CalpontSystemCatalog::EC);

        CalpontSystemCatalog::TableColName tableColName;
        tableColName.table = table;
        tableColName.schema = schema;

	    dmlpackage::ColumnList columns;
		getColumnsForTable(sessionID, schema,table,columns);

	    dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();

        while( column_iterator != columns.end() && false == constraintViolation )
        {
            DMLColumn* columnPtr = *column_iterator;

            // see if this column has any constraints
            // no need to get coltype. only need to get constraintType
            tableColName.column  = columnPtr->get_Name();
            CalpontSystemCatalog::IndexNameList indexNameList =  systemCatalogPtr->colValueSysconstraint( tableColName );
            CalpontSystemCatalog::OID colOID = systemCatalogPtr->lookupOID(tableColName);
            CalpontSystemCatalog::ColType colType = systemCatalogPtr->colType(colOID);
            
            unsigned int colOffset = 0;
            
            CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
            for (indexIter = indexNameList.begin(); indexIter != indexNameList.end() && constraintViolation == false; indexIter++)
            {
                CalpontSystemCatalog::ConstraintInfo constraintInfo = systemCatalogPtr->constraintInfo((*indexIter));
                if (constraintInfo.constraintType == CalpontSystemCatalog::PRIMARYKEY_CONSTRAINT)
                    constraintViolation = 
                        violatesReferenceConstraint_PK(oldValueList, rowIDList.size(), colType, colOffset, constraintInfo, result);                               
            }

            ++column_iterator;
            ++colOffset;
        }

        VERBOSE_INFO("Finished Checking Constraints...");
        return constraintViolation;

    }

    bool DMLPackageProcessor::violatesReferenceConstraint_PK( 
        const WriteEngine::ColValueList& oldValueList,
        const CalpontSystemCatalog::ColType& colType, 
        unsigned int colOffset, 
        const CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        DMLResult& result )
    {
        bool violatesConstraint = false;
        //RowList::const_iterator row_iterator = rows.begin();
        //@BUG 434
		WriteEngine::ColTupleList colTuples = oldValueList[colOffset];
		WriteEngine::ColTupleList::const_iterator val_iterator = colTuples.begin();
        vector<string> valList;
        while (val_iterator != colTuples.end() && violatesConstraint == false)
        {
            WriteEngine::ColTuple colTuple = *val_iterator;
            
            // get delete old value list
            ostringstream oss;
            switch( colType.colDataType )
            {
            case WriteEngine::INT :
            case WriteEngine::MEDINT:
            case WriteEngine::DATE:
                                         if( colTuple.data.type() == typeid(int))
                                             oss << boost::any_cast<int>( colTuple.data );
                                          else
                                             oss << boost::any_cast<i32>( colTuple.data );
                                          break;
            case WriteEngine::DECIMAL :
            case WriteEngine::CHAR:
            case WriteEngine::VARCHAR:
            case WriteEngine::CLOB:
            case WriteEngine::BLOB:
                                          oss << boost::any_cast<std::string>( colTuple.data );
                                          break;
            case WriteEngine::FLOAT:      oss << boost::any_cast<float>( colTuple.data );
                                          break;
            case WriteEngine::DOUBLE:     oss << boost::any_cast<double>( colTuple.data );
                                          break;
            case WriteEngine::SMALLINT:   oss << boost::any_cast<short>( colTuple.data );
                                          break;
            case WriteEngine::BIT:    
            case WriteEngine::TINYINT:
                                          oss << boost::any_cast<char>( colTuple.data );
                                          break;
            case WriteEngine::BIGINT:
            case WriteEngine::DATETIME:
                                          if( colTuple.data.type() == typeid(long long ))
                                             oss << boost::any_cast<long long>( colTuple.data );
                                          else
                                             oss << boost::any_cast<i64>( colTuple.data );
                                          break;
            } // end of switch( colType )
                        
            valList.push_back(oss.str());
            ++ val_iterator;
        }
        
        // get reference column name from the foreign key table
        CalpontSystemCatalog::IndexNameList indexNameList;
        CalpontSystemCatalog::IndexName indexName;
        indexName.schema = constraintInfo.constraintName.schema;
        indexName.table = constraintInfo.constraintName.table;
        indexName.index = constraintInfo.constraintName.index;
        
        CalpontSystemCatalog *cat = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
        cat->identity(CalpontSystemCatalog::EC);
        
        indexNameList = cat->referenceConstraints(indexName);

        CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
        for (indexIter = indexNameList.begin(); indexIter != indexNameList.end(); indexIter++)
        {
            // only support 1-1 mapping for reference constraint
            // step 1. look up sysconstraintcol to find column(s) with this constraint name
            // step 2. construct execution plan for this column. select col from FKtable where col in valuelist;
            // step 3. violation true if any row returned.
            //@BUG 434
            CalpontSystemCatalog::TableColNameList tableColNameList= cat->constraintColNames( (*indexIter).index );
            
            // assume one column in the tableColNameList (only support 1 to 1 mapping)
            if (tableColNameList.size() == 0)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint validation for delete: no column found for foreign key ");
                args.add( (*indexIter).schema );
                args.add( (*indexIter).table );
                args.add( (*indexIter).index );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
                return false;
            }
            if (tableColNameList.size() > 1)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint validation for delete: only one column foreign key is supported ");
                args.add( (*indexIter).table );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
                return false;
            }
            
            // construct execution plan
            CalpontSelectExecutionPlan csep;          
            CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
            CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
            CalpontSelectExecutionPlan::ColumnMap colMap;  
            
            
            string refColName = tableColNameList[0].schema+"."+tableColNameList[0].table+"."+tableColNameList[0].column;
            SimpleColumn* refCol = new SimpleColumn(refColName, fSessionID);

            SRCP srcp(refCol);
            colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(refColName, srcp));
            csep.columnMapNonStatic(colMap);
            
            ConstantFilter *cf = new ConstantFilter();
            SSC ssc(refCol->clone());
            cf->col(ssc);
            cf->op(opor);
            
            vector<string>::const_iterator valIter = valList.begin();
            for (; valIter != valList.end(); valIter++)
            {
                SimpleFilter *sf = new SimpleFilter(opeq,
                                                    refCol->clone(),
                                                    new ConstantColumn((*valIter), ConstantColumn::LITERAL)); // the datatype chould be NUM as well. but joblist does not care this field. 
                cf->pushFilter(sf);                                                
            }
            filterTokenList.push_back(cf);
            csep.filterTokenList(filterTokenList); 
            
            CalpontSystemCatalog::NJLSysDataList valueList;
	    executePlan (csep, valueList,
		cat->tableRID(execplan::make_table(tableColNameList[0].schema, tableColNameList[0].table)).objnum);            
        
            vector<ColumnResult*>::const_iterator it;
            for (it = valueList.begin(); it != valueList.end(); it++)
            {
                if ((*it)->dataCount() != 0)
                    violatesConstraint = true;
            }
                        
            if (violatesConstraint)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint violation: ");
                args.add( refColName );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
            }
            return violatesConstraint;
        }
        return violatesConstraint;
    }

 bool DMLPackageProcessor::violatesReferenceConstraint_PK(
        std::vector<void *>& oldValueList,
        const int totalRows,
        const CalpontSystemCatalog::ColType& colType,
        unsigned int colOffset,
        const CalpontSystemCatalog::ConstraintInfo& constraintInfo,
        DMLResult& result )
    {
        bool violatesConstraint = false;
        //RowList::const_iterator row_iterator = rows.begin();
        //@BUG 434
        WriteEngine::ColTupleList colTuples;
        WriteEngine::ColStruct colStruct;
        colStruct.tokenFlag = false;
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
        fWriteEngine.getColumnOp().convertColType( &colStruct ); // This call is to get colType. No column operation as such.
        fWriteEngine.convertValArray(totalRows,colStruct.colType,colTuples,oldValueList[colOffset],false);
        
		if(oldValueList[colOffset] != NULL)
		{
            free(oldValueList[colOffset]);
            oldValueList[colOffset]=NULL;
        }
        WriteEngine::ColTupleList::const_iterator val_iterator = colTuples.begin();
        vector<string> valList;
        while (val_iterator != colTuples.end() && violatesConstraint == false)
        {
            WriteEngine::ColTuple colTuple = *val_iterator;

            // get delete old value list
            ostringstream oss;
            switch( colType.colDataType )
            {
            case WriteEngine::INT :
            case WriteEngine::MEDINT:
            case WriteEngine::DATE:
                                         if( colTuple.data.type() == typeid(int))
                                             oss << boost::any_cast<int>( colTuple.data );
                                          else
                                             oss << boost::any_cast<i32>( colTuple.data );
                                          break;
            case WriteEngine::DECIMAL :
            case WriteEngine::CHAR:
            case WriteEngine::VARCHAR:
            case WriteEngine::CLOB:
            case WriteEngine::BLOB:
                                          oss << boost::any_cast<std::string>( colTuple.data );
                                          break;
            case WriteEngine::FLOAT:      oss << boost::any_cast<float>( colTuple.data );
                                          break;
            case WriteEngine::DOUBLE:     oss << boost::any_cast<double>( colTuple.data );
                                          break;
            case WriteEngine::SMALLINT:   oss << boost::any_cast<short>( colTuple.data );
                                          break;
            case WriteEngine::BIT:
            case WriteEngine::TINYINT:
                                          oss << boost::any_cast<char>( colTuple.data );
                                          break;
            case WriteEngine::BIGINT:
            case WriteEngine::DATETIME:
                                          if( colTuple.data.type() == typeid(long long ))
                                             oss << boost::any_cast<long long>( colTuple.data );
                                          else
                                             oss << boost::any_cast<i64>( colTuple.data );
                                          break;
            } // end of switch( colType )

            valList.push_back(oss.str());
            ++ val_iterator;
        }
        colTuples.clear();

        // get reference column name from the foreign key table
        CalpontSystemCatalog::IndexNameList indexNameList;
        CalpontSystemCatalog::IndexName indexName;
        indexName.schema = constraintInfo.constraintName.schema;
        indexName.table = constraintInfo.constraintName.table;
        indexName.index = constraintInfo.constraintName.index;

        CalpontSystemCatalog *cat = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
        cat->identity(CalpontSystemCatalog::EC);

        indexNameList = cat->referenceConstraints(indexName);

        CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
        for (indexIter = indexNameList.begin(); indexIter != indexNameList.end(); indexIter++)
        {
            // only support 1-1 mapping for reference constraint
            // step 1. look up sysconstraintcol to find column(s) with this constraint name
            // step 2. construct execution plan for this column. select col from FKtable where col in valuelist;
           // step 3. violation true if any row returned.
            //@BUG 434
            CalpontSystemCatalog::TableColNameList tableColNameList= cat->constraintColNames( (*indexIter).index );

            // assume one column in the tableColNameList (only support 1 to 1 mapping)
            if (tableColNameList.size() == 0)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint validation for delete: no column found for foreign key ");
                args.add( (*indexIter).schema );
                args.add( (*indexIter).table );
                args.add( (*indexIter).index );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
                return false;
            }
            if (tableColNameList.size() > 1)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint validation for delete: only one column foreign key is supported ");
                args.add( (*indexIter).table );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
                return false;
            }

            // construct execution plan
            CalpontSelectExecutionPlan csep;
            CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
            CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
            CalpontSelectExecutionPlan::ColumnMap colMap;


            string refColName = tableColNameList[0].schema+"."+tableColNameList[0].table+"."+tableColNameList[0].column;
            SimpleColumn* refCol = new SimpleColumn(refColName, fSessionID);

            SRCP srcp(refCol);
            colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(refColName, srcp));
            csep.columnMapNonStatic(colMap);

            ConstantFilter *cf = new ConstantFilter();
            SSC ssc(refCol->clone());
            cf->col(ssc);
            cf->op(opor);

            vector<string>::const_iterator valIter = valList.begin();
            for (; valIter != valList.end(); valIter++)
            {
                SimpleFilter *sf = new SimpleFilter(opeq,
                                                    refCol->clone(),
                                                    new ConstantColumn((*valIter), ConstantColumn::LITERAL)); // the datatype chould be NUM as well. but joblist does not care this field.
                cf->pushFilter(sf);
            }
            filterTokenList.push_back(cf);
            csep.filterTokenList(filterTokenList);
			valList.clear();

            CalpontSystemCatalog::NJLSysDataList valueList;
			executePlan (csep, valueList,
			cat->tableRID(execplan::make_table(tableColNameList[0].schema, tableColNameList[0].table)).objnum);

            vector<ColumnResult*>::const_iterator it;
            for (it = valueList.begin(); it != valueList.end(); it++)
            {
                if ((*it)->dataCount() != 0)
                    violatesConstraint = true;
            }

            if (violatesConstraint)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint violation: ");
                args.add( refColName );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
            }
            return violatesConstraint;
        }
        return violatesConstraint;
    }
                                                                                                                
    bool DMLPackageProcessor::violatesUpdtRefConstraints( u_int32_t sessionID, 
            const std::string& schema, 
            const std::string& table,
            const dmlpackage::RowList& rows, 
            DMLResult& result )
    {
        SUMMARY_INFO("DMLPackageProcessor::violatesCheckConstraint");

        VERBOSE_INFO("Checking Constraints...");

        bool constraintViolation = false;
        
        // only check the first row meta data and validate all rows
        RowList::const_iterator row_iterator = rows.begin();

        CalpontSystemCatalog* systemCatalogPtr =
            CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );

        CalpontSystemCatalog::TableColName tableColName;
        tableColName.table = table;
        tableColName.schema = schema;

        Row* rowPtr = *row_iterator;

	    dmlpackage::ColumnList columns = rowPtr->get_ColumnList();

	    dmlpackage::ColumnList::const_iterator column_iterator = columns.begin();

        while( column_iterator != columns.end() && false == constraintViolation )
        {
            DMLColumn* columnPtr = *column_iterator;

            // see if this column has any constraints
            // no need to get coltype. only need to get constraintType
            tableColName.column  = columnPtr->get_Name();
            CalpontSystemCatalog::IndexNameList indexNameList =  systemCatalogPtr->colValueSysconstraint( tableColName );
            unsigned int colOffset = 0;
            
            CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
            for (indexIter = indexNameList.begin(); indexIter != indexNameList.end() && constraintViolation == false; indexIter++)
            {
                CalpontSystemCatalog::ConstraintInfo constraintInfo = systemCatalogPtr->constraintInfo((*indexIter));
                if (constraintInfo.constraintType == CalpontSystemCatalog::PRIMARYKEY_CONSTRAINT)
                {
                    constraintViolation = violatesReferenceConstraint_updt( rows, colOffset, constraintInfo, result);
                }                
            }

            ++column_iterator;
            ++colOffset;
        }

        VERBOSE_INFO("Finished Checking Constraints...");
        return constraintViolation;

    }
    
    bool DMLPackageProcessor::violatesReferenceConstraint_updt( const dmlpackage::RowList& rows,
        unsigned int colOffset, 
        const CalpontSystemCatalog::ConstraintInfo& constraintInfo, 
        DMLResult& result )
    {
        bool violatesConstraint = false;
        RowList::const_iterator row_iterator = rows.begin();

        vector<string> valList;

        while (row_iterator != rows.end() && violatesConstraint == false)
        {
            Row* rowPtr = *row_iterator;
	        dmlpackage::ColumnList columns = rowPtr->get_ColumnList();
	        dmlpackage::DMLColumn* column = columns[colOffset];
            
            // get distinct insert/update data list
            //distDataMap.insert(map<string, int>::value_type(column->get_Data(),0));
            valList.push_back(column->get_Data());
            ++ row_iterator;
        }
        
        // get reference column name from the foreign key table
        CalpontSystemCatalog::IndexNameList indexNameList;
        CalpontSystemCatalog::IndexName indexName;
        indexName.schema = constraintInfo.constraintName.schema;
        indexName.table = constraintInfo.constraintName.table;
        indexName.index = constraintInfo.constraintName.index;
        
        CalpontSystemCatalog *cat = CalpontSystemCatalog::makeCalpontSystemCatalog( fSessionID );
        cat->identity(CalpontSystemCatalog::EC);
        
        indexNameList = cat->referenceConstraints(indexName);

        CalpontSystemCatalog::IndexNameList::const_iterator indexIter;
        for (indexIter = indexNameList.begin(); indexIter != indexNameList.end(); indexIter++)
        {
            // only support 1-1 mapping for reference constraint
            // step 1. look up sysconstraintcol to find column(s) with this constraint name
            // step 2. construct execution plan for this column. select col from FKtable where col in valuelist;
            // step 3. violation true if any row returned.
            CalpontSystemCatalog::TableColNameList tableColNameList = cat->indexColNames((*indexIter));
            
            // assume one column in the tableColNameList (only support 1 to 1 mapping)
            if (tableColNameList.size() == 0)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint validation for delete: no column found for foreign key ");
                args.add( (*indexIter).schema );
                args.add( (*indexIter).table );
                args.add( (*indexIter).index );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
                return true;
            }
            if (tableColNameList.size() > 1)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint validation for delete: only one column foreign key is supported ");
                args.add( (*indexIter).table );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
                return true;
            }
            
            // construct execution plan
            CalpontSelectExecutionPlan csep;          
            CalpontSelectExecutionPlan::ReturnedColumnList returnedColumnList;
            CalpontSelectExecutionPlan::FilterTokenList filterTokenList;
            CalpontSelectExecutionPlan::ColumnMap colMap;  
            
            
            string refColName = tableColNameList[0].schema+"."+tableColNameList[0].table+"."+tableColNameList[0].column;
            SimpleColumn* refCol = new SimpleColumn(refColName, fSessionID);

            SRCP srcp(refCol);
            colMap.insert(CalpontSelectExecutionPlan::ColumnMap::value_type(refColName, srcp));
            csep.columnMapNonStatic(colMap);
            
            ConstantFilter *cf = new ConstantFilter();
            SSC ssc(refCol->clone());
            cf->col(ssc);
            cf->op(opor);
            
            vector<string>::const_iterator valIter = valList.begin();
            for (; valIter != valList.end(); valIter++)
            {
                SimpleFilter *sf = new SimpleFilter(opeq,
                                                    refCol->clone(),
                                                    new ConstantColumn((*valIter), ConstantColumn::LITERAL)); // the datatype chould be NUM as well. but joblist does not care this field. 
                cf->pushFilter(sf);                                                
            }
            filterTokenList.push_back(cf);
            csep.filterTokenList(filterTokenList); 
            
            CalpontSystemCatalog::NJLSysDataList valueList;
	    executePlan (csep, valueList,
		cat->tableRID(execplan::make_table(tableColNameList[0].schema, tableColNameList[0].table)).objnum);            
        
            vector<ColumnResult*>::const_iterator it;
            for (it = valueList.begin(); it != valueList.end(); it++)
            {
                if ((*it)->dataCount() == 0)
                    violatesConstraint = true;
            }
                        
            if (violatesConstraint)
            {
                logging::Message::Args args;
                logging::Message message(5);
                args.add("Reference constraint violation: ");
                args.add( refColName );
                message.format( args );
                result.result = REFERENCE_VIOLATION;
                result.message = message;
            }
            return violatesConstraint;
        }
        return violatesConstraint;
    }
#endif
    
    void DMLPackageProcessor::executePlan(CalpontSelectExecutionPlan& csep,
                                          CalpontSystemCatalog::NJLSysDataList& valueList,
					  const execplan::CalpontSystemCatalog::OID& tableOID)
    {    
        // start up new transaction
        SessionManager sm;
        BRM::TxnID txnID;
        txnID = sm.getTxnID(fSessionID);
        if (!txnID.valid)
        {
            txnID.id = 0;
            txnID.valid = true;
        }
		TableBand band;
        CalpontSystemCatalog::SCN verID;
        verID = sm.verID();
        csep.txnID(txnID.id);
        csep.verID(verID);
        csep.sessionID(fSessionID); 

	CalpontSystemCatalog* csc = CalpontSystemCatalog::makeCalpontSystemCatalog(fSessionID);
	csc->identity(CalpontSystemCatalog::EC);   
        ResourceManager rm;            
        DistributedEngineComm *ec = DistributedEngineComm::instance(rm);
        ec->Open();
        joblist::SJLP jl = JobListFactory::makeJobList(&csep, rm);

        jl->putEngineComm(ec);
		jl->doQuery();
		do {
			band = jl->projectTable(tableOID);
			band.convertToSysDataList(valueList, csc);
		} while (band.getRowCount() != 0);
		jl.reset();
    }


    boost::any DMLPackageProcessor::tokenizeData( execplan::CalpontSystemCatalog::SCN txnID,
        execplan::CalpontSystemCatalog::ColType colType,
        const std::string& data, DMLResult& result, bool isNULL )
    {
        SUMMARY_INFO("DMLPackageProcessor::tokenizeData");

        bool retval = true;
        boost::any value;

        if (isNULL)
        {
            WriteEngine::Token nullToken;
            value = nullToken;
        }
        else
        {
	    if ( data.length() > (unsigned int)colType.colWidth )
	    {
		retval = false;
                // build the logging message
                logging::Message::Args args;
                logging::Message message(6);
                args.add("Insert value is too large for colum ");
                message.format( args );

                result.result = INSERT_ERROR;
                result.message = message;
	    }
	    else
	    {
            	//Tokenize the data value
            	WriteEngine::DctnryStruct dictStruct;
            	dictStruct.dctnryOid = colType.ddn.dictOID;
            	//cout << "Dictionary OIDs: " << colType.ddn.treeOID << " " << colType.ddn.listOID << endl;
            	WriteEngine::DctnryTuple  dictTuple;
            	memcpy(dictTuple.sigValue, data.c_str(), data.length());
            	dictTuple.sigSize = data.length();
            	int error = NO_ERROR;
            	if ( NO_ERROR != (error = fWriteEngine.tokenize( txnID, dictStruct, dictTuple)) )
            	{
                	retval = false;
                	//cout << "Error code from WE: " << error << endl;
                	// build the logging message
                	logging::Message::Args args;
                	logging::Message message(1);
                	args.add("Tokenization failed on: ");
                	args.add(data);
                	args.add("error number: ");
                	args.add( error );
                	message.format( args );

                	result.result = TOKEN_ERROR;
                	result.message = message;
            	}
            	WriteEngine::Token aToken = dictTuple.token;
            	value = aToken;
	    }

        }
        return value;
    }

    void DMLPackageProcessor::getColumnsForTable(u_int32_t sessionID, std::string schema,
        std::string table,
	dmlpackage::ColumnList& colList)
    {

        CalpontSystemCatalog::TableName tableName;
        tableName.schema = schema;
        tableName.table = table;

        CalpontSystemCatalog* systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog( sessionID );
        CalpontSystemCatalog::RIDList ridList = systemCatalogPtr->columnRIDs(tableName, true);

        CalpontSystemCatalog::RIDList::const_iterator rid_iterator = ridList.begin();
        while (rid_iterator != ridList.end())
        {
            CalpontSystemCatalog::ROPair roPair = *rid_iterator;
            DMLColumn* columnPtr = new DMLColumn();
            CalpontSystemCatalog::TableColName tblColName = systemCatalogPtr->colName( roPair.objnum );
            columnPtr->set_Name(tblColName.column);

            colList.push_back(columnPtr);

            ++rid_iterator;
        }

    }
    
    char* DMLPackageProcessor::strlower(char* in)
    {
      char* p = in;
      if (p)
      {
    	while (*p)
    	{
    	  *p = tolower(*p);
    	  p++;
    	}
      }
      return in;
    }
	
	

	void 	DMLPackageProcessor::convertRidToColumn (u_int64_t& rid, unsigned& dbRoot, unsigned& partition, 
														unsigned& segment, unsigned filesPerColumnPartition, 
														unsigned  extentsPerSegmentFile, unsigned extentRows, 
														unsigned startDBRoot, unsigned dbrootCnt, const unsigned startPartitionNum )
	{
		partition = rid / ( filesPerColumnPartition * extentsPerSegmentFile * extentRows );
		
		segment =( ( ( rid % ( filesPerColumnPartition * extentsPerSegmentFile * extentRows )) / extentRows ) ) % filesPerColumnPartition;
		
		dbRoot = ((startDBRoot - 1 + segment) % dbrootCnt) + 1;
		
		//Calculate the relative rid for this segment file
        u_int64_t relRidInPartition = rid - ((u_int64_t)partition * (u_int64_t)filesPerColumnPartition * (u_int64_t)extentsPerSegmentFile * (u_int64_t)extentRows);
		assert ( relRidInPartition <= (u_int64_t)filesPerColumnPartition * (u_int64_t)extentsPerSegmentFile * (u_int64_t)extentRows );
        uint32_t numExtentsInThisPart = relRidInPartition / extentRows;
        unsigned numExtentsInThisSegPart = numExtentsInThisPart / filesPerColumnPartition;
        u_int64_t relRidInThisExtent = relRidInPartition - numExtentsInThisPart * extentRows;
        rid = relRidInThisExtent +  numExtentsInThisSegPart * extentRows;
	}

    
	string DMLPackageProcessor::projectTableErrCodeToMsg(uint ec)
	{
		if (ec < 1000) // pre IDB error code
		{
			ErrorCodes ecObj;
			string errMsg("Statement failed.");
			errMsg += ecObj.errorString(ec).substr(150); // substr removes ErrorCodes::fPreamble
			return errMsg;
		}

		// IDB error
		return IDBErrorInfo::instance()->errorMsg(ec);
	}
	
	bool DMLPackageProcessor::validateNextValue(execplan::CalpontSystemCatalog::ColType colType, int64_t value, bool & offByOne)
	{
		bool validValue = true;
		offByOne = false;
		switch (colType.colDataType)
		{
			case CalpontSystemCatalog::BIGINT:
				{
					if (value > MAX_BIGINT)
					{
						validValue = false;
						if ((value-1) == MAX_BIGINT)
							offByOne = true;
					}
				}
				break;
			case CalpontSystemCatalog::INT:
				{
					if (value > MAX_INT)
					{
						validValue = false;
						if ((value-1) == MAX_INT)
							offByOne = true; 
					}
				}
				break;
			case CalpontSystemCatalog::SMALLINT:
			{
				if (value > MAX_SMALLINT)
				{
					validValue = false;
					if ((value-1) == MAX_SMALLINT)
						offByOne = true; 
				}
			}
			break;
			case CalpontSystemCatalog::TINYINT:
			{
				if (value > MAX_TINYINT)
				{
					validValue = false;
					if ((value-1) == MAX_TINYINT)
						offByOne = true; 
				}
			}
			break;
			default:
				validValue = false;
			break;
		}
		return validValue;
	}
	
	
	bool DMLPackageProcessor::validateVarbinaryVal( std::string & inStr)
	{
		bool invalid = false;
		for (unsigned i=0; i < inStr.length(); i++)
		{
			if (!isxdigit(inStr[i]))
			{
				invalid = true;
				break;
			}
		
		}
		return invalid;
	}

}
