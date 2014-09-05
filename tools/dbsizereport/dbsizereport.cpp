/* Copyright (C) 2013 Calpont Corp. */


/*
* $Id: dbsizereport.cpp 2124 2013-04-10 13:37:00Z chao $
*/

#include <iostream>
#include <vector>
#include <algorithm>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <string>
#include <unistd.h>
using namespace std;

#include <boost/thread/mutex.hpp>
#include <boost/shared_ptr.hpp>
using namespace boost;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "threadpool.h"
using namespace threadpool;

#include "liboamcpp.h"
using namespace oam;

#include "resourcemanager.h"
#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "we_messages.h"
using namespace WriteEngine;

#include "atomicops.h"

namespace {
boost::mutex outputLock;
bool iflg = false;
bool cflg = false;
bool sflg = false;
bool tflg = false;
bool rflag = false;
double totalSize = 0;
uint32_t maxSchemaLen = 10;
uint32_t maxTableLen = 10;
struct TableInfo
{
	string schemaName;
	string tableName;
	bool operator < (const TableInfo& rhs) const
	{
		if (schemaName <  rhs.schemaName)
			return true;
		if (schemaName == rhs.schemaName && tableName < rhs.tableName)
			return true;		
		return false;
	}
};

typedef std::vector<TableInfo> TableList;
struct ColumnInfo
{
	string columnName;
	int32_t columnOid;
	CalpontSystemCatalog::ColType colType;
	bool operator < (const ColumnInfo& rhs) const
	{
		if (columnName <  rhs.columnName)
			return true;		
		return false;
	}
};
typedef std::vector<ColumnInfo> ColumnList;
struct FileInfo
{
        uint32_t       partition;          /** @brief Partition for a file*/
        uint16_t       segment;            /** @brief Segment for a file */
        uint16_t       dbRoot;             /** @brief DbRoot for a file */
        string         segFileName;        /** @brief seg file path */
		double  	   fileSize;		   /** @brief seg file size in giga bytes */
		void deserialize(ByteStream & bs)
		{
			bs >> partition;
			bs >> segment;
			bs >> dbRoot;
			bs >> segFileName;
			bs >> (uint64_t&)fileSize;
		}
};
//------------------------------------------------------------------------------
// Display usage
//------------------------------------------------------------------------------
void usage()
{
	cout << "Usage: databaseSizeReport [options]" << endl;
	cout << "   -h                           \tdisplay this help" << endl;
	cout << "   -s schemaName                \tdisplay all tables in the schema" << endl;
	cout << "   -s schemaName -t tableName   \tdisplay the table only" << endl;
	cout << "   -c                           \tdisplay all tables in the database at column level" << endl;	
	cout << "   -r                           \treport compressed bytes without including unconsumed preallocated file space" << endl;
	cout << "   -i                           \tformat the output to be used by import to the following table:" << endl;
	cout << "        CREATE TABLE dbsize( tableschema varchar(128), tablename varchar(128),columnName varchar(128),  "<<endl;
	cout << "        dataType int, columnWidth int, dbroot int, partition int, segment int, filename varchar(255),"<<endl;
	cout << "        size double ) ENGINE=InfiniDB;" << endl;
	cout << "\nSizes are reported in gigabytes." << endl;
}

class ActiveThreadCounter
{
public:
	ActiveThreadCounter(int size) : factiveThreadCount(size){}
	virtual ~ActiveThreadCounter() {}

	void decr()
	{
		int atc;
		for (;;) {
			atomicops::atomicMb();
			atc = factiveThreadCount;
			if (atc <= 0)		//hopefully atc will never be < 0!
				return;
			if (atomicops::atomicCAS(&factiveThreadCount, atc, (atc - 1)))
				return;
			atomicops::atomicYield();
		}
	}

	uint32_t cur()
	{
		return factiveThreadCount;
	}

private:
	ActiveThreadCounter(const ActiveThreadCounter& rhs);
	ActiveThreadCounter& operator=(const ActiveThreadCounter& rhs);

	volatile int32_t factiveThreadCount;
};

ActiveThreadCounter *activeThreadCounter;

class reportTableThread
{
public:
	reportTableThread(string schemaName, string aTablename);
	virtual ~reportTableThread();
	virtual void operator()();
	
private:	
	string fSchema;
	string fTable;
};

reportTableThread::reportTableThread(string schemaName, string aTablename) : fSchema(schemaName), fTable(aTablename)
{
}
reportTableThread::~reportTableThread()
{
}

void reportTableThread::operator()()
{
		boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
		CalpontSystemCatalog::TableName tableName;
		tableName.schema = fSchema;
		tableName.table = fTable;
		CalpontSystemCatalog::RIDList columnList;
		try {
			columnList = systemCatalogPtr->columnRIDs(tableName);
		}
		catch ( ...)
		{
			cerr << "Error in getting table information from systables." << endl;
			exit(1);
		}
		
		ColumnList columns;
		ColumnInfo aColumn;
		CalpontSystemCatalog::TableColName tableColName;
		uint32_t maxColLen = 40;
		for (uint32_t i=0; i < columnList.size(); i++)
		{
			//cout << "column oid = " <<  columnList[i].objnum << endl;
			aColumn.columnOid = columnList[i].objnum;
			aColumn.colType = systemCatalogPtr->colType(columnList[i].objnum);
			tableColName = systemCatalogPtr->colName(columnList[i].objnum);
			aColumn.columnName = tableColName.column;
			columns.push_back(aColumn);
		} 
	
		//set up connection to WES
		joblist::ResourceManager rm;
		oam::Oam oam;
		string ipAddress;
		ModuleTypeConfig moduletypeconfig;
		try {
			oam.getSystemConfig("pm", moduletypeconfig);
		} catch (...)
		{
			cerr << "Error in getting system config." << endl;
			exit(1);
		}

		uint32_t pmCountConfig =  moduletypeconfig.ModuleCount;
		int moduleID = 1;
	
		char buff[32];
		
		typedef std::vector<boost::shared_ptr<messageqcpp::MessageQueueClient> > WESClientList;
		
		WESClientList ClientList;
		
		for (unsigned i = 0; i < pmCountConfig; i++) {
			//Find the module id
			moduleID = atoi((moduletypeconfig.ModuleNetworkList[i]).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
			snprintf(buff, sizeof(buff), "pm%u_WriteEngineServer", moduleID);
			string fServer (buff);

			boost::shared_ptr<MessageQueueClient> cl (new MessageQueueClient(fServer, rm.getConfig()));
			
			try {
				if (cl->connect()) {
					//cout << "connect to " << fServer << endl;
					ClientList.push_back(cl);
				}
				else
					cerr << "Could not connect to " << fServer << endl;
			}
			catch (std::exception& ex) {
				cerr << "Could not connect to " << fServer << ": " << ex.what() << endl;
				exit(1);
			}
			catch (...) {
				cerr << "Could not connect to " << fServer << endl;
				exit(1);
			}
		}
		if (ClientList.size() == 0)
		{
			cerr << "There is no performance module to send the request." << endl;
			exit(1);
		}
		//send table infomation to WES
		ByteStream bs, bsRead;
		bs << (ByteStream::byte) WE_SVR_GET_FILESIZES;
		bs << fSchema;
		bs << fTable;
		bs << (ByteStream::byte) rflag;
		//WESClientList::iterator itor = ClientList.begin();
		//cout  <<"thread " << pthread_self() << " is sending to WEs." << endl;
		for (uint32_t i=0; i < ClientList.size(); i++)
		{	
			 ClientList[i]->write(bs);
		}
		
		typedef std::vector<FileInfo> Files;
		typedef std::map<uint32_t, Files> columnMap;
		columnMap columnsMap;
		FileInfo fileInfo;
		ByteStream::byte rc = 0;
		string errMsg;
		uint32_t columnOid = 0;
		Files aColumnFiles;
		uint64_t size;
		columnMap::iterator iter = columnsMap.begin();
		try {
			for (uint32_t i=0; i < ClientList.size(); i++)
			{
				bsRead = ClientList[i]->read();
				if ( bsRead.length() == 0 ) //read error
				{
					cerr << "Lost connection to WES.";	
					exit(1);					
				}			
				else
				{
					//cout  <<"thread " << pthread_self() << " got reply from WEs." << endl;
					//parse the message
					bsRead >> rc;
					bsRead >> errMsg;
					if ( rc != 0 )
					{
						cerr << errMsg;
						exit(1);
					}
					else
					{
						//cout  <<"thread " << pthread_self() << " got reply from WEs with length " << bsRead.length() << endl;
						while ( bsRead.length() > 0 )
						{
							bsRead >> columnOid;
							bsRead >> size;
							aColumnFiles.clear();
							for (uint32_t i = 0; i < size; i++) {
								fileInfo.deserialize(bsRead);
								aColumnFiles.push_back(fileInfo);
							}
							
							iter = columnsMap.find(columnOid);
							if ( iter != columnsMap.end() )
							{
								iter->second.insert( iter->second.end(), aColumnFiles.begin(), aColumnFiles.end() );
								//cout  <<"thread " << pthread_self() << " adding data to columnmap." << endl;
							}
							else
							{
								columnsMap[columnOid] = aColumnFiles;
								//cout  <<"thread " << pthread_self() << " assigning data to columnmap." << endl;
							}
						}
					}				
				}
			}
		}
		catch (std::exception&)
		{
			cerr << "Exception on communicating to WES " << endl;	
			exit(1);
		}
		
		//Close connections
		ClientList.clear();
		
		//prepare the report	
		if (tflg && sflg && !cflg && !iflg) // -s -t, table level
		{
			double tableSize = 0.0;
			for (iter = columnsMap.begin(); iter != columnsMap.end(); iter++)
			{
				//cout << "column oid " << iter->first << ":" << endl;
				for (uint32_t i=0; i < iter->second.size(); i++)
				{
					tableSize += iter->second[i].fileSize;
				}
			}
			boost::mutex::scoped_lock lk(outputLock);
			cout.precision(6);
			cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable <<  " " << fixed << tableSize <<" GB" << endl;
			totalSize += tableSize;
		}
		else if (tflg && sflg && cflg && !iflg)
		{
			double tableSize = 0.0;
			CalpontSystemCatalog::TableColName aColName;
			boost::mutex::scoped_lock lk(outputLock);
			cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(maxColLen)
				<< left << "Column" << " " << setw(16) << left << "Size" <<endl;
			
			for (uint32_t i=0; i < columns.size(); i++)
			{
				iter = columnsMap.find( columns[i].columnOid);
				if (iter != columnsMap.end())
				{
					if (columns[i].colType.ddn.dictOID > 0)
					{
						string colName = columns[i].columnName + " (token)";
						cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " 
							<< setw(maxColLen) << left << colName << " ";
					}
					else
						cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " 
							<< setw(maxColLen) << left << columns[i].columnName << " ";
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					cout.precision(6);
					cout << fixed << columnSize <<" GB" << endl;
					tableSize += columnSize;
				}
				iter = columnsMap.find( columns[i].colType.ddn.dictOID);
				if (iter != columnsMap.end())
				{
					string colName = columns[i].columnName + " (string)";
					cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " 
						<< setw(maxColLen) << left << colName << " ";
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					cout.precision(6);
					cout << fixed << columnSize <<" GB" << endl;
					tableSize += columnSize;
				}	
			}
			cout.precision(6);
			totalSize += tableSize;
			//cout << "Table total size:\t" << fixed << tableSize <<" GB" << endl;
		}
		else if (!tflg && sflg && cflg && !iflg)
		{
			double tableSize = 0.0;
			CalpontSystemCatalog::TableColName aColName;
			boost::mutex::scoped_lock lk(outputLock);
			//cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(maxColLen)
			//	<< left << "Column" << " " << setw(16) << left << "Size" <<endl;
			
			for (uint32_t i=0; i < columns.size(); i++)
			{
				iter = columnsMap.find( columns[i].columnOid);
				if (iter != columnsMap.end())
				{
					if (columns[i].colType.ddn.dictOID > 0)
					{
						string colName = columns[i].columnName + " (token)";
						cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " 
							<< setw(maxColLen) << left << colName << " ";
					}
					else
						cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable 
							<< " " << setw(maxColLen) << left << columns[i].columnName << "\t";
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					cout.precision(6);
					cout << fixed << columnSize <<" GB" << endl;
					tableSize += columnSize;
				}
				iter = columnsMap.find( columns[i].colType.ddn.dictOID);
				if (iter != columnsMap.end())
				{
					string colName = columns[i].columnName + " (string)";
					cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " 
						<< setw(maxColLen) << left << colName << " ";
						
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					cout.precision(6);
					cout << fixed << columnSize <<" GB" << endl;
					tableSize += columnSize;
				}	
			}
			totalSize += tableSize;	
		}
		else if (tflg && iflg ) //column level is implied. cpimport format is displayed
		{
			boost::mutex::scoped_lock lk(outputLock);
			for (uint32_t j=0; j < columns.size(); j++)
			{
				iter = columnsMap.find( columns[j].columnOid);
				if (iter != columnsMap.end())
				{
					string colName = columns[j].columnName;
					if (columns[j].colType.ddn.dictOID > 0)
						colName += " (token)";
						
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						cout <<fSchema<<"|"<<fTable<<"|"<<colName <<"|"<< columns[j].colType.colDataType<< "|"
							<<columns[j].colType.colWidth <<"|" << iter->second[i].dbRoot <<"|"<<iter->second[i].partition<<"|"<< iter->second[i].segment
							<<"|"<<iter->second[i].segFileName<<"|"<<fixed<<iter->second[i].fileSize <<"|"<< endl;
					}
				}
				iter = columnsMap.find( columns[j].colType.ddn.dictOID);
				if (iter != columnsMap.end())
				{
					string colName = columns[j].columnName + " (string)";
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						cout <<fSchema<<"|"<<fTable<<"|"<<colName <<"|"<< columns[j].colType.colDataType<< "|"
							<<columns[j].colType.colWidth <<"|" << iter->second[i].dbRoot <<"|"<<iter->second[i].partition<<"|"<< iter->second[i].segment
							<<"|"<<iter->second[i].segFileName<<"|"<<fixed<<iter->second[i].fileSize <<"|"<< endl;
					}
				}	
			}
		}
		else if (iflg) //whole Database report, bar delimited
		{
			boost::mutex::scoped_lock lk(outputLock);
			for (uint32_t j=0; j < columns.size(); j++)
			{
				iter = columnsMap.find( columns[j].columnOid);
				if (iter != columnsMap.end())
				{
					string colName = columns[j].columnName;
					if (columns[j].colType.ddn.dictOID > 0)
						colName += " (token)";
						
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						cout <<fSchema<<"|"<< fTable<<"|"<< colName <<"|"<< columns[j].colType.colDataType<< "|"
							<<columns[j].colType.colWidth <<"|" << iter->second[i].dbRoot <<"|"<<iter->second[i].partition<<"|"<< iter->second[i].segment
							<<"|"<<iter->second[i].segFileName<<"|"<<fixed<<iter->second[i].fileSize <<"|"<< endl;
					}
				}
				iter = columnsMap.find( columns[j].colType.ddn.dictOID);
				if (iter != columnsMap.end())
				{
					string colName = columns[j].columnName + " (string)";
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						cout <<fSchema<<"|"<<fTable<<"|"<<colName <<"|"<< columns[j].colType.colDataType<< "|"
							<<columns[j].colType.colWidth <<"|" << iter->second[i].dbRoot <<"|"<<iter->second[i].partition<<"|"<< iter->second[i].segment
							<<"|"<<iter->second[i].segFileName<<"|"<<fixed<<iter->second[i].fileSize <<"|"<< endl;
					}
				}	
			}
		}
		else if (cflg)//whole Database or schema report, column level
		{
			boost::mutex::scoped_lock lk(outputLock);
			double tableSize = 0.0;	
			
			for (uint32_t i=0; i < columns.size(); i++)
			{
				iter = columnsMap.find( columns[i].columnOid);
				if (iter != columnsMap.end())
				{
					string colName = columns[i].columnName;
					if (columns[i].colType.ddn.dictOID > 0)
						colName += " (token)";
					cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " << setw(maxColLen) << left << colName << " ";
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					cout.precision(6);
					cout << fixed << columnSize <<" GB" << endl;
					tableSize += columnSize;
				}
				iter = columnsMap.find( columns[i].colType.ddn.dictOID);
				if (iter != columnsMap.end())
				{
					string colName = columns[i].columnName + " (string)";
					cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " "  << setw(maxColLen) << left << colName << " ";
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					cout.precision(6);
					cout << fixed << columnSize <<" GB" << endl;
					tableSize += columnSize;
				}	
			}
			totalSize += tableSize;		
		}
		else //whole Database or schema report, table level
		{
			boost::mutex::scoped_lock lk(outputLock);
			double tableSize = 0.0;	
			
			for (uint32_t i=0; i < columns.size(); i++)
			{
				iter = columnsMap.find( columns[i].columnOid);
				if (iter != columnsMap.end())
				{
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					tableSize += columnSize;
				}
				iter = columnsMap.find( columns[i].colType.ddn.dictOID);
				if (iter != columnsMap.end())
				{
					double columnSize = 0.0;
					for (uint32_t i=0; i < iter->second.size(); i++)
					{
						columnSize += iter->second[i].fileSize;
					}
					tableSize += columnSize;
				}	
			}
			cout.precision(6);
			cout << setw(maxSchemaLen) << left << fSchema << " " << setw(maxTableLen) << left << fTable << " " << fixed << tableSize <<" GB" << endl;
			totalSize += tableSize;		
		}
		activeThreadCounter->decr();
}

void reportTable(string schemaName, string aTablename, ThreadPool& tp)
{
	if (activeThreadCounter == 0)
		activeThreadCounter = new ActiveThreadCounter(1);
		
	if (tflg && sflg && !cflg && !iflg)
	{
		cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(16) << left << "Size" <<endl;
	}
    reportTableThread reportTableObject(schemaName,aTablename);
	tp.invoke(reportTableObject);
}

void reportWholeDB( ThreadPool& tp)
{
    //get tables in the database
    std::vector< std::pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> > tableList;
    boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
	try {
		tableList = systemCatalogPtr->getTables();
	}
	catch ( ...)
	{
		cerr << "Error in getting table information from systables." << endl;
		exit(1);
	}

	//@Bug 5456. Added systables.
	tableList.push_back( make_pair(SYSTABLE_BASE, make_table(CALPONT_SCHEMA, SYSTABLE_TABLE)));
    tableList.push_back( make_pair(SYSCOLUMN_BASE, make_table(CALPONT_SCHEMA, SYSCOLUMN_TABLE)));
	
	TableList aTableList;
	TableInfo aTable;
	activeThreadCounter = new ActiveThreadCounter(tableList.size());
	for (uint32_t i=0; i < tableList.size(); i++) //build a sorted table list
	{
		aTable.schemaName = tableList[i].second.schema;
		if (maxSchemaLen < aTable.schemaName.length())
			maxSchemaLen = aTable.schemaName.length();
			
		aTable.tableName = tableList[i].second.table;
		if (maxTableLen < aTable.tableName.length())
			maxTableLen = aTable.tableName.length();
			
		aTableList.push_back(aTable);
	}
	sort(aTableList.begin(), aTableList.end());	
	if (!iflg && cflg)
	{
		cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(20)
			<< left << "Column" << " " << setw(16) << left << "Size" <<endl;
	}
	else if (!iflg)
	{
		cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(16) << left << "Size" <<endl;
	}
    for (uint32_t i=0; i < aTableList.size(); i++)
    {
       //cout << "tablename = " << aTableList[i].schemaName << "  " << aTableList[i].tableName<<endl;
	   reportTable(aTableList[i].schemaName, aTableList[i].tableName, tp);
    }
}

void reportSchemaTables(string schemaName,  ThreadPool& tp)
{
    //get tables in the schema
    std::vector< std::pair<CalpontSystemCatalog::OID, CalpontSystemCatalog::TableName> > tableList;
    boost::shared_ptr<CalpontSystemCatalog> systemCatalogPtr = CalpontSystemCatalog::makeCalpontSystemCatalog(0);
	try {
		tableList = systemCatalogPtr->getTables(schemaName);
	}
	catch (...)
	{
		cerr << "Error in getting table information from systables." << endl;
		exit(1);
	}
	activeThreadCounter = new ActiveThreadCounter(tableList.size());
	TableList aTableList;
	TableInfo aTable;
	for (uint32_t i=0; i < tableList.size(); i++) //build a sorted table list
	{
		aTable.schemaName = tableList[i].second.schema;
		if (maxSchemaLen < aTable.schemaName.length())
			maxSchemaLen = aTable.schemaName.length();
			
		aTable.tableName = tableList[i].second.table;
		
		if (maxTableLen < aTable.tableName.length())
			maxTableLen = aTable.tableName.length();
			
		aTableList.push_back(aTable);
	}
	sort(aTableList.begin(), aTableList.end());	
	if (!tflg && sflg && cflg && !iflg) {
		cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(20)
			<< left << "Column" << " " << setw(16) << left << "Size" <<endl;
	}
	else if (sflg && !iflg)
	{
		cout << setw(maxSchemaLen) << left << "Schema" << " " << setw(maxTableLen) << left << "Table" << " " << setw(16) << left << "Size" <<endl;
	}
	
    for (uint32_t i=0; i < aTableList.size(); i++)
    {
       //cout << "tablename = " << aTableList[i].schemaName << "  " << aTableList[i].tableName<<endl;
	   reportTable(aTableList[i].schemaName, aTableList[i].tableName, tp);
    }
}


}

//------------------------------------------------------------------------------
// main entry point into this program
//------------------------------------------------------------------------------
int main(int argc, char** argv)
{
#if !defined(_MSC_VER) && !defined(SKIP_OAM_INIT)
	//check system status before further processing the request
	Oam oam;
    SystemStatus systemstatus;

    try
    {
         oam.getSystemStatus(systemstatus);
         if (systemstatus.SystemOpState != ACTIVE)	
		 {
			cerr << "The system is not ready to process the report." << endl;
			return 1;
		 }			
	}
	catch (...)
    {
		cerr << "oam.getSystemStatus call failed." << endl;
		return 1;
	}
#endif

	int c;
	string schemaName, tableName;
	while ((c = getopt(argc, argv, "s:t:cirh")) != EOF)
		switch (c)
		{
		case 's':
			sflg = true;
			schemaName = optarg;
			break;
		case 't':
			tflg = true;
			tableName = optarg;
			break;
		case 'c':
			cflg = true;
			break;
		case 'i':
			iflg = true;
			break;
		case 'r':
			rflag = true;
			break;
		case 'h':
		case '?':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}
	
	if (tflg && !sflg)
	{
		cerr << "Schema name is required for a table." << endl;
		return 1;
	}
	int serverThreads = 1;
	int serverQueueSize = serverThreads * 100;
	threadpool::ThreadPool tp(serverThreads,serverQueueSize);
		
	
	if (sflg && tflg)
		reportTable(schemaName,tableName, tp);
	else if (sflg)
		reportSchemaTables(schemaName, tp);
	else
		reportWholeDB(tp);
	
	//check whether all threads finish
	const unsigned sleepTime = 100; // sleep 100 milliseconds between checks

	//wait for a thread to make a new ActiveThreadCounter
	while (activeThreadCounter == 0)
		usleep(sleepTime * 1000);

	//wait for threads to finish
	do
	{
		usleep(sleepTime * 1000);
	} while (activeThreadCounter->cur() > 0);
	
	if ( totalSize > 0.0 )
	{
		boost::mutex::scoped_lock lk(outputLock);
		cout.precision(6);
		cout << "Total \t"  << fixed << totalSize <<" GB" << endl;	
	}
	
	return 0;
}
// vim:ts=4 sw=4:

