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

//  $Id: systemcatalog.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include <iostream>
#include <assert.h>
#include <sys/time.h>
using namespace std;

#include "systemcatalog.h"
#include "configcpp.h"
using namespace execplan;

#include "we_colopcompress.h"
using namespace WriteEngine;

#include "resourcemanager.h"
using namespace joblist;

#include "dbbuilder.h"

void SystemCatalog::build()
{
  TxnID txnID = 0;
  int rc;
  //int t= 1000;

  remove();

  cout << "Creating System Catalog..." << endl;
  cout << endl;
  // SYSTABLE

  timeval startTime;
  gettimeofday( &startTime, 0);
  ostringstream msg;
  WErrorCodes ec;

  //------------------------------------------------------------------------------
  // Get the DBRoot count, and rotate the tables through those DBRoots.
  // All the columns in the first table (SYSTABLE) start on DBRoot1, all the
  // columns in the second table (SYSCOLUMN) start on DBRoot2, etc.
  //------------------------------------------------------------------------------
  config::Config* cf   = config::Config::makeConfig();
  string root          = cf->getConfig("SystemConfig","DBRootCount");
  uint32_t dbRootCount = cf->uFromText(root);

  //------------------------------------------------------------------------------
  // Create SYSTABLE table
  //------------------------------------------------------------------------------
  uint32_t dbRoot = 1;
  int compressionType = 0;
  uint32_t partition = 0;
  uint16_t segment=0;
  
  ResourceManager rm;
   std::map<uint32_t,uint32_t> oids;
  if( rm.useHdfs() )
  {
	compressionType = 2;
	oids[OID_SYSTABLE_TABLENAME] = OID_SYSTABLE_TABLENAME;
	oids[DICTOID_SYSTABLE_TABLENAME] = DICTOID_SYSTABLE_TABLENAME;
	oids[OID_SYSTABLE_SCHEMA] = OID_SYSTABLE_SCHEMA;
	oids[DICTOID_SYSTABLE_SCHEMA] = DICTOID_SYSTABLE_SCHEMA;
	oids[OID_SYSTABLE_OBJECTID] = OID_SYSTABLE_OBJECTID;
	oids[OID_SYSTABLE_CREATEDATE] = OID_SYSTABLE_CREATEDATE;
	oids[OID_SYSTABLE_LASTUPDATE] = OID_SYSTABLE_LASTUPDATE;
	oids[OID_SYSTABLE_INIT] = OID_SYSTABLE_INIT;
	oids[OID_SYSTABLE_NEXT] = OID_SYSTABLE_NEXT;
	oids[OID_SYSTABLE_NUMOFROWS] = OID_SYSTABLE_NUMOFROWS;
	oids[OID_SYSTABLE_AVGROWLEN] = OID_SYSTABLE_AVGROWLEN;
	oids[OID_SYSTABLE_NUMOFBLOCKS] = OID_SYSTABLE_NUMOFBLOCKS;
	oids[OID_SYSTABLE_AUTOINCREMENT] = OID_SYSTABLE_AUTOINCREMENT;
  }
  
  fWriteEngine.setTransId(1);
  fWriteEngine.setBulkFlag(true);
  cout << "Creating SYSTABLE" << endl;
  cout << "---------------------------------------" << endl;
  // TableName
  msg << "  Creating TableName column OID: "<< OID_SYSTABLE_TABLENAME;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_TABLENAME, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  msg << "  Creating TableName column dictionary";
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSTABLE_TABLENAME, 65, dbRoot, partition, segment, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Schema
  msg << "  Creating Schema column OID: "<<OID_SYSTABLE_SCHEMA;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_SCHEMA, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));


  msg.str("  Creating Schema column dictionary");
  cout << msg.str() << endl;
  //Dictionary files
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSTABLE_SCHEMA, 65, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ObjectId
  msg << "  Creating ObjectId column OID: " <<OID_SYSTABLE_OBJECTID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_OBJECTID, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  // CreateDate
  msg << "  Creating CreateDate column OID: "<<OID_SYSTABLE_CREATEDATE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_CREATEDATE, CalpontSystemCatalog::DATE, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // LastUpdateDate
  msg << "  Creating LastUpdate column OID: "<<OID_SYSTABLE_LASTUPDATE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_LASTUPDATE, CalpontSystemCatalog::DATE, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // INIT
  msg << "  Creating INIT column OID: "<<OID_SYSTABLE_INIT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_INIT, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // NEXT
  msg << "  Creating NEXT column OID: "<<OID_SYSTABLE_NEXT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_NEXT, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  //NUMOFROWS
  msg << "  Creating NUMOFROWS column OID: "<<OID_SYSTABLE_NUMOFROWS;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_NUMOFROWS, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  //AVGROWLEN
  msg << "  Creating AVGROWLEN column OID: "<<OID_SYSTABLE_AVGROWLEN;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_AVGROWLEN, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  //NUMOFBLOCKS
  msg << "  Creating NUMOFBLOCKS column OID: "<<OID_SYSTABLE_NUMOFBLOCKS;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_NUMOFBLOCKS, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
  
  //AUTOINCREMENT
  msg << "  Creating AUTOINCREMENT column OID: "<<OID_SYSTABLE_AUTOINCREMENT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_AUTOINCREMENT, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
        
  //------------------------------------------------------------------------------
  // Create SYSCOLUMN table
  //------------------------------------------------------------------------------
  //dbRoot++;
  //if (dbRoot > dbRootCount)
  //  dbRoot = 1;

  //SYSCOLUMN
  if( rm.useHdfs() )
  {
	oids[OID_SYSCOLUMN_SCHEMA] = OID_SYSCOLUMN_SCHEMA;
	oids[DICTOID_SYSCOLUMN_SCHEMA] = DICTOID_SYSCOLUMN_SCHEMA;
	oids[OID_SYSCOLUMN_TABLENAME] = OID_SYSCOLUMN_TABLENAME;
	oids[DICTOID_SYSCOLUMN_TABLENAME] = DICTOID_SYSCOLUMN_TABLENAME;
	oids[OID_SYSCOLUMN_COLNAME] = OID_SYSCOLUMN_COLNAME;
	oids[DICTOID_SYSCOLUMN_COLNAME] = DICTOID_SYSCOLUMN_COLNAME;
	oids[OID_SYSCOLUMN_OBJECTID] = OID_SYSCOLUMN_OBJECTID;
	oids[OID_SYSCOLUMN_DICTOID] = OID_SYSCOLUMN_DICTOID;
	oids[OID_SYSCOLUMN_LISTOBJID] = OID_SYSCOLUMN_LISTOBJID;
	oids[OID_SYSCOLUMN_TREEOBJID] = OID_SYSCOLUMN_TREEOBJID;
	oids[OID_SYSCOLUMN_DATATYPE] = OID_SYSCOLUMN_DATATYPE;
	oids[OID_SYSCOLUMN_COLUMNLEN] = OID_SYSCOLUMN_COLUMNLEN;
	oids[OID_SYSCOLUMN_COLUMNPOS] = OID_SYSCOLUMN_COLUMNPOS;
	oids[OID_SYSCOLUMN_LASTUPDATE] = OID_SYSCOLUMN_LASTUPDATE;
	oids[OID_SYSCOLUMN_DEFAULTVAL] = OID_SYSCOLUMN_DEFAULTVAL;
	oids[DICTOID_SYSCOLUMN_DEFAULTVAL] = DICTOID_SYSCOLUMN_DEFAULTVAL;
	oids[OID_SYSCOLUMN_NULLABLE] = OID_SYSCOLUMN_NULLABLE;
	oids[OID_SYSCOLUMN_SCALE] = OID_SYSCOLUMN_SCALE;
	oids[OID_SYSCOLUMN_PRECISION] = OID_SYSCOLUMN_PRECISION;
	oids[OID_SYSCOLUMN_AUTOINC] = OID_SYSCOLUMN_AUTOINC;
	oids[OID_SYSCOLUMN_DISTCOUNT] = OID_SYSCOLUMN_DISTCOUNT;
	oids[OID_SYSCOLUMN_NULLCOUNT] = OID_SYSCOLUMN_NULLCOUNT;
	oids[OID_SYSCOLUMN_MINVALUE] = OID_SYSCOLUMN_MINVALUE;
	oids[DICTOID_SYSCOLUMN_MINVALUE] = DICTOID_SYSCOLUMN_MINVALUE;
	oids[OID_SYSCOLUMN_MAXVALUE] = OID_SYSCOLUMN_MAXVALUE;
	oids[DICTOID_SYSCOLUMN_MAXVALUE] = DICTOID_SYSCOLUMN_MAXVALUE;
	oids[OID_SYSCOLUMN_COMPRESSIONTYPE] = OID_SYSCOLUMN_COMPRESSIONTYPE;
	oids[OID_SYSCOLUMN_NEXTVALUE] = OID_SYSCOLUMN_NEXTVALUE;
  }
  cout<< endl;
  cout << "Creating SYSCOLUMN" << endl;
  // Schema
  cout << "---------------------------------------" << endl;
  msg << "  Creating Schema column OID: "<<OID_SYSCOLUMN_SCHEMA;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_SCHEMA, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating Schema column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_SCHEMA, 65, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // TableName
  msg << "  Creating TableName column OID: "<<OID_SYSCOLUMN_TABLENAME;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_TABLENAME, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating TableName column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_TABLENAME, 65, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ColumnName
  msg << "  Creating ColumnName column OID: "<<OID_SYSCOLUMN_COLNAME;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COLNAME, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating ColumnName column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_COLNAME, 65, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ObjectID
  msg << "  Creating ObjectID column OID: "<<OID_SYSCOLUMN_OBJECTID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_OBJECTID, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DictOID
  msg << "  Creating DictOID column OID: "<<OID_SYSCOLUMN_DICTOID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DICTOID, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ListOID
  msg << "  Creating ListOID column OID: "<< OID_SYSCOLUMN_LISTOBJID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_LISTOBJID, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // TreeOID
  msg << "  Creating TreeOID column OID: "<< OID_SYSCOLUMN_TREEOBJID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_TREEOBJID, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DataType
  msg << "  Creating DataType column OID: "<< OID_SYSCOLUMN_DATATYPE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DATATYPE, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ColumnLength
  msg << "  Creating ColumnLength column OID: "<< OID_SYSCOLUMN_COLUMNLEN;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COLUMNLEN, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ColumnPos
  msg << "  Creating ColumnPos column OID: "<<OID_SYSCOLUMN_COLUMNPOS;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COLUMNPOS, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
  
  // LastUpdate
  msg << "  Creating LastUpdate column OID: "<< OID_SYSCOLUMN_LASTUPDATE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_LASTUPDATE, CalpontSystemCatalog::DATE, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DefaultValue
  msg << "  Creating DefaultValue column OID: "<< OID_SYSCOLUMN_DEFAULTVAL;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DEFAULTVAL, CalpontSystemCatalog::VARCHAR, 8, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  msg.str("  Creating DefaultValue column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_DEFAULTVAL, 9, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Nullable
  msg << "  Creating Nullable column OID: "<<OID_SYSCOLUMN_NULLABLE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_NULLABLE, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Scale
  msg << "  Creating Scale column OID: "<<OID_SYSCOLUMN_SCALE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_SCALE, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Precision
  msg << "  Creating Precision column OID: "<<OID_SYSCOLUMN_PRECISION;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_PRECISION, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // AutoInc
  msg << "  Creating AutoInc column OID: "<<OID_SYSCOLUMN_AUTOINC;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_AUTOINC, CalpontSystemCatalog::CHAR, 1, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DISTCOUNT
  msg << "  Creating DISTCOUNT column OID: "<<OID_SYSCOLUMN_DISTCOUNT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DISTCOUNT, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
   
  // NULLCOUNT
  msg << "  Creating NULLCOUNT column OID: "<<OID_SYSCOLUMN_NULLCOUNT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_NULLCOUNT, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  // MINVALUE
  msg << "  Creating MINVALUE column OID: "<<OID_SYSCOLUMN_MINVALUE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_MINVALUE, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating MINVALUE column dictionary...");
  cout << msg.str() << endl;
  //Dictionary files
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_MINVALUE, 65, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  // MAXVALUE
  msg << "  Creating MAXVALUE column OID: "<<OID_SYSCOLUMN_MAXVALUE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_MAXVALUE, CalpontSystemCatalog::VARCHAR, 40, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating MAXVALUE column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_MAXVALUE, 65, dbRoot, partition, segment, compressionType);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
   
  // CompressionType
  msg << "  Creating CompressionType column OID: "<<OID_SYSCOLUMN_COMPRESSIONTYPE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COMPRESSIONTYPE, CalpontSystemCatalog::INT, 4, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");   
  
  // nextvalue
  msg << "  Creating NEXTVALUE column OID: "<<OID_SYSCOLUMN_NEXTVALUE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_NEXTVALUE, CalpontSystemCatalog::UBIGINT, 8, dbRoot, partition, compressionType );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");   
  
  //------------------------------------------------------------------------------
  // Create SYSCONSTRAINT table
  //------------------------------------------------------------------------------
  dbRoot++;
  if (dbRoot > dbRootCount)
    dbRoot = 1;

//flush data files
  fWriteEngine.flushDataFiles(rc, 1, oids);
  // save brm
  msg.str("  BRMWrapper saving state ");

  rc = BRMWrapper::getInstance()->saveState();
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
 
  timeval endTime;
  gettimeofday( &endTime, 0);
  double elapsedTime =
    (endTime.tv_sec   + (endTime.tv_usec   / 1000000.0)) -
    (startTime.tv_sec + (startTime.tv_usec / 1000000.0));
  cout << "System Catalog creation took: " << elapsedTime <<
    " seconds to complete." << endl;

  cout << endl;
  cout << "System Catalog created" << endl;
  cout << endl;
}

void SystemCatalog::remove()
{
  ColumnOpCompress0 colOp;

  for ( int c = 1001; c <= 1074; c++ )
    colOp.deleteFile( c );
  for ( int d = 2001; d <= 2312; d++ )
    colOp.deleteFile( d );

}
