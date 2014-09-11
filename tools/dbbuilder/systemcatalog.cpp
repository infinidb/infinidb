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

//  $Id: systemcatalog.cpp 1422 2011-03-09 21:42:52Z chao $

#include <iostream>
#include <assert.h>
#include <sys/time.h>
using namespace std;

#include "systemcatalog.h"
#include "configcpp.h"
using namespace execplan;

#include "we_colopcompress.h"
using namespace WriteEngine;

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

  cout << "Creating SYSTABLE" << endl;
  cout << "---------------------------------------" << endl;
  // TableName
  msg << "  Creating TableName column OID: "<< OID_SYSTABLE_TABLENAME;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_TABLENAME, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  msg << "  Creating TableName column dictionary";
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSTABLE_TABLENAME, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Schema
  msg << "  Creating Schema column OID: "<<OID_SYSTABLE_SCHEMA;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_SCHEMA, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));


  msg.str("  Creating Schema column dictionary");
  cout << msg.str() << endl;
  //Dictionary files
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSTABLE_SCHEMA, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ObjectId
  msg << "  Creating ObjectId column OID: " <<OID_SYSTABLE_OBJECTID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_OBJECTID, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  // CreateDate
  msg << "  Creating CreateDate column OID: "<<OID_SYSTABLE_CREATEDATE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_CREATEDATE, WriteEngine::DATE, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // LastUpdateDate
  msg << "  Creating LastUpdate column OID: "<<OID_SYSTABLE_LASTUPDATE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_LASTUPDATE, WriteEngine::DATE, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // INIT
  msg << "  Creating INIT column OID: "<<OID_SYSTABLE_INIT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_INIT, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // NEXT
  msg << "  Creating NEXT column OID: "<<OID_SYSTABLE_NEXT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_NEXT, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  //NUMOFROWS
  msg << "  Creating NUMOFROWS column OID: "<<OID_SYSTABLE_NUMOFROWS;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_NUMOFROWS, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  //AVGROWLEN
  msg << "  Creating AVGROWLEN column OID: "<<OID_SYSTABLE_AVGROWLEN;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_AVGROWLEN, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  //NUMOFBLOCKS
  msg << "  Creating NUMOFBLOCKS column OID: "<<OID_SYSTABLE_NUMOFBLOCKS;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_NUMOFBLOCKS, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
  
  //AUTOINCREMENT
  msg << "  Creating AUTOINCREMENT column OID: "<<OID_SYSTABLE_AUTOINCREMENT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSTABLE_AUTOINCREMENT, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
        
  //------------------------------------------------------------------------------
  // Create SYSCOLUMN table
  //------------------------------------------------------------------------------
  dbRoot++;
  if (dbRoot > dbRootCount)
    dbRoot = 1;

  //SYSCOLUMN
  cout<< endl;
  cout << "Creating SYSCOLUMN" << endl;
  // Schema
  cout << "---------------------------------------" << endl;
  msg << "  Creating Schema column OID: "<<OID_SYSCOLUMN_SCHEMA;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_SCHEMA, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating Schema column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_SCHEMA, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // TableName
  msg << "  Creating TableName column OID: "<<OID_SYSCOLUMN_TABLENAME;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_TABLENAME, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating TableName column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_TABLENAME, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ColumnName
  msg << "  Creating ColumnName column OID: "<<OID_SYSCOLUMN_COLNAME;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COLNAME, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating ColumnName column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_COLNAME, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ObjectID
  msg << "  Creating ObjectID column OID: "<<OID_SYSCOLUMN_OBJECTID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_OBJECTID, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DictOID
  msg << "  Creating DictOID column OID: "<<OID_SYSCOLUMN_DICTOID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DICTOID, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ListOID
  msg << "  Creating ListOID column OID: "<< OID_SYSCOLUMN_LISTOBJID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_LISTOBJID, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // TreeOID
  msg << "  Creating TreeOID column OID: "<< OID_SYSCOLUMN_TREEOBJID;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_TREEOBJID, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DataType
  msg << "  Creating DataType column OID: "<< OID_SYSCOLUMN_DATATYPE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DATATYPE, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ColumnLength
  msg << "  Creating ColumnLength column OID: "<< OID_SYSCOLUMN_COLUMNLEN;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COLUMNLEN, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // ColumnPos
  msg << "  Creating ColumnPos column OID: "<<OID_SYSCOLUMN_COLUMNPOS;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COLUMNPOS, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
  
  // LastUpdate
  msg << "  Creating LastUpdate column OID: "<< OID_SYSCOLUMN_LASTUPDATE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_LASTUPDATE, WriteEngine::DATE, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DefaultValue
  msg << "  Creating DefaultValue column OID: "<< OID_SYSCOLUMN_DEFAULTVAL;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DEFAULTVAL, WriteEngine::VARCHAR, 8, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  msg.str("  Creating DefaultValue column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_DEFAULTVAL, 9, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Nullable
  msg << "  Creating Nullable column OID: "<<OID_SYSCOLUMN_NULLABLE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_NULLABLE, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Scale
  msg << "  Creating Scale column OID: "<<OID_SYSCOLUMN_SCALE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_SCALE, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // Precision
  msg << "  Creating Precision column OID: "<<OID_SYSCOLUMN_PRECISION;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_PRECISION, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // AutoInc
  msg << "  Creating AutoInc column OID: "<<OID_SYSCOLUMN_AUTOINC;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_AUTOINC, WriteEngine::CHAR, 1, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");

  // DISTCOUNT
  msg << "  Creating DISTCOUNT column OID: "<<OID_SYSCOLUMN_DISTCOUNT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_DISTCOUNT, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
   
  // NULLCOUNT
  msg << "  Creating NULLCOUNT column OID: "<<OID_SYSCOLUMN_NULLCOUNT;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_NULLCOUNT, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  // MINVALUE
  msg << "  Creating MINVALUE column OID: "<<OID_SYSCOLUMN_MINVALUE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_MINVALUE, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating MINVALUE column dictionary...");
  cout << msg.str() << endl;
  //Dictionary files
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_MINVALUE, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");
    
  // MAXVALUE
  msg << "  Creating MAXVALUE column OID: "<<OID_SYSCOLUMN_MAXVALUE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_MAXVALUE, WriteEngine::VARCHAR, 40, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));

  msg.str("  Creating MAXVALUE column dictionary...");
  //Dictionary files
  cout << msg.str() << endl;
  rc = fWriteEngine.createDctnry(txnID, DICTOID_SYSCOLUMN_MAXVALUE, 65, dbRoot);
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
   
  // CompressionType
  msg << "  Creating CompressionType column OID: "<<OID_SYSCOLUMN_COMPRESSIONTYPE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_COMPRESSIONTYPE, WriteEngine::INT, 4, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");   
  
  // nextvalue
  msg << "  Creating NEXTVALUE column OID: "<<OID_SYSCOLUMN_NEXTVALUE;
  cout << msg.str() << endl;
  rc = fWriteEngine.createColumn( txnID, OID_SYSCOLUMN_NEXTVALUE, WriteEngine::BIGINT, 8, dbRoot );
  if (rc) throw runtime_error(msg.str() + ec.errorString(rc));
  msg.str("");   
  
  //------------------------------------------------------------------------------
  // Create SYSCONSTRAINT table
  //------------------------------------------------------------------------------
  dbRoot++;
  if (dbRoot > dbRootCount)
    dbRoot = 1;

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
