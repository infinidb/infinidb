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

#include <stdio.h>
#include <string>
#include <stdexcept>
#include <sys/timeb.h>
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "writeengine.h"
#include "we_colop.h"

using namespace WriteEngine;

extern WriteEngine::BRMWrapper* brmWrapperPtr;

class WriteEngineTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( WriteEngineTest );

// Column related operation testing
/*
CPPUNIT_TEST( testCreateColumnFiles );
CPPUNIT_TEST( testWriteColumnFiles );
CPPUNIT_TEST( testReadColumnFiles );

CPPUNIT_TEST( testInsertRowInt );
CPPUNIT_TEST( testInsertRowLonglong );
CPPUNIT_TEST( testInsertRowDouble );
CPPUNIT_TEST( testInsertRowChar );
CPPUNIT_TEST( testInsertBulk );

// Table related operation testing
CPPUNIT_TEST( testTableAllocateRowId );

// Wrapper interface testing
CPPUNIT_TEST( testInterfaceCreateColumnFile );
CPPUNIT_TEST( testInterfaceInsertUpdateDeleteRow );
CPPUNIT_TEST( testInterfaceInsertRowMedIntDouble );

CPPUNIT_TEST( testInterfaceInsertRowChar );

CPPUNIT_TEST( testInterfaceInsertRowCharQAWidth4 );
CPPUNIT_TEST( testInterfaceInsertRowCharQAWidth3 );
CPPUNIT_TEST( testInterfaceInsertRowCharQAWidth8 );
CPPUNIT_TEST( testInterfaceInsertRowCharQAWidth1 );


CPPUNIT_TEST( testInterfaceInsertRowLongLong );
CPPUNIT_TEST( testInterfaceInsertRowByte );
CPPUNIT_TEST( testInterfaceInsertRowToken );
CPPUNIT_TEST( testInterfaceInsertRowSingleChar );
CPPUNIT_TEST( testInterfaceInsertRowDoubleChar );

CPPUNIT_TEST( testInterfaceCreateIndex );
CPPUNIT_TEST( testInterfaceDropIndex );
CPPUNIT_TEST( testInterfaceUpdateIndexChar ); 
CPPUNIT_TEST( testInterfaceUpdateMultiColIndex);

CPPUNIT_TEST( testInterfaceDctnryToken );
CPPUNIT_TEST( testInterfaceDctnryTokenRollBack );

CPPUNIT_TEST( testInsertCommitRollback );
CPPUNIT_TEST( testInterfaceInsertRowHwm );

CPPUNIT_TEST( testInterfaceCombineIndexLoad );   // note: this test case must be the last one
//CPPUNIT_TEST( testTmpBulkPerformance );   // note: this test case must be the last one
*/
//CPPUNIT_TEST(testBoostFloat);
/*
CPPUNIT_TEST(testCreateColumnFiles1);
CPPUNIT_TEST(testCreateColumnFiles2);
CPPUNIT_TEST(testCreateColumnFiles3);
CPPUNIT_TEST(testCreateColumnFiles4);
*/

//CPPUNIT_TEST(testCreateFileMultipleIONBF);
//CPPUNIT_TEST(testCreateFile64MBIONBF);
//CPPUNIT_TEST(testCreateFileMultipleBF);
//CPPUNIT_TEST(testCreateFilelargerBF);
CPPUNIT_TEST(testSoloman);
/*
CPPUNIT_TEST(testCreateFileNoBRM);
CPPUNIT_TEST(testCreateFileBRM);
*/
CPPUNIT_TEST_SUITE_END();

private:
   WriteEngineWrapper      m_wrapper;
   Session                 m_session;

public:
	void setUp() {
      m_wrapper.setDebugLevel( DEBUG_3 );
      m_session.txnid = 10;
      BRMWrapper::setUseBrm( true );
	}

	void tearDown() {

	}

   void SetColumnStruct( ColStruct& column, OID dataOid, int colWidth, CalpontSystemCatalog::ColDataType colDataType )
   {
      column.dataOid = dataOid;
      column.colWidth = colWidth;
      column.colDataType = colDataType;
      column.tokenFlag = false;
   }

   void CreateColumnFile( FID fid, int width, CalpontSystemCatalog::ColDataType colDataType, ColType colType )
   {
      int rc;
      ColumnOp colOp;
      Column   curCol;
 
      colOp.deleteFile( fid );
      CPPUNIT_ASSERT( colOp.exists( fid ) == false );

      // create column files
      rc = colOp.createColumn( curCol, 2, width, colDataType, colType, fid );
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testCreateColumnFiles() {
      ColumnOp colOp;
      Column   curCol;
      int      rc;

 
      colOp.initColumn( curCol );

      // file opertaions
      colOp.deleteFile( 100 );
      CPPUNIT_ASSERT( colOp.exists( 100 ) == false );

      colOp.deleteFile( 101 );
      CPPUNIT_ASSERT( colOp.exists( 101 ) == false );

      colOp.deleteFile( 103 );
      CPPUNIT_ASSERT( colOp.exists( 103 ) == false );

      rc = colOp.createColumn( curCol, 2, 50, WriteEngine::CHAR, WriteEngine::WR_CHAR, 101 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      colOp.deleteFile( 101 );
      rc = colOp.createColumn( curCol, 2, 3, WriteEngine::CHAR, WriteEngine::WR_CHAR, 101 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      colOp.deleteFile( 101 );
      rc = colOp.createColumn( curCol, 2, 5, WriteEngine::CHAR, WriteEngine::WR_CHAR, 101 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // create column files
      rc = colOp.createColumn( curCol, 2, 8, WriteEngine::CHAR, WriteEngine::WR_CHAR, 100 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = colOp.createColumn( curCol, 2, 8, WriteEngine::CHAR, WriteEngine::WR_CHAR, 100 );
      CPPUNIT_ASSERT( rc == ERR_FILE_EXIST );

      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      colOp.clearColumn( curCol );

      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == ERR_INVALID_PARAM );

      rc = colOp.createTable();
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testWriteColumnFiles() {
      ColumnOp colOp;
      Column   curCol;
      int      rc;
      unsigned char buf[BYTE_PER_BLOCK];

 
      colOp.initColumn( curCol );
      CPPUNIT_ASSERT( curCol.colNo == 0 );
      CPPUNIT_ASSERT( curCol.colWidth == 0 );
      CPPUNIT_ASSERT( curCol.dataFile.pFile == NULL );
//      CPPUNIT_ASSERT( curCol.bitmapFile.pFile == NULL );
      CPPUNIT_ASSERT( curCol.dataFile.fid == 0 );
//      CPPUNIT_ASSERT( curCol.bitmapFile.fid == 0 );

      colOp.setColParam( curCol, 2, 4, INT, WriteEngine::WR_INT, 100  );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      int testVal = 60;
      memset( buf, 0, BYTE_PER_BLOCK );
      memcpy( buf+8, &testVal, 4 );

      rc = colOp.writeDBFileFbo( curCol.dataFile.pFile, buf, 1, 1 );  // block 2
      CPPUNIT_ASSERT( rc == NO_ERROR );

      colOp.clearColumn( curCol );

      CPPUNIT_ASSERT( curCol.colNo == 0 );
      CPPUNIT_ASSERT( curCol.colWidth == 0 );
      CPPUNIT_ASSERT( curCol.dataFile.pFile == NULL );
//      CPPUNIT_ASSERT( curCol.bitmapFile.pFile == NULL );
      CPPUNIT_ASSERT( curCol.dataFile.fid == 0 );
//      CPPUNIT_ASSERT( curCol.bitmapFile.fid == 0 );

      CPPUNIT_ASSERT( colOp.isValid( curCol ) == false );

   }

   void testReadColumnFiles() {
      ColumnOp colOp;
      Column   curCol, errCol;
      int      rc;
      unsigned char buf[BYTE_PER_BLOCK];
      CalpontSystemCatalog::ColDataType colDataType;
      
//      colOp.setUseBrm(false);
      CPPUNIT_ASSERT( colOp.getColDataType( "integer", colDataType ) == true );
      CPPUNIT_ASSERT( colDataType == INT );
      CPPUNIT_ASSERT( colOp.getColDataType( "Int1", colDataType ) == false );

      // check error situation
      colOp.initColumn( errCol );
      colOp.setColParam( errCol, 2, 8, CHAR, WriteEngine::WR_CHAR, 103 );
      rc = colOp.openColumnFile( errCol );
      CPPUNIT_ASSERT( rc == ERR_FILE_READ );
      colOp.clearColumn( errCol );


      colOp.initColumn( errCol );
      colOp.setColParam( errCol, 2, 8, CHAR, WriteEngine::WR_CHAR, 103 );
      rc = colOp.openColumnFile( errCol );
      CPPUNIT_ASSERT( rc == ERR_FILE_READ );
      colOp.clearColumn( errCol );

      // check normal situation
      colOp.initColumn( curCol );

      colOp.setColParam( curCol, 2, 8, CHAR, WriteEngine::WR_CHAR, 100 );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      memset( buf, 0, BYTE_PER_BLOCK );

      rc = colOp.readDBFile( curCol.dataFile.pFile, buf, 1, true );  // block 2
      CPPUNIT_ASSERT( rc == NO_ERROR );

      int testVal = 0;
      memcpy( &testVal, buf+8, 8 );
      printf( "\nread the test value : %d", testVal );
      CPPUNIT_ASSERT( testVal == 60 );

      colOp.clearColumn( curCol );
   }


   void testInsertRowInt() {
      ColumnOp colOp;
      Column   curCol;
      int      rc, valArray[5], oldValArray[5];
      RID      rowIdArray[5];
      
 
      rowIdArray[0] = 1;
      rowIdArray[1] = 3;
      rowIdArray[2] = 4;
      rowIdArray[3] = 7;
      rowIdArray[4] = 8;

      valArray[0] = 8;
      valArray[1] = 5;
      valArray[2] = 3;
      valArray[3] = 0;
      valArray[4] = 16;

      CreateColumnFile( 100, 4, WriteEngine::INT, WriteEngine::WR_INT );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, 4, INT, WriteEngine::WR_INT, 100 );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = colOp.writeRow( curCol, 5, (RID*) rowIdArray, valArray, oldValArray );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( int i = 0; i < 5; i++ )
         CPPUNIT_ASSERT( oldValArray[i] == (int)0x80000001 );

      colOp.clearColumn( curCol );
   }

   void testInsertRowLonglong() {
      ColumnOp    colOp;
      Column      curCol;
      int         rc;
      RID         rowIdArray[3];
      int         width = 8, totalRow = 3;
      FID         fid = 100;
      long long   valArray[3];
      uint64_t    oldValArray[3], verifyArray[3];
      CalpontSystemCatalog::ColDataType colDataType = CalpontSystemCatalog::BIGINT;
      ColType     colType = WriteEngine::WR_LONGLONG;
      uint64_t     emptyVal = 0x8000000000000001LL;

 
      rowIdArray[0] = 1;
      rowIdArray[1] = 3;
      rowIdArray[2] = 4;

      verifyArray[0] = valArray[0] = 32111238;
      verifyArray[1] = valArray[1] = 1231235;
      verifyArray[2] = valArray[2] = 67731233;

      CreateColumnFile( fid, width, colDataType, colType );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, width, colDataType, colType, fid );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = colOp.writeRow( curCol, totalRow, (RID*) rowIdArray, valArray, oldValArray );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( int i = 0; i < totalRow; i++ )
         CPPUNIT_ASSERT( oldValArray[i] == emptyVal );

      valArray[0] = 1900003;
      valArray[1] = 2349000;
      valArray[2] = 78900123;

      rc = colOp.writeRow( curCol, totalRow, (RID*) rowIdArray, valArray, oldValArray );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( int i = 0; i < totalRow; i++ )
         CPPUNIT_ASSERT( oldValArray[i] == verifyArray[i] );

      colOp.clearColumn( curCol );
   }


   void testInsertRowDouble() {
      ColumnOp    colOp;
      Column      curCol;
      int         rc;
      RID         rowIdArray[3];
      int         width = 8, totalRow = 3;
      FID         fid = 100;
      double      valArray[3];
      int64_t     oldValArray[3];
      CalpontSystemCatalog::ColDataType colDataType = CalpontSystemCatalog::DOUBLE;
      ColType     colType = WriteEngine::WR_DOUBLE;
      int64_t     emptyVal = 0xFFFAAAAAAAAAAAABLL;

 
      rowIdArray[0] = 1;
      rowIdArray[1] = 3;
      rowIdArray[2] = 4;

      valArray[0] = 8;
      valArray[1] = 5;
      valArray[2] = 3;

      CreateColumnFile( fid, width, colDataType, colType );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, width, colDataType, colType, fid );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = colOp.writeRow( curCol, totalRow, (RID*) rowIdArray, valArray, oldValArray );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( int i = 0; i < totalRow; i++ )
         CPPUNIT_ASSERT( oldValArray[i] == emptyVal );

      colOp.clearColumn( curCol );
   }

   void testInsertRowChar() {
      ColumnOp    colOp;
      Column      curCol;
      int         rc;
      RID         rowIdArray[3];
      int         width = 8, totalRow = 3;
      FID         fid = 100;
      char        valArray[24], buf[8];
      char        testValue[3][8] = { "abc", "eDFFF", "GHK" };
      uint64_t    oldValArray[3];
      CalpontSystemCatalog::ColDataType colDataType = CalpontSystemCatalog::CHAR;
      ColType     colType = WriteEngine::WR_CHAR;
      uint64_t    emptyVal = 0xFFFFFFFFFFFFFFFFLL;

 
      rowIdArray[0] = 1;
      rowIdArray[1] = 3;
      rowIdArray[2] = 4;

      memset( valArray, 0, 24 );
      colOp.writeBufValue( (unsigned char*)buf, testValue[0], 8, true );
      memcpy( valArray, buf, 8 );
      colOp.writeBufValue( (unsigned char*)buf, testValue[1], 8, true );
      memcpy( valArray+8, buf, 8 );
      colOp.writeBufValue( (unsigned char*)buf, testValue[2], 8, true );
      memcpy( valArray+16, buf, 8 );

      CreateColumnFile( fid, width, colDataType, colType );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, width, colDataType, colType, fid );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = colOp.writeRow( curCol, totalRow, (RID*) rowIdArray, valArray, oldValArray );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( int i = 0; i < totalRow; i++ )
         CPPUNIT_ASSERT( oldValArray[i] == emptyVal );

      colOp.clearColumn( curCol );
   }

   void testInsertBulk() {
      FILE*       pSourceFile = NULL;
      ColumnOp    colOp;
      Column      curCol;
      int         rc, width = 8;
      int         hwm;
      FID         fid = 999;
      CalpontSystemCatalog::ColDataType colDataType = CalpontSystemCatalog::CHAR;
      ColType     colType = WriteEngine::WR_CHAR;
      
      colOp.deleteFile( 999 );
      colOp.deleteFile( 998 );
      CPPUNIT_ASSERT( colOp.exists( 999 ) == false );
      CPPUNIT_ASSERT( colOp.exists( 998 ) == false );

      int allocSize =0;
      CPPUNIT_ASSERT( colOp.createFile( 998, 20, allocSize, 0xEEEE, 2 ) == NO_ERROR );
      CreateColumnFile( fid, width, colDataType, colType );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, width, colDataType, colType, fid );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      pSourceFile = colOp.openFile( 998 );
      CPPUNIT_ASSERT( pSourceFile != NULL );

      hwm = colOp.getFileSize( curCol.dataFile.pFile )/BYTE_PER_BLOCK - 5 ;
      rc = colOp.insertBulk( curCol, pSourceFile, hwm, 20 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      colOp.clearColumn( curCol );
      colOp.closeFile( pSourceFile );
   }

   void testTableAllocateRowId() {
      ColumnOp    colOp;
      Column      curCol;
      RID         rowIdArray[3];
      int         rc;
      int         width = 4, hwm = 0;
      FID         fid = 100;
      CalpontSystemCatalog::ColDataType colDataType = CalpontSystemCatalog::INT;
      ColType     colType = WriteEngine::WR_INT;

 
      CreateColumnFile( fid, width, colDataType, colType );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, width, colDataType, colType, fid );

      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      colOp.allocRowId( curCol, 3, rowIdArray, hwm );
      CPPUNIT_ASSERT( rowIdArray[0] == 0 );
      CPPUNIT_ASSERT( rowIdArray[1] == 1 );
      CPPUNIT_ASSERT( rowIdArray[2] == 2 );
   }


   void testInterfaceCreateColumnFile() {
      ColumnOp colOp;
      FID      fid = 100;

 
      if( colOp.exists( fid ) ) {
         CPPUNIT_ASSERT( m_wrapper.dropColumn( m_session.txnid, fid ) == NO_ERROR );
         CPPUNIT_ASSERT( m_wrapper.dropColumn( m_session.txnid, fid ) == ERR_FILE_NOT_EXIST );
      }

      CPPUNIT_ASSERT( m_wrapper.createColumn( m_session.txnid, fid, WriteEngine::DOUBLE, 8 ) == NO_ERROR );
      CPPUNIT_ASSERT( colOp.exists( 100 ) == true );
   }

   void testInterfaceInsertUpdateDeleteRow() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      int                     rc, total, valArray[3];
      float                   dVal;
//      ColumnOp                colOp;

//      colOp = m_wrapper.getColumnOp();
 

      CreateColumnFile( 100, 4, WriteEngine::INT, WriteEngine::WR_INT );
      CreateColumnFile( 200, 4, WriteEngine::FLOAT, WriteEngine::WR_FLOAT );

      // test column struct list
      SetColumnStruct( curColStruct, 100, 4, WriteEngine::INT );
      colStructList.push_back( curColStruct );

      SetColumnStruct( curColStruct, 200, 4, WriteEngine::FLOAT );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 4 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::INT );

      testColStruct = colStructList[1];
      CPPUNIT_ASSERT( testColStruct.dataOid == 200 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 4 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::FLOAT );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      curTuple.data = 3;
      curTupleList.push_back( curTuple );

      curTuple.data = 0;
      curTupleList.push_back( curTuple );

      curTuple.data = 100;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      m_wrapper.convertValArray( 3, WriteEngine::WR_INT, curTupleList, valArray );
      CPPUNIT_ASSERT( valArray[0] == 3 );
      CPPUNIT_ASSERT( valArray[1] == 0 );
      CPPUNIT_ASSERT( valArray[2] == 100 );

      curTupleList.clear();

      // add values for the second column
      curTuple.data = 0.0f;
      curTupleList.push_back( curTuple );

      curTuple.data = 1234.78f;
      curTupleList.push_back( curTuple );

      curTuple.data = 999.98f;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      float valArray1[3];
      m_wrapper.convertValArray( 3, WriteEngine::WR_FLOAT, curTupleList, valArray1 );
      CPPUNIT_ASSERT( valArray1[0] == 0.0f );
      CPPUNIT_ASSERT( valArray1[1] == 1234.78f);
      CPPUNIT_ASSERT( valArray1[2] == 999.98f );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 3 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 0 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 100 );

      // second column
      testTupleList = static_cast<ColTupleList>(colValueList[1]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      dVal = boost::any_cast<float>( testTuple.data );
      CPPUNIT_ASSERT( dVal == 0.0f );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<float>( testTuple.data ) == 1234.78f );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<float>( testTuple.data ) == 999.98f );

//      m_wrapper.printInputValue( colStructList, colValueList, ridList );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 3 );
      CPPUNIT_ASSERT( ridList[1] == 4 );
      CPPUNIT_ASSERT( ridList[2] == 5 );

      // try to update rows
      colValueList.clear();
      curTupleList.clear();

      curTuple.data = 9;
      curTupleList.push_back( curTuple );

      curTuple.data = 15;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );

      // add values for the second column
      curTupleList.clear();

      curTuple.data = 1.99f;
      curTupleList.push_back( curTuple );

      curTuple.data = 3000.15f;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );

      ridList.clear();
      ridList.push_back( (RID) 2 );
      ridList.push_back( (RID) 3 );

      rc = m_wrapper.updateColumnRec( m_session.txnid, colStructList, colValueList, colOldValList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( colOldValList.size() == 2 );

      m_wrapper.printInputValue( colStructList, colOldValList, ridList );
      curTupleList = colOldValList[0];
      curTuple = curTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( curTuple.data ) == 100 );
      curTuple = curTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<int>( curTuple.data ) == 3 );

      curTupleList = colOldValList[1];
      curTuple = curTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<float>( curTuple.data ) == 999.98f );
      curTuple = curTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<float>( curTuple.data ) == 0.0f );

      ridList[0] = 1;
      ridList[1] = 2;

      rc = m_wrapper.deleteRow( m_session.txnid, colStructList, colOldValList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( colOldValList.size() == 2 );

      m_wrapper.printInputValue( colStructList, colOldValList, ridList );
      curTupleList = colOldValList[0];
      curTuple = curTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( curTuple.data ) == 0 );
      curTuple = curTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<int>( curTuple.data ) == 9 );

      curTupleList = colOldValList[1];
      curTuple = curTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<float>( curTuple.data ) == 1234.78f );
      curTuple = curTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<float>( curTuple.data ) == 1.99f );

   }

   void testInterfaceInsertRowMedIntDouble() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      int                     total, rc;
      double                  dVal;

 

      CreateColumnFile( 100, 4, WriteEngine::MEDINT, WriteEngine::WR_INT );
      CreateColumnFile( 200, 8, WriteEngine::DOUBLE, WriteEngine::WR_DOUBLE );

      // test column struct list
      SetColumnStruct( curColStruct, 100, 4, WriteEngine::MEDINT );
      colStructList.push_back( curColStruct );

      SetColumnStruct( curColStruct, 200, 8, WriteEngine::DOUBLE );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 4 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::MEDINT );

      testColStruct = colStructList[1];
      CPPUNIT_ASSERT( testColStruct.dataOid == 200 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 8 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::DOUBLE );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      curTuple.data = 102;
      curTupleList.push_back( curTuple );

      curTuple.data = 0;
      curTupleList.push_back( curTuple );

      curTuple.data = 200;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      // add values for the second column
      curTuple.data = 0.0;
      curTupleList.push_back( curTuple );

      curTuple.data = 1234.78;
      curTupleList.push_back( curTuple );

      curTuple.data = 999.98;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 9 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 102 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 0 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 200 );

      // second column
      testTupleList = static_cast<ColTupleList>(colValueList[1]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      dVal = boost::any_cast<double>( testTuple.data );
      CPPUNIT_ASSERT( dVal == 0.0 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<double>( testTuple.data ) == 1234.78 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<double>( testTuple.data ) == 999.98 );

//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );

      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 3 );
      CPPUNIT_ASSERT( ridList[1] == 4 );
      CPPUNIT_ASSERT( ridList[2] == 5 );
   }


   void testInterfaceInsertRowChar() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     total, rc, width = 5;

 
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc != NO_ERROR );

      rc = m_wrapper.updateColumnRec( m_session.txnid, colStructList, colValueList, colOldValList, ridList );
      CPPUNIT_ASSERT( rc != NO_ERROR );

      // test column struct list
      CreateColumnFile( 100, width, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, width, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == width );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      curStr = "aaaaa";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "bbbbb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "ccccc";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 9 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "aaaaa" );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "bbbbb" );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "ccccc" );

//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 3 );
      CPPUNIT_ASSERT( ridList[1] == 4 );
      CPPUNIT_ASSERT( ridList[2] == 5 );
   }


   void testInterfaceInsertRowCharQAWidth4() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     rc, width = 4;

       // test column struct list
      CreateColumnFile( 100, width, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, width, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == width );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curStr = "aaaa";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "aaa";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "aa";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "a";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );
      CPPUNIT_ASSERT( ridList[3] == 3 );

      // separate inserts
      colValueList.clear();
      curStr = "bbbb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 4 );

      colValueList.clear();
      curStr = "bbb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 5 );

      colValueList.clear();
      curStr = "bb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 6 );

      colValueList.clear();
      curStr = "b";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 7 );
   }

   void testInterfaceInsertRowCharQAWidth3() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     rc, width = 3;

       // test column struct list
      CreateColumnFile( 100, width, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, width, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == width );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curStr = "aaa";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "aa";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "a";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // separate inserts
      colValueList.clear();
      curStr = "bbb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 3 );

      colValueList.clear();
      curStr = "bb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 4 );

      colValueList.clear();
      curStr = "b";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 5 );
   }

   void testInterfaceInsertRowCharQAWidth8() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     rc, width = 8;

       // test column struct list
      CreateColumnFile( 100, width, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, width, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == width );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curStr = "12345678";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "123456";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "1234";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // separate inserts
      colValueList.clear();
      curStr = "bbbb1234";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 3 );

      colValueList.clear();
      curStr = "bb";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 4 );

      colValueList.clear();
      curStr = "b";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 5 );
   }

   void testInterfaceInsertRowCharQAWidth1() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     rc, width = 1;

       // test column struct list
      CreateColumnFile( 100, width, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, width, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == width );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curStr = "a";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "b";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      curStr = "c";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // separate inserts
      colValueList.clear();
      curStr = "1";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 3 );

      colValueList.clear();
      curStr = "2";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 4 );

      colValueList.clear();
      curStr = "3";
      curTuple.data = curStr;
      curTupleList.push_back( curTuple );
      colValueList.push_back( curTupleList );
      curTupleList.clear();
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 5 );
   }


   void testInterfaceInsertRowSingleChar() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      string                  curString;
      int                     total, rc;
      ColumnOp                colOp;

//      colOp = m_wrapper.getColumnOp();
 
      colOp.deleteFile( 100 );

      // test column struct list
      CreateColumnFile( 100, 1, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, 1, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 1 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curString = "\376";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "a";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "c";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "G";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );


      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "a" );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "c" );

      testTuple = testTupleList[3];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "G" );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      printf( "\nrc=%d", rc );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );
      CPPUNIT_ASSERT( ridList[3] == 3 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 4 );
      CPPUNIT_ASSERT( ridList[1] == 5 );
      CPPUNIT_ASSERT( ridList[2] == 6 );
      CPPUNIT_ASSERT( ridList[3] == 7 );
   }

   void testInterfaceInsertRowDoubleChar() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      string                  curString;
      int                     total, rc;
      ColumnOp                colOp;

//      colOp = m_wrapper.getColumnOp();
 
      colOp.deleteFile( 100 );

      // test column struct list
      CreateColumnFile( 100, 2, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, 2, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 2 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curString = "\377\376";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "ab";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "c";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "Ge";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );


      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "ab" );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "c" );

      testTuple = testTupleList[3];
      CPPUNIT_ASSERT( boost::any_cast<std::string>( testTuple.data ) == "Ge" );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      printf( "\nrc=%d", rc );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );
      CPPUNIT_ASSERT( ridList[3] == 3 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 4 );
      CPPUNIT_ASSERT( ridList[1] == 5 );
      CPPUNIT_ASSERT( ridList[2] == 6 );
      CPPUNIT_ASSERT( ridList[3] == 7 );
   }

   void testInterfaceInsertRowLongLong() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      int                     total, rc;

 

      CreateColumnFile( 100, 8, WriteEngine::BIGINT, WriteEngine::WR_LONGLONG );
      CreateColumnFile( 200, 2, WriteEngine::SMALLINT, WriteEngine::WR_SHORT );

      // test column struct list
      SetColumnStruct( curColStruct, 100, 8, WriteEngine::BIGINT );
      colStructList.push_back( curColStruct );

      SetColumnStruct( curColStruct, 200, 2, WriteEngine::SMALLINT );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 8 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::BIGINT );

      testColStruct = colStructList[1];
      CPPUNIT_ASSERT( testColStruct.dataOid == 200 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 2 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::SMALLINT );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      curTuple.data = (long long)1021231;
      curTupleList.push_back( curTuple );

      curTuple.data = (long long)0;
      curTupleList.push_back( curTuple );

      curTuple.data = (long long)93232200;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      // add values for the second column
      curTuple.data = (short)1000;
      curTupleList.push_back( curTuple );

      curTuple.data = (short)5678;
      curTupleList.push_back( curTuple );

      curTuple.data = (short)9;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 9 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<long long>( testTuple.data ) == 1021231 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<long long>( testTuple.data ) == 0 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<long long>( testTuple.data ) == 93232200 );

      // second column
      testTupleList = static_cast<ColTupleList>(colValueList[1]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<short>( testTuple.data ) == 1000 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<short>( testTuple.data ) == 5678 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<short>( testTuple.data ) == 9 );

//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );

      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 3 );
      CPPUNIT_ASSERT( ridList[1] == 4 );
      CPPUNIT_ASSERT( ridList[2] == 5 );
   }


   void testInterfaceInsertRowByte() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      int                     total, rc;
      ColumnOp                colOp;

      CreateColumnFile( 100, 1, WriteEngine::TINYINT, WriteEngine::WR_BYTE );
      CreateColumnFile( 200, 4, WriteEngine::INT, WriteEngine::WR_INT );

      // test column struct list
      SetColumnStruct( curColStruct, 100, 1, WriteEngine::TINYINT );
      colStructList.push_back( curColStruct );

      SetColumnStruct( curColStruct, 200, 4, WriteEngine::INT );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 1 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::TINYINT );

      testColStruct = colStructList[1];
      CPPUNIT_ASSERT( testColStruct.dataOid == 200 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 4 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::INT );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      curTuple.data = (char)21;
      curTupleList.push_back( curTuple );

      curTuple.data = (char)-30;
      curTupleList.push_back( curTuple );

      curTuple.data = (char)127;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      // add values for the second column
      curTuple.data = 1000;
      curTupleList.push_back( curTuple );

      curTuple.data = 5678;
      curTupleList.push_back( curTuple );

      curTuple.data = 9;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 9 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<ColTupleList>(colValueList[0]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<char>( testTuple.data ) == 21 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<char>( testTuple.data ) == -30 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<char>( testTuple.data ) == 127 );

      // second column
      testTupleList = static_cast<ColTupleList>(colValueList[1]);
      total = testTupleList.size();
      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 1000 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 5678 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 9 );

//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );

      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 3 );
      CPPUNIT_ASSERT( ridList[1] == 4 );
      CPPUNIT_ASSERT( ridList[2] == 5 );

   }


   void testInterfaceInsertRowToken() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     rc;
      ColumnOp                colOp;
      OID                     dctnryOID=800, treeOID=801, listOID=802;
      DctnryTuple             dctnryTuple;

      // no matter what happened, drop stores
//      BRMWrapper::setUseBrm(false);
      m_wrapper.dropStore( m_session.txnid, dctnryOID, treeOID, listOID);
      rc = m_wrapper.createStore( m_session.txnid, dctnryOID, treeOID, listOID);
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_wrapper.openStore( m_session.txnid, dctnryOID, treeOID, listOID);
      CPPUNIT_ASSERT( rc == NO_ERROR );

      //colOp = m_wrapper.getColumnOp();

      // test column struct list
      CreateColumnFile( 100, 8, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 100, 8, WriteEngine::CHAR );
      curColStruct.tokenFlag = true;
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 100 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 8 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      strcpy( (char*)dctnryTuple.sigValue, "ABCD 12345" );
      dctnryTuple.sigSize = 10; 
      rc = m_wrapper.tokenize( m_session.txnid, dctnryTuple );
      if (rc!= NO_ERROR)
       printf("1197 Tokenize failed, rc %i\n",rc);  
      CPPUNIT_ASSERT( rc == NO_ERROR );
      curTuple.data = dctnryTuple.token;
      curTupleList.push_back( curTuple );

      Token test1;
      test1 = boost::any_cast<Token>( curTuple.data );
//      printf( "\ncurTuple.datatype = %d\n", curTuple.data.type() );

      strcpy( (char*)dctnryTuple.sigValue, "CBED 1334678" );
      dctnryTuple.sigSize = 12; 
      rc = m_wrapper.tokenize( m_session.txnid, dctnryTuple );
      if (rc!= NO_ERROR)
       printf("1208 Tokenize failed, rc %i\n",rc); 
      CPPUNIT_ASSERT( rc == NO_ERROR );
      curTuple.data = dctnryTuple.token;
      curTupleList.push_back( curTuple );

      strcpy( (char*)dctnryTuple.sigValue, "GHED 2334" );
      dctnryTuple.sigSize = 9; 
      rc = m_wrapper.tokenize( m_session.txnid, dctnryTuple ); 
      CPPUNIT_ASSERT( rc == NO_ERROR );
      curTuple.data = dctnryTuple.token;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 9 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );
      CPPUNIT_ASSERT( ridList[0] == 0 );
      CPPUNIT_ASSERT( ridList[1] == 1 );
      CPPUNIT_ASSERT( ridList[2] == 2 );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( ridList[0] == 3 );
      CPPUNIT_ASSERT( ridList[1] == 4 );
      CPPUNIT_ASSERT( ridList[2] == 5 );

      m_wrapper.dropStore( m_session.txnid, dctnryOID, treeOID, listOID);
   }

   void testInterfaceCreateIndex() {
      int         rc;
      ColumnOp    colOp;

      if( colOp.exists( 900 ) ) {
         rc = m_wrapper.dropIndex( m_session.txnid, 900, 901 );
         CPPUNIT_ASSERT( rc == NO_ERROR );
      }

      rc = m_wrapper.createIndex( m_session.txnid, 900, 901 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testInterfaceDropIndex() {
      int         rc;

      rc = m_wrapper.dropIndex( m_session.txnid, 900, 901 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void SetIndexStruct( IdxStruct& curStruct, OID treeOid, OID listOid, int width, CalpontSystemCatalog::ColDataType dataType )
   {
      curStruct.treeOid = treeOid;
      curStruct.listOid = listOid;
      curStruct.idxWidth = width;
      curStruct.idxDataType = dataType;
      curStruct.tokenFlag = false;
   }

   void testInterfaceUpdateIndexChar() {
      IdxStruct               curStruct, testStruct;
      IdxTuple                curTuple, testTuple;
      IdxTupleList            curTupleList, testTupleList;
      IdxStructList           idxStructList;
      IdxValueList            idxValueList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     total, rc;

      testInterfaceCreateIndex();

      CPPUNIT_ASSERT( m_wrapper.updateIndexRec( m_session.txnid, idxStructList, idxValueList, ridList ) != NO_ERROR );

      // test column struct list
      SetIndexStruct( curStruct, 900, 901, 4, WriteEngine::MEDINT );
      idxStructList.push_back( curStruct );

      testStruct = idxStructList[0];
      CPPUNIT_ASSERT( testStruct.treeOid == 900 );
      CPPUNIT_ASSERT( testStruct.listOid == 901 );
      CPPUNIT_ASSERT( testStruct.idxWidth == 4 );
      CPPUNIT_ASSERT( testStruct.idxDataType == WriteEngine::MEDINT );

      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // add values for the first index
      curTuple.data = 102;
      curTupleList.push_back( curTuple );

      curTuple.data = 102;
      curTupleList.push_back( curTuple );

      curTuple.data = 102;
      curTupleList.push_back( curTuple );

      curTuple.data = 0;
      curTupleList.push_back( curTuple );

      curTuple.data = 200;
      curTupleList.push_back( curTuple );

      idxValueList.push_back( curTupleList );

      curTupleList.clear();

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 15 );
      ridList.push_back( (RID) 16 );
      ridList.push_back( (RID) 17 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<IdxTupleList>(idxValueList[0]);
      total = testTupleList.size();

      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 102 );

      testTuple = testTupleList[3];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 0 );

      testTuple = testTupleList[4];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 200 );

      rc = m_wrapper.updateIndexRec( m_session.txnid, idxStructList, idxValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

//      CPPUNIT_ASSERT( m_wrapper.deleteIndexRec( idxStructList, idxValueList, ridList ) == NO_ERROR );
   }

   void testInterfaceUpdateMultiColIndex() {
      IdxStruct               curStruct, testStruct;
      IdxTuple                curTuple, testTuple;
      IdxTupleList            curTupleList, testTupleList;
      IdxStructList           idxStructList;
      IdxValueList            idxValueList;
      RIDList                 ridList;
      std::string             curStr, testStr;
      int                     total, rc;

      testInterfaceCreateIndex();

      CPPUNIT_ASSERT( m_wrapper.updateIndexRec( m_session.txnid, idxStructList, idxValueList, ridList ) != NO_ERROR );

      // test column struct list
      SetIndexStruct( curStruct, 900, 901, 4, WriteEngine::MEDINT );
      idxStructList.push_back( curStruct );

      SetIndexStruct( curStruct, 900, 901, 2, WriteEngine::SMALLINT );
      idxStructList.push_back( curStruct );

      testStruct = idxStructList[0];
      CPPUNIT_ASSERT( testStruct.treeOid == 900 );
      CPPUNIT_ASSERT( testStruct.listOid == 901 );
      CPPUNIT_ASSERT( testStruct.idxWidth == 4 );
      CPPUNIT_ASSERT( testStruct.idxDataType == WriteEngine::MEDINT );

      testStruct = idxStructList[1];
      CPPUNIT_ASSERT( testStruct.treeOid == 900 );
      CPPUNIT_ASSERT( testStruct.listOid == 901 );
      CPPUNIT_ASSERT( testStruct.idxWidth == 2 );
      CPPUNIT_ASSERT( testStruct.idxDataType == WriteEngine::SMALLINT );

      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // add values for the first index
      curTuple.data = 102;
      curTupleList.push_back( curTuple );

      curTuple.data = 0;
      curTupleList.push_back( curTuple );

      curTuple.data = 200;
      curTupleList.push_back( curTuple );

      idxValueList.push_back( curTupleList );

      curTupleList.clear();

      // add values for the second index
      curTuple.data = (short)1;
      curTupleList.push_back( curTuple );

      curTuple.data = (short)2;
      curTupleList.push_back( curTuple );

      curTuple.data = (short)3;
      curTupleList.push_back( curTuple );

      idxValueList.push_back( curTupleList );

      curTupleList.clear();

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == ERR_ROWID_VALUE_NOT_MATCH );
      ridList.push_back( (RID) 9 );
      ridList.push_back( (RID) 10 );
      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<IdxTupleList>(idxValueList[0]);
      total = testTupleList.size();

      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 102 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 0 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 200 );

      // second column
      testTupleList = static_cast<IdxTupleList>(idxValueList[1]);
      total = testTupleList.size();

      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<short>( testTuple.data ) == 1 );

      testTuple = testTupleList[1];
      CPPUNIT_ASSERT( boost::any_cast<short>( testTuple.data ) == 2 );

      testTuple = testTupleList[2];
      CPPUNIT_ASSERT( boost::any_cast<short>( testTuple.data ) == 3 );

      rc = m_wrapper.processMultiColIndexRec( m_session.txnid, idxStructList, idxValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

   }

   void testInterfaceDeleteIndexChar() {
      IdxStruct               curStruct, testStruct;
      IdxTuple                curTuple, testTuple;
      IdxTupleList            curTupleList, testTupleList;
      std::vector<IdxStruct>  idxStructList;
      std::vector<IdxTupleList> idxValueList;
      std::vector<RID>        ridList;
      std::string             curStr, testStr;
      int                     total;

      // test column struct list
      SetIndexStruct( curStruct, 900, 901, 4, WriteEngine::MEDINT );
      idxStructList.push_back( curStruct );

      testStruct = idxStructList[0];
      CPPUNIT_ASSERT( testStruct.treeOid == 900 );
      CPPUNIT_ASSERT( testStruct.listOid == 901 );
      CPPUNIT_ASSERT( testStruct.idxWidth == 4 );
      CPPUNIT_ASSERT( testStruct.idxDataType == WriteEngine::MEDINT );

      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // add values for the first index
      curTuple.data = 102;
      curTupleList.push_back( curTuple );

      idxValueList.push_back( curTupleList );

      curTupleList.clear();

      // add one rowId
      ridList.push_back( (RID) 5 );
      CPPUNIT_ASSERT( m_wrapper.checkIndexValid( m_session.txnid, idxStructList, idxValueList, ridList ) == NO_ERROR );

      // retrieve the values back
      // first column
      testTupleList = static_cast<IdxTupleList>(idxValueList[0]);
      total = testTupleList.size();

      testTuple = testTupleList[0];
      CPPUNIT_ASSERT( boost::any_cast<int>( testTuple.data ) == 102 );


      CPPUNIT_ASSERT( m_wrapper.deleteIndexRec( m_session.txnid, idxStructList, idxValueList, ridList ) == NO_ERROR );
   }

   void testInterfaceDctnryToken() {
/*
      int           rc;
      OID           dctnryOID=880, treeOID=881, listOID=882;
      DctnryTuple   dctnryTuple;
      DctnryStruct  dctnryStruct;
      
      printf("Running testInterfaceDctnryToken\n");
      memset(&dctnryTuple, 0, sizeof(dctnryTuple));
      memset(&dctnryStruct,0, sizeof(dctnryStruct));
      
      dctnryStruct.dctnryOid = dctnryOID;
      dctnryStruct.treeOid   = treeOID;
      dctnryStruct.listOid   = listOID;

      BRMWrapper::setUseBrm(true);
      m_wrapper.dropStore( m_session.txnid, dctnryOID, treeOID, listOID);   
      rc = m_wrapper.createStore( m_session.txnid, dctnryOID, treeOID, listOID);
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_wrapper.openStore( m_session.txnid, dctnryOID, treeOID, listOID);
      CPPUNIT_ASSERT( rc == NO_ERROR );
      
       Token token[17], token2[17];
       memset(token, 0,  17*sizeof(Token));
       memset(token2, 0, 17*sizeof(Token));
       int i,j,k,smallSize=1000;
       j=255;
       for (k=1; k<17; k++)
       {
         for (i=0; i<smallSize; i++)
         {
          if (j==0)
           j=255;           
          dctnryTuple.sigValue[i]=k;
          j--;          
         }
         dctnryTuple.token.fbo=(uint64_t)-1;
         dctnryTuple.token.op =(int)-1;
         dctnryTuple.sigSize = smallSize;         
         rc = m_wrapper.tokenize( m_session.txnid, dctnryTuple); 
         if (rc!= NO_ERROR)
         {
          printf("1219.Tokenize failed ERROR CODE : %i k %i\n", rc , k); 
          return;       
         }
         else
         {
            printf("fbo %llu op %i\n",dctnryTuple.token.fbo,dctnryTuple.token.op);
         }
         CPPUNIT_ASSERT( rc == NO_ERROR );
         if ((dctnryTuple.token.fbo ==(uint64_t)-1) ||(dctnryTuple.token.op==(int)-1))
         {
           printf("1440.Tokenize failed ERROR CODE : %i k %i\n", rc , k); 
          return; 
         }
         token[k].fbo = dctnryTuple.token.fbo;
         token[k].op = dctnryTuple.token.op;
                 
         rc = m_wrapper.tokenize( m_session.txnid, dctnryTuple);       
         if (rc!= NO_ERROR)
         {
          printf("k %i \n", k);
          printf("1227.Second time tokenize failed ERROR CODE : %i \n", rc); 
          return;       
         }
         else
         {
           printf("Second tiem fbo %llu op %i\n",dctnryTuple.token.fbo,dctnryTuple.token.op);
         }
         CPPUNIT_ASSERT( rc == NO_ERROR );
         token2[k].fbo = dctnryTuple.token.fbo;
         token2[k].op  = dctnryTuple.token.op;
       } 
       m_wrapper.closeStore( m_session.txnid );
       for (int i=0; i< 17; i++) 
       {
         //CPPUNIT_ASSERT(token[i].fbo == token2[i].fbo);
         CPPUNIT_ASSERT(token[i].op  == token2[i].op);
         rc = m_wrapper.deleteToken( m_session.txnid, dctnryStruct, token[i]);
         if (i==0)
          CPPUNIT_ASSERT( rc != NO_ERROR );
         else
          CPPUNIT_ASSERT( rc == NO_ERROR );
       }    

      BRMWrapper::setUseBrm( true );

       m_wrapper.openStore( m_session.txnid, dctnryOID, treeOID, listOID);   
       rc = m_wrapper.tokenize( m_session.txnid, dctnryStruct,dctnryTuple);
      printf("rc %i \n", rc);
      rc = m_wrapper.rollbackTran(m_session.txnid);
      printf("This is rollback rc %i \n", rc);
      rc = m_wrapper.tokenize( m_session.txnid, dctnryStruct,dctnryTuple);
      printf("This is after rollback tokenize rc %i \n", rc);
      cout << "fbo " << dctnryTuple.token.fbo << " op " << dctnryTuple.token.op << endl;
      m_wrapper.closeStore( m_session.txnid );
     // rc = m_wrapper.dropStore( m_session.txnid, dctnryOID, treeOID, listOID);   
     // CPPUNIT_ASSERT( rc == NO_ERROR );       
*/
   }
   void testInterfaceDctnryTokenRollBack() {
/*
      int           rc;
      OID           dctnryOID=850, treeOID=851, listOID=852;
      DctnryTuple   dctnryTuple;
      DctnryStruct  dctnryStruct;
      m_session.txnid =100;
      BRMWrapper::setUseBrm(true);
      printf("Running testInterfaceDctnryTokenRollBack\n");
      memset(&dctnryTuple, 0, sizeof(dctnryTuple));
      memset(&dctnryStruct,0, sizeof(dctnryStruct));
      
      dctnryStruct.dctnryOid = dctnryOID;
      dctnryStruct.treeOid   = treeOID;
      dctnryStruct.listOid   = listOID;

//      BRMWrapper::setUseBrm(true);
      m_wrapper.dropStore( m_session.txnid, dctnryOID, treeOID, listOID );
      rc = m_wrapper.createStore( m_session.txnid, dctnryOID, treeOID, listOID);
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_wrapper.openStore( m_session.txnid, dctnryOID, treeOID, listOID);
      CPPUNIT_ASSERT( rc == NO_ERROR );
      
       Token token[17], token2[17];
       memset(token, 0,  17*sizeof(Token));
       memset(token2, 0, 17*sizeof(Token));
       
       int i,j,k,smallSize=10;
       j=255;
       for (k=1; k<2; k++)
       {
         for (i=0; i<smallSize; i++)
         {
          if (j==0)
           j=255;           
          dctnryTuple.sigValue[i]=k;
          j--;          
         }
         dctnryTuple.token.fbo=(uint64_t)-1;
         dctnryTuple.token.op =(int)-1;
         dctnryTuple.sigSize = smallSize;     
             
         rc = m_wrapper.tokenize( m_session.txnid, dctnryTuple); 
         if (rc!= NO_ERROR)
         {
          printf("1529.Tokenize failed ERROR CODE : %i k %i\n", rc , k); 
          return;       
         }
         else
         {
            printf("fbo %llu op %i\n",dctnryTuple.token.fbo,dctnryTuple.token.op);
         }
         CPPUNIT_ASSERT( rc == NO_ERROR );
         if ((dctnryTuple.token.fbo ==(uint64_t)-1) ||(dctnryTuple.token.op==(int)-1))
         {
           printf("1440.Tokenize failed ERROR CODE : %i k %i\n", rc , k); 
          return; 
         }
        
       } 
    
      rc = m_wrapper.rollbackTran(m_session.txnid );
      m_wrapper.closeStore( m_session.txnid );
*/
   }

   void testInsertCommitRollback() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      string                  curString;
      int                     rc;
      ColumnOp                colOp;

      BRMWrapper::setUseBrm( true );
      // test column struct list
      CreateColumnFile( 150, 4, WriteEngine::CHAR, WriteEngine::WR_CHAR );

      SetColumnStruct( curColStruct, 150, 4, WriteEngine::CHAR );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 150 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 4 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::CHAR );

      // test column values
      // add values for the first column
      curString = "ab1";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "ab2";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      curTupleList.clear();
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // try to insert more rows and row id should increase
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_wrapper.commit( m_session.txnid );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // txnid increase by one
      m_session.txnid++;

      colValueList.clear();
      curString = "bb1";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "bb2";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_wrapper.commit( m_session.txnid );
      CPPUNIT_ASSERT( rc == NO_ERROR );


      // txnid increase by one
      m_session.txnid++;

      colValueList.clear();
      curString = "cb1";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      curString = "cb2";
      curTuple.data = curString;
      curTupleList.push_back( curTuple );

      colValueList.push_back( curTupleList );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_wrapper.rollbackTran( m_session.txnid );
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testInterfaceInsertRowHwm() {
      ColStruct               curColStruct, testColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;
      int                     rc;
      ColumnOp                colOp;

      CreateColumnFile( 300, 1, WriteEngine::TINYINT, WriteEngine::WR_BYTE );
      CreateColumnFile( 400, 4, WriteEngine::INT, WriteEngine::WR_INT );

      // test column struct list
      SetColumnStruct( curColStruct, 300, 1, WriteEngine::TINYINT );
      colStructList.push_back( curColStruct );

      SetColumnStruct( curColStruct, 400, 4, WriteEngine::INT );
      colStructList.push_back( curColStruct );

      testColStruct = colStructList[0];
      CPPUNIT_ASSERT( testColStruct.dataOid == 300 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 1 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::TINYINT );

      testColStruct = colStructList[1];
      CPPUNIT_ASSERT( testColStruct.dataOid == 400 );
      CPPUNIT_ASSERT( testColStruct.colWidth == 4 );
      CPPUNIT_ASSERT( testColStruct.colDataType == WriteEngine::INT );

      m_session.txnid++;
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      // test column values
      // add values for the first column
      for( int i = 0; i < 9000; i++ ) {
         curTuple.data = (char)(i%20);
         curTupleList.push_back( curTuple );
      }
      colValueList.push_back( curTupleList );

      curTupleList.clear();

      // add values for the second column
      for( int i = 0; i < 9000; i++ ) {
         curTuple.data = i;
         curTupleList.push_back( curTuple );
      }
      colValueList.push_back( curTupleList );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

//      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( ridList.size() == 9000 );
   }


   void testInterfaceCombineIndexLoad() {
      int                     rc;
      ColumnOp                colOp;
      ColStruct               curColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;

      Cache::init();
      Cache::setUseCache( true );

      m_wrapper.setDebugLevel( DEBUG_0 );

      colOp.dropColumn( 400 );
      CreateColumnFile( 400, 4, WriteEngine::INT, WriteEngine::WR_INT );

      // test column struct list
      SetColumnStruct( curColStruct, 400, 4, WriteEngine::INT );
      colStructList.push_back( curColStruct );

      m_session.txnid++;
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      curTupleList.clear();
      int totalValue = 2000000;
      // add values for the first column
      for( int i = 0; i < totalValue; i++ ) {
         curTuple.data = rand() % 100;
         curTupleList.push_back( curTuple );
      }
      colValueList.push_back( curTupleList );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      //      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT(  (int)ridList.size() == totalValue );

      if( colOp.exists( 900 ) ) {
         rc = m_wrapper.dropIndex( m_session.txnid, 900, 901 );
         CPPUNIT_ASSERT( rc == NO_ERROR );
      }

      rc = m_wrapper.createIndex( m_session.txnid, 900, 901 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      uint64_t totalRows;
      rc = m_wrapper.buildIndex( 400, 900, 901, INT, 4, 0, true, totalRows );
      printf( "\nrc=%d", rc );
      printf( "\ntotalRows=%llu", totalRows );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      Cache::freeMemory();
   }

   void testTmpBulkPerformance() {
      int                     rc;
      ColumnOp                colOp;
      ColStruct               curColStruct;
      ColTuple                curTuple, testTuple;
      ColTupleList            curTupleList, testTupleList;
      ColStructList           colStructList;
      ColValueList            colValueList, colOldValList;
      RIDList                 ridList;

      Cache::init();
      Cache::setUseCache( true );

      m_wrapper.setDebugLevel( DEBUG_0 );

/*      colOp.dropColumn( 400 );
      CreateColumnFile( 400, 4, WriteEngine::INT, WriteEngine::WR_INT );

      // test column struct list
      SetColumnStruct( curColStruct, 400, 4, WriteEngine::INT );
      colStructList.push_back( curColStruct );

      m_session.txnid++;
      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == ERR_STRUCT_VALUE_NOT_MATCH );

      curTupleList.clear();
      int totalValue = 2000000;
      // add values for the first column
      for( int i = 0; i < totalValue; i++ ) {
         curTuple.data = rand() % 100;
         curTupleList.push_back( curTuple );
      }
      colValueList.push_back( curTupleList );

      CPPUNIT_ASSERT( m_wrapper.checkValid( m_session.txnid, colStructList, colValueList, ridList ) == NO_ERROR );

      //      m_wrapper.printInputValue( (OID)103, colStructList, colValueList, ridList );
      rc = m_wrapper.insertColumnRec( m_session.txnid, colStructList, colValueList, ridList );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT(  (int)ridList.size() == totalValue );
*/

      if( colOp.exists( 900 ) ) {
         rc = m_wrapper.dropIndex( m_session.txnid, 900, 901 );
         CPPUNIT_ASSERT( rc == NO_ERROR );
      }

      rc = m_wrapper.createIndex( m_session.txnid, 900, 901 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      uint64_t totalRows;
      std::string indexName;
      rc = m_wrapper.buildIndex( "CPL_299_23839.dat", 900, 901, INT, 4, 0, false, totalRows , indexName, 0, 2000000 );
      printf( "\nrc=%d", rc );
      printf( "\ntotalRows=%llu", totalRows );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      Cache::freeMemory();
   }
void testBoostFloat()
{
   boost::any anyVal;
   
   unsigned int      fValue =0xffaaaaaa;
   unsigned int      testfValue;
   unsigned int      testfValue2;
  
   float             fValue1 = 0xffaaaaaa;
   float             testfValue11 =0 ;
   float             testfValue12 =0;
   int               iSize = 0;
   char              charBuf[100];
   FILE*             pFile;
   
   ColumnOp          colOp;
   Column            curCol;
   int               rc;
   RID               rowIdArray[3];
   int               width = 4, totalRow = 3;
   FID               fid = 800;
   float             valArray[3];
   uint64_t          oldValArray[3];
   CalpontSystemCatalog::ColDataType       colDataType = CalpontSystemCatalog::FLOAT;
   ColType           colType = WriteEngine::WR_FLOAT;
   //uint64_t          emptyVal = 0xFFAAAAAB;
   
 

 
      rowIdArray[0] = 0;
      rowIdArray[1] = 1;
      rowIdArray[2] = 2;

      valArray[0] = 8.123;
      valArray[1] = 5.345;
      valArray[2] = 3.789;

      CreateColumnFile( fid, width, colDataType, colType );

      colOp.initColumn( curCol );
      colOp.setColParam( curCol, 2, width, colDataType, colType, fid );
      rc = colOp.openColumnFile( curCol );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = colOp.writeRow( curCol, totalRow, (RID*) rowIdArray, valArray, oldValArray );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( int i = 0; i < totalRow; i++ )
         cout<< "emptyVal=" << oldValArray[i]<< endl;

      colOp.closeFile(curCol.dataFile.pFile);
      pFile = fopen( "/home/jhuang/tmp/000.dir/000.dir/003.dir/FILE032.cdf", "r" );
      iSize = fread( charBuf, 1, 4, pFile );
      memcpy(&testfValue12, charBuf, 4);
      cout << "Value1=" << testfValue12 << endl;
      iSize = fread( charBuf, 1, 4, pFile );
      memcpy(&testfValue12, charBuf, 4);
      cout << "Value2=" << testfValue12 << endl;
      iSize = fread( charBuf, 1, 4, pFile );
      memcpy(&testfValue12, charBuf, 4);
      cout << "Value3=" << testfValue12 << endl;
      fclose(pFile);
  
   anyVal = fValue1;
   testfValue11 = boost::any_cast<float>( anyVal );   
   memcpy(charBuf, &testfValue11, 4);
   pFile = fopen( "test", "w+b" );
   if( pFile != NULL ) {
      iSize = fwrite((char*) charBuf, 4, 1, pFile );
      printf( "\niSize=%d \n", iSize );
      fclose( pFile );
   }
   unsigned char charBuf2[100];
   memset(charBuf2,'\0', 100);
   pFile = fopen("test","r");
   iSize = fread( charBuf2, 4, 1, pFile );
   memcpy(&testfValue12, charBuf2, 4);
   
   cout << " Using Float, NULL value=" << testfValue12 << endl;
   
   anyVal = fValue;
   testfValue = boost::any_cast<unsigned int>( anyVal );   
   memcpy(charBuf, &testfValue, 4);
   pFile = fopen( "test", "w+b" );
   if( pFile != NULL ) {
      iSize = fwrite((char*) charBuf, 4, 1, pFile );
      printf( "\niSize=%d \n", iSize );
      fclose( pFile );
   }
   memset(charBuf2,'\0', 100);
   pFile = fopen("test","r");
   iSize = fread( charBuf2, 4, 1, pFile );
   memcpy(&testfValue2, charBuf2, 4);
   cout << " Using unsinged int, NULL value=" << testfValue2 << endl;

   // Other method
   
   anyVal = fValue;
   testfValue = boost::any_cast<unsigned int>( anyVal );  
   ofstream fout("file.dat", ios::binary);
   fout.write((char *)(&testfValue), sizeof(testfValue));
   fout.close();
   
   ifstream fin("file.dat", ios::binary);
   fin.read((char *)(&testfValue2), sizeof(testfValue2));
   cout << "Using unsigned int NULL value="<< testfValue2 << endl; 
   
}
   void testCreateColumnFiles1() {
      ColumnOp colOp;
      Column   curCol;
      int      rc;
      OID      fid = 10;
      
      m_session.txnid++;
      // file opertaions
      
      //colOp.startfTimer();
      colOp.initColumn( curCol );
      rc = colOp.createColumn( curCol, 0, 8, WriteEngine::CHAR, WriteEngine::WR_CHAR, (FID)fid );
      //colOp.stopfTimer();
      colOp.clearColumn( curCol );
      //rc = m_wrapper.createColumn( m_session.txnid, fid, WriteEngine::CHAR, 8);
      
      //cout<< "total run time for 1th column we_char size=8 -> " << colOp.getTotalfRunTime() << " msec"<< endl;
      colOp.deleteFile( fid);
      sleep(3);
   }

   void testCreateColumnFiles2() {
      ColumnOp colOp;
      Column   curCol;
      int      rc;
      OID      fid = 20;

      m_session.txnid++;
      // file opertaions
      
      //colOp.startfTimer();
      colOp.initColumn( curCol );
      rc = colOp.createColumn( curCol, 0, 4, WriteEngine::INT, WriteEngine::WR_CHAR, (FID)fid );
      //colOp.stopfTimer();
      colOp.clearColumn( curCol );
      //rc = m_wrapper.createColumn( m_session.txnid, fid, WriteEngine::INT, 4);
      //colOp.stopfTimer();
      //cout<< "total run time for 2th column we_int size=4 -> " << colOp.getTotalfRunTime() << " msec"<< endl;
      CPPUNIT_ASSERT( rc == NO_ERROR );
      colOp.deleteFile( fid);
      sleep(5);
   }
   void testCreateColumnFiles3() {
      ColumnOp colOp;
      Column   curCol;
      int      rc;
      OID      fid = 30;

      m_session.txnid++;
      // file opertaions
      
      //colOp.startfTimer();
      colOp.initColumn( curCol );
      rc = colOp.createColumn( curCol, 0, 4, WriteEngine::CHAR, WriteEngine::WR_CHAR, (FID)fid );
      colOp.clearColumn( curCol );
      //colOp.stopfTimer();
      //rc = m_wrapper.createColumn( m_session.txnid, fid, WriteEngine::CHAR, 4);
      //colOp.stopfTimer();
      //cout<< "total run time for 3th column we_char size=4 -> " << colOp.getTotalfRunTime() << " msec"<< endl;
      CPPUNIT_ASSERT( rc == NO_ERROR );
      colOp.deleteFile( fid);
      sleep(3);
   }
   void testCreateColumnFiles4() {
      ColumnOp colOp;
      Column   curCol;
      int      rc;
      OID      fid = 40;

      m_session.txnid++;
      // file opertaions
      
      //colOp.startfTimer();
      colOp.initColumn( curCol );
      rc = colOp.createColumn( curCol, 0, 2, WriteEngine::SMALLINT, WriteEngine::WR_CHAR, (FID)fid );
      colOp.clearColumn( curCol );
      //rc = m_wrapper.createColumn( m_session.txnid, fid, WriteEngine::SMALLINT, 2 );
      //colOp.stopfTimer();
      //cout<< "total run time for 4th column we_smallint size=2 -> " << colOp.getTotalfRunTime() << " msec"<< endl;
      CPPUNIT_ASSERT( rc == NO_ERROR );
      colOp.deleteFile( fid);
   }
   void testCreateFileMultipleBF() 
   {
     FileOp fileop;
     ColumnOp colOp;
     char  fileName[FILE_NAME_SIZE];
     int   rc, numOfBlock=8192,allocSize=8192;
     FILE* pFile;
     int t_diff;                   
     struct timeb   t_start, t_current;   

     
     OID fid=600;
     uint64_t emptyVal = 0;
     int width =4;
         
     rc = fileop.oid2FileName( fid, fileName, true ) ;
     emptyVal = colOp.getEmptyRowValue( WriteEngine::INT, width);
     int multiple=1;
     int writeSize = multiple* BYTE_PER_BLOCK;      
     unsigned char  writeBuf[writeSize];     
     BRMWrapper::getInstance()->allocateExtent( (const OID)fid, numOfBlock, allocSize );
     
     pFile = fopen( fileName, "w+b" );
     if( pFile != NULL ) {
         fileop.setEmptyBuf( writeBuf, writeSize , emptyVal, width );
         //setvbuf(pFile , NULL, _IONBF, 0 );
         int loopSize = numOfBlock/multiple;
         ftime(&t_start);
         for( int i = 0; i < loopSize; i++ )
         {
            fwrite( writeBuf, writeSize, 1, pFile );           
         }
         ftime(&t_current);
         t_diff= (int) (1000.0 * (t_current.time - t_start.time)+ 
                                     (t_current.millitm - t_start.millitm));
         cout<< "total run time for create file 64MB with writesize= " << writeSize << " byte" << "loop size="
         << loopSize << " times" << " Time for Multiple Write 64MB with Buffer->we_int size=4 -> " << t_diff << " msec"<< endl;

         fileop.closeFile( pFile );
     }
          colOp.deleteFile( fid);
   }
   void testCreateFilelargerBF()
   {
     FileOp fileop;
     ColumnOp colOp;
     char  fileName[FILE_NAME_SIZE];
     int   rc, numOfBlock=8192,allocSize=8192;
     FILE* pFile;

     int t_diff;
     struct timeb   t_start, t_current;


     OID fid=1600;
     uint64_t emptyVal = 0;
     int width =4;

     rc = fileop.oid2FileName( fid, fileName, true ) ;
     emptyVal = colOp.getEmptyRowValue( WriteEngine::INT, width);
     int multiple=1;
     int writeSize = multiple* BYTE_PER_BLOCK;
     unsigned char  writeBuf[writeSize];
     BRMWrapper::getInstance()->allocateExtent( (const OID)fid, numOfBlock, allocSize );

     pFile = fopen( fileName, "w+b" );
     if( pFile != NULL ) {
         fileop.setEmptyBuf( writeBuf, writeSize , emptyVal, width );
         
         setvbuf(pFile , NULL, _IOFBF, DEFAULT_BUFSIZ);
         //setvbuf(pFile , (char*)writeBuf, _IOFBF, writeSize/*DEFAULT_WRITE_BUFSIZE*/);
         int loopSize = allocSize/multiple;
         ftime(&t_start);
         for( int i = 0; i < loopSize; i++ )
         {
            fwrite( writeBuf, writeSize, 1, pFile );
         }
         ftime(&t_current);
         t_diff= (int) (1000.0 * (t_current.time - t_start.time)+
                                     (t_current.millitm - t_start.millitm));
         cout<<"Buffer size =" << DEFAULT_BUFSIZ << endl;
         cout<< "total run time for create file 64MB with writesize= " << writeSize << " byte" << "loop size="
         << loopSize << " times" << " Time for Multiple Write 64MB with Buffer->we_int size=4 -> " << t_diff << " msec"<< endl;

         fileop.closeFile( pFile );
     }
          colOp.deleteFile( fid);
   }

   void testCreateFileMultipleIONBF() 
   {
     FileOp fileop;
     ColumnOp colOp;
     char  fileName[FILE_NAME_SIZE];
     int   rc, numOfBlock=8192,allocSize=8192;
     FILE* pFile;
     int t_diff;                   
     struct timeb   t_start, t_current;   

     
     OID fid=400;
     uint64_t emptyVal = 0;
     int width =4;
         
     rc = fileop.oid2FileName( fid, fileName, true ) ;
     emptyVal = colOp.getEmptyRowValue( WriteEngine::INT, width);
     int multiple=128;
     int writeSize = multiple* BYTE_PER_BLOCK;      
     unsigned char  writeBuf[writeSize];     
     BRMWrapper::getInstance()->allocateExtent( (const OID)fid, numOfBlock, allocSize );
     
     pFile = fopen( fileName, "w+b" );
     if( pFile != NULL ) {
         fileop.setEmptyBuf( writeBuf, writeSize , emptyVal, width );
         setvbuf(pFile , NULL, _IONBF, 0 );
         int loopSize = numOfBlock/multiple;
         ftime(&t_start);
         for( int i = 0; i < loopSize; i++ )
         {
            fwrite( writeBuf, writeSize, 1, pFile );           
         }
         ftime(&t_current);
         t_diff= (int) (1000.0 * (t_current.time - t_start.time)+ 
                                     (t_current.millitm - t_start.millitm));
         cout<< "total run time for create file 64MB with writesize= " << writeSize << " byte" << "loop size="
         << loopSize << " times" << " Time for Multiple Write 64MB NO Buffer->we_int size=4 -> " << t_diff << " msec"<< endl;

         fileop.closeFile( pFile );
     }
          colOp.deleteFile( fid);
   }
   void testCreateFile64MBIONBF() 
   {
     FileOp fileop;
     ColumnOp colOp;
     char  fileName[FILE_NAME_SIZE];
     int   rc, numOfBlock=8192,allocSize=8192;
     FILE* pFile;
     int t_diff;                   
     struct timeb   t_start, t_current;   

     OID fid=500;
     uint64_t emptyVal = 0;
     int width = 4;
         
     rc = fileop.oid2FileName( fid, fileName, true ) ;
     emptyVal = colOp.getEmptyRowValue( WriteEngine::INT, width);
     int multiple= 8192;
     int writeSize = multiple* BYTE_PER_BLOCK;      
     unsigned char*  writeBuf;     
     BRMWrapper::getInstance()->allocateExtent( (const OID)fid, numOfBlock, allocSize );
     
     pFile = fopen( fileName, "w+b" );
     if( pFile != NULL ) {
         writeBuf = new unsigned char[writeSize];
         fileop.setEmptyBuf( writeBuf, writeSize , emptyVal, width );
         setvbuf(pFile , NULL, _IONBF, 0 );
         
         ftime(&t_start);
         fwrite( writeBuf, writeSize, 1, pFile );           
         ftime(&t_current);
         t_diff= (int) (1000.0 * (t_current.time - t_start.time)+ 
                                     (t_current.millitm - t_start.millitm));

         cout<< "total run time for create file Single Write 64MB NO Buffer->we_int size=4 -> " << t_diff << " msec"<< endl;
         fileop.closeFile( pFile );
     }
     colOp.deleteFile( fid);
   }
   void testCreateFileNoBRM() 
   {
     FileOp fileop;
     ColumnOp colOp;
     char  fileName[FILE_NAME_SIZE];
     int   rc, allocSize=8192;
     
     OID fid=401;
     uint64_t emptyVal = 0;
     
     
     //colOp.deleteFile( fid);
     
     //cout << "start time=" << colOp.t_start << " msec" << endl;
     
     rc = fileop.oid2FileName( fid, fileName, true ) ;
     emptyVal = colOp.getEmptyRowValue( WriteEngine::INT, 4);
     //colOp.startfTimer();
     
     fileop.createFile( fileName, allocSize, emptyVal, 4 );
     //colOp.stopfTimer();
     //cout<<"stop time=" << colOp.t_current << " msec"<< endl;
     //cout<< "total run time for column NO BRM we_int size=4 -> " << colOp.getTotalfRunTime() << " msec"<< endl;
     colOp.deleteFile( fid);
     
   }

   void testSoloman()
   {
     char  fileName[80]="/usr/local/Calpont/data1/test.dat";
     char  outfile[80] = "/usr/local/Calpont/data1/out.dat";
     int   numOfBlock=8192,blockSize=8192;
     FILE* testFile;
     FILE* outFile;
     int t_diff;
     struct timeb   t_start, t_current;
     int multiple=numOfBlock;
     int writeSize = multiple * blockSize;
     unsigned char* writeBuf;
     writeBuf = new unsigned char[writeSize];
     memset(writeBuf, 0, writeSize);
     testFile = fopen( fileName, "w+b" );
     outFile = fopen(outfile, "w");
     if( testFile != NULL ) {
         setvbuf(testFile , (char*)writeBuf, _IOFBF, writeSize);
         int loopSize = numOfBlock/multiple;
         ftime(&t_start);
         for( int i = 0; i < loopSize; i++ )
         {
            fwrite( writeBuf, writeSize, 1, testFile );
         }
         ftime(&t_current);
         t_diff= (int) (1000.0 * (t_current.time - t_start.time)+
                                     (t_current.millitm - t_start.millitm));
         char buff[256];
         sprintf(buff, " create a 64MB file with writesize= %d byte", writeSize);
         fwrite(buff, strlen(buff), 1 ,outFile);
         sprintf(buff, " loop size = %d", loopSize);
         fwrite(buff, strlen(buff), 1 ,outFile);
         sprintf(buff," Time for writing %d = %d msec", writeSize, t_diff);
         fwrite(buff, strlen(buff), 1 ,outFile);
         fclose( testFile );
         fclose( outFile);
     }
     else
         fprintf(outFile,"FILE DOES NOT EXIST: %s", fileName);
     delete [] writeBuf;

   }

};

CPPUNIT_TEST_SUITE_REGISTRATION( WriteEngineTest );

#include <cppunit/extensions/TestFactoryRegistry.h>
#include <cppunit/ui/text/TestRunner.h>

int main( int argc, char **argv)
{
  CppUnit::TextUi::TestRunner runner;
  CppUnit::TestFactoryRegistry &registry = CppUnit::TestFactoryRegistry::getRegistry();
  runner.addTest( registry.makeTest() );
  bool wasSuccessful = runner.run( "", false );
  return (wasSuccessful ? 0 : 1);
}


