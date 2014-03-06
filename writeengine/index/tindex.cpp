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
#include <stdlib.h>

#include <bitset>
#include <map>

using namespace std;

#include <boost/scoped_ptr.hpp>
#include <boost/progress.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "we_indextree.h"
#include "we_freemgr.h"
#include "we_indexlist.h"

using namespace WriteEngine;

class IndexTest : public CppUnit::TestFixture {


CPPUNIT_TEST_SUITE( IndexTest );

//CPPUNIT_TEST( tmpTest );

//PUNIT_TEST( multimapdemo );
//PUNIT_TEST( sortvecdemo );
//PUNIT_TEST( sortarrdemo );

// CPPUNIT_TEST( spacehog );
//CPPUNIT_TEST( test1 );
// CPPUNIT_TEST( test2 );

// Index basic testing

 CPPUNIT_TEST( testStructIO );
 CPPUNIT_TEST( testAddTreeNode );
 CPPUNIT_TEST( testSetupBittestArray );

// Index tree testing
 CPPUNIT_TEST( testTreeGetTestbitValue );
 CPPUNIT_TEST( testTreeGetTreeNodeInfo );
 CPPUNIT_TEST( testTreeGetTreeMatchEntry );
 CPPUNIT_TEST( testTreeMoveEntry );
 CPPUNIT_TEST( testTreeBuildEmptyTree );
 CPPUNIT_TEST( testTreeBuildExistTree1 );
 CPPUNIT_TEST( testTreeBuildExistTree2 );
 CPPUNIT_TEST( testTreeIntegration );
// 
 CPPUNIT_TEST( testIntegrationVolume );

 CPPUNIT_TEST( testCreateAndUpdateIndex );

// 
 CPPUNIT_TEST( testDeleteIndex );
// 
 CPPUNIT_TEST( testResetIndex );
// 
 CPPUNIT_TEST( testUpdateMutiColIndex );



// Index free manager testing
 CPPUNIT_TEST( testFreeMgrBRM );
 CPPUNIT_TEST( testFreeMgrInit );
 CPPUNIT_TEST( testFreeMgrInit2 );
 CPPUNIT_TEST( testFreeMgrAssign );
 CPPUNIT_TEST( testFreeMgrAssignList );
 CPPUNIT_TEST( testFreeMgrAssignListCk );
 CPPUNIT_TEST( testFreeMgrAssignListCk2 );
 CPPUNIT_TEST( testFreeMgrAssignListLots );
 CPPUNIT_TEST( testFreeMgrAssignListBlocks );
 CPPUNIT_TEST( testFreeMgrAssignExtend );
 CPPUNIT_TEST( testFreeMgrExtendLots );
 CPPUNIT_TEST( testFreeMgrRelease );
 CPPUNIT_TEST( testFreeMgrFragment );
 CPPUNIT_TEST( testFreeMgrChain );

// Index list testing
CPPUNIT_TEST(testIndexListMultiKey);
CPPUNIT_TEST(testIndexListUpdate);
CPPUNIT_TEST(testIndexListDelete);
CPPUNIT_TEST(testIndexListReleaseMgrBack);
CPPUNIT_TEST(testIndexListMultipleAddHdr);
CPPUNIT_TEST(testIndexListMultipleUpdate);


CPPUNIT_TEST_SUITE_END();

private:
   IndexTree                  m_index;
   IndexList                  m_indexlist;
   FreeMgr                    m_freeMgr;
public:
	void setUp() {
        m_index.setUseFreeMgr( false );
        m_index.setUseListMgr( false );
        m_index.setTransId( 124353 );  // a dummy transaction id

        // init the 000.dir
//        int rc = m_index.createIndex( 20, 21 );
//        CPPUNIT_ASSERT( rc == NO_ERROR );
        BRMWrapper::setUseBrm(true);
         m_indexlist.setDebugLevel(DEBUG_0);
         m_indexlist.setNarray(true);
	}

	void testInit() {

	}

	void tearDown() {
	}
/*
    void tmpTest() {
      int   rc, width = 32, i;

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );
      BRMWrapper::setUseBrm( true );
      BRMWrapper::setUseVb( false );
      Cache::setUseCache( true );
      Cache::init();

      m_index.setTransId( 12345678 );
      m_index.dropIndex( 990, 991 );
      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
//       m_index.m_freeMgr.setDebugLevel( DEBUG_1 );
      FILE* pFile;
      uint64_t   key, curkey, lastkey;
      RID   rid, ridArray[5000];
      int   counter = 0, ridCounter = 0;

      pFile = fopen( "test.txt", "r" );
      CPPUNIT_ASSERT( pFile != NULL );

      while( !feof( pFile ) && rc == NO_ERROR )
      {
         fscanf( pFile, "%lld %lld\n", &key, &rid );
         curkey = key;
         if( curkey == lastkey || counter == 0 || ridCounter == MAX_IDX_RID - 1 )
            ridArray[ridCounter++] = rid;
         else {
            m_index.m_multiRid.setMultiRid( ridArray, ridCounter );
            rc = m_index.updateIndex( lastkey, width, ridArray[0] );
            ridCounter = 0;
            ridArray[ridCounter++] = rid;
         } // end of if( curkey == lastkey ) {
         lastkey = curkey;

//               printf( "%d %d\n", (int)key, (int)rid );
         counter++;
         if( counter%1000 == 0 )
                 printf( "\ncounter=%d", counter );
      } // end of while


      // last piece
      if( rc == NO_ERROR && ridCounter > 0 ) {
         m_index.m_multiRid.setMultiRid( ridArray, ridCounter );
         rc = m_index.updateIndex( lastkey, width, ridArray[0] );
      }

      m_index.flushCache();
      m_index.closeIndex();

      Cache::freeMemory();
    }
*/


#define NUMELEM 30000000
    void multimapdemo() {
        multimap < int, int  >mul;

        timer t1, t2;
        unsigned int idx;
        map < int, int > mm;
        
        for( idx = 0; idx < NUMELEM; idx++){
            mul.insert( pair<int, int>(idx, idx) );
            if( idx%10000 == 0){
                printf("\nMultimap: index is %06i and time is:%lf (elapsed is %lf)", idx, t1.elapsed(), t2.elapsed());
                t1.restart();
            }
        }
        
        mul.clear();
    }
    bool pairCompare( const pair<int, int> &lhs, const pair<int, int> &rhs){
        return lhs.second > rhs.second;
    }
    void sortvecdemo() {
        vector< pair<int, int> > idxvec;

        timer t1, t2;
        unsigned int idx;
        
        for( idx = 0; idx < NUMELEM; idx++){
            idxvec.push_back( pair<int, int>(idx, idx) );
            if( idx%10000 == 0){
                printf("Sortvec: index is %06i and time is:%lf (elapsed is %lf)\n", idx, t1.elapsed(), t2.elapsed());
                t1.restart();
            }
        }
        sort(idxvec.begin(), idxvec.end());
        printf("Sortvec:  time is:%lf (elapsed is %lf)\n", t1.elapsed(), t2.elapsed());
        idxvec.clear();
    }
    
    void sortarrdemo() {
        int *idxarr;
        idxarr = (int*)malloc(sizeof(int)*NUMELEM);
        if (idxarr==0){
            printf("\nOOPs..\n");
exit(-1);
}
        timer t1, t2;
        unsigned int idx;
        
        for( idx = 0; idx < NUMELEM; idx++){
            idxarr[idx] = idx;
            if( idx%10000 == 0){
                printf("Sortarr: index is %06i and time is:%lf (elapsed is %lf)\n", idx, t1.elapsed(), t2.elapsed());
                t1.restart();
            }
        }
        sort(idxarr, idxarr+NUMELEM);
        printf("Sortarr:  time is:%lf (elapsed is %lf)\n", t1.elapsed(), t2.elapsed());
        
    }
    
    void spacehog() {
        // test case to estimate space requirements
        // generate an index with 200K entries, each unique

        int   rc;
        uint64_t i;
        printf("\nUT: Beginning Space Hog\n");
        m_index.setUseFreeMgr( true );
        m_index.setUseListMgr( true );

        rc = m_index.createIndex( 123, 124 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        rc = m_index.openIndex( 123, 124 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        for(i=0; i< 800*1000; i++)
        {
            if (i%(1000*10)==0){
                printf("UT: I is now: %i\n", (int)i);
            }
            rc = m_index.updateIndex( i*10*rand(), 32, i );

            if (rc != NO_ERROR)
            {
                printf("\nShock and horror! i=%llu  rc=%i\n",i, rc);
            }

            CPPUNIT_ASSERT( rc == NO_ERROR );
        }


        m_index.setAssignFbo( 1 );
        m_index.closeIndex();

        rc = m_index.dropIndex( 123, 124 );
        CPPUNIT_ASSERT( rc == NO_ERROR );

        m_index.setUseFreeMgr( false );
        m_index.setUseListMgr( false );

        printf("\nUT: Finishing Space Hog\n");
    }

    void test1() {
      int   rc, width = 64, i;

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );
      BRMWrapper::setUseBrm( true );
      BRMWrapper::setUseVb( false );
//      Cache::setUseCache( true );
//      Cache::init();

      m_index.setTransId( 12345678 );
      m_index.dropIndex( 990, 991 );
      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
//       m_index.m_freeMgr.setDebugLevel( DEBUG_1 );
      DataBlock curBlock;
      char   buf[20];
      uint64_t key;
      timer t1, t2;
        for( i = 0; i < 100000; i++ ) {
            sprintf( buf, "%dSuccess", i );
            memcpy( &key, buf, 8 );

            if (i%(1000)==0){
                printf("UT: I is now: %i, elapsed time %lf s, delta %lf s\n", (int)i, t1.elapsed(), t2.elapsed());
                t2.restart();
            }

            rc = m_index.updateIndex( key, width, i );
            if( rc != NO_ERROR )
                printf( "\n i =%d  rc=%d", i, rc );
            CPPUNIT_ASSERT( rc == NO_ERROR );
        }
        printf("UT: I is now: %i, elapsed time %lf s, delta %lf s\n", (int)i, t1.elapsed(), t2.elapsed());

        curBlock = m_index.getRootBlock();
        printf( "\n block 0 subblock 0 " );
        m_index.printMemSubBlock( &curBlock, 0 );
        printf( "\n block 0 subblock 1 " );
        m_index.printMemSubBlock( &curBlock, 1 );

        m_index.printSubBlock( 11, 10 );
        m_index.closeIndex();

//        rc = m_index.dropIndex( 990, 991 );
//        CPPUNIT_ASSERT( rc == NO_ERROR );

        m_index.setUseFreeMgr( false );
        m_index.setUseListMgr( false );

//      Cache::freeMemory();
    }

   void test2() {
       int   rc, width = 16, i;

       m_index.setUseFreeMgr( true );
       m_index.setUseListMgr( true );

       rc = m_index.createIndex( 990, 991 );
       CPPUNIT_ASSERT( rc == NO_ERROR );

       rc = m_index.openIndex( 990, 991 );
       CPPUNIT_ASSERT( rc == NO_ERROR );


       DataBlock curBlock;

       for( i = 10; i < 381; i++ ) {

           curBlock = m_index.getRootBlock();
           printf( "\n i=%d", i );

           if (i==130){
               printf("\n130");;
           }
           if( i==20 || i == 128 || i == 129 || i==64 || i==66) {
               printf( "\n******* before call updateIndex (i=%i)", i );
               m_index.printMemSubBlock( &curBlock, 0 );
               m_index.printSubBlock( 63, 19 );
               m_index.printSubBlock( 63, 31 );
           }

           rc = m_index.updateIndex( 2*i, width, i*100 );
           if( rc != NO_ERROR )
               printf( "\n i =%d  rc=%d", i, rc );

           if( i==20 || i == 128 || i == 129 || i==64 || i==66) {
               printf( "\n******* after call updateIndex (i=%i)", i );
               m_index.printMemSubBlock( &curBlock, 0 );
               m_index.printSubBlock( 63, 19 );
               m_index.printSubBlock( 63, 31 );
           }

           CPPUNIT_ASSERT( rc == NO_ERROR );
       }


       m_index.setAssignFbo( 1 );
       m_index.closeIndex();

       rc = m_index.dropIndex( 990, 991 );
       CPPUNIT_ASSERT( rc == NO_ERROR );

       m_index.setUseFreeMgr( false );
       m_index.setUseListMgr( false );

   }

   void testStructIO() {
      DataBlock               curBlock;
      FILE*                   pFile;
      int                     rc, fbo = 1, sbid = 2, entryNo = 5, width = 8, allocSize;
      IdxBitmapPointerEntry   bitmapPtr;

      memset( &bitmapPtr, 0, 8 );
      memset( curBlock.data, 0, sizeof( curBlock.data ) );

      m_index.deleteFile( 999 );
      CPPUNIT_ASSERT( m_index.exists( 999 ) == false );

      CPPUNIT_ASSERT( m_index.createFile( 999, 10, allocSize ) == NO_ERROR );

      pFile = m_index.openFile( 999 );
      CPPUNIT_ASSERT( pFile != NULL );

      rc = m_index.readSubBlockEntry( pFile, &curBlock, fbo, sbid, entryNo, width, &bitmapPtr );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      bitmapPtr.type = 1;
      bitmapPtr.fbo = 2;
      bitmapPtr.sbid = 8;
      bitmapPtr.entry = 7;

      rc = m_index.writeSubBlockEntry( pFile, &curBlock, fbo, sbid, entryNo, width, &bitmapPtr );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      m_index.closeFile( pFile );

      // reload the value
      memset( &bitmapPtr, 0, 8 );
      memset( curBlock.data, 0, sizeof( curBlock.data ) );

      CPPUNIT_ASSERT( m_index.isAddrPtrEmpty( &bitmapPtr, EMPTY_PTR ) == true );

      pFile = m_index.openFile( 999 );
      CPPUNIT_ASSERT( pFile != NULL );

      rc = m_index.readDBFile( pFile, curBlock.data, fbo );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.getSubBlockEntry( curBlock.data, sbid, entryNo, width, &bitmapPtr );

      CPPUNIT_ASSERT( bitmapPtr.type == 1 );
      CPPUNIT_ASSERT( bitmapPtr.fbo == 2 );
      CPPUNIT_ASSERT( bitmapPtr.sbid == 8 );
      CPPUNIT_ASSERT( bitmapPtr.entry == 7 );

      CPPUNIT_ASSERT( m_index.isAddrPtrEmpty( &bitmapPtr, BITMAP_PTR/*EMPTY_PTR*/ ) == false );

      uint64_t i;

      for( i = 0; i < 15; i++ ) {
         bitmapPtr.type = i%8;
         bitmapPtr.fbo = 2 * i;
         bitmapPtr.sbid = i;
         bitmapPtr.entry = i + 6;

         m_index.setSubBlockEntry( curBlock.data, i, 0, 8, &bitmapPtr );
      }
      rc = m_index.writeDBFile( pFile, curBlock.data, fbo );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.closeFile( pFile );

      for( i = 0; i < 15; i++ ) {
         m_index.getSubBlockEntry( curBlock.data, i, 0, 8, &bitmapPtr );
         CPPUNIT_ASSERT( bitmapPtr.type == i%8 );
//         CPPUNIT_ASSERT( bitmapPtr.oid == i );
         CPPUNIT_ASSERT( bitmapPtr.fbo == 2 * i );
         CPPUNIT_ASSERT( bitmapPtr.sbid == i );
         CPPUNIT_ASSERT( bitmapPtr.entry == i + 6 );
      }
   }

   void testAddTreeNode() {
      uint64_t          key = 123, rid = 1900, width = 8, testbitVal = 3;
      IdxTree           myTree;
      IdxBitmapPointerEntry bitmapEntry;

      m_index.clearTree( &myTree );
      bitmapEntry.fbo = 6;
      bitmapEntry.sbid = 1;
      bitmapEntry.entry =9;

      m_index.setTreeHeader( &myTree, key, rid, width, testbitVal, bitmapEntry);
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.rid == rid );
      CPPUNIT_ASSERT( myTree.maxLevel == ((width/5) + 1) );
      CPPUNIT_ASSERT( myTree.node[0].level == 0 );
      CPPUNIT_ASSERT( myTree.node[0].allocCount == ENTRY_PER_SUBBLOCK );
//      CPPUNIT_ASSERT( myTree.node[0].group == ENTRY_32 );
      CPPUNIT_ASSERT( myTree.node[0].used == true );
      CPPUNIT_ASSERT( myTree.node[0].next.fbo == bitmapEntry.fbo );
      CPPUNIT_ASSERT( myTree.node[0].next.sbid == bitmapEntry.sbid );
      CPPUNIT_ASSERT( myTree.node[0].next.entry == bitmapEntry.entry );

   }

   void testSetupBittestArray() {
      char              charVal[] = "abcde";
      short             i8Val = 21;
      uint16_t          i16Val = 1098, curPos = 0;
      uint32_t          i32Val = 0x0A6E6D8E, compareVal;
      uint64_t          i64Val = 0xDE1B4213;
      int               width, testbitVal, rc, i, shiftPos;

      // compare with old code to make sure we can still be consistent
      // test 64 bit
      width = 8;

      rc = m_index.setBitsetColumn( &i64Val, 1, width*8, WriteEngine::WR_LONGLONG );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      //cout << "bitArray =        " << m_index.m_multiColKey.bitSet.to_string() << endl;

      m_index.calculateBittestArray();
      //cout << "\nMax level = " << m_index.m_multiColKey.maxLevel << endl;
      for( i = 0; i < m_index.m_multiColKey.maxLevel; i++ ) {
         rc = m_index.getTestbitValue( i64Val, width*8, i, &testbitVal );
         CPPUNIT_ASSERT( rc == true );
         printf( "\tOld Level[%d] bit test value : %d \n", i, testbitVal );
         shiftPos = i == m_index.m_multiColKey.maxLevel - 1? 4 : 5;
         compareVal = compareVal << shiftPos | testbitVal;
         CPPUNIT_ASSERT( testbitVal == m_index.m_multiColKey.testbitArray[i] );
      }

      //cout << "\nNew bitset array value " << endl;
      for( i = 0; i < m_index.m_multiColKey.maxLevel; i++ ) 
         printf( "\tLevel[%d] bit test value : %d \n", i, m_index.m_multiColKey.testbitArray[i] );

      // test 32 bit
      width = 4;
      m_index.m_multiColKey.clear();
      rc = m_index.setBitsetColumn( &i32Val, 1, width*8, WriteEngine::WR_INT );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_index.calculateBittestArray();
      CPPUNIT_ASSERT( rc == NO_ERROR );

      for( i = 0; i < m_index.m_multiColKey.maxLevel; i++ ) {
         rc = m_index.getTestbitValue( i32Val, width*8, i, &testbitVal );
         CPPUNIT_ASSERT( rc == true );
         CPPUNIT_ASSERT( testbitVal == m_index.m_multiColKey.testbitArray[i] );
      }

      // test 16 bit
      width = 2;
      m_index.m_multiColKey.clear();
      rc = m_index.setBitsetColumn( &i16Val, 1, width*8, WriteEngine::WR_SHORT );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_index.calculateBittestArray();
      CPPUNIT_ASSERT( rc == NO_ERROR );

      for( i = 0; i < m_index.m_multiColKey.maxLevel; i++ ) {
         rc = m_index.getTestbitValue( i16Val, width*8, i, &testbitVal );
         CPPUNIT_ASSERT( rc == true );
         CPPUNIT_ASSERT( testbitVal == m_index.m_multiColKey.testbitArray[i] );
      }

      // test 8 bit
      width = 1;
      m_index.m_multiColKey.clear();
      rc = m_index.setBitsetColumn( &i8Val, 1, width*8, WriteEngine::WR_BYTE );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      rc = m_index.calculateBittestArray();
      CPPUNIT_ASSERT( rc == NO_ERROR );

      for( i = 0; i < m_index.m_multiColKey.maxLevel; i++ ) {
         rc = m_index.getTestbitValue( i8Val, width*8, i, &testbitVal );
         CPPUNIT_ASSERT( rc == true );
         CPPUNIT_ASSERT( testbitVal == m_index.m_multiColKey.testbitArray[i] );
      }

      // here is the real one for multi-column
      m_index.m_multiColKey.clear();
      width = 1;
      rc = m_index.setBitsetColumn( &i8Val, curPos++, width*8, WriteEngine::WR_BYTE );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      width = 5;
      rc = m_index.setBitsetColumn( charVal, curPos++, width*8, WriteEngine::WR_CHAR );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      width = 4;
      rc = m_index.setBitsetColumn( &i32Val, curPos++, width*8, WriteEngine::WR_INT );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      width = 9;
      strcpy( charVal, "test12345" );
      rc = m_index.setBitsetColumn( charVal, curPos++, width*8, WriteEngine::WR_CHAR );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.calculateBittestArray();
      CPPUNIT_ASSERT( rc == NO_ERROR );

      CPPUNIT_ASSERT( m_index.m_multiColKey.maxLevel == 31 );
      CPPUNIT_ASSERT( m_index.m_multiColKey.totalBit == 152 );

      //cout << "\nMulti-column bitset array value " << endl;
      for( i = 0; i < m_index.m_multiColKey.maxLevel; i++ ) 
         printf( "\tLevel[%d] bit test value : %d \n", i, m_index.m_multiColKey.testbitArray[i] );
   }

   void testCreateAndUpdateIndex() {
      int         rc;
      bool        bEmptyFlag;

      BRMWrapper::setUseBrm(false);
      m_index.dropIndex( 990, 991 );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // check invalid file not exist
      rc = m_index.openIndex( 990, 992 );
      CPPUNIT_ASSERT( rc != NO_ERROR );

      rc = m_index.openIndex( 992, 991 );
      CPPUNIT_ASSERT( rc != NO_ERROR );

      // here is the correct check
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      bEmptyFlag = m_index.isTreeEmpty();
      CPPUNIT_ASSERT( bEmptyFlag == true );

      rc = m_index.updateIndex( 123, 8, 1 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      bEmptyFlag = m_index.isTreeEmpty();
      CPPUNIT_ASSERT( bEmptyFlag == false );

      // test insert multiple rid
      RID   ridArray[6];

      ridArray[0] = 23;
      ridArray[1] = 26;
      ridArray[2] = 27;
      ridArray[3] = 31;
      ridArray[4] = 39;
      ridArray[5] = 40;

      m_index.setUseMultiRid( true );
      m_index.m_multiRid.setMultiRid( ridArray, 6 );

      rc = m_index.updateIndex( 123, 8, ridArray[0] );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.m_multiRid.clearMultiRid();
      m_index.closeIndex();
   }

   void testDeleteIndex() {
      int         rc;

      
      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // 8 bit
      rc = m_index.deleteIndex( 123, 8, 1 );
      CPPUNIT_ASSERT( rc == ERR_IDX_LIST_INVALID_DELETE );

      rc = m_index.updateIndex( 123, 8, 1 );
      printf("383. rc %i \n", rc);
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.deleteIndex( 123, 8, 1 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // 16 bit
      rc = m_index.deleteIndex( 0xA1C9, 16, 3 );
      CPPUNIT_ASSERT( rc != NO_ERROR );

      rc = m_index.updateIndex( 0xA1C9, 16, 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.deleteIndex( 0xA1C9, 16, 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // 32 bit
      rc = m_index.updateIndex( 0xCA6E6D8E, 32, 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.deleteIndex( 0xCA6E6D8E, 32, 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // repeat one more time, should fail
      rc = m_index.deleteIndex( 0xCA6E6D8E, 32, 3 );
      CPPUNIT_ASSERT( rc != NO_ERROR );

      m_index.closeIndex();
   }

   void testResetIndex() {
      int rc, width = 64, i;
      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );
      BRMWrapper::setUseBrm( true );
      BRMWrapper::setUseVb( false );

      m_index.setTransId( 12345678 );
      m_index.dropIndex( 990, 991 );
      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.closeIndex();

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      char   buf[20];
      uint64_t key;

      for( i = 0; i < 10000; i++ ) {
         sprintf( buf, "%dSuccess", i );
         memcpy( &key, buf, 8 );

         rc = m_index.updateIndex( key, width, i );
         if( rc != NO_ERROR )
             printf( "\n i =%d  rc=%d", i, rc );
         CPPUNIT_ASSERT( rc == NO_ERROR );
      }
      m_index.closeIndex();

      // reset index
//      rc = m_index.openIndex( 990, 991 );
//      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.resetIndexFile( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
//      m_index.closeIndex();

      printf( "\nre-insert begin\n" );
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      for( i = 0; i < 10000; i++ ) {
         sprintf( buf, "%dSuccess", i );
         memcpy( &key, buf, 8 );

         rc = m_index.updateIndex( key, width, i );
         if( rc != NO_ERROR )
             printf( "\n i =%d  rc=%d", i, rc );
         CPPUNIT_ASSERT( rc == NO_ERROR );
      }
      m_index.closeIndex();

      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testUpdateMutiColIndex() {
      uint16_t i16Val;
      uint32_t i32Val;
      uint64_t key;
      int rc, width = 64, i, pos = 0, totalWidth = 0;
      char charVal[50];

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );
      BRMWrapper::setUseBrm( true );
      BRMWrapper::setUseVb( false );

      m_index.setTransId( 12345678 );
      m_index.dropIndex( 990, 991 );
      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.closeIndex();

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      m_index.setUseMultiCol( true );

      printf( "\nBegin of multicol index insert\n" );
      for( i = 0; i < 10000; i++ ) {
         totalWidth = 0;
         // init part
         m_index.m_multiColKey.clear();

         // multi colum inserts
         // column 1
         width = 4;
         totalWidth += width;
         i32Val = i;
         rc = m_index.setBitsetColumn( &i32Val, pos++, width*8, WriteEngine::WR_INT );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         // column 2
         width = 4;
         totalWidth += width;
         i32Val = i*3;
         rc = m_index.setBitsetColumn( &i32Val, pos++, width*8, WriteEngine::WR_INT );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         // column 3
         width = 2;
         totalWidth += width;
         i16Val = i;
         rc = m_index.setBitsetColumn( &i16Val, pos++, width*8, WriteEngine::WR_SHORT );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         // column 4
         width = 15;
         totalWidth += width;
         sprintf( charVal, "%7d success", i );
         rc = m_index.setBitsetColumn( charVal, pos++, width*8, WriteEngine::WR_CHAR );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         // column 5
         width = 7;
         totalWidth += width;
         sprintf( charVal, "%7d", i );
         rc = m_index.setBitsetColumn( charVal, pos++, width*8, WriteEngine::WR_CHAR );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         // final part
         rc = m_index.calculateBittestArray();
         CPPUNIT_ASSERT( rc == NO_ERROR );
         memcpy( &key, m_index.m_multiColKey.keyBuf, 8 );
//         printf( "\ntotalWidth = %d", totalWidth );
         rc = m_index.updateIndex( key, totalWidth*8, i );
         if( rc != NO_ERROR )
             printf( "\n i =%d  rc=%d", i, rc );
         CPPUNIT_ASSERT( rc == NO_ERROR );
         if( i%500 == 0 )
            printf( "\n process i=%d", i );
      }
      m_index.closeIndex();

      printf( "\nEnd of multicol index insert\n" );
      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testIntegrationVolume() {
      int   rc, width = 8, count = 3, i, j;
      uint64_t   key = 123;
      RID   rid = 1;
      IdxTree   myTree;
BRMWrapper::setUseBrm(false);
      m_index.setUseFreeMgr( false );
      m_index.setUseListMgr( true );
      m_index.setDebugLevel( DEBUG_3 );

      for( i = 0; i < 3; i++ ) {
         switch( i ) {
            case 0:   key = 123; width = 8; rid = 1; break;
            case 1:   key = 0xA1C9; width = 16; rid = 100; break;
            case 2:   key = 0xCA6E6D8E; width = 32; rid = 1000; break;
         }

         rc = m_index.dropIndex( 990, 991 );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         rc = m_index.createIndex( 990, 991 );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         rc = m_index.openIndex( 990, 991 );
         CPPUNIT_ASSERT( rc == NO_ERROR );

         for( j = 0; j < count; j++ ) {
            printf( "\nj=%d", j );
            rc = m_index.updateIndex( key+j, width, rid + j );
            if( rc != NO_ERROR )
               printf( "\nrc=%d", rc );
            CPPUNIT_ASSERT( rc == NO_ERROR );
            myTree = m_index.getTree();

            m_index.setAssignFbo( 1 + j );
         }

         for( j = 0; j < count; j++ ) {
            rc = m_index.deleteIndex( key+j, width, rid + j );
            CPPUNIT_ASSERT( rc == NO_ERROR );
         }

         m_index.closeIndex();

      }


   }


   void testTreeGetTestbitValue() {
      uint64_t    key;
      int         width, bittestVal;
      bool        bStatus;
BRMWrapper::setUseBrm(false);
      // test 8 bit
      width = 8;
      key = 35;
      bStatus = m_index.getTestbitValue( key, width, 0, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 4 );
      bStatus = m_index.getTestbitValue( key, width, 1, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 3 );
      bStatus = m_index.getTestbitValue( key, width, 2, &bittestVal );
      CPPUNIT_ASSERT( bStatus == false );

      // test 16 bit
      width = 16;
      key = 0xA1C9;
      bStatus = m_index.getTestbitValue( key, width, 0, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 20 );
      bStatus = m_index.getTestbitValue( key, width, 1, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 7 );
      bStatus = m_index.getTestbitValue( key, width, 2, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 4 );
      bStatus = m_index.getTestbitValue( key, width, 3, &bittestVal );
      CPPUNIT_ASSERT( bittestVal == 1 );
      CPPUNIT_ASSERT( bStatus == true );
      bStatus = m_index.getTestbitValue( key, width, 4, &bittestVal );
      CPPUNIT_ASSERT( bStatus == false );

      // test 32 bit
      width = 32;
      key = 0xCA6E6D8E;
      bStatus = m_index.getTestbitValue( key, width, 0, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 25 );
      bStatus = m_index.getTestbitValue( key, width, 1, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 9 );
      bStatus = m_index.getTestbitValue( key, width, 2, &bittestVal );
      CPPUNIT_ASSERT( bStatus == true );
      CPPUNIT_ASSERT( bittestVal == 23 );
      bStatus = m_index.getTestbitValue( key, width, 3, &bittestVal );
      CPPUNIT_ASSERT( bittestVal == 6 );
      CPPUNIT_ASSERT( bStatus == true );
      bStatus = m_index.getTestbitValue( key, width, 4, &bittestVal );
      CPPUNIT_ASSERT( bittestVal == 27 );
      CPPUNIT_ASSERT( bStatus == true );
      bStatus = m_index.getTestbitValue( key, width, 5, &bittestVal );
      CPPUNIT_ASSERT( bittestVal == 3 );
      CPPUNIT_ASSERT( bStatus == true );
      bStatus = m_index.getTestbitValue( key, width, 6, &bittestVal );
      CPPUNIT_ASSERT( bittestVal == 2 );
      CPPUNIT_ASSERT( bStatus == true );
      bStatus = m_index.getTestbitValue( key, width, 7, &bittestVal );
      CPPUNIT_ASSERT( bStatus == false );

   }

   void testTreeGetTreeNodeInfo() {
      int               rc, fbo = 6, sbid = 1, entry = 2, testbitVal = 5;
      int               curSbid = 3, curEntry = 2, allocCount, realCount;
      IdxTreeGroupType  curGroup;
      bool              entryMap[ENTRY_PER_SUBBLOCK];
      DataBlock         block;
      IdxBitTestEntry   bittestEntry;

      // test ENTRY_1
      m_index.clearBlock( &block );
      curGroup = ENTRY_1;

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo, sbid, entry );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 3, curGroup, fbo + 6, sbid +3, entry + 1 );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 1, 8, &bittestEntry );

      rc = m_index.getTreeNodeInfo( &block, curSbid, curEntry, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( allocCount == 1 );
      CPPUNIT_ASSERT( realCount == 1 );
      CPPUNIT_ASSERT( entryMap[0] == true );

      // test ENTRY_2
      m_index.clearBlock( &block );
      curGroup = ENTRY_2;

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo, sbid, entry );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 3, curGroup, fbo + 6, sbid +3, entry + 1 );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 1, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo + 6, sbid +3, entry + 1 );
      bittestEntry.type = EMPTY_LIST;
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 2, 8, &bittestEntry );

      rc = m_index.getTreeNodeInfo( &block, curSbid, curEntry, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( allocCount == 2 );
      CPPUNIT_ASSERT( realCount == 2 );
      CPPUNIT_ASSERT( entryMap[0] == true );
      CPPUNIT_ASSERT( entryMap[1] == true );
      CPPUNIT_ASSERT( entryMap[2] == false );

      // test ENTRY_8
      m_index.clearBlock( &block );
      curGroup = ENTRY_8;

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo, sbid, entry );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo + 6, sbid +3, entry + 1 );
      bittestEntry.type = EMPTY_LIST;
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 3, 8, &bittestEntry );

      rc = m_index.getTreeNodeInfo( &block, curSbid, curEntry, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == ERR_IDX_TREE_INVALID_TYPE );

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo + 6, sbid +3, entry + 1 );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 3, 8, &bittestEntry );

      bittestEntry.type = EMPTY_ENTRY;
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 2, 2, &bittestEntry );

      rc = m_index.getTreeNodeInfo( &block, curSbid, curEntry, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( allocCount == 8 );
      CPPUNIT_ASSERT( realCount == 2 );
      CPPUNIT_ASSERT( entryMap[0] == true );
      CPPUNIT_ASSERT( entryMap[1] == false );
      CPPUNIT_ASSERT( entryMap[2] == false );
      CPPUNIT_ASSERT( entryMap[3] == true );
      CPPUNIT_ASSERT( entryMap[4] == false );
   }

   void testTreeGetTreeMatchEntry() {
      int               rc, fbo = 6, sbid = 1, entry = 2, testbitVal = 5;
      int               curSbid = 3, curEntry = 2, allocCount, realCount, matchEntry;
      IdxTreeGroupType  curGroup;
      bool              entryMap[ENTRY_PER_SUBBLOCK];
      DataBlock         block;
      IdxBitTestEntry   bittestEntry, checkEntry;
      bool              bFound;

      //ENTRY_8
      m_index.clearBlock( &block );
      curGroup = ENTRY_8;

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo, sbid, entry );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 5, curGroup, fbo + 9, sbid + 6, entry + 2 );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 4, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 9, curGroup, fbo + 6, sbid +3, entry + 1 );
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 3, 8, &bittestEntry );

      bittestEntry.type = EMPTY_ENTRY;
      m_index.setSubBlockEntry( block.data, curSbid, curEntry + 2, 2, &bittestEntry );

      rc = m_index.getTreeNodeInfo( &block, curSbid, curEntry, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( allocCount == 8 );
      CPPUNIT_ASSERT( realCount == 3 );
      CPPUNIT_ASSERT( entryMap[0] == true );
      CPPUNIT_ASSERT( entryMap[1] == false );
      CPPUNIT_ASSERT( entryMap[2] == false );
      CPPUNIT_ASSERT( entryMap[3] == true );
      CPPUNIT_ASSERT( entryMap[4] == true );
      CPPUNIT_ASSERT( entryMap[5] == false );

      memset( &checkEntry, 0, 8 );
      checkEntry.type = BIT_TEST;
      checkEntry.bitTest = testbitVal + 4;
      bFound = m_index.getTreeMatchEntry( &block, curSbid, curEntry, 8, allocCount, entryMap, &matchEntry, &checkEntry );
      CPPUNIT_ASSERT( bFound == false );

      checkEntry.bitTest = testbitVal + 9;
      bFound = m_index.getTreeMatchEntry( &block, curSbid, curEntry, 8, allocCount, entryMap, &matchEntry, &checkEntry );
      CPPUNIT_ASSERT( bFound == true );
      CPPUNIT_ASSERT( matchEntry == 3 );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 5, curGroup, fbo + 9, sbid + 6, entry + 2 );
      checkEntry.bitTest = testbitVal + 5;
      bFound = m_index.getTreeMatchEntry( &block, curSbid, curEntry, 8, allocCount, entryMap, &matchEntry, &checkEntry );
      CPPUNIT_ASSERT( bFound == true );
      CPPUNIT_ASSERT( matchEntry == 4 );
      CPPUNIT_ASSERT( !memcmp( &checkEntry, &bittestEntry, 8 ) );

   }


   void testTreeMoveEntry() {
      int               rc, fbo = 6, sbid = 1, entry = 2, testbitVal = 5;
      int               curSbid = 3, curEntry = 2, allocCount, realCount, moveCount;
      IdxTreeGroupType  curGroup;
      bool              entryMap[ENTRY_PER_SUBBLOCK];
      DataBlock         oldBlock, newBlock, testBlock, blankBlock;
      IdxBitTestEntry   bittestEntry;
      FILE*             treeFile;

      if( m_index.exists( 990 ) ) {
         rc = m_index.dropIndex( 990, 991 );
         CPPUNIT_ASSERT( rc == NO_ERROR );
      }
      BRMWrapper::setUseBrm(false);
      rc = m_index.createIndex( 990, 991, false );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      treeFile = m_index.openFile( 990 );
      CPPUNIT_ASSERT( treeFile != NULL );

      //ENTRY_8
      m_index.clearBlock( &oldBlock );
      m_index.clearBlock( &newBlock );
      m_index.clearBlock( &testBlock );
      m_index.clearBlock( &blankBlock );
      curGroup = ENTRY_8;

      m_index.setBittestEntry( &bittestEntry, testbitVal, curGroup, fbo, sbid, entry );
      m_index.setSubBlockEntry( oldBlock.data, curSbid, curEntry, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 5, curGroup, fbo + 9, sbid + 6, entry + 2 );
      m_index.setSubBlockEntry( oldBlock.data, curSbid, curEntry + 4, 8, &bittestEntry );

      m_index.setBittestEntry( &bittestEntry, testbitVal + 9, curGroup, fbo + 6, sbid +3, entry + 1 );
      m_index.setSubBlockEntry( oldBlock.data, curSbid, curEntry + 3, 8, &bittestEntry );

      bittestEntry.type = EMPTY_ENTRY;
      m_index.setSubBlockEntry( oldBlock.data, curSbid, curEntry + 2, 2, &bittestEntry );

      rc = m_index.getTreeNodeInfo( &oldBlock, curSbid, curEntry, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( allocCount == 8 );
      CPPUNIT_ASSERT( realCount == 3 );
      CPPUNIT_ASSERT( entryMap[0] == true );
      CPPUNIT_ASSERT( entryMap[1] == false );
      CPPUNIT_ASSERT( entryMap[2] == false );
      CPPUNIT_ASSERT( entryMap[3] == true );
      CPPUNIT_ASSERT( entryMap[4] == true );
      CPPUNIT_ASSERT( entryMap[5] == false );

      rc = m_index.writeDBFile( treeFile, &oldBlock, 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      m_index.closeFile( treeFile );


      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.moveEntry( 3, curSbid, curEntry, 8,
                              5, curSbid + 10, curEntry + 1, curGroup, allocCount, entryMap, &moveCount );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( moveCount == 3 );

      m_index.closeIndex();

      treeFile = m_index.openFile( 990 );
      CPPUNIT_ASSERT( treeFile != NULL );
      rc = m_index.readDBFile( treeFile, &testBlock, 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( !memcmp( testBlock.data, blankBlock.data, sizeof( testBlock.data )));

      rc = m_index.readDBFile( treeFile, &testBlock, 5 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.getTreeNodeInfo( &testBlock, curSbid + 10, curEntry + 1, 8, curGroup, &allocCount, &realCount, entryMap );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( allocCount == 8 );
      CPPUNIT_ASSERT( realCount == 3 );
      CPPUNIT_ASSERT( entryMap[0] == true );
      CPPUNIT_ASSERT( entryMap[1] == true );
      CPPUNIT_ASSERT( entryMap[2] == true );
      CPPUNIT_ASSERT( entryMap[3] == false );
      CPPUNIT_ASSERT( entryMap[4] == false );
      CPPUNIT_ASSERT( entryMap[5] == false );

      m_index.closeFile( treeFile );
   }

   void testTreeNodeAssert( IdxTree myTree, int curLevel, uint64_t curBitTest, uint16_t curGroup,
                            uint64_t curFbo, uint64_t curSbid, uint64_t curEntry, uint64_t nextFbo, uint64_t nextSbid, uint64_t nextEntry )
   {
      CPPUNIT_ASSERT( myTree.node[curLevel].current.bitTest == curBitTest );
      CPPUNIT_ASSERT( myTree.node[curLevel].current.group == curGroup );
      CPPUNIT_ASSERT( myTree.node[curLevel].current.fbo == curFbo );
      CPPUNIT_ASSERT( myTree.node[curLevel].current.sbid == curSbid );
      CPPUNIT_ASSERT( myTree.node[curLevel].current.entry == curEntry );

      if( curLevel == 0 )
         CPPUNIT_ASSERT( myTree.node[curLevel].current.type == BITMAP_PTR );
      else
      if( curLevel == myTree.maxLevel - 1 )
         CPPUNIT_ASSERT( myTree.node[curLevel].current.type == LEAF_LIST );
      else
         CPPUNIT_ASSERT( myTree.node[curLevel].current.type == BIT_TEST );

      CPPUNIT_ASSERT( myTree.node[curLevel].next.fbo == nextFbo );
      CPPUNIT_ASSERT( myTree.node[curLevel].next.sbid == nextSbid );
      CPPUNIT_ASSERT( myTree.node[curLevel].next.entry == nextEntry );

      CPPUNIT_ASSERT( myTree.node[curLevel].allocCount == (0x1 << curGroup) );

      CPPUNIT_ASSERT( myTree.node[curLevel].used == true );
   }

   void testTreeBuildEmptyTree() {
      int  rc, width = 8, rid = 3;
      uint64_t key = 123;
      IdxTree myTree;
BRMWrapper::setUseBrm(false);
      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.buildEmptyTreePart( key, width, rid, 0 );
      CPPUNIT_ASSERT( rc == ERR_IDX_TREE_INVALID_LEVEL );


      rc = m_index.buildEmptyTreePart( key, 4, rid, 0 );
      CPPUNIT_ASSERT( rc == ERR_IDX_TREE_INVALID_LEVEL );

      rc = m_index.updateIndex( key, width, rid );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // width = 8
      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 2 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) rid );

      testTreeNodeAssert( myTree, 0, 15, ENTRY_32, 0, 1, 15,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 3, ENTRY_1, 1,3,4, /* */ 3, 3, 4 );
      CPPUNIT_ASSERT( myTree.node[2].used == false );

      // width = 16
      key = 0xA1C9;
      width = 16;
      rc = m_index.updateIndex( key, width, rid );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();

      CPPUNIT_ASSERT( myTree.maxLevel == 4 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) rid );

      testTreeNodeAssert( myTree, 0, 20, ENTRY_32, 0, 1, 20,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 7, ENTRY_1, 1,3,4, /* */ 1, 4, 4 );
      testTreeNodeAssert( myTree, 2, 4, ENTRY_1, 1,4,4, /* */ 1, 5, 4 );
      testTreeNodeAssert( myTree, 3, 1, ENTRY_1, 1,5,4,/* */ 3, 3, 6 );
      CPPUNIT_ASSERT( myTree.node[4].used == false );

      // width = 32
      key = 0xCA6E6D8E;
      width = 32;
      rc = m_index.updateIndex( key, width, rid );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();

      CPPUNIT_ASSERT( myTree.maxLevel == 7 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) rid );

      testTreeNodeAssert( myTree, 0, 25, ENTRY_32, 0, 1, 25,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 9, ENTRY_1, 1,3,4, /* */ 1, 4, 4 );
      testTreeNodeAssert( myTree, 2, 23, ENTRY_1, 1,4,4, /* */ 1, 5, 4 );
      testTreeNodeAssert( myTree, 3, 6, ENTRY_1, 1,5,4,/* */ 1, 6, 4 );
      testTreeNodeAssert( myTree, 4, 27, ENTRY_1, 1,6,4, /* */ 1, 7, 4 );
      testTreeNodeAssert( myTree, 5, 3, ENTRY_1, 1,7,4,/* */ 1, 8, 4 );
      testTreeNodeAssert( myTree, 6, 2, ENTRY_1, 1,8,4,/* */ 3, 3, 9 );

      CPPUNIT_ASSERT( myTree.node[7].used == false );


      m_index.closeIndex();
   }



   void testTreeBuildExistTree1() {
      int  rc, width = 8, rid = 3;
      uint64_t key = 123;
      IdxTree myTree;
BRMWrapper::setUseBrm(false);
      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.updateIndex( key, width, rid );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // width = 8
      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 2 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) rid );

      testTreeNodeAssert( myTree, 0, 15, ENTRY_32, 0, 1, 15,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 3, ENTRY_1, 1,3,4,/* */ 3, 3, 4 );
      CPPUNIT_ASSERT( myTree.node[2].used == false );

      // insert again
      m_index.setAssignFbo( 1 );
      rc = m_index.updateIndex( key, width, rid + 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 2 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 3) );

      testTreeNodeAssert( myTree, 0, 15, ENTRY_32, 0, 1, 15,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 3, ENTRY_1, 1,3,4,/* */ 3, 3, 4 );

      CPPUNIT_ASSERT( myTree.node[2].used == false );

      // insert with a little different key
      m_index.setAssignFbo( 2 );
      rc = m_index.updateIndex( key + 1, width, rid + 4 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 2 );
      CPPUNIT_ASSERT( myTree.key == key + 1 );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 4) );

      testTreeNodeAssert( myTree, 0, 15, ENTRY_32, 0, 1, 15,/* */ 3, 4, 4 );
      testTreeNodeAssert( myTree, 1, 4, ENTRY_2, 3, 4, 4,/* */ 3, 3, 4 );
      CPPUNIT_ASSERT( myTree.node[2].used == false );

      // insert with a different key and different rid
      m_index.setAssignFbo( 3 );
      rc = m_index.updateIndex( key + 2, width, rid + 5 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 2 );
      CPPUNIT_ASSERT( myTree.key == key + 2 );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 5) );

      testTreeNodeAssert( myTree, 0, 15, ENTRY_32, 0, 1, 15,/* */ 4, 4, 4 );
      testTreeNodeAssert( myTree, 1, 5, ENTRY_4, 4, 4, 4,/* */ 3, 3, 4 );

      CPPUNIT_ASSERT( myTree.node[1].useCount == 3 );
      CPPUNIT_ASSERT( myTree.node[2].used == false );


      // insert with a different key and different rid
      m_index.setAssignFbo( 4 );
      rc = m_index.updateIndex( key + 3, width, rid + 7 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 2 );
      CPPUNIT_ASSERT( myTree.key == key + 3 );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 7) );

      testTreeNodeAssert( myTree, 0, 15, ENTRY_32, 0, 1, 15,/* */ 4, 4, 4 );
      testTreeNodeAssert( myTree, 1, 6, ENTRY_4, 4, 4, 4,/* */ 3, 3, 4 );

      CPPUNIT_ASSERT( myTree.node[2].used == false );

      m_index.closeIndex();

   }


   void testTreeBuildExistTree2() {
      int  rc, width = 16, rid = 3;
      uint64_t key = 0xA1C9;
      IdxTree myTree;
BRMWrapper::setUseBrm(false);
      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.updateIndex( key, width, rid );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      // width = 16
      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 4 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) rid );

      testTreeNodeAssert( myTree, 0, 20, ENTRY_32, 0, 1, 20,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 7, ENTRY_1, 1,3,4, /* */ 1, 4, 4 );
      testTreeNodeAssert( myTree, 2, 4, ENTRY_1, 1,4,4, /* */ 1, 5, 4 );
      testTreeNodeAssert( myTree, 3, 1, ENTRY_1, 1,5,4,/* */ 3, 3, 6 );

      CPPUNIT_ASSERT( myTree.node[4].used == false );

      // insert again
      m_index.setAssignFbo( 1 );
      rc = m_index.updateIndex( key, width, rid + 3 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 4 );
      CPPUNIT_ASSERT( myTree.key == key );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 3));
      testTreeNodeAssert( myTree, 0, 20, ENTRY_32, 0, 1, 20,/* */ 1, 3, 4 );
      testTreeNodeAssert( myTree, 1, 7, ENTRY_1, 1,3,4, /* */ 1, 4, 4 );
      testTreeNodeAssert( myTree, 2, 4, ENTRY_1, 1,4,4, /* */ 1, 5, 4 );
      testTreeNodeAssert( myTree, 3, 1, ENTRY_1, 1,5,4,/* */ 3, 3, 6 );

      CPPUNIT_ASSERT( myTree.node[4].used == false );

      // insert with a little different key
      m_index.setAssignFbo( 2 );
      rc = m_index.updateIndex( key + 1, width, rid + 4 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 4 );
      CPPUNIT_ASSERT( myTree.key == key + 1 );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 4));

      testTreeNodeAssert( myTree, 0, 20, ENTRY_32, 0, 1, 20,/* */ 1, 3, 4 );
  //    testTreeNodeAssert( myTree, 1, 7, ENTRY_1, 1,3,4, /* */ 3, 5, 5 );
      testTreeNodeAssert( myTree, 2, 5, ENTRY_2, 3,5,5, /* */ 3, 3, 4 );
      testTreeNodeAssert( myTree, 3, 0, ENTRY_1, 3,3,4,/* */ 3, 3, 6 );
      CPPUNIT_ASSERT( myTree.node[4].used == false );

      // insert with a different key and different rid
      m_index.setAssignFbo( 3 );
      rc = m_index.updateIndex( key + 2, width, rid + 5 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 4 );
      CPPUNIT_ASSERT( myTree.key == key + 2 );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 5));

      testTreeNodeAssert( myTree, 0, 20, ENTRY_32, 0, 1, 20,/* */ 1, 3, 4 );
//      testTreeNodeAssert( myTree, 1, 7, ENTRY_1, 1,3,4, /* */ 3, 5, 5 );
      testTreeNodeAssert( myTree, 2, 5, ENTRY_2, 3,5,5, /* */ 4, 6, 4 );
      testTreeNodeAssert( myTree, 3, 1, ENTRY_2, 4,6,4,/* */ 3, 3, 6 );
      CPPUNIT_ASSERT( myTree.node[4].used == false );

      // insert with a different key and different rid
      m_index.setAssignFbo( 4 );
      rc = m_index.updateIndex( key + 3, width, rid + 7 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      myTree = m_index.getTree();
      CPPUNIT_ASSERT( myTree.maxLevel == 4 );
      CPPUNIT_ASSERT( myTree.key == key + 3 );
      CPPUNIT_ASSERT( myTree.width == width );
      CPPUNIT_ASSERT( myTree.rid == (RID) (rid + 7));

      testTreeNodeAssert( myTree, 0, 20, ENTRY_32, 0, 1, 20,/* */ 1, 3, 4 );
//      testTreeNodeAssert( myTree, 1, 7, ENTRY_1, 1,3,4, /* */ 3, 5, 5 );
      testTreeNodeAssert( myTree, 2, 6, ENTRY_4, 5,5,6, /* */ 5, 3, 4 );
      testTreeNodeAssert( myTree, 3, 0, ENTRY_1, 5,3,4,/* */ 3, 3, 6 );
      CPPUNIT_ASSERT( myTree.node[4].used == false );

      m_index.closeIndex();

      // set a blank entry
      IdxBitTestEntry   curEntry;
      FILE*             treeFile;
      DataBlock         curBlock;

      treeFile = m_index.openFile( 990 );
      CPPUNIT_ASSERT( treeFile != NULL );

      rc = m_index.readSubBlockEntry( treeFile, &curBlock, 1, 3, 4, MAX_COLUMN_BOUNDARY, &curEntry );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.setBlankEntry( &curEntry );

      rc = m_index.writeSubBlockEntry( treeFile, &curBlock, 1, 3, 4, MAX_COLUMN_BOUNDARY, &curEntry );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.closeFile( treeFile );

      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.updateIndex( key, width, rid );
      CPPUNIT_ASSERT( rc == ERR_STRUCT_EMPTY );

      m_index.closeIndex();
   }

   void testTreeIntegration() {
      IdxEmptyListEntry myEntry;
      int rc;
BRMWrapper::setUseBrm(false);
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.assignSegment( ENTRY_1, &myEntry, 0 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.releaseSegment( ENTRY_1, &myEntry );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.setUseFreeMgr( true );
      rc = m_index.assignSegment( ENTRY_1, &myEntry, 0 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.releaseSegment( ENTRY_1, &myEntry );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.closeIndex();
   }

   void testFreeMgrBRM() {

       int rc;
       int allocSize;
       CommBlock cb;
       uint64_t lbid;
       uint64_t fbo;
       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("\nUT: Begin testFreeMgrBRM\n");

      
       m_freeMgr.deleteFile( 777 );
       m_freeMgr.createFile( 777, 80, allocSize );
       m_freeMgr.createFile( 778, 80, allocSize );

       cb.file.oid = 777 ;
       fbo =0;
       lbid = m_freeMgr.mapLBID( cb, fbo, rc );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       printf("OID:  %u FBO:  %llu LBID:  %llu\n", cb.file.oid, fbo, lbid);

       cb.file.oid = 777 ;
       fbo =10;
       lbid = m_freeMgr.mapLBID( cb, fbo, rc );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       printf("OID:  %u FBO:  %llu LBID:  %llu\n", cb.file.oid, fbo, lbid);

       cb.file.oid = 777 ;
       fbo =1023;
       lbid = m_freeMgr.mapLBID( cb, fbo, rc );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       printf("OID:  %u FBO:  %llu LBID:  %llu\n", cb.file.oid, fbo, lbid);

       cb.file.oid = 777 ;
       fbo =1020;
       lbid = m_freeMgr.mapLBID( cb, fbo, rc );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       printf("OID:  %u FBO:  %llu LBID:  %llu\n", cb.file.oid, fbo, lbid);

       cb.file.oid = 778 ;
       fbo = 0;
       lbid = m_freeMgr.mapLBID( cb, fbo, rc );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       printf("OID:  %u FBO:  %llu LBID:  %llu\n", cb.file.oid, fbo, lbid);

       cb.file.oid = 778 ;
       fbo = 1000;
       lbid = m_freeMgr.mapLBID( cb, fbo, rc );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       printf("OID:  %u FBO:  %llu LBID:  %llu\n", cb.file.oid, fbo, lbid);

       CPPUNIT_ASSERT( rc == NO_ERROR );
   }

   void testFreeMgrInit() {
       FILE*       indexFile;
       DataBlock   blockZero;
       IdxEmptyListEntry emptyMap, assignPtr;
       CommBlock cb;
       int allocSize, rc;
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_1 );
       printf("\nUT: Begin testFreeMgrInit\n");

      

       assignPtr.fbo = 0;
       assignPtr.sbid = 0;
       assignPtr.entry = 0;
       assignPtr.type= 0;


       cb.file.pFile = NULL;
       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == ERR_INVALID_PARAM );
       m_freeMgr.deleteFile( 777 );
       m_freeMgr.deleteFile( 778 );
       m_freeMgr.createFile( 778, 8, allocSize ); // create a second file to try and force lbid range to not collide with fbo
       m_freeMgr.createFile( 777, 8, allocSize );

       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;

//        m_indexlist.createFile(777,800, allocSize);
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, 1234 )  == ERR_INVALID_PARAM );

       // init a freemgr for a tree
       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb.file.pFile, blockZero.data, lbid );
       m_freeMgr.getSubBlockEntry( blockZero.data, 0, 1+ENTRY_32, 8, &emptyMap );
       // check the entries are as expected

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.entry == 31 ); // points to last entry in sub-block

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyMap);
       // check the entries are as expected

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_16 );
//        printf("UT: fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);
       CPPUNIT_ASSERT( emptyMap.fbo == 0 );
       CPPUNIT_ASSERT( emptyMap.entry == 0 );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_8, 8, &emptyMap);
       // check the entries are as expected

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_8 );
       CPPUNIT_ASSERT( emptyMap.fbo == 0 );
       CPPUNIT_ASSERT( emptyMap.entry == 0 );


       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_4, 8, &emptyMap);
       // check the entries are as expected

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_4 );
       CPPUNIT_ASSERT( emptyMap.fbo == 0 );
       CPPUNIT_ASSERT( emptyMap.entry == 0 );


       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_2, 8, &emptyMap);
       // check the entries are as expected

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_2 );
       CPPUNIT_ASSERT( emptyMap.fbo == 0 );
       CPPUNIT_ASSERT( emptyMap.entry == 0 );


       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_1, 8, &emptyMap);
       // check the entries are as expected

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_1 );
       CPPUNIT_ASSERT( emptyMap.fbo == 0 );
       CPPUNIT_ASSERT( emptyMap.entry == 0 );

       // assign a segment and check that entries in sb0 change

       printf("UT: Init: Assign ENTRY_32 fbo: %llu  sbid: %i entry: %i\n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry);
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );
       CPPUNIT_ASSERT( assignPtr.entry == 0 ); // points to start of sub-block
//        CPPUNIT_ASSERT( assignPtr.fbo == 1 ); //
       CPPUNIT_ASSERT( assignPtr.group == ENTRY_32 );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.entry == 30 ); // points to second to last entry in sub-block

       // release segment and see if head pointer moves
       rc = m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr );
       CPPUNIT_ASSERT( rc  == NO_ERROR );
       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 31 );
//        CPPUNIT_ASSERT( emptyMap.entry == 31 ); // points to last entry in sub-block


              // release to wrong list
       printf("UT: Put sub-block segment on list of 16 (expect error)\n");
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == ERR_FM_RELEASE_ERR );

       // tweak type then try to release
//        printf("UT: Release EMPTY_LIST entry\n");
//        assignPtr.type = EMPTY_LIST;
//        CPPUNIT_ASSERT( freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == ERR_FM_BAD_TYPE );

       printf("UT: assign ENTRY_16 segment\n");
             // assign ENTRY_16 segment
       CPPUNIT_ASSERT_MESSAGE( "Failed assigning ENTRY_16 segment", m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyMap);

       CPPUNIT_ASSERT_MESSAGE( "Empty map type should be EMPTY_LIST", emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_16 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 0 );
//        CPPUNIT_ASSERT( emptyMap.entry == 1 ); // points to last entry in sub-block


       // release ENTRY_16 segment
       printf("UT: Release ENTRY_16 segment\n");
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );

       // legal segment size but list is empty - causes list to be populated
       printf("UT: Assign from ENTRY_4 list - which is empty\n");
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_4,  &assignPtr ) == NO_ERROR );
       printf("UT: After Assign ENTRY_4 fbo: %llu  sbid: %i entry: %i\n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry);

       //Check ENTRY_4 in map
       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_4, 8, &emptyMap);
       printf("UT: fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       // ENTRY_4 segment size
       printf("UT: Assign from ENTRY_4 list - which is no longer empty\n");
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_4,  &assignPtr ) == NO_ERROR );
       printf("UT: Assign from ENTRY_4 fbo: %llu  sbid: %i entry: %i\n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry);


       // cleanup
       m_freeMgr.writeDBFile( indexFile, blockZero.data, lbid );
       m_freeMgr.closeFile( indexFile );

   }
       // list tests
   void testFreeMgrInit2() {
       FILE*       indexFile;
       CommBlock   cb;
       DataBlock   blockZero;
       IdxEmptyListEntry emptyMap, assignPtr;
       int allocSize, rc;
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("\nUT: Begin testFreeMgrInit2\n");

       

       assignPtr.fbo = 0;
       assignPtr.sbid = 0;
       assignPtr.entry = 0;
       assignPtr.type= 0;

       m_freeMgr.deleteFile( 776 );
       rc = m_freeMgr.createFile( 776, 80, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 776 );
       cb.file.oid = 776;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readSubBlockEntry( cb, &blockZero, lbid, 0, 1+ENTRY_BLK, 8, &emptyMap );

       // check the entries are as expected
       printf("UT: fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_BLK );
       CPPUNIT_ASSERT( emptyMap.fbo != 0 );
//        CPPUNIT_ASSERT( emptyMap.entry == 31 ); // points to last entry in sub-block

       // assign a segment and check that entries in sb0 change

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_BLK,  &assignPtr ) == NO_ERROR );

       CPPUNIT_ASSERT( assignPtr.group == ENTRY_BLK ); // right group
//        CPPUNIT_ASSERT( assignPtr.entry == 0 ); // right position
//        CPPUNIT_ASSERT( assignPtr.sbid == 31 ); // right position
//        CPPUNIT_ASSERT( assignPtr.fbo == 1 ); // right block

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_BLK, 8, &emptyMap);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_BLK );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 0 );
//        CPPUNIT_ASSERT( emptyMap.entry == 30 ); // points to second to last entry in sub-block

       // illegal segment size
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_16,  &assignPtr ) == ERR_INVALID_PARAM );

       // illegal segment size
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_8,  &assignPtr ) == ERR_INVALID_PARAM );

       // illegal segment size
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_2,  &assignPtr ) == ERR_INVALID_PARAM );

       // illegal segment size
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_1,  &assignPtr ) == ERR_INVALID_PARAM );

       // legal segment size but list is empty
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &assignPtr ) == NO_ERROR );

       CPPUNIT_ASSERT( assignPtr.group == ENTRY_4 ); // points to start of sub-block
       CPPUNIT_ASSERT( assignPtr.entry == 28 );
//        CPPUNIT_ASSERT( assignPtr.sbid == 29 );
//        CPPUNIT_ASSERT( assignPtr.fbo == 1 );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_4, 8, &emptyMap);
       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_4 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 30 );
      CPPUNIT_ASSERT( emptyMap.entry == 7 ); // eight segments in list
//        CPPUNIT_ASSERT( emptyMap.entry == 31 ); // 32 segments if using blocks for buckets

       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_4,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_4, 8, &emptyMap);
       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_4 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 30 );
       CPPUNIT_ASSERT( emptyMap.entry == 8 ); // eight segments in list
//        CPPUNIT_ASSERT( (int)emptyMap.entry == 32 ); // 32 segments if using blocks for buckets

       m_freeMgr.writeDBFile( cb, blockZero.data, lbid );
       m_freeMgr.closeFile( indexFile );

   }


   void testFreeMgrAssign() {
   /**
        *  Assign tests
    */
       FILE*       indexFile;
       CommBlock   cb;
       DataBlock   blockZero;
       IdxEmptyListEntry assignPtr, assignPtr2;
       IdxEmptyListEntry assignPtr3, assignPtr4;
       IdxEmptyListEntry emptyMap;
       int idx, result;
       int allocSize, rc;
       IdxEmptyListEntry entries[1500];

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("\nUT: Begin testFreeMgrAssign\n");

       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 8, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;

       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
/*       m_freeMgr.readDBFile( indexFile, blockZero.data, 0  );
       m_index.printMemSubBlock(  &blockZero , 0 );*/
       uint64_t lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( indexFile, blockZero.data, lbid );
//        m_index.printMemSubBlock(  &blockZero , 0 );
       printf("\nUT: Cont testFreeMgrAssign\n");

//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, 0, TREE, ENTRY_32,  &assignPtr )  == ERR_INVALID_PARAM );
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  0 )  == ERR_INVALID_PARAM );
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, 1234, ENTRY_32, &assignPtr )  == ERR_INVALID_PARAM );

       // Assign a segment
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );
       // Assign a second segment and make sure that we get different pointers
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr2 ) == NO_ERROR );
       printf("UT: sbid: %i  sbid2: %i\n", (unsigned int)assignPtr.sbid, (unsigned int)assignPtr2.sbid);
       CPPUNIT_ASSERT( assignPtr.sbid != assignPtr2.sbid); // assigned from sb list so cannot have same sbid
       CPPUNIT_ASSERT( assignPtr.type = EMPTY_PTR );
       CPPUNIT_ASSERT( assignPtr2.type = EMPTY_PTR );
       // release in 'wrong' order ..
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr2 ) == NO_ERROR );
       // .. then check that the list issues them in the return order
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr3 )  == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr4 ) == NO_ERROR );
//        CPPUNIT_ASSERT( assignPtr2.sbid == assignPtr3.sbid);
//        CPPUNIT_ASSERT( assignPtr.sbid == assignPtr4.sbid);

       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr3 ) == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr4 ) == NO_ERROR );

              //assign lots of sub-blocks
       printf("UT: Assign lots of sub-blocks\n");

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %03i fbo: %llu  sbid: %02i entry: %02i in 'Assign lots of sub-blocks, assigning'\n", idx, entries[idx].fbo, (unsigned int)(entries[idx].sbid), (unsigned int)(entries[idx].entry));

       }
       printf("UT: Releasing\n");

       for (idx --; idx>-1; idx--){
//            printf("UT: Visit: %i\n", idx);
           printf("UT: Visit: %03i fbo: %llu  sbid: %02i entry: %02i in 'Assign lots of sub-blocks, releasing'\n", idx, entries[idx].fbo, (unsigned int)(entries[idx].sbid), (unsigned int)(entries[idx].entry));

           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &entries[idx] )  == NO_ERROR );
       }

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %03i fbo: %llu  sbid: %02i entry: %02i in 'Assign lots of sub-blocks, assigning'\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
       }


       //assign lots of smaller segments
       printf("UT: Assign lots of smaller segments\n");

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_4,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_2,  &assignPtr )  == NO_ERROR );
       printf("UT: fbo: %llu sbid: %i entry: %i \n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );

       printf("UT: Assign lots of segments after init\n");


       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );

       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( indexFile, blockZero.data, lbid );

       // use up 32 entries
       // assign more than 32 entries, keeping track of them all, then release them causing the chain to extend over two blocks
       printf("UT: Assign more than 32 segments\n");

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_4,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i in 'Assign more than 32 segments'\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);

       }
       printf("UT: Releasing\n");

       for (idx --; idx>-1; idx--){
//            printf("UT: Visit: %i\n", idx);
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i in 'Assign more than 32 segments, releasing'\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_4,  &entries[idx] )  == NO_ERROR );
       }

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_4,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i in 'Assign more than 32 segments'\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
       }

       // commmenting this next bit breaks things
//        CPPUNIT_ASSERT( freeMgr.init( indexFile, TREE )  == NO_ERROR );
//        dbFileOp.readDBFile( indexFile, blockZero.data, 0 );

       printf("UT: Use almost all free space\n");

       for (idx=0; idx<215; idx++){
           result = m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr );
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i, result = %i\n", idx, assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry, result);
           CPPUNIT_ASSERT ( result == NO_ERROR );
       }
       // assign one more than is available, expect error
       CPPUNIT_ASSERT( (result = m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr2 ))  != ERR_FM_NO_SPACE );
       // release last assigned segment
       CPPUNIT_ASSERT( (result = m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )) == NO_ERROR );
       // assign segment, expect no error
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );
       // assign segment after using up the list
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr2 ) != ERR_FM_NO_SPACE );
       //release the segment, expect to become first bucket
       printf("UT: Putting sub-block back\n");
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);
       printf("UT: After putting one sb back fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 7 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 0 );
//        CPPUNIT_ASSERT( emptyMap.entry == 1 ); // points to second to last entry in sub-block

       printf("UT: Request space after using all space\n");
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  != ERR_FM_NO_SPACE );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,   &assignPtr )  != ERR_FM_NO_SPACE );
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_4,   &assignPtr )  == NO_ERROR + 1);
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_2,   &assignPtr )  != ERR_FM_NO_SPACE );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_1,   &assignPtr )  != ERR_FM_NO_SPACE );

       m_freeMgr.writeDBFile( indexFile, blockZero.data, lbid );
       m_freeMgr.closeFile( indexFile );
       printf("UT: End testFreeMgrAssign\n");

   }


   void testFreeMgrAssignList() {
   /**
        *  Assign tests
    */
       FILE*       indexFile;

       DataBlock   blockZero;
       IdxEmptyListEntry assignPtr, assignPtr2;
       IdxEmptyListEntry assignPtr3, assignPtr4;
       IdxEmptyListEntry emptyMap;
       int rc;
       int allocSize;
       CommBlock cb;
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrAssignList\n");


       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 8, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;

       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, 0, LIST, ENTRY_32,  &assignPtr )  == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  0 )  == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, 1234, ENTRY_32, &assignPtr )  == ERR_INVALID_PARAM );

       // Assign a segment
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr )  == NO_ERROR );
       // Assign a second segment and make sure that we get different pointers
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr2 ) == NO_ERROR );
       printf("UT: sbid: %i  sbid2: %i\n", (unsigned int)assignPtr.sbid, (unsigned int)assignPtr2.sbid);
       CPPUNIT_ASSERT( assignPtr.sbid != assignPtr2.sbid); // assigned from sb list so cannot have same sbid
       CPPUNIT_ASSERT( assignPtr.type = EMPTY_PTR );
       CPPUNIT_ASSERT( assignPtr2.type = EMPTY_PTR );
       // release in 'wrong' order ..
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr ) == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr2 ) == NO_ERROR );
       // .. then check that the list issues them in the return order
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr3 )  == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr4 ) == NO_ERROR );
//        CPPUNIT_ASSERT( assignPtr2.sbid == assignPtr3.sbid);
//        CPPUNIT_ASSERT( assignPtr.sbid == assignPtr4.sbid);

       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr3 ) == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr4 ) == NO_ERROR );

       //assign lots of sub-blocks
       printf("UT: Assign lots of sub-blocks\n");

       int idx;
       IdxEmptyListEntry entries[40];
       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &entries[idx] )  == NO_ERROR );
       }
       printf("UT: Releasing\n");

       for (idx --; idx>-1; idx--){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &entries[idx] )  == NO_ERROR );
       }

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &entries[idx] )  == NO_ERROR );
       }

       //assign lots of smaller segments
       printf("UT: Assign lots of smaller segments\n");

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &assignPtr )  == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &assignPtr )  == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &assignPtr )  == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &assignPtr )  == NO_ERROR );

       printf("UT: Assign lots of segments after init\n");
       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );
       m_freeMgr.readDBFile( indexFile, blockZero.data, lbid );

       // use up 32 entries
       // assign more than 32 entries, keeping track of them all, then release them causing the chain to extend over two blocks
       printf("UT: Assign more than 32 segments\n");

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &entries[idx] )  == NO_ERROR );
       }
       printf("UT: Releasing\n");

       for (idx --; idx>-1; idx--){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_4,  &entries[idx] )  == NO_ERROR );
       }

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &entries[idx] )  == NO_ERROR );
       }

      // printf("UT: sbid: %i entry: %i \n", (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr )  == NO_ERROR );
       printf("UT: sbid: %i entry: %i \n", (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry );

       // now use up almost all free space
       for (idx=0; idx<215; idx++){
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr )  == NO_ERROR );
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i\n", idx, assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry);
       }
       // assign one more than is available, expect error
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr )  != ERR_FM_NO_SPACE );

       //check empty map
       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);

       // check the entries are as expected
       printf("UT: Empty - fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 7 );
//        CPPUNIT_ASSERT( emptyMap.entry == 0 ); // points to last entry in sub-block

       // release last assigned segment
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_16,  &assignPtr ) == ERR_FM_RELEASE_ERR );
       assignPtr.group = ENTRY_16;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_16,  &assignPtr ) == ERR_INVALID_PARAM );
       assignPtr.group = ENTRY_32;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr ) == NO_ERROR );
       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);

       // check the entries are as expected
       printf("UT: Empty - fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 7 );
//        CPPUNIT_ASSERT( emptyMap.entry == 1 ); // points to last entry in sub-block

       // assign segment, expect no error
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr ) == NO_ERROR );
       // assign segment after using up the list
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr2 ) != ERR_FM_NO_SPACE);
       //release the segment, expect to become first bucket
       printf("UT: Putting sub-block back\n");
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_32,  &assignPtr ) == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);
       printf("UT: After putting one sb back fbo: %llu  sbid: %i entry: %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 7 );
//        CPPUNIT_ASSERT( emptyMap.sbid == 0 );
//        CPPUNIT_ASSERT( emptyMap.entry == 1 ); // points to second to last entry in sub-block

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_16,  &assignPtr )  == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_8,   &assignPtr )  == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_2,   &assignPtr )  == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_1,   &assignPtr )  == ERR_INVALID_PARAM );

       m_freeMgr.writeDBFile( cb, blockZero.data, lbid );
       m_freeMgr.closeFile( indexFile );
       printf("UT: End testFreeMgrAssignList\n");

   }

   void testFreeMgrAssignListCk() {
   /**
        *  Assign tests
    */
       FILE*       indexFile;
       CommBlock cb;
       int rc, allocSize;
       DataBlock   blockZero;
       int idx;
       IdxEmptyListEntry entries[100];
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrAssignListCk\n");


       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 8, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;

       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );


       printf("UT: Assign lots of segments\n");
       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       // use up 32 entries
       // assign more than 32 entries, keeping track of them all, then release them causing the chain to extend over two blocks
       printf("UT: Assign more than 32 segments\n");

       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
       }
       printf("UT: Releasing segments\n");

       for (idx --; idx>-1; idx--){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, ENTRY_4,  &entries[idx] )  == NO_ERROR );
       }

       printf("UT: Assign again\n");
       for (idx=0; idx<39; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, ENTRY_4,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo: %llu  sbid: %i entry: %i\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
       }


       printf("UT: End testFreeMgrAssignListCk\n");

   }

   void testFreeMgrAssignListLots() {
   /**
        *  Assign tests
    */
       FILE*       indexFile;
       DbFileOp    dbFileOp;
       DataBlock   blockZero;
       DataBlock   tempBlock;
       int idx, rc, allocSize;
       IdxEmptyListEntry entries[4000];
       IdxEmptyListEntry map;
       CommBlock cb;
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrAssignListLots\n");


       int freemgr_type = LIST;

       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 8, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, freemgr_type )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       printf("UT: Assign lots of segments\n");

       // use up 32 entries
       // assign more than 32 entries, keeping track of them all, then release them causing the chain to extend over two blocks
       printf("UT: Assign more than 32 segments\n");

       int iters = 172;
       IdxTreeGroupType entryType = ENTRY_32;

       for (idx=0; idx<iters; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, freemgr_type, entryType,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo %llu sbid %02i entry %02i (assigning)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
           rc = m_freeMgr.readDBFile( cb, tempBlock.data, entries[idx].fbo );
           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &map);
           printf("UT: Map is now: %i fbo %llu sbid %02i entry %02i (assigning)\n", idx, map.fbo, (unsigned int)map.sbid, (unsigned int)map.entry);

       }
       printf("UT: Releasing segments\n");

      for (idx=0; idx<iters; idx++){
//              for (idx=iters-1; idx>-1; idx--){
           printf("UT: Visit: %i fbo %llu sbid %02i entry %02i (releasing)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, freemgr_type, entryType,  &entries[idx] )  == NO_ERROR );
           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &map);
           printf("UT: Map is now: %i fbo %llu sbid %02i entry %02i (releasing)\n", idx, map.fbo, (unsigned int)map.sbid, (unsigned int)map.entry);

       }

       printf("UT: Assign again\n");
       for (idx=0; idx<iters; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, freemgr_type, entryType,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo %llu sbid %02i entry %02i (assigning again)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &map);
           printf("UT: Map is now: %i fbo %llu sbid %02i entry %02i (assigning again)\n", idx, map.fbo, (unsigned int)map.sbid, (unsigned int)map.entry);

       }
//        printf("UT: Releasing segments again\n");
//
//        for (idx=0; idx<iters; idx++){
// //              for (idx=iters-1; idx>-1; idx--){
//            printf("UT: Visit: %i fbo %llu sbid %02i entry %02i (releasing again)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
//            CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, freemgr_type, entryType,  &entries[idx] )  == NO_ERROR );
//            getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &map);
//            printf("UT: Map is now: %i fbo %llu sbid %02i entry %02i (releasing again )\n", idx, map.fbo, (unsigned int)map.sbid, (unsigned int)map.entry);
//
//        }



       printf("UT: End testFreeMgrAssignListLots\n");

   }



   void testFreeMgrAssignListBlocks() {
   /**
        *  Assign tests
    */
       FILE*       indexFile;
       DataBlock   blockZero;
       int rc, allocSize;
       CommBlock cb;
       int idx;
       IdxEmptyListEntry entries[8000];
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrAssignListBlocks\n");

       m_freeMgr.deleteFile( 776 );
       rc = m_freeMgr.createFile( 776, 80, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 776 );
       cb.file.oid = 776;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );

       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );
       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       printf("UT: Assign blocks from list\n");

       // use up 32 entries
       // assign more than 32 entries, keeping track of them all, then release them causing the chain to extend over two blocks

       int iters = 77;
       IdxTreeGroupType entryType = ENTRY_BLK;

       for (idx=0; idx<iters; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, entryType,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo %llu sbid %02i entry %02i (assigning)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
       }
       printf("UT: Releasing segments\n");

       for (idx=0; idx<iters; idx++){
//              for (idx=iters-1; idx>-1; idx--){
           printf("UT: Visit: %i fbo %llu  sbid %02i entry %02i (releasing)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, entryType,  &entries[idx] )  == NO_ERROR );
           entries[idx].fbo  = 0;
       }

       printf("UT: Assign again\n");
       for (idx=0; idx<iters; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, LIST, entryType,  &entries[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo: %llu  sbid: %02i entry: %02i (assigning again)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
       }

       for (idx=0; idx<iters; idx++){
//              for (idx=iters-1; idx>-1; idx--){
           printf("UT: Visit: %i fbo %llu sbid %02i entry %02i (releasing again)\n", idx, entries[idx].fbo, (unsigned int)entries[idx].sbid, (unsigned int)entries[idx].entry);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, LIST, entryType,  &entries[idx] )  == NO_ERROR );
           entries[idx].fbo  = 0;
       }


       printf("UT: End testFreeMgrAssignListBlocks\n");

   }



   void testFreeMgrAssignListCk2() {
   /**
        *  Assign tests
    */
       FILE*       indexFile;
       DataBlock   blockZero;
       int rc, allocSize;
       CommBlock cb;
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrAssignListCk2\n");

       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 8, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );
       m_freeMgr.readDBFile( cb, blockZero.data, lbid );


       int idx;
       IdxEmptyListEntry e1[3400];
       IdxEmptyListEntry e32[3400];

       printf("UT: Assign lots of segments\n");
       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );
       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       // use up 32 entries
       // assign more than 32 entries, keeping track of them all, then release them causing the chain to extend over two blocks
       printf("UT: Assign more than 32 segments\n");

       for (idx=0; idx<340; idx++){
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_1,  &e1[idx] )  == NO_ERROR );
           printf("UT: Visit: %i fbo %llu sbid %i entry %i\n", idx, e1[idx].fbo, (unsigned int)e1[idx].sbid, (unsigned int)e1[idx].entry);
           if(idx%10==0){
               CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &e32[idx] )  == NO_ERROR );
           }
           e1[idx].type= 7;
       }
       printf("UT: Releasing segments\n");

       for (idx = 0; idx<340; idx++){
//            printf("UT: Visit: %i\n", idx);
           rc = m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_1,  &e1[idx] );
           CPPUNIT_ASSERT( rc  == NO_ERROR );
           if(idx%10==0){
               rc = m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &e1[idx] );
           }
       }

       printf("UT: Assign again\n");
       for (idx=0; idx<340; idx++){
           rc = m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_1,  &e1[idx] );
           CPPUNIT_ASSERT( rc == NO_ERROR );
           printf("UT: Visit: %i fbo %llu sbid %i entry %i\n", idx, e1[idx].fbo, (unsigned int)e1[idx].sbid, (unsigned int)e1[idx].entry);
       }

       printf("UT: Releasing segments\n");

       for (idx = 0; idx<340; idx++){
//            printf("UT: Visit: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_1,  &e1[idx] )  == NO_ERROR );
       }
//
       printf("UT: End testFreeMgrAssignListCk2\n");

   }


   void testFreeMgrAssignExtend() {
       /**
        *  Test the Extend method
        **/
       FILE*       indexFile;
       int         allocSize;
       int rc;
       DataBlock   blockZero;
       CommBlock cb;
       uint64_t lbid;
       DbFileOp db;
       
       printf("UT: Begin testFreeMgrAssignExtend\n");
       m_indexlist.deleteFile( 777 );
       m_freeMgr.setDebugLevel( DEBUG_1 );

       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 800, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       CPPUNIT_ASSERT( m_freeMgr.extendFreespace( cb, &blockZero, LIST ) == NO_ERROR );
       
       int currSize = db.getFileSize( indexFile )/BYTE_PER_BLOCK ;

       lbid = m_freeMgr.mapLBID( cb, currSize-1, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

   }


   void testFreeMgrExtendLots() {
       /**
        *  Test the Extend method
        **/
       FILE*       indexFile;
       int         allocSize;
       int idx, rc;
       DataBlock   blockZero;
       CommBlock cb;
       uint64_t lbid;
       DbFileOp db;
       
       printf("UT: Begin testFreeMgrExtendLots\n");
       m_indexlist.deleteFile( 777 );
       m_freeMgr.setDebugLevel( DEBUG_1 );

       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 800, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

      // CPPUNIT_ASSERT( m_freeMgr.init( cb, LIST )  == NO_ERROR );
   //    lbid = m_freeMgr.mapLBID( cb, 0, rc);
     //  CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       for (idx=0; idx<5; idx++){
           printf("Loop count: %i\n", idx);
           CPPUNIT_ASSERT( m_freeMgr.extendFreespace( cb, &blockZero, LIST ) == NO_ERROR );
       }
       
       //int currSize = db.getFileSize( indexFile )/BYTE_PER_BLOCK ;

       //lbid = m_freeMgr.mapLBID( cb, currSize-1, rc);
   //    CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );
       printf("UT: End testFreeMgrExtendLots\n");

   }


   void testFreeMgrRelease() {
       FILE*       indexFile;
       DataBlock   blockZero;
       IdxEmptyListEntry assignPtr, emptyMap;
       CommBlock cb;
       int rc, allocSize;
       uint64_t lbid;

       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrRelease\n");


       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 80, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, 0, TREE, ENTRY_32,  &assignPtr ) == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  0 ) == ERR_INVALID_PARAM );
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);

       // check the entries are as expected
       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
//        CPPUNIT_ASSERT( emptyMap.entry == 31 ); // points to last entry in sub-block


       //CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );

       m_freeMgr.writeDBFile( cb, blockZero.data, 0 );
       m_freeMgr.closeFile( indexFile );

       printf("UT: End testFreeMgrRelease\n");
   }
   void testFreeMgrFragment() {

       FILE*       indexFile;
       CommBlock   cb;
       int         rc, idx, allocSize;
       DataBlock   blockZero;
       IdxEmptyListEntry assignPtr, emptyMap;

       // test if possible to split a list of segments and populate smaller lists
       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrFragment\n");

        

       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 80, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );

       m_freeMgr.readSubBlockEntry( cb, &blockZero, 0, 0, 1+ENTRY_1, 8, &emptyMap);

       CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );

       printf("UT: fbo %llu sbid %i entry %i\n", assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry);

       assignPtr.type = EMPTY_PTR;
       assignPtr.group = ENTRY_16;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyMap);
       printf("UT: fbo %llu sbid %i entry %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       assignPtr.type = EMPTY_PTR;
       assignPtr.group = ENTRY_8;
       assignPtr.entry = 16;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_8, 8, &emptyMap);
       printf("UT: fbo %llu  sbid %i entry %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       assignPtr.type = EMPTY_PTR;
       assignPtr.group = ENTRY_4;
       assignPtr.entry = 24;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_4,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_4, 8, &emptyMap);
       printf("UT: fbo %llu  sbid %i entry %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       assignPtr.type = EMPTY_PTR;
       assignPtr.group = ENTRY_2;
       assignPtr.entry = 28;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_2,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_2, 8, &emptyMap);
       printf("UT: fbo %llu  sbid %i entry %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       assignPtr.type = EMPTY_PTR;
       assignPtr.group = ENTRY_1;
       assignPtr.entry = 30;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_1,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_1, 8, &emptyMap);
       printf("UT: fbo %llu sbid %i entry %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       assignPtr.type = EMPTY_PTR;
       assignPtr.group = ENTRY_1;
       assignPtr.entry = 31;
       CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_1,  &assignPtr )  == NO_ERROR );

       m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_1, 8, &emptyMap);

       // check the entries are as expected
       printf("UT: fbo %llu  sbid %i entry %i\n", emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       CPPUNIT_ASSERT( emptyMap.type == EMPTY_LIST );
       CPPUNIT_ASSERT( emptyMap.group == ENTRY_1 );
//        CPPUNIT_ASSERT( emptyMap.fbo == 1 );
       CPPUNIT_ASSERT( emptyMap.entry == 2 ); // points to second entry in sub-block

       /**
        * Now do it many times
        **/

       for (idx = 0; idx < 185; idx++)
       {
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );
//            printf("\nVisit: %i\n", idx);
           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);
//            printf("UT: Chain: %i fbo: %llu  sbid: %i entry: %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

           assignPtr.type = EMPTY_PTR;
           assignPtr.group = ENTRY_16;
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );

           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyMap);
//            printf("UT: Chain: %i fbo %llu  sbid %i entry %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

           assignPtr.type = EMPTY_PTR;
           assignPtr.group = ENTRY_8;
           assignPtr.entry = 16;
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr )  == NO_ERROR );

           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_8, 8, &emptyMap);
//            printf("UT: Chain: %i ffbo %llu  sbid %i entry %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

           assignPtr.type = EMPTY_PTR;
           assignPtr.group = ENTRY_4;
           assignPtr.entry = 24;
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_4,  &assignPtr )  == NO_ERROR );

           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_4, 8, &emptyMap);
//            printf("UT: Chain: %i fbo %llu  sbid %i entry %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

           assignPtr.type = EMPTY_PTR;
           assignPtr.group = ENTRY_2;
           assignPtr.entry = 28;
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_2,  &assignPtr )  == NO_ERROR );

           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_2, 8, &emptyMap);
//            printf("UT: Chain: %i fbo %llu  sbid %i entry %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

           assignPtr.type = EMPTY_PTR;
           assignPtr.group = ENTRY_1;
           assignPtr.entry = 30;
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_1,  &assignPtr )  == NO_ERROR );

           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_1, 8, &emptyMap);
//            printf("UT: Chain: %i fbo: %llu  sbid: %i entry: %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

           assignPtr.type = EMPTY_PTR;
           assignPtr.group = ENTRY_1;
           assignPtr.entry = 31;
           CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_1,  &assignPtr )  == NO_ERROR );
       }
       //CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr ) == NO_ERROR );

       m_freeMgr.writeDBFile( indexFile, blockZero.data, 0 );
       m_freeMgr.closeFile( indexFile );

       printf("UT: End testFreeMgrFragment\n");
   }


   void testFreeMgrChain() {

       FILE*       indexFile;
       DataBlock   blockZero;
       IdxEmptyListEntry assignPtr, emptyMap;
       int         rc, allocSize;
       CommBlock   cb;
       uint64_t lbid;

       // test if possible to split a list of segments and populate smaller lists
       m_freeMgr.setDebugLevel( DEBUG_0 );
       printf("UT: Begin testFreeMgrChain\n");
        
       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 80, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );

       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

      /**
        * Now do it many times
       **/

       int idx;
       for (idx = 0; idx < 208; idx++)
       {
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );
//            printf("\nVisit: %i\n", idx);
           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyMap);
//            printf("UT: ENTRY_32: %i fbo: %llu  sbid: %i entry: %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);


           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyMap);
//            printf("UT: ENTRY_16: %i fbo: %llu  sbid: %i entry: %i\n", (unsigned int)assignPtr.group, emptyMap.fbo, (unsigned int)emptyMap.sbid, (unsigned int)emptyMap.entry);

       }


       m_freeMgr.deleteFile( 777 );
       rc = m_freeMgr.createFile( 777, 10, allocSize );
       CPPUNIT_ASSERT( rc == NO_ERROR );
       cb.file.pFile = m_freeMgr.openFile( 777 );
       cb.file.oid = 777;
       indexFile = cb.file.pFile;

       CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
       lbid = m_freeMgr.mapLBID( cb, 0, rc);
       CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );

       m_freeMgr.readDBFile( cb, blockZero.data, lbid );

       printf("UT: Assigning from ENTRY_16 chain\n");

       for (idx = 0; idx < 420; idx++)
       {
           CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr )  == NO_ERROR );
//            printf("Visit: %i\n", idx);
           m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyMap);
           printf("UT: ENTRY_16: fbo: %llu  sbid: %i entry: %i\n",  assignPtr.fbo, (unsigned int)assignPtr.sbid, (unsigned int)assignPtr.entry);
       }

       m_freeMgr.writeDBFile( cb, blockZero.data, 0 );
       m_freeMgr.closeFile( indexFile );

       printf("UT: End testFreeMgrChain\n");
   }


//    void testFreeMgrEvil() {
//
//        FILE*       indexFile;
//        int         rc, allocSize;
//        DataBlock   blockZero, workBlock;
//        IdxEmptyListEntry assignPtr, emptyPtr, emptyEntry;
//        CommBlock cb;
//        uint64_t lbid;
//
//        /**
//         * Forget Google.. be evil
//         **/
//
//        test if possible to split a list of segments and populate smaller lists
//        m_freeMgr.setDebugLevel( DEBUG_0 );
//        printf("UT: Begin testFreeMgrEvil\n");
//
//         
//        m_freeMgr.deleteFile( 777 );
//        rc = m_freeMgr.createFile( 777, 80, allocSize );
//        CPPUNIT_ASSERT( rc == NO_ERROR );
//        cb.file.pFile = m_freeMgr.openFile( 777 );
//        cb.file.oid = 777;
//        indexFile = cb.file.pFile;
//
//        CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
//        lbid = m_freeMgr.mapLBID( cb, 0, rc);
//        CPPUNIT_ASSERT_MESSAGE( "Error using mapLBID", rc == NO_ERROR );
//
//        m_freeMgr.readDBFile( cb, blockZero.data, lbid );
//
//        m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_32, 8, &emptyPtr);
//
//        check the entries are as expected
//        CPPUNIT_ASSERT( emptyPtr.type == EMPTY_LIST );
//        CPPUNIT_ASSERT( emptyPtr.group == ENTRY_32 );
//        CPPUNIT_ASSERT( emptyPtr.fbo == 1 );
//        CPPUNIT_ASSERT( emptyPtr.entry == 31 ); // points to last entry in sub-block
//
//        printf("UT: Mess up fbo in emptyMap for ENTRY_32\n");
//        emptyPtr.fbo =999999999;
//        m_freeMgr.writeSubBlockEntry( cb, &blockZero, 0, 0, 1+ENTRY_32, 8, &emptyPtr);
//
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == ERR_FM_BAD_FBO );
//        emptyPtr.fbo =7;
//        m_freeMgr.writeSubBlockEntry( cb, &blockZero, 0, 0, 1+ENTRY_32, 8, &emptyPtr);
//
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );
//
//        CPPUNIT_ASSERT( assignPtr.fbo == 7 );
//        assignPtr.fbo =1234;
//        first release with bad fbo then with good
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == ERR_FM_BAD_FBO );
//
//        assignPtr.fbo =7;
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_32,  &assignPtr )  == NO_ERROR );
//
//
//        printf("UT: Mess up fbo in emptyMap for ENTRY_16\n");
//        m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_16, 8, &emptyPtr);
//        CPPUNIT_ASSERT( emptyPtr.fbo == 0 ); // zero because nothing in list yet
//
//        emptyPtr.fbo =999999999;
//        m_freeMgr.writeSubBlockEntry( cb, &blockZero, 0, 0, 1+ENTRY_16, 8, &emptyPtr);
//
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == ERR_FM_BAD_FBO );
//        emptyPtr.fbo =0;
//        m_freeMgr.writeSubBlockEntry( cb, &blockZero, 0, 0, 1+ENTRY_16, 8, &emptyPtr);
//
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == NO_ERROR );
//
//        CPPUNIT_ASSERT( assignPtr.fbo == 7 );
//        assignPtr.fbo =1234;
//        first release with bad fbo then with good
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == ERR_FM_BAD_FBO );
//
//        assignPtr.fbo =7;
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == NO_ERROR );
//
//        Mess with type field
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == NO_ERROR );
//
//        m_freeMgr.getSubBlockEntry( blockZero.data, 0, 1+ENTRY_16, 8, &emptyPtr );
//        m_freeMgr.readDBFile( cb, &workBlock, emptyPtr.fbo );
//        m_freeMgr.getSubBlockEntry( &workBlock, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry );
//
//        CPPUNIT_ASSERT( emptyEntry.type == 2 ); //check is EMPTY_PTR
//        CPPUNIT_ASSERT( emptyEntry.fbo == 7 ); //check is last sb
//        emptyEntry.type = 7; // set to garbage
//        m_freeMgr.writeSubBlockEntry( cb, &workBlock, emptyPtr.fbo, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry);
//        check assign fails
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == ERR_FM_BAD_TYPE );
//
//        emptyEntry.type = 2;
//        emptyEntry.fbo = 0;
//        m_freeMgr.writeSubBlockEntry( cb, &workBlock, emptyPtr.fbo, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry);
//        check assign fails
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == ERR_FM_BAD_TYPE );
//
//
//        fix type field and re-try assign
//        emptyEntry.type = 2;
//        emptyEntry.fbo = 7;
//        m_freeMgr.writeSubBlockEntry( cb, &workBlock, emptyPtr.fbo, emptyPtr.sbid, emptyPtr.entry, 8, &emptyEntry);
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == NO_ERROR  );
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_16,  &assignPtr ) == NO_ERROR );
//
//
//        printf("UT: Zero fbo in emptyMap for ENTRY_8\n");
//        //start 8 list
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr ) == NO_ERROR );
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr ) == NO_ERROR );
//
//        m_freeMgr.getSubBlockEntry( &blockZero, 0, 1+ENTRY_8, 8, &emptyPtr);
//        CPPUNIT_ASSERT( emptyPtr.fbo == 7 );
//        CPPUNIT_ASSERT( emptyPtr.type == 2 );
//
//        emptyPtr.fbo = 0;
//        m_freeMgr.writeSubBlockEntry( indexFile, &blockZero, 0, 0, 1+ENTRY_8, 8, &emptyPtr);
//
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr ) == NO_ERROR + 1 );
//        //cleanup
//        emptyPtr.fbo = 7;
//        m_freeMgr.writeSubBlockEntry( indexFile, &blockZero, 0, 0, 1+ENTRY_8, 8, &emptyPtr);
//
//        check that assign and release works ok
//        CPPUNIT_ASSERT( m_freeMgr.assignSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr ) == NO_ERROR );
//        CPPUNIT_ASSERT( m_freeMgr.releaseSegment( cb, &blockZero, TREE, ENTRY_8,  &assignPtr ) == NO_ERROR );
//
//        m_freeMgr.writeDBFile( indexFile, blockZero.data, 0 );
//        CPPUNIT_ASSERT( m_freeMgr.init( cb, TREE )  == NO_ERROR );
//        m_freeMgr.closeFile( indexFile );
//
//        printf("UT: End testFreeMgrEvil\n");
//
//    }
void testIndexListMultiKey() {

       DataBlock               curBlock;
       FILE*                   pFile =NULL;
       int                     rc, allocSize;
       uint64_t                key=0x123;
       int                     rowId;
       int                     file_number=20000;
       int                     fbo , sbid , entry ;
       IdxEmptyListEntry       newIdxListHdrPtr;
       IdxRidListHdr           newIdxRidListHdr;
      
       printf("\nRunning multiple keys for adding header \n");
       BRMWrapper::setUseBrm(true);
      //Then increment the rowId as it goes

       rowId = 0x567 ;

      /*******************************************************
       *  TEST with FreeMgr for coverage
       *  Start here for add header and delete header entries
       *
      *******************************************************/
       m_indexlist.deleteFile(file_number);
       m_indexlist.createFile(file_number,10, allocSize);
       pFile= m_indexlist.openFile( file_number );
       CPPUNIT_ASSERT( pFile != NULL );
       CommBlock cb;
       File      file;
       file.oid = file_number;
       file.pFile = pFile;
       cb.file = file;
       m_indexlist.init(cb,LIST);

       int loop;

       for (loop = 0; loop < 10 ; loop++)
       {
           rc = m_indexlist.addIndexListHdr(cb, rowId, key, &newIdxListHdrPtr);
            if (rc!= NO_ERROR)
              printf("196. error code:%i \n", rc);
           //printf("The fbo is:%i sbid is %i entry is %i \n",(int)newIdxListHdrPtr.fbo, (int)newIdxListHdrPtr.sbid, (int)newIdxListHdrPtr.entry);
           CPPUNIT_ASSERT( rc == NO_ERROR );
           fbo  = newIdxListHdrPtr.fbo; //fbo=? from the free manager
           sbid = newIdxListHdrPtr.sbid;//sbid =? from the free manager
           entry = newIdxListHdrPtr.entry;//entry=? from the free manager
           memset( curBlock.data, 0, sizeof(curBlock.data));

           rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
           m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, LIST_HDR_SIZE, 
                                      &newIdxRidListHdr );
           //cout<< "rowId->" << rowId << " key->" << key << " size->" << 
                            //newIdxRidListHdr.idxRidListSize.size << endl;
           rowId++;
           key++;
       }
       //cout << " \n Successfully ran testIndexListMultiKey \n" << endl;
       return;
   }//testIndexListMultiKey

void testIndexListUpdate() {

      DataBlock               curBlock;
      FILE*                   pFile =NULL;
      int                     rc, fbo , sbid , entry ;
      uint64_t                key=123;
      int                     count =40000;
      int                     rowId;
      int                     i;
      int                     file_number=8001;

      IdxEmptyListEntry newIdxListHdrPtr;
      IdxRidListHdr     newIdxRidListHdr;
      //bool found = false;
      //int  p_sbid, p_entry, 
      //uint64_t  p_fbo;
      int allocSize;
      
        BRMWrapper::setUseBrm(true);
        printf("\nRunning testIndexListUpdate\n");

        memset( curBlock.data, 0, sizeof(curBlock.data));
       m_indexlist.deleteFile(file_number);
        m_indexlist.createFile(file_number,10, allocSize);
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        //Free Manager
       
       CommBlock cb;
       File      file;
       file.oid = file_number;
       file.pFile = pFile;
       cb.file = file;
       m_indexlist.init(cb,LIST);

      //Initialize first rowId to 0 
 
        rowId = 0 ;     
        rc = m_indexlist.addIndexListHdr(cb, rowId, key, &newIdxListHdrPtr);
        if (rc!= NO_ERROR)
            //printf("245. error code:%i \n", rc);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        
        fbo  = newIdxListHdrPtr.fbo; //fbo=? from the free manager
        sbid = newIdxListHdrPtr.sbid;//sbid =? from the free manager
        entry = newIdxListHdrPtr.entry;//entry=? from the free manager
        memset( curBlock.data, 0, sizeof(curBlock.data));

        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, LIST_HDR_SIZE, 
                                      &newIdxRidListHdr );
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == 1 );
        rowId++; 
        //cout << " Single Insert->start Timer :inserting  " << count << " rowId " <<  endl;
        m_indexlist.startfTimer();                                     
        for (i=0; i<count; i++)
        {
            ////cout << " count number-> " << i << endl;
            rc =m_indexlist.updateIndexList(cb, rowId, key,&newIdxListHdrPtr);
            if (rc != NO_ERROR)
            {
                //cout << "error code->" << rc << " i->" << i << endl;
                return;
            }
            rowId++;
        }
        m_indexlist.stopfTimer();
        //cout << " Single Insert ->End Timer :inserting " << count << " rowId " <<  endl;
        //cout << " Total used time (msec) " << m_indexlist.getTotalfRunTime() << endl;
        m_indexlist.closeFile(pFile);
        //
        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, LIST_HDR_SIZE, 
                                      &newIdxRidListHdr );
        //cout<< " key->" << key << " size->" << 
                            //newIdxRidListHdr.idxRidListSize.size << endl;
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (uint64_t)(count+1));
        RID ridArray[count+1];
        int size;
        m_indexlist.getRIDArrayFromListHdr(pFile, key,
                                           &newIdxListHdrPtr,
                                           ridArray, size);
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (uint64_t)size);
        //cout << "size=" << size << endl;
        //cout << " newIdxRidListHdr.idxRidListSize.size=" << newIdxRidListHdr.idxRidListSize.size << endl;
        //for (int i=0; i< size ; i++)
         ////cout<< " ridArray[i]->" <<  ridArray[i] << " i->" << i << endl;
        
        uint64_t firstLbid;
        rc = m_indexlist.findFirstBlk(pFile, key, &newIdxListHdrPtr, firstLbid);
        //cout << " Single RID insert->FirstLbid is ->" << firstLbid << endl;
        //cout << " This will print level and counts for Children " << endl;
        //if (m_indexlist.getUseNarray())
        //  m_indexlist.printBlocks(firstLbid);
        CPPUNIT_ASSERT(rc==NO_ERROR);
        m_indexlist.closeFile(pFile);
        printf("\nSuccessfully Running testIndexListUpdate\n");
 }//testIndexListUpdate

void testIndexListDelete() {
      DataBlock               curBlock;
      FILE*                   pFile =NULL;
      int                     rc, fbo , sbid , entry , width = 32, allocSize;
      uint64_t                key=123;
      int                     count =2890;
      int                     rowId;
      int                     i;
      int                     k=count+1;
      int                     delete_count;
      int                     file_number=8002;
      IdxEmptyListEntry       newIdxListHdrPtr;
      IdxRidListHdr           newIdxRidListHdr;
      bool                    found = false;
      int                     p_sbid, p_entry;
      uint64_t                p_fbo;

      //Initialize first rowId to 0
      //Then increment the rowId as it goes
       printf("\nRunning testIndexListDelete\n");
       rowId = 0 ;
       delete_count =0;
       BRMWrapper::setUseBrm(true);
      /*******************************************************
       *  TEST with FreeMgr for coverage
       *  Start here for add header and delete header entries
       *
       *******************************************************/
        memset( curBlock.data, 0, sizeof(curBlock.data));
        m_indexlist.deleteFile( file_number);
        m_indexlist.createFile(file_number,10, allocSize);
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        CommBlock cb;
        File      file;
        file.oid = file_number;
        file.pFile = pFile;
        cb.file = file;
        m_indexlist.init(cb,LIST);

        rc = m_indexlist.addIndexListHdr(cb, rowId, key, &newIdxListHdrPtr);
        if (rc!= NO_ERROR)
            //printf("388. error code:%i \n", rc);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        rowId++;
        rc=m_indexlist.deleteIndexList( pFile,   0, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        delete_count++;
        //delete the same item again, should not be there, return an error
        rc=m_indexlist.deleteIndexList( pFile,   0, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc != NO_ERROR );
        // delete an rowId which does not exist in the index list
        // return a error
        rc=m_indexlist.deleteIndexList( pFile,   1, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc != NO_ERROR );
        //
        rc =m_indexlist.updateIndexList(pFile, 5, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        rc =m_indexlist.updateIndexList(pFile, 6, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        rc=m_indexlist.deleteIndexList( pFile,   5, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        rc=m_indexlist.deleteIndexList( pFile,   6, key, &newIdxListHdrPtr);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        fbo  = newIdxListHdrPtr.fbo; //fbo=? from free manager
        sbid = newIdxListHdrPtr.sbid;//sbid =?
        entry = newIdxListHdrPtr.entry;//entry=?
        //printf("Index List Header: fbo: %i  sbid: %i entry: %i\n", (int)fbo, (int)sbid, (int)entry);
        m_indexlist.closeFile(pFile);
       // Reading the Header out
        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );

        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == 0 );
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.type == 0 );
        CPPUNIT_ASSERT( newIdxRidListHdr.key == key );
        CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.type == 7 );
        CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.rid == (RID)0 );
        CPPUNIT_ASSERT( newIdxRidListHdr.nextIdxRidListPtr.type == 7 );
        CPPUNIT_ASSERT( newIdxRidListHdr.nextIdxRidListPtr.llp == 0x0 );
        m_indexlist.closeFile(pFile);

        //Add Header again and also Update the index list
        rowId =0;
        delete_count =0;
        //
        memset( curBlock.data, 0, sizeof(curBlock.data));
        m_indexlist.deleteFile( file_number);
        m_indexlist.createFile(file_number,10, allocSize);
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        

        file.oid = file_number;
        file.pFile = pFile;
        cb.file = file;
        m_indexlist.init(cb,LIST);
        rc = m_indexlist.addIndexListHdr(pFile, rowId, key, &newIdxListHdrPtr);
        found = m_indexlist.findRowId(pFile, rowId,key,&newIdxListHdrPtr, p_fbo, p_sbid, p_entry);
        //printf("Find RowId: %i in : fbo: %i  sbid: %i entry: %i\n", (int) rowId, (int)p_fbo, (int)p_sbid, (int)p_entry);
        //cout << " RowId->" << rowId <<" Fbo->" << p_fbo << " Sbid->" << p_sbid << " Entry->" << p_entry << endl;
        rowId++;

        CPPUNIT_ASSERT( rc == NO_ERROR );
        fbo  = newIdxListHdrPtr.fbo; //fbo=? from the free manager
        sbid = newIdxListHdrPtr.sbid;//sbid =? from the free manager
        entry = newIdxListHdrPtr.entry;//entry=? from the free manager
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, width, &newIdxRidListHdr );

        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == 1 );
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.type == 0 );
        CPPUNIT_ASSERT( newIdxRidListHdr.key == key );
        CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.rid == (RID)rowId-1 );
        CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.type == 3 );
        CPPUNIT_ASSERT( newIdxRidListHdr.nextIdxRidListPtr.llp == 0x0 );
        CPPUNIT_ASSERT( newIdxRidListHdr.nextIdxRidListPtr.type == 7 );


        for (i=0; i<count; i++)
        {
           //printf("Index List Count: i: %i Row Id:  %i \n", i, rowId);
           rc =m_indexlist.updateIndexList(pFile, rowId, key,&newIdxListHdrPtr);
         
           rowId++;
        //   if (rc != NO_ERROR)
            // printf("ERROR ->2940 i %i \n rowID %i \n", i, rowId);
           //CPPUNIT_ASSERT( rc == NO_ERROR );
        }
        for (i=0; i<count; i++)
        {
           found = m_indexlist.findRowId(pFile, i,key,&newIdxListHdrPtr, p_fbo, p_sbid, p_entry);
           //printf("Find RowId: %i in : fbo: %i  sbid: %i entry: %i\n", (int) rowId, (int)p_fbo, (int)p_sbid, (int)p_entry);
           //if (found)
             //cout << " Found RowId->" << i <<" Fbo->" << p_fbo << " Sbid->" << p_sbid << " Entry->" << p_entry << endl;
           //else
             //cout << " Not Found RowId->" << i << endl;
           //Check Header again, we need to read the block again
           //since it is changed
         }
         rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
         m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, width, &newIdxRidListHdr );

         CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (uint64_t)k );
         CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.type == 0 );
         CPPUNIT_ASSERT( newIdxRidListHdr.key == key );
         CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.rid == (RID)0 );
         CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.type == 3 );
         CPPUNIT_ASSERT( newIdxRidListHdr.nextIdxRidListPtr.type == 4 );
         //printf("Index List Header: fbo: %i  sbid: %i entry: %i\n", (int)fbo, (int)sbid, (int)entry);
         m_indexlist.closeFile(pFile);

      //Delete test
      //Read now
      //Check Header

      delete_count =0;
      fbo  = newIdxListHdrPtr.fbo; //fbo=?
      sbid = newIdxListHdrPtr.sbid;//sbid =?
      entry = newIdxListHdrPtr.entry;//entry=?

      memset( &newIdxRidListHdr, 0, 32 );
      memset( curBlock.data, 0, sizeof( curBlock.data ) );

      //Delete action
      pFile = m_indexlist.openFile( file_number);
      CPPUNIT_ASSERT( pFile != NULL );

      rc = m_indexlist.deleteIndexList( pFile,   0, key, &newIdxListHdrPtr);//rowId 0 deleted
      if (rc == NO_ERROR)
       delete_count++;
      else
      {
        //cout << "cannot find rowId->" << 0 << endl;
      }
      rc = m_indexlist.deleteIndexList( pFile, 100, key, &newIdxListHdrPtr);//rowId 100 deleted
      if (rc == NO_ERROR)
       delete_count++;
      else
      {
        //cout << "cannot find rowId->" << 100 << endl;
      }
      rc = m_indexlist.deleteIndexList( pFile, 975, key, &newIdxListHdrPtr);//rowId 975 deleted
      if (rc == NO_ERROR)
       delete_count++;
      else
      {
        //cout << "cannot find rowId->" << 975 << endl;
      }
      rc = m_indexlist.deleteIndexList( pFile, 2000, key, &newIdxListHdrPtr);//rowId 2000 deleted
      if (rc == NO_ERROR)
       delete_count++;
      else
      {
        //cout << "cannot find rowId->" << 2000 << endl;
      }

      m_indexlist.closeFile(pFile);

      //Check Answers Read File at block fbo=0;
      memset( curBlock.data, 0, sizeof(curBlock.data));
      pFile = m_indexlist.openFile( file_number );
      CPPUNIT_ASSERT( pFile != NULL );
      rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo);
      width =32;
      m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, width, &newIdxRidListHdr );
      k=k-delete_count;
      CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (uint64_t)k );
      CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.type == 0 );
      CPPUNIT_ASSERT( newIdxRidListHdr.key == key );

      CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.type == 7 );
      CPPUNIT_ASSERT( newIdxRidListHdr.firstIdxRidListEntry.rid  == 0 );
      m_indexlist.closeFile(pFile);
      printf("\nSuccessfully Running testIndexListDelete\n");
}//testIndexListDelete


void testIndexListReleaseMgrBack() {


       FILE*                   pFile =NULL;
       int                     rc, allocSize;
       int                     file_number=8006;
       DataBlock               BlockZero;
       int                     i;
       IdxEmptyListEntry        newIdxListHdrPtr;
       IdxRidListHdr            idxRidListHdr;
       int                     rowId =0;
       int                     newCnt = 2000;
       CommBlock cb;
       File      file;
       file.oid = file_number;
       file.pFile = pFile;
       cb.file = file;
       BRMWrapper::setUseBrm(true);
       m_indexlist.init(cb,LIST);
       int key=123;
       printf("\nRunning testIndexListReleaseMgrBack\n");
       
        memset( BlockZero.data, 0, sizeof(BlockZero.data));
        m_indexlist.deleteFile( file_number);
        m_indexlist.createFile(file_number,10, allocSize);
        pFile= m_indexlist.openFile( file_number );
        file.oid = file_number;
        file.pFile = pFile;
        cb.file = file;
        CPPUNIT_ASSERT( pFile != NULL );
        m_indexlist.init(cb, LIST);
        rc = m_indexlist.addIndexListHdr(cb, rowId, key, &newIdxListHdrPtr);
        /*
        if (rc != NO_ERROR)
        {
            //cout << "rc -> " << rc << endl;
            //cout << "i->"<< i << endl;
            //cout << "rowId->" << rowId << endl;
            //cout << "key->" << key << endl;
        }
        */
        CPPUNIT_ASSERT( rc==NO_ERROR);

        rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );
        //if (rc!= NO_ERROR)
            //printf("724. error code:%i \n", rc);
        CPPUNIT_ASSERT( rc==NO_ERROR);
        rowId++;
        rc =m_indexlist.updateIndexList(pFile, rowId, key+1, &newIdxListHdrPtr);
        /*
        if (rc != NO_ERROR)
        {
            //cout << "rc -> " << rc << endl;
            //cout << "rowId->" << rowId << endl;
            //cout << "key+1->" << key+1 << endl;
        }
        */
         CPPUNIT_ASSERT( rc!=NO_ERROR);
         rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );
         /*
         if (rc != NO_ERROR)
         {
            //cout <<" getHdrInfo has error-> rc " << rc << endl;
         }
         */
         CPPUNIT_ASSERT( rc==NO_ERROR);         
         for (i=0; i<newCnt; i++)
         {
           rc =m_indexlist.updateIndexList(pFile, rowId, key,&newIdxListHdrPtr);
           /*
           if (rc != NO_ERROR)
           {
             //cout <<" updateIndexList has error->" << endl;
             //cout << "rc -> " << rc  << endl;
             //cout << "i->"<< i << endl;
             //cout << "rowId->" << rowId << endl;
             //cout << "key->" << key << endl;
           }
           */
           CPPUNIT_ASSERT( rc==NO_ERROR);
           rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );
           /*
           if (rc != NO_ERROR)
           {
             //cout <<" getHdrInfo has error->" << endl;
             //cout << "rc -> " << rc  << endl;
             //cout << "i->"<< i << endl;
             //cout << "rowId->" << rowId << endl;
             //cout << "key->" << key << endl;
           } 
          */
           CPPUNIT_ASSERT( rc==NO_ERROR);
           rowId++;
         }
         rowId--;
         rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );
         rc = m_indexlist.deleteIndexList( pFile, rowId,
                                           key+1, &newIdxListHdrPtr);
         CPPUNIT_ASSERT( rc!=NO_ERROR);
         rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );
         
        for (i=newCnt; i>=0; i--)
        {
            rc = m_indexlist.deleteIndexList( pFile, rowId,
                                              key, &newIdxListHdrPtr);
            rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );                                         
            CPPUNIT_ASSERT( rc==NO_ERROR);
            rowId--;
        }
        rowId++;
       
        for (i=0; i<newCnt; i++)
        {
          rc =m_indexlist.updateIndexList(pFile, rowId, key, &newIdxListHdrPtr);
          rc = m_indexlist.getHdrInfo(&newIdxListHdrPtr, &idxRidListHdr );
          /*
          if (rc!=NO_ERROR)
          {
              //cout << " tindex 3099-> i " << i << " rc " << rc << endl;
          }
          */
          CPPUNIT_ASSERT( rc==NO_ERROR);
          rowId++;
          
        }
        m_indexlist.closeFile(pFile);
        printf("\nSuccessfully Running testIndexListReleaseMgrBack\n");
        return;
 }
 void testIndexListMultipleAddHdr() {

      DataBlock               curBlock;
      DataBlock               curBlock2;
      DataBlock               curBlock3;
      DataBlock               curBlock4;
      FILE*                   pFile =NULL;
      int                     rc, fbo , sbid , entry ;
      int                     fbo2, sbid2, entry2;
      int                     fbo3, sbid3, entry3;
      int                     fbo4, sbid4, entry4;
      uint64_t                key  = 1;
      uint64_t                key2 = 2;
      uint64_t                key3 = 3;
      uint64_t                key4 = 4;
      int                     count =2000;
      int                     i =0;
      int                     delete_count;
      int                     file_number=8010;
      RID                     rowIdArray[count+1];
      RID                     rowIdArray2[count+1];
      RID                     rowIdArray3[count+1];
      RID                     rowIdArray4[count+1];
      int j, k,l;
      RID                     rowId;

      IdxEmptyListEntry newIdxListHdrPtr;
      IdxRidListHdr     newIdxRidListHdr;
      IdxEmptyListEntry newIdxListHdrPtr2;
      IdxRidListHdr     newIdxRidListHdr2;
      IdxEmptyListEntry newIdxListHdrPtr3;
      IdxRidListHdr     newIdxRidListHdr3;
      IdxEmptyListEntry newIdxListHdrPtr4;
      IdxRidListHdr     newIdxRidListHdr4;
      
      bool found = false;
      int  p_sbid, p_entry, allocSize;
      uint64_t  p_fbo;
      BRMWrapper::setUseBrm(true);
      m_indexlist.setUseSortFlag(false);
      printf("\nRunning testIndexListMultipleAddHdr\n");
       delete_count =0;
       memset( curBlock.data, 0, sizeof(curBlock.data));
       m_indexlist.deleteFile( file_number);
       m_indexlist.createFile(file_number,10, allocSize);
       pFile= m_indexlist.openFile( file_number );
       CPPUNIT_ASSERT( pFile != NULL );
       CommBlock cb;
       File      file;
       file.oid = file_number;
       file.pFile = pFile;
       cb.file = file;
       m_indexlist.init(cb,LIST);

       for (i=0; i<count+1; i++)
       {
         rowIdArray[i]=i;
       }

       for (j=0; j<count+1; j++)
       {
         rowIdArray2[j]= i;
         i++;
       }
       for (k=0; k<count+1; k++)
       {
         rowIdArray3[k]=i;
         i++;
       }
       for (l=0; l<count+1; l++)
       {
         rowIdArray4[l]= i;
         i++;
       }
       i = count+1;
       //Initialize first rowId to 0 
        //cout << " start Timer :inserting  " << count << " rowId " <<  endl;
        m_indexlist.startfTimer();  
        int count_size = count+1;   
        rc = m_indexlist.addIndexListHdr(cb, &rowIdArray[0], count_size, key, &newIdxListHdrPtr);
        
        rc = m_indexlist.addIndexListHdr(cb, &rowIdArray2[0],count_size, key2, &newIdxListHdrPtr2);
        rc = m_indexlist.addIndexListHdr(cb, &rowIdArray3[0],count_size, key3, &newIdxListHdrPtr3);
        rc = m_indexlist.addIndexListHdr(cb, &rowIdArray4[0],count_size, key4, &newIdxListHdrPtr4);
        
        m_indexlist.stopfTimer();
        //cout << " End Timer :inserting  " << count << " rowId " <<  endl;
        //cout << " Total used time (msec) " << m_indexlist.getTotalfRunTime() << endl;
        //if (rc!= NO_ERROR)
         //   printf("245. error code:%i \n", rc);
        CPPUNIT_ASSERT( rc == NO_ERROR );
        
        fbo  = newIdxListHdrPtr.fbo; //fbo=? from the free manager
        sbid = newIdxListHdrPtr.sbid;//sbid =? from the free manager
        entry = newIdxListHdrPtr.entry;//entry=? from the free manager
        
        fbo2   = newIdxListHdrPtr2.fbo; //fbo=? from the free manager
        sbid2  = newIdxListHdrPtr2.sbid;//sbid =? from the free manager
        entry2 = newIdxListHdrPtr2.entry;//entry=? from the free manager
        

        fbo3  = newIdxListHdrPtr3.fbo; //fbo=? from the free manager
        sbid3 = newIdxListHdrPtr3.sbid;//sbid =? from the free manager
        entry3 = newIdxListHdrPtr3.entry;//entry=? from the free manager
 
        fbo4  = newIdxListHdrPtr4.fbo; //fbo=? from the free manager
        sbid4 = newIdxListHdrPtr4.sbid;//sbid =? from the free manager
        entry4 = newIdxListHdrPtr4.entry;//entry=? from the free manager
                      
                
                
        memset( curBlock.data, 0, sizeof(curBlock.data));
        
        memset( curBlock2.data, 0, sizeof(curBlock2.data));
        memset( curBlock3.data, 0, sizeof(curBlock.data));
        memset( curBlock4.data, 0, sizeof(curBlock2.data));
        

        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );
        RID ridArray[count+1];
        int ridSize;
        rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key,
                                               &newIdxListHdrPtr,
                                               ridArray, ridSize);
        //cout << "ridSize ->" << ridSize << endl;
        CPPUNIT_ASSERT( ridSize == (int)newIdxRidListHdr.idxRidListSize.size);
        
        rc = m_indexlist.readDBFile( pFile, curBlock2.data, fbo2 );
        m_indexlist.getSubBlockEntry( curBlock2.data, sbid2, entry2, 32, &newIdxRidListHdr2 );
        RID ridArray2[count+1];
        int ridSize2;
        rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key2,
                                               &newIdxListHdrPtr2,
                                               ridArray2, ridSize2);
        //cout << "ridSize2 ->" << ridSize2 << endl;
        CPPUNIT_ASSERT( ridSize2 == (int)newIdxRidListHdr2.idxRidListSize.size);
        
        rc = m_indexlist.readDBFile( pFile, curBlock3.data, fbo3 );
        m_indexlist.getSubBlockEntry( curBlock3.data, sbid3, entry3, 32, &newIdxRidListHdr3 );
        RID ridArray3[count+1];
        int ridSize3;
        rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key3,
                                               &newIdxListHdrPtr3,
                                               ridArray3, ridSize3);
        //cout << "ridSize3 ->" << ridSize3 << endl;
        CPPUNIT_ASSERT( ridSize3 == (int)newIdxRidListHdr3.idxRidListSize.size);
        
        rc = m_indexlist.readDBFile( pFile, curBlock4.data, fbo4 );
        m_indexlist.getSubBlockEntry( curBlock4.data, sbid4, entry4, 32, &newIdxRidListHdr4 );
        RID ridArray4[count+1];
        int ridSize4;
        rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key4,
                                               &newIdxListHdrPtr4,
                                               ridArray4, ridSize4);
        //cout << "ridSize4 ->" << ridSize4 << endl;
        CPPUNIT_ASSERT( ridSize4 == (int)newIdxRidListHdr4.idxRidListSize.size);
        //m_indexlist.stopTimer();

        ////cout << " Total used time (sec) " << m_indexlist.getTotalRunTime() << endl;
        
        uint64_t firstLbid;
        uint64_t firstLbid2;
        uint64_t firstLbid3;
        uint64_t firstLbid4;
        rc = m_indexlist.findFirstBlk(pFile, key,&newIdxListHdrPtr, firstLbid);
        
        rc = m_indexlist.findFirstBlk(pFile, key2,&newIdxListHdrPtr2, firstLbid2);
        rc = m_indexlist.findFirstBlk(pFile, key3,&newIdxListHdrPtr3, firstLbid3);
        rc = m_indexlist.findFirstBlk(pFile, key4,&newIdxListHdrPtr4, firstLbid4);
        
        //cout << " Multiple RIDS Header insert->FirstLbid is ->" << firstLbid << endl;
        
        //cout << " Multiple RIDS Header insert->FirstLbid2 is ->" << firstLbid2 << endl;
        //cout << " Multiple RIDS Header insert->FirstLbid3 is ->" << firstLbid3 << endl;
        //cout << " Multiple RIDS Header insert->FirstLbid4 is ->" << firstLbid4 << endl;
        
        //cout << " This will print level and counts for Children " << endl;
        /*
        if (m_indexlist.getUseNarray())
        {
          m_indexlist.printBlocks(firstLbid);
          
          m_indexlist.printBlocks(firstLbid2);
          m_indexlist.printBlocks(firstLbid3);
          m_indexlist.printBlocks(firstLbid4);
          
        }
        */
        CPPUNIT_ASSERT(rc==NO_ERROR);
        m_indexlist.closeFile(pFile);
        //
        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        /*
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (uint64_t)(count+1));
        int arraySize=0;
        arraySize = newIdxRidListHdr.idxRidListSize.size;
        if (arraySize >0)
        {
         RID ridArray[arraySize];
         int ridSize;
         rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key,
                                               &newIdxListHdrPtr,
                                               ridArray, ridSize);
         //cout << "ridSize->" << ridSize << endl;
         CPPUNIT_ASSERT(ridSize==arraySize);

         if (rc!= NO_ERROR)
         {
           printf("ERROR CODE :%i \n", rc);

         }
        }
 */          
        //uint64_t p2_fbo ;
        //int p2_sbid, p2_entry;
        rowId =0;
        int rowId2=count+1;
        int rowId3=2*(count+1);
        int rowId4=3*(count+1);
        
        for (i=0; i<count; i++)
        {
            found = m_indexlist.findRowId(pFile, rowId, key,&newIdxListHdrPtr, p_fbo, p_sbid, p_entry);
           /* 
            if (!found)
            {
              printf(" \n Not Found tindex 3659->i %i rowId %i  Not Found \n", i, (int)rowId);
            }
            else
            {
             //cout << " findRowId-> count in findRowId line 3663-> " << i <<" rowId " << rowId << endl;
             //cout << " p_fbo " << p_fbo << " p_sbid " << p_sbid << " p_entry " << p_entry << endl;
            }
           */
            CPPUNIT_ASSERT( found == true );
            /*rc = m_indexlist.deleteIndexList( pFile, rowId,
                                              key, &newIdxListHdrPtr,
                                              p2_fbo, p2_sbid, p2_entry);
            */
            
           // //cout << " deleteIndexList-> p2_fbo " << p2_fbo << " p2_sbid " << p2_sbid << " p2_entry " << p2_entry << endl;
            //CPPUNIT_ASSERT( rc == NO_ERROR );
            
            found = m_indexlist.findRowId(pFile, rowId2, key2,&newIdxListHdrPtr2, p_fbo, p_sbid, p_entry);
           /* 
            if (!found)
            {
              printf(" \n Not Found tindex 3678->i %i rowId2 %i  Not Found \n", i, (int)rowId2);
            }
            else
            {
             //cout << " findRowId-> count in findRowId line 3682-> " << i <<" rowId2 " << rowId2 << endl;
             //cout << " p_fbo " << p_fbo << " p_sbid " << p_sbid << " p_entry " << p_entry << endl;
            }
           */
            CPPUNIT_ASSERT( found == true );
            
            found = m_indexlist.findRowId(pFile, rowId3, key3,&newIdxListHdrPtr3, p_fbo, p_sbid, p_entry);
           /* 
            if (!found)
            {
              printf(" \n Not Found tindex 3691->i %i rowId3 %i  Not Found \n", i, (int)rowId3);
            }
            else
            {
             //cout << " findRowId-> count in findRowId line 3695-> " << i <<" rowId3 " << rowId3 << endl;
             //cout << " p_fbo " << p_fbo << " p_sbid " << p_sbid << " p_entry " << p_entry << endl;
            }
           */
            CPPUNIT_ASSERT( found == true );
            
            found = m_indexlist.findRowId(pFile, rowId4, key4,&newIdxListHdrPtr4, p_fbo, p_sbid, p_entry);
           /* 
            if (!found)
            {
              printf(" \n Not Found tindex 3704->i %i rowId4 %i  Not Found \n", i, (int)rowId4);
            }
            else
            {
             //cout << " findRowId-> count in findRowId line 3708-> " << i <<" rowId4 " << rowId4 << endl;
             //cout << " p_fbo " << p_fbo << " p_sbid " << p_sbid << " p_entry " << p_entry << endl;
            }
           */
            CPPUNIT_ASSERT( found == true );
            
            rowId++;
            
            rowId2++;
            rowId3++;
            rowId4++;
            
        }
/*
         rc = m_indexlist.deleteIndexList( pFile, rowId,
                                           key, &newIdxListHdrPtr,
                                           p_fbo, p_sbid, p_entry);
         ////cout << " p_fbo " << p_fbo << " p_sbid " << p_sbid << " p_entry " << p_entry << endl;
         
         CPPUNIT_ASSERT( rc == NO_ERROR );         
         m_indexlist.closeFile(pFile);

        //
        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == 0 );
*/        
        m_indexlist.closeFile(pFile);
        printf("\nSuccessfully Running testIndexListMultipleAddHdr\n");
 }//testIndexListMultipleAddHdr
void testIndexListMultipleUpdate() {

      DataBlock               curBlock;
      FILE*                   pFile =NULL;
      int                     rc, fbo , sbid , entry ;
      uint64_t                key=123;
      int                     count =10000;
      int                     rowId;
      int                     i;
      int                     delete_count;
      int                     file_number=8012;
      RID                     rowIdArray[count+1];

      IdxEmptyListEntry newIdxListHdrPtr;
      IdxRidListHdr     newIdxRidListHdr;
      bool found = false;
      int  p_sbid, p_entry, allocSize;
      uint64_t  p_fbo;
BRMWrapper::setUseBrm(true);
m_indexlist.setUseSortFlag(false);
       printf("\nRunning testIndexListMultipleUpdate\n");
        rowId = 0 ;
        delete_count =0;
        memset( curBlock.data, 0, sizeof(curBlock.data));
        m_indexlist.deleteFile( file_number);
        m_indexlist.createFile(file_number,10, allocSize);
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        //Free Manager
       
       CommBlock cb;
       File      file;
       file.oid = file_number;
       file.pFile = pFile;
       cb.file = file;
       m_indexlist.init(cb,LIST);
       for (int i=0; i<count+1; i++)
       {
         rowIdArray[i]=i;
       }
       rowId =0;
      //Initialize first rowId to 0      
        rc = m_indexlist.addIndexListHdr(cb, rowId, key, &newIdxListHdrPtr);
        /*
        if (rc!= NO_ERROR)
            printf("245. error code:%i \n", rc);
        */
        CPPUNIT_ASSERT( rc == NO_ERROR );
        
        fbo  = newIdxListHdrPtr.fbo; //fbo=? from the free manager
        sbid = newIdxListHdrPtr.sbid;//sbid =? from the free manager
        entry = newIdxListHdrPtr.entry;//entry=? from the free manager
        memset( curBlock.data, 0, sizeof(curBlock.data));

        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );
        RID ridArray2[count+5];
        int ridSize2;
        rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key,
                                               &newIdxListHdrPtr,
                                               ridArray2, ridSize2);
         CPPUNIT_ASSERT( ridSize2 == 1 );
         
         //Then increment the rowId as it goes  
       string   timeStr;
       
       m_indexlist.startfTimer();
       //cout << " line 3571->startTimer for inserting "<< count <<" rowId " << endl;

       rc =m_indexlist.updateIndexList(cb, &rowIdArray[1], count, key,&newIdxListHdrPtr);
       /*
       if (rc != NO_ERROR)
       {
                printf("rc %i i %i \n", rc, i);
       }
       */

        m_indexlist.stopfTimer();
        //cout << " End Time :inserting "<< count << "rowId " <<  endl;
        //cout << " Total used time (msec) " << m_indexlist.getTotalfRunTime() << endl;
        
        m_indexlist.closeFile(pFile);
        //
        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );
        //cout << "newIdxRidListHdr.idxRidListSize.size->" << newIdxRidListHdr.idxRidListSize.size << endl;
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (uint64_t)(count+1));
        //CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == (int64_t)ridSize2);

       int arraySize=0;
       rowId =0;
       arraySize = newIdxRidListHdr.idxRidListSize.size;
       if (arraySize >0)
       {
         RID ridArray[arraySize];
         int ridSize;
         rc= m_indexlist.getRIDArrayFromListHdr(pFile,
                                               key,
                                               &newIdxListHdrPtr,
                                               ridArray, ridSize);
         CPPUNIT_ASSERT(ridSize==arraySize);
         /*
         if (rc!= NO_ERROR)
         {
           printf("ERROR CODE :%i \n", rc);

         }
         */
         //for (int i=0; i< ridSize ; i++)
         //  //cout<< " line 3591->ridArray[i]->" <<  ridArray[i] << " i->" << i << endl;
        }
        uint64_t firstLbid;
        rc = m_indexlist.findFirstBlk(pFile, key, &newIdxListHdrPtr, firstLbid);
        //cout << " Multiple RIDS  insert->FirstLbid is ->" << firstLbid << endl;
        //cout << " This will print level and counts for Children " << endl;
        /*
        if (m_indexlist.getUseNarray())
          m_indexlist.printBlocks(firstLbid);        
        */
         
        uint64_t p2_fbo ;
        int p2_sbid, p2_entry;
        rowId =0;
        for (i=0; i<count+1; i++)
        {
            found = m_indexlist.findRowId(pFile, rowId,key,&newIdxListHdrPtr, p_fbo, p_sbid, p_entry);
            /*
            if (!found)
            {
              printf(" tindex 3600->i %i rowId %i  Not Found \n", i, rowId);
              //return;
            }
            else
            {
               ////cout << " Found count in  findRowId line 3605-> " << i <<" rowId " << rowId << endl;
               ////cout << " p_fbo " << p_fbo << " p_sbid " << p_sbid << " p_entry " << p_entry << endl;

            }
            */
            CPPUNIT_ASSERT( found == true );
            rc = m_indexlist.deleteIndexList( pFile, rowId,
                                              key, &newIdxListHdrPtr,
                                              p2_fbo, p2_sbid, p2_entry);
            
            ////cout << " p2_fbo " << p2_fbo << " p2_sbid " << p2_sbid << " p2_entry " << p2_entry << endl;
            CPPUNIT_ASSERT( rc == NO_ERROR );
            rowId++;
        }
        
         rc = m_indexlist.deleteIndexList( pFile, rowId,
                                           key, &newIdxListHdrPtr,
                                           p_fbo, p_sbid, p_entry);
         
         CPPUNIT_ASSERT( rc != NO_ERROR );         
         m_indexlist.closeFile(pFile);
        //
        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );
        rc = m_indexlist.readDBFile( pFile, curBlock.data, fbo );
        m_indexlist.getSubBlockEntry( curBlock.data, sbid, entry, 32, &newIdxRidListHdr );
        CPPUNIT_ASSERT( newIdxRidListHdr.idxRidListSize.size == 0 );
        
        m_indexlist.closeFile(pFile);
        printf("\nSuccessfully Running testIndexListMultipleUpdate\n");
 }//testIndexListMultipleUpdate

};

CPPUNIT_TEST_SUITE_REGISTRATION( IndexTest );

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


