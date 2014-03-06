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
using namespace std;

#include <boost/scoped_ptr.hpp>
using namespace boost;

#include <cppunit/extensions/HelperMacros.h>

#include "we_indextree.h"
#include "we_freemgr.h"
#include "we_indexlist.h"


using namespace WriteEngine;

typedef struct {
	IdxRidListHdr	hdrRec;
    IdxRidListPtr	listPtr;
	int				fbo;
	int				sbid;
	int				entry;
	int				sbSeqNo;
	int				recCnt;
	int				sbCnt;
	int				fullSbCnt;
} IdxListStat;

typedef struct {
	int				mapSize;
	int				entryCnt;
} IdxFreeSpaceStat;

	const int		QA_FILE_TYPE_TREE         = 1;	
	const int		QA_FILE_TYPE_LIST         = 2;

	const int		QA_FREEMAP_1ENTRY         = 1;
	const int		QA_FREEMAP_2ENTRY         = 2;
	const int		QA_FREEMAP_4ENTRY         = 3;
	const int		QA_FREEMAP_8ENTRY         = 4;
	const int		QA_FREEMAP_16ENTRY        = 5;
	const int		QA_FREEMAP_32ENTRY        = 6;


class IndexTest : public CppUnit::TestFixture {

CPPUNIT_TEST_SUITE( IndexTest );

/* ====================================================
   Index stress test
   These tests will create the maxium number:
   1) index key records, one row ID for each
   2) row IDs for an index key
======================================================= */

//CPPUNIT_TEST( testCreateIndex );


/* ====================================================
   TTest Suites
   ======================================================= */

CPPUNIT_TEST(qaTSCStressTestMaxRowID);
//CPPUNIT_TEST(qaTSCStressTestMaxKey);
//CPPUNIT_TEST(qaTSCIndexTestUpdate);
//CPPUNIT_TEST(qaTSCIndexTestDelete);
//CPPUNIT_TEST(qaTSCFreeSpaceMapTest);

//CPPUNIT_TEST(qatestIndexList);



CPPUNIT_TEST_SUITE_END();

private:
   IndexTree                  m_index;
   IndexList                  m_indexlist;
   FILE* curFile;

public:
	void setUp() {
	}

	void tearDown() {
	}


    /* ==========================================================
	Create maximum number of row ids for key and verify all
	index list header records
	============================================================= */

   void qaTSCStressTestMaxRowID() {

	    IdxFreeSpaceStat mapInfo;

	   	uint64_t  	idxKey = 1;
		int			testCaseNo;
		int			numOfRowID;
		int			expSbCnt;
		int			expFullSbCnt;
		char		testCaseDesc[256];
	
		int			tokenLen = 1;
		int			rc;
		int			execMode;

		m_index.setUseFreeMgr( true );
		m_index.setUseListMgr( true );
		execMode = 0;

		strcpy(testCaseDesc, "8 bit key, max row stress test");

		tokenLen		= 8;
		testCaseNo		= 10520;
		numOfRowID		= 256;
		expSbCnt		= 9;
		expFullSbCnt	= 8;

		qaSupCreateMaxRowID(idxKey, tokenLen);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);
   		qaDspIndexListByKey(idxKey, tokenLen);


		strcpy(testCaseDesc, "16 bit key, max row stress test");

		testCaseNo		= 10521;
		numOfRowID		= 65536;
		expSbCnt		= 2115;
		expFullSbCnt	= 2114;

		qaSupCreateMaxRowID(idxKey, tokenLen);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);


}
   /* ==========================================================
	Create maximum number key for a given key length and 
	verify each index list header record
	============================================================= */

   void qaTSCStressTestMaxKey() {

		IdxFreeSpaceStat mapInfo;

		int			testCaseNo;
		char		testCaseDesc[256];
		int			tokenLen;
		int			entryCnt;
		int			execMode;

		execMode = 1;
//		qaSupCreateMaxHdrRec(tokenLen);

		strcpy(testCaseDesc, "8 bit key, max key stress test");

		tokenLen		= 8;
		testCaseNo		= 10522;

		qaSupCreateMaxHdrRec(tokenLen);
		qaTstStressMaxKeyDriver(testCaseNo, testCaseDesc, tokenLen);


//		qaSupCreateMaxHdrRec(tokenLen);

		strcpy(testCaseDesc, "16 bit key, max key stress test");

		tokenLen		= 16;
		testCaseNo		= 10523;

		qaSupCreateMaxHdrRec(tokenLen);
		qaTstStressMaxKeyDriver(testCaseNo, testCaseDesc, tokenLen);

   }

  /* ==========================================================
	Test cases for for updating index
	============================================================= */
   void qaTSCIndexTestUpdate() {
    
	uint64_t idxKey		= 1;
    int		 tokenLen	= 16;
	int		 testCaseNo;
	int		 startRowID	= 1;
	int		 numOfRowID;
	int		 expSbCnt;
	int		 expFullSbCnt;
	char	 testCaseDesc[256];

		strcpy(testCaseDesc, "Create 1 index key with one row ID, expecting no sub block and no full sub block.");

	    testCaseNo		= 10524;
		numOfRowID		= 1;
		expSbCnt		= 0;
		expFullSbCnt	= 0;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);

		strcpy(testCaseDesc, "Create 2 index key with one row ID, expecting no sub block and no full sub block.");

	    testCaseNo		= 10525;
		numOfRowID		= 2;
		expSbCnt		= 0;
		expFullSbCnt	= 0;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);

		strcpy(testCaseDesc, "Create 3 index key with one row ID, expecting 1 sub block and no full sub block.");

	    testCaseNo		= 10526;
		numOfRowID		= 3;
		expSbCnt		= 1;
		expFullSbCnt	= 0;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);

		strcpy(testCaseDesc, "Create 1 index key with 32 row IDs, expecting 1 sub block and 1 full sub block.");

	    testCaseNo		=10527;
		numOfRowID		= 32;
		expSbCnt		= 1;
		expFullSbCnt	= 1;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);

		strcpy(testCaseDesc, "Create 1 index key with 33 row IDs, expecting 2 sub block and 1 full sub block.");

	    testCaseNo		= 10528;
		numOfRowID		= 33;
		expSbCnt		= 2;
		expFullSbCnt	= 1;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);

		strcpy(testCaseDesc, "Create 1 index key with 63 row IDs, expecting 2 sub block and 2 full sub block.");
	    testCaseNo		= 10529;
		numOfRowID		= 63;
		expSbCnt		= 2;
		expFullSbCnt	= 2;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);
   }
/* ==========================================================
	Test cases deleting index
	============================================================= */
   void qaTSCIndexTestDelete() {
    

		uint64_t idxKey = 1;
		int		 tokenLen	= 16;
		int		 testCaseNo;
		int		 startRowID	= 1;
		int		 numOfRowID;
		int		 expSbCnt;
		int		 expFullSbCnt;
		char	 testCaseDesc[256];

		int		 i;
		int		 rc;

		//Create 1 row IDs, then remove it.
		//Expecting 0 row IDs, 0 sub blocks and 0 full sub blocks.

		strcpy(testCaseDesc, "Create 1 row ID, then remove it.  Expecting 0 row ID, 0 sub block and 0 full sub block.");
	    testCaseNo		= 10530;
		numOfRowID		= 1;
		expSbCnt		= 0;
		expFullSbCnt	= 0;
    
		m_index.setUseFreeMgr( true );
        m_index.setUseListMgr( true );

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);

		rc = m_index.openIndex( 990, 991 );
		CPPUNIT_ASSERT( rc == NO_ERROR );
 		rc = m_index.deleteIndex(idxKey, tokenLen, startRowID );
		CPPUNIT_ASSERT( rc == NO_ERROR );
		m_index.closeIndex();

		numOfRowID		= 0;
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);
//		qaDspIndexListStatsByKey(idxKey, tokenLen);

		//Create 3 row IDs, then remove the first two.
		//Expecting 1 row IDs, 1 sub blocks and 0 full sub blocks.

		strcpy(testCaseDesc, "Create 3 row IDs, then remove the first two.  Expecting 1 row ID, 1 sub block and 0 full sub block.");
	    testCaseNo		= 10531;
		numOfRowID		= 3;
		expSbCnt		= 1;
		expFullSbCnt	= 0;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);

		rc = m_index.openIndex( 990, 991 );
		CPPUNIT_ASSERT( rc == NO_ERROR );
		for (i=startRowID; i<startRowID + numOfRowID - 1; i++) {
			rc = m_index.deleteIndex(idxKey, tokenLen, i );
			CPPUNIT_ASSERT( rc == NO_ERROR );
		}
		m_index.closeIndex();

		numOfRowID		= 1;
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);
//		qaDspIndexListStatsByKey(idxKey, tokenLen);


 		//Create 100 row IDs, then remove them.
		//Expecting 0 row IDs, 0 sub blocks and 0 full sub blocks.

		strcpy(testCaseDesc, "Create 100 row IDs, then remove them.  Expecting 0 row ID, 0 sub block and 0 full sub block.");
	    testCaseNo		= 10532;
		numOfRowID		= 100;
		expSbCnt		= 0;
		expFullSbCnt	= 0;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);

		rc = m_index.openIndex( 990, 991 );
		CPPUNIT_ASSERT( rc == NO_ERROR );

 		for (i=startRowID; i<startRowID + numOfRowID; i++) {
			rc = m_index.deleteIndex(idxKey, tokenLen, i );
			CPPUNIT_ASSERT( rc == NO_ERROR );
		}
		m_index.closeIndex();

		numOfRowID		= 0;
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);
//		qaDspIndexListStatsByKey(idxKey, tokenLen);
  
		//create 63 row ids so that two full sub blocks will be allocated.
		//delete all row ids, 2 to 32, in the first sub block

		strcpy(testCaseDesc, "Create 63 row ids, then remove all in the 2nd sub block.  Verify that row id sub block gets released when empty.");
	    testCaseNo		= 10533;
		numOfRowID		= 63;
		expSbCnt		= 1;
		expFullSbCnt	= 1;

		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);

		rc = m_index.openIndex( 990, 991 );
		CPPUNIT_ASSERT( rc == NO_ERROR );
 
		for (i=2; i<33; i++) {
			rc = m_index.deleteIndex(idxKey, tokenLen, i );
			CPPUNIT_ASSERT( rc == NO_ERROR );
		}
		m_index.closeIndex();

		numOfRowID		= 32;
		qaTstIndexListDriver(testCaseNo, testCaseDesc, idxKey, tokenLen, numOfRowID, expSbCnt, expFullSbCnt);

   }
/* ==========================================================
	Test cases for empty maps
	============================================================= */

void qaTSCFreeSpaceMapTest() {

		IdxFreeSpaceStat mapInfo;
		uint64_t idxKey = 1;
		int		 tokenLen = 16;
		int		 execMode = 0;
		int		 testCaseNo, startKey, numOfKey;
		int		 startRowID, numOfRowID;
		int		 i;
		int		 expMapSize, expEntryCnt;
		char	 testCaseDesc[256];

		strcpy(testCaseDesc, "No keys created.  There should not be a empty list for header record.");
	    testCaseNo		= 10534;
		startKey		= 1;
		numOfKey		= 0;
		expMapSize		= 0;
		expEntryCnt		= 0;

		qaSupCreateRangeKey(idxKey, tokenLen, startKey, numOfKey);
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_4ENTRY, execMode, mapInfo);
		qaTstFreeMapDriver(testCaseNo, testCaseDesc, expMapSize, expEntryCnt, mapInfo); 

		strcpy(testCaseDesc, "Create 1 key.  There will be space left for 7 more header records.");
	    testCaseNo		= 10535;
		startKey		= 1;
		numOfKey		= 1;
		expMapSize		= 1;
		expEntryCnt		= 7;

		qaSupCreateRangeKey(idxKey, tokenLen, startKey, numOfKey);
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_4ENTRY, execMode, mapInfo);
		qaTstFreeMapDriver(testCaseNo, testCaseDesc, expMapSize, expEntryCnt, mapInfo); 

		strcpy(testCaseDesc, "Create 8 row ids.  There will be space left for 0 more header records.");
	    testCaseNo		= 10536;
		startKey		= 1;
		numOfKey		= 8;
		expMapSize		= 1;
		expEntryCnt		= 0;


		qaSupCreateRangeKey(idxKey, tokenLen, startKey, numOfKey);
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_4ENTRY, execMode, mapInfo);
		qaTstFreeMapDriver(testCaseNo, testCaseDesc, expMapSize, expEntryCnt, mapInfo); 

		strcpy(testCaseDesc, "Create 9 key.  There will be space left for 7 more header records.");
	    testCaseNo		= 10537;
		startKey		= 1;
		numOfKey		= 1;
		expMapSize		= 1;
		expEntryCnt		= 7;

		qaSupCreateRangeKey(idxKey, tokenLen, startKey, numOfKey);
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_4ENTRY, execMode, mapInfo);
		qaTstFreeMapDriver(testCaseNo, testCaseDesc, expMapSize, expEntryCnt, mapInfo); 
/*
		strcpy(testCaseDesc, "Verify that a sub block holding empty list entries will be released when empty.");
	    testCaseNo		= 10538;
		startRowID		= 1;
		expMapSize		= 0;
		expEntryCnt		= 0;
		//Create files without key records, then get number of 32-entry free blocks.
		numOfRowID		= 0;
		qaSupCreateRangeKey(idxKey, tokenLen, startRowID, numOfRowID);
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_32ENTRY, execMode, mapInfo);
		int initialSize = mapInfo.mapSize;
		int initialEntry = mapInfo.entryCnt;
		execMode = 1;
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_32ENTRY, execMode, mapInfo);
	printf("      size:   %d\n", mapInfo.mapSize);
	printf("      entry:   %d\n", mapInfo.entryCnt);

		numOfRowID		= 900;  //There are 31 entries in a 32-entry empty list sub block.
								//1 ptr allocated for 4-entry free map, 1 for hdr, 29 entries remaining.
								//29 entriess, pointing 29 sub blocks.  29 x 31 = 899, plus 1 in hdr = 900 entries
		qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, numOfRowID);
		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_32ENTRY, execMode, mapInfo);
		expMapSize		= initialSize - 1 - 1 + 1;		// - 1 hdr block, -1 4-entry list, + 1 free 32-entry block.  
		expEntryCnt		= initialEntry - 29 - 1 - 1 + 1;	// -29 data sub blocks, - 1 for 4-entry list ptr, 1 for hdr record ptr, +1 free 32-entry ptr
		expEntryCnt++					;	// add 1 for the free 32-entry sub block.

		qaTstFreeMapDriver(testCaseNo, testCaseDesc, expMapSize, expEntryCnt, mapInfo); 
		qaDspIndexListByKey(idxKey, tokenLen);
		qaDspIndexListStatsByKey(idxKey, tokenLen);

*/


		execMode = 1;
//		qaDspFreeSpaceMap(QA_FILE_TYPE_LIST, QA_FREEMAP_32ENTRY, execMode, mapInfo);

//   		qaDspIndexListByKey(idxKey, tokenLen);
//		qaDspIndexListStatsByKey(idxKey, tokenLen);
  

}

/* ==========================================================
	verify each index list header record for a given key length
	============================================================= */

void qaTstStressMaxKeyDriver(int testCaseNo, char* testCaseDesc, int tokenLen) {
bool	rc, passed, ;
	int		checkMode		= 2;
	int		i, maxKey;
	int		numOfRowID		= 1;
	int		expSbCnt		= 0;
	int		expFullSbCnt	= 0;
	
	qaDspTestStatusHeader(testCaseNo, testCaseDesc);

    maxKey = (1 << tokenLen);
if (maxKey > 160) maxKey = 120;
	
	passed = true;
	for (i=0; i<maxKey; i++) {
		rc = qaSupVerifyIndexByKey(i, tokenLen, checkMode, numOfRowID, expSbCnt, expFullSbCnt);
		if (rc != NO_ERROR) passed = false;
	}
	qaDspTestStatusFooter(testCaseNo, passed);
}

 /* ==========================================================
	A driver function to execution test cases for the 
	index list.
	============================================================= */
void qaTstIndexListDriver(int testCaseNo, char* testCaseDesc, uint64_t idxKey, int tokenLen, int numOfRowID, int expSbCnt, int expFullSbCnt) {

	bool	passed;
	int		checkMode = 2;

	qaDspTestStatusHeader(testCaseNo, testCaseDesc);
	passed = qaSupVerifyIndexByKey(idxKey, tokenLen, checkMode, numOfRowID, expSbCnt, expFullSbCnt);
	qaDspTestStatusFooter(testCaseNo, passed);
}

/* ==========================================================
	A driver function to verify free map list
	============================================================= */
void qaTstFreeMapDriver(int testCaseNo, char* testCaseDesc, int expMapSize, int expEntryCnt, IdxFreeSpaceStat mapInfo) {

	int		passed			= true;

	qaDspTestStatusHeader(testCaseNo, testCaseDesc);

	if (mapInfo.mapSize != expMapSize) {
		printf("      Free map size mismatch:     Expected: %d    Returned: %d\n", expMapSize, (int)mapInfo.mapSize );
		passed = false;
	}

	if (mapInfo.entryCnt != expEntryCnt) {
		printf("      Free map entry mismatch:    Expected: %d    Returned: %d\n", expEntryCnt, (int)mapInfo.entryCnt );
		passed = false;
	}
	
	qaDspTestStatusFooter(testCaseNo, passed);
}
/* ==========================================================
	Verify index list record with it's integral components
	and expected values from the calling program.
============================================================= */
	bool qaSupVerifyIndexByKey(uint64_t idxKey, int tokenLen, int checkMode, int expRowIDCnt, int expSbCnt, int expFullSbCnt) {
	
   	IdxListStat	idxKeyInfo;

	int			execMode	= 0;
	int			passed		= true;
	
    //checkMode != 1		Verify index header information only
	//checkMode  = 2		verify header information, plus matching expected results.


	qaSupWalkIndexList(idxKey, tokenLen, execMode, idxKeyInfo);

	// verify for matching record keys
	if (idxKeyInfo.hdrRec.key != idxKey) {
		printf("      Index key mismatch:         Expected: %d    Returned: %d\n", (int)idxKey, (int)idxKeyInfo.hdrRec.key );
		passed = false;
	}

	//	verify row id count in the hdr record against row id count gathered from the index list record.
	if (idxKeyInfo.hdrRec.idxRidListSize.size != idxKeyInfo.recCnt) {
		printf("      Index Hdr cnt mismatch:     In Hdr  : %d    In SB:    %d\n", idxKeyInfo.hdrRec.idxRidListSize.size, idxKeyInfo.recCnt );
		passed = false;
	}

	// verify that when there is not sub blocks for the row IDs, the header's link list pointer should not be used.
	if ((idxKeyInfo.sbCnt == 0) && (idxKeyInfo.hdrRec.nextIdxRidListPtr.type == 4)) {
		printf("      Incorrect list pointer:     Ptr type: %d     SB Cnt:  %d\n", idxKeyInfo.hdrRec.idxRidListSize.size, idxKeyInfo.recCnt );
		passed = false;
	}

	if (checkMode = 2) {

		// verify row id count in the hdr record against expected row id count
		if (idxKeyInfo.hdrRec.idxRidListSize.size != expRowIDCnt) {
			printf("      Row ID cnt mismatch:        Expected: %d    Returned: %d\n", expRowIDCnt, idxKeyInfo.hdrRec.idxRidListSize.size);
			passed = false;
		}

		//	verify sub block count gathered from the index list record against the expected sub block count
		if (idxKeyInfo.sbCnt != expSbCnt) {
			printf("      Subblock cnt mismatch:      Expected: %d    Returned: %d\n", expSbCnt, idxKeyInfo.sbCnt );
			passed = false;
		}

		//	verify full sub block count gathered from the index list record against the expected full sub block count
		if (idxKeyInfo.fullSbCnt != expFullSbCnt) {
			printf("      Full subblock cnt mismatch: Expected: %d    Returned: %d\n", expFullSbCnt, idxKeyInfo.fullSbCnt );
			passed = false;
		}
	}

	return passed;

    }   
 


/* ==========================================================
	Display index record for a given key
============================================================= */

   void qaGetListSubblockByKey(uint64_t idxKey, int tokenLen, int sbSeqNo, int& fbo, int& sbid, int& entry) {

	   int		p_fbo, p_sbid, p_entry;
	   int		execMode = 1;
	   bool		exist;

//	   qaSupWalkIndexList(idxKey, tokenLen, execMode, sbSeqNo, p_fbo, p_sbid, p_entry, hdrCntMatch, hdrRecCnt, hdrSbCnt, hdrFullSbCnt);
	   fbo = p_fbo;
	   sbid = p_sbid;
	   entry = p_entry;
   }


/* ==========================================================
	Display tree empty list map for a given block size
============================================================= */
    void qaDspFreeSpaceMap(int fileType, int mapType, int execMode, IdxFreeSpaceStat& mapInfo) {

		DataBlock	curBlock;
		FILE*       pFile =NULL;
		
		int         fileNum;
 		int			p_fbo, p_sbid, p_entry, p_type, p_group;
		int			i, j; 
		int			p_mapType;
		bool		done;

	  
		IdxEmptyListEntry emptyMap;

		if (fileType == QA_FILE_TYPE_TREE)
			fileNum = 990;		//index tree
		else
			fileNum = 991;		//index list

        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( fileNum );
        CPPUNIT_ASSERT( pFile != NULL );

		//Free Manager
         m_indexlist.setUseFreeMgr( true );

    	p_fbo = 0;
		p_sbid = 0;
		p_entry = mapType; //Entry location in Fb 0 and SB 0;

		if (execMode == 1) {
			printf("\n------------------------------------\n");
			if (fileType == 1)
				printf("Index Tree %d-ENTRY free space map\n", 1<<(mapType-1));
			else
				printf("Index list %d-ENTRY free space map\n", 1<<(mapType-1));
			printf("------------------------------------\n");
		}
        m_indexlist.readSubBlockEntry( pFile, &curBlock, p_fbo, p_sbid, p_entry, 8, &emptyMap);
		if (execMode == 1) {
//			printf("\nList Ptr:  Type = %d  Group = %d  fbo = %d  sbid = %d Entry = %d\n", p_type, p_group, p_fbo, p_sbid, p_entry);
			printf("\nList:       Type = %d  Group = %d Addr(%d, %d, %d) ----> Addr(%d, %d, %d)\n", int(emptyMap.type), int(emptyMap.group), p_fbo, p_sbid, p_entry, (int)emptyMap.fbo, (int)emptyMap.sbid, (int)emptyMap.entry);
			printf("-------------------------------------------------------------------------\n");
		}

		p_type = emptyMap.type;
		p_group = emptyMap.group;
		p_fbo = emptyMap.fbo;
		p_sbid = emptyMap.sbid;
		p_entry = emptyMap.entry;

		p_mapType = emptyMap.type;
		mapInfo.mapSize = 0;
		mapInfo.entryCnt = 0;

		done = ((p_mapType == 1) && (p_fbo == 0) && (p_sbid == 0) && (p_entry == 0));
		
		while (not done ) {

			j = p_entry;
			mapInfo.mapSize++;
			for (i=j; i >= 0; i--) {
				m_indexlist.readSubBlockEntry( pFile, &curBlock, p_fbo, p_sbid, i, 8, &emptyMap);
				if (execMode == 1)
//					printf("EntryPtr: %d Type = %d  Group = %d  fbo = %d  sbid = %d Entry = %d\n", i, int(emptyMap.type), int(emptyMap.group), (int)emptyMap.fbo, (int)emptyMap.sbid, (int)emptyMap.entry);
					printf("Entry: %d   Type = %d  Group = %d Addr(%d, %d, %d) ----> Addr(%d, %d, %d)\n", i,  int(emptyMap.type), int(emptyMap.group), p_fbo, p_sbid, i, (int)emptyMap.fbo, (int)emptyMap.sbid, (int)emptyMap.entry);
				if (i != 0) mapInfo.entryCnt++;
			}
			p_fbo = emptyMap.fbo;
			p_sbid = emptyMap.sbid;
			p_entry = emptyMap.entry;
			p_mapType = emptyMap.type;

			done = ( (p_fbo == 0) && (p_sbid == 0) && (p_entry == 0));
			if (not done && (execMode == 1)) {
//				printf("\nList Ptr:  Type: %d  Group: %d  FBO: %d  SBID: %d ENTRY: %d\n", p_type, p_group, p_fbo, p_sbid, p_entry);
				printf("\nList:       Type = %d  Group = %d Addr(%d, %d, %d) ----> Addr(%d, %d, %d)\n", int(emptyMap.type), int(emptyMap.group), p_fbo, p_sbid, p_entry, (int)emptyMap.fbo, (int)emptyMap.sbid, (int)emptyMap.entry);
				printf("-------------------------------------------------------------------------\n");
			}
		}
 
		m_indexlist.closeFile(pFile);
 
	} // qaDspFreeSpaceMap

/* ==========================================================
	Show branch list for all keys for a given key length
============================================================= */

      void qaDspTreePathAll(uint64_t idxKey, int tokenLen) {

		uint64_t	i;
		int			maxKey;

		maxKey = (1 << tokenLen);

		for (i=0; i<maxKey; i++)
			qaDspTreePathByKey(idxKey, tokenLen);
	}

	/*******************************************************
.   Display branch list for a key
*******************************************************/
	void qaDspTreePathByKey(uint64_t idxKey, int tokenLen) {

		int		p_fbo, p_sbid, p_entry;
		bool	exist;
		int		execMode = 1;
	
		qaSupWalkIndexTree(idxKey, tokenLen, execMode, p_fbo, p_sbid, p_entry, exist);

   } // qaDspTreePathByKey
/* ==========================================================
	Verfiy hdr records all keys for a given key length
============================================================= */
void qaDspIndexListStatsByKey(uint64_t idxKey, int tokenLen) {

	IdxListStat	idxKeyInfo;

	int			execMode = 0;
	int			sbSeqNo = 0;

	qaSupWalkIndexList (idxKey, tokenLen, execMode, idxKeyInfo);

	printf("\nINDEX STATS for key: %d\n", (int)idxKey);
	printf("--------------------------------------\n");
	printf("Key:                %d\n", (int)idxKeyInfo.hdrRec.key );
	printf("Hdr Record Count:   %d\n", idxKeyInfo.hdrRec.idxRidListSize.size );

	if (idxKeyInfo.hdrRec.firstIdxRidListEntry.type == 3)
		printf("RowID 0   :         %d\n", idxKeyInfo.hdrRec.firstIdxRidListEntry.rid);
	if (idxKeyInfo.hdrRec.nextIdxRidListPtr.type == 3)
		printf("RowID 1   :         %d\n", (int)idxKeyInfo.hdrRec.nextIdxRidListPtr.llp);
    printf("File Block:         %d\n", idxKeyInfo.fbo);
    printf("Sub Block:          %d\n", idxKeyInfo.sbid);
    printf("Entry:              %d\n", idxKeyInfo.entry);
    printf("Actual Row ID Cnt:  %d\n", idxKeyInfo.recCnt);
    printf("Sub Block Cnt:      %d\n", idxKeyInfo.sbCnt);
    printf("Full Sub Block Cnt: %d\n", idxKeyInfo.fullSbCnt);
	printf("Row ID Cnt Match?:  ");
	if (idxKeyInfo.hdrRec.idxRidListSize.size == idxKeyInfo.recCnt)
		printf("Yes\n");
	else
		printf("No\n");
}
/* ==========================================================
	Display index record all keys for a given key length
============================================================= */

  void qaDspIndexListAll(uint64_t, int tokenLen) {
		uint64_t	i;
		int			maxKey;

		maxKey = (1 << tokenLen);

		for (i=0; i<maxKey; i++)
			qaDspIndexListByKey(i, tokenLen);
	}

/* ==========================================================
	Display index record for a given key
============================================================= */
   void qaDspIndexListByKey(uint64_t idxKey, int tokenLen) {

	   IdxListStat	idxKeyInfo;

	   int		p_fbo, p_sbid, p_entry;
	   int		execMode = 1;
	   bool		exist;
	   int		sbSeqNo = 0;

	   qaSupWalkIndexList (idxKey, tokenLen, execMode, idxKeyInfo);
   }
/* ==========================================================
	Display test case status header
	============================================================= */
void qaDspTestStatusHeader(int testCaseNo, char* testCaseDesc) {

	printf("\nBEGIN IdxHdrTestCase %d\n", testCaseNo);
	printf("      Desc:   %s\n", testCaseDesc);
}

/* ==========================================================
	Display test case status header
	============================================================= */
void qaDspTestStatusFooter(int testCaseNo, bool passed) {

	if (passed) 
		printf("      Status: Passed\n");
	else 
	{
		printf("      Status: Failed  -- [**ERROR**]\n");
	}
	printf("END   IdxHdrTestCase %d\n", testCaseNo);
}
/* ==========================================================
   Populate tree with max key for a given bit length
   Populate tree row ID for a given bitlength
============================================================= */
   void qaSupCreateMaxRowID(uint64_t idxKey, int tokenLen) {

	  int			maxKey;
	  int			startRowID = 0;

	  maxKey = (1 << tokenLen);

	  qaSupCreateRangeRowID(idxKey, tokenLen, startRowID, maxKey);

   } //qaSupCreateMaxRowID
  
/* ==========================================================
   Populate tree with a range of row IDs for a key.
============================================================= */
   void qaSupCreateRangeRowID(uint64_t idxKey, int tokenLen, int startRowID, int numOfRowID) {

      int			rc;
	  int			i;
	  int			maxKey;

      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );

      // here is the correct check
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

	  for (i=0; i<numOfRowID; i++) {
			rc = m_index.updateIndex( idxKey, tokenLen, startRowID + i );  //1 key, multiple rows, multiple row ids
		CPPUNIT_ASSERT( rc == NO_ERROR );
	  }
	  m_index.closeIndex();

   } //qaSupCreateMaxRowID
 
   /* ==========================================================
   Populate tree with max key for a given bit length
   Populate tree row ID for a given bitlength
============================================================= */
   void qaSupCreateMaxHdrRec(int tokenLen) {

      int			rc;
	  uint64_t		i;
	  int			maxKey;

      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );

      // here is the correct check
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

	  maxKey = (1 << tokenLen);
if (maxKey > 160) maxKey = 160;

	for (i=0; i<maxKey; i++)
		rc = m_index.updateIndex( i, tokenLen, i );  //1 row id per key.
	
		m_index.closeIndex();

   } //qaSupCreateMaxHdrRec
   /* ==========================================================
   Populate tree with range of keys
============================================================= */

 void qaSupCreateRangeKey(uint64_t idxKey, int tokenLen, int startKey, int numOfKey) {
      int			rc;
	  uint64_t   	i;
	  int			maxKey;

      rc = m_index.dropIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      rc = m_index.createIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

      m_index.setUseFreeMgr( true );
      m_index.setUseListMgr( true );

      // here is the correct check
      rc = m_index.openIndex( 990, 991 );
      CPPUNIT_ASSERT( rc == NO_ERROR );

	  for (i=startKey; i<startKey + numOfKey; i++)
		rc = m_index.updateIndex( i, tokenLen, i );  //1 row id per key.
	
	  m_index.closeIndex();

   } //qaSupCreateRangeKey

/*******************************************************
.  This function navigate through the index list record.
few global variables are set relating to index list.
*******************************************************/
   void qaSupWalkIndexList (uint64_t idxKey, int tokenLen, int execMode, IdxListStat& idxKeyInfo) {

      DataBlock  indexBlock;
	  FILE*			pFileIndexList=NULL;
	  int			file_indexList=991;

      int p_fbo, p_sbid, p_entry;
	  int i,;
	  int SBRecCnt;
	  bool endOfList;
	  bool exist;
	  int tempExecMode;


	  IdxRidListEntry listEntry;
	  IdxRidListPtr listPtr;
  	  IdxRidListHdr indexListHdrRec;


		//Free Manager
      m_indexlist.setUseFreeMgr( true );
	  tempExecMode = execMode;
	  execMode = 0;
      qaSupWalkIndexTree(idxKey, tokenLen, execMode, p_fbo, p_sbid, p_entry, exist);
	  execMode = tempExecMode;

	  idxKeyInfo.fbo = p_fbo;		//Save the address for the header
	  idxKeyInfo.sbid = p_sbid;
	  idxKeyInfo.entry = p_entry;

	  if (not exist) 
		  printf("Tree node not found: Key = %d\n", (int)idxKey);
	  else
	  {
		//read index list header record
	
		i = 0;

		idxKeyInfo.sbCnt = 0;
		idxKeyInfo.fullSbCnt = 0;
		idxKeyInfo.recCnt = 0; //first entry in the hdr
		endOfList = false;

        memset( indexBlock.data, 0, sizeof(indexBlock.data));
        pFileIndexList= m_indexlist.openFile( file_indexList );
        CPPUNIT_ASSERT( pFileIndexList != NULL );

		m_indexlist.readSubBlockEntry( pFileIndexList, &indexBlock, p_fbo, p_sbid, p_entry, 32, &indexListHdrRec);

		idxKeyInfo.hdrRec = indexListHdrRec;


		if (execMode == 1) {
			printf("\n---------------------------------\n");
			printf("Index record for key: %d\n", (int)idxKey);
			printf("---------------------------------\n");
			printf("Size:    %d\n", (int)indexListHdrRec.idxRidListSize.size);
			printf("Key:     %d\n", (int)indexListHdrRec.key);
			printf("RowID 1: %d    Type:  %d\n", (int)indexListHdrRec.firstIdxRidListEntry.rid, (int)indexListHdrRec.firstIdxRidListEntry.type);
			if (indexListHdrRec.nextIdxRidListPtr.type == 3 )
				printf("RowID 2: %d    Type: %d\\nn", (int)indexListHdrRec.nextIdxRidListPtr.llp, (int)indexListHdrRec.nextIdxRidListPtr.type);
			else if (indexListHdrRec.nextIdxRidListPtr.type == 4 )
				printf("Ptr:     Addr(%d, %d, %d) Type = %d\n\n", (int)((IdxEmptyListEntry*)&(indexListHdrRec.nextIdxRidListPtr))->fbo, (int)((IdxEmptyListEntry*)&(indexListHdrRec.nextIdxRidListPtr))->sbid, (int)((IdxEmptyListEntry*)&(indexListHdrRec.nextIdxRidListPtr))->entry, (int)indexListHdrRec.nextIdxRidListPtr.type);
		}			

		if (int(indexListHdrRec.firstIdxRidListEntry.type) == 3) 
			idxKeyInfo.recCnt++;

		if (indexListHdrRec.nextIdxRidListPtr.type == 3 ) {
			idxKeyInfo.recCnt++;
		} else
			if (indexListHdrRec.nextIdxRidListPtr.type == 4 ) {
				p_fbo=((IdxEmptyListEntry*)&(indexListHdrRec.nextIdxRidListPtr))->fbo;
				p_sbid=((IdxEmptyListEntry*)&(indexListHdrRec.nextIdxRidListPtr))->sbid;
				p_entry=((IdxEmptyListEntry*)&(indexListHdrRec.nextIdxRidListPtr))->entry;
				while ( not endOfList ) {
					idxKeyInfo.sbCnt++;
					SBRecCnt = 0;
					m_indexlist.readSubBlockEntry( pFileIndexList, &indexBlock, p_fbo, p_sbid, 31, 8, &listPtr);
					printf("Index list subblock: #%d\n",idxKeyInfo.sbCnt);
					for (i=0; i<=30; i++) {
						m_indexlist.readSubBlockEntry( pFileIndexList, &indexBlock, p_fbo, p_sbid, i, 8, &listEntry);
						if (listEntry.type == 3) {
							SBRecCnt++;
							if (execMode == 1) 
								printf("Entry %d: Addr(%d, %d, %d)  RowId: %d \n", i, p_fbo, p_sbid, i, listEntry.rid);
						}
					}
					p_fbo=((IdxEmptyListEntry*)&(listPtr))->fbo;
					p_sbid=((IdxEmptyListEntry*)&(listPtr))->sbid;
					if ((idxKeyInfo.sbSeqNo != 0 ) && (idxKeyInfo.sbSeqNo == (idxKeyInfo.sbCnt-1))) {
						idxKeyInfo.fbo = p_fbo;		//Save the address for the request block
						idxKeyInfo.sbid = p_sbid;
						idxKeyInfo.entry = p_entry;
					}
					if (execMode == 1 )
						printf("Entry 31: Addr(%d, %d, %d)   RecCnt = %d\n\n", p_fbo, p_sbid, p_entry, (int)listPtr.count);
					idxKeyInfo.recCnt += SBRecCnt;
					if (SBRecCnt == 31) idxKeyInfo.fullSbCnt++;

					if (SBRecCnt != listPtr.count)
						printf("[**ERROR**] Sub Block entry count Error: In Ptr Rec: %d  In sub block: %d\n", listPtr.count, SBRecCnt);
					endOfList = (listPtr.type == 0);
				
				}
			}
		}
		m_indexlist.closeFile(pFileIndexList);
}

/*******************************************************
.  This function navigate through the tree, from the
bitmap entry to the leaf.  It can be used for many
functions that require access to the hdr record also.
*******************************************************/
void qaSupWalkIndexTree(uint64_t idxKey, int tokenLen, int execMode, int& fbo, int& sbid, int& entry, bool& exist) {

      DataBlock               curBlock;
      FILE*                   pFile =NULL;
      int                     file_number=990;

      IdxBitmapPointerEntry   bitmapRec;
	  IdxBitTestEntry		  treeNodeRec;
      int p_fbo, p_sbid, p_entry, p_type, p_group,p_bitTest;
	  int i;
	  int maskValue = 31;  //mask for 11111.
	  int bitMapLoc, key1, shiftBits;
	  int bitCnt;
 	  bool found;
	  int mask[5] = {1, 3, 7, 15, 31};
	  int group[7] = {1, 2, 4, 8, 16, 32, 64};

        memset( curBlock.data, 0, sizeof(curBlock.data));
        pFile= m_indexlist.openFile( file_number );
        CPPUNIT_ASSERT( pFile != NULL );

		//Free Manager
        m_indexlist.setUseFreeMgr( true );
		
		if (tokenLen > 5)
			bitMapLoc = ((idxKey >> tokenLen-5) & maskValue);		 
		else
			bitMapLoc = idxKey;

		p_fbo = 0;
		p_sbid = 1;
		p_entry = bitMapLoc;

		if (execMode == 1) {
			printf("\n---------------------------------\n");
			printf("Tree branch path for key: %d\n", (int)idxKey);
			printf("---------------------------------\n");
		}
		m_indexlist.readSubBlockEntry( pFile, &curBlock, p_fbo, p_sbid, p_entry, 8, &bitmapRec);

		shiftBits = 1;
		p_type = bitmapRec.type;
		p_fbo = bitmapRec.fbo;
		p_sbid = bitmapRec.sbid;
		p_entry = bitmapRec.entry;
		if (execMode == 1) // branch list
			printf("Bitmap:   Type= %d            BitTest= %d  FBO= %d  SBID= %d Entry= %d\n", int(p_type),int(p_bitTest), (int)p_fbo, (int)p_sbid, (int)p_entry);

		bitCnt = tokenLen - 5;
		while (bitCnt > 0) {
			if (bitCnt> 5)
				key1 = ((idxKey >> (bitCnt-5)) & maskValue);
			else
				key1 = ((idxKey) & mask[bitCnt-1]);
			m_indexlist.readSubBlockEntry( pFile, &curBlock, p_fbo, p_sbid, p_entry, 8, &treeNodeRec);

			int groupIdx = treeNodeRec.group;
//		printf("group: %d  count: %d\n", groupIdx, group[groupIdx]);

			found = false;
			for (i=0;i<group[groupIdx];i++) { 
				m_indexlist.readSubBlockEntry( pFile, &curBlock, p_fbo, p_sbid, p_entry + i, 8, &treeNodeRec);
				found = ( treeNodeRec.bitTest == key1);
  				p_type = treeNodeRec.type;
//			printf("key1: %d  bitTest: %d\n", key1, treeNodeRec.bitTest);
			if (found) i = group[groupIdx];

			};
//			m_indexlist.readSubBlockEntry( pFile, &curBlock, p_fbo, p_sbid, p_entry, 8, &treeNodeRec);

			p_type = treeNodeRec.type;
			p_group = treeNodeRec.group;
			p_bitTest = treeNodeRec.bitTest;
			p_fbo = treeNodeRec.fbo;
			p_sbid = treeNodeRec.sbid;
			p_entry = treeNodeRec.entry;
			bitCnt = bitCnt - 5;
			if (execMode == 1) // branch list
				printf("Level:    Type= %d  Group= %d  BitTest= %d  FBO= %d  SBID= %d Entry= %d\n", int(p_type),int(p_group),int(p_bitTest), (int)p_fbo, (int)p_sbid, (int)p_entry);
		}
//set global variables so that calling functions can access to the address of the index header record
		
		exist = found;
		fbo = p_fbo;
		sbid = p_sbid;
		entry = p_entry;
		
		m_indexlist.closeFile(pFile);
   } // qaSupWalkIndexTree

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


