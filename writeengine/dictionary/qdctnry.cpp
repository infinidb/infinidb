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

#include "we_dctnry.h"
#include "we_dctnrystore.h"

using namespace WriteEngine;

class DctnryTest : public CppUnit::TestFixture {


CPPUNIT_TEST_SUITE( DctnryTest );

// Dctnry basic testing
//CPPUNIT_TEST( testDctnryInsertLarge );
//CPPUNIT_TEST( testDctnryInsertDelete );
///CPPUNIT_TEST( testDctnryInsertStress);
//CPPUNIT_TEST( testGetBlockHdr);
//CPPUNIT_TEST( testGetFreeSpace);
CPPUNIT_TEST( testDctnryInsertDeleteStore);
CPPUNIT_TEST_SUITE_END();

private:
   Dctnry                    m_Dctnry;
   DctnryStore               m_DctnryStore;
   int                       m_oId;
public:
	void setUp() {
	}

	void tearDown() {
	}
 DctnryTest()
 {
    m_oId =0;
 } 
void testDctnryInsertLarge() {
       int                     oId=2000;
//     int                     blockCount =10;
       
       FILE*                   dFile =NULL;
       
       int                     rc =0;
       DataBlock               curBlock;
//       int                     largeSize=6144;
       int                     largeSize=2724;
       int                     smallSize=2032;
       
       uint16_t freeSpace;
       uint64_t nextPtr;
       uint16_t offSet0;
       uint16_t endHeader;
       unsigned char dctnryHeader[14];
       m_Dctnry.setUseSmallSize(false);
       m_Dctnry.setDebugLevel( DEBUG_3 );
       m_oId = oId;
       
       memset(curBlock.data,0, sizeof (curBlock.data));
       memset(dctnryHeader,0, sizeof(dctnryHeader));
       printf("\nRunning testDctnryInsertLarge \n");

       rc = m_Dctnry.deleteFile(oId);
       rc = m_Dctnry.createDctnry(oId,3);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       rc = m_Dctnry.openDctnry(oId);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
       

       rc = m_Dctnry.initDctnryHdr( dFile);
       CPPUNIT_ASSERT( rc == NO_ERROR );       
       m_Dctnry.closeDctnry(); 
       printf("After initDctnryHdr \n");
       testGetBlockHdr();     

       //dFile= m_Dctnry.openFile( oId); 
       rc = m_Dctnry.openDctnry();
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
       //12 for 6 bytes and 14 for 8 bytes        
       rc =m_Dctnry.readSubBlockEntry( dFile, &curBlock, 0, 0, 0, 14, &dctnryHeader); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       memcpy(&freeSpace,dctnryHeader,2);
       memcpy(&nextPtr,dctnryHeader+2,8); // 8 bytes
       memcpy(&offSet0,dctnryHeader+10,2);
       memcpy(&endHeader,dctnryHeader+12,2);   

       unsigned char sgnature_value[largeSize];
       memset(sgnature_value,0, sizeof(sgnature_value));
       int j=0;
       for (int i=0; i<largeSize; i++)
       {
//          if (j>255)
//           j=0;           
          sgnature_value[i]=119;
 //         j++;       
       }
       //insert a signature value
       Token token;

       rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                                  sgnature_value, token);
       CPPUNIT_ASSERT( rc == NO_ERROR );
	   testGetBlockHdr();


       rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                                  sgnature_value, token);
       CPPUNIT_ASSERT( rc == NO_ERROR );
	   testGetBlockHdr();


	          rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                                  sgnature_value, token);
       CPPUNIT_ASSERT( rc == NO_ERROR );
	   testGetBlockHdr();

	   	   token.fbo = 0;
	   	   token.op = 3;
	          rc =m_Dctnry.deleteDctnryValue( dFile, token);
	   testGetBlockHdr();

	          rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                                  sgnature_value, token);
       CPPUNIT_ASSERT( rc == NO_ERROR );
	   testGetBlockHdr();


	   return;

	   //add the same dictionary again.  it should not be added
	   rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                                  sgnature_value, token);
	   testGetBlockHdr();
	   //add the same dictionary again.  it should not be added to the next block

	   	   rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                                  sgnature_value, token);
	   testGetBlockHdr();

       printf("token fbo = %i",token.fbo);
       printf("token op = %i",token.op);
	   token.fbo = 0;
	   token.op = 2;
//	   token.op++;
	          rc =m_Dctnry.deleteDctnryValue( dFile, token);
	   testGetBlockHdr();

	   return;
	   
	   //add a 2nd dictionary to fillup the whole file block

	   unsigned char sgnature_value_s[smallSize];
       memset(sgnature_value_s,0, sizeof(sgnature_value_s));

	          for (int i=0; i<smallSize; i++)
       {
//          if (j>255)
//           j=0;           
          sgnature_value_s[i]=118;
 //         j++;       
       }
       //insert a signature value
       rc = m_Dctnry.insertDctnry(dFile, smallSize, 
                                  sgnature_value_s, token);
       CPPUNIT_ASSERT( rc == NO_ERROR );
	   testGetBlockHdr();


	   return;
              //insert a signature value
       Token token3;
       rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                           sgnature_value, token3);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       m_Dctnry.closeDctnry(dFile);
       printf("After insert dictionary \n");
       testGetBlockHdr();
       rc = m_Dctnry.openDctnry(oId);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       rc =m_Dctnry.deleteDctnryValue( dFile, token);
       m_Dctnry.closeDctnry(dFile);
       printf("After delete dictionary \n");
       testGetBlockHdr();
       
       CPPUNIT_ASSERT( rc == NO_ERROR );
       j=255;
       for (int i=0; i<smallSize; i++)
       {
          if (j==0)
           j=255;           
          sgnature_value[i]=j;
          j--;       
       }
       //insert another signature value
       rc = m_Dctnry.openDctnry(oId);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
       Token token2;
       rc = m_Dctnry.insertDctnry(dFile, smallSize, 
                           sgnature_value, token2);                           
       CPPUNIT_ASSERT( rc == NO_ERROR );
       m_Dctnry.closeDctnry(dFile);
       printf("After insert dictionary \n");
       testGetBlockHdr();
       m_Dctnry.dropDctnry(oId);       
       return;
   }//testDctnryInsert

void testDctnryInsertDelete() {
       int                     oId=2001;
//     int                     blockCount =10;
       
       FILE*                   dFile =NULL;
       
       int                     rc =0;
       DataBlock               curBlock;
       int                     largeSize=9000;
       int                     smallSize=1000;

       unsigned char dctnryHeader[14];        
       uint16_t freeSpace;
       uint64_t nextPtr;
       uint16_t offSet0;
       uint16_t endHeader;
       
       m_Dctnry.setDebugLevel( DEBUG_3 );  
       m_Dctnry.setUseSmallSize(true);
       m_oId =oId;
       memset(curBlock.data,0, sizeof (curBlock.data));
       memset(dctnryHeader,0, sizeof(dctnryHeader));
       printf("\nRunning testDctnryInsertDelete \n");

       rc = m_Dctnry.deleteFile(oId);
       
       rc = m_Dctnry.createDctnry((FID)-1);
       CPPUNIT_ASSERT( rc == ERR_OPEN_FILE );
       rc = m_Dctnry.createDctnry(oId,10);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       rc = m_Dctnry.openDctnry();
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL ); 
       
       rc = m_Dctnry.initDctnryHdr( dFile);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       //CPPUNIT_ASSERT(rc==NO_ERROR);
       //m_Dctnry.closeFile(dFile);
       m_Dctnry.closeDctnry();
       printf("After initDctnryHdr");
       testGetBlockHdr();
       //testGetFreeSpace();
       rc = m_Dctnry.openDctnry(); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );     
       //12 for 6 bytes and 14 for 8 bytes   
       rc =m_Dctnry.readSubBlockEntry( dFile, &curBlock, 0, 0, 0, 14, &dctnryHeader); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       memcpy(&freeSpace,dctnryHeader,2);
       memcpy(&nextPtr,dctnryHeader+2,8); // 8 bytes
       memcpy(&offSet0,dctnryHeader+10,2);
       memcpy(&endHeader,dctnryHeader+12,2);   

       unsigned char sgnature_value[largeSize];
       memset(sgnature_value,0, sizeof(sgnature_value));
       int j=0;
       for (int i=0; i<largeSize; i++)
       {
          if (j>255)
           j=0;
           
          sgnature_value[i]=j;
          j++;      
       }
       //insert a signature value
       Token token;
       
       rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                           sgnature_value, token);
       CPPUNIT_ASSERT( rc ==ERR_DICT_SIZE_GT_8000);       
       largeSize = 6000;
       Token token3;
       for (int i=0; i<largeSize; i++)
       {
          if (j>255)
           j=0;          
          sgnature_value[i]=j;
          j++;       
       }
       rc = m_Dctnry.insertDctnry(dFile, largeSize, 
                           sgnature_value, token3);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       CPPUNIT_ASSERT( token3.fbo == 0 );
       CPPUNIT_ASSERT( token3.op == 1 ); 
       m_Dctnry.closeDctnry(dFile);
       printf("After insert token 3 into dictionary \n");
       testGetBlockHdr();  
       rc = m_Dctnry.openDctnry(); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );    
       j=255;
       for (int i=0; i<smallSize; i++)
       {
          if (j==0)
           j=255;           
          sgnature_value[i]=j;
          j--;       
       }
      //insert another signature value
      Token token2;
      rc = m_Dctnry.insertDctnry(dFile, smallSize, 
                                 sgnature_value, token2);                           
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( rc == NO_ERROR );
      CPPUNIT_ASSERT( token2.fbo == 0 );
      CPPUNIT_ASSERT( token2.op == 2 );
      m_Dctnry.closeDctnry(dFile);
      printf("After insert token 2 into dictionary \n");
       testGetBlockHdr();
      //delete 
       memset(&token,0, sizeof(token));
       rc = m_Dctnry.openDctnry(); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
       //token dose not exist in dictionary
       rc =m_Dctnry.deleteDctnryValue( dFile, token);
       CPPUNIT_ASSERT( rc == ERR_DICT_NO_OP_DELETE );
        
       //rc =m_Dctnry.deleteDctnryValue( dFile, token3);
       rc =m_Dctnry.deleteDctnryValue( dFile, token2);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       m_Dctnry.closeDctnry();
       printf("After delete dictionary token2 \n");
       testGetBlockHdr();
       //delete right after
       rc = m_Dctnry.openDctnry(); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
       //rc =m_Dctnry.deleteDctnryValue( dFile, token2);
       rc =m_Dctnry.deleteDctnryValue( dFile, token3);
       CPPUNIT_ASSERT( rc == NO_ERROR );

       //m_Dctnry.closeFile( dFile );
       m_Dctnry.closeDctnry();
       printf("After delete dictionary token3\n");
       testGetBlockHdr();
       rc = m_Dctnry.openDctnry(); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
       //rc =m_Dctnry.deleteDctnryValue( dFile, token2);
       rc =m_Dctnry.deleteDctnryValue( dFile, token3);
       CPPUNIT_ASSERT( rc == ERR_DICT_NO_OP_DELETE );
       rc =m_Dctnry.deleteDctnryValue( dFile, token2);
       CPPUNIT_ASSERT( rc == ERR_DICT_NO_OP_DELETE );
       //CPPUNIT_ASSERT( rc == ERR_DICT_ZERO_LEN );
       m_Dctnry.closeDctnry();
       return;
   }//testDctnryInsertDelete
   
void testDctnryInsertStress() {

       int                     oId=2002;
       int                     blockCount =2;      
       FILE*                   dFile =NULL;       
       int                     rc =0;
       DataBlock               curBlock;
       int                     smallSize=1000;       
       uint16_t                freeSpace;
       Offset                  hdrOffsets[4039];
       int                     opCount = 0;

       m_Dctnry.setDebugLevel( DEBUG_3);
       m_Dctnry.setUseSmallSize(true);
       m_oId = oId;
       memset(curBlock.data,0, sizeof (curBlock.data));
       memset(hdrOffsets,0, sizeof(hdrOffsets));
       printf("\nRunning testDctnryInsertStress \n");

       rc = m_Dctnry.deleteFile(oId);
       rc = m_Dctnry.createFile(oId,blockCount);
       
       dFile= m_Dctnry.openFile( oId );
       CPPUNIT_ASSERT( dFile != NULL );

       rc = m_Dctnry.initDctnryHdr( dFile);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       //m_Dctnry.closeFile(dFile);
       m_Dctnry.closeDctnry();  
       printf("After initDctnryHdr");
       testGetBlockHdr();
       dFile= m_Dctnry.openFile( oId);                 
       unsigned char sgnature_value[smallSize];
       Token token[100]; 
       int i,j,k;
       j=255;
       for (k=0; k<18; k++)
       {
         for (int i=0; i<smallSize; i++)
         {
          if (j==0)
           j=255;           
          sgnature_value[i]=k;
          j--;
       
         }// endfor i
         rc = m_Dctnry.insertDctnry(dFile, smallSize, 
                           sgnature_value, token[k]);
         if (rc != NO_ERROR)
         {
         
           printf ("k: %i Error Code is: %i \n",k, rc);
           //return;
         }
         if (k >15)
         {
          CPPUNIT_ASSERT( rc != NO_ERROR );
          printf("294. Error code is: %i \n", rc);
         }
         else
          CPPUNIT_ASSERT( rc == NO_ERROR );
         
      }//endfor k
       Offset prevOffset, curOffset;
       j=0;
       unsigned char* value = NULL;
       //This is to get the value out
       for (i=0; i<blockCount; i++)
       {         
        rc =m_Dctnry.readSubBlockEntry( dFile, &curBlock, i, 0, 0, 2, &freeSpace); 
        CPPUNIT_ASSERT( rc == NO_ERROR );
        j=0;
       
        prevOffset.hdrLoc =2+8; // 8 bytes
        memcpy(&(prevOffset.offset),&(curBlock.data[prevOffset.hdrLoc]),2);
        curOffset.hdrLoc = prevOffset.hdrLoc+2;
        memcpy(&(curOffset.offset),&(curBlock.data[curOffset.hdrLoc]),2);
        
        int op=1;
        int size = prevOffset.offset - curOffset.offset;
        
        value = (unsigned char*)malloc(sizeof(unsigned char)*size);
        memcpy(value, &curBlock.data[curOffset.offset], size );
        
        while (curOffset.offset!= 0xFFFF)
        {           
           //printf("fbo: %i op: %i starting offset: %i ending offset  %i size: %i  \n",
           //        i, op, curOffset.offset, prevOffset.offset, size);
                   
           //printf("value : ");
            for (k=0; k<size; k++)
            {
                  //printf("%u",value[k]);
            }
          // printf("\n");
           //start again
           free(value);
           value = NULL;
           prevOffset.hdrLoc = curOffset.hdrLoc;
           prevOffset.offset = curOffset.offset;          
           curOffset.hdrLoc+=2;          
           memcpy(&(curOffset.offset), &curBlock.data[curOffset.hdrLoc],2);          
           size = prevOffset.offset - curOffset.offset;
           if (curOffset.offset!= 0xFFFF)
           {
            value = (unsigned char*)malloc(sizeof(unsigned char)*size);
            memcpy(value, &curBlock.data[curOffset.offset], size );
           }
           op++;
           
        }//end while 
        
        //Get Offset info
        m_Dctnry.closeDctnry();
        printf("After insertDctnry");
        testGetBlockHdr();
        dFile= m_Dctnry.openFile( oId );
        CPPUNIT_ASSERT( dFile != NULL );
        m_Dctnry.getBlockHdr(dFile, i, opCount, hdrOffsets);
        int opCount2 =0;
              for (k=0; k< opCount; k++)
              {
                Token token;
                token.fbo = i;
                token.op = k+1;
                rc = m_Dctnry.findTokenValue(dFile, token,sgnature_value, size);
                CPPUNIT_ASSERT( rc == NO_ERROR);
                CPPUNIT_ASSERT( size == smallSize);
                 rc =m_Dctnry.deleteDctnryValue( dFile, token);
                 printf("After deleteDctnryValue fbo %i op %i\n", (int)i, k+1);
                 m_Dctnry.getBlockHdr(dFile, i, opCount2, hdrOffsets);
                  Offset startOffset, endOffset;
                  printf("Header Info for fbo: %i \n %i %llu ", i, (int)m_Dctnry.getFree(),
                                               m_Dctnry.getNextPtr());
           
                  endOffset.hdrLoc = 10;
                  endOffset.offset = 8192;
                  printf("%i ",endOffset.offset);  
                  for (int k1=0; k1< opCount2; k1++)
                  {
                        startOffset.hdrLoc = hdrOffsets[k1].hdrLoc;
                        startOffset.offset = hdrOffsets[k1].offset;
                        printf("%i ",startOffset.offset);                        
                  }//end for k1
                  
                  rc =m_Dctnry.deleteDctnryValue( dFile, token);
                  
                  printf("%x \n", 0xFFFF);  
                  printf("k-> %i i-> %i error code -> %i \n ", k, i, rc);
                  if (k<opCount-1)
                   CPPUNIT_ASSERT( rc == ERR_DICT_ZERO_LEN); 
                  else
                   CPPUNIT_ASSERT( rc == ERR_DICT_NO_OP_DELETE);                     
              }//end for k   
       } //end for i  
       //m_Dctnry.closeFile( dFile );
       char sigString[] = "Hello, I am a string; what are you? I am testing dictionary as a signature value, I don't know how long I am but we will find out in a minute"; 
       int sigStringSize = strlen(sigString);
       char resultString[sigStringSize+1];
       Token stringToken;
       rc = m_Dctnry.insertDctnry(dFile, sigStringSize, 
                           (unsigned char*)sigString, stringToken);
       rc = m_Dctnry.findTokenValue(dFile,stringToken, (unsigned char*)resultString,sigStringSize);
       resultString[sigStringSize] ='\0';
       printf("result String is %s  --->size is %i\n",resultString, strlen(resultString) );
       m_Dctnry.closeDctnry();
       printf("After insert the result string, the header look like the following:\n");
       testGetBlockHdr();
       return;
   }//testDctnryInsertStress

    void testGetBlockHdr() {

    int blockCount=2;
    int oId =2002;
    int i =0, k=0;
    FILE* dFile;
    int opCount;
    Offset hdrOffsets[4040];
      memset(hdrOffsets,0, sizeof(hdrOffsets));
       m_Dctnry.setDebugLevel( DEBUG_3 );
       
 
       printf("\nRunning testGetBlockHdr \n");
       
       dFile= m_Dctnry.openFile( m_oId );
       if (dFile==NULL)
         dFile= m_Dctnry.openFile( oId );
       CPPUNIT_ASSERT( dFile != NULL );
       blockCount = m_Dctnry.getFileSize( dFile )/BYTE_PER_BLOCK ;
       for (i=0; i<blockCount; i++)
       { 
           m_Dctnry.getBlockHdr(dFile, i, opCount, hdrOffsets);
           Offset startOffset, endOffset;
           int sigSize;
           printf("Header Info for fbo: %i \n %i %llu ", i, (int)m_Dctnry.getFree(),
                                               m_Dctnry.getNextPtr());
           //printf(" fbo %i  total offset number %i \n", i, opCount);
           endOffset.hdrLoc = 10;
           endOffset.offset = 8192;
           printf("%i ",endOffset.offset);  
           for (k=0; k< opCount; k++)
           {
                 startOffset.hdrLoc = hdrOffsets[k].hdrLoc;
                 startOffset.offset = hdrOffsets[k].offset;
                 sigSize = endOffset.offset - startOffset.offset;
                 
                 //printf("  OP %i signature size : %i \n from %i to %i \n", k+1,
                 //         sigSize, startOffset.offset,endOffset.offset  ); 
                 endOffset.hdrLoc = startOffset.hdrLoc ;
                 endOffset.offset = startOffset.offset; 
                 printf("%i ",endOffset.offset);                        
           }//end for k
           printf("%x \n", 0xFFFF);  
        }//endfor i
        m_Dctnry.closeDctnry();

    }
    void testGetFreeSpace() {
    int  oId =2002;  
    FILE* dFile;
    
       m_Dctnry.setDebugLevel( DEBUG_1 );
      
 
       printf("\nRunning testGetFreeSpace \n");       
       dFile= m_Dctnry.openFile( m_oId );
       if (dFile==NULL)
         dFile= m_Dctnry.openFile( oId );
       CPPUNIT_ASSERT( dFile != NULL );
       m_Dctnry.getFreeSpaceArray(dFile);
       for (int i=0; i<m_Dctnry.getNumBlocks(); i++)
       {
          printf("fbo  %i -->free space:%i \n", i, m_Dctnry.m_freeSpaceArray[i]);        
       }
        m_Dctnry.closeDctnry();
    }
    void testDctnryInsertDeleteStore() {
       int                     rc =0;
       int                     smallSize=1000; 
       FID                     dctnryOID =2002;
       FID                     treeOID = 101;
       FID                     listOID = 102;            
       m_DctnryStore.setDebugLevel( DEBUG_3 );
       printf("\nRunning testDctnryInsertDeleteStore \n");
       rc = m_DctnryStore.dropDctnryStore( treeOID, dctnryOID, listOID);
       //CPPUNIT_ASSERT( rc == NO_ERROR );
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
        rc = m_DctnryStore.dropDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
        rc = m_DctnryStore.dropDctnryStore( treeOID, dctnryOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
        rc = m_DctnryStore.dropDctnryStore();
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
             
       rc = m_DctnryStore.openDctnryStore(dctnryOID, treeOID, listOID );
       CPPUNIT_ASSERT( rc ==NO_ERROR );      
       
       m_DctnryStore.closeDctnryStore();
       CPPUNIT_ASSERT( rc ==NO_ERROR );

       rc = m_DctnryStore.openDctnryStore();
       CPPUNIT_ASSERT( rc ==NO_ERROR );      
             
       unsigned char sgnature_value[smallSize];
       Token token[100];
       memset(token, 0, sizeof(token));
       int i,j,k;
       for (int i=0; i<smallSize; i++)
       {
//          if (j>255)
//           j=0;           
          sgnature_value[i]=119;
 //         j++;       
       }
       //insert a signature value
	   for (k=1;k<12;k++) {         
 
		 rc = m_DctnryStore.updateDctnryStore(sgnature_value, smallSize,token[k]);
       printf("token fbo = %i",token[k].fbo);
       printf("token op = %i",token[k].op);
	   }
		 testGetBlockHdr();
		 return;		 

		 rc = m_DctnryStore.updateDctnryStore(sgnature_value, smallSize,token[1]);
	   testGetBlockHdr();


	   //Recheck if the tokens are all inserted
       for (k=1; k<99; k++)
       {
         for (i=0; i<smallSize; i++)
         {
            if (j==0)
                 j=255;  
		    if (k>10)
			     sgnature_value[i]=k;
		    else
		    {
			     if (i<8)
                    sgnature_value[i] =1;
			     else
                    sgnature_value[i]=k;
		    }
           j--;          
         }//endfor i ; second time
         rc = m_DctnryStore.updateDctnryStore(sgnature_value, 
                                              smallSize,token[k]); 
         if (rc!= NO_ERROR)
         {
			 printf("443. Attentione!!! ERROR CODE : %i \n", rc);        
         }
         CPPUNIT_ASSERT( rc == NO_ERROR );
       } //endof for k; second time
       
         int tempSize = 7;      
         rc = m_DctnryStore.updateDctnryStore(sgnature_value, 
                                               tempSize,token[k]);         
         if (rc!= NO_ERROR)
         {
             printf("452. Predicted Error Code should be 1363:  The result ERROR CODE : %i \n", rc);        
         }
         CPPUNIT_ASSERT( rc ==1363 ); 
         
         tempSize = 8;
         rc = m_DctnryStore.updateDctnryStore(sgnature_value, 
                                               tempSize,token[k]);         
         if (rc!= NO_ERROR)
         {
            printf("461. Attention!!! ERROR CODE : %i \n", rc);        
         }
         CPPUNIT_ASSERT( rc == NO_ERROR ); 
	 for (i=1; i<99; i++)
         {
		   if( m_DctnryStore.isDebug( DEBUG_3 )) 
		   {
		    printf("i : %i  token.fbo %i   token.op %i \n", i, (int)token[i].fbo, (int)token[i].op);
		   }
	}
               
        for (i=1; i<99; i++)
        {
         rc = m_DctnryStore.deleteDctnryToken(token[i]);
         
         if (rc!= NO_ERROR)
         {
          printf("475 . Attention!!! ERROR CODE : %i \n", rc);       
         }
         CPPUNIT_ASSERT( rc == NO_ERROR ); 
        }  
     
       m_DctnryStore.closeDctnryStore();
       //rc = m_DctnryStore.dropDctnryStore();
       //CPPUNIT_ASSERT( rc == NO_ERROR );
       return;
   }//testDctnryInsertDeleteStore
};

CPPUNIT_TEST_SUITE_REGISTRATION( DctnryTest );

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


