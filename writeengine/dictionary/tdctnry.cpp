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
#include <cmath>

//using namespace dataconvert;
using namespace WriteEngine;

class DctnryTest : public CppUnit::TestFixture {


CPPUNIT_TEST_SUITE( DctnryTest );

// Dctnry basic testing
CPPUNIT_TEST( testDctnryInsertDelete );
CPPUNIT_TEST( testDctnryInsertDeleteStore);
CPPUNIT_TEST( testDctnryInsertStressStore);
CPPUNIT_TEST(testDctnryDropCreate);
CPPUNIT_TEST(testDctnryOpen);
CPPUNIT_TEST( testDctnryMultipleInsert);

CPPUNIT_TEST_SUITE_END();

private:
   Dctnry                    m_Dctnry;
   DctnryStore               m_DctnryStore;
   int                       m_oId;
public:
	void setUp() {
   BRMWrapper::setUseVb(false);
	}

	void tearDown() {
	
	}
 DctnryTest()
 {
    m_oId =-1;
 } 

void testDctnryInsertDelete() {
       int                     oId=2001;       
       FILE*                   dFile =NULL;      
       int                     rc =0;
       DataBlock               curBlock;
       int                     largeSize=9000;
       int                     smallSize=1000;

       unsigned char dctnryHeader[DCTNRY_HEADER_SIZE];        
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

       rc = m_Dctnry.dropDctnry(oId);       
       rc = m_Dctnry.createDctnry(oId,10);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       rc = m_Dctnry.openDctnry();
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );     
       //12 for 6 bytes and 14 for 8 bytes  
       uint64_t lbid =0; 
       BRMWrapper::getInstance()->getBrmInfo( oId, 0, lbid ); 
       printf("passed brm above \n");
       rc =m_Dctnry.readSubBlockEntry( dFile, &curBlock, lbid, 0, 0, DCTNRY_HEADER_SIZE, &dctnryHeader); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       
       memcpy(&freeSpace,dctnryHeader,HDR_UNIT_SIZE);
       memcpy(&nextPtr,dctnryHeader+HDR_UNIT_SIZE,NEXT_PTR_BYTES); // 8 bytes
       memcpy(&offSet0,dctnryHeader+NEXT_PTR_BYTES+HDR_UNIT_SIZE, HDR_UNIT_SIZE);
       memcpy(&endHeader,dctnryHeader+NEXT_PTR_BYTES+HDR_UNIT_SIZE+HDR_UNIT_SIZE,HDR_UNIT_SIZE);   

       unsigned char sgnature_value[largeSize];
       memset(sgnature_value,0, sizeof(sgnature_value));
       int j=0;
       //j is a 1 byte character;
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
       largeSize = 4000;
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
       cout<<"rc=" << rc << endl;
       CPPUNIT_ASSERT( rc == NO_ERROR );
       int fboCnt;
       BRMWrapper::getInstance()->getFboOffset( token3.fbo, fboCnt);
       CPPUNIT_ASSERT( fboCnt == 0 );
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
      
      BRMWrapper::getInstance()->getFboOffset( token2.fbo, fboCnt);
      CPPUNIT_ASSERT( fboCnt == 0 );
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
       CPPUNIT_ASSERT( rc == ERR_DICT_BAD_TOKEN_OP );
        
       
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
       unsigned char sigValue3[8000];
       int sigSize3;
       rc =m_Dctnry.findTokenValue (dFile, token3, 
                           sigValue3, 
                           sigSize3 );
       CPPUNIT_ASSERT(sigSize3==4000);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       rc =m_Dctnry.deleteDctnryValue( dFile, token3);
       CPPUNIT_ASSERT( rc == NO_ERROR );

       m_Dctnry.closeDctnry();
       printf("After delete dictionary token3\n");
       testGetBlockHdr();
       rc = m_Dctnry.openDctnry(); 
       CPPUNIT_ASSERT( rc == NO_ERROR );
       dFile = m_Dctnry.getDctnryFile();
       CPPUNIT_ASSERT( dFile != NULL );
        rc =m_Dctnry.deleteDctnryValue( dFile, token3);
       CPPUNIT_ASSERT( rc == ERR_DICT_BAD_TOKEN_OP );
       rc =m_Dctnry.deleteDctnryValue( dFile, token2);
       CPPUNIT_ASSERT( rc == ERR_DICT_BAD_TOKEN_OP );
       m_Dctnry.closeDctnry();
       printf("\nSuccessfully Running testDctnryInsertDelete \n");
       return;
   }//testDctnryInsertDelete
   

   void testGetBlockHdr() {
      int blockCount=2;  
      int i =0, k=0;
      FILE* dFile;
      int opCount;
      int rc;
   
      Offset hdrOffsets[1024];
      memset(hdrOffsets,0, sizeof(hdrOffsets));
      m_Dctnry.setDebugLevel( DEBUG_3 );
       
        
      printf("\nRunning testGetBlockHdr \n");
      rc = m_Dctnry.openDctnry( m_oId );
       
      CPPUNIT_ASSERT( rc ==NO_ERROR );
      dFile = m_Dctnry.getDctnryFile(); 
      blockCount = m_Dctnry.getFileSize( dFile )/BYTE_PER_BLOCK ;
      for (i=0; i<blockCount; i++)
      { 
           int freeSpace;
           freeSpace = (int)m_Dctnry.getFree();
           if (freeSpace == 8178)
            break;
           printf("Header Info for fbo: %i %i %llu ", i, freeSpace,
                                               m_Dctnry.getNextPtr());
           m_Dctnry.getBlockHdr(dFile, i, opCount, hdrOffsets);
           if (opCount ==-1)
           {
              printf("ERROR BAD DICITONARY FILE|n");
              return;
           }
           Offset startOffset, endOffset;
           int sigSize;
           
           printf(" fbo %i  total offset number %i \n", i, opCount);
           endOffset.hdrLoc = 10;
           endOffset.offset = 8192;
           
           for (k=0; k< opCount; k++)
           {
                 startOffset.hdrLoc = hdrOffsets[k].hdrLoc;
                 startOffset.offset = hdrOffsets[k].offset;
                 sigSize = endOffset.offset - startOffset.offset;
                 
                 printf("  OP %i signature size : %i \n from %i to %i \n", k+1,
                          sigSize, startOffset.offset,endOffset.offset  ); 
                 endOffset.hdrLoc = startOffset.hdrLoc ;
                 endOffset.offset = startOffset.offset; 
                 printf("%i ",endOffset.offset);                        
           }//end for k
           //printf("%x \n", 0xFFFF);  
        }//endfor i
        m_Dctnry.closeDctnry();
      printf("\nSuccessfully Running testGetBlockHdr \n");
      return;
    }
   void testDctnryDropCreate() {
       int                     rc =0;
       FID                     dctnryOID =700;
       FID                     treeOID = 101;
       FID                     listOID = 102; 
                 
       m_DctnryStore.setDebugLevel( DEBUG_3 );
        
       printf("\nRunning testDctnryDropCreate \n");
       
       rc = m_DctnryStore.dropDctnryStore( dctnryOID, treeOID, listOID);
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       rc = m_DctnryStore.openDctnryStore(dctnryOID, treeOID, listOID );
       CPPUNIT_ASSERT( rc ==NO_ERROR ); 
       m_DctnryStore.closeDctnryStore();
          
       return;    
   }

   void testDctnryOpen() {
       int                     rc =0;
       FID                     dctnryOID =700;
       FID                     treeOID = 101;
       FID                     listOID = 102; 
                 
       m_DctnryStore.setDebugLevel( DEBUG_3 );
        
       printf("\nRunning testDctnryOpen \n");
       
       rc = m_DctnryStore.openDctnryStore(dctnryOID, treeOID, listOID );
       CPPUNIT_ASSERT( rc ==NO_ERROR ); 
       m_DctnryStore.closeDctnryStore();
        
       return;     
   }
    void testDctnryInsertDeleteStore() {
       int                     rc =0;
       int                     smallSize=1000; 
       FID                     dctnryOID =100;
       FID                     treeOID = 101;
       FID                     listOID = 102; 
       Token                   token2[100];
                 
       m_DctnryStore.setDebugLevel( DEBUG_3 );
        
       printf("\nRunning testDctnryInsertDeleteStore \n");
       printf("dropDctnryStore\n");
       rc = m_DctnryStore.dropDctnryStore( dctnryOID, treeOID, listOID);
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       rc = m_DctnryStore.openDctnryStore(dctnryOID, treeOID, listOID );
       CPPUNIT_ASSERT( rc ==NO_ERROR ); 
             
       unsigned char sgnature_value[smallSize];
       Token token[500];
       memset(token, 0, sizeof(token));
       int i,j,k;
       j=255;
       for (k=1; k<499; k++)
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
          }//endfor i
          rc = m_DctnryStore.updateDctnryStore(sgnature_value, 
                                              smallSize,token[k]);
          if (rc!= NO_ERROR)
            printf("rc %i k %i \n", rc, k);
          CPPUNIT_ASSERT( rc == NO_ERROR ); 
       } //endof for k
       printf("first time inserted %i values token\n", k);
	   //Recheck if the tokens are all inserted
	      memset(token2, 0, sizeof(token2)); 
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
                                              smallSize,token2[k]); 
          if (rc!= NO_ERROR)
          {
			          printf("443. Attentione!!! ERROR CODE : %i \n", rc);        
          }
          CPPUNIT_ASSERT( rc == NO_ERROR );
       } //endof for k; second time
      printf("second time inserted %i values token\n", k);   
         int tempSize = 7;      
         rc = m_DctnryStore.updateDctnryStore(sgnature_value, 
                                               tempSize,token2[k]);         
         if (rc!= NO_ERROR)
         {
             printf("452. Predicted Error Code should be 1363:  The result ERROR CODE : %i \n", rc);        
         }
         CPPUNIT_ASSERT( rc ==NO_ERROR ); 
         
         tempSize = 8;
         rc = m_DctnryStore.updateDctnryStore(sgnature_value, 
                                               tempSize,token2[k]);         
         if (rc!= NO_ERROR)
         {
            printf("461. Attention!!! ERROR CODE : %i \n", rc);        
         }
         CPPUNIT_ASSERT( rc == NO_ERROR ); 
	 for (i=1; i<99; i++)
         {
	          printf("first time->i : %i  token.fbo %i   token.op %i \n", i, (int)token[i].fbo, 
                  (int)token[i].op);
	 }
	
         cout<< "finished printing the tokens"    << endl;  
         cout<< "start deleting the tokens"    << endl;
         for (i=1; i<99; i++)
         {
          cout<< "start deleting the tokens i="  << i  << endl;
 
          rc = m_DctnryStore.deleteDctnryToken(token[i]);
         
          if (rc!= NO_ERROR)
          {
               printf("475 . Attention!!! ERROR CODE : %i \n", rc);       
          }
          CPPUNIT_ASSERT( rc == NO_ERROR ); 
         }  
         cout<< "finish deleting the tokens"    << endl;
         m_DctnryStore.closeDctnryStore();
         printf("\nSuccessfully Running testDctnryInsertDelete \n");
         return;
   }//testDctnryInsertDeleteStore
    void testDctnryInsertStressStore() {
       int                     rc =0;
       
       FID                     dctnryOID =897;
       FID                     treeOID = 101;
       FID                     listOID = 102; 
       int                     count = 5000;    
       Token stringToken;
       string msg;
       string   timeStr;
       
       Token pToken;
       int sigStringSize;
              
       m_DctnryStore.setDebugLevel( DEBUG_3 );
       m_DctnryStore.setUseHashMap(true);
        
       printf("\nRunning testDctnryInsertStressStore \n");
              
       rc = m_DctnryStore.dropDctnryStore( dctnryOID, treeOID, listOID);
       cout << "m_DctnryStore.dropDctnryStore error code=" << rc << " dctnryOID=" << dctnryOID << " treeOID=" << treeOID << " listOID=" << listOID << endl;
       rc = m_DctnryStore.createDctnryStore( dctnryOID, treeOID, listOID);
       cout << "m_DctnryStore.createDctnryStore error code=" << rc << " dctnryOID=" << dctnryOID << " treeOID=" << treeOID << " listOID=" << listOID << endl;
       cout << " I am here " << endl;
       CPPUNIT_ASSERT( rc == NO_ERROR );
             
       rc = m_DctnryStore.openDctnryStore(dctnryOID, treeOID, listOID );
       CPPUNIT_ASSERT( rc ==NO_ERROR );             
        
       char insertString[] = "Hello, I am a string; what are you? I am testing dictionary as a signature value, I don't know how long I am but we will find out in a minute"; 
       char sigString[500];
       memset(sigString,'\0', 500);
      
       for (int i=0; i<count ; i++)
       {
           sprintf(sigString, "%d%s", i, insertString); 
           //sprintf(sigString, "%s%d", insertString, i); 
           sigStringSize = strlen(sigString); 
       
           m_DctnryStore.setAllTransId(10);
           rc = m_DctnryStore.updateDctnryStore((unsigned char*)sigString, 
                                                 sigStringSize,
                                                 stringToken);
         cout << "stringToken token->fbo " << stringToken.fbo << " op->" << stringToken.op << endl;
       }
      
      for (int i=0; i<count ; i++)
      {
         sprintf(sigString, "%d%s", i, insertString); 
         sigStringSize = strlen(sigString); 	      
         m_DctnryStore.setAllTransId(10);
         rc = m_DctnryStore.updateDctnryStore((unsigned char*)sigString, 
                                               sigStringSize,
                                               pToken);                                                 
         cout << "pToken token->fbo " << pToken.fbo << " op->" << pToken.op << endl;
      }  
      m_DctnryStore.clearMap();
      m_DctnryStore.closeDctnryStore();
      printf("\nSuccessfully Running testDctnryInsertStressStore \n");
      return;
   }//testDctnryInsertStressStore

              
             
void testDctnryMultipleInsert() {
       int                     rc =0;
       FID                     dctnryOID =200;
       int                     totalsize = 1;
       int                     size = 18;//3,18
       int                     letterSize=2;//27,2
       int                     mapSize=1000;
       //char                  base[]={'b', 'b','c','d','e','f','g','h','i','j','k','l',
       //                              'm','n','o','p','q','r','s','t','u','v','w','x','y','z',' '};
                                       
                  
   
       m_DctnryStore.setDebugLevel( DEBUG_3 );
        
       printf("\nRunning testDctnryMultipleInsert \n");
       rc = m_Dctnry.dropDctnry( dctnryOID);
       rc = m_Dctnry.createDctnry( dctnryOID);
       CPPUNIT_ASSERT( rc == NO_ERROR );
       rc = m_Dctnry.openDctnry(dctnryOID);
       CPPUNIT_ASSERT( rc ==NO_ERROR ); 
                             
       for (int i = 0; i<(size); i++)
       {
           totalsize = totalsize*letterSize;
       }
       cout<<"totalsize=" << totalsize << endl;
   // There will not be a leak if the following throws an exception:
  
       int  totalRow = totalsize;       
       char buf[size+1];
       ColPosPair** pos = new ColPosPair*[totalRow];
       int loc =0;
       //ColPosPair* pos[totalRow];    
       for (int i = 0; i<totalRow; i++)
       {
          
          pos[i]= new ColPosPair[1];
          pos[i][0].start = loc;
          pos[i][0].offset = size;
          loc=loc+size+1;
       }
       int i=0;
       /*
       while (i<totalRow)
       {
         memset(buf[i], '\0', size+1);
         
         for (int j=0; j< letterSize; j++)
         {
              for (int k=0; k < letterSize; k++)
              {
                   for (int l=0; l < letterSize; l++)
                   {
                       buf[i][0]= base[j]; 
                       buf[i][1]= base[k]; 
                       buf[i][2]= base[l];
                       buf[i][3]='|';
                       char tmp[5];
                       memset(tmp,'\0',5);
                       strncpy(tmp, buf[i],4);
                       cout << "i=" << i << " buf[i]=" << tmp<< endl;
                       i++;
                   }
              }
         }
       }
       */
       int totalUnitSize[size];
       for (int i=0; i< size; i++)
       {
          totalUnitSize[i]= (int)pow((double)letterSize, (double)(size-i-1));
          //cout << "totalUnitSize[i]" << totalUnitSize[i] << endl;
       }
       i=0;
       int tmpsize=100000;
       char buf2[tmpsize*(size+1)];
       while (i<tmpsize)
       {
         //memset(buf[i], '\0', size+1);
         for (int m=0; m<size ; m++)
         {
              //cout << m<<"totalUnitSize[m]" << totalUnitSize[m] << endl;
              int index;
              int level=i/totalUnitSize[m];
              index=(level%letterSize);
              //cout <<"index=" << index <<endl;
              //buf[i][m]= base[index] ;
              buf[m]='c';
         }
         buf[size]='|';
         memcpy(buf2+i*(size+1), buf, size+1);
         /*
         char tmp[size+1];
         memset(tmp,'\0',size+1);
         strncpy(tmp, buf[i],size);
         cout << "i=" << i << " buf[i]=" << tmp<< endl;   
         */      
         i++;
       }

       int rowStatus[tmpsize];
       char tokenBuf[tmpsize*8];
       mapSize=1000;
       m_Dctnry.setHashMapSize(mapSize);
       m_Dctnry.startfTimer();
       
       rc = m_Dctnry.insertDctnry((const char*)buf2,  
                                   (ColPosPair**)pos, 
                                   (const int)tmpsize, 
                                    0, 
                                    (int*)rowStatus, 
                                    (char*)tokenBuf);
       
       m_Dctnry.stopfTimer();
       cout <<"total time in mlsec=" << m_Dctnry.getTotalfRunTime() << endl;
       m_Dctnry.closeDctnry();
       for (int i = totalRow; i > 0; --i)
       {
           //cout <<"i-1="<< i-1 << endl;
           delete[] pos[i-1];
          // delete [] buf[i-1];
       }
       delete[] pos;
       //delete [] buf;

   }     
             
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


