/****************************************************************
 * $Id$
 *
 ***************************************************************/

/** @file
 * Validataion tool for index validation
 *
 * This tool is to validate the index tree and list structure. It starts
 * from the index tree file, walk through the tree structure until it hits
 * a leaf node, then locates the index list block based on the leaf pointer.
 * It continues to get all the RIDs for that index key, and also goes to
 * the column OID file to validate the column value with the index key.
 */

#include <iostream>
#include <fstream>
#include <cassert>
#include <vector>
#include <algorithm>
#include <iterator>
using namespace std;

#include <unistd.h>

#include "bytestream.h"
using namespace messageqcpp;

#include "dmlpackageprocessor.h"
using namespace dmlpackageprocessor;

#include "writeengine.h"
#include "we_indextree.h"
using namespace WriteEngine;

#include "configcpp.h"
using namespace config;

#include "dm.h"

/** Debug macro */
#define _DEBUG 0
#if _DEBUG
#define DEBUG cout
#else
#define DEBUG if (0) cout
#endif

namespace {

const streamsize entrysize = sizeof(IdxBitTestEntry);
const streamsize subbloacksize = SUBBLOCK_TOTAL_BYTES;
const streamsize listHdrSize = sizeof(IdxRidListHdr);
uint32_t treeOID, listOID;
uint32_t colOID = 0;
uint32_t columnSize = 0;
ifstream indexTreeFile, indexListFile, columnFile;
bool vFlag = false;
bool nFlag = false;
int64_t keyNumber = 0;
FILE *pFile;
IndexList indexList;
u_int64_t keyvalue;
int  totalRids = 0;

void usage()
{
    cout << "evalidx [-h] -t OID -l OID [-v -c OID -b colSize -k keyvalue -n ]" << endl;
    cout << "\t-h display this help" << endl;
    cout << "\t-t OID index tree" << endl;
    cout << "\t-l OID index list" << endl;
    cout << "\t-v validate index value (need to go with -c and -b)" << endl;
    cout << "\t-c OID column" << endl;
    cout << "\t-b column size in number of byte (default = 4)" << endl;
    cout << "\t-k keyvalue to return index list header for this key" << endl;
    cout << "\t-n read RID from tree design" << endl;
}

int oid2file(uint32_t oid, string& filename)
{
//ITER17_Obsolete
// This code and this program is obsolete at this point since we are not
// currently supporting indexes.  This function and it's use of getFileName
// needs to be changed, if we ever resurrect this program, since getFileName
// now normally requires the DBRoot, partition, and segment number in
// addition to the OID.
#if 0
	FileOp fileOp;
	char file_name[WriteEngine::FILE_NAME_SIZE];
	if (fileOp.getFileName(oid, file_name) == WriteEngine::NO_ERROR)
	{
		filename = file_name;
		return 0;
	}
	else
	{
		cerr << "WriteEngine::FileOp::getFileName() error!" << endl;
		return -1;
	}
#endif
	return 0;
}

int validateValue(WriteEngine::RID rid, int64_t key)
{
    int64_t byteoffset = rid * columnSize;
    ByteStream::byte inbuf[columnSize];
    int64_t colVal = 0;
    
    columnFile.seekg(byteoffset, ios::beg);
    columnFile.read(reinterpret_cast<char*>(inbuf), columnSize);
    memcpy(&colVal, inbuf, columnSize);
    if (key != colVal)
    {
        cerr << "rowid:     " << rid << endl
             << "index:     " << key << endl
             << "column:    " << colVal << endl; 
        return 1;
    }
    return 0;
}

void walkBlock (streamsize byteoffset)
{                
    int64_t newByteoffset = 0;
    int fbo;
    int groupNo;
    ByteStream::byte inbuf[entrysize];
    ByteStream::byte listHdr[listHdrSize];  
    IdxBitTestEntry *entry; 
    IdxRidListHdr *hdrEntry;    
    IdxRidListHdrSize* hdrSize;  
    
    // get group number
    indexTreeFile.seekg(byteoffset, ios::beg);
    indexTreeFile.read(reinterpret_cast<char*>(inbuf), entrysize);
    if (indexTreeFile.eof()) return;
    entry = (IdxBitTestEntry *) inbuf;
    groupNo = entry->group;
        
    // continue to walk next stage if not leaf node for each entry in the group
    for (int i = 0; i < 1 << groupNo; i++)
    {   
        indexTreeFile.seekg(byteoffset, ios::beg);
        indexTreeFile.read(reinterpret_cast<char*>(inbuf), entrysize);
        if (indexTreeFile.eof()) return;
        entry = (IdxBitTestEntry *) inbuf;
        byteoffset += entrysize;

        DEBUG << ": fbo=" << (int)entry->fbo << 
            " sbid=" << entry->sbid << " sbentry=" << entry->entry <<
            " group=" << entry->group << " bittest=" << entry->bitTest <<
            " type=" << entry->type << endl;        
        
	if (entry->type == WriteEngine::EMPTY_ENTRY || 
	    entry->type == WriteEngine::EMPTY_LIST || 
	    entry->type == WriteEngine::EMPTY_PTR) 
            continue; 
        
        // convert lbid to real fob number
        uint16_t dbRoot;
        uint32_t partition;
        uint16_t segment;
        BRMWrapper::getInstance()->getFboOffset(entry->fbo, dbRoot, partition, segment, fbo);
        newByteoffset = ((int64_t)fbo)*BLOCK_SIZE+entry->sbid*subbloacksize+entry->entry*entrysize;
        
        if (entry->type > 6)
        {
            cerr << "invalid type= " << entry->type << endl;
            cerr << "fbo= " << fbo << " sbid= " << entry->sbid << " entry= " << entry->entry << endl;
            throw runtime_error("invalid type of tree block");
        }
            
        // stop walking index tree if leaf node. go walk index list	then
        if (entry->type == LEAF_LIST)
        {   
            keyNumber++;
            IdxEmptyListEntry listPtr;
            int size, rc;
            CommBlock cbList;
            
            listPtr.fbo = entry->fbo;
            listPtr.sbid = entry->sbid;
            listPtr.entry = entry->entry;
            
            indexListFile.seekg(newByteoffset, ios::beg); 
            indexListFile.read(reinterpret_cast<char*>(listHdr), listHdrSize);
            hdrEntry = reinterpret_cast<IdxRidListHdr*>(listHdr);
            hdrSize = reinterpret_cast<IdxRidListHdrSize*>(listHdr);
            DEBUG << "\nkey= " << hdrEntry->key 
                 << " rowsize= " << hdrSize->size;    	    
            
            // add feather for Jean. print out list header for a given key value
            if (keyvalue == hdrEntry->key)
            {
                cerr << "fbo= " << listPtr.fbo 
                     << " sbid= " << listPtr.sbid 
                     << " entry= " << listPtr.entry 
                     << " key : " << keyvalue << endl;
            }
            
            cbList.file.oid = listOID; 
            cbList.file.pFile = pFile;
	    //WriteEngine::RID ridArray[MAX_BLOCK_ENTRY*10];
            int rSize = 0;
            rSize = hdrSize->size;
            WriteEngine::RID* ridArray = new WriteEngine::RID[rSize];
            size = 0;
            if (!nFlag)
             rc = indexList.getRIDArrayFromListHdr(cbList, hdrEntry->key, &listPtr, ridArray, size);
            else
             rc = indexList.getRIDArrayFromListHdrNarray(cbList, hdrEntry->key, &listPtr, ridArray, size, true);
            totalRids = totalRids + size;
            if (rc)
            {
                cerr << "Get RID array failed for index block: " << rc << endl;
                cerr << "new byte offset= " << newByteoffset << endl;
                cerr << "file good? " << indexListFile.good() << endl;
                cerr << "fbo= " << listPtr.fbo 
                     << " sbid= " << listPtr.sbid 
                     << " entry= " << listPtr.entry << endl;
                for (int64_t j = 0; j < size; j++)
                    cerr << " " << ridArray[j] << endl;
                throw runtime_error("Get RID array failed");
            }
            
            if (hdrSize->size != static_cast<unsigned int>(size))
            {
                cerr << "row size not match with list header" << endl;
                cerr << "fbo= " << listPtr.fbo 
                     << " sbid= " << listPtr.sbid 
                     << " entry= " << listPtr.entry << endl;
                for (int64_t j = 0; j < size; j++)
                    cerr << " " << ridArray[j] << endl;
                throw runtime_error("row size not match with list header");                    
            }
            
            for (int64_t j = 0; j < size; j++)
            {
                DEBUG << " " << ridArray[j] << endl;

                // validate column value with the index value
                if (vFlag)
                    idbassert(validateValue(ridArray[j], hdrEntry->key) == 0);
            }
           delete [] ridArray;
        }
        else
            walkBlock(newByteoffset);	    	    
    }    
    
}

}

int main(int argc, char* argv[])
{     
    int c;
    int i;
    string filename;   
       
    while ((c = getopt(argc, argv, "ntlhbcvk")) != EOF)
        switch (c)
        {
        case 't':
            treeOID = atoi(argv[optind]);
            if (oid2file(treeOID, filename)) return 1;
            DEBUG << "tree: " << filename << endl;
            indexTreeFile.open(filename.c_str());
            break;
        case 'l':
            listOID = atoi(argv[optind]);
            if (oid2file(listOID, filename)) return 1;
            DEBUG << "list: " << filename << endl;
                        
            indexListFile.open(filename.c_str());
            pFile = fopen(filename.c_str(), "rb");
            if (!pFile) 
            {
                cerr << "Invalid OID " << listOID << " for index list" << endl;
                exit(1);
            }
            break;
        case 'v':
            vFlag = true;
            break;
        case 'c':
            colOID = atoi(argv[optind]);
            if (oid2file(colOID, filename)) return 1;
            DEBUG << "column: " << filename << endl;
            columnFile.open(filename.c_str());
            break;
        case 'b':
            columnSize = atoi(argv[optind]);
            break;		    
        case 'k':
            keyvalue = atoi(argv[optind]);
            break;
        case 'h':
            usage();
            return 0;
            break;		    
        case 'n':
            nFlag = true;
            break; 
        default:
            usage();
            return 1;
            break;
        }
        
    if ((argc - optind) < 1)
    {
        usage();
        return 1;
    }
    
    if (argc < 5)
    {
        usage();
        return 1;
    }
    
    if (vFlag && (colOID == 0 || columnSize == 0))
    {
        cerr << "Please provide both -c and -b option if -v is indicated." << endl;
        usage();
        return 1;
    }
    
    if (vFlag && !columnFile.good())
    {
        cerr << "Bad column OID" << endl;
        return 1;
    }
        
    if (!indexTreeFile.good() || !indexListFile.good())
    {
        cerr << "Bad index OIDs" << endl;
        return 1;
    }   
    
   
    // walk through the index tree file   
    for (i = 0; i < 32; i++)
        walkBlock (0 * BLOCK_SIZE + 1 * subbloacksize + i * entrysize);
    
    cout << "\n" << keyNumber << " index value validated!" << endl;
    cout << "Total RIDs for this column=" << totalRids << endl;
    indexListFile.close();
    indexTreeFile.close();
    fclose(pFile);
    return 0;
}

