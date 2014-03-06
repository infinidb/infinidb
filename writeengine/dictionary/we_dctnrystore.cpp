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

/*******************************************************************************
* $Id: we_dctnrystore.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
#include <string.h>
#ifndef _MSC_VER
#include <inttypes.h>
#endif
#define WRITEENGINEDCTSTORE_DLLEXPORT
#include "we_dctnrystore.h"
#undef WRITEENGINEDCTSTORE_DLLEXPORT

namespace WriteEngine
{  

/***********************************************************
 * Constructor
 ***********************************************************/ 
DctnryStore::DctnryStore() 
    :m_hashMapFlag(true), m_hashMapSize(MAX_HASHMAP_SIZE)
{
    m_dctnry.setUseHashMap(m_hashMapFlag);
}

/***********************************************************
 * Destructor
 ***********************************************************/ 
DctnryStore::~DctnryStore()
{
} 

/***********************************************************
 * Open a dictionary store file
 ***********************************************************/ 
const int DctnryStore::openDctnryStore(const OID& dctnryOID, 
        const OID&     treeOID,
        const OID&     listOID,
        const uint16_t dbRoot,
        const uint32_t partition,
        const uint16_t segment)
{
    int rc2;
    rc2 = m_dctnry.openDctnry(dctnryOID, dbRoot, partition, segment);
    setUseHashMap(true);

    return rc2;
}

/***********************************************************
 * Create a dictionary store file
 ***********************************************************/    
const int  DctnryStore::createDctnryStore( const OID& dctnryOID, 
        const OID& treeOID, const OID& listOID, int colWidth, const uint16_t dbRoot,
        const uint32_t partition, const uint16_t segment )
{ 
    int rc2 ;
 
    rc2 = m_dctnry.createDctnry(dctnryOID, colWidth, dbRoot, partition, segment);
      
    return rc2;
}

/***********************************************************
 * Drop a dictionary store file
 ***********************************************************/            
const int  DctnryStore::dropDctnryStore( const OID& dctnryOID, 
                                         const OID& treeOID,
                                         const OID& listOID)
{
    int  rc2;
    rc2 = m_dctnry.dropDctnry(dctnryOID);

    return rc2;                                
}

/***********************************************************
 * Add a signature value to the dictionary store.
 * Function first checks to see if the signature is already
 * in our string cache, and returns the corresponding token
 * if it is found in the cache.
 ***********************************************************/    
const int  DctnryStore::updateDctnryStore(unsigned char* sigValue, 
                                          int& sigSize,
                                          Token& token)
{
    int rc = NO_ERROR;
    Signature sig;
    sig.signature = sigValue;
    sig.size = sigSize; 

    //if String cache is enabled then look for string in cache
    if (m_hashMapFlag) 
    {
        if (m_dctnry.m_arraySize < (int)m_hashMapSize)
        {
            bool found = false;
            found = m_dctnry.getTokenFromArray(sig);
            if (found)
            {
                token = sig.token;
                return NO_ERROR;
            }
        } //end if use hash map and size >0
    }
       
    //Insert into Dictionary
    rc = m_dctnry.insertDctnry(sigSize, sigValue, token);              

    //Add the new signature and token into cache if the hashmap flag is on
    // (We currently use an array instead of a hashmap.)
    if ((m_hashMapFlag) && (m_dctnry.m_arraySize < (int)m_hashMapSize))
    {
        Signature sig;
        sig.size = sigSize;
        sig.signature = new unsigned char[sigSize];
        memcpy (sig.signature, sigValue, sigSize);
        sig.token = token;
        m_dctnry.m_sigArray[m_dctnry.m_arraySize]=sig;
        m_dctnry.m_arraySize++;
    }

    return rc;
}  

/***********************************************************
 * Delete signature from the dictionary store file
 ***********************************************************/        
const int  DctnryStore::deleteDctnryToken(Token& token) 
{
    int rc ;
    int sigSize;
    unsigned char* sigValue = NULL;

    rc = m_dctnry.deleteDctnryValue( token, sigSize, &sigValue);
    if (rc!=NO_ERROR)
    {
        return rc;
    }

    if (!sigValue)
    {
        rc = ERR_DICT_TOKEN_NOT_FOUND;
        return rc;
    }

    free(sigValue);

    return rc;  
}                                                                     

} //end of namespace
