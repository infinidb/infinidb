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
* $Id: we_dctnrystore.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/**
 *  Wrapper around the Dctnry class used to maintain a dictionary store file.
 *  When signature(s) are specified to the update functions, the value(s) are
 *  stored in the dictionary, and token(s) are returned to denote the location
 *  of the signature(s).  The caller is then responsible for storing the
 *  token(s) in the corresponding segment column file.
 *   
 *  References to tree and list OIDs are not currently pertinent, and may be
 *  removed at a later time.
 *
 *  A deleteDctnryToken function is provided for possible future use to
 *  delete tokens.  But this function is not currently used.
 */

#ifndef _WE_DctnryStore_H_
#define _WE_DctnryStore_H_

#include <string>

#include "we_dbfileop.h"
#include "we_type.h"
#include "we_dctnry.h"

#if defined(_MSC_VER) && defined(WRITEENGINEDCTSTORE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/**@brief Namespace WriteEngine */

namespace WriteEngine
{
/**@brief class  DctnryStore
 *    A wrapper class for dictionary
 */

class DctnryStore : public DbFileOp
{
public:
   /**
    * @brief Constructor
    */
    EXPORT DctnryStore();

   /**
    * @brief Destructor
    */
    EXPORT ~DctnryStore();

   /**
    * @brief Close a dictionary store after it has been opened.
    */
    EXPORT int closeDctnryStore(){return m_dctnry.closeDctnry();}

   /**
    * @brief Close a dictionary store without flushing the block buffer or
    * updating BRM with HWM.  Intended to be used for immediate file closure
    * to shut down a job that has encountered an error, and intends to do
    * a rollback.
    */
    EXPORT int closeDctnryStoreOnly(){return m_dctnry.closeDctnryOnly();}
                                 
    /**
    * @brief create dictionary store
    *
    * @param dctnryOID - dictionary file OID
    * @param treeOID   - index tree OID (not used)
    * @param listOID   - list OID       (not used)
    * @param colWidth  - column width
    * @param dbRoot    - DBRoot for store file
    * @param partition - partition number for store file
    * @param segment   - column segment number for store file
    */
    EXPORT const int createDctnryStore(
                        const OID& dctnryOID, const OID& treeOID, 
                        const OID& listOID, int colWidth, const uint16_t dbRoot,
                        const uint32_t partition, const uint16_t segment );

   /**
    * @brief Delete a token from dictionary store, for maintanance use
    *
    * @param token     - token to be deleted
    */
    EXPORT const int  deleteDctnryToken(Token& token);

   /**
    * @brief Drop dictionary store (for DDL/DML use)
    *
    * @param dctnryOID - dictionary file OID
    * @param treeOID   - index tree OID (not used)
    * @param listOID   - list OID       (not used)
    */
    EXPORT const int dropDctnryStore( const OID& dctnryOID, const OID& treeOID, 
                        const OID& listOID);
   /**
    * @brief Open a dictionary store after creation
    *
    * @param dctnryOID - dictionary file OID
    * @param treeOID   - index tree OID (not used)
    * @param listOID   - list OID       (not used)
    * @param dbRoot    - DBRoot for store file
    * @param partition - partition number for store file
    * @param segment   - column segment number for store file
    */
    EXPORT const int  openDctnryStore(const OID& dctnryOID, const OID& treeOID,
                        const OID& listOID,
                        const uint16_t dbRoot,
                        const uint32_t partition,
                        const uint16_t segment);

   /**
    * @brief Update dictionary store with tokenized strings (for DDL/DML use)
    *
    * @param sigValue  - signature value
    * @param sigSize   - signature size
    * @param token     - (output) token that was added
    */              
    EXPORT const int  updateDctnryStore(unsigned char* sigValue, 
                        int& sigSize, Token& token); 

   /**
    * @brief Update dictionary store with tokenized strings (for Bulk use)
    *
    * @param buf       - bulk buffer containing strings to be parsed
    * @param pos       - list of offsets into buf
    * @param totalRow  - total number of rows in buf
    * @param col       - the column to be parsed from buf
    * @param colWidth  - width of the dictionary column being parsed
    * @param tokenBuf  - (output) list of tokens for the parsed strings
    */              
    const int  updateDctnryStore(const char*  buf, 
                        ColPosPair ** pos,
                        const int totalRow,
                        const int col, 
                        const int colWidth, 
                        char*     tokenBuf) 
          { return(m_dctnry.insertDctnry(
            buf, pos, totalRow, col, colWidth, tokenBuf)); }

   /**
    * @brief TransId related function
    *
    * @param transId   - Current transaction id (for DDL/DML use)
    */
    void  setAllTransId(const TxnID& transId){m_dctnry.setTransId(transId);}

   /**                                               
    * @brief String cache related routines
    */
    void  clearMap() { m_dctnry.clearMap();  }
    void  createMap(){ m_dctnry.createMap(); }
    void  setUseHashMap(bool flag)
          { m_hashMapFlag = flag;
            m_dctnry.setUseHashMap(flag); }
    void  setHashMapSize(int size)
          { if (size < MAX_HASHMAP_SIZE) 
                m_hashMapSize = size;
            else
                m_hashMapSize = MAX_HASHMAP_SIZE;
            m_dctnry.setHashMapSize(m_hashMapSize); }

    HWM   getHWM()                   const { return m_dctnry.getHWM(); }
    const std::string& getFileName() const { return m_dctnry.getFileName();}

   /**
    * @brief public instance
    */
    Dctnry        m_dctnry;

private:                            
    // Used to configure string cache usage
    bool          m_hashMapFlag;
    int           m_hashMapSize;
};

} //end of namespace

#undef EXPORT

#endif // _WE_DctnryStore_H_
