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

/******************************************************************************************
* $Id: we_semop.h 33 2006-08-31 14:35:14Z wzhou $
*
******************************************************************************************/
/** @file */

#ifndef _WE_SEMOP_H_
#define _WE_SEMOP_H_
#include <stdio.h>
#include <ctype.h>
#include <stdlib.h>
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>

#include <we_obj.h>

/** Namespace WriteEngine */
namespace WriteEngine
{
// it's so weird semun definition is not in the above header files
union semun
{
   i32                  val;
   struct   semid_ds*   buf;
   i32*                 array;
} /*semctl_arg = {0}*/;

/** Class Log */
class SemOp : public WEObj
{
public:
   /**
    * @brief Constructor
    */
   SemOp();

   /**
    * @brief Default Destructor
    */
   ~SemOp();

   /**
    * @brief Create a semaphore
    */
   const int      createSem( int& sid, const key_t key, const int semNum ) const;

   /**
    * @brief Delete a semaphore
    */
   void           deleteSem( const int sid ) const { semctl(sid, 0, IPC_RMID, 0); }

   /**
    * @brief Check the semaphore exist or not
    */
   bool           existSem( int& sid, const key_t key ) const { return !openSem( sid, key ); }

   /**
    * @brief Get the maximum semaphore value
    */
   int            getMaxSemVal() {  return m_maxValue; }          

   /**
    * @brief Get a key value
    */
   const bool     getKey( const char* fileName, key_t& key ) const;

   /**
    * @brief Get semaphore member count
    */
   const int      getSemCount( const int sid ) const;

   /**
    * @brief Get a semaphore value
    */
   const int      getVal( const int sid, const int member ) const { return semctl(sid, member, GETVAL, 0); }

   /**
    * @brief Lock a semaphore
    */
   const int      lockSem( const int sid, const int member ) const;

   /**
    * @brief Open a semaphore
    */
   const int      openSem( int& sid, const key_t key ) const { return (( sid = semget( key, 0, 0666 )) == -1)? ERR_SEM_NOT_EXIST : NO_ERROR; }

   /**
    * @brief Print a semaphore value
    */
   void           printVal( const int sid, const int member ) const { printf("\nsemval for member %d is %d", member, semctl(sid, member, GETVAL, 0)); }
   void           printAllVal( const int sid ) const { int total = getSemCount( sid ); for( int i=0; i<total; i++) printVal( sid, i ); }

   /**
    * @brief Set the maximum semaphore value
    */
   void           setMaxSemVal( const int value ) {  m_maxValue = value; }          
   /**
    * @brief Unlock a semaphore
    */
   const int      unlockSem( const int sid, const int member ) const;

private:
   int            m_maxValue;             // max semaphore value
};


} //end of namespace
#endif // _WE_SEMOP_H_
