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
* $Id: we_semop.cpp 33 2006-08-31 14:35:14Z wzhou $
*
******************************************************************************************/
/** @file */

#include "we_semop.h"

namespace WriteEngine
{

   /**
    * Constructor
    */
   SemOp::SemOp() : m_maxValue( 1 )
   {
   }

   /**
    * Default Destructor
    */
   SemOp::~SemOp()
   {}


   /***********************************************************
    * DESCRIPTION:
    *    Create a semaphore
    * PARAMETERS:
    *    sid - semaphore id
    *    key - semaphore key
    *    semNum - total number in the semaphore
    * RETURN:
    *    NO_ERROR if success, other number if fail
    ***********************************************************/
   const int SemOp::createSem( int& sid, const key_t key, const int semNum ) const
   {
      union semun semopts;

      if( semNum > MAX_SEM_NUM )
         return ERR_MAX_SEM;

      if(( sid = semget( key, semNum, IPC_CREAT|IPC_EXCL|0666)) == -1)
         return ERR_SEM_EXIST;

      semopts.val = m_maxValue;

      for( int i = 0; i < semNum; i++)
         semctl( sid, i, SETVAL, semopts);

      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Get a unique key based on the filename
    * PARAMETERS:
    *    fileName - file name
    *    key - generated key
    * RETURN:
    *    true if success, false if fail
    ***********************************************************/
   const bool SemOp::getKey( const char* fileName, key_t& key ) const
   {
      if( fileName == NULL )
         return false;
      
      key = ftok( fileName, 'S' );

      return key < 0 ? false: true;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Get semaphore member count
    * PARAMETERS:
    *    sid - semaphore id
    * RETURN:
    *    member count
    ***********************************************************/
   const int SemOp::getSemCount( const int sid ) const
   {
      int rc;
      struct semid_ds mysemds;
      union semun semopts;

      semopts.buf = &mysemds;
      if((rc = semctl(sid, 0, IPC_STAT, semopts)) == -1) 
         return 0;

      return(semopts.buf->sem_nsems);
   }

   /***********************************************************
    * DESCRIPTION:
    *    lock a semaphore
    * PARAMETERS:
    *    sid - semaphore id
    *    member - semaphore set number
    * RETURN:
    *    NO_ERROR if success, other number if fail
    ***********************************************************/
   const int SemOp::lockSem( const int sid, const int member ) const
   {
      struct sembuf semLock={ 0, -1, IPC_NOWAIT};

      if( member < 0 /*|| member > (get_member_count(sid)-1)*/)
         return ERR_VALUE_OUTOFRANGE;

      // lock the the semaphore 
      if( !getVal( sid, member ))
         return ERR_NO_SEM_RESOURCE;

      semLock.sem_num = member;
      if(( semop(sid, &semLock, 1 )) == -1)
         return ERR_LOCK_FAIL;

      return NO_ERROR;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Unlock a semaphore
    * PARAMETERS:
    *    sid - semaphore id
    *    member - semaphore set number
    * RETURN:
    *    NO_ERROR if success, other number if fail
    ***********************************************************/
   const int SemOp::unlockSem( const int sid, const int member ) const
   {
      struct sembuf semUnlock={ member, 1, IPC_NOWAIT};
      int semVal;

      if( member < 0 /*|| member>(get_member_count(sid)-1)*/)
         return ERR_VALUE_OUTOFRANGE;

      // check the semaphore is locked
      semVal = getVal( sid, member );
      if( semVal == m_maxValue ) 
         return ERR_NO_SEM_LOCK;

      semUnlock.sem_num = member;
      if(( semop( sid, &semUnlock, 1)) == -1)
         return ERR_UNLOCK_FAIL;

      return NO_ERROR;
   }
} //end of namespace

