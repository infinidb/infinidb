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
* $Id: we_stats.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#include <we_stats.h>

using namespace std;

namespace WriteEngine
{
#ifdef PROFILE
   /* static */ bool                            Stats::fProfiling = false;
   /* static */ boost::mutex                    Stats::fRegisterReaderMutex;
   /* static */ boost::mutex                    Stats::fRegisterParseMutex;
   /* static */ std::vector<pthread_t>          Stats::fReadProfThreads;
   /* static */ std::vector<pthread_t>          Stats::fParseProfThreads;
   /* static */ std::vector<logging::StopWatch> Stats::fReadStopWatch;
   /* static */ std::vector<logging::StopWatch> Stats::fParseStopWatch;
#endif

   struct IoStats Stats::m_ioStats = { 0, 0 };
   bool Stats::m_bUseStats = false;
   /***********************************************************
    * DESCRIPTION:
    *    Increase the counter for block read
    * PARAMETERS:
    *    blockNum - the number of blocks
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::incIoBlockRead( const int blockNum )
   {
      if( !m_bUseStats )
         return;
      m_ioStats.blockRead += blockNum;
   }

   /***********************************************************
    * DESCRIPTION:
    *    Increase the counter for block write
    * PARAMETERS:
    *    blockNum - the number of blocks
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::incIoBlockWrite( const int blockNum )
   {
      if( !m_bUseStats )
         return;
      m_ioStats.blockWrite += blockNum;
   }

#ifdef PROFILE
//-------------------------------------------------------------------------------
// Functions that follow are used for profiling using the StopWatch class
//-------------------------------------------------------------------------------

   /***********************************************************
    * DESCRIPTION:
    *    Enable/Initialize the profiling functions
    * PARAMETERS:
    *    nReadThreads  - number of read threads to be profiled
    *    nParseThreads - number of parse threads to be profiled
    * RETURN:
    *    none
    ***********************************************************/
   void Stats::enableProfiling(int nReadThreads, int nParseThreads)
   {
      fProfiling = true;

      // @bug 2625: pre-reserve space for our vectors; else we could have a race
      // condition whereby one parsing thread is adding itself to the vectors
      // and thus "growing" the vector (in registerParseProfThread), at the
      // same time that another parsing thread is reading the vector in parse-
      // Event().  By pre-reserving the space, the vectors won't be growing,
      // thus eliminating the problem with this race condition.
      fReadProfThreads.reserve ( nReadThreads  );
      fReadStopWatch.reserve   ( nReadThreads  );
      fParseProfThreads.reserve( nParseThreads );
      fParseStopWatch.reserve  ( nParseThreads );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Register the current thread as a Read thread to be profiled
    * PARAMETERS:
    *    none
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::registerReadProfThread( )
   {
      boost::mutex::scoped_lock lk(fRegisterReaderMutex);

      fReadProfThreads.push_back( pthread_self() );
      logging::StopWatch readStopWatch;
      fReadStopWatch.push_back  ( readStopWatch  );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Register the current thread as a Parse thread to be profiled
    * PARAMETERS:
    *    none
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::registerParseProfThread( )
   {
      boost::mutex::scoped_lock lk(fRegisterParseMutex);

      fParseProfThreads.push_back( pthread_self() );
      logging::StopWatch parseStopWatch;
      fParseStopWatch.push_back  ( parseStopWatch );
   }

   /***********************************************************
    * DESCRIPTION:
    *    Track the specified Read event in the current Read thread.
    * PARAMETERS:
    *    eventString - string that identifies the event.
    *    start       - boolean indicating whether the is the start or the
    *                  end of the event.  TRUE=>start FALSE=>end
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::readEvent ( const std::string& eventString, bool start )
   {
      if (fProfiling)
      {
         pthread_t thread = pthread_self();
         for (unsigned i=0; i<fReadProfThreads.size(); i++)
         {
            if (fReadProfThreads[i] == thread)
            {
               if (start)
                  fReadStopWatch[i].start( eventString );
               else
                  fReadStopWatch[i].stop ( eventString );
               break;
            }
         }
      }
   }

   /***********************************************************
    * DESCRIPTION:
    *    Track the specified Parse event in the current Parse thread.
    * PARAMETERS:
    *    eventString - string that identifies the event.
    *    start       - boolean indicating whether the is the start or the
    *                  end of the event.  TRUE=>start FALSE=>end
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::parseEvent ( const std::string& eventString, bool start )
   {
      if (fProfiling)
      {
         pthread_t thread = pthread_self();
         for (unsigned i=0; i<fParseProfThreads.size(); i++)
         {
            if (fParseProfThreads[i] == thread)
            {
               if (start)
                  fParseStopWatch[i].start( eventString );
               else
                  fParseStopWatch[i].stop ( eventString );
               break;
            }
         }
      }
   }

   /***********************************************************
    * DESCRIPTION:
    *    Print profiling results.
    * PARAMETERS:
    *    none
    * RETURN:
    *    none
    ***********************************************************/
   void  Stats::printProfilingResults ( )
   {
      if (fProfiling)
      {
         std::cout << endl;
         for (unsigned j=0; j<fReadStopWatch.size(); j++)
         {
            std::cout << "Execution Stats for Read Thread " << j << " (" <<
                         fReadProfThreads[j] << ")"         << std::endl <<
                         "-------------------------------"  << std::endl;
            fReadStopWatch[j].finish();
            std::cout << std::endl;
         }

         for (unsigned j=0; j<fParseStopWatch.size(); j++)
         {
            std::cout << "Execution Stats for Parse Thread "<< j << " (" <<
                         fParseProfThreads[j] << ")"        << std::endl <<
                         "--------------------------------" << std::endl;
            fParseStopWatch[j].finish();
            std::cout << std::endl;
         }

      }
   }
#endif

} //end of namespace

