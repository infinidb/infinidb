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

/***********************************************************************
*   $Id: dmlresultbuffer.h 927 2013-01-21 14:11:25Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef DMLRESULTBUFFER_H
#define DMLRESULTBUFFER_H
#include <deque>
#include <boost/thread/thread.hpp>
#include <boost/thread/mutex.hpp>
#include "dmlpackageprocessor.h"

namespace dmlprocessor
{
/** @brief holds dml results
  */
class DMLResultBuffer
{
public:
    typedef boost::mutex::scoped_lock scoped_lock;

    /** @brief the type of a <DMLResult, sessionID> pair
      *
      */
    struct ResultPair
    {
        dmlpackageprocessor::DMLPackageProcessor::DMLResult result;
        int sessionID;
    };

    /** @brief ctor
      */	
    DMLResultBuffer();
	
    /** @brief ctor
      *
      */
    DMLResultBuffer(int bufferSize);

    /** @brief dtor
      *
      */
    ~DMLResultBuffer();
	
    /** @brief set the size of the buffer
      *
      * @param bufferSize the size of the buffer
      */
    inline void setBufferSize(int bufferSize) {fBufferSize=bufferSize;}
	
    /** @brief put results in the buffer
      *
      */
    void put( dmlpackageprocessor::DMLPackageProcessor::DMLResult result, int sessionID );

    /** @brief get results from the buffer
      *
      */
    ResultPair get();

private:
    boost::mutex fMutex;
    boost::condition fCond;

    typedef std::deque<ResultPair> ResultBuffer;
    ResultBuffer fResultBuffer;

    int fBufferSize;
    int fFull;
};

} //namespace dmlprocessor

#endif //DMLRESULTPROCESSOR_H
