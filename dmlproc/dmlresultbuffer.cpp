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
*   $Id: dmlresultbuffer.cpp 927 2013-01-21 14:11:25Z rdempsey $
*
*
***********************************************************************/
/** @file */

#include "dmlresultbuffer.h"

namespace dmlprocessor
{

DMLResultBuffer::DMLResultBuffer()
:fBufferSize(1), fFull(0)
{
    
}

DMLResultBuffer::DMLResultBuffer(int bufferSize)
:fBufferSize(bufferSize), fFull(0)
{

}

DMLResultBuffer::~DMLResultBuffer()
{
    fResultBuffer.clear();
}

void DMLResultBuffer::put(dmlpackageprocessor::DMLPackageProcessor::DMLResult result, int sessionID)
{
    scoped_lock lock(fMutex);
    if (fFull == fBufferSize)
    {
        while (fFull == fBufferSize)
            fCond.wait(lock);
    }
    ResultPair rp;
    rp.result = result;
    rp.sessionID = sessionID;

    fResultBuffer.push_back(rp);
    ++fFull;
    fCond.notify_one();
}

DMLResultBuffer::ResultPair DMLResultBuffer::get()
{
    scoped_lock lk(fMutex);
    if (fFull == 0)
    {
        while (fFull == 0)
            fCond.wait(lk);
    }
    ResultPair rp = fResultBuffer[0];
    fResultBuffer.pop_front();
    --fFull;
    fCond.notify_one();
    return rp;
}

} //namespace dmlprocessor


