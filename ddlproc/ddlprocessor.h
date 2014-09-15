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
*   $Id: ddlprocessor.h 6 2006-06-23 17:58:51Z rcraighead $
*
*
***********************************************************************/
/** @file */
#ifndef DDLPROCESSOR_H
#define DDLPROCESSOR_H

#include "ddlproc.h"
#include "threadpool.h"
#include "calpontsystemcatalog.h"
#include "querytele.h"

namespace ddlprocessor
{
/** @brief process ddl packages as they arrive
  */
class DDLProcessor
{
public:
    /** @brief ctor
      *
      * @param packageMaxThreads the maximum number of threads to process ddl packages
      * @param packageWorkQueueSize the maximum number of ddl packages in the work queue 
      */
    DDLProcessor( int packageMaxThreads, int packageWorkQueueSize );
	~DDLProcessor();
    /** @brief entry point for the DDLProcessor
      */
    void process();
	int commitTransaction(uint32_t txnID, std::string & errorMsg);

    /** @brief get the ddl package thread pool size
      */
    inline int getPackageThreadPoolSize() const { return fPackageMaxThreads; }

    /** @brief set the ddl package thread pool size
      */
    inline void setPackageThreadPoolSize( int threadPoolSize ) { fPackageMaxThreads = threadPoolSize; }

    /** @brief get the maximum number of ddl packages allowed in the work queue
      */
    inline int getPackageWorkQueueSize() const { return fPackageWorkQueueSize; }

    /** @brief set the maximum number of ddl packages allowed in the work queue
      */
    inline void setPackageWorkQueueSize( int workQueueSize ) { fPackageWorkQueueSize = workQueueSize; }


protected:

private:
    /** @brief ctor
      */
    DDLProcessor();

    /** @brief the thread pool for processing ddl packages
      */
    threadpool::ThreadPool fDdlPackagepool;

    int fPackageMaxThreads;     /** @brief max number of threads to process ddl packages */
    int fPackageWorkQueueSize;  /** @brief max number of ddl packages waiting in the work queue */

    messageqcpp::MessageQueueServer fMqServer;
    boost::shared_ptr<execplan::CalpontSystemCatalog> csc;
    WriteEngine::WEClients* fWEClient;
    uint32_t fPMCount;
    querytele::QueryTeleClient fQtc;
};


} //namespace ddlprocessor

#endif //DDLPROCESSOR_H

