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
*   $Id: dmlprocessor.h 651 2011-11-04 20:51:41Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef DMLPROCESSOR_H
#define DMLPROCESSOR_H

#include "threadpool.h"
#include "messagequeue.h"
#include "bytestream.h"
#include "insertdmlpackage.h"
#include "deletedmlpackage.h"
#include "updatedmlpackage.h"
#include "commanddmlpackage.h"
#include "insertpackageprocessor.h"
#include "deletepackageprocessor.h"
#include "updatepackageprocessor.h"
#include "commandpackageprocessor.h"
#include "messagelog.h"
#include "distributedenginecomm.h"
#include "calpontsystemcatalog.h"

namespace dmlprocessor
{

/** @brief main server thread
  */
class DMLServer
{
public:
	DMLServer(int packageMaxThreads, int packageWorkQueueSize);

	~DMLServer() { }


	void start(); //Does not return

    /** @brief get the dml package thread pool size
      */
    inline int  getPackageThreadPoolSize() const { return fPackageMaxThreads; }

    /** @brief set the dml package thread pool size
      *
      * @param threadPoolSize the maximum number of threads to process dml packages
      */
    inline void setPackageThreadPoolSize( int threadPoolSize ) { fPackageMaxThreads = threadPoolSize; }

    /** @brief get the maximum number of dml packages allowed in the work queue
      */
    inline int getPackageWorkQueueSize() const { return fPackageWorkQueueSize; }

    /** @brief set the dml package work queue size
      *
      * @param workQueueSize the maximum number of dml packages in the work queue
      */
    inline void setPackageWorkQueueSize( int workQueueSize ) {  fPackageWorkQueueSize = workQueueSize; }

private:
	//not copyable
	DMLServer(const DMLServer& rhs);
	DMLServer& operator=(const DMLServer& rhs);

    /** @brief the thread pool for processing dml packages
      */
    threadpool::ThreadPool fDmlPackagepool;

    int fPackageMaxThreads;    /** @brief max number of threads to process dml packages */
    int fPackageWorkQueueSize; /** @brief max number of packages waiting in the work queue */

    messageqcpp::MessageQueueServer* fMqServer;
};

/** @brief processes dml packages as they arrive
  */
class DMLProcessor
{
public:
    /** @brief ctor
      *
      * @param packageMaxThreads the maximum number of threads to process dml  packages
      * @param packageWorkQueueSize the maximum number of dml packages in the work queue
      */
    DMLProcessor(messageqcpp::IOSocket ios);

    /** @brief entry point for the DMLProcessor
      */
    void operator()();

private:
    /** @brief stop accepting incoming DML packages
      *
      * @param ios the io socket to send repsonse
      */
    void stopAcceptingPackages(messageqcpp::IOSocket& ios);

    /** @brief resume accepting incoming DML packages
      *
      * @param ios the io socket to send response
      */
    void resumeAcceptingPackages(messageqcpp::IOSocket& ios);

	messageqcpp::IOSocket fIos;
	execplan::SessionManager sessionManager;
	execplan::CalpontSystemCatalog* csc;
};

} // namespace dmlprocessor

#endif //DMLPROCESSOR_H
// vim:ts=4 sw=4:

