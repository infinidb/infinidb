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
* $Id: we_simplesyslog.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_SIMPLESYSLOG_H_
#define _WE_SIMPLESYSLOG_H_

#include <boost/thread.hpp>

#include "messagelog.h"
#include "messageobj.h"

#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/**
 * @brief SimpleSysLog class is a simple logger that only logs to syslog.
 *
 * Note that the instance() and setLoggingID() funtions are not thread-safe.
 * They should be called once, upfront by the main thread to perform setup.
 */
class SimpleSysLog
{
public:
   /**
    * @brief Singleton accessor.
    */
    EXPORT static SimpleSysLog* instance();

   /**
    * @brief Modify the LoggingID to be used.  Mainly used to control the
    * subsystem ID.
    */
    EXPORT void setLoggingID( const logging::LoggingID& loggingID );
    
   /**
    * @brief Function that logs a syslog msg.
    */
    EXPORT void logMsg( const logging::Message::Args& msgArgs,
                        logging::LOG_TYPE             logType,
                        logging::Message::MessageID   msgId );

private:
    SimpleSysLog( );
    SimpleSysLog( const SimpleSysLog& );
    SimpleSysLog& operator= ( const SimpleSysLog& );

    static SimpleSysLog* fSysLogger;
    logging::LoggingID   fLoggingID;
    boost::mutex         fWriteLockMutex; // logging mutex
};

#undef EXPORT

} //end of namespace
#endif // _WE_SIMPLESYSLOG_H_
