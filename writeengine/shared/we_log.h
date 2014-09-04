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
* $Id: we_log.h 4506 2013-02-02 00:11:15Z bpaul $
*
*******************************************************************************/
/** @file */

#ifndef _WE_LOG_H_
#define _WE_LOG_H_

#include <time.h>
#include <sys/types.h>
#include <unistd.h> 

#include <iostream>
#include <fstream>
#include <sstream>

#include <we_obj.h>

#include <boost/thread.hpp>

#if defined(_MSC_VER) && defined(WELOG_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
const	std::string MSG_LEVEL_STR[] = { 
	"INFO",
	"INFO",
	"WARN",
	"ERR ",
	"CRIT" };

/** @brief Class is used to format and write log messages to cpimport.bin log
 *  file.  When applicable, messages are also logged to syslog logs as well.
 */
class Log : public WEObj
{
public:
   /**
    * @brief Constructor
    */
    EXPORT Log();

   /**
    * @brief Destructor
    */
    EXPORT ~Log();

   /**
    * @brief Log a cpimport.bin logfile message; logs errors to syslog as well
    */
    EXPORT void     logMsg( const char* msg, int code, MsgLevel level );
    EXPORT void     logMsg( const char* msg, MsgLevel level )
                        { logMsg( msg, 0, level ); }
    EXPORT void     logMsg( const std::string& msg, MsgLevel level )
                        { logMsg( msg.c_str(), level ); }
    EXPORT void     logMsg( const std::string& msg, int code, MsgLevel level )
                        { logMsg( msg.c_str(), code, level ); }

   /**
    * @brief Set log file name
    */
    EXPORT void     setLogFileName( const char* logfile,
                                const char* errlogfile,
                                bool consoleFlag = true );

    // BUG 5022
    /**
     * @brief Set log files close other than calling d'tor
     */
    EXPORT void 	closeLog();


private:
    void            logSyslog ( const std::string& msg,
                                int                statusCode);
    void            formatMsg(  const std::string& msg,
                                MsgLevel           level,
                                std::ostringstream& oss,
                                int                code = 0 ) const;

    bool            m_bConsoleOutput;              // flag allowing INFO2 msg
                                                   //   to display to console
    std::string     m_logFileName;                 // log file name
    std::string     m_errlogFileName;              // error log file name
    pid_t           m_pid;                         // current pid

    std::ofstream   m_logFile;                     // log file stream
    std::ofstream   m_errLogFile;                  // error log file stream

    boost::mutex    m_WriteLockMutex;              // logging mutex
};

} //end of namespace

#undef EXPORT

#endif // _WE_LOG_H_
