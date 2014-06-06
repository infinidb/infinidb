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
* $Id: we_log.cpp 4504 2013-02-02 00:07:43Z bpaul $
*
*******************************************************************************/

#include "we_log.h"

#include "messageids.h"
#include "we_define.h"
#include "we_simplesyslog.h"
#include "we_convertor.h"

namespace WriteEngine
{
    WriteEngine::WErrorCodes ec; // referenced as extern by chunkmanager

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
Log::Log() : m_bConsoleOutput( true ),
             m_logFileName( "" ),
             m_errlogFileName( "" )
{
    m_pid = ::getpid();
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
Log::~Log()
{
    m_logFile.close();
    m_errLogFile.close();
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//   Format a message to be logged
// PARAMETERS:
//   msg - message
//   level - message level
//   oss - formatted msg
//   code - return status code to include in the message
// RETURN:
//   none
//------------------------------------------------------------------------------
void Log::formatMsg( const std::string&  msg,
                     MsgLevel       level,
                     std::ostringstream& oss,
                     int            code ) const
{
    // Constructing and logging the entire message as one string, should
    // help avoid any thread contention that could cause logging output
    // to be interweaved between threads.
    oss << Convertor::getTimeStr();

    // Include thread id in log message based on debug level
    if (isDebug( DEBUG_2 ))
    {
        oss << " (" << m_pid << ":" <<
#ifdef _MSC_VER
        GetCurrentThreadId()
#else
        pthread_self()
#endif
        << ") " <<
        MSG_LEVEL_STR[level] << " : " << msg ;
    }
    else
    {
        oss << " (" << m_pid << ") " << MSG_LEVEL_STR[level] << " : " << msg ;
    }

    if( code > 0 )
        oss << " [" << code << "]";
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//   Log a message to the log file and to the console.
//   We used to log error and critical error msgs to error log only.
//   But changed to log to m_logFile too, because it's easy to
//   forget to look at the err log.  And seeing the error log msg
//   in the correct timeline context in the m_logFile helps to see
//   what else was going on when the problem occurred.
// PARAMETERS:
//   msg - message
//   code - return status code to include in the message
//   level - message level
// RETURN:
//    none
//------------------------------------------------------------------------------
void Log::logMsg( const char* msg,
                  int         code,
                  MsgLevel    level )
{
    std::ostringstream oss;
    formatMsg( msg, level, oss, code );

    // log error and critical msgs to syslog
    if( level == MSGLVL_ERROR || level == MSGLVL_CRITICAL ) 
    {
        { //log to log file and error log file within scope of mutex lock.
          //logSyslog uses SimpleSyslog which has it's own lock.
            boost::mutex::scoped_lock lk(m_WriteLockMutex);

            m_errLogFile << oss.str() << std::endl;
            m_logFile    << oss.str() << std::endl;

            std::cerr << oss.str() << std::endl;
        }

        logSyslog( std::string(msg), code );
    }
    else
    {
        std::ostringstream oss2;

        // Format msg again without including the status code.
        // Only log INFO2 msgs to console if m_bConsoleOutput is TRUE;
        // All other msg levels always go to console.
        if( (level != MSGLVL_INFO2) || (m_bConsoleOutput) )
            formatMsg ( msg, level, oss2 );

        boost::mutex::scoped_lock lk(m_WriteLockMutex);

        m_logFile << oss.str() << std::endl;

        if( (level != MSGLVL_INFO2) || (m_bConsoleOutput) )
            std::cout << oss2.str() << std::endl;
    }
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//   Set log file name
// PARAMETERS:
//   logfile - log file name
//   errlogfile - error log file name
// RETURN:
//   none
//------------------------------------------------------------------------------
void Log::setLogFileName( const char* logfile,
                          const char* errlogfile,
                          bool        consoleFlag ) 
{
    m_logFileName = logfile;
    m_errlogFileName = errlogfile;
    m_bConsoleOutput = consoleFlag;
#ifdef _MSC_VER
    // cpimport.bin calls BulkLoad::loadJobInfo() before calling
    // BulkLoad::processJob(). loadJobInfo() attempts to write to this log
    // before it's opened (by processJob()). This doesn't seem to bother Linux
    // but puts Windows in a bad state. Once this logic is fixed, this hack can
    // go away.
    // This code probably wouldn't hurt if run on Linux, but I'll leave this
    // here as a reminder to fix the logic for all platforms.
    m_logFile.close();
    m_logFile.clear();
    m_errLogFile.close();
    m_errLogFile.clear();
#endif
    m_logFile.open( m_logFileName.c_str(),
                    std::ofstream::out | std::ofstream::app );
    m_errLogFile.open(m_errlogFileName.c_str(),
                      std::ofstream::out | std::ofstream::app);
}

//------------------------------------------------------------------------------
// DESCRIPTION:
//   Log specified (error or critical) message to syslog error log.
// PARAMETERS:
//   msg        - message
//   statusCode - WriteEngine return status code to include in the message
// RETURN:
//    none
//------------------------------------------------------------------------------
void Log::logSyslog( const std::string& msg,
                     int         statusCode )
{
    logging::Message::MessageID msgId = logging::M0087;

    switch (statusCode)
    {
        case ERR_FILE_DISK_SPACE:
        {
            msgId = logging::M0076;
            break;
        }
		case ERR_UNKNOWN:
		{
			msgId = logging::M0017;
            break;
		}
        default:
        {
            msgId = logging::M0087;
            break;
        }
    }

    logging::Message::Args errMsgArgs;
    errMsgArgs.add( msg );
    SimpleSysLog::instance()->logMsg(
        errMsgArgs,
        logging::LOG_TYPE_ERROR,
        msgId);
}
//------------------------------------------------------------------------------
// DESCRIPTION:
// BUG 5022
//  Close the log files with out calling d'tor. That way we can use the
//	object again for logging while importing another table or so
// PARAMETERS:
//   none
// RETURN:
//   none
//------------------------------------------------------------------------------

void Log::closeLog()
{
    m_logFile.close();
    m_errLogFile.close();
}

} //end of namespace

