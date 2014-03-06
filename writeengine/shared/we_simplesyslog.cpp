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
* $Id: we_simplesyslog.cpp 4607 2013-04-11 21:38:09Z rdempsey $
*
*******************************************************************************/

#include "we_simplesyslog.h"

#include "we_define.h"
#include "messagelog.h"

using namespace std;

namespace WriteEngine
{
/*static*/ SimpleSysLog* SimpleSysLog::fSysLogger = 0;

//------------------------------------------------------------------------------
// Singleton instance accessor (Warning, not thread-safe)
//------------------------------------------------------------------------------
/* static */
SimpleSysLog* SimpleSysLog::instance()
{
    if ( !fSysLogger )
        fSysLogger = new SimpleSysLog();

    return fSysLogger;
}

//------------------------------------------------------------------------------
// SimpleSysLog constructor.
//------------------------------------------------------------------------------
SimpleSysLog::SimpleSysLog() : fLoggingID( SUBSYSTEM_ID_WE )
{
}

//------------------------------------------------------------------------------
// Reset LoggingID (in order to set the subsystem id)
//------------------------------------------------------------------------------
void SimpleSysLog::setLoggingID( const logging::LoggingID& loggingID )
{
    fLoggingID = loggingID;
}

//------------------------------------------------------------------------------
// Log arguments (msgArgs) for specified msgId to the requested log (logType).
//------------------------------------------------------------------------------
void SimpleSysLog::logMsg( const logging::Message::Args& msgArgs,
                           logging::LOG_TYPE             logType,
                           logging::Message::MessageID   msgId )
{
    logging::MessageLog ml( fLoggingID );

    logging::Message m(msgId);
    m.format(msgArgs);

    boost::mutex::scoped_lock lk(fWriteLockMutex);
    switch (logType)
    {
        case logging::LOG_TYPE_DEBUG:
        {
            ml.logDebugMessage(m);
            break;
        }
        case logging::LOG_TYPE_INFO:
        default:
        {
            ml.logInfoMessage(m);
            break;
        }
        case logging::LOG_TYPE_WARNING:
        {
            ml.logWarningMessage(m);
            break;
        }
        case logging::LOG_TYPE_ERROR:
        {
            ml.logErrorMessage(m);
            break;
        }
        case logging::LOG_TYPE_CRITICAL:
        {
            ml.logCriticalMessage(m);
            break;
        }
    }
}

} //end of namespace

