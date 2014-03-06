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

/*
 * $Id: logger.cpp 686 2008-08-21 22:08:33Z rdempsey $
 */

#include <boost/thread.hpp>
using namespace boost;

#include "messageobj.h"
#include "messageids.h"
#include "loggingid.h"
using namespace logging;

#include "logger.h"

namespace primitiveprocessor
{

Logger::Logger() :
	fMl1(LoggingID(28))
{
	fMsgMap[logging::M0000] = Message(logging::M0000);
	fMsgMap[logging::M0016] = Message(logging::M0016);
	fMsgMap[logging::M0045] = Message(logging::M0045);
	fMsgMap[logging::M0053] = Message(logging::M0053);
}

void Logger::logMessage(const Message::MessageID mid,
                        const Message::Args& args,
                        bool  critical)
{
	mutex::scoped_lock lk(fLogLock);
	MsgMap::iterator msgIter = fMsgMap.find(mid);
	if (msgIter == fMsgMap.end())
		msgIter = fMsgMap.find(logging::M0000);

	msgIter->second.reset();
	msgIter->second.format(args);

	if (critical)
	{
		fMl1.logCriticalMessage(msgIter->second);
	}
	else
	{
		fMl1.logWarningMessage(msgIter->second);
	}
}

}

