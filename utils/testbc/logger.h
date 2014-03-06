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
 * $Id: logger.h 706 2008-09-16 18:25:49Z bwelch $
 */

/** @file */

#ifndef PRIMPROC_LOGGER_H_
#define PRIMPROC_LOGGER_H_

#include <map>
#include <boost/thread.hpp>

#include "messageobj.h"
#include "messagelog.h"

namespace primitiveprocessor
{

/** @brief message log wrapper class */
class Logger
{
public:
	Logger();

	void logMessage(const logging::Message::MessageID mid,
                    const logging::Message::Args& args,
                    bool  critical=false);

	void logMessage(const std::string& msg, bool critical = true, logging::Message::MessageID mid = 0 )
	{
		logging::Message::Args args;
		args.add(msg);
		logMessage(mid, args, true);
	}

private:
	// defaults okay
	//Logger(const Logger& rhs);
	//Logger& operator=(const Logger& rhs);

	typedef std::map<logging::Message::MessageID, logging::Message> MsgMap;

	MsgMap fMsgMap;
	boost::mutex fLogLock;
	logging::MessageLog fMl1;
};


}

#endif

