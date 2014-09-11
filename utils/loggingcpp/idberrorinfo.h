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

/******************************************************************************************
* $Id: idberrorinfo.h 2394 2011-02-08 14:36:22Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef LOGGING_IDBERRORINFO_H
#define LOGGING_IDBERRORINFO_H

#include <iosfwd>
#include <string>
#include <map>
#include <boost/any.hpp>
#include "messageobj.h"
#include "messagelog.h"

namespace logging {

/** @brief an IDB error info class
 *
 * Contains a error message map for looking up and constructing formatted message
 */

typedef std::map<unsigned, std::string> ErrorMap;

class IDBErrorInfo
{
public:
	static IDBErrorInfo* instance();
	std::string errorMsg(const unsigned eid, const Message::Args& args);
	std::string errorMsg(const unsigned eid);
	std::string logError(const logging::LOG_TYPE logLevel,
		                   const LoggingID logid,
		                   const unsigned eid,
		                   const Message::Args& args);
	~IDBErrorInfo();

private:
	static IDBErrorInfo* fInstance;
	ErrorMap fErrMap;
	IDBErrorInfo();
	void format(std::string& messageFormat, const Message::Args& args);
	std::string lookupError(const unsigned eid);
};

}//namespace logging

#endif
