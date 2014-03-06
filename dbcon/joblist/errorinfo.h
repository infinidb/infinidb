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

//  $Id: jobstep.h 9636 2013-06-20 14:23:36Z rdempsey $


/** @file */

#ifndef JOBLIST_ERROR_INFO_H_
#define JOBLIST_ERROR_INFO_H_

#include <string>
#include <boost/shared_ptr.hpp>

namespace joblist
{

/** @brief struct ErrorInfo
 *
 * struct ErrorInfo stores the error code and message
 */
struct ErrorInfo {
    ErrorInfo() : errCode(0) { }
    uint32_t errCode;
    std::string errMsg;
    // for backward compat
    ErrorInfo(uint16_t v) : errCode(v) { }
    ErrorInfo & operator=(uint16_t v) { errCode = v; errMsg.clear(); return *this; }
};
typedef boost::shared_ptr<ErrorInfo> SErrorInfo;


}

#endif  // JOBLIST_ERROR_INFO_H_
// vim:ts=4 sw=4:



