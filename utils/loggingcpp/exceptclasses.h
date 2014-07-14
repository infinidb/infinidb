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

/******************************************************************************
 * $Id: exceptclasses.h 3857 2013-06-04 18:19:28Z pleblanc $
 *
 *****************************************************************************/

/** @file */

#ifndef LOGGING_EXCEPTCLASSES_H
#define LOGGING_EXCEPTCLASSES_H

#include <iostream>
#include <sstream>
#include <stdexcept>
#include <string>

#include "errorcodes.h"
#include "errorids.h"
#include "idberrorinfo.h"
#include "logger.h"

namespace logging
{

/** @brief specific error exception class
*
*
*/
class IDBExcept : public std::runtime_error
{
public:
	IDBExcept(uint16_t code) :
		std::runtime_error(IDBErrorInfo::instance()->errorMsg(code)), fErrCode(code) {}
	IDBExcept(uint16_t code, const Message::Args& args) :
		std::runtime_error(IDBErrorInfo::instance()->errorMsg(code, args)), fErrCode(code){}
	IDBExcept(const std::string& msg, uint16_t code) :
		std::runtime_error(msg), fErrCode(code){ }
	void errorCode(uint16_t code) { fErrCode = code; }
	uint16_t errorCode() const    { return fErrCode; }
protected:
	unsigned fErrCode;
};

class DictionaryBufferOverflow : public IDBExcept
{
public:
	DictionaryBufferOverflow() : IDBExcept(ERR_DICTBUFFER_OVERFLOW)
	{ }
};

class LargeDataListExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	explicit LargeDataListExcept(const std::string&  msg) : std::runtime_error(msg) { }

	virtual	~LargeDataListExcept() throw() { }
};


/** @brief specific error exception class for query data
*   @bug 1155
*
*/
class QueryDataExcept : public IDBExcept
{
public:
	/** Takes a character string describing the error.  */
	QueryDataExcept(const std::string& msg, uint16_t code) :
		IDBExcept(msg, code)  { }

	virtual	~QueryDataExcept() throw() { }
private:
	//defaults okay
	//QueryDataExcept(const QueryDataExcept& rhs);
	//QueryDataExcept& operator=(const QueryDataExcept& rhs);
};

/** @brief specific error exception class for VBBM Version Buffer overflow
*   @bug 1949
*
*/
class VBBMBufferOverFlowExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	VBBMBufferOverFlowExcept(const std::string& msg) :
		std::runtime_error(msg), fErrorCode(8) { }
	//ERR_VBBM_OVERFLOW = 8 defined in brmtypes.h
	int8_t errorCode() const { return fErrorCode; }

private:
	int8_t fErrorCode;
};

/** @brief specific error exception class for VBBM Version Buffer overflow
*   @bug 1949
*
*/
class PrimitiveColumnProjectResultExcept : public QueryDataExcept
{
public:
	/** Takes a character string describing the error.  */
	PrimitiveColumnProjectResultExcept(const std::string& msg) :
		QueryDataExcept(msg, projectResultErr) { }
};

/** @brief specific error exception class for PrimProc invalid HWM
*   @bug 2173
*
*/
class InvalidRangeHWMExcept : public QueryDataExcept
{
public:
	/** Takes a character string describing the error.  */
	InvalidRangeHWMExcept(const std::string& msg) :
		QueryDataExcept(msg, hwmRangeSizeErr){ }
};


/** @brief Exception for F&E framework to throw
 *  Invalid Operation Exception
 */
class InvalidOperationExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	InvalidOperationExcept(const std::string& msg) :
		std::runtime_error(msg){ }
};

/** @brief Exception for F&E framework to throw
 *  Invalid Conversion Exception
 */
class InvalidConversionExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	InvalidConversionExcept(const std::string& msg) :
		std::runtime_error(msg){ }
};

/** @brief Exception for F&E framework -- function evaluation to throw
 *  Invalid Argument Exception
 */
class InvalidArgumentExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	InvalidArgumentExcept(const std::string& msg) :
		std::runtime_error(msg){ }
};

/** @brief Exception for F&E framework -- function evaluation to throw
 *  Invalid Argument Exception
 */
class NotImplementedExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	NotImplementedExcept(const std::string& msg) :
		std::runtime_error(msg){ }
};

/** @brief specific error exception class for getSysData in Calpontsystemcatalog.
*   @bug 2574
*
*/
class NoTableExcept : public std::runtime_error
{
public:
	/** Takes a character string describing the error.  */
	NoTableExcept(const std::string& msg) :
		std::runtime_error(msg){ }
};

class MoreThan1RowExcept : public IDBExcept
{
public:
	MoreThan1RowExcept() :
		IDBExcept(ERR_MORE_THAN_1_ROW)
	{ }
};

class CorrelateFailExcept : public IDBExcept
{
public:
	CorrelateFailExcept() :
		IDBExcept(ERR_CORRELATE_FAIL)
	{ }
};

class DBRMException : public std::runtime_error
{
public:
	DBRMException(const std::string& emsg) :
	std::runtime_error(emsg) {}
};

class ProtocolError : public std::logic_error
{
public:
	ProtocolError(const std::string &emsg) : std::logic_error(emsg) { }
};

#ifndef __STRING
#define __STRING(x) #x
#endif
#define idbassert(x) do { \
	if (!(x)) { \
		std::ostringstream os; \
\
		os << __FILE__ << "@" << __LINE__ << ": assertion \'" << __STRING(x) << "\' failed"; \
		std::cerr << os.str() << std::endl; \
		logging::MessageLog logger((logging::LoggingID())); \
		logging::Message message; \
		logging::Message::Args args; \
\
		args.add(os.str()); \
		message.format(args); \
		logger.logErrorMessage(message); \
		throw logging::IDBExcept(logging::ERR_ASSERTION_FAILURE); \
	} \
} while (0)

#define idbassert_s(x, s) do { \
	if (!(x)) { \
		std::ostringstream os; \
\
		os << __FILE__ << "@" << __LINE__ << ": assertion \'" << __STRING(x) << "\' failed.  Error msg \'" << s << "\'"; \
		std::cerr << os.str() << std::endl; \
		logging::MessageLog logger((logging::LoggingID())); \
		logging::Message message; \
		logging::Message::Args args; \
\
		args.add(os.str()); \
		message.format(args); \
		logger.logErrorMessage(message); \
		throw logging::IDBExcept(logging::ERR_ASSERTION_FAILURE); \
	} \
} while (0)

}


#endif
// vim:ts=4 sw=4:
