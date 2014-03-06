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
* $Id: messageobj.h 3495 2013-01-21 14:09:51Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef LOGGING_MESSAGEOBJ_H
#define LOGGING_MESSAGEOBJ_H

#include <string>
#include <vector>
#include <boost/any.hpp>
#include <sys/types.h>
#include <stdint.h>

namespace config
{
class Config;
}

class MessageLoggingTest;

namespace logging {

/** @brief a message class
 *
 * Contains a message to be logged
 */
class Message
{
public:
	/** @brief the args to a formatted message
	*/
	class Args
	{
	public:
		/** @brief the args vector
		*/
		typedef std::vector<boost::any> AnyVec;

		// default ctors & dtors okay

		/** @brief add an int arg to the message
		*/
		void add(int i);

		/** @brief add an unsigned 64 bit int arg to the message
		*/
		void add(uint64_t i);

		/** @brief add a float arg to the message
		*/
		void add(double d);

		/** @brief add a string arg to the message
		*/
		void add(const std::string& s);

		/** @brief args accessor
		*/
		const AnyVec& args() const { return fArgs; }

		/** @brief reset the args list
		*
		*/
		void reset();

		friend class ::MessageLoggingTest;

	private:
		AnyVec fArgs;	/// the args vector
	};

	/** @brief the MessageID type
	*/
	typedef unsigned MessageID;

	/** @brief default ctor
	*/
	explicit Message(const MessageID msgid=0);
	
	/** @brief ctor turn a message string to a Message
	*
	* For error handling framework use to log error
	*/
	Message(const std::string msg);

	/** @brief format message with args
	*/
	void format(const Args& args);

	/** @brief msg accessor
	*/
	const std::string& msg() const { return fMsg; }

	/** @brief swap
	*/
	void swap(Message& rhs);

	/** @brief lookup the message format string
	*
	* looks up the message format string for msgid. Returns a reasonable
	* default if one can't be found.
	*/
	static const std::string lookupMessage(const MessageID& msgid);

	/** @brief reset the formated string
	*
	* The original, unformated string is restored, and this Message
	*  is then ready for a new call to format().
	*/
	void reset();

	/** @brief msgID accessor
	*/
	const MessageID& msgID() const { return fMsgID; }

	friend class ::MessageLoggingTest;

private:
	//defaults okay
	//Message(const Message& rhs);
	//Message& operator=(const Message& rhs);

	MessageID fMsgID;	/// the msgID
	std::string fMsg;	/// the formated or unformated message
	config::Config* fConfig; /// config file ptr
};

}//namespace logging

namespace std
{
   template<> inline void swap<logging::Message>(logging::Message& lhs, logging::Message&rhs)
   {
      lhs.swap(rhs);
   }
}//namespace std

#endif
