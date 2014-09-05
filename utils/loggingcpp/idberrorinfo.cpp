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
* $Id: idberrorinfo.cpp 3626 2013-03-11 15:36:08Z xlou $
*
******************************************************************************************/
#include <iostream>
#include <iomanip>
#include <string>
#include <iterator>
#include <sstream>
#include <stdexcept>
#include <map>
#include <fstream>
using namespace std;

#include <boost/format.hpp>
#include <boost/tokenizer.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;
#include "loggingid.h"
#include "logger.h"
#include "idberrorinfo.h"

#include "installdir.h"

namespace logging {

IDBErrorInfo* IDBErrorInfo::fInstance = 0;
mutex mx;

IDBErrorInfo* IDBErrorInfo::instance()
{
	mutex::scoped_lock lk(mx);
	if (!fInstance)
		fInstance = new IDBErrorInfo();
	return fInstance;
}

IDBErrorInfo::IDBErrorInfo()
{
	Config* cf = Config::makeConfig();
	string configFile(cf->getConfig("SystemConfig", "ErrorMessageFile"));
	if (configFile.length() == 0)
		configFile = startup::StartUp::installDir() + "/etc/ErrorMessage.txt";
	ifstream msgFile(configFile.c_str());
	while (msgFile.good())
	{
		stringbuf* sb = new stringbuf;
		msgFile.get(*sb);
		string m = sb->str();
		delete sb;
		if (m.length() > 0 && m[0] != '#')
		{
			typedef boost::tokenizer<boost::char_separator<char> > tokenizer;
			boost::char_separator<char> sep("\t");
			tokenizer tokens(m, sep);
			tokenizer::iterator tok_iter = tokens.begin();
			if (tok_iter != tokens.end())
			{
				int msgid = atoi(tok_iter->c_str());
				++tok_iter;
				if (tok_iter != tokens.end())
				{
					++tok_iter;
					if (tok_iter != tokens.end())
					{
						string msgtext = *tok_iter;
						fErrMap[msgid] = msgtext;
					}
				}
			}
		}
		ios_base::iostate st = msgFile.rdstate();
		if ((st & ios_base::failbit) && !(st & ios_base::eofbit))
			msgFile.clear();
		(void)msgFile.get();
	}
}

IDBErrorInfo::~IDBErrorInfo()
{
}

string IDBErrorInfo::errorMsg(const unsigned eid, const Message::Args& args)
{
	string errMsg = lookupError(eid);
	format(errMsg, args);
	return errMsg;
}

string IDBErrorInfo::errorMsg(const unsigned eid)
{
	string errMsg = lookupError(eid);
	Message::Args args; // empty args
	format(errMsg, args);
	return errMsg;
}

string IDBErrorInfo::errorMsg(const unsigned eid, int i)
{
	string errMsg = lookupError(eid);
	Message::Args args;
	args.add(i);
	format(errMsg, args);
	return errMsg;
}

string IDBErrorInfo::errorMsg(const unsigned eid, const string& s)
{
	string errMsg = lookupError(eid);
	Message::Args args;
	args.add(s);
	format(errMsg, args);
	return errMsg;
}

string IDBErrorInfo::logError(const logging::LOG_TYPE logLevel,
                  const logging::LoggingID logid,
                  const unsigned eid,
                  const logging::Message::Args& args)
{
	Logger logger(logid.fSubsysID);
	Message message(errorMsg(eid, args));
	return logger.logMessage(logLevel, message, logid);
}	              	

void IDBErrorInfo::format(string& errMsg, const Message::Args& args)
{
	Message::Args::AnyVec::const_iterator iter = args.args().begin();
	Message::Args::AnyVec::const_iterator end = args.args().end();

	boost::format fmt(errMsg);
	fmt.exceptions(boost::io::no_error_bits);

	while (iter != end)
	{
		if (iter->type() == typeid(long))
		{
			long l = any_cast<long>(*iter);
			fmt % l;
		}
		else if (iter->type() == typeid(uint64_t))
		{
			uint64_t u64 = any_cast<uint64_t>(*iter);
			fmt % u64;
		}
		else if (iter->type() == typeid(double))
		{
			double d = any_cast<double>(*iter);
			fmt % d;
		}
		else if (iter->type() == typeid(string))
		{
			string s = any_cast<string>(*iter);
			fmt % s;
		}
		else
		{
			throw logic_error("IDBErrorInfo::format: unexpected type in argslist");
		}
		++iter;
	}

	errMsg = fmt.str();
}

/* static */
string IDBErrorInfo::lookupError(const unsigned eid)
{
	string msgstr;
	ErrorMap::const_iterator iter = fErrMap.find(eid);
	if (iter == fErrMap.end())
		msgstr = "Unknown Error %1% %2% %3% %4% %5%";
	else
		msgstr = iter->second;
	ostringstream oss;
	oss << "IDB-" << setw(4) << setfill('0') << eid << ": " << msgstr;
	return oss.str();
}

}


