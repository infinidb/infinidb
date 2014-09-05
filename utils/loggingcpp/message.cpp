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
* $Id: message.cpp 3495 2013-01-21 14:09:51Z rdempsey $
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
#include "messageobj.h"

#include "installdir.h"

namespace {

mutex mx;
bool catalogLoaded = false;

typedef map<int, string> CatMap;

CatMap catmap;

void loadCatalog()
{
	Config* cf = Config::makeConfig();
	string configFile(cf->getConfig("MessageLog", "MessageLogFile"));
	if (configFile.length() == 0)
		configFile = startup::StartUp::installDir() + "/etc/MessageFile.txt";
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
					string msgtext = *tok_iter;
					catmap[msgid] = msgtext;
				}
			}
		}
		ios_base::iostate st = msgFile.rdstate();
		if ((st & ios_base::failbit) && !(st & ios_base::eofbit))
			msgFile.clear();
		(void)msgFile.get();
	}
}

}

namespace logging {

Message::Message(const MessageID msgid) :
	fMsgID(msgid), fMsg(lookupMessage(msgid)), fConfig(Config::makeConfig())
{
}

Message::Message(const string msg):
	fMsgID(0), fMsg(msg), fConfig(Config::makeConfig())
{
}

void Message::swap(Message& rhs)
{
	std::swap(fMsgID, rhs.fMsgID);
	std::swap(fMsg, rhs.fMsg);
	std::swap(fConfig, rhs.fConfig);
}

void Message::Args::add(int i)
{
	fArgs.push_back(long(i));
}

void Message::Args::add(uint64_t u64)
{
	fArgs.push_back(u64);
}

void Message::Args::add(const string& s)
{
	fArgs.push_back(s);
}

void Message::Args::add(double d)
{
	fArgs.push_back(d);
}

void Message::Args::reset()
{
	fArgs.clear();
}

void Message::format(const Args& args)
{
	Args::AnyVec::const_iterator iter = args.args().begin();
	Args::AnyVec::const_iterator end = args.args().end();

	boost::format fmt(fMsg);
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
			throw logic_error("Message::format: unexpected type in argslist");
		}
		++iter;
	}

	fMsg = fmt.str();
}

/* static */
const string Message::lookupMessage(const MessageID& msgid)
{
	if (!catalogLoaded)
	{
		mutex::scoped_lock lock(mx);
		if (!catalogLoaded)
		{
			loadCatalog();
			catalogLoaded = true;
		}
	}
	string msgstr;
	CatMap::const_iterator iter = catmap.find(msgid);
	if (iter == catmap.end())
	{
		iter = catmap.find(0);
		if (iter == catmap.end())
		{
			msgstr = "%1% %2% %3% %4% %5%";
		}
		else
		{
			msgstr = iter->second;
		}
	}
	else
	{
		msgstr = iter->second;
	}
	ostringstream oss;
	oss << "CAL" << setw(4) << setfill('0') << msgid << ": " << msgstr;
	return oss.str();
}

void Message::reset()
{
	fMsg = lookupMessage(fMsgID);
}

}

