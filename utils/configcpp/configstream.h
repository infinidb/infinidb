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
* $Id$
*
******************************************************************************************/
/**
 * @file
 */
#ifndef CONFIG_CONFIGSTREAM_H_
#define CONFIG_CONFIGSTREAM_H_

#include <string>
#include <libxml/parser.h>

#include "bytestream.h"

#include "xmlparser.h"

namespace config
{

/** @brief a config ByteStream I/F class
 *
 */
class ConfigStream
{
public:
	ConfigStream(const messageqcpp::ByteStream& bs, const std::string& installDir);
	ConfigStream(const std::string& str, const std::string& installDir);
	ConfigStream(const char* cptr, const std::string& installDir);
	~ConfigStream();

	const std::string getConfig(const std::string& section, const std::string& name) const
		{ return fParser.getConfig(fDoc, section, name); }

private:
	ConfigStream(const ConfigStream& rhs);
	ConfigStream& operator=(const ConfigStream& rhs);

	void init(const xmlChar* xp);

	XMLParser fParser;
	xmlDocPtr fDoc;
};

} //namespace

#endif
// vim:ts=4 sw=4:

