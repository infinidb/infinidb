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
#ifndef CONFIG_XMLPARSER_H_
#define CONFIG_XMLPARSER_H_

#include <string>
#include <vector>
#include <libxml/parser.h>

namespace config
{

/** class XMLParser */
class XMLParser
{
public:
	XMLParser(const std::string& installDir) : fInstallDir(installDir) { }
	~XMLParser() { }

	const std::string getConfig(const xmlDocPtr doc, const std::string& section, const std::string& name) const;

	void getConfig(const xmlDocPtr doc, const std::string& section, const std::string& name,
		std::vector<std::string>& values) const;

	void setConfig(xmlDocPtr doc, const std::string& section, const std::string& name,
		const std::string& value);

	void delConfig(xmlDocPtr doc, const std::string& section, const std::string& name);

	const std::vector<std::string> enumConfig(const xmlDocPtr doc) const;

	const std::vector<std::string> enumSection(const xmlDocPtr doc, const std::string& section) const;

private:
	//defaults okay
	//XMLParser(const XMLParser& rhs);
	//XMLParser& operator=(const XMLParser& rhs);

	/** @brief expand macros in config file to actual values
	*/
	const std::string expand(const std::string& in) const;

	const std::string fInstallDir;

};

} //namespace

#endif
// vim:ts=4 sw=4:

