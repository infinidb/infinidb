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
#include "config.h"

#include <string>
#include <libxml/xmlmemory.h>
#include <libxml/parser.h>
#include <stdexcept>
using namespace std;

#include "bytestream.h"
using namespace messageqcpp;

#include "configstream.h"

namespace config
{

ConfigStream::ConfigStream(const ByteStream& bs, const string& installDir) :
	fParser(installDir)
{
	init(reinterpret_cast<const xmlChar*>(bs.buf()));
}

ConfigStream::ConfigStream(const string& str, const string& installDir) :
	fParser(installDir)
{
	init(reinterpret_cast<const xmlChar*>(str.c_str()));
}

ConfigStream::ConfigStream(const char* cptr, const string& installDir) :
	fParser(installDir)
{
	init(reinterpret_cast<const xmlChar*>(cptr));
}

ConfigStream::~ConfigStream()
{
	if (fDoc != NULL)
		xmlFreeDoc(fDoc);
}

void ConfigStream::init(const xmlChar* xp)
{
	fDoc = xmlParseDoc(xp);
	if (fDoc == NULL)
		throw runtime_error("ConfigStream::ConfigStream: bad XML stream");
}

} //namespace
// vim:ts=4 sw=4:

