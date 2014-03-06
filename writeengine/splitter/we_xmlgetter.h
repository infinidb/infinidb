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

/*******************************************************************************
* $Id$
*
*******************************************************************************/

/*
 * we_xmlgetter.h
 *
 *  Created on: Feb 7, 2012
 *      Author: bpaul
 */

#ifndef WE_XMLGETTER_H_
#define WE_XMLGETTER_H_

#include <libxml/parser.h>

namespace WriteEngine
{

class WEXmlgetter
{
public:
	WEXmlgetter(std::string& ConfigName);
	virtual ~WEXmlgetter();

public:
	//..Public methods
	std::string getValue(const vector<string>& section) const;
	std::string getAttribute(const std::vector<string>& sections,
		const std::string& Tag) const;
	void getConfig(const std::string& section,
		const std::string& name, std::vector<std::string>& values ) const;
	void getAttributeListForAllChildren(
		const vector<string>& sections,
		const string& attributeTag,
		vector<string>& attributeValues);

private:
	//..Private methods
	const xmlNode* getNode(const xmlNode* pParent,
		const std::string& section)const;
	bool getNodeAttribute(const xmlNode* pNode,
		const char* pTag, std::string& strVal ) const;
	bool getNodeContent( const xmlNode* pNode, std::string& strVal) const;

	//..Private data members
    std::string 	fConfigName;				// xml filename
    xmlDocPtr      	fDoc;                    	// xml document pointer
    xmlNode*       	fpRoot;                   	// root element
};

} /* namespace WriteEngine */
#endif /* WE_XMLGETTER_H_ */
