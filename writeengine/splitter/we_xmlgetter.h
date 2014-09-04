/*

   Copyright (C) 2009-2012 Calpont Corporation.

   Use of and access to the Calpont InfiniDB Community software is subject to the
   terms and conditions of the Calpont Open Source License Agreement. Use of and
   access to the Calpont InfiniDB Enterprise software is subject to the terms and
   conditions of the Calpont End User License Agreement.

   This program is distributed in the hope that it will be useful, and unless
   otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
   the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
   Please refer to the Calpont Open Source License Agreement and the Calpont End
   User License Agreement for more details.

   You should have received a copy of either the Calpont Open Source License
   Agreement or the Calpont End User License Agreement along with this program; if
   not, it is your responsibility to review the terms and conditions of the proper
   Calpont license agreement by visiting http://www.calpont.com for the Calpont
   InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
   the Calpont InfiniDB Community Calpont Open Source License Agreement.

   Calpont may make changes to these license agreements from time to time. When
   these changes are made, Calpont will make a new copy of the Calpont End User
   License Agreement available at http://www.calpont.com and a new copy of the
   Calpont Open Source License Agreement available at http:///www.infinidb.org.
   You understand and agree that if you use the Program after the date on which
   the license agreement authorizing your use has changed, Calpont will treat your
   use as acceptance of the updated License.

*/

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
	bool getNodeAttribute(const xmlNode* pNode,
								const char* pTag, char* pVal ) const;
	bool getNodeContent( const xmlNode* pNode, char* pVal) const;
	std::string getValue(const vector<string>& section) const;
	std::string getAttribute(const std::vector<string>& sections, const std::string& Tag) const;
	std::string getConfig(const std::string& section, const std::string& name) const;
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
