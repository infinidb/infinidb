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
 * we_xmlgetter.cpp
 *
 *  Created on: Feb 7, 2012
 *      Author: bpaul
 */


#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <stdexcept>
#include <iostream>

#include <string>
#include <vector>
using namespace std;

#include "we_xmlgetter.h"

using namespace std;

namespace WriteEngine
{

//------------------------------------------------------------------------------
// constructor stub
WEXmlgetter::WEXmlgetter(std::string& ConfigName):
		fConfigName(ConfigName),
		fDoc( NULL ),
		fpRoot( NULL )
{
	//  xmlNodePtr curPtr;
	fDoc = xmlParseFile( ConfigName.c_str() );
	if(fDoc == NULL )
		throw runtime_error("WEXmlgetter::getConfig(): no XML document!");

	fpRoot = xmlDocGetRootElement( fDoc );
	if( fpRoot == NULL )
	{
		xmlFreeDoc( fDoc );
	    fDoc = NULL;
	    throw runtime_error("WEXmlgetter::getConfig(): no XML Root Tag!");
	}
}

//------------------------------------------------------------------------------

WEXmlgetter::~WEXmlgetter()
{
	xmlFreeDoc( fDoc );
    fDoc = NULL;
}

//------------------------------------------------------------------------------
/*
 * pNode	Node
 * pTag		Attribute Name
 *
 */
bool WEXmlgetter::getNodeAttribute(const xmlNode* pNode,
											const char* pTag, char* pVal) const
{
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    pTmp = xmlGetProp( const_cast<xmlNode*>(pNode), (xmlChar*) pTag );
    if( pTmp ) {
        bFound = true;
        strncpy( pVal, (char*) pTmp, 256);
        *(pVal+255) = 0;
        xmlFree( pTmp );
    } // end if

    return bFound;
}

//------------------------------------------------------------------------------

bool WEXmlgetter::getNodeContent( const xmlNode* pNode, char* pVal) const
{
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    if( pNode->children != NULL ) {
        pTmp = xmlNodeGetContent( pNode->children );
        if( pTmp ) {
            bFound = true;
            strncpy( pVal, (char*) pTmp, 256);
            *(pVal+255) = 0;
            xmlFree( pTmp );
        }
    }

    return bFound;
}

//------------------------------------------------------------------------------
// Get/returns node content for the "first" child node under each section/name.
// Example:
//   <section>
//     <name>
//       <subname1>
//       </subname1>
//     </name>
//     <name>
//       <subname1>
//       </subname1>
//     </name>
//   </section>
// 
// Looks like xml2 is currently returning the text node as the first child
// node under a node.  So in the example above, this function is currently
// always returning the text node content inside each <name> rather than
// any <subname1> node that might be within each <name> tag.
//------------------------------------------------------------------------------
void WEXmlgetter::getConfig(const string& section,
								const string& name, vector<string>& values) const
{
    string res;
    if (section.length() == 0)
        throw invalid_argument("Config::getConfig: section must have a length");

    xmlNode* pPtr = fpRoot->xmlChildrenNode;
    while (pPtr != NULL)
    {
        if ((!xmlStrcmp(pPtr->name, (const xmlChar *)section.c_str())))
        {
            xmlNodePtr pPtr2 = pPtr->xmlChildrenNode;
            while (pPtr2 != NULL)
            {
                if ((!xmlStrcmp(pPtr2->name, (const xmlChar*)name.c_str())))
                {
                        xmlNodePtr pPtr3 = pPtr2->xmlChildrenNode;
                        values.push_back((const char*)pPtr3->content);
                }
                pPtr2 = pPtr2->next;
            }
        }
        pPtr = pPtr->next;
    }

}

//------------------------------------------------------------------------------

std::string WEXmlgetter::getValue(const vector<string>& sections) const
{
	std::string aRet;
    const xmlNode* pPtr = fpRoot;
	int aSize = sections.size();
	int aIdx = 0;
	//cout << aSize << endl;
	while(aIdx < aSize)
	{
		//cout << aIdx <<" "<< sections[aIdx] << endl;
		pPtr = getNode(pPtr, sections[aIdx]);
		if((pPtr == NULL) || (aIdx == aSize-1)) break;
		else
		{
			//cout << "getValue Name " << (const char*)pPtr->name << endl;
			pPtr = pPtr->xmlChildrenNode;
			aIdx++;
		}
	}

	if(pPtr != NULL)
	{
		//aRet = (const char*)pPtr->content;
		char aBuff[256];
		if(getNodeContent(pPtr, aBuff)) aRet = aBuff;
	}
	return aRet;
}

//------------------------------------------------------------------------------
const xmlNode* WEXmlgetter::getNode(const xmlNode* pParent,
												const string& section)const
{
	if(pParent == NULL) return NULL;
    const xmlNode* pPtr = pParent;
    while(pPtr != NULL )
    {
    	//cout << "getNode Name " << (const char*)pPtr->name << endl;
    	if(!xmlStrcmp(pPtr->name, (const xmlChar *)section.c_str()))
    		return pPtr;
    	else
    		pPtr = pPtr->next;
    }
    return pPtr;
}


//------------------------------------------------------------------------------

std::string WEXmlgetter::getAttribute(const vector<string>& sections,
														const string& Tag) const
{
	std::string aRet;
    const xmlNode* pPtr = fpRoot;
	int aSize = sections.size();
	if(aSize==0)
		throw invalid_argument("WEXmlgetter::getAttribute(): section must be valid");
	int aIdx = 0;
	//cout << aSize << endl;
	while(aIdx < aSize)
	{
		//cout << aIdx <<" "<< sections[aIdx] << endl;
		pPtr = getNode(pPtr, sections[aIdx]);
		if((pPtr == NULL) || (aIdx == aSize-1)) break;
		else
		{
			//cout << "getValue Name " << (const char*)pPtr->name << endl;
			pPtr = pPtr->xmlChildrenNode;
			aIdx++;
		}
	}

	if(pPtr != NULL)
	{
		char aBuff[256];
		//cout << "attrTagNode Name " << (const char*)pPtr->name << endl;
		getNodeAttribute(pPtr, Tag.c_str(), aBuff);
		aRet = aBuff;
		//aRet = (const char*)pPtr->content;
		//cout << "Attribute("<<Tag<<") = "<< aRet<< endl;
	}
	return aRet;
}

//------------------------------------------------------------------------------



} /* namespace WriteEngine */

