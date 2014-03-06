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
// WEXmlgetter constructor
//------------------------------------------------------------------------------
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
// WEXmlgetter destructor
//------------------------------------------------------------------------------
WEXmlgetter::~WEXmlgetter()
{
	xmlFreeDoc( fDoc );
    fDoc = NULL;
}

//------------------------------------------------------------------------------
// Get/return the property or attribute value (strVal) for the specified xml tag
// (pNode) and property/attribute (pTag)
//------------------------------------------------------------------------------
bool WEXmlgetter::getNodeAttribute(const xmlNode* pNode,
									const char* pTag, std::string& strVal) const
{
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    pTmp = xmlGetProp( const_cast<xmlNode*>(pNode), (xmlChar*) pTag );
    if( pTmp ) {
        bFound = true;
        strVal = (char*)pTmp;
        xmlFree( pTmp );
    }
    else {
        strVal.clear();
    } // end if

    return bFound;
}

//------------------------------------------------------------------------------
// Get/return the node content (strVal) for the specified xml tag (pNode)
//------------------------------------------------------------------------------
bool WEXmlgetter::getNodeContent( const xmlNode* pNode,
    std::string& strVal) const
{
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    if( pNode->children != NULL ) {
        pTmp = xmlNodeGetContent( pNode->children );
        if( pTmp ) {
            bFound = true;
            strVal = (char*)pTmp;
            xmlFree( pTmp );
        }
        else {
            strVal.clear();
        }
    }
    else {
        strVal.clear();
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
		//cout << "pPtr->name:    " <<
		//	(const xmlChar*)pPtr->name << std::endl;

        if ((!xmlStrcmp(pPtr->name, (const xmlChar *)section.c_str())))
        {
            xmlNodePtr pPtr2 = pPtr->xmlChildrenNode;
            while (pPtr2 != NULL)
            {
				//cout << "  pPtr2->name: " <<
				//	(const xmlChar*)pPtr2->name << std::endl;

                if ((!xmlStrcmp(pPtr2->name, (const xmlChar*)name.c_str())))
                {
                    xmlNodePtr pPtr3 = pPtr2->xmlChildrenNode;
                    values.push_back((const char*)pPtr3->content);

					//cout << "    pPtr3->name: " <<
					//	(const xmlChar*)pPtr3->name <<
					//	"; content: " << (const xmlChar*)pPtr3->content <<
					//	"; len: " << strlen((char*)pPtr3->content) << std::endl;
                }
                pPtr2 = pPtr2->next;
            }
        }
        pPtr = pPtr->next;
    }
}

//------------------------------------------------------------------------------
// Returns node content for the last node in the node tree defined by
// "sections".  So if sections[] were:
//   sections[0] = "house" 
//   sections[1] = "room"
// Then this function would return the node content for the first <room>
// tag found under the first <house> tag.
// Function assumes that the desired node has no children nodes other than
// the text content node.
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
		std::string aBuff;
		if(getNodeContent(pPtr, aBuff)) aRet = aBuff;
	}

	return aRet;
}

//------------------------------------------------------------------------------
// Iterate through the sibling nodes starting with pParent, looking for
// a node with the specified name (section).  The xmlNode (if found) is
// returned.
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
// Iterate down through the node tree represented by the sections vector.
// In the last child of this tree, we look for the specified attribute tag,
// and return its value.
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
		std::string aBuff;
		//cout << "attrTagNode Name " << (const char*)pPtr->name << endl;
		if (getNodeAttribute(pPtr, Tag.c_str(), aBuff))
			aRet = aBuff;
		//aRet = (const char*)pPtr->content;
		//cout << "Attribute("<<Tag<<") = "<< aRet<< endl;
	}

	return aRet;
}

//------------------------------------------------------------------------------
// Iterate down through the node tree represented by the sections vector.
// At the end of the branch, there may be several sibling nodes matching
// the node search vector.
// For each of the matching children nodes found, we look for the specified
// attribute tag, and return its value.  Hence a vector of attribute values
// is returned.
//------------------------------------------------------------------------------
void WEXmlgetter::getAttributeListForAllChildren(
	const vector<string>& sections,
	const string& attributeTag,
	vector<string>& attributeValues)
{
    const xmlNode* pPtr = fpRoot;
	int aSize = sections.size();
	if(aSize==0)
	{
		throw invalid_argument("WEXmlgetter::getAttributeListForAllChildren():"
			" No XML nodes specified in section search list");
	}

	// Step down the branch that has the nodes of interest
	int aIdx = 0;
	while (aIdx < aSize)
	{
		pPtr = getNode(pPtr, sections[aIdx]);
		if ((pPtr == NULL) || (aIdx == aSize-1))
		{
			break;
		}
		else
		{
			pPtr = pPtr->xmlChildrenNode;
			aIdx++;
		}
	}

	// Look for all the "matching" nodes at the end of the branch, and 
	// get the requested attribute value for each matching node.
	if (pPtr != NULL)
	{
    	while(pPtr != NULL )
    	{
			std::string attrib;
			if (getNodeAttribute(pPtr, attributeTag.c_str(), attrib))
			{
				attributeValues.push_back(attrib);
			}

   			pPtr = pPtr->next;
		}
    }
}

} /* namespace WriteEngine */
