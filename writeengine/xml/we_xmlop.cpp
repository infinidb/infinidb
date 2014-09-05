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
* $Id: we_xmlop.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "we_xmlop.h"

namespace WriteEngine
{

//------------------------------------------------------------------------------
// Constructor
//------------------------------------------------------------------------------
XMLOp::XMLOp( ) : m_fDoc( NULL ), m_pRoot( NULL )
{
}

//------------------------------------------------------------------------------
// Destructor
//------------------------------------------------------------------------------
XMLOp::~XMLOp()
{
    closeDoc();
    xmlCleanupParser();
}

//------------------------------------------------------------------------------
// Close xml doc
//------------------------------------------------------------------------------
void XMLOp::closeDoc() 
{
    if( m_fDoc != NULL ) {
        xmlFreeDoc( m_fDoc );
        m_fDoc = NULL;         
    }
}

//------------------------------------------------------------------------------
// Convert node value
// buf - data buffer
// dataType - data type
//------------------------------------------------------------------------------
void XMLOp::convertNodeValue( void* pVal, const char* buf,
    XML_DTYPE dataType ) const
{
    switch( dataType ) {
        case TYPE_CHAR      :  strcpy( (char*) pVal, buf ); break;
        case TYPE_DOUBLE    :  
        case TYPE_FLOAT     :  *((float*)pVal) = (float) atof( buf ); break;
        case TYPE_LONGLONG  :  *((long long*)pVal) = (long long) atoll( buf );
                               break;
        case TYPE_INT       :  
        default           :  *((int*)pVal) = (int) atoi( buf ); break;
    }
}

//------------------------------------------------------------------------------
// Get node attribute value
// pNode - current node
// pTag - compare tag
// dataType - column data type
// pVal (output) - return value buffer
// returns TRUE if found, FALSE otherwise
//------------------------------------------------------------------------------
bool XMLOp::getNodeAttribute( xmlNode* pNode, const char* pTag,
    void* pVal, XML_DTYPE dataType ) const
{
    char     buf[XML_NODE_BUF_SIZE];
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    pTmp = xmlGetProp( pNode, (xmlChar*) pTag );
    if( pTmp ) {
        bFound = true;
        strcpy( buf, (char*) pTmp );
        xmlFree( pTmp );

        convertNodeValue( pVal, buf, dataType );
    } // end if 

    return bFound;
}

//------------------------------------------------------------------------------
// Get node attribute value for strings
// pNode - current node
// pTag - compare tag
// dataType - column data type
// pVal (output) - return value buffer
// returns TRUE if found, FALSE otherwise
//------------------------------------------------------------------------------
bool XMLOp::getNodeAttributeStr( xmlNode* pNode, const char* pTag,
    std::string& strVal ) const
{
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    pTmp = xmlGetProp( pNode, (xmlChar*) pTag );
    if( pTmp ) {
        bFound = true;
        strVal = (char*)pTmp;
        xmlFree( pTmp );
    } // end if 

    return bFound;
}

//------------------------------------------------------------------------------
// Get node element content
// pNode - current node
// pVal - return value
// dataType - column data type
// returns TRUE if found, FALSE otherwise
//------------------------------------------------------------------------------
bool XMLOp::getNodeContent( const xmlNode* pNode, void* pVal,
    XML_DTYPE dataType )
{
    char     buf[XML_NODE_BUF_SIZE];
    xmlChar* pTmp = NULL;
    bool     bFound = false;

    if( pNode->children != NULL ) {
        pTmp = xmlNodeGetContent( pNode->children );
        if( pTmp ) {
            bFound = true;
            strcpy( buf, (char*) pTmp );
            xmlFree( pTmp );
            convertNodeValue( pVal, buf, dataType );
        }
    }

    return bFound;      
}

//------------------------------------------------------------------------------
// Get node element content for strings
// pNode - current node
// strVal - return value
// returns TRUE if found, FALSE otherwise
//------------------------------------------------------------------------------
bool XMLOp::getNodeContentStr( const xmlNode* pNode, std::string& strVal)
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
    }

    return bFound;      
}

//------------------------------------------------------------------------------
// Parse the document
// xmlFileName - xml file name
// returns NO_ERROR if success; other if failure
//------------------------------------------------------------------------------
int XMLOp::parseDoc( const char* xmlFileName ) 
{
    int   rc;

    rc = readDoc( xmlFileName );
    if( rc != NO_ERROR )
        return rc;

    return processNode( m_pRoot ) ? NO_ERROR : ERR_XML_PARSE;
}

//------------------------------------------------------------------------------
// Read from an xml file
// xmlFileName - xml file name
// returns NO_ERROR if success; other if failure
//------------------------------------------------------------------------------
int XMLOp::readDoc( const char* xmlFileName ) 
{
//  xmlNodePtr curPtr;
    m_fDoc = xmlParseFile( xmlFileName );

    if( m_fDoc == NULL )
        return ERR_XML_FILE;

    m_pRoot = xmlDocGetRootElement( m_fDoc );

    if( m_pRoot == NULL )
    {
        closeDoc();
        return ERR_XML_ROOT_ELEM;
    }

    return NO_ERROR;
}

//------------------------------------------------------------------------------
// Process a node
// pParentNode - parent node
// returns true if more nodes, else returns false.
//------------------------------------------------------------------------------
bool XMLOp::processNode( xmlNode* pParentNode )
{
    xmlNode* pCurNode = NULL;
    bool     bContinue = true;

    for( pCurNode = pParentNode->children; pCurNode; pCurNode = pCurNode->next)
        if( pCurNode->type == XML_ELEMENT_NODE) 
            bContinue = processNode( pCurNode );

    return bContinue;      
}

} //end of namespace
