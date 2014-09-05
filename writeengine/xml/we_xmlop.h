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
* $Id: we_xmlop.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_XMLOP_H_
#define _WE_XMLOP_H_

#include "we_xmltag.h"
#include "we_type.h"

/** Namespace WriteEngine */
namespace WriteEngine
{
   // Size of buffer used to parse non-string node content or attribute
   const int XML_NODE_BUF_SIZE = 256;

/** @brief Class wrapper around XML2  API.  Used by XmlGenProc to save Job XML
 *  file for input into cpimport.bin.
 *
 */
class XMLOp
{
protected:
    /** @brief XML types used in parsing XML content and attributes
     *
     */
    enum XML_DTYPE
    {
        TYPE_EMPTY    = 1,
        TYPE_CHAR     = 2,
        TYPE_DOUBLE   = 3,
        TYPE_FLOAT    = 4,
        TYPE_LONGLONG = 5,
        TYPE_INT      = 6
    };

    /**
     * @brief Constructor
     */
    XMLOp();

    /**
     * @brief Default Destructor
     */
    virtual ~XMLOp();

    /**
     * @brief Get node attribute for non-strings
     */
    bool getNodeAttribute( xmlNode* pNode, const char* pTag,
                           void* pVal, XML_DTYPE dataType ) const;

    /**
     * @brief Get node attribute for strings
     */
    bool getNodeAttributeStr( xmlNode* pNode, const char* pTag,
                           std::string& strVal ) const;

    /**
     * @brief Get node content for non-strings
     */
    bool getNodeContent( const xmlNode* pNode, void* pVal,
                         XML_DTYPE dataType );

    /**
     * @brief Get node content for strings
     */
    bool getNodeContentStr( const xmlNode* pNode, std::string& strVal);

    /**
     * @brief Check whether it is certain tag
     */
    bool isTag( const xmlNode* pNode, const xmlTag tag )
    { return !xmlStrcmp( pNode->name, (const xmlChar *)xmlTagTable[tag] ); }

    /**
     * @brief Parse xml document
     */
    int  parseDoc( const char* xmlFileName );

    /**
     * @brief Process node (recursion)
     */
    virtual bool processNode( xmlNode* pParentNode );

    xmlDocPtr      m_fDoc;                    // xml document pointer
    xmlNode*       m_pRoot;                   // root element

private:
    void           closeDoc();
    void           convertNodeValue( void* pVal, const char* buf,
                                     XML_DTYPE dataType ) const;
    int            readDoc( const char* xmlFileName );
};


} //end of namespace
#endif // _WE_XMLOP_H_
