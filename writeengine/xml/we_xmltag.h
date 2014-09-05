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
* $Id: we_xmltag.h 4450 2013-01-21 14:13:24Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_XMLTAG_H_
#define _WE_XMLTAG_H_
#include <libxml/parser.h>

/** Namespace WriteEngine */
namespace WriteEngine
{
   const int  MAX_XML_TAG_NAME_SIZE = 30; /** @brief Max size of xml tag name */

   enum xmlTag {
      // Element
      TAG_BULK_JOB,
      TAG_COLUMN,
      TAG_CREATE_DATE,
      TAG_CREATE_TIME,
      TAG_DEFAULT_COLUMN, 
      TAG_DELIMITER,
      TAG_DESC,
      TAG_ENCLOSED_BY_CHAR,
      TAG_ESCAPE_CHAR,
      TAG_ID,
      TAG_IGNORE_FIELD,
      TAG_NAME,
      TAG_PATH, // obsolete, but keep to be backwards compatible with old files
      TAG_SCHEMA,
      TAG_TABLE,
      TAG_TYPE, // obsolete, but kept to be backwards compatible with old files
      TAG_USER,
      TAG_READ_BUFFERS,
      TAG_WRITE_BUFFER_SIZE,

      // Attributes
      TAG_NO_OF_READ_BUFFERS,
      TAG_READ_BUFFER_SIZE,
      TAG_COL_NAME,
      TAG_COL_OID,
      TAG_COL_TYPE,
      TAG_COMPRESS_TYPE,
      TAG_DATA_TYPE,
      TAG_AUTOINCREMENT_FLAG,
      TAG_DVAL_OID,
      TAG_LOAD_NAME,
      TAG_MAX_ERR_ROW,
      TAG_NOT_NULL,
      TAG_DEFAULT_VALUE,
      TAG_ORIG_NAME, //@bug 3599: deprecated; kept for backwards compatibility
      TAG_PRECISION,
      TAG_SCALE,
      TAG_TBL_NAME,
      TAG_TBL_OID,
      TAG_WIDTH,
      TAG_SCHEMA_NAME,
      NUM_OF_XML_TAGS
   };

   const char xmlTagTable[NUM_OF_XML_TAGS + 1][MAX_XML_TAG_NAME_SIZE] = {
      // Elements
      "BulkJob",
      "Column",
      "CreateDate",
      "CreateTime",
      "DefaultColumn",
      "Delimiter",
      "Desc",
      "EnclosedByChar",
      "EscapeChar",
      "Id",
      "IgnoreField",
      "Name",
      "Path",     //@bug 3777: keep obsolete tag
      "Schema",
      "Table",
      "Type",     //@bug 3777: keep obsolete tag
      "User",
      "ReadBuffers",
      "WriteBufferSize",

      // Attributes
      "count",
      "size",
      "colName",  //@bug 3599: replaces origName
      "colOid",
      "colType",
      "compressType",
      "dataType",
      "autoincrement",
      "dValOid",
      "loadName",
      "maxErrRow",
      "notnull",
      "defaultValue",
      "origName", //@bug 3599: deprecated; kept for backwards compatibility
      "precision",
      "scale",
      "tblName",  //@bug 3599: replaces origName
      "tblOid",
      "width",
      "Name"
   };

} //end of namespace
#endif // _WE_XMLTAG_H_
