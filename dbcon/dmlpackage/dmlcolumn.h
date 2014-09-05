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

/***********************************************************************
*   $Id: dmlcolumn.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */

#ifndef DMLCOLUMN_H
#define DMLCOLUMN_H
#include <string>
#include <vector>
#include "dmlobject.h"
#include "bytestream.h"
#include <boost/algorithm/string/case_conv.hpp>

#if defined(_MSC_VER) && defined(xxxDMLPKGCOLUMN_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackage
{
/** @brief concrete implementation of a DMLObject
  * Specifically for representing a database column
  */
class DMLColumn : public DMLObject
{

public:
    /** @brief ctor
      */
    EXPORT DMLColumn();

    /** @brief ctor
      */

	EXPORT DMLColumn(std::string name, std::string value, bool isFromCol = false, uint32_t funcScale=0, bool isNULL=false);
	/** @brief new ctor
	  * isNUll is currently not in use. It supposed to indicate whether each value is null or not.
      */

    EXPORT DMLColumn(std::string name, std::vector<std::string>& valueList, bool isFromCol = false, uint32_t funcScale=0, bool isNULL=false );

    /** @brief dtor
      */
    EXPORT ~DMLColumn();

    /** @brief read a DMLColumn from a ByteStream
      * 
      * @param bytestream the ByteStream to read from
      */
    EXPORT int read(messageqcpp::ByteStream& bytestream);

    /** @brief write a DML column to a ByteStream
      * 
      * @param bytestream the ByteStream to write to
      */
    EXPORT int write(messageqcpp::ByteStream& bytestream);

    /** @brief get the data for the column
      */
    const std::string get_Data() const { return fData; }
	
	
    const std::vector<std::string>& get_DataVector() const { return fColValuesList; }

   /** @brief get the data for the column
      */
    const bool get_isnull() const { return fisNULL; }
	/** @brief get the fIsFromCol data for the column
      */
    const bool get_isFromCol() const { return fIsFromCol; }
	/** @brief get the fFuncScale data for the column
      */
    const uint32_t get_funcScale() const { return fFuncScale; }
    /** @brief get the column name
      */
    const std::string get_Name() const { return fName; }
    /** @brief set the column name
      */
    EXPORT void  set_Name( std::string name) 
    { 	boost::algorithm::to_lower(name);
    	fName = name; }
    /** @brief set the NULL flag
      */
    void  set_isnull( bool isNULL) 
    { fisNULL = isNULL; }    
	/** @brief set the fIsFromCol flag
      */
    void  set_isFromCol( bool isFromCol) 
    { fIsFromCol = isFromCol; }   
	/** @brief set the fFuncScale
      */
	void  set_funcScale( uint32_t funcScale) 
    { fFuncScale = funcScale; }    
    void set_Data ( std::string data)
    { fData = data; }
	
	void set_DataVector ( std::vector<std::string>&  dataVec)
    { fColValuesList = dataVec; }

protected:

private:
    std::string fName;
    std::string fData;
	std::vector<std::string> fColValuesList;
    bool fisNULL;
	bool fIsFromCol;
	uint32_t fFuncScale;

};

/** @brief a vector of DMLColumns
  */
typedef std::vector<DMLColumn*>ColumnList;

}

#undef EXPORT

#endif //DMLCOLUMN_H

