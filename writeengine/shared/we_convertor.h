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
* $Id: we_convertor.h 2873 2011-02-08 14:35:57Z rdempsey $
*
*******************************************************************************/
/** @file */

#ifndef _WE_CONVERTOR_H_
#define _WE_CONVERTOR_H_

#include <time.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h> 
#include <string>

#include <iostream>
#include <fstream>

#include <dm.h>

#include "we_obj.h"
#include "we_config.h"

#if defined(_MSC_VER) && defined(WRITEENGINECONVERTOR_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{
const char DATE_TIME_FORMAT[] = "%04d-%02d-%02d %02d:%02d:%02d";

/** Class Convertor */
class Convertor 
{
public:
   /**
    * @brief Constructor
    */
   Convertor(){}

   /**
    * @brief Default Destructor
    */
   ~Convertor(){}

   /**
    * @brief Get time string 
    */
   EXPORT static const std::string   getTimeStr();

   /**
    * @brief Convert float value to a string 
    */
   EXPORT static const std::string   float2Str(const float val);

   /**
    * @brief Convert int value to a string
    */
   EXPORT static const std::string   int2Str(const int val);

   /**
    * @brief Convert unsigned 64 bit integer to a string
    */
   EXPORT static const std::string   i64ToStr(const i64 val);

   /**
    * @brief Convert an oid to a full file name (with partition and segment
    *        being included in the filename).  This is used for all column and
    *        dictionary store db files.  If dealing with a version buffer file,
    *        a partition and segment number of 0 should be used.
    */
   EXPORT static const int oid2FileName(const FID fid, char* fullFileName,
      char dbDirName[][MAX_DB_DIR_NAME_SIZE],
      const uint32_t partition, const uint16_t segment);

   /**
    * @brief Convert errno to associated error msg string
    */
   EXPORT static void mapErrnoToString(int errNum, std::string& errString);

   /**
    * @brief Convert interface column type to a internal column type
    *
    * @param dataType Interface data-type
    * @param internalType Internal data-type used for storing
    */
   //BUG931
   EXPORT static void convertColType(ColDataType dataType,ColType& internalType,bool isToken=false);

   /**
    * @brief Convert interface column type to a internal column type
    */
   EXPORT static void convertColType(void* curStruct, const FuncType curType = FUNC_WRITE_ENGINE);

   /***********************************************************
    * @brief Get the correct width for a row
    */
   EXPORT static int getCorrectRowWidth( const ColDataType dataType, const int width );

};

} //end of namespace

#undef EXPORT

#endif // _WE_CONVERTOR_H_
