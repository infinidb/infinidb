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
* $Id: we_convertor.h 4726 2013-08-07 03:38:36Z bwilkinson $
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

#include "we_obj.h"
#include "we_config.h"
#include "calpontsystemcatalog.h"
#if defined(_MSC_VER) && defined(WRITEENGINE_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

/** Namespace WriteEngine */
namespace WriteEngine
{

/** Class Convertor */
class Convertor 
{
public:
   /**
    * @brief Default Constructor
    */
    Convertor(){}

   /**
    * @brief Destructor
    */
    ~Convertor(){}

   /**
    * @brief Get date/time string based on current date and time
    */
    EXPORT static const std::string   getTimeStr();

   /**
    * @brief Convert specified integer value to a string
    *
    * @param val Integer value to be converted to a string
    */
    EXPORT static const std::string   int2Str(int val);

   /**
    * @brief Convert an oid to a full file name (with partition and segment
    *        being included in the filename).  This is used for all column and
    *        dictionary store db files.  If dealing with a version buffer file,
    *        a partition and segment number of 0 should be used.
    */
    EXPORT static int oid2FileName(FID fid, char* fullFileName,
        char dbDirName[][MAX_DB_DIR_NAME_SIZE],
        uint32_t partition, uint16_t segment);

   /**
    * @brief Convert specified errno to associated error msg string
    *
    * @param errNum System errno to be converted.
    * @param errString Error msg string associated with the specified errno.
    */
    EXPORT static void mapErrnoToString(int errNum, std::string& errString);

   /**
    * @brief Convert specified ColDataType to internal storage type (ColType)
    *
    * @param dataType Interface data-type
    * @param internalType Internal data-type used for storing
    */
    //BUG931
    EXPORT static void convertColType(execplan::CalpontSystemCatalog::ColDataType dataType,
        ColType& internalType, bool isToken=false);
   /**
    * @brief Convert specified internal storage type (ColType) to 
    *        ColDataType
    *
    * @param internalType Internal data-type used for storing
    * @param dataType Interface data-type
    */
    EXPORT static void convertWEColType(ColType internalType, 
        execplan::CalpontSystemCatalog::ColDataType& dataType);

   /**
    * @brief Convert interface column type to a internal column type.
    * curStruct is interpreted as a ColStruct.
    */
    EXPORT static void convertColType(ColStruct* curStruct);

   /*
    * @brief Get the correct width for a row
    */
    EXPORT static int getCorrectRowWidth( execplan::CalpontSystemCatalog::ColDataType dataType, int width );

   /*
    * @brief Convert a Decimal string to it's equivalent integer value.
    *        errno can be checked upon return to see if input value was
    *        out of range (ERANGE).
    *
    * field      decimal string to be converted
    * fieldLengh length of "field" in bytes
    * scale      decimal scale to be applied to value
    */
    EXPORT static long long convertDecimalString ( const char* field,
        int         fieldLength,
        int         scale );

private:

   struct dmFilePathArgs_t;
   static int dmOid2FPath(uint32_t oid, uint32_t partition, uint32_t segment,
       dmFilePathArgs_t* pArgs);

};

} //end of namespace

#undef EXPORT

#endif // _WE_CONVERTOR_H_
