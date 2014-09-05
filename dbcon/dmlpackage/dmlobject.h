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
*   $Id: dmlobject.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */


#ifndef DMLOBJECT_H
#define DMLOBJECT_H
#include <string>
#include"bytestream.h"


namespace dmlpackage
{
/** @brief an abstract class that represents
  * a database object to be inserted, updated or
  * deleted by a DML statement
  */
class DMLObject
{

public:

    /**	@brief ctor
      */
    DMLObject();

    /** @brief dtor
      */
    virtual ~DMLObject();

    /** @brief read a DMLObject from a ByteStream
      * 
      *  @param bytestream the ByteStream to read from
      */
    virtual int read(messageqcpp::ByteStream& bytestream) = 0;

    /** @brief write a DMLObject to a ByteStream
      * 
      * @param bytestream the ByteStream to write to
      */
    virtual int write(messageqcpp::ByteStream& bytestream) = 0;

protected:

private:

};

}
#endif //DMLOBJECT_H


