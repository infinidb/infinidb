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

// $Id$

/** @file
 *
 * File contains basic typedefs shared externally with other parts of the
 * system.  These types are placed here rather than in we_type.h in order
 * to decouple we_type.h and we_define.h from external components, so that
 * the other libraries won't have to recompile every time we change something
 * in we_type.h and/or we_define.h.
 */

#ifndef _WE_TYPEEXT_H_
#define _WE_TYPEEXT_H_
#include <stdint.h>
#include <sys/types.h>

/** Namespace WriteEngine */
namespace WriteEngine
{
   /************************************************************************
    * Type definitions
    ************************************************************************/
    typedef uint64_t           RID; // Row ID

   /************************************************************************
    * Dictionary related structure
    ************************************************************************/
    struct Token {
        uint64_t op       :  10;   // ordinal position within a block
        uint64_t fbo      :  36;   // file block number
        uint64_t spare    :  18;   // spare
        Token()                   // constructor, set to null value
        {
            op  = 0x3FE;
            fbo = 0xFFFFFFFFFLL;
            spare = 0x3FFFF;
        }
   };


} //end of namespace

#endif // _WE_TYPEEXT_H_
