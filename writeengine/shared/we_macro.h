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

/******************************************************************************************
* $Id: we_macro.h 33 2006-10-18 14:37:12Z wzhou $
*
******************************************************************************************/
/** @file */

#ifndef _WE_MACRO_H_
#define _WE_MACRO_H_

#include <we_type.h>


/** Namespace WriteEngine */
namespace WriteEngine
{
#define	RETURN_ON_ERROR( statement )    \
        {   int rcVal = (statement);    \
            if( rcVal != NO_ERROR )     \
               return rcVal;       }

#define	RETURN_ON_NULL( obj, rc )    \
            if( obj == NULL )  \
               return rc;

#define	RETURN_ON_WE_ERROR( oldRc, newRc )    \
            if( oldRc != NO_ERROR )          \
               return newRc;

#define	RETURN_RC( oldRc, newRc )    \
            if( oldRc != NO_ERROR )   \
               return newRc;          \
            else                      \
               return NO_ERROR;

} //end of namespace
#endif // _WE_MACRO_H_
