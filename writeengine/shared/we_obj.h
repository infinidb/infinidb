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
* $Id: we_obj.h 33 2006-08-17 10:31:20Z wzhou $
*
******************************************************************************************/
/** @file */

#ifndef _WE_OBJ_H_
#define _WE_OBJ_H_

//#include <we_type.h>
#include <we_macro.h>

/** Namespace WriteEngine */
namespace WriteEngine
{
   class Log;

/** Class WEObj */
class WEObj
{
public:
   /**
    * @brief Constructor
    */
   WEObj() : m_debugLevel( DEBUG_0 ), m_log( 0 ) {}

   /**
    * @brief Default Destructor
    */
   ~WEObj() {}

   /**
    * @brief Is it required to debug
    */
   const bool     isDebug( const DebugLevel level ) const { return level <= m_debugLevel; } 

   /**
    * @brief Get debug level
    */
   const DebugLevel   getDebugLevel() const { return m_debugLevel; } 

   /**
    * @brief Get Logger object
    */
   Log*         getLogger() const { return m_log; } 

   /**
    * @brief Set debug level
    */
   void           setDebugLevel( const DebugLevel level ) { m_debugLevel = level; } 

   /**
    * @brief Set debug logger and debug level
    */
   void           setLogger( Log* logger ) { m_log = logger; }

private:
   DebugLevel     m_debugLevel;              // internal use debug level
   Log*           m_log;                     // logger object for debug output
};


} //end of namespace
#endif // _WE_OBJ_H_
