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
*
* $Id: primproc.h 2035 2013-01-21 14:12:19Z rdempsey $
*
******************************************************************************************/
/**
 * @file
 */
#ifndef PRIMPROC_H
#define PRIMPROC_H

#include <string>
#include <sstream>
#include <exception>
#include <iostream>
#include <unistd.h>
#include <stdexcept>
#include <boost/thread.hpp>
#include <map>

#include "pp_logger.h"

namespace primitiveprocessor
{

#define SUMMARY_INFO( message ) \
        if ( isDebug(SUMMARY) ) \
        { \
            std::cout << message << std::endl; \
        }

#define SUMMARY_INFO2( message1, message2 ) \
	if ( isDebug(SUMMARY) ) \
	{ \
	  std::cout << message1 << message2 << std::endl; \
	}

#define SUMMARY_INFO3( message1, message2, message3 ) \
        if ( isDebug(SUMMARY) ) \
        { \
          std::cout << message1 << message2 << message3 << std::endl; \
        }

#define DETAIL_INFO( message ) \
        if ( isDebug(DETAIL) ) \
        { \
            std::cout << message << std::endl; \
        }

#define DETAIL_INFO2( message1, message2 ) \
        if ( isDebug(DETAIL) ) \
        { \
          std::cout << message1 << message2 << std::endl; \
        }

#define DETAIL_INFO3( message1, message2, message3 ) \
        if ( isDebug(DETAIL) ) \
        { \
          std::cout << message1 << message2 << message3 << std::endl; \
        }

#define VERBOSE_INFO( message ) \
        if ( isDebug(VERBOSE) ) \
        { \
            std::cout << message << std::endl; \
        }

#define VERBOSE_INFO2( message1, message2 ) \
        if ( isDebug(VERBOSE) ) \
        { \
          std::cout << message1 << message2 << std::endl; \
        }

#define VERBOSE_INFO3( message1, message2, message3 ) \
        if ( isDebug(VERBOSE) ) \
        { \
          std::cout << message1 << message2 << message3 << std::endl; \
        }

       enum DebugLevel                   /** @brief Debug level type enumeration */
       {
           NONE                    = 0,  /** @brief No debug info */
           STATS                   = 1,  /** @brief stats info */
           SUMMARY                 = 2,  /** @brief Summary level debug info */
           DETAIL                  = 3,  /** @brief A little detail debug info */
           VERBOSE                 = 4,  /** @brief Detailed debug info */
       };

       bool isDebug( const DebugLevel level );

       const int MAX_BUFFER_SIZE = 32768 * 2;

       // message log globals
       //const logging::LoggingID lid1(28);
       //extern logging::Message msg16;
       //extern logging::MessageLog ml1;
       //extern boost::mutex logLock;
       extern Logger* mlp;
}

#endif //PRIMPROC_H
