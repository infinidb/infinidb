/* Copyright (C) 2013 Calpont Corp.

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
 *   $Id: dmlparser.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef DMLPARSER_H
#define DMLPARSER_H

#include <stdexcept>
#include "dmlpkg.h"

namespace dmlpackage
{
	typedef std::vector<char*> valbuf_t;

	
    typedef SqlStatementList ParseTree;

    /** @brief BISON parser wrapper class
     */
    class DMLParser
    {
        public:
            /** @brief ctor
             */
            DMLParser();

            /** @brief dtor
             */
            virtual ~DMLParser();

            /** @brief parse the supplied dml statement
             *
             * @param dmltext the dml statement to parse
             */
            int parse(const char* dmltext);

            /** @brief get the parse tree
             */
            const ParseTree& getParseTree();

			void setDefaultSchema(std::string schema);

            /** @brief was the parse successful
             */
            bool good();

            /** @brief put the parser in debug mode so as to dump
             * diagnostic information
             */
            void setDebug(bool debug);

        protected:
            ParseTree fParseTree;
            int fStatus;
            bool fDebug;

        private:

    };

    /** @brief specialization of the DMLParser class
     * specifically for reading the dml statement
     * from a file
     */
    class DMLFileParser : public DMLParser
    {
        public:
            /** @brief ctor
             */
            DMLFileParser();

            /** @brief parse the dml statement contained in the
             *	supplied file
             *
             * @param fileName the fully qualified file name to open
             * and parse the contents of
             */
            int parse(const std::string& fileName);

        protected:

        private:
    };

}
#endif                                            // DMLPARSER_H
