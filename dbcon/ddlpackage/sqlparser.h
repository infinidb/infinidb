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
*   $Id: sqlparser.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file
 *
 * This contains a class wrapper for the Bison parsing machinery.
 */

#include <stdexcept>
#include "ddlpkg.h"

#if defined(_MSC_VER) && defined(xxxDDLPKGSQLPARSER_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace ddlpackage
{

typedef SqlStatementList ParseTree;

/** @brief SqlParser is a class interface around the Bison parser
 * machinery for DDL.
 * 
 * Example:
 *
 * @verbatim
 
     SqlParser parser;
     parser.Parse(sqlbuf);
 
     or
 
     SqlFileParser parser;
	 parser.setDefaultSchema("tpch");
     parser.Parse(sqlFileName);
 
     if (parser.Good()) {
       const ParseTree &ptree = parser.GetParseTree();
       cout << ptree.fList.size() << " " << "SQL statements" << endl;
       cout << ptree << endl;
     }
     else {
       cout << "Parser failed." << endl;
     }
 
  @endverbatim
 */


class SqlParser
{
public:
    EXPORT SqlParser(void);

    EXPORT virtual ~SqlParser();

    EXPORT int Parse(const char* sqltext);

    /** @brief Return the ParseTree if state is Good.  Otherwise
      *	throw a logic_error. 
      */
    EXPORT const ParseTree& GetParseTree(void);

    /** @brief Tells whether current state resulted from a good
      * parse. 
      */
    EXPORT bool Good(void);

	/** @brief Control bison debugging
	  */
    EXPORT void SetDebug(bool debug); 

	 /** @brief Set the default schema to use if it is not
       * supplied in the DDL statement
	   *
	   * @param schema the default schema
       */
    EXPORT void setDefaultSchema(std::string schema);

protected:
    ParseTree fParseTree;
    int fStatus; ///< return from yyparse() stored here.
    bool fDebug; ///< Turn on bison debugging.
};


/** SqlFileParser is a testing device.
  */
class SqlFileParser : public SqlParser
{
public:
    SqlFileParser();
    int Parse(const std::string& fileName);
};

}

#undef EXPORT
