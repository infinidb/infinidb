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
 *   $Id: inputmgr.h 2101 2013-01-21 14:12:52Z rdempsey $
 *
 ***********************************************************************/
/** @file */
#ifndef INPUTMGR_H
#define INPUTMGR_H

#include <string>
#include "we_xmlgendata.h"

namespace bulkloadxml
{

/** @brief Stores Input to colxml; used to generate Job XML file for cpimport.
 */
class InputMgr : public WriteEngine::XMLGenData
{
  public:

    /** @brief Constructor to manage colxml input.
     *
     * @param job is the Job Number
     */
    InputMgr(const std::string& job);
    virtual ~InputMgr( );
 
    /** @brief Specify parameter data to be written to Job XML file
     *
     * @param argc Number of arguments in argv
     * @param argv Input arguments
     * @return Return true if input accepted; false indicates an error
     */
    bool input(int argc, char **argv);

    /** @brief Load list of tables in system catalog for relevant schema.
     */
    bool loadCatalogTables();

    /** @brief Print contents of this object
     */
    virtual void print(std::ostream& os) const;
 
    friend std::ostream& operator<<(std::ostream& os, const InputMgr& m);

  private:
    void  printUsage();
    int   verifyArgument(char *arg);
};

}

#endif
