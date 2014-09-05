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
 *   $Id: createindexprocessor.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef CREATEINDEXPROCESSOR_H
#define CREATEINDEXPROCESSOR_H

#include "ddlpackageprocessor.h"
#include "ddlpkg.h"
const int BULK_LOAD = 1;
namespace ddlpackageprocessor
{
    /** @brief specialization of a DDLPackageProcessor
     * for interacting with the Write Engine to process
     * create index ddl statements
     */
    class CreateIndexProcessor : public DDLPackageProcessor
    {
        public:
            /** @brief process a create index statement
             *
             * @param createIndexStmt the create index statement
             */
            DDLResult processPackage(ddlpackage::CreateIndexStatement& createIndexStmt);

        protected:
	    DDLResult rollBackCreateIndex(const std::string& error, BRM::TxnID& txnID, int sessionId);
	    void rollBackIndex(BRM::TxnID& txnID);
	    std::string  errorString(const std::string& msg, int error);
        private:

    };

}                                                 //namespace ddlpackageprocessor
#endif                                            //CREATEINDEXPROCESSOR_H
