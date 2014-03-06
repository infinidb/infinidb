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
 *   $Id: updatepackageprocessor.h 9302 2013-03-07 16:06:59Z chao $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef UPDATEPACKAGEPROCESSOR_H
#define UPDATEPACKAGEPROCESSOR_H
#include <string>
#include "dmlpackageprocessor.h"
#include "dataconvert.h"
#include <vector>
#include "joblist.h"

#if defined(_MSC_VER) && defined(DMLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{
/** @brief concrete implementation of a DMLPackageProcessor.
 * Specifically for interacting with the Write Engine to
 * process UPDATE dml statements.
 */
class UpdatePackageProcessor : public DMLPackageProcessor
{

public:
	UpdatePackageProcessor(BRM::DBRM* aDbrm, uint32_t sid) : DMLPackageProcessor(aDbrm, sid) {
	}
    /** @brief process an UpdateDMLPackage
     *
     * @param cpackage the UpdateDMLPackage to process
     */
    EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);
	
protected:

private:
    /** @brief send execution plan to ExeMgr and fetch rows
     *
	 * @param cpackage the UpdateDMLPackage to process
	 * @param result the result of the operation
     * @return rows processed
     */
    uint64_t fixUpRows(dmlpackage::CalpontDMLPackage& cpackage, DMLResult& result, const uint64_t uniqueId, const uint32_t tableOid);


    /** @brief send row group to the PM to process
     *
	 * @param aRowGroup the row group to be sent
	 * @param result the result of the operation
     * @return the error code
     */
    bool processRowgroup(messageqcpp::ByteStream & aRowGroup, DMLResult& result, const uint64_t uniqueId, dmlpackage::CalpontDMLPackage& cpackage, std::map<unsigned, bool>& pmState, bool isMeta = false, uint32_t dbroot=1);
	bool receiveAll(DMLResult& result, const uint64_t uniqueId, std::vector<int>& fPMs, std::map<unsigned, bool>& pmState, const uint32_t tableOid);
	};

}

#undef EXPORT

#endif                                            //UPDATEPACKAGEPROCESSOR_H
