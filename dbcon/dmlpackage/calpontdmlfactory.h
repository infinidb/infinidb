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
 *   $Id: calpontdmlfactory.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */
#ifndef CALPONTDMLFACTORY_H
#define CALPONTDMLFACTORY_H
#include <string>
#include "dmlpackage.h"
#include "calpontdmlpackage.h"
#include "vendordmlstatement.h"
#include <boost/thread.hpp>

#if defined(_MSC_VER) && defined(xxxCALPONTDMLFACTORY_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif
namespace dmlpackage
{

class CalpontDMLFactory
{
    /** @brief a concrete implementation responsible for creating
     * the proper concrete realization of a CalpontDMLPackage
     * given a VendorDMLStatement.
     */
public:

    /** @brief factory method
     *
     * @param vpackage the VendorDMLStatement
     * @param defaultSchema the default schema to be used for DML statements
     */
    EXPORT static dmlpackage::CalpontDMLPackage* makeCalpontDMLPackage (dmlpackage::VendorDMLStatement& vpackage,
            std::string defaultSchema = "" );

    /** @brief old factory method!
     *
     * @param vpackage the VendorDMLStatement
     */
    EXPORT static dmlpackage::CalpontDMLPackage* makeCalpontDMLPackageFromBuffer(dmlpackage::VendorDMLStatement& vpackage);
	
	EXPORT static dmlpackage::CalpontDMLPackage* makeCalpontDMLPackageFromMysqlBuffer(dmlpackage::VendorDMLStatement& vpackage);
	static dmlpackage::CalpontDMLPackage* makeCalpontUpdatePackageFromMysqlBuffer(dmlpackage::VendorDMLStatement& vpackage, dmlpackage::UpdateSqlStatement& updateStmt);

protected:

private:
	static boost::mutex fParserLock;
};

}

#undef EXPORT

#endif                                            //CALPONTDMLFACTORY_H
