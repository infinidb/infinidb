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
*   $Id: ddlpackageprocessorfactory.h 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
/** @file */
#ifndef DDLPACKAGEPROCESSORFACTORY_H
#define DDLPACKAGEPROCESSORFACTORY_H
#include <string>
#include "ddlpkg.h"
#include "ddlpackageprocessor.h"


namespace ddlpackageprocessor { 

/** @brief create a ddlPackageProcessor object from a CalpontddlPackage object
 *
 */
class DDLPackageProcessorFactory {

public:

/** @brief static ddlPackageProcessor constructor method
  * 
  * @param packageType the ddl Package type
  * @param cpackage the CalpontddlPackage from which the ddlPackageProcessor is constructed
  */
  static DDLPackageProcessor* 
	makePackageProcessor( int packageType, ddlpackage::CalpontDDLPackage& cpackage );

protected:

private:
};

} 
#endif //DDLPACKAGEPROCESSORFACTORY_H

