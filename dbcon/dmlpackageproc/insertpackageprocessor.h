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
*   $Id: insertpackageprocessor.h 9473 2013-05-02 15:15:44Z dcathey $
*
*
***********************************************************************/
/** @file */

#ifndef INSERTPACKAGEPROCESSOR_H
#define INSERTPACKAGEPROCESSOR_H
#include <string>
#include <vector>
#include <boost/any.hpp>
#include "dmlpackageprocessor.h"
#include "dmltable.h"
#include "dataconvert.h"
#include "we_chunkmanager.h"

#if defined(_MSC_VER) && defined(DMLPKGPROC_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace dmlpackageprocessor
{
/** @brief concrete implementation of a DMLPackageProcessor.
  * Specifically for interacting with the Write Engine to
  * process INSERT dml statements.
  */
class InsertPackageProcessor : public DMLPackageProcessor
{

public:
	InsertPackageProcessor(BRM::DBRM* aDbrm, uint32_t sid) : DMLPackageProcessor(aDbrm, sid) {
	}
    /** @brief process an InsertDMLPackage
      *
      * @param cpackage the InsertDMLPackage to process
      */
    EXPORT DMLResult processPackage(dmlpackage::CalpontDMLPackage& cpackage);

protected:

private:

};

}

#undef EXPORT

#endif //INSERTPACKAGEPROCESSOR_H

