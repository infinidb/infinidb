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
*   $Id: dmlpackageprocessorfactory.cpp 9210 2013-01-21 14:10:42Z rdempsey $
*
*
***********************************************************************/
#define DMLPKGPROCFACTORY_DLLEXPORT
#include "dmlpackageprocessorfactory.h"
#undef DMLPKGPROCFACTORY_DLLEXPORT
#include "dmlpackage.h"
#include "insertpackageprocessor.h"
#include "updatepackageprocessor.h"
#include "deletepackageprocessor.h"
#include "commandpackageprocessor.h"

using namespace dmlpackage;

namespace dmlpackageprocessor {

DMLPackageProcessor*  DMLPackageProcessorFactory::
makePackageProcessor(int packageType, dmlpackage::CalpontDMLPackage& cpackage)
{
	DMLPackageProcessor* dmlProcPtr = 0;

	switch( packageType )
   	{
		case DML_INSERT:
		  dmlProcPtr = new InsertPackageProcessor();
		break;
		
		case DML_DELETE:
		  dmlProcPtr = new DeletePackageProcessor();
		break;

		case DML_UPDATE:
		  dmlProcPtr = new UpdatePackageProcessor();
		break;
		
		case DML_COMMAND:
		  dmlProcPtr = new CommandPackageProcessor();
		break;
	
	}	

	return dmlProcPtr;	  
}

} // namespace dmlpackageprocessor

