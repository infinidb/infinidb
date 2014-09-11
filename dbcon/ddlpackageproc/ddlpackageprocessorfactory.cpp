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
*   $Id: ddlpackageprocessorfactory.cpp 7409 2011-02-08 14:38:50Z rdempsey $
*
*
***********************************************************************/
#include "ddlpackageprocessorfactory.h"
#include "ddlpackage.h"
#include "alterpackageprocessor.h"
#include "createpackageprocessor.h"
#include "droppackageprocessor.h"

using namespace ddlpackage;

namespace ddlpackageprocessor {

DDLPackageProcessor*  DDLPackageProcessorFactory::
makePackageProcessor(int packageType, ddlpackage::CalpontDDLPackage& cpackage)
{
	DDLPackageProcessor* ddlProcPtr = 0;

	switch( packageType )
   	{
		case DDL_CREATE:
		  ddlProcPtr = new CreatePackageProcessor();
		break;
		
		case DDL_ALTER:
		  ddlProcPtr = new AlterPackageProcessor();
		break;

		case DDL_DROP:
		  ddlProcPtr = new DropPackageProcessor();
		break;

	}	

	return ddlProcPtr;	  
}

} // namespace ddlpackageprocessor
