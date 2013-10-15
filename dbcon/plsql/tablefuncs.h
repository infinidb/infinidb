/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*
*  $Id: tablefuncs.h 9210 2013-01-21 14:10:42Z rdempsey $
*/
// The following ifdef block is the standard way of creating macros which make exporting 
// from a DLL simpler. All files within this DLL are compiled with the EXECPROC_EXPORTS
// symbol defined on the command line. this symbol should not be defined on any project
// that uses this DLL. This way any other project whose source files include this file see 
// EXECPROC_API functions as being imported from a DLL, wheras this DLL sees symbols
// defined with this macro as being exported.
#ifndef linux
#ifdef EXECPROC_EXPORTS
#define EXECPROC_API __declspec(dllexport)
#else
#define EXECPROC_API __declspec(dllimport)
#endif
#else
#define EXECPROC_API
#endif

#ifndef OCI_ORACLE
# include <oci.h>
#endif
#include <odci.h>
#include <vector>

/********************** SQL Types C representation **********************/

/* Table function's implementation type */

struct CalpontImpl
{
  OCIRaw* key;
};
typedef struct CalpontImpl CalpontImpl;

struct CalpontImpl_ind
{
  short _atomic;
  short key;
};
typedef struct CalpontImpl_ind CalpontImpl_ind;

struct BindValue
{
  OCIString* sqltext;
  OCIString* bindVar;
  OCIString* bindValue;
};
typedef struct BindValue BindValue;

struct BindValue_ind
{
  short _atomic;
  short sqltext;
  short bindVar;
  short bindValue;
};
typedef struct BindValue_ind BindValue_ind;
  
/* Table function's output collection type */

typedef OCITable BindValueSet;



#ifdef __cplusplus
extern "C" {
#endif

EXECPROC_API int ODCITableStart(OCIExtProcContext* extProcCtx,
	CalpontImpl* self, CalpontImpl_ind* self_ind,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* schemaName, short schemaName_ind,
	char* tableName, short tableName_ind,
	char* ownerName, short ownerName_ind,
	char* tblTypeName, short tblTypeName_ind,
	char* rowTypeName, short rowTypeName_ind,
	char* curSchemaName, short curSchemaName_ind,
	BindValueSet* bindValList, short bindValList_ind);

/** @brief table handle preparation at query compilation time
 *  The purpose of this function is to change Oracle's call
 *  routine. With the implementation of this interface function,
 *  Oracle calls TableClose once for each table at the end of
 *  the query, where is a good place to clear the result set
 *  and reset the query state.
 */
EXECPROC_API int ODCITablePrePare(OCIExtProcContext* extProcCtx,
	CalpontImpl* self, CalpontImpl_ind* self_ind,
	ODCITabFuncInfo* info, short info_ind,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* schemaName, short schemaName_ind,
	char* tableName, short tableName_ind,
	char* ownerName, short ownerName_ind,
	char* tblTypeName, short tblTypeName_ind,
	char* rowTypeName, short rowTypeName_ind,
	char* curSchemaName, short curSchemaName_ind,
	BindValueSet* bindValList, short bindValList_ind);

EXECPROC_API int ODCITableDescribe(OCIExtProcContext* extProcCtx,
	OCIType** rtype, short* rtype_ind,
	OCINumber* sessionIDn, short sessionIDn_ind,
	char* schemaName, short schemaName_ind,
	char* tableName, short tableName_ind,
	char* ownerName, short ownerName_ind,
	char* tblTypeName, short tblTypeName_ind,
	char* rowTypeName, short rowTypeName_ind,
	char* curSchemaName, short curSchemaName_ind,
	BindValueSet* bindValList, short bindValList_ind);

EXECPROC_API int ODCITableFetch(OCIExtProcContext* extProcCtx,
	CalpontImpl* self, CalpontImpl_ind* self_ind,
	OCINumber* nrows,
	OCIAnyDataSet** outSet, short* outSet_ind);

EXECPROC_API int ODCITableClose(OCIExtProcContext* extProcCtx,
	CalpontImpl* self, CalpontImpl_ind* self_ind);

EXECPROC_API void CalLogoff(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind);
	
EXECPROC_API void CalLogon(OCIExtProcContext* extProcCtx,
	OCINumber* sessionIDn, short sessionIDn_ind, char* schemaName, short schemaName_ind);	

EXECPROC_API int Get_rowcount();

#ifdef __cplusplus
}
#endif

