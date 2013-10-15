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
*  $Id: execproc.h 9210 2013-01-21 14:10:42Z rdempsey $
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

extern "C" EXECPROC_API int Execute_procedure(OCIExtProcContext*, char*, short, char*, short, char*, short,
											  char*, short, char*, short, char*, short, char*, short, char*, short,
											  char*, short, char*, short);

