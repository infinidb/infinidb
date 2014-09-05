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

/**
*	$Id: dm.h 591 2013-01-21 14:11:20Z rdempsey $
*
*	@file dm.h
*	@author Robert King <rking@calpont.com>
*
*	@brief Interface file for the Disk Manager (dm) Library.
*
*	This file contains the interface specification for the Disk Manager
*	Library.  Additionally, any structures used by this interface are
*	defined here as well.
*
*
*/
#ifndef DM_H
#define DM_H

#ifndef _MSC_VER
#include <stdint.h>
#ifndef _BASETSD_H_
typedef uint32_t UINT32;
#endif
#else
typedef unsigned __int32 UINT32;
#endif

#ifdef __cplusplus
extern "C" {
#endif

/**
*	@brief Contains directory and filename buffers.
*
*	dmFilePathArgs_t contains buffers for reporting back the directories
*	and filename that correspond to an Object ID (OID)
*
*	Directories are organized in a 3-layer tree (used for version buffer files):
*
*		<DirA>/<DirB>/<DirC>/<FName>
*
*	OR in a 5-layer tree (used for all other db files besides version buffer):
*
*		<DirA>/<DirB>/<DirC>/<DirD>/<DirE>/<FName>
*
*	@note All buffers must be pre-allocated by the calling routine.
*	@note 5-layer directory structure includes partition and segment number.
*/
typedef struct
{
	char* pDirA;	/**< OUT -- DirA's buffer*/
	char* pDirB;	/**< OUT -- DirB's buffer */
	char* pDirC;	/**< OUT -- DirC's buffer */
	char* pDirD;	/**< OUT -- DirD's buffer */                    //dmc-Branch
	char* pDirE;	/**< OUT -- DirE's buffer */                    //dmc-Branch
	char* pFName;	/**< OUT -- Filename buffer */
	int	ALen;		/**< IN -- Size in bytes of DirA's Buffer. */
	int	BLen;		/**< IN -- Size in bytes of DirB's Buffer. */
	int	CLen;		/**< IN -- Size in bytes of DirC's Buffer. */
	int	DLen;		/**< IN -- Size in bytes of DirD's Buffer. */   //dmc-Branch
	int	ELen;		/**< IN -- Size in bytes of DirE's Buffer. */   //dmc-Branch
	int	FNLen;		/**< IN -- Size in bytes of Filename's Buffer. */
	int	Arc;		/**< OUT -- result code for formatting DirA. */
	int	Brc;		/**< OUT -- result code for formatting DirB. */
	int	Crc;		/**< OUT -- result code for formatting DirC. */
	int	Drc;		/**< OUT -- result code for formatting DirD. */ //dmc-Branch
	int	Erc;		/**< OUT -- result code for formatting DirE. */ //dmc-Branch
	int	FNrc;		/**< OUT -- result code for formatting Filename. */
} dmFilePathArgs_t;

/**
* @brief fill in dmFilePathArgs_t struct for OID.
*/
int	dmOid2FPath(UINT32 oid, UINT32 partition, UINT32 segment, dmFilePathArgs_t* pArgs); //dmc-Branch

/**
* @brief calculate OID given dmFilePathArgs_t struct.
*/
int	dmFPath2Oid(dmFilePathArgs_t* pArgs, UINT32* oid);

#ifdef __cplusplus
}
#endif

#endif
