/*

Copyright (C) 2009-2013 Calpont Corporation.

Use of and access to the Calpont InfiniDB Community software is subject to the
terms and conditions of the Calpont Open Source License Agreement. Use of and
access to the Calpont InfiniDB Enterprise software is subject to the terms and
conditions of the Calpont End User License Agreement.

This program is distributed in the hope that it will be useful, and unless
otherwise noted on your license agreement, WITHOUT ANY WARRANTY; without even
the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
Please refer to the Calpont Open Source License Agreement and the Calpont End
User License Agreement for more details.

You should have received a copy of either the Calpont Open Source License
Agreement or the Calpont End User License Agreement along with this program; if
not, it is your responsibility to review the terms and conditions of the proper
Calpont license agreement by visiting http://www.calpont.com for the Calpont
InfiniDB Enterprise End User License Agreement or http://www.infinidb.org for
the Calpont InfiniDB Community Calpont Open Source License Agreement.

Calpont may make changes to these license agreements from time to time. When
these changes are made, Calpont will make a new copy of the Calpont End User
License Agreement available at http://www.calpont.com and a new copy of the
Calpont Open Source License Agreement available at http:///www.infinidb.org.
You understand and agree that if you use the Program after the date on which
the license agreement authorizing your use has changed, Calpont will treat your
use as acceptance of the updated License.

*/

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
