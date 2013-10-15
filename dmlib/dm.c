/*

Copyright (C) 2009-2011 Calpont Corporation.

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
 *	$Id: dm.c 414 2011-02-10 14:23:02Z rdempsey $
 *
 *	@file dm.c
 *	@author Robert King <rking@calpont.com>
 *
 *	@brief Implementation file for the Disk Manager (dm) Library.
 *
 *	This file contains the implementation for the following Disk Manager
 *	Library functions:
 *
 *		- dmOid2FPath()
 *		- _doDir()
 *		- _doFile()
 */

#include <stdlib.h>
#include <stdio.h>

#include "dm.h"

#ifdef _MSC_VER
#define snprintf _snprintf
#endif

#define STATIC static

STATIC int _doDir(char* pBuffer, int blen, unsigned int val);       //dmc-Branch
STATIC int _doFile(char* pBuffer, int blen, unsigned char val);

/** @brief Converts an OID into a group if directories and a filename.
*
*/
int dmFPath2Oid(dmFilePathArgs_t* pArgs, UINT32* OID)
{
	unsigned part;

	*OID = 0;
	part = atoi(pArgs->pDirA);
	if (part > 255) return -1;
	*OID |= (part << 24);
	part = atoi(pArgs->pDirB);
	if (part > 255) return -1;
	*OID |= (part << 16);
	part = atoi(pArgs->pDirC);
	if (part > 255) return -1;
	*OID |= (part << 8);
	part = atoi(&pArgs->pFName[4]);
	if (part > 255) return -1;
	*OID |= (part << 0);
	return 0;
}

/** @brief Converts an OID into a group if directories and a filename.
*
*	This function takes a 32-bit Object ID (OID).  If DLen is 0, then the
*	OID is converted into 3 hierarchical directory names and a filename.
*	If DLen is >0 then the OID is converted into 5 hierarchical directory
*	names and a filename (using the partition and segment as additional
*	input into the filepath.  The actual location
*	of the file is <DBRoot>/<DirA>/<DirB>/<DirC>/<FName>, or
*	<DBRoot>/<DirA>/<DirB>/<DirC>/<DirD>/<part#>/<segFName>.  The <DBRoot>
*	entry must be pre-pended by the calling application after calling
*	this function.  The value for <DBRoot> is stored in the Calpont.xml
*	configuration file.
*
*	@param	oid				INPUT -- The Object Id.
*	@param	partition		INPUT -- partition to be included in filepath.
*	@param	segment			INPUT -- segment to be included in filepath.
*	@param	dmFilePathArgs*	I/O -- Points to a buffer structure
*
*	@return	0 if everything went OK.  -1 if an error occured.  Two
*			kinds of errors are possible:
*
*				- a null pointer was passed in
*				- truncation occured.
*
*			If a null buffer pointer is passed in, a return code
*			of -1 will be returned FOR THAT BUFFER.
*
*			Truncation can occur if the buffer length specified in
*			dmFilePathArgs is too small.
*
*			If a buffer's return code is not zero, the appropriate
*			return code in dmfilePathArgs can be examined.  If a
*			buffer's return code is be less than zero, the
*			corresponding buffer pointer was NULL.  If it is greater
*			or equal to the buffer's length argument, length is too small
*.
*/
int	dmOid2FPath(UINT32 oid, UINT32 partition, UINT32 segment,
   dmFilePathArgs_t* pArgs)                                         //dmc-Branch
{

	pArgs->Arc = _doDir(
		pArgs->pDirA,
		pArgs->ALen,
		(unsigned int)oid>>24);

	pArgs->Brc = _doDir(
		pArgs->pDirB,
		pArgs->BLen,
		(unsigned int)(oid&0x00ff0000)>>16);

	pArgs->Crc = _doDir(
		pArgs->pDirC,
		pArgs->CLen,
		(unsigned int)(oid&0x0000ff00)>>8);

//dmc-Branch-Begin
	// include partition and segment number in the file path if they are present
	if (pArgs->DLen > 0)
	{
		pArgs->Drc = _doDir(
			pArgs->pDirD,
			pArgs->DLen,
			(unsigned int)(oid&0x000000ff));

		pArgs->Erc = _doDir(
			pArgs->pDirE,
			pArgs->ELen,
			partition);

		pArgs->FNrc = _doFile(
			pArgs->pFName,
			pArgs->FNLen,
			segment);

		if (
			(pArgs->Drc < 0) ||
			(pArgs->Erc < 0)
		)
			return -1;

		if (
			(pArgs->Drc >= pArgs->ALen) ||
			(pArgs->Erc >= pArgs->ALen)
		)
			return -1;
	}
	else
	{
		pArgs->FNrc = _doFile(
			pArgs->pFName,
			pArgs->FNLen,
			(unsigned int)(oid&0x000000ff));
	}
//dmc-Branch-End

	if (
		(pArgs->Arc < 0) ||
		(pArgs->Brc < 0) ||
		(pArgs->Crc < 0) ||
		(pArgs->FNrc < 0)
	)
		return -1;

	if (
		(pArgs->Arc >= pArgs->ALen) ||
		(pArgs->Brc >= pArgs->BLen) ||
		(pArgs->Crc >= pArgs->CLen) ||
		(pArgs->FNrc >= pArgs->FNLen)
	)
		return -1;
	else
		return 0;
}




/** @brief Formats a directory name.
 *
 *	This function takes a 8-bit value and converts it into a directory
 *	name.
 *
 *	@param	pBuffer		OUPUT - A pointer to the output buffer.
 *	@param	blen		INPUT - The length of the output buffer, in bytes.
 *	@param	val			INPUT - value to be used in the formatted name.
 *
 *	@return	The number of characters in the output buffer is returned
 *			(not including the NULL terminator). -1 of the input buffer
 *			pointer is NULL.
 *	@note	If the input pointer is invalid (points to unmapped memoty,
 *			for example,) this function can raise a segmentation violation
 *			or a bus error.
 *
 */
STATIC int _doDir(char* pBuffer, int blen, unsigned int val)        //dmc-Branch
{
	int rc;

	if (!pBuffer)
	{
		rc = -1;

	} else {
		rc = snprintf(pBuffer, blen, "%03u.dir", val);              //dmc-Branch
		pBuffer[blen-1] = (char)0;
	}

	return rc;
}




/** @brief Formats a file name.
 *
 *	This function takes a 8-bit value and converts it into a file name.
 *
 *	@param	pBuffer		OUPUT - A pointer to the output buffer.
 *	@param	blen		INPUT - The length of the output buffer, in bytes.
 *	@param	val			INPUT - value to be used in the formatted name.
 *
 *	@return	The number of characters in the output buffer is returned
 *			(not including the NULL terminator). -1 of the input buffer
 *			pointer is NULL.
 *	@note	If the input pointer is invalid (points to unmapped memoty,
 *			for example,) this function can raise a segmentation violation
 *			or a bus error.
 *
 */
STATIC int _doFile(char* pBuffer, int blen, unsigned char val)
{
	int rc;

	if (!pBuffer)
	{
		rc = -1;

	} else {
		rc = snprintf(pBuffer, blen, "FILE%03d.cdf", val);
		pBuffer[blen-1] = (char)0;
	}

	return rc;
}

