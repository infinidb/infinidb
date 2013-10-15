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

// $Id: tdriver.c 414 2011-02-10 14:23:02Z rdempsey $

#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>


#include "dm.h"

int main(int argc, char** argv)
{
	char aBuff[10];
	char bBuff[10];
	char cBuff[10];
	char fnBuff[16];

	int rc;

	dmFilePathArgs_t args;

	args.pDirA = aBuff;
	args.pDirB = bBuff;
	args.pDirC = cBuff;
	args.pFName = fnBuff;


	//--------------------------------------------------------------------------
	// test 1 - test for correctness
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0xd5aa9687, &args);
	assert(strcmp(args.pDirA, "213.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "170.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "150.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE135.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == 0);


	//--------------------------------------------------------------------------
	// test 2 - test for short numbers -- zero-fill
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0x00010203, &args);
	assert(strcmp(args.pDirA, "000.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "001.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "002.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE003.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == 0);


	//--------------------------------------------------------------------------
	// test 3 - test for ZERO
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0x00000000, &args);
	assert(strcmp(args.pDirA, "000.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "000.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "000.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE000.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == 0);


	//--------------------------------------------------------------------------
	// test 4 - test for MAXINT
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0xffffffff, &args);
	assert(strcmp(args.pDirA, "255.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "255.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "255.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE255.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == 0);


	//--------------------------------------------------------------------------
	// test 5 - test for <DirA> buffer too small
	//--------------------------------------------------------------------------
	args.ALen = 6;
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0xffffffff, &args);
	assert(strcmp(args.pDirA, "255.d") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "255.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "255.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE255.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == -1);



	//--------------------------------------------------------------------------
	// test 6 - test for <DirB> buffer too small
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = 6;
	args.CLen = sizeof(cBuff);
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0xffffffff, &args);
	assert(strcmp(args.pDirA, "255.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "255.d") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "255.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE255.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == -1);



	//--------------------------------------------------------------------------
	// test 7 - test for <DirC> buffer too small
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = 6;
	args.FNLen = sizeof(fnBuff);

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0xffffffff, &args);
	assert(strcmp(args.pDirA, "255.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "255.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "255.d") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE255.cdf") == 0);
	assert(args.FNrc == 11);
	assert(rc == -1);



	//--------------------------------------------------------------------------
	// test 8 - test for <FName> buffer too small
	//--------------------------------------------------------------------------
	args.ALen = sizeof(aBuff);
	args.BLen = sizeof(bBuff);
	args.CLen = sizeof(bBuff);
	args.FNLen = 10;

	args.Arc = 0;
	args.Brc = 0;
	args.Crc = 0;
	args.FNrc = 0;

	rc = dmOid2FPath((UINT32)0xffffffff, &args);
	assert(strcmp(args.pDirA, "255.dir") == 0);
	assert(args.Arc == 7);
	assert(strcmp(args.pDirB, "255.dir") == 0);
	assert(args.Brc == 7);
	assert(strcmp(args.pDirC, "255.dir") == 0);
	assert(args.Crc == 7);
	assert(strcmp(args.pFName, "FILE255.c") == 0);
	assert(args.FNrc == 11);
	assert(rc == -1);

	UINT32 OID = 0;
	strcpy(args.pDirA, "000.dir");
	strcpy(args.pDirB, "001.dir");
	strcpy(args.pDirC, "002.dir");
	args.Arc = args.Brc = args.Crc = 7;
	strcpy(args.pFName, "FILE003.cdf");
	args.FNrc = 11;
	rc = dmFPath2Oid(&args, &OID);
	assert(rc == 0);
	assert(OID == (UINT32)0x00010203);

	strcpy(args.pDirA, "255.dir");
	strcpy(args.pDirB, "255.dir");
	strcpy(args.pDirC, "255.dir");
	strcpy(args.pFName, "FILE255.cdf");
	rc = dmFPath2Oid(&args, &OID);
	assert(rc == 0);
	assert(OID == (UINT32)0xffffffff);

	strcpy(args.pDirA, "213.dir");
	strcpy(args.pDirB, "170.dir");
	strcpy(args.pDirC, "150.dir");
	strcpy(args.pFName, "FILE135.cdf");
	rc = dmFPath2Oid(&args, &OID);
	assert(rc == 0);
	assert(OID == (UINT32)0xd5aa9687);

	strcpy(args.pFName, "FILE999.cdf");
	rc = dmFPath2Oid(&args, &OID);
	assert(rc != 0);

exit(0);
}

