/*

   Copyright (C) 2009-2012 Calpont Corporation.

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

/*******************************************************************************
* $Id$
*
*******************************************************************************/

/*
 * we_brmrprtparser.cpp
 *
 *  Created on: Dec 13, 2011
 *      Author: bpaul
 */

#include <fstream>
#include <iostream>
using namespace std;

#include "we_brmrprtparser.h"

namespace WriteEngine
{

BrmReportParser::BrmReportParser():fRptFile()
{

}

BrmReportParser::~BrmReportParser()
{

}


bool BrmReportParser::serialize(std::string RptFileName,
									messageqcpp::ByteStream& Bs)
{
	try
	{
	fRptFile.open(RptFileName.c_str(), ifstream::in);
	}
	catch(std::exception& ex)
	{
		cout <<"Failed to open BRMRptFile "<< RptFileName <<endl;
		cout << ex.what() << endl;
		throw runtime_error(ex.what());
	}
	if(fRptFile.good())
	{
		char aBuff[1024];
		unsigned int aLen=0;
		while(fRptFile.good() && !fRptFile.eof())
		{
			fRptFile.getline(aBuff, sizeof(aBuff)-1);
			aLen=fRptFile.gcount();
			if((aLen != (sizeof(aBuff)-1)) && (aLen>0))
			{
				//aBuff[aLen-1] = '\n';
				//aBuff[aLen]=0;
				//cout << "Data Read " << aBuff <<endl;
				if(aBuff[0] != '#')	// do not serialize comments
				{
					std::string strData = aBuff;
					Bs << strData;
				}
			}
		}// while
		fRptFile.close();
		cout << "Closed File : "<< RptFileName <<" "<< errno << endl;
	}
	else
	{
		cout << "Failed to open : "<< RptFileName <<" "<< errno << endl;
		return false;
	}
	return true;
}

//------------------------------------------------------------------------------
// Serialize specified RptFileName into the ByteStream Bs.
// Serialization is done in 8192 byte blocks instead of by line or by record,
// so that this function will work for both character and binary files.
//------------------------------------------------------------------------------
bool BrmReportParser::serializeBlocks(std::string RptFileName,
		    messageqcpp::ByteStream& Bs)
{
	try
	{
		fRptFile.open(RptFileName.c_str(), ifstream::in);
	}
	catch (std::exception& ex)
	{
		cout <<"Failed to open Report File "<< RptFileName << endl;
		cout << ex.what() << endl;
		return false;
	}

	if (fRptFile.good())
	{
		char aBuff[8192];
		unsigned int aLen=0;
		std::string strBuff;

		while ( fRptFile.good() )
		{
			fRptFile.read(aBuff, sizeof(aBuff));
			aLen = fRptFile.gcount();

			if (aLen > 0)
			{
				strBuff.assign( aBuff, aLen );
				Bs << strBuff;
			}
		}

		fRptFile.close();
		cout << "Closed Report File : "<< RptFileName << endl;
	}
	else
	{
		std::ostringstream oss;
		oss << "Failed to open Report File " << RptFileName << endl;
		cout << oss.str() << endl;
		return false;
	}

	return true;
}


void BrmReportParser::unserialize(messageqcpp::ByteStream& Bs)
{
	//TODO to be changed. left it here to understand how to implement
	/*
	ObjectReader::checkType(b, ObjectReader::SIMPLECOLUMN);
	ReturnedColumn::unserialize(b); // parent class unserialize
	b >> (u_int32_t&) fOid;
	b >> fData;
	b >> reinterpret_cast<ByteStream::doublebyte&>(fReturnAll);
	b >> (u_int32_t&) fSequence;
	*/

	std::string aStrLine;
	while(Bs.length()>0)
	{
		Bs >> aStrLine;
		cout << aStrLine;
	}

}





} /* namespace WriteEngine */
