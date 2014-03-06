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

//------------------------------------------------------------------------------
// Serialize specified RptFileName into the ByteStream Bs.
// Serialization is done one record at a time.  File should be an ASCII file
// with newlines markers, because getline() is used.
// Function is limited to records that are no longer than 255 bytes.
// If a record is longer than 255 bytes, the function misbehaves.
//------------------------------------------------------------------------------
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
	b >> (uint32_t&) fOid;
	b >> fData;
	b >> reinterpret_cast<ByteStream::doublebyte&>(fReturnAll);
	b >> (uint32_t&) fSequence;
	*/

	std::string aStrLine;
	while(Bs.length()>0)
	{
		Bs >> aStrLine;
		cout << aStrLine;
	}

}





} /* namespace WriteEngine */
