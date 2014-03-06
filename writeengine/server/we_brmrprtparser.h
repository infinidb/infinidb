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
 * we_brmrprtparser.h
 *
 *  Created on: Dec 13, 2011
 *      Author: bpaul
 */

#ifndef WE_BRMRPRTPARSER_H_
#define WE_BRMRPRTPARSER_H_

#include <fstream>
#include <iostream>
using namespace std;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

/*
 *
  #CP:   startLBID max min seqnum type newExtent
  #HWM:  oid partition segment hwm
  #ROWS: numRowsRead numRowsInserted
  #DATA: columnNumber numOutOfRangeValues
  #ERR:  error message file
  #BAD:  bad data file, with rejected rows
  CP: 234496 6 6 -1 0 0
  HWM: 3091 0 0 1
  ERR: /home/dcathey/t1.tbl.Job_3090_27983.err
  BAD: /home/dcathey/t1.tbl.Job_3090_27983.bad
  ROWS: 3 1
 *
 */

namespace WriteEngine
{

class BrmReportParser
{
public:
	BrmReportParser();
	virtual ~BrmReportParser();

public:
	bool serialize(std::string RptFileName, messageqcpp::ByteStream& Bs);
	bool serializeBlocks(std::string RptFileName, messageqcpp::ByteStream& Bs);
	void unserialize(messageqcpp::ByteStream& Bs);

private:
	ifstream fRptFile;

};

} /* namespace WriteEngine */
#endif /* WE_BRMRPRTPARSER_H_ */
