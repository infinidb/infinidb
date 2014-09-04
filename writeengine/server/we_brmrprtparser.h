
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
  #CP:   startLBID max min seqnum isChar newExtent
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
