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
 * we_brmupdater.h
 *
 *  Created on: Dec 13, 2011
 *      Author: bpaul
 */

#ifndef WE_BRMUPDATER_H_
#define WE_BRMUPDATER_H_


namespace WriteEngine
{

class WESDHandler;					// forward deceleration


class WEBrmUpdater
{
public:
	WEBrmUpdater(WESDHandler& Ref):	fRef(Ref), fpBrm(0) {}
	virtual ~WEBrmUpdater() {}

public:
    bool updateCasualPartitionAndHighWaterMarkInBRM();
    int updateCPAndHWMInBRM();
    int updateCasualPartitionInBRM();
    int updateHighWaterMarkInBRM();
    bool prepareCasualPartitionInfo();
    bool prepareHighWaterMarkInfo();

    bool createBrmConnection()
    {
        fpBrm = new BRM::DBRM();
        return (fpBrm)?true:false;
    }
    void releaseBrmConnection()
    {
    	delete fpBrm;
    	fpBrm = 0;
    }



public:
    static bool prepareRowsInsertedInfo(std::string Entry,	int& TotRows, int& InsRows);
    static bool prepareColumnOutOfRangeInfo(std::string Entry, int& ColNum, ColDataType& ColType,
                                            std::string& ColName, int& OorValues);
    static bool prepareErrorFileInfo(std::string Entry, std::string& ErrFileName);
    static bool prepareBadDataFileInfo(std::string Entry, std::string& BadFileName);

private:
    WESDHandler& fRef;
    BRM::DBRM* fpBrm;


	//BRM::CPInfoMergeList_t fCPInfo;
    std::vector<BRM::CPInfoMerge> fCPInfo;
	std::vector<BRM::BulkSetHWMArg> fHWMInfo;
	std::vector<BRM::CPInfo> fCPInfoData;


};

} /* namespace WriteEngine */
#endif /* WE_BRMUPDATER_H_ */
