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
    static bool prepareRowsInsertedInfo(std::string Entry, int64_t& TotRows, 
											int64_t& InsRows);
    static bool prepareColumnOutOfRangeInfo(std::string Entry, int& ColNum, 
                                            CalpontSystemCatalog::ColDataType& ColType,
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
