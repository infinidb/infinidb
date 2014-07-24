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
 * we_brmupdater.cpp
 *
 *  Created on: Dec 13, 2011
 *      Author: bpaul
 */




#include <iostream>
#include <sstream>
using namespace std;

#include "we_define.h"

#include <boost/lexical_cast.hpp>
using namespace boost;

//#include "we_simplesyslog.h"
#include "we_sdhandler.h"

#include "brm.h"
#include "brmtypes.h"
using namespace BRM;

#include "we_brmupdater.h"


namespace WriteEngine
{


//-----------------------------------------------------------------------------

bool WEBrmUpdater::updateCasualPartitionAndHighWaterMarkInBRM()
{

    try
    {
    	bool aGood = prepareCasualPartitionInfo();
    	if(!aGood)
    		cout << "prepareCasualPartitionInfo Failed" << endl;
    }
    catch(std::exception& ex)
    {
    	std::string aStr = "Exception in prepareCasualPartitionInfo(); Error Ignored";
        logging::Message::Args errMsgArgs;
        errMsgArgs.add(aStr);
        errMsgArgs.add(ex.what());
    	fRef.sysLog(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
    	cout << aStr << endl;
    }


    try
    {
    	bool aSuccess = prepareHighWaterMarkInfo();
    	if(!aSuccess)
    		throw(std::runtime_error("prepareHWMInfo Failed"));
    }
    catch(std::exception& ex)
    {
    	std::string aStr = "prepareHWMInfo() failed... Bailing out!!";
        logging::Message::Args errMsgArgs;
        errMsgArgs.add(aStr);
        aStr = "Need to rollback bulk upload";
        errMsgArgs.add(aStr);
        errMsgArgs.add(ex.what());
    	fRef.sysLog(errMsgArgs, logging::LOG_TYPE_ERROR, logging::M0000);
    	cout << aStr << endl;

    	return false;
    }

    // If we are here, we packaged informations properly
    // Lets connect to BRM


    if(!createBrmConnection())
    {
    	cout << "Brm Connection FAILED" << endl;
    	return false;
    }


    int aRc = updateCPAndHWMInBRM();
    if(aRc != 0)
    {
    	cout << "Updating High Water Mark Failed" << endl;
    }
    else
    {
		if(fRef.getDebugLvl())
			cout << "Updating High Water Mark Successful!!" << endl << endl;
    }


    /*
	int cpRc = updateCasualPartitionInBRM();
	if(cpRc != 0)
		cout << "Updating Casual Partition Failed" << endl;
	else
	{
		if(fRef.getDebugLvl())
			cout << "Updating Casual Partition Successful" << endl;
	}

	int hwmRc = updateHighWaterMarkInBRM();
	if(hwmRc != 0)
		cout << "Updating High Water Mark Failed" << endl;
	else
	{
		if(fRef.getDebugLvl())
			cout << "Updating High Water Mark Successful!!" << endl << endl;
	}
	*/


	releaseBrmConnection();


	//return(!hwmRc)? true: false;
	return(!aRc)? true: false;
}


//------------------------------------------------------------------------------
// Update Casual Partition information in BRM
//------------------------------------------------------------------------------
int WEBrmUpdater::updateCasualPartitionInBRM()
{
    int rc = 0;

    if (fCPInfo.size() > 0)
    {
        //std::ostringstream oss;
        //oss << "Committing " << fCPInfo.size() << " CP updates for table " <<
        //		fRef.getTableName() << " to BRM";
        //cout << endl << oss.str() << endl;


        //TODO - NOTE. later make this Objection creation once for both CP & HWM
        if(fpBrm)
        {
        rc = fpBrm->mergeExtentsMaxMin(fCPInfo);
        if(rc != BRM::ERR_OK)
        {
        	std::string errStr;
        	BRM::errString(rc, errStr);
        	cout << "BRM ERROR is ***** " << errStr << endl;

            std::ostringstream oss;
            oss << "Error updating BRM with CP data for table " <<
            		fRef.getTableName() <<" Error: "<< 	errStr << endl;

            cout << endl << oss.str() << endl;
        }
        }
    }

    return rc;
}



//------------------------------------------------------------------------------
// Send HWM update information to BRM
//------------------------------------------------------------------------------
int WEBrmUpdater::updateHighWaterMarkInBRM()
{
    int rc = 0;

    if (fHWMInfo.size() > 0)
    {
        //std::ostringstream oss;
        //oss << "Committing " << fHWMInfo.size() << " HWM update(s) for table "<<
        //    fRef.getTableName() << " to BRM";
        //cout << endl << oss.str() << endl;

        if(fpBrm)
        {
        rc = fpBrm->bulkSetHWM(fHWMInfo, 0);
        if(rc != BRM::ERR_OK)
        {
        	std::string errStr;
        	BRM::errString(rc, errStr);
        	cout << "BRM ERROR is ***** " << errStr << endl;

            std::ostringstream oss;
            oss << "Error updating BRM with HWM data for table "<<
            		fRef.getTableName() << "error: " << errStr << endl;
            cout << endl <<  oss.str() << endl;
            return rc;
        }
        }
    }

    return rc;
}


//-----------------------------------------------------------------------------

int WEBrmUpdater::updateCPAndHWMInBRM()
{
	int rc = 0;
	//BUG 4232. some imports may not contain CP but HWM
    if ((fCPInfo.size() > 0) || (fHWMInfo.size() > 0))
    {
        //TODO - NOTE. later make this Objection creation once for both CP & HWM
        if(fpBrm)
        {
            /*
            rc = bulkSetHWMAndCP(const std::vector<BulkSetHWMArg> &,
            					const std::vector<CPInfo> & setCPDataArgs,
            					const std::vector<CPInfoMerge> & mergeCPDataArgs,
            					VER_t transID = 0) DBRM_THROW;
            */
        	rc = fpBrm->bulkSetHWMAndCP(fHWMInfo, fCPInfoData, fCPInfo, 0);
        	//rc = fpBrm->mergeExtentsMaxMin(fCPInfo);
            //rc = fpBrm->bulkSetHWM(fHWMInfo, 0);
            if(rc != BRM::ERR_OK)
            {
            	std::string errStr;
            	BRM::errString(rc, errStr);
            	cout << "BRM ERROR is ***** " << errStr << endl;

                std::ostringstream oss;
                oss << "Error updating BRM with HWM data for table "<<
                		fRef.getTableName() << "error: " << errStr << endl;
                cout << endl <<  oss.str() << endl;
            	cout << "ERROR: HWM and CP set failed!!" << endl;
            	//throw runtime_error("ERROR: bulSetHWMAndCp Failed!!");
            	return rc;
            }
        }
        else
        	return rc;
    }
    return rc;
}

//------------------------------------------------------------------------------


bool WEBrmUpdater::prepareCasualPartitionInfo()
{
	//cout << "Started prepareCasualPartitionInfo()!!" << endl;
	//CP: 275456 6000000 4776193 -1 0 1
    WESDHandler::StrVec::iterator aIt = fRef.fBrmRptVec.begin();
	while (aIt != fRef.fBrmRptVec.end())
	{
		std::string aEntry = *aIt;
		if ((!aEntry.empty()) && (aEntry.at(0) == 'C'))
		{
			BRM::CPInfoMerge cpInfoMerge;
			const int BUFLEN=128;
			char aBuff[BUFLEN];
			strncpy(aBuff, aEntry.c_str(),BUFLEN);
			aBuff[BUFLEN-1]=0;

			char*pTok = strtok(aBuff, " ");
			if (!pTok) // ignore the Msg Body
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad Body in CP entry string"));
			}

			pTok = strtok(NULL, " ");
			if (pTok)
				cpInfoMerge.startLbid = boost::lexical_cast<uint64_t>(pTok);
			else
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad startLbid in CP entry string"));
			}

			pTok = strtok(NULL, " ");
			if (pTok)
				cpInfoMerge.max = boost::lexical_cast<int64_t>(pTok);
			else
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad MAX in CP entry string"));
			}

			pTok = strtok(NULL, " ");
			if (pTok)
				cpInfoMerge.min = boost::lexical_cast<int64_t>(pTok);
			else
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad MIN in CP entry string"));
			}

			pTok = strtok(NULL, " ");
			if (pTok)
				cpInfoMerge.seqNum = atoi(pTok);
			else
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad seqNUM in CP entry string"));
			}

			pTok = strtok(NULL, " ");
			if (pTok)
				cpInfoMerge.type = (execplan::CalpontSystemCatalog::ColDataType)atoi(pTok);
			else
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad type in CP entry string"));
			}

			pTok = strtok(NULL, " ");
			if (pTok)
				cpInfoMerge.newExtent = (atoi(pTok) != 0);
			else
			{
				//cout << "CP Entry : " << aEntry << endl;
				throw(runtime_error("Bad newExtent in CP entry string"));
			}

			fCPInfo.push_back(cpInfoMerge);
		}
		++aIt;
	}

	if(fRef.getDebugLvl())
		cout << "Finished prepareCasualPartitionInfo()!!" << endl;
	return true;
}


//-----------------------------------------------------------------------------

bool WEBrmUpdater::prepareHighWaterMarkInfo()
{
	//HWM: 3056 0 0 8191
	WESDHandler::StrVec::iterator aIt = fRef.fBrmRptVec.begin();
	while(aIt != fRef.fBrmRptVec.end())
	{
		std::string aEntry = *aIt;
		if ((!aEntry.empty()) && (aEntry.at(0) == 'H'))
		{
			BRM::BulkSetHWMArg hwmArg;
			const int BUFLEN=128;
			char aBuff[BUFLEN];
			strncpy(aBuff, aEntry.c_str(),BUFLEN);
			aBuff[BUFLEN-1]=0;

			char*pTok = strtok(aBuff, " ");
			if(!pTok) // ignore the Msg Body
			{
				//cout << "HWM Entry : " << aEntry << endl;
				throw (runtime_error("Bad Body in HWM entry string"));
			}

			pTok = strtok(NULL, " ");
			if(pTok)
				hwmArg.oid = atoi(pTok);
			else
			{
				//cout << "HWM Entry : " << aEntry << endl;
				throw (runtime_error("Bad OID in HWM entry string"));
			}

			pTok = strtok(NULL, " ");
			if(pTok)
				hwmArg.partNum = atoi(pTok);
			else
			{
				//cout << "HWM Entry : " << aEntry << endl;
				throw (runtime_error("Bad partNum in HWM entry string"));
			}

			pTok = strtok(NULL, " ");
			if(pTok)
				hwmArg.segNum = atoi(pTok);
			else
			{
				//cout << "HWM Entry : " << aEntry << endl;
				throw (runtime_error("Bad partNum in HWM entry string"));
			}

			pTok = strtok(NULL, " ");
			if(pTok)
				hwmArg.hwm = atoi(pTok);
			else
			{
				//cout << "HWM Entry : " << aEntry << endl;
				throw (runtime_error("Bad partNum in HWM entry string"));
			}

			fHWMInfo.push_back( hwmArg );

   		}
		++aIt;

	}

	if(fRef.getDebugLvl())
		cout << "prepareHighWaterMarkInfo() finished" << endl;

	return true;
}


//------------------------------------------------------------------------------
//#ROWS: numRowsRead numRowsInserted

bool WEBrmUpdater::prepareRowsInsertedInfo(std::string Entry,
									int64_t& TotRows, int64_t& InsRows)
{
	bool aFound=false;
	//ROWS: 3 1
	if ((!Entry.empty()) && (Entry.at(0) == 'R'))
	{
		aFound = true;
		const int BUFLEN=128;
		char aBuff[BUFLEN];
		strncpy(aBuff, Entry.c_str(),BUFLEN);
		aBuff[BUFLEN-1]=0;

		char*pTok = strtok(aBuff, " ");
		if (!pTok) // ignore the Msg Body
		{
			//cout << "ROWS Entry : " << aEntry << endl;
			throw(runtime_error("Bad Body in entry string"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
			TotRows = strtol(pTok, NULL, 10);
		else {
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad Tot ROWS entry string"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
			InsRows = strtol(pTok, NULL, 10);
		else {
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad inserted ROWS in entry string"));
		}

	}

	return aFound;
}


//------------------------------------------------------------------------------
//#DATA: columnNumber columnType columnName numOutOfRangeValues

bool WEBrmUpdater::prepareColumnOutOfRangeInfo(std::string Entry,
											   int& ColNum, 
											   CalpontSystemCatalog::ColDataType& ColType, 
											   std::string& ColName, 
											   int& OorValues)
{
	bool aFound=false;
	//DATA: 3 1
	if ((!Entry.empty()) && (Entry.at(0) == 'D'))
	{
		aFound = true;
		const int BUFLEN=128;
		char aBuff[BUFLEN];
		strncpy(aBuff, Entry.c_str(),BUFLEN);
		aBuff[BUFLEN-1]=0;

		char* pTok = strtok(aBuff, " ");
		if (!pTok) // ignore the Msg Body
		{
			//cout << "ROWS Entry : " << aEntry << endl;
			throw(runtime_error("Bad OOR entry info"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
		{
			ColNum = atoi(pTok);
		}
		else
		{
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad Oor Column Number entry string"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
		{
			ColType = (CalpontSystemCatalog::ColDataType)atoi(pTok);
		}
		else
		{
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad Oor Column Type entry string"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
		{
			ColName = pTok;
		}
		else
		{
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad Column Name entry string"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
		{
			OorValues = atoi(pTok);
		}
		else
		{
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad OorValues entry string"));
		}
	}

	return aFound;
}

//------------------------------------------------------------------------------
//#ERR:  error message file

bool WEBrmUpdater::prepareErrorFileInfo(std::string Entry, std::string& ErrFileName)
{
	bool aFound=false;
	if ((!Entry.empty()) && (Entry.at(0) == 'E'))
	{
		aFound = true;
		const int BUFLEN=128;
		char aBuff[BUFLEN];
		strncpy(aBuff, Entry.c_str(),BUFLEN);
		aBuff[BUFLEN-1]=0;

		char*pTok = strtok(aBuff, " ");
		if (!pTok) // ignore the Msg Body
		{
			//cout << "ROWS Entry : " << aEntry << endl;
			throw(runtime_error("Bad ERR File entry info"));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
		{
			ErrFileName = pTok;
			//int aLen = ErrFileName.length();
			//if(aLen>0) ErrFileName.insert(aLen-1, 1, 0);
		}
		else
		{
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad Error Filename entry string"));
		}

	}

	return aFound;
}


//------------------------------------------------------------------------------
//#BAD:  bad data file, with rejected rows

bool WEBrmUpdater::prepareBadDataFileInfo(std::string Entry, std::string& BadFileName)
{
	bool aFound=false;
	if ((!Entry.empty()) && (Entry.at(0) == 'B'))
	{
		aFound = true;
		const int BUFLEN=128;
		char aBuff[BUFLEN];
		strncpy(aBuff, Entry.c_str(),BUFLEN);
		aBuff[BUFLEN-1]=0;

		char*pTok = strtok(aBuff, " ");
		if (!pTok) // ignore the Msg Body
		{
			//cout << "ROWS Entry : " << aEntry << endl;
			throw(runtime_error("Bad BAD Filename entry "));
		}

		pTok = strtok(NULL, " ");
		if (pTok)
		{
			BadFileName = pTok;
			//int aLen = BadFileName.length();
			//if(aLen>0) BadFileName.insert(aLen-1, 1, 0);
		}
		else
		{
			//cout << "HWM Entry : " << aEntry << endl;
			throw(runtime_error("Bad BAD Filename entry string"));
		}
	}

	return aFound;
}


//------------------------------------------------------------------------------


} /* namespace WriteEngine */
