/* Copyright (C) 2013 Calpont Corp.

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
 * we_splitterapp.cpp
 *
 *  Created on: Oct 7, 2011
 *      Author: bpaul
 */

#include <cstdlib>
#include <csignal>

#include <string>
#include <stdexcept>
using namespace std;

#include "batchloader.h"
using namespace batchloader;

#include "we_messages.h"
#include "we_splitterapp.h"

#include "installdir.h"

static int SPLTR_EXIT_STATUS=0;


namespace WriteEngine
{
bool WESplitterApp::fContinue = true;
bool WESplitterApp::fSignaled = false;
bool WESplitterApp::fSigHup = false;

SimpleSysLog* WESplitterApp::fpSysLog = 0;


//WESplitterApp::WESplitterApp(WECmdArgs& CmdArgs) :
//		fCmdArgs(CmdArgs), fDh(*this), fpSysLog(0)
WESplitterApp::WESplitterApp(WECmdArgs& CmdArgs) :
		fCmdArgs(CmdArgs), fDh(*this)
{
	fpSysLog = SimpleSysLog::instance();
	fpSysLog->setLoggingID(logging::LoggingID(SUBSYSTEM_ID_WE_SPLIT));
	setupSignalHandlers();
	std::string err;
	fDh.setDebugLvl(fCmdArgs.getDebugLvl());

	fDh.check4CpiInvokeMode();

	fCmdArgs.checkForCornerCases();

	if (fCmdArgs.isCpimportInvokeMode())
	{
		try
		{
			invokeCpimport();
		}
		catch(std::exception& ex)
		{
			cout << "Invoking Mode 3" << endl;
			cout << ex.what() << endl;
			SPLTR_EXIT_STATUS=1;
			exit(SPLTR_EXIT_STATUS);
		}
		exit(SPLTR_EXIT_STATUS);
	}
	else
	{
		if(fCmdArgs.isHelpMode()) fCmdArgs.usage();

		if(fCmdArgs.getMultiTableCount() <= 1)
		{
			try
			{
				fDh.setup();
			}
			catch (std::exception& ex)
			{
				//err = string("Error in constructing WESplitterApp") + ex.what();
				err = ex.what();		//cleaning up for BUG 4298
				logging::Message::Args errMsgArgs;
				errMsgArgs.add(err);
				fpSysLog->logMsg(errMsgArgs,logging::LOG_TYPE_ERROR,logging::M0000);
				SPLTR_EXIT_STATUS=1;
				//cout << err << endl;
				fDh.fLog.logMsg( err, MSGLVL_ERROR );
				fContinue = false;
				//throw runtime_error(err); BUG 4298
			}
		}
	}

}

WESplitterApp::~WESplitterApp()
{
	//fDh.shutdown();
	usleep(1000); //1 millisec just checking

	std::string aStr = "Calling WESplitterApp Destructor\n";
	if(fDh.getDebugLvl()) cout << aStr << endl;

}

//------------------------------------------------------------------------------
// Initialize signal handling
//------------------------------------------------------------------------------

void WESplitterApp::setupSignalHandlers()
{
#ifdef _MSC_VER
	//FIXME
#else
	signal(SIGPIPE, SIG_IGN);
	signal(SIGINT, WESplitterApp::onSigInterrupt);
	signal(SIGTERM, WESplitterApp::onSigTerminate);
	signal(SIGHUP, WESplitterApp::onSigHup);
#endif
}
//------------------------------------------------------------------------------
// handles on signal Terminate 
//------------------------------------------------------------------------------
void WESplitterApp::onSigTerminate(int aInt)
{
	if(15 == aInt)
	{
		fSignaled = true;
	}
	fContinue = false; //force to call destructor
	if(aInt == 1) SPLTR_EXIT_STATUS = 1;

}

//------------------------------------------------------------------------------
// handles on signal Interrupt 
//------------------------------------------------------------------------------
void WESplitterApp::onSigInterrupt(int aInt)
{
	if(2 == aInt)
	{
		fSignaled = true;
	}
	fContinue = false; //force to call destructor
	if(aInt == 1) SPLTR_EXIT_STATUS = 1;
}

//------------------------------------------------------------------------------
// handles on signal HUP send by OAM
//------------------------------------------------------------------------------
void WESplitterApp::onSigHup(int aInt)
{
	fSigHup = true;
	fContinue = false;
	std::string aStr = "Interrupt received...Program Exiting...";
	cout << aStr << endl;
	if(aInt == 1) SPLTR_EXIT_STATUS = 1;
	logging::Message::Args errMsgArgs;
	errMsgArgs.add(aStr);
	fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0000);

	exit(SPLTR_EXIT_STATUS);	//BUG 4534 - exit w/o writing to log
}

//------------------------------------------------------------------------------
// Process messages on Main thread 
//------------------------------------------------------------------------------
void WESplitterApp::processMessages()
{
	messageqcpp::ByteStream aBs;
	unsigned int aRollCount = 0;

	if(fDh.getDebugLvl())
		cout << "Inside WESplitterApp::processMessages() "
				<< "Mode = " << fCmdArgs.getMode() << endl;

	//TODO - handle all the messages here
	if (fCmdArgs.getMode() == 2)
	{
		try
		{
			aBs << (ByteStream::byte) WE_CLT_SRV_MODE;
			aBs << (ByteStream::quadbyte) fCmdArgs.getMode();
			fDh.send2Pm(aBs);

			std::string aJobId = fCmdArgs.getJobId();
			if(aJobId.length()>0) // Export jobFile NOW
			{
				std::string aJobFileName = fCmdArgs.getJobFileName();
				fDh.exportJobFile(aJobId, aJobFileName );
			}

			aBs.restart();
			std::string aCpImpCmd = fCmdArgs.getCpImportCmdLine();
			fDh.fLog.logMsg( aCpImpCmd, MSGLVL_INFO2 );
			if(fDh.getDebugLvl())
				cout << "CPImport cmd line - " << aCpImpCmd << endl;
			aBs << (ByteStream::byte) WE_CLT_SRV_CMDLINEARGS;
			aBs << aCpImpCmd;
			fDh.send2Pm(aBs);

			aBs.restart();
			std::string aBrmRpt = fCmdArgs.getBrmRptFileName();
			if(fDh.getDebugLvl())
				cout << "BrmReport FileName - " << aBrmRpt << endl;
			aBs << (ByteStream::byte) WE_CLT_SRV_BRMRPT;
			aBs << aBrmRpt;
			fDh.send2Pm(aBs);

		}
		catch (std::exception& exp)
		{
			//cout << exp.what() << endl;
			SPLTR_EXIT_STATUS=1;
			//exit(SPLTR_EXIT_STATUS);
			throw runtime_error(exp.what());
		}
	}
	else if (fCmdArgs.getMode() == 1)
	{
		try
		{
			// In this mode we ignore almost all cmd lines args which
			// are usually send to cpimport
			aBs << (ByteStream::byte) WE_CLT_SRV_MODE;
			aBs << (ByteStream::quadbyte) fCmdArgs.getMode();
			fDh.send2Pm(aBs);

			std::string aJobId = fCmdArgs.getJobId();
			if(fDh.getDebugLvl()) cout<<"ProcessMsgs aJobId "<<aJobId<<endl;
			if(aJobId.length()>0)				// Export jobFile NOW
			{
				std::string aJobFileName = fCmdArgs.getJobFileName();
				if(fDh.getDebugLvl()) cout<<"ProcessMsgs Calling exportJobFile "<<endl;
				fDh.exportJobFile(aJobId, aJobFileName );
				if(fDh.getDebugLvl()) cout<<"ProcessMsgs Calling exportJobFile "<<endl;
			}

			aBs.restart();
			std::string aCpImpCmd = fCmdArgs.getCpImportCmdLine();
			fDh.fLog.logMsg( aCpImpCmd, MSGLVL_INFO2 );
			if(fDh.getDebugLvl())
				cout << "CPImport cmd line - " << aCpImpCmd << endl;
			aBs << (ByteStream::byte) WE_CLT_SRV_CMDLINEARGS;
			aBs << aCpImpCmd;
			fDh.send2Pm(aBs);

			aBs.restart();
			std::string aBrmRpt = fCmdArgs.getBrmRptFileName();
			if(fDh.getDebugLvl())
				cout << "BrmReport FileName - " << aBrmRpt << endl;
			aBs << (ByteStream::byte) WE_CLT_SRV_BRMRPT;
			aBs << aBrmRpt;
			fDh.send2Pm(aBs);

		} catch (std::exception& exp)
		{
			//cout << exp.what() << endl;
			SPLTR_EXIT_STATUS=1;
			//exit(SPLTR_EXIT_STATUS);
			throw runtime_error(exp.what());
		}
	}
	else if (fCmdArgs.getMode() == 0)
	{
		try
		{
			// In this mode we ignore almost all cmd lines args which
			// are usually send to cpimport
			aBs << (ByteStream::byte) WE_CLT_SRV_MODE;
			aBs << (ByteStream::quadbyte) fCmdArgs.getMode();
			fDh.send2Pm(aBs);

			aBs.restart();
			std::string aCpImpFileName = fCmdArgs.getPmFile();
			if(aCpImpFileName.length()==0)
			{
				fCmdArgs.setPmFile(fCmdArgs.getLocFile());
				aCpImpFileName = fCmdArgs.getPmFile();
				if((aCpImpFileName.length()==0)||(aCpImpFileName == "STDIN"))
				{
					throw (runtime_error("PM Remote filename not specified"));
				}
			}
			if(fDh.getDebugLvl())
				cout << "CPImport FileName - " << aCpImpFileName << endl;
			aBs << (ByteStream::byte) WE_CLT_SRV_IMPFILENAME;
			aBs << aCpImpFileName;
			fDh.send2Pm(aBs);
		}
		catch (std::exception& exp)
		{
			//cout << exp.what() << endl;
			SPLTR_EXIT_STATUS=1;
			//exit(SPLTR_EXIT_STATUS);
			throw runtime_error(exp.what());
		}
	}

	int aNoSec=2;
	bool bRollback = false;
	bool bForce = false;
	int  iShutdown;
	// TODO - this is for just time being....
	// we need to process message of main thread here..
	// here we need to cont check the status of different things
	while (fContinue)
	{
		++aRollCount;
		usleep(1000000);
		// Check to see if someone has ordered a shutdown with rollback or force.
		iShutdown = fDh.fDbrm.getSystemShutdownPending(bRollback, bForce);
		if (iShutdown >= 0)
		{
			if (bRollback)
			{
				if (iShutdown > 0) // Means a shutdown, stop or restart
				{
					cout << "System stop has been ordered. Rollback" << endl;
				}
				else
				{
					cout << "Database writes have been suspended. Rollback" << endl;
				}
				fSignaled = true;
				fContinue = false;
			}
			else
			if (bForce)
			{
				//BUG 5012  - added to avoid rollback
				fContinue = false;
				ostringstream oss;
				oss << "Table "<<fCmdArgs.getSchemaName()<<".";
				oss << fCmdArgs.getTableName() << ": (OID-";
				oss << fDh.getTableOID() << ") was NOT successfully loaded.";
				cout << oss.str() << endl;
				logging::Message::Args errMsgArgs;
				//BUG 4152
				errMsgArgs.add(fCmdArgs.getSchemaName());
				errMsgArgs.add(fCmdArgs.getTableName());
				errMsgArgs.add(fDh.getTableOID());
				std::string aStr = "Immediate system stop has been ordered, rollback deferred";
				cout << aStr << endl;
				SPLTR_EXIT_STATUS = 1;
				errMsgArgs.add(aStr);
				fpSysLog->logMsg(errMsgArgs, logging::LOG_TYPE_INFO, logging::M0096);
				exit(SPLTR_EXIT_STATUS);
				//BUG 5012  - commented out to avoid rollback
				//cout << "Immediate system stop has been ordered. No rollback" << endl;
				//fSignaled = true;
				//fContinue = false;
			}
		}

		// Send out a heartbeat to the WriteEnginServers every 10 seconds
		if ((0 == (aRollCount % aNoSec)) && (!fSignaled)) // Debugging - every 10 seconds
		{
			if(aNoSec<10) aNoSec++;		//progressively go up to 10Sec interval
			aBs.restart();
			aBs << (ByteStream::byte) WE_CLT_SRV_KEEPALIVE;
			mutex::scoped_lock aLock(fDh.fSendMutex);
			fDh.send2Pm(aBs);
			aLock.unlock();
            //fDh.sendHeartbeats();
            //fDh.checkForConnections(); - decided to recv SIGHUP from OAM instead of this
		}
	}

	fDh.shutdown();

} // processMessages

void WESplitterApp::invokeCpimport()
{
	//BUG 4361 - check cpimport.bin is available or not
	std::string aCpiBinFile = getCalpontHome() + "/cpimport.bin";			//BUG 4361
	ifstream iGoodFile(aCpiBinFile.c_str());										//BUG 4361
	if(!iGoodFile.good()) throw runtime_error("Error : Missing File "+ aCpiBinFile);//BUG 4361

	fCmdArgs.setMode(3);
	std::string aCmdLineStr = fCmdArgs.getCpImportCmdLine();

	updateCmdLineWithPath(aCmdLineStr);

	if(fDh.getDebugLvl())
		cout << "CPI CmdLineArgs : " << aCmdLineStr << endl;

	std::vector<char*> Cmds;


    std::istringstream ss(aCmdLineStr);
    std::string arg;
    std::vector<std::string> v2;
    while(ss >> arg)
    {
		//we need something that works on Windows as well as linux
		char* ptr = 0;
        v2.push_back(arg);
		//we're going to exec() below, so don't worry about freeing
		ptr = strdup(v2.back().c_str());
        Cmds.push_back(ptr);
    }

    Cmds.push_back(0);    //null terminate

	int aRet = execv(Cmds[0], &Cmds[0]);	//NOTE - works with full Path

	if(fDh.getDebugLvl())
		cout << "Return status of cpimport is " << aRet <<endl;

}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Include the absolute path to prgm name, which is
 * @brief 	the first element in the vector
 * @param 	V vector which contains each element of argv
 *
 **/
std::string WESplitterApp::getCalpontHome()
{
	string calpontDir = config::Config::makeConfig()->getConfig(
			"SystemConfig", "CalpontHome");
	if(0 == calpontDir.length())
	{
		calpontDir = startup::StartUp::installDir() + "/bin";
	}
	else
	{
		calpontDir += "/bin";
	}

	return calpontDir;
}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Include the absolute path to prgm name, which is
 * @brief 	the first element in the vector
 * @param 	V vector which contains each element of argv
 *
 **/
std::string WESplitterApp::getPrgmPath(std::string& PrgmName)
{
	std::string cpimportPath = getCalpontHome();
	cpimportPath += "/";
	cpimportPath += PrgmName;
	return cpimportPath;
}

//-----------------------------------------------------------------------------
/**
 *
 * @brief 	Include the absolute path to prgm name, which is
 * @brief 	the first element in the vector
 * @param 	V vector which contains each element of argv
 *
 **/

void WESplitterApp::updateCmdLineWithPath(string& CmdLine)
{
	std::istringstream iSs(CmdLine);
	std::ostringstream oSs;
	std::string aArg;
	int aCount=0;
	while(iSs >> aArg)
	{
		if(0 == aCount)
		{
			string aPrgmPath = getPrgmPath(aArg);
			oSs << aPrgmPath;
		}
		else
		{
			oSs << " ";
			oSs << aArg;

		}
		++aCount;
	}

	CmdLine = oSs.str();
}

//-----------------------------------------------------------------------------
void WESplitterApp::updateWithJobFile(int aIdx)
{
	fCmdArgs.updateWithJobFile(aIdx);
}

//-----------------------------------------------------------------------------


} /* namespace WriteEngine */

//------------------------------------------------------------------------------
// main function 
//------------------------------------------------------------------------------

int main(int argc, char** argv)
{
	std::string err;
	setuid(0);		//@BUG 4343 set effective userid to root.
	std::cin.sync_with_stdio(false);
	try
	{
		WriteEngine::WECmdArgs aWeCmdArgs(argc, argv);
		WriteEngine::WESplitterApp aWESplitterApp(aWeCmdArgs);
		int aTblCnt = aWESplitterApp.fCmdArgs.getMultiTableCount();
		if(aTblCnt>1)
		{
			for(int idx=0; idx<aTblCnt; idx++)
			{
				aWESplitterApp.fDh.reset();
				aWESplitterApp.fContinue = true;
				aWESplitterApp.updateWithJobFile(idx);
				try
				{
					aWESplitterApp.fDh.setup();
				}
				catch (std::exception& ex)
				{
					//err = string("Error in constructing WESplitterApp") + ex.what();
					err = ex.what();		//cleaning up for BUG 4298
					logging::Message::Args errMsgArgs;
					errMsgArgs.add(err);
					aWESplitterApp.fpSysLog->logMsg(errMsgArgs,logging::LOG_TYPE_ERROR,logging::M0000);
					SPLTR_EXIT_STATUS=1;
					aWESplitterApp.fDh.fLog.logMsg( err, MSGLVL_ERROR );
					aWESplitterApp.fContinue = false;
					//throw runtime_error(err); BUG 4298
				}
				aWESplitterApp.processMessages();

				if(SPLTR_EXIT_STATUS == 1) break;
			}
		}
		else
		{
			aWESplitterApp.processMessages();
		}
	} catch (std::exception& exp)
	{
		cerr << exp.what() << endl;
		SPLTR_EXIT_STATUS = 1;
		exit(SPLTR_EXIT_STATUS);	// exit with an error
	}

	return SPLTR_EXIT_STATUS;
}
// vim:ts=4 sw=4:

