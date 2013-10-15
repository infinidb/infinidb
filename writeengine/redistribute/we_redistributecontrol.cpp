/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

/*
* $Id: we_redistributecontrol.cpp 4450 2013-01-21 14:13:24Z rdempsey $
*/

#include <iostream>
#include <set>
#include <vector>
#include <cassert>
#include <stdexcept>
#include <sstream>
#include <string>
#include <ctime>
#include <unistd.h>
//#include <sys/stat.h>
using namespace std;

#include "boost/scoped_ptr.hpp"
#include "boost/scoped_array.hpp"
#include "boost/thread.hpp"
#include "boost/thread/mutex.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/filesystem/operations.hpp"
using namespace boost;

#include "installdir.h"

#include "configcpp.h"
using namespace config;

#include "liboamcpp.h"
using namespace oam;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "logger.h"

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "we_messages.h"
#include "we_redistributedef.h"
#include "we_redistributecontrolthread.h"
#include "we_redistributeworkerthread.h"
#include "we_redistributecontrol.h"

namespace redistribute
{

RedistributeControl* RedistributeControl::fInstance = NULL;
boost::mutex instanceMutex;

const string RedistributeDir("/data1/systemFiles/redistribute");
const string InfoFileName("/redistribute.info");
const string PlanFileName("/redistribute.plan");


RedistributeControl* RedistributeControl::instance()
{
	// The constructor is protected by instanceMutex lock.
	mutex::scoped_lock lock(instanceMutex);
	if (fInstance == NULL)
		fInstance = new RedistributeControl();

	return fInstance;
}


RedistributeControl::RedistributeControl() : fInfoFilePtr(NULL), fPlanFilePtr(NULL)
{
	// default path /usr/local/Calpont/data1/systemFiles/redistribute 
	string installDir = startup::StartUp::installDir();
	fRedistributeDir = installDir + RedistributeDir;
	fInfoFilePath = fRedistributeDir + InfoFileName;
	fPlanFilePath = fRedistributeDir + PlanFileName;

	fOam.reset(new oam::Oam);
	fDbrm.reset(new BRM::DBRM);
	fSysLogger.reset(new logging::Logger(32));  //32 - writeengineserver in SubsystemIDs.txt
	logging::MsgMap msgMap;
	msgMap[logging::M0002] = logging::Message(logging::M0002);
	fSysLogger->msgMap(msgMap);

	//struct stat st;
	//if (stat(fRedistributeDir.c_str(), &st) != 0)
	//filesystem::path dirPath(fRedistributeDir);
	if (filesystem::exists(fRedistributeDir))
	{
		// try to open info file for update if dir exists
		RedistributeInfo info;
		fInfoFilePtr = fopen(fInfoFilePath.c_str(), "r+");
		if (fInfoFilePtr != NULL && 1 == fread(&info, sizeof(info), 1, fInfoFilePtr))
		{
			fRedistributeInfo = info;

			// if there was an active session, mark it as failed until support resume.
			if (fRedistributeInfo.state == RED_STATE_ACTIVE)
				updateState(RED_STATE_FAILED);
		}
	}
}


RedistributeControl::~RedistributeControl()
{
	fOam.reset();
	fDbrm.reset(); 
	delete fInstance;
	fInstance = NULL;
}


int RedistributeControl::handleUIMsg(messageqcpp::ByteStream& bs, messageqcpp::IOSocket& so)
{
	mutex::scoped_lock sessionLock(fSessionMutex);

	uint32_t status = RED_STATE_UNDEF;
	const RedistributeMsgHeader* h = (const RedistributeMsgHeader*) bs.buf();
	try
	{
		switch (h->messageId)
		{
			case RED_CNTL_START:
				status = handleStartMsg(bs, so);
				break;

			case RED_CNTL_STOP:
				status = handleStopMsg(bs, so);
				break;

			case RED_CNTL_CLEAR:
				status = handleClearMsg(bs, so);
				break;

			case RED_CNTL_STATUS:
			default:
				status = handleStatusMsg(bs, so);
				break;
		}
	}
	catch (const std::exception& ex)
	{
		if (fUIResponse.empty())
			fUIResponse = ex.what();
	}
	catch (...)
	{
		if (fUIResponse.empty())
			fUIResponse = "Failed to process the redistribute command.";
	}

	// log the response
	logMessage(fUIResponse);

	//	bs restart() in handlers
	bs.restart();
	bs << (ByteStream::byte) WriteEngine::WE_SVR_REDISTRIBUTE;  // dummy, keep for now.
	bs << status;
	bs << fUIResponse;
	so.write(bs);

	return status;
}


int RedistributeControl::handleStartMsg(messageqcpp::ByteStream& bs, messageqcpp::IOSocket& so)
{
	ostringstream oss;
	uint32_t status = getCurrentState();
	if (status != RED_STATE_IDLE)
	{
		if (status == RED_STATE_ACTIVE)
			oss << "Redistribute is already running.  Command is ignored.  You need to stop and clear this active session before start a new one.";
		else
			oss << "Redistribute is not in IDLE state.  Command is ignored.  Please check the status of last session, then reset the state to IDLE using action CLEAR.";

		fUIResponse = oss.str();

		return status;
	}

	// must be IDLE state
	try
	{
		// skip the header part, may need save it.
		bs.advance(sizeof(RedistributeMsgHeader));

		getStartOptions(bs);
		RedistributeControlThread::setStopAction(false);
		updateState(RED_STATE_ACTIVE);
	}
	catch (const std::exception& ex)
	{
		if (fErrorMsg.empty())
			fErrorMsg = ex.what();

		fRedistributeInfo.state = RED_STATE_FAILED;
	}
	catch (...)
	{
		fRedistributeInfo.state = RED_STATE_FAILED;
	}

	status = fRedistributeInfo.state;
	if (status == RED_STATE_ACTIVE)
	{
		oss << "Redistribute is started.";
		fControlThread.reset(new boost::thread(RedistributeControlThread(RED_CNTL_START)));
		// Let go the new thread unless we want to call interrupt on this thread in future.
		// Not going to join() because the redistribution could take very long.
		fControlThread->detach();
		fControlThread.reset();
	}
	else
	{
		updateState(RED_STATE_FAILED);
		oss << "Starting redistribute failed.";
		if (!fErrorMsg.empty())
			oss << "  " << fErrorMsg;
	}

	fUIResponse = oss.str();

	return status;
}


int RedistributeControl::handleStatusMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket& so)
{
	ostringstream oss;
	uint32_t status = getCurrentState();
	RedistributeInfo info = fRedistributeInfo;
	switch (status)
	{
		case RED_STATE_IDLE:
			oss << "Redistribute is in IDLE state.";
			break;

		case RED_STATE_ACTIVE:
			oss << "Redistribute is in progress: total " << info.planned;
			if (info.planned > 1)
				oss << " logical partitions are planned to move.\n";
			else
				oss << " logical partition is planned to move.\n";
			if (info.planned > 0)
			{
				if (info.endTime > 0)
					oss << "In " << (info.endTime - info.startTime) << " seconds, ";
				oss << info.success << " success, "
					<< info.skipped << " skipped, "
					<< info.failed << " failed, "
					<< ((info.success+info.skipped+info.failed)*100/info.planned) << "%.";
			}
			break;

		case RED_STATE_FINISH:
			oss << "Redistribute is finished.\n"
				<< info.success << " success, "
				<< info.skipped << " skipped, "
				<< info.failed << " failed.\n";
			if (info.endTime > 0)
				oss << "Total time: " << (info.endTime - info.startTime) << " seconds.\n";
			break;

		case RED_STATE_FAILED:
			oss << "Redistribute is failed.\n";
			try
			{
				size_t l = 0;  // message length
				size_t n = fread(&l, sizeof(int), 1, fInfoFilePtr);
				if (n == 1)
				{
					boost::scoped_array<char> buf(new char[l+1]);
					n = fread(buf.get(), 1, l, fInfoFilePtr);
					if (n == l)
					{
						buf[l] = '\0';
						fErrorMsg += buf.get();
						oss << buf.get();
					}
				}
			}
			catch (const std::exception&)
			{
			}
			catch (...)
			{
			}
			break;

		case RED_STATE_STOPPED:
			oss << "Redistribute is stopped by user.\n";
			if (info.planned > 0)
			{
				if (info.endTime > 0)
					oss << "In " << (info.endTime - info.startTime) << " seconds, ";
				oss << info.success << " success, "
					<< info.skipped << " skipped, "
					<< info.failed << " failed, "
					<< ((info.success+info.skipped+info.failed)*100/info.planned) << "%.";
			}
			break;

		default:
			oss << "Failed to retrieve redistribute information, the file "
				<< fInfoFilePath << " may be corrupted.";
			break;
	}

	fUIResponse = oss.str();

	return status;
}


int RedistributeControl::handleStopMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket& so)
{
	ostringstream oss;
	uint32_t status = getCurrentState();
	if (status != RED_STATE_ACTIVE)
	{
		oss << "Redistribute is not running.  Command is ignored.";
	}
	else
	{
		RedistributeControlThread::setStopAction(true);
		updateState(RED_STATE_STOPPED);
		status = RED_STATE_STOPPED;
		boost::thread rct((RedistributeControlThread(RED_CNTL_STOP)));
		rct.join();
		oss << "Redistribute is stopped.";
	}

	fUIResponse = oss.str();

	return status;
}


int RedistributeControl::handleClearMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket& so)
{
	ostringstream oss;
	uint32_t status = getCurrentState();
	if (status == RED_STATE_ACTIVE)
	{
		oss << "Redistribute is running.  Command is ignored.  To CLEAR, you have to wait or stop the running session.";
	}
	else
	{
		updateState(RED_STATE_IDLE);
		status = RED_STATE_IDLE;
		oss << "Cleared.";
	}

	fUIResponse = oss.str();

	return status;
}


uint32_t RedistributeControl::getCurrentState()
{
	uint32_t status = RED_STATE_UNDEF;
	ostringstream oss;
	mutex::scoped_lock lock(fInfoFileMutex);
	if (!fInfoFilePtr)
	{
		status = RED_STATE_IDLE;
	}
	else
	{
		rewind(fInfoFilePtr);
		RedistributeInfo info;
		size_t n = fread(&info, sizeof(info), 1, fInfoFilePtr);
		if (n == 1)
		{
			fRedistributeInfo = info;
			status = info.state;
		}
	}

	return status;
}


bool RedistributeControl::getStartOptions(messageqcpp::ByteStream& bs)
{
	bool ret = true;
	uint32_t n = 0;
	uint32_t d = 0;

	try
	{
		bs >> fOptions;

		bs >> n;
		fSourceList.reserve(n);
		for (uint32_t i = 0; i < n; i++)
		{
			bs >> d;
			fSourceList.push_back(d);
		}
		bs >> n;
		fDestinationList.reserve(n);
		for (uint32_t i = 0; i < n; i++)
		{
			bs >> d;
			fDestinationList.push_back(d);
		}

		if (fSourceList.size() == 0 || fDestinationList.size() == 0)
			throw runtime_error("Failed to get dbroot lists.");
	}
	catch (const std::exception& ex)
	{
		ret = false;
		fErrorMsg = ex.what();
	}
	catch (...)
	{
		ret = false;
		fErrorMsg = "Failed to get dbroot lists.";
	}

	return ret;
}


void RedistributeControl::updateState(uint32_t s)
{
	mutex::scoped_lock lock(fInfoFileMutex);

	// allowed state change:
	//   idle    ->  active
	//   active  ->  finish
	//   active  ->  stopped
	//   active  ->  failed
	//   finish  ->  idle
	//   stopped ->  idle
	//   failed  ->  idle


	if (s == RED_STATE_IDLE)
	{
		if (fRedistributeInfo.state == RED_STATE_ACTIVE)
			return;

		// close the files if they are already opened
		if (fInfoFilePtr != NULL)
		{
			fclose(fInfoFilePtr);
			fInfoFilePtr = NULL;
		}

		if (fPlanFilePtr != NULL)
		{
			fclose(fPlanFilePtr);
			fPlanFilePtr = NULL;
		}

		// move old files to archive
		// zip or compress if the .plan file gets large
		time_t t = fRedistributeInfo.startTime;
		if (t == 0)
			t = time(NULL);

		ostringstream oss;
		struct tm m;
		localtime_r(&t, &m);
		oss << setfill('0') << setw(4) << (m.tm_year+1900) << setw(2) << (m.tm_mon+1)
			<< setw(2) << (m.tm_mday) << setw(2) << (m.tm_hour) << setw(2) << (m.tm_min)
			<< setw(2) << (m.tm_sec);
		try
		{
			if (filesystem::exists(fInfoFilePath) && filesystem::exists(fPlanFilePath))
			{
				bool mergeOk = false;
				FILE* infoPtr = fopen(fInfoFilePath.c_str(), "r+b");
				FILE* entryPtr = fopen(fPlanFilePath.c_str(), "rb");
				int rc = 1;
				if (infoPtr != NULL && entryPtr !=NULL)
				{
					rc = fseek(infoPtr, sizeof(RedistributeInfo), SEEK_SET);
					RedistributePlanEntry entry;
					while (rc == 0)
					{
						size_t n = fread(&entry, sizeof(entry), 1, entryPtr);
						if (n != 1)
							break;

						n = fwrite(&entry, sizeof(entry), 1, infoPtr);
						fflush(infoPtr);
						if (n != 1)
							rc = -1;
					}

				}

				if (rc == 0 && feof(entryPtr))
					mergeOk = true;

				if (infoPtr != NULL)
					fclose(infoPtr);

				if (entryPtr != NULL)
					fclose(entryPtr);

				if (mergeOk)
					filesystem::remove(fPlanFilePath);
			}

			if (filesystem::exists(fInfoFilePath))
			{
				string newInfoPath = fRedistributeDir + "/archive" + InfoFileName + "." + oss.str();
				filesystem::rename(fInfoFilePath, newInfoPath);
			}

			if (filesystem::exists(fPlanFilePath))
			{
				string newPlanPath = fRedistributeDir + "/archive" + PlanFileName + "." + oss.str();
				filesystem::rename(fPlanFilePath, newPlanPath);
			}
		}
		catch (const std::exception&)
		{
		}
		catch (...)
		{
		}

		fRedistributeInfo = RedistributeInfo();
		return;
	}

	// safety check
	if (s != RED_STATE_ACTIVE && fRedistributeInfo.state != RED_STATE_ACTIVE)
		return;

	// in IDLE state there is no redistribute.info file
	if (s == RED_STATE_ACTIVE)
	{
//		filesystem::path dirPath(fRedistributeDir);
//		if (filesystem::exists(fRedistributeDir) && !filesystem::is_directory(fRedistributeDir))
//			filesystem::remove(fRedistributeDir);
		if (!filesystem::exists(fRedistributeDir))
		{
			errno = 0;
			filesystem::create_directory(fRedistributeDir);
			if (!filesystem::exists(fRedistributeDir))
			{
				int e = errno;
				ostringstream oss;
				oss << "Failed to create redistribute directory: ";
				oss << strerror(e) << " (" << e << ")";
				throw runtime_error(oss.str());
			}

			errno = 0;
			filesystem::path archivePath(fRedistributeDir + "/archive");
			filesystem::create_directory(archivePath);
			if (!filesystem::exists(archivePath))
			{
				int e = errno;
				ostringstream oss;
				oss << "Failed to create redistribute archive directory: ";
				oss << strerror(e) << " (" << e << ")";
				throw runtime_error(oss.str());
			}
		}

		fRedistributeInfo.startTime = time(NULL); 
	}


	// open the info file to write
	errno = 0;
	if (fInfoFilePtr == NULL)
		fInfoFilePtr = fopen(fInfoFilePath.c_str(), "w+");
	if (fInfoFilePtr == NULL)
	{
		int e = errno;
		ostringstream oss;
		oss << "Failed to open " << fInfoFilePath << ": " << strerror(e) << " (" << e << ")";
		throw runtime_error(oss.str());
	}

	fRedistributeInfo.state = s;
	if (s == RED_STATE_FINISH)
		fRedistributeInfo.endTime = time(NULL);

	rewind(fInfoFilePtr);
	size_t n = fwrite(&fRedistributeInfo, sizeof(fRedistributeInfo), 1, fInfoFilePtr);
	if (n != 1)
	{
		fclose(fInfoFilePtr);
		fInfoFilePtr = NULL;

		int e = errno;
		ostringstream oss;
		oss << "Failed to write into " << fInfoFilePath << ": " << strerror(e) << " (" << e << ")";
		throw runtime_error(oss.str());
	}

	fflush(fInfoFilePtr);
}


void RedistributeControl::setEntryCount(uint32_t entryCount)
{
	mutex::scoped_lock lock(fInfoFileMutex);
	fRedistributeInfo.planned = entryCount;

	rewind(fInfoFilePtr);
	fwrite(&fRedistributeInfo, sizeof(fRedistributeInfo), 1, fInfoFilePtr);
	fflush(fInfoFilePtr);
}


void RedistributeControl::updateProgressInfo(uint32_t s, time_t t)
{
	mutex::scoped_lock lock(fInfoFileMutex);
	fRedistributeInfo.endTime = t;
	switch(s)
	{
		case RED_TRANS_SUCCESS:
			fRedistributeInfo.success++;
			break;

		case RED_TRANS_SKIPPED:
			fRedistributeInfo.skipped++;
			break;

		default:
			fRedistributeInfo.failed++;
			break;
	}

	rewind(fInfoFilePtr);
	fwrite(&fRedistributeInfo, sizeof(fRedistributeInfo), 1, fInfoFilePtr);
	fflush(fInfoFilePtr);
}


int RedistributeControl::handleJobMsg(messageqcpp::ByteStream& bs, messageqcpp::IOSocket& so)
{
//	mutex::scoped_lock jobLock(fJobMutex);

	uint32_t status = RED_TRANS_SUCCESS;
	try
	{
		fWorkThread.reset(new boost::thread(RedistributeWorkerThread(bs, so)));
		fWorkThread->join();
	}
	catch (const std::exception& ex)
	{
		status = RED_TRANS_FAILED;
		logMessage(ex.what());
	}
	catch (...)
	{
		status = RED_TRANS_FAILED;
	}

	return status;
}


void RedistributeControl::logMessage(const string& msg)
{
    logging::Message::Args args;
	args.add(string("RED:"));
    args.add(msg);

    fSysLogger->logMessage(
			logging::LOG_TYPE_INFO, logging::M0002, args, logging::LoggingID(32, 0, 0));
}


} // namespace

// vim:ts=4 sw=4:

