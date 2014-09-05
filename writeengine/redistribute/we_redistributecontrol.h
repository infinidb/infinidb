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

/*
* $Id: we_redistributecontrol.h 4450 2013-01-21 14:13:24Z rdempsey $
*/

#ifndef WE_REDISTRIBUTECONTROL_H
#define WE_REDISTRIBUTECONTROL_H

#include <vector>
#include <stdio.h>

#include "boost/shared_ptr.hpp"
#include "boost/thread/mutex.hpp"

#include "liboamcpp.h"

// forward reference
namespace messageqcpp
{
class ByteStream;
class IOSocket;
}

namespace oam
{
class Oam;
}

namespace BRM
{
class DBRM;
}

namespace logging
{
class Logger;
}

namespace redistribute
{

class RedistributeControl
{
  public:
	~RedistributeControl();

	int handleUIMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket&);
	int handleJobMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket&);

	static RedistributeControl* instance();
	static void destroyInstace();

  private:
	int handleStartMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket&);
	int handleStatusMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket&);
	int handleStopMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket&);
	int handleClearMsg(messageqcpp::ByteStream&, messageqcpp::IOSocket&);

	int handleStatusRpt(messageqcpp::ByteStream&, messageqcpp::IOSocket&);

	uint32_t getCurrentState();
	bool getStartOptions(messageqcpp::ByteStream&);

	void setEntryCount(uint32_t);
	void updateState(uint32_t);
	void updateProgressInfo(uint32_t, time_t);

	void logMessage(const std::string&);

	boost::mutex fSessionMutex;
	boost::mutex fInfoFileMutex;

	boost::scoped_ptr<boost::thread> fControlThread;
	boost::scoped_ptr<boost::thread> fWorkThread;

	FILE*               fInfoFilePtr;
	FILE*               fPlanFilePtr;
	std::string         fRedistributeDir;
	std::string         fInfoFilePath;
	std::string         fPlanFilePath;
	std::string         fUIResponse;

	uint32_t            fOptions;
	std::vector<int>    fSourceList;
	std::vector<int>    fDestinationList;
	std::vector<RedistributePlanEntry> fRedistributePlan;
	RedistributeInfo    fRedistributeInfo;

	std::string         fErrorMsg;

	// for work threads, they don't have to create their own.
	boost::shared_ptr<oam::Oam>        fOam;
	boost::shared_ptr<BRM::DBRM>       fDbrm;
	boost::shared_ptr<logging::Logger> fSysLogger;

	// singleton instance
	static RedistributeControl* fInstance;

	// private constructor
	RedistributeControl();

	// disable copy constructor and assignment operator
	// private without implementation
	RedistributeControl(const RedistributeControl&);
	RedistributeControl& operator=(const RedistributeControl&);


	friend class RedistributeControlThread;
	friend class RedistributeWorkerThread;
};


} // namespace


#endif  // WE_REDISTRIBUTECONTROL_H

// vim:ts=4 sw=4:

