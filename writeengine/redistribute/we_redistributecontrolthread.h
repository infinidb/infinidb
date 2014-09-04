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
* $Id: we_redistributecontrolthread.h 4299 2012-11-02 06:00:33Z xlou $
*/

#ifndef WE_REDISTRIBUTECONTROLTHREAD_H
#define WE_REDISTRIBUTECONTROLTHREAD_H

#include <map>
#include <set>
#include <vector>

#include "boost/shared_ptr.hpp"
#include "boost/thread/mutex.hpp"


// forward reference
namespace config
{
class Config;
}

namespace oam
{
class OamCache;
}


namespace messageqcpp
{
class ByteStream;
class IOSocket;
}

namespace messagequeue
{
class MessageQueueClient;
}


namespace redistribute
{
class RedistributeControl;


class RedistributeControlThread
{
  public:
	RedistributeControlThread(uint32_t act);
	~RedistributeControlThread();

	int handleJobMsg(RedistributeMsgHeader&, messageqcpp::ByteStream&, messageqcpp::IOSocket&);

	void operator()();

	// used by control to change status
	static void setStopAction(bool);

  private:
	// struct for sort partitions
	struct PartitionInfo
	{
		int32_t dbroot;
		int32_t partition;

		PartitionInfo() : dbroot(0), partition(0) {}
		PartitionInfo(int32_t d, int32_t p) : dbroot(d), partition(p) {}

		bool operator < (const struct PartitionInfo& rhs) const
		{ return ((dbroot < rhs.dbroot) || (dbroot == rhs.dbroot && partition < rhs.partition)); }
		
		bool operator == (const struct PartitionInfo& rhs) const
		{ return (dbroot == rhs.dbroot && partition == rhs.partition); }

	};

	void doRedistribute();
	void doStopAction();

	int  setup();
	int  makeRedistributePlan();
	int  executeRedistributePlan();

	int  connectToWes(int);
	void dumpPlanToFile(uint64_t, vector<PartitionInfo>&, int);


	uint32_t                      fAction;
	oam::OamCache*                fOamCache;
	config::Config*               fConfig;
	boost::shared_ptr<messageqcpp::MessageQueueClient>  fMsgQueueClient;

	std::set<int> fSourceSet;
	std::set<int> fTargetSet;
	std::set<int> fDbrootSet;
	int           fMaxDbroot;
	uint32_t      fEntryCount;
	std::string   fErrorMsg;
	int32_t       fErrorCode;

	RedistributeControl* fControl;

	static boost::mutex  fActionMutex;
	static volatile bool fStopAction;
	static std::string   fWesInUse;
};


} // namespace


#endif  // WE_REDISTRIBUTECONTROLTHREAD_H

// vim:ts=4 sw=4:

