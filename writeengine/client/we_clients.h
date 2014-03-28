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

// $Id: weclients.h 525 2010-01-19 23:18:05Z xlou $
//
/** @file */

#ifndef WECLIENTS_H__
#define WECLIENTS_H__

#include <iostream>
#include <vector>
#include <queue>
#include <string>
#include <map>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>
#include <boost/scoped_array.hpp>

#include "bytestream.h"
//#include "we_message.h"
#include "threadsafequeue.h"
#include "rwlock_local.h"
#include "resourcemanager.h"

#if defined(_MSC_VER) && defined(xxxWECLIENTS_DLLEXPORT)
#define EXPORT __declspec(dllexport)
#else
#define EXPORT
#endif

namespace WriteEngine {
class WEClients
{
public:
	/**
	 * Constructors
	 */
	EXPORT WEClients(int PrgmID);
	EXPORT ~WEClients();
	
	//static boost::mutex map_mutex;
	EXPORT void addQueue(uint32_t key);
	EXPORT void removeQueue(uint32_t key);
	EXPORT void shutdownQueue(uint32_t key);

	/** @brief read a Write Engine Server response
	 *
	 * Returns the next message in the inbound queue for unique ids.
	 * @param bs A pointer to the ByteStream to fill in.
	 * @note: saves a copy vs read(uint, uint).
	 */
	EXPORT void read(uint32_t key, messageqcpp::SBS &);

	/** @brief write function to write to specified PM
	*/
	EXPORT void write(const messageqcpp::ByteStream &msg, uint connection);
	
	/** @brief write function to write to all PMs
	*/
	EXPORT void write_to_all(const messageqcpp::ByteStream &msg);

	/** @brief Shutdown this object
	 *
	 * Closes all the connections created during Setup() and cleans up other stuff.
	 */
	EXPORT int Close();

	/** @brief Start listening for Write Engine Server responses
	 *
	 * Starts the current thread listening on the client socket for Write Engine Server response messages. Will not return
	 * until busy() returns false or a zero-length response is received.
	 */
	EXPORT void Listen(boost::shared_ptr<messageqcpp::MessageQueueClient> client, uint connIndex);

	/** @brief set/unset busy flag
	 *
	 * Set or unset the busy flag so Listen() can return.
	 */
	EXPORT void makeBusy(bool b) { fBusy = b; }

	/** @brief fBusy accessor
	 *
	 */
	EXPORT bool Busy() const { return fBusy; }

	EXPORT void Setup();

	uint64_t connectedWEServers() const { return fPmConnections.size(); }
	
	/** @brief accessor
	 */
	uint getPmCount() { return pmCount; }
private:
	WEClients(const WEClients& weClient);
	WEClients& operator=(const WEClients& weClient);
	typedef std::vector<boost::thread*> ReaderList;
	typedef std::map<unsigned, boost::shared_ptr<messageqcpp::MessageQueueClient> > ClientList;

	//A queue of ByteStreams coming in from Write Engine Server
	typedef joblist::ThreadSafeQueue<messageqcpp::SBS> WESMsgQueue;
	
	/* To keep some state associated with the connection */
	struct MQE {
		MQE(uint pCount) : ackSocketIndex(0), pmCount(pCount){
			unackedWork.reset(new volatile uint32_t[pmCount]);
			memset((void *) unackedWork.get(), 0, pmCount * sizeof(uint32_t));
		}
		WESMsgQueue queue;
		uint ackSocketIndex;
		boost::scoped_array<volatile uint32_t> unackedWork;
		uint pmCount;
	};
	
	//The mapping of session ids to StepMsgQueueLists
	typedef std::map<unsigned, boost::shared_ptr<MQE> > MessageQueueMap;

	void StartClientListener(boost::shared_ptr<messageqcpp::MessageQueueClient> cl, uint connIndex);

	/** @brief Add a message to the queue
	 *
	 */
	void addDataToOutput(messageqcpp::SBS, uint connIndex);

	int fPrgmID;
	
	ClientList fPmConnections; // all the Write Engine servers
	ReaderList fWESReader;	// all the reader threads for the pm servers
	MessageQueueMap fSessionMessages; // place to put messages from the pm server to be returned by the Read method
  	boost::mutex fMlock; //sessionMessages mutex
 	std::vector<boost::shared_ptr<boost::mutex> > fWlock; //WES socket write mutexes
	bool fBusy;
	volatile uint closingConnection;
	uint pmCount;
	boost::mutex fOnErrMutex;   // to lock function scope to reset pmconnections under error condition
	
	boost::mutex ackLock;
public:
	enum {DDLPROC=0, SPLITTER, DMLPROC, BATCHINSERTPROC};
};

}

#undef EXPORT

#endif
// vim:ts=4 sw=4:
