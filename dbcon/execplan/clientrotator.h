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

 /*********************************************************************
 * $Id: clientrotator.h 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/

/** @file */

#ifndef CLIENTROTATOR_H
#define CLIENTROTATOR_H

#include <iostream>
#include <vector>
#include <boost/thread.hpp>
#include <stdint.h>
#include <string>

#include "bytestream.h"
#include "messagequeue.h"
#include "configcpp.h"

namespace execplan
{

/** @brief connection handle structure */
class ClientRotator
{
public:
	/** @brief ctor
	*/
	ClientRotator(uint32_t sid, const std::string& name, bool localQuery=false);

	/** @brief dtor
	*/
	~ClientRotator()
	{
		if (fClient)
		{
			fClient->shutdown();
			delete fClient;
		}
	}

	/** @brief connnect
	 *
	 * Try connecting to client based on session id.  If no connection,
	 * try connectList.
	 * @param timeout  in seconds.
	*/
	void connect(double timeout=50);

	/** @brief write
	 *
	 * Write msg to fClient.  If unsuccessful, get new connection with
	 * connectList and write.
	*/
	void write(const messageqcpp::ByteStream& msg);

	/** @brief shutdown
	*/
	void shutdown()
	{
		if (fClient)
		{
			fClient->shutdown();
			delete fClient;
			fClient = 0;
		}
	}

	/** @brief read
	*/
	messageqcpp::ByteStream read();

	/** @brief getClient
	*/
	messageqcpp::MessageQueueClient* getClient() const { return fClient; }

	/** @brief getSessionId
	*/
	uint32_t getSessionId() const { return fSessionId; }

	/** @brief setSessionId
	*/
	void setSessionId(uint32_t sid) { fSessionId = sid; }

	friend std::ostream& operator<<(std::ostream& output, const ClientRotator& rhs);

	/** @brief reset fClient */
	void resetClient();

	bool localQuery() { return fLocalQuery; }
	void localQuery(bool localQuery) { fLocalQuery = localQuery; }
	static std::string getModule();

private:

	//Not copyable
	ClientRotator(const ClientRotator& );
	ClientRotator& operator=(const ClientRotator& );

	/** @brief load Clients
	 *
	 * Put all entries for client name tag from config file into client list
	 */
	void loadClients();

	/** @brief execute connect
	 *
	 * Make connection and return success.
	 */
	bool exeConnect(const std::string& clientName );

	/** @brief connnect to list
	 *
	 * Try connecting to next client on list
	 * until timeout lapses. Then throw exception.
	 */
	void connectList(double timeout=0.005);

	/** @brief write to message log
	 *
	 * writes message with file name to debug or
	 * critical log.
	 */
	void writeToLog(int line, const std::string& msg, bool critical) const;

	const std::string fName;
	uint32_t fSessionId;
	messageqcpp::MessageQueueClient* fClient;
	typedef std::vector<std::string> ClientList;
	ClientList fClients;
	config::Config* fCf;
	int fDebug;
	boost::mutex fClientLock;
	bool fLocalQuery;
};


} // namespace
#endif
// vim:ts=4 sw=4:

