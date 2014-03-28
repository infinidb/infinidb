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

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <iostream>
#include <fstream>
#include <iomanip>
#include <cstdlib>
#include <cerrno>
#include <exception>
#include <stdexcept>

#include "liboamcpp.h"
#include "messagelog.h"
#include "messageobj.h"

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace config;

#pragma pack(push,1)
struct NotifyMsgStruct
{
	uint32_t magic;
	uint32_t msgno;
	char node[8];
	uint32_t paylen;
};
#pragma pack(pop)

/*****************************************************************************
* @brief	main
*
* purpose:	Notification Receiver Test App
*
*
******************************************************************************/

int main (int argc, char** argv)
{
	ByteStream msg;
	IOSocket fIos;

	string msgPort = "CMP1";

	//check if configured
	Config* sysConfig = Config::makeConfig();
	string ipaddr = sysConfig->getConfig(msgPort, "IPAddr");
	if (ipaddr.empty())
	{
		cerr << "CMP1 not configured in Calpont.xml, example of what to add to Calpont.xml:" << endl;
		cerr << "<CMP1>" << endl;
		cerr << "  <IPAddr>127.0.0.1</IPAddr>" << endl;
		cerr << "  <Port>8650</Port>" << endl;
		cerr << "</CMP1>" << endl << endl;
		cerr << "Exiting..." << endl;
		return 1;
	}

	//read and cleanup port before trying to use
	string port;
	try {
		Config* sysConfig = Config::makeConfig();
		port = sysConfig->getConfig(msgPort, "Port");
		string cmd = "fuser -k " + port + "/tcp >/dev/null 2>&1";
		system(cmd.c_str());
	}
	catch(...)
	{}

	cout << endl << "Notification Receiver Tester Started" << endl << endl;

	//wait for notification messages
	while(true)
	{
		try
		{
			int ls = -1;
			ls = ::socket(PF_INET, SOCK_STREAM, IPPROTO_TCP);
			if (ls < 0) throw runtime_error("socket create error");
			int rc = 0;
			struct sockaddr_in serv_addr;
			struct in_addr la;
			::inet_aton(ipaddr.c_str(), &la);
			memset(&serv_addr, 0, sizeof(serv_addr));
			serv_addr.sin_family = AF_INET;
			serv_addr.sin_addr.s_addr = la.s_addr;
			serv_addr.sin_port = htons(atoi(port.c_str()));
			rc = ::bind(ls, (sockaddr*)&serv_addr, sizeof(serv_addr));
			if (rc < 0) throw runtime_error("socket bind error");
			rc = ::listen(ls, 5);
			if (rc < 0) throw runtime_error("socket listen error");
			int ds = -1;
			for (;;)
			{
				try
				{
					ds = ::accept(ls, 0, 0);
					if (ds < 0) throw runtime_error("socket accept error");
					char hbuf[sizeof(NotifyMsgStruct)];
					ssize_t bytesRead = 0;
					ssize_t bytesNeeded = sizeof(NotifyMsgStruct);
					while (bytesRead < bytesNeeded)
					{
						ssize_t thisRead = -1;
						thisRead = ::read(ds, &hbuf[bytesRead], bytesNeeded);
						if (thisRead < 0) throw runtime_error("socket read error");
						bytesRead += thisRead;
						bytesNeeded -= thisRead;
					}
					NotifyMsgStruct* nmsp = (NotifyMsgStruct*)&hbuf[0];

					if (bytesRead > 0) {
						if ( nmsp->magic != oam::NOTIFICATIONKEY ) {
							cout << "ERROR: Invalid Header Key received, tossing the msg" << endl;
							cout << "headerKey received: " << nmsp->magic << endl;
							continue;
						}

						time_t now;
						now = time(NULL);
						struct tm tm;
						localtime_r(&now, &tm);
						char timestamp[200];
						strftime (timestamp, 200, "%H:%M:%S", &tm);

						cout << "Message Received: " << timestamp;
						cout << "  device: " << nmsp->node;
						cout << "  requestType ID: " << nmsp->msgno;

						if ( nmsp->paylen > 0 ) {
							char* payLoad;
							payLoad = (char*)alloca(nmsp->paylen);
							bytesRead = 0;
							bytesNeeded = nmsp->paylen;
							while (bytesRead < bytesNeeded)
							{
								ssize_t thisRead = -1;
								thisRead = ::read(ds, &payLoad[bytesRead], bytesNeeded);
								if (thisRead < 0) throw runtime_error("socket read error");
								bytesRead += thisRead;
								bytesNeeded -= thisRead;
							}
							cout << "  payload: " << payLoad;
						}

						switch (nmsp->msgno) {
							case START_PM_MASTER_DOWN:
							{
								cout << "  requestType: START_PM_MASTER_DOWN" << endl;
							}
							break;

							case START_PM_STANDBY_DOWN:
							{
								cout << "  requestType: START_PM_STANDBY_DOWN" << endl;
							}
							break;

							case START_PM_COLD_DOWN:
							{
								cout << "  requestType: START_PM_COLD_DOWN" << endl;
							}
							break;

							case START_UM_DOWN:
							{
								cout << "  requestType: START_UM_DOWN" << endl;
							}
							break;

							case MODULE_DOWN:
							{
								cout << "  requestType: MODULE_DOWN" << endl;
							}
							break;

							case START_STANDBY_TO_MASTER:
							{
								cout << "  requestType: START_STANDBY_TO_MASTER" << endl;
							}
							break;

							case PM_MASTER_ACTIVE:
							{
								cout << "  requestType: PM_MASTER_ACTIVE" << endl;
							}
							break;

							case PM_STANDBY_ACTIVE:
							{
								cout << "  requestType: PM_STANDBY_ACTIVE" << endl;
							}
							break;

							case PM_COLD_ACTIVE:
							{
								cout << "  requestType: PM_COLD_ACTIVE" << endl;
							}
							break;

							case UM_ACTIVE:
							{
								cout << "  requestType: UM_ACTIVE" << endl;
							}
							break;

							case PM_MASTER_FAILED_DISABLED:
							{
								cout << "  requestType: PM_MASTER_FAILED_DISABLED" << endl;
							}
							break;

							case DBROOT_DOWN:
							{
								cout << "  requestType: DBROOT_DOWN" << endl;
							}
							break;

							case DBROOT_UP:
							{
								cout << "  requestType: DBROOT_UP" << endl;
							}
							break;

							case DB_HEALTH_CHECK_FAILED:
							{
								cout << "  requestType: DB_HEALTH_CHECK_FAILED" << endl;
							}
							break;

							case DBROOT_MOUNT_FAILURE:
							{
								cout << "  requestType: DBROOT_MOUNT_FAILURE" << endl;
							}
							break;

							case MODULE_UP:
							{
								cout << "  requestType: MODULE_UP" << endl;
							}
							break;

							default:
							{
								cout << "  Invalid requestType: " << nmsp->msgno << endl;
							}
							break;
						}
					}
					else
						cout << "Message received of size 0" << endl;

					::shutdown(ds, SHUT_RDWR);
					::close(ds);
				}
				catch(...)
				{
					cout << "accept/read exception received" << endl;
				}
			}
		}
		catch(...)
		{
			cout << "MessageQueueServer exception received" << endl;
		}
	}

	return 0;
}
// vim:ts=4 sw=4:

