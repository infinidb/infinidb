/***************************************************************************
 * $Id: procmonMonitor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: David Hill
 ***************************************************************************/

#include "serverMonitor.h"

using namespace std;
using namespace oam;
using namespace snmpmanager;
using namespace logging;
using namespace servermonitor;
using namespace messageqcpp;

/************************************************************************************************************
* @brief	procmonMonitor function
*
* purpose:	Monitor Local Process Monitor (like a local heartbeat check) abd reset when it's not responding
*
*
************************************************************************************************************/

void procmonMonitor()
{
	ServerMonitor serverMonitor;
	Oam oam;

	//wait before monitoring is started
	sleep(60);

	// get current server name
	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		// Critical error, Log this event and exit
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("Failed to read local module Info");
		msg.format(args);
		ml.logCriticalMessage(msg);
		exit(-1);
	}

	string msgPort = moduleName + "_ProcessMonitor";

	int heartbeatCount = 0;

	// loop forever monitoring Local Process Monitor
	while(true)
	{

		ByteStream msg;
		ByteStream::byte requestID = LOCALHEARTBEAT;
	
		msg << requestID;
	
		try
		{
			MessageQueueClient mqRequest(msgPort);
			mqRequest.write(msg);
		
			// wait 10 seconds for response
			ByteStream::byte returnACK;
			ByteStream::byte returnRequestID;
			ByteStream::byte requestStatus;
			ByteStream receivedMSG;
		
			struct timespec ts = { 10, 0 };
			try {
				receivedMSG = mqRequest.read(&ts);
	
				if (receivedMSG.length() > 0) {
					receivedMSG >> returnACK;
					receivedMSG >> returnRequestID;
					receivedMSG >> requestStatus;
			
					if ( returnACK == oam::ACK &&  returnRequestID == requestID) {
						// ACK for this request
						heartbeatCount = 0;
					}
				}
				else
				{
					LoggingID lid(SERVER_MONITOR_LOG_ID);
					MessageLog ml(lid);
					Message msg;
					Message::Args args;
					args.add("procmonMonitor: ProcMon Msg timeout!!!");
					msg.format(args);
					ml.logWarningMessage(msg);

					heartbeatCount++;

					if ( heartbeatCount > 2 ) {
						//Process Monitor not responding, restart it
						system("pkill ProcMon");
					LoggingID lid(SERVER_MONITOR_LOG_ID);
					MessageLog ml(lid);
					Message msg;
					Message::Args args;
					args.add("procmonMonitor: Restarting ProcMon");
					msg.format(args);
					ml.logWarningMessage(msg);

						sleep(60);
						heartbeatCount = 0;
					}
				}
		
				mqRequest.shutdown();
	
			}
			catch (SocketClosed &ex) {
				string error = ex.what();

				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("procmonMonitor: EXCEPTION ERROR on mqRequest.read: " + error);
				msg.format(args);
				ml.logErrorMessage(msg);
			}
			catch (...) {
				LoggingID lid(SERVER_MONITOR_LOG_ID);
				MessageLog ml(lid);
				Message msg;
				Message::Args args;
				args.add("procmonMonitor: EXCEPTION ERROR on mqRequest.read: Caught unknown exception");
				msg.format(args);
				ml.logErrorMessage(msg);
			}
		}
		catch (exception& ex)
		{
			string error = ex.what();

			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("procmonMonitor: EXCEPTION ERROR on MessageQueueClient.read: " + error);
			msg.format(args);
			ml.logErrorMessage(msg);
		}
		catch(...)
		{
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("procmonMonitor: EXCEPTION ERROR on MessageQueueClient: Caught unknown exception");
			msg.format(args);
			ml.logErrorMessage(msg);
		}

		sleep(60);
	} //while loop
}
