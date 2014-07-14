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

/***************************************************************************
 * $Id: msgProcessor.cpp 34 2006-09-29 21:13:54Z dhill $
 *
 *   Author: David Hill
 ***************************************************************************/

#include "serverMonitor.h"

using namespace std;
using namespace oam;
using namespace messageqcpp;
using namespace logging;
using namespace servermonitor;
using namespace config;

extern float currentCpuUsage;
extern unsigned long totalMem;
extern SystemDiskList sdl;
extern ProcessCPUList pcl;
extern ProcessMemoryList pml;
extern pthread_mutex_t CPU_LOCK;
extern pthread_mutex_t MEMORY_LOCK;

/**
 * constants define
 */ 

struct PendingSQLStatement
{
	string 		sqlStatement;
	time_t 		startTime;
};


/*****************************************************************************************
* @brief	msgProcessor Thread
*
* purpose:	Process incoming message request
*
*****************************************************************************************/
void msgProcessor()
{
	ServerMonitor serverMonitor;
	Oam oam;

	ByteStream msg;
	IOSocket fIos;

	string name[5];
	double usage[5];

	bool DBhealthActive = false;

	// get current server name
	string moduleName;
	oamModuleInfo_t st;
	try {
		st = oam.getModuleInfo();
		moduleName = boost::get<0>(st);
	}
	catch (...) {
		LoggingID lid(SERVER_MONITOR_LOG_ID);
		MessageLog ml(lid);
		Message msg;
		Message::Args args;
		args.add("Failed to get Module Name");
		msg.format(args);
		ml.logErrorMessage(msg);
		moduleName = "Unknown Server";
	}

	string msgPort = moduleName + "_ServerMonitor";

	//read and cleanup port before trying to use
	try {
		Config* sysConfig = Config::makeConfig();
		string port = sysConfig->getConfig(msgPort, "Port");
		string cmd = "fuser -k " + port + "/tcp";
		int user;
		user = getuid();
		if (user != 0)
			cmd = "sudo fuser -k " + port + "/tcp";

		system(cmd.c_str());
	}
	catch(...)
	{
	}

	for (;;)
	{
		try
		{
			MessageQueueServer mqs(msgPort);
			for (;;)
			{
				try
				{
					fIos = mqs.accept();
					msg = fIos.read();

					if (msg.length() > 0) {
						ByteStream::byte requestType;
						msg >> requestType;

						switch (requestType) {
							case GET_PROC_CPU_USAGE:
							{
								ByteStream::byte top_users;
								ByteStream ackmsg;

								msg >> top_users;

								int count = 0;

								//
								// get Process and System CPU usage
								//
								pthread_mutex_lock(&CPU_LOCK);
								serverMonitor.getCPUdata();

								ProcessCPUList::iterator p = pcl.begin();
								while(p != pcl.end())
								{
									double cpuUsage =  (*p).usedPercent;

									if ( cpuUsage != 0 ) {
										usage[count] = cpuUsage;
										name[count] = (*p).processName;
										count++;
									}
									p++;
								}

								// if all processes are idle, puch the first one for display
								if ( count == 0 ) {
									p = pcl.begin();
									usage[count] = (*p).usedPercent;
									name[count] = (*p).processName;
									count++;
								}

								pthread_mutex_unlock(&CPU_LOCK);

								if ( count < top_users )
									ackmsg << (ByteStream::byte) count;
								else
									ackmsg << (ByteStream::byte) top_users;

								// output top requested processes
								for (int i = 0 ; i < count ; i++)
								{
									ackmsg << name[i];
									ackmsg << (ByteStream::quadbyte) usage[i];
									if ( i == top_users )
										break;
								}

								fIos.write(ackmsg);

								ackmsg.reset();
							}
							break;

							case GET_MODULE_CPU_USAGE:
							{
								ByteStream ackmsg;
								//
								// get Process and System CPU usage
								//
								pthread_mutex_lock(&CPU_LOCK);
								serverMonitor.getCPUdata();

								ackmsg << (ByteStream::byte) currentCpuUsage;

								pthread_mutex_unlock(&CPU_LOCK);

								fIos.write(ackmsg);

								ackmsg.reset();
							}
							break;

							case GET_PROC_MEMORY_USAGE:
							{
								ByteStream ackmsg;
								ByteStream::byte top_users;

								msg >> top_users;

								//
								// get top Memory users by process
								//

								pthread_mutex_lock(&MEMORY_LOCK);
								serverMonitor.outputProcMemory(false);

								ackmsg << (ByteStream::byte) pml.size();

								ProcessMemoryList::iterator p = pml.end();
								while(p != pml.begin())
								{
									p--;
									ackmsg << (*p).processName;
									ackmsg << (ByteStream::quadbyte) (*p).usedBlocks;
									ackmsg << (ByteStream::byte) (*p).usedPercent;
								}

								pthread_mutex_unlock(&MEMORY_LOCK);

								fIos.write(ackmsg);

								ackmsg.reset();
							}
							break;

							case GET_MODULE_MEMORY_USAGE:
							{
								//
								// get Module Memory/Swap usage
								//

								ByteStream ackmsg;

								// get cache MEMORY stats
								system("cat /proc/meminfo | grep Cached -m 1 | awk '{print $2}' > /tmp/cached");
						
								ifstream oldFile ("/tmp/cached");
						
								string strCache;
								long long cache;
						
								char line[400];
								while (oldFile.getline(line, 400))
								{
									strCache = line;
									break;
								}
								oldFile.close();
						
								if (strCache.empty() )
									cache = 0;
								else
									cache = atol(strCache.c_str()) * 1024;
						
  								struct sysinfo myinfo; 
								sysinfo(&myinfo);
						
								//get memory stats
								unsigned long mem_total = myinfo.totalram ; 
								unsigned long freeMem = myinfo.freeram ;
						
								// adjust for cache, which is available memory
								unsigned long mem_used = mem_total - freeMem - cache;
						
								//get swap stats
								unsigned long swap_total = myinfo.totalswap ;
								unsigned long freeswap = myinfo.freeswap ;
								unsigned long swap_used = swap_total - freeswap ;

								unsigned int memoryUsagePercent;
								memoryUsagePercent =  (mem_used / (mem_total / 100));
						
								unsigned int swapUsagePercent;
								if ( swap_total == 0 )
									swapUsagePercent = 0;
								else
									swapUsagePercent =  (swap_used / (swap_total / 100));

								ackmsg << (ByteStream::quadbyte) (mem_total / 1024);
								ackmsg << (ByteStream::quadbyte) (mem_used / 1024);
								ackmsg << (ByteStream::quadbyte) (cache / 1024);
								ackmsg << (ByteStream::byte) memoryUsagePercent;
								ackmsg << (ByteStream::quadbyte) (swap_total / 1024);
								ackmsg << (ByteStream::quadbyte) (swap_used / 1024);
								ackmsg << (ByteStream::byte) swapUsagePercent;

								fIos.write(ackmsg);

								ackmsg.reset();
							}
							break;

							case GET_MODULE_DISK_USAGE:
							{
								//
								// get Module Disk usage
								//

								ByteStream ackmsg;
 
								ackmsg << (ByteStream::byte) sdl.size();

								SystemDiskList::iterator p = sdl.begin();
								while(p != sdl.end())
								{
									ackmsg << (*p).deviceName;
									//ackmsg << (ByteStream::quadbyte) ((*p).totalBlocks / 1024) ;
									//ackmsg << (ByteStream::quadbyte) ((*p).usedBlocks / 1024);
									//ackmsg << (ByteStream::byte) (*p).usedPercent;
									ackmsg << (uint64_t) ((*p).totalBlocks / 1024) ;
									ackmsg << (uint64_t) ((*p).usedBlocks / 1024);
									ackmsg << (uint8_t) (*p).usedPercent;
									p++;
								}

								fIos.write(ackmsg);

								ackmsg.reset();
							}
							break;

							case GET_ACTIVE_SQL_QUERY:
							{
								//
								// get Active SQL Query
								//  determined from UM debug.log file
								//
								map<uint64_t, PendingSQLStatement> pendingSQLStatements;
								map<uint64_t, PendingSQLStatement>::iterator pendingIter;

								ByteStream ackmsg;
								char line[15100];
								char* pos;
								uint64_t  currentSessionID;
								const char* szStartSql = "Start SQL statement";
								const char* szEndSql = "End SQL statement";

								time_t rawtime;
								struct tm tmStartTime;
								time_t moduleStartTime = 0;
								time_t queryStartTime = 0;

								string fileName = "/var/log/Calpont/debug.log";
								try
								{
									// Get ServerMonitor start time. We don't report any SQL that started before then.
									Oam oam;
									ProcessStatus procstat;
									oam.getProcessStatus("ExeMgr", moduleName, procstat);

									if (strptime((procstat.StateChangeDate).c_str(), "%a %b %d %H:%M:%S %Y", &tmStartTime) != NULL)
									{
										tmStartTime.tm_isdst = -1;
										moduleStartTime = mktime(&tmStartTime);
									}

									cout << "UM start time " << moduleStartTime << endl;
									// Open the Calpont debug.log file
									ifstream file (fileName.c_str());
									if (!file)
									{
										try {
											LoggingID lid(SERVER_MONITOR_LOG_ID);
											MessageLog ml(lid);
											Message msg;
											Message::Args args;
											args.add("File open error: ");
											args.add(fileName);
											msg.format(args);
											ml.logErrorMessage(msg);
										}
										catch (...)
										{}

										ackmsg << (ByteStream::byte) oam::API_FILE_OPEN_ERROR;
										fIos.write(ackmsg);
										break;
									}

									ackmsg << (ByteStream::byte) oam::API_SUCCESS;

									// Read the file. Filter out anything we don't care about. Store
									// each SQL Start statement. When a SQL End statement is found, remove the
									// corresponding SQL statement from the collection.
									while (file.good())
									{
										file.getline(line, 15100, '\n');
										pos = strstr(line, szStartSql);

										if (pos)
										{
											//filter out System Catalog inqueries
											if (strstr(pos+21, "/FE") || strstr(pos+21, "/EC"))
											{
												continue;
											}
											// Filter any query that started before the ServerMonitor
											if (strptime(line, "%b %d %H:%M:%S", &tmStartTime) != NULL)
											{
												// The date in the debug.log file doesn't have a year.
												// Assume the start time is no more than a year ago.
												struct tm tmNow;
												tmStartTime.tm_isdst = -1;
												time ( &rawtime );
												localtime_r ( &rawtime, &tmNow );
												// Allow for New year turnover
												if (tmStartTime.tm_mon > tmNow.tm_mon)
												{
													tmStartTime.tm_year = tmNow.tm_year - 1;
												}
												else
												{
													tmStartTime.tm_year = tmNow.tm_year;
												}
												queryStartTime = mktime(&tmStartTime);

												// Ignore if the query started before this process
												if (queryStartTime < moduleStartTime)
												{
													continue;
												}
											}
											else
											{
												continue;
											}

											// Find the sessionid
											char* pos1;
											char* pos2;
											pos1 = strchr(line, '|');
											if (!pos1)
											{
												continue;
											}
											pos2 = strchr(pos1+1, '|');
											if (!pos2)
											{
												continue;
											}

											currentSessionID = strtoll(pos1+1, NULL, 0);

											// Check the map for this sessionid. If found, we have two pending
											// SQL statements from the same session, which is theoretically
											// impossible. Throw the first one away for now. Error handling?
											if ((pendingIter = pendingSQLStatements.find(currentSessionID)) != pendingSQLStatements.end())
											{
												pendingSQLStatements.erase(pendingIter);
											}

											PendingSQLStatement pendingSQLStatement;
											pendingSQLStatement.sqlStatement = pos + 21;
											pendingSQLStatement.startTime = queryStartTime;
											pair<int64_t, PendingSQLStatement> sqlPair;
											sqlPair.first = currentSessionID;
											sqlPair.second = pendingSQLStatement;
											pendingSQLStatements.insert(sqlPair);
										}
										else
										{
											pos = strstr(line, szEndSql);
											if (pos)
											{
												// Find the sessionid
												char* pos1;
												char* pos2;
												pos1 = strchr(line, '|');
												if (!pos1)
												{
													continue;
												}
												pos2 = strchr(pos1+1, '|');
												if (!pos2)
												{
													continue;
												}

												currentSessionID = strtoll(pos1+1, NULL, 0);

												// Check the map for this sessionid. If found, this is a completed SQL statement
												// remove it from our collection
												if ((pendingIter = pendingSQLStatements.find(currentSessionID)) != pendingSQLStatements.end())
												{
													pendingSQLStatements.erase(pendingIter);
												}
											}
										}
									}

									file.close();

									// Send the number of pending statements
									ackmsg << (ByteStream::byte) pendingSQLStatements.size();

									// Send the pending statements we discovered.
									for (pendingIter = pendingSQLStatements.begin(); 
										 pendingIter != pendingSQLStatements.end();
										 ++pendingIter)
									{
										ackmsg << (*pendingIter).second.sqlStatement;
										ackmsg << (unsigned)(*pendingIter).second.startTime;
										ackmsg << (*pendingIter).first;
									}
								}
								catch (...)
								{
								}

								fIos.write(ackmsg);
								ackmsg.reset();
							}
							break;

							case RUN_DBHEALTH_CHECK:
							{
								ByteStream::byte action;
								msg >> action;

								ByteStream ackmsg;

								ackmsg << (ByteStream::byte) RUN_DBHEALTH_CHECK;

								try {
									LoggingID lid(SERVER_MONITOR_LOG_ID);
									MessageLog ml(lid);
									Message msg;
									Message::Args args;
									args.add("RUN_DBHEALTH_CHECK called");
									msg.format(args);
									ml.logDebugMessage(msg);
								}
								catch (...)
								{}

								if ( DBhealthActive ) {
									try {
										LoggingID lid(SERVER_MONITOR_LOG_ID);
										MessageLog ml(lid);
										Message msg;
										Message::Args args;
										args.add("RUN_DBHEALTH_CHECK already Active, exiting");
										msg.format(args);
										ml.logDebugMessage(msg);
									}
									catch (...)
									{}

									ackmsg << (ByteStream::byte) oam::API_ALREADY_IN_PROGRESS;
									fIos.write(ackmsg);
									ackmsg.reset();

									break;
								}

								DBhealthActive = true;

								int ret = serverMonitor.healthCheck(action);

								if ( ret == API_SUCCESS ) {
									try {
										LoggingID lid(SERVER_MONITOR_LOG_ID);
										MessageLog ml(lid);
										Message msg;
										Message::Args args;
										args.add("RUN_DBHEALTH_CHECK passed");
										msg.format(args);
										ml.logDebugMessage(msg);
									}
									catch (...)
									{}

									ackmsg << (ByteStream::byte) oam::API_SUCCESS;
									fIos.write(ackmsg);
									ackmsg.reset();
								}
								else
								{
									try {
										LoggingID lid(SERVER_MONITOR_LOG_ID);
										MessageLog ml(lid);
										Message msg;
										Message::Args args;
										args.add("RUN_DBHEALTH_CHECK failed, check /tmp/dbhealthTest.log");
										msg.format(args);
										ml.logDebugMessage(msg);
									}
									catch (...)
									{}

									ackmsg << (ByteStream::byte) oam::API_FAILURE;
									fIos.write(ackmsg);
									ackmsg.reset();
								}

								DBhealthActive = false;
								break;
							}

							default:
								break;
		
						} // end of switch
					}

					try {
						fIos.close();
					}
					catch(...)
					{}
				}
				catch(...)
				{}
			} // end of for loop
		}
        catch (exception& ex)
        {
			string error = ex.what();
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("EXCEPTION ERROR on ServerMonitor: ");
			args.add(error);
			msg.format(args);
			ml.logErrorMessage(msg);
			// takes 2 - 4 minites to free sockets, sleep and retry
			sleep(10);
        }
        catch(...)
        {
			LoggingID lid(SERVER_MONITOR_LOG_ID);
			MessageLog ml(lid);
			Message msg;
			Message::Args args;
			args.add("EXCEPTION ERROR on ServerMonitor");
			msg.format(args);
			ml.logErrorMessage(msg);
			// takes 2 - 4 minites to free sockets, sleep and retry
			sleep(10);
        }
	}
}

