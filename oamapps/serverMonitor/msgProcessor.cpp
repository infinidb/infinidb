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
									ackmsg << (ByteStream::quadbyte) ((*p).totalBlocks / 1024) ;
									ackmsg << (ByteStream::quadbyte) ((*p).usedBlocks / 1024);
									ackmsg << (ByteStream::byte) (*p).usedPercent;
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

								ByteStream ackmsg;

								string fileName = "/var/log/Calpont/debug.log";
							
								ifstream file (fileName.c_str());
								if (!file) {
									ackmsg << (ByteStream::byte) API_FILE_OPEN_ERROR;
									fIos.write(ackmsg);
									break;
								}

								vector <string> lines;
								char line[15100];
								string buf;
								string startSql = "Start SQL statement";
								string endSql = "End SQL statement";
								vector <string> sessionID;
								vector <string> sqlStatement;
								vector <string> startTime;
								vector <string> sendSessionID;
								string currentSessionID;

								while (file.getline(line, 15100, '\n'))
								{
									buf = line;
									lines.push_back(buf);
								}

								file.close();

								vector <string>::reverse_iterator aPtr = lines.rbegin();
								for (; aPtr != lines.rend() ; aPtr++)
								{
									string::size_type pos = (*aPtr).find(endSql,0);
									if (pos != string::npos)
									{
										string::size_type pos1 = (*aPtr).find("|",0);
										if (pos1 != string::npos)
										{
											string::size_type pos2 = (*aPtr).find("|",pos1+1);
											if (pos2 != string::npos)
											{
												sessionID.push_back((*aPtr).substr(pos1+1, pos2-pos1-1));
											}
										}
									}
									else
									{
										string::size_type pos = (*aPtr).find(startSql,0);
										if (pos != string::npos)
										{
											string::size_type pos1 = (*aPtr).find("|",0);
											if (pos1 != string::npos)
											{
												string::size_type pos2 = (*aPtr).find("|",pos1+1);
												if (pos2 != string::npos)
												{
													currentSessionID = (*aPtr).substr(pos1+1, pos2-pos1-1);

													bool FOUND=false;
													vector <string>::iterator bPtr = sessionID.begin();

													for (; bPtr != sessionID.end() ; bPtr++)
													{
														if ( *bPtr == currentSessionID )
														{
															sessionID.erase(bPtr);
															FOUND=true;
															break;
														}
													}

													if(!FOUND) {
														//filter out System Catalog inqueries
														string SQLStatement = (*aPtr).substr(pos+21, 15000);
														string::size_type pos3 = SQLStatement.find("/FE",SQLStatement.size()-5);
														string::size_type pos4 = SQLStatement.find("/EC",SQLStatement.size()-5);

														if ( pos3 == string::npos && pos4 == string::npos ) {
															sqlStatement.push_back(SQLStatement);
															startTime.push_back((*aPtr).substr(0, 16));
															sendSessionID.push_back(currentSessionID);
														}
													}
												}
											}
										}
									}
								}

								//get local ExeMgr start time and filter out queries
								SystemStatus systemstatus;
								SystemModuleTypeConfig systemmoduletypeconfig;
								ModuleTypeConfig moduletypeconfig;
								struct tm tm;
								time_t moduleStartTime = 0;

								try {
									Oam oam;
									ProcessStatus procstat;
									oam.getProcessStatus("ExeMgr", moduleName, procstat);

									if( strptime((procstat.StateChangeDate).c_str(), "%a %b %d %H:%M:%S %Y", &tm) != NULL) {
										tm.tm_isdst = -1;
										moduleStartTime = mktime(&tm);
									}

									vector <string>::iterator bPtr = sqlStatement.begin();
									vector <string>::iterator cPtr = startTime.begin();
									vector <string>::iterator dPtr = sendSessionID.begin();
	
									ackmsg << (ByteStream::byte) sqlStatement.size();
	
									for (; bPtr != sqlStatement.end(); ++bPtr,++cPtr,++dPtr)
									{
										//filter out queries that started before a UM start time
										time_t queryStartTime;
										if( strptime((*cPtr).c_str(), "%b %d %H:%M:%S", &tm) != NULL) {
											tm.tm_isdst = -1;
											time_t rawtime;
											struct tm timeinfo;
											time ( &rawtime );
											localtime_r ( &rawtime, &timeinfo );
											tm.tm_year = timeinfo.tm_year;
											queryStartTime = mktime(&tm);
	
											if ( queryStartTime < moduleStartTime ) {
												//send dummy info, need to match count already sent
												ackmsg << "-1";
												ackmsg << "-1";
												ackmsg << "-1";
												continue;
											}
										}
	
										//send query and start time and sessions ID
										ackmsg << *bPtr;
										ackmsg << *cPtr;
										ackmsg << *dPtr;
									}

								}
								catch(...)
								{}


								fIos.write(ackmsg);
								ackmsg.reset();

								break;
							}

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

