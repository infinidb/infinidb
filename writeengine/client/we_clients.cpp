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

#include <sstream>
#include <stdexcept>
#include <cassert>
#include <ctime>
#include <algorithm>
#include <unistd.h>
#ifndef _MSC_VER
#include <arpa/inet.h>
#else
#include <intrin.h>
#endif
#if __FreeBSD__
#include <sys/socket.h>
#endif
using namespace std;

#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "messagequeue.h"
#include "bytestream.h"
using namespace messageqcpp;

#include "configcpp.h"
using namespace config;

#include "errorids.h"
#include "exceptclasses.h"
#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "liboamcpp.h"
#include "snmpmanager.h"
using namespace snmpmanager;
using namespace oam;

#include "we_clients.h"
#include "we_messages.h"
using namespace WriteEngine;

namespace {
 void  writeToLog(const char* file, int line, const string& msg, LOG_TYPE logto = LOG_TYPE_INFO)
  {
        LoggingID lid(05);
        MessageLog ml(lid);
        Message::Args args;
        Message m(0);
        args.add(file); 
        args.add("@");
        args.add(line);
        args.add(msg);
        m.format(args); 
	switch (logto)
	{
        	case LOG_TYPE_DEBUG:	ml.logDebugMessage(m); break;		
        	case LOG_TYPE_INFO: 	ml.logInfoMessage(m); break;	
        	case LOG_TYPE_WARNING:	ml.logWarningMessage(m); break;	
        	case LOG_TYPE_ERROR:	ml.logWarningMessage(m); break;	
        	case LOG_TYPE_CRITICAL:	ml.logCriticalMessage(m); break;	
	}
  }
  
 string getModuleNameByIPAddr(oam::ModuleTypeConfig moduletypeconfig, 
				 string ipAddress)
{
  	string modulename = "pm1";
  	DeviceNetworkList::iterator pt = moduletypeconfig.ModuleNetworkList.begin();
		for( ; pt != moduletypeconfig.ModuleNetworkList.end() ; pt++)
		{
			modulename = (*pt).DeviceName;
			HostConfigList::iterator pt1 = (*pt).hostConfigList.begin();
			for( ; pt1 != (*pt).hostConfigList.end() ; pt1++)
			{
				if (ipAddress == (*pt1).IPAddr)
					return modulename;
			}
		}
		return modulename;
}
  
struct WEClientRunner
{
    WEClientRunner(WriteEngine::WEClients *jl,
		boost::shared_ptr<MessageQueueClient> cl, uint connectionIndex) : jbl(jl), client(cl),
		connIndex(connectionIndex) {}
    WriteEngine::WEClients *jbl;
    boost::shared_ptr<MessageQueueClient> client;
	uint connIndex;
    void operator()()
    {
      //cout << "Listening on client at 0x" << hex << (ptrdiff_t)client << dec << endl;
      try
      {
        jbl->Listen(client, connIndex);
		//cout << "Listening connIndex " << connIndex << endl;
      }
      catch(std::exception& ex)
      {
        string what(ex.what());
        cerr << "exception caught in WEClient: " << what << endl;
        if (what.find("St9bad_alloc") != string::npos)
        {
	  writeToLog(__FILE__, __LINE__, what, LOG_TYPE_CRITICAL);
//           abort();
        }
	else  writeToLog(__FILE__, __LINE__, what);
      }
      catch(...)
      {
	string msg("exception caught in WEClientRunner.");
	writeToLog(__FILE__, __LINE__, msg);
        cerr << msg << endl;
      }
    }
};
  
template <typename T>
struct QueueShutdown : public unary_function<T&, void>
{
	void operator()(T& x)
	{
		x.shutdown();
	}
};

#ifdef _MSC_VER
mutex inet_ntoa_mutex;
#endif

inline const string sin_addr2String(const in_addr src)
{
	string s;
#ifdef _MSC_VER
	mutex::scoped_lock lk(inet_ntoa_mutex);
	s = inet_ntoa(src);
#else
	char dst[INET_ADDRSTRLEN];
	s = inet_ntop(AF_INET, &src, dst, INET_ADDRSTRLEN);
#endif
	return s;
}
}

namespace WriteEngine {
	boost::mutex WEClients::map_mutex;
	WEClients* WEClients::fInstance = 0;
  
  /*static*/
  WEClients* WEClients::instance(int PrgmID)
  {
    boost::mutex::scoped_lock lock(map_mutex);
    if (fInstance == 0)
        fInstance = new WEClients(PrgmID);

    return fInstance;
  }

  WEClients::WEClients(int PrgmID) :
	fPrgmID(PrgmID),
	pmCount(0)
  {
    Setup();
  }

  WEClients::~WEClients()
  {
    Close();
  }

void WEClients::Setup()
{
    makeBusy(true);
    joblist::ResourceManager rm;
    oam::Oam oam;
    string ipAddress;
    ModuleTypeConfig moduletypeconfig; 
	try {
    	oam.getSystemConfig("pm", moduletypeconfig);
	} catch (...) {
		writeToLog(__FILE__, __LINE__, "oam.getSystemConfig error, unknown exception", LOG_TYPE_ERROR);
		throw runtime_error("Setup failed");
	}

	uint pmCountConfig =  moduletypeconfig.ModuleCount;
	pmCount = 0;
	int moduleID = 1;
	
    char buff[32];
	ByteStream bs, bsRead;
	if (fPrgmID == DDLPROC)
	{
		bs << (ByteStream::byte) WE_SVR_DDL_KEEPALIVE;
		bs << (ByteStream::octbyte) moduleID;
	}
	else if (fPrgmID == DMLPROC)
	{
		bs << (ByteStream::byte) WE_SVR_DML_KEEPALIVE;
		bs << (ByteStream::octbyte) moduleID;
	}
	else if (fPrgmID == SPLITTER)
	{
		bs << (ByteStream::byte) WE_CLT_SRV_KEEPALIVE;
	}
	else if (fPrgmID == BATCHINSERTPROC)
	{
		bs << (ByteStream::byte) WE_SVR_BATCH_KEEPALIVE;
		bs << (ByteStream::octbyte) moduleID;
	}
    for (unsigned i = 0; i < pmCountConfig; i++) {
		//Find the module id
		moduleID = atoi((moduletypeconfig.ModuleNetworkList[i]).DeviceName.substr(MAX_MODULE_TYPE_SIZE,MAX_MODULE_ID_SIZE).c_str());
		//cout << "setting connection to moduleid " << moduleID << endl;
        snprintf(buff, sizeof(buff), "pm%u_WriteEngineServer", moduleID);
        string fServer (buff);

        boost::shared_ptr<MessageQueueClient>
			cl(new MessageQueueClient(fServer, rm.getConfig()));
        boost::shared_ptr<boost::mutex> nl(new boost::mutex());
        try {
            if (cl->connect()) {
				cl->write(bs);
				bsRead = cl->read();
                fPmConnections[moduleID] = cl;
				//cout << "connection is open. this = " << this << endl;
				//cout << "set up connection to mudule " << moduleID << endl;
                // assign the module name
                //ipAddress = sin_addr2String(cl->serv_addr().sin_addr);
                ipAddress = cl->addr2String();
				cl->moduleName(getModuleNameByIPAddr(moduletypeconfig, ipAddress));
                StartClientListener(cl, i);
				pmCount++;
            } else {
                throw runtime_error("Connection refused");
            }
        } catch (std::exception& ex) {
            writeToLog(__FILE__, __LINE__, "Could not connect to " + fServer + ": " + ex.what(), LOG_TYPE_ERROR);
            cerr << "Could not connect to " << fServer << ": " << ex.what() << endl;
        } catch (...) {
            writeToLog(__FILE__, __LINE__, "Could not connect to " + fServer, LOG_TYPE_ERROR);
        }
    }
	
}

int WEClients::Close()
{
    makeBusy(false);
	cout << "connection is closed. this = " << this << endl;
    // for each MessageQueueClient in pmConnections delete the MessageQueueClient;
    fPmConnections.clear();
    fWESReader.clear();
    return 0;
}

void WEClients::Listen ( boost::shared_ptr<MessageQueueClient> client, uint connIndex)
{
	SBS sbs;

	try {
		while ( Busy() )
		{
			//TODO: This call blocks so setting Busy() in another thread doesn't work here...
			sbs = client->read();
			if ( sbs->length() != 0 )
			{
				//cout << "adding data to connIndex " << endl;
				addDataToOutput(sbs, connIndex);
			}
			else // got zero bytes on read, nothing more will come
			{
				cerr << "WEC got 0 byte message" << endl;
				goto Error;
			}
		}
		return;
	} catch (std::exception& e)
	{
		cerr << "WEC Caught EXCEPTION: " << e.what() << endl;
		goto Error;
	}
	catch (...)
	{
		cerr << "WEC Caught UNKNOWN EXCEPT" << endl;
		goto Error;
	}
Error:
	// error condition! push 0 length bs to messagequeuemap and
	// eventually let jobstep error out.
	mutex::scoped_lock lk(fMlock);

	MessageQueueMap::iterator map_tok;
	sbs.reset(new ByteStream(0));

	for (map_tok = fSessionMessages.begin(); map_tok != fSessionMessages.end(); ++map_tok)
	{
		map_tok->second->queue.clear();
#ifdef _MSC_VER
		InterlockedIncrement(&map_tok->second->unackedWork[0]);
#else
		__sync_add_and_fetch(&map_tok->second->unackedWork[0], 1);  // prevent an error msg
#endif
		map_tok->second->queue.push(sbs);
	}
	lk.unlock();

	// reset the pmconnection map
	{
		mutex::scoped_lock onErrLock(fOnErrMutex);
		string moduleName = client->moduleName();
		ClientList::iterator itor = fPmConnections.begin();
		while (itor != fPmConnections.end())
		{
			if (moduleName == (itor->second)->moduleName())
			{
				(fPmConnections[itor->first]).reset();
				pmCount--;
			}
				
			itor++;
		}
		// send alarm
		SNMPManager alarmMgr;
//		string alarmItem = sin_addr2String(client->serv_addr().sin_addr);
		string alarmItem = client->addr2String();
		alarmItem.append(" WriteEngineServer");
		alarmMgr.sendAlarmReport(alarmItem.c_str(), oam::CONN_FAILURE, SET);
	}
	return;
}

void WEClients::addQueue(uint32_t key)
{
	bool b;

	mutex* lock = new mutex();
	condition* cond = new condition();
	boost::shared_ptr<MQE> mqe(new MQE(pmCount));
	
	mqe->queue = WESMsgQueue(lock, cond);
	
	mutex::scoped_lock lk ( fMlock );
	b = fSessionMessages.insert(pair<uint32_t, boost::shared_ptr<MQE> >(key, mqe)).second;
	if (!b) {
		ostringstream os;
		os << "WEClient: attempt to add a queue with a duplicate ID " << key << endl;
		throw runtime_error(os.str());
	}
}

void WEClients::removeQueue(uint32_t key)
{
	mutex::scoped_lock lk(fMlock);
	MessageQueueMap::iterator map_tok = fSessionMessages.find(key);
	if (map_tok == fSessionMessages.end())
		return;
	map_tok->second->queue.shutdown();
	map_tok->second->queue.clear();
	fSessionMessages.erase(map_tok);
}

void WEClients::shutdownQueue(uint32_t key)
{
	  mutex::scoped_lock lk(fMlock);
	  MessageQueueMap::iterator map_tok = fSessionMessages.find(key);
	  if (map_tok == fSessionMessages.end())
		  return;
	  map_tok->second->queue.shutdown();
	  map_tok->second->queue.clear();
}

void WEClients::read(uint32_t key, SBS &bs)
{
	boost::shared_ptr<MQE> mqe;
	
	//Find the StepMsgQueueList for this session
    mutex::scoped_lock lk(fMlock);
    MessageQueueMap::iterator map_tok = fSessionMessages.find(key);
    if(map_tok == fSessionMessages.end())
    {
      ostringstream os;
      cout << " reading for key " << key << " not found" << endl;
      os << "WEClient: attempt to read(bs) from a nonexistent queue\n";
      throw runtime_error(os.str());
    }
	
    mqe = map_tok->second;
 	lk.unlock();

    //this method can block: you can't hold any locks here...
    joblist::TSQSize_t queueSize = mqe->queue.pop(&bs);
	
	if (!bs)
		bs.reset(new ByteStream());
}

void WEClients::write(const messageqcpp::ByteStream &msg, uint connection)
{
	if (pmCount == 0)
	{
		throw runtime_error("There is no WriteEngineServer to send message to.");
	}
	if (fPmConnections[connection] != 0)
		fPmConnections[connection]->write(msg);
	else {
		ostringstream os;
		os << "Lost connection to WriteEngineServer on pm" << connection;
		throw runtime_error(os.str());
	}
}

void WEClients::write_to_all(const messageqcpp::ByteStream &msg)
{
	if (pmCount == 0)
	{
		throw runtime_error("There is no WriteEngineServer to send message to.");
	}

	ClientList::iterator itor = fPmConnections.begin();
	while (itor != fPmConnections.end())
	{
		if (itor->second != NULL)
		{
			itor->second->write(msg);
		}
		itor++;
	}
}

void WEClients::StartClientListener(boost::shared_ptr<MessageQueueClient> cl, uint connIndex)
{
    boost::thread *thrd = new boost::thread(WEClientRunner(this, cl, connIndex));
    fWESReader.push_back(thrd);
}

void WEClients::addDataToOutput(SBS sbs, uint connIndex)
  {
   // ISMPacketHeader *hdr = (ISMPacketHeader*)(sbs->buf());
   // PrimitiveHeader *p = (PrimitiveHeader *)(hdr+1);
	//uint32_t uniqueId = p->UniqueID;
	uint64_t uniqueId = 0;
	*sbs >> uniqueId;
	boost::shared_ptr<MQE> mqe;
 
    mutex::scoped_lock lk(fMlock);
    MessageQueueMap::iterator map_tok = fSessionMessages.find(uniqueId);
    if(map_tok == fSessionMessages.end())
    {
        return;
    }
 	mqe = map_tok->second;
	lk.unlock();
    
	if (pmCount > 0) {
#ifdef _MSC_VER
		InterlockedIncrement(&mqe->unackedWork[connIndex % pmCount]);
#else
		__sync_add_and_fetch(&mqe->unackedWork[connIndex % pmCount], 1);
#endif
	}
	joblist::TSQSize_t queueSize = mqe->queue.push(sbs);
	
} 
  
}

// vim:ts=4 sw=4:
