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
#if __FreeBSD__
#include <sys/socket.h>
#endif
#endif
using namespace std;

#include <boost/thread/mutex.hpp>
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

#include "atomicops.h"

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
        	case LOG_TYPE_ERROR:	ml.logErrorMessage(m); break;	
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
		boost::shared_ptr<MessageQueueClient> cl, uint32_t connectionIndex) : jbl(jl), client(cl),
		connIndex(connectionIndex) {}
    WriteEngine::WEClients *jbl;
    boost::shared_ptr<MessageQueueClient> client;
	uint32_t connIndex;
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

inline const string sin_addr2String(const in_addr src)
{
	string s;
#ifdef _MSC_VER
	s = inet_ntoa(src);
#else
	char dst[INET_ADDRSTRLEN];
	s = inet_ntop(AF_INET, &src, dst, INET_ADDRSTRLEN);
#endif
	return s;
}
}

namespace WriteEngine {
  WEClients::WEClients(int PrgmID) :
	fPrgmID(PrgmID),
	pmCount(0)
  {
	closingConnection = 0;
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

	uint32_t pmCountConfig =  moduletypeconfig.ModuleCount;
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
		//Bug 5224. Take out the retrys. If connection fails, we assume the server is down.
        try {
            if (cl->connect()) {
				try {
					cl->write(bs);
				}
				catch (std::exception& ex1)
				{
					ostringstream oss;
					oss << "Write to WES during connect failed due to " << ex1.what();
					 throw runtime_error(oss.str());
				}
				try
				{
					bsRead = cl->read();
					if (bsRead.length() == 0)
						throw runtime_error("Got byte 0 during reading " );
				}
				catch (std::exception& ex2) {
					ostringstream oss;
					oss << "Read from WES during connect failed due to " << ex2.what() << " and this = " << this;
					throw runtime_error(oss.str());
				}
                fPmConnections[moduleID] = cl;
				//cout << "connection is open. this = " << this << endl;
				//cout << "set up connection to mudule " << moduleID << endl;
                // assign the module name
                //ipAddress = sin_addr2String(cl->serv_addr().sin_addr);
                ipAddress = cl->addr2String();
				cl->moduleName(getModuleNameByIPAddr(moduletypeconfig, ipAddress));
                StartClientListener(cl, i);
				pmCount++;
				//ostringstream oss;
				//oss << "WECLIENT: connected to " << fServer + " and this = " << this << " and pmcount is now " << pmCount;
				//writeToLog(__FILE__, __LINE__, oss.str() , LOG_TYPE_DEBUG);
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
	closingConnection = 1;
	ByteStream bs;
	bs << (ByteStream::byte) WE_SVR_CLOSE_CONNECTION;
	write_to_all(bs);
//cout << "connection is closed. this = " << this << " and closingConnection = " << closingConnection << endl;
	for (uint32_t i=0; i < fWESReader.size(); i++)
	{
		fWESReader[i]->join();
	}
	fWESReader.clear();
    fPmConnections.clear();
	pmCount = 0;
	//ostringstream oss;
	//oss << "WECLIENT: closed connection to wes and this = " << this << " and pmcount is now " << pmCount;
	//writeToLog(__FILE__, __LINE__, oss.str() , LOG_TYPE_DEBUG);
    return 0;
}

void WEClients::Listen ( boost::shared_ptr<MessageQueueClient> client, uint32_t connIndex)
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
				if (closingConnection > 0)
				{
					return;
				}
				cerr << "WEC got 0 byte message for object " << this << endl;
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
		(void)atomicops::atomicInc(&map_tok->second->unackedWork[0]);
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
				ostringstream oss;
				//oss << "WECLIENT: connection to is reset and this = " << this << " and pmcount is decremented.";
				//writeToLog(__FILE__, __LINE__, oss.str() , LOG_TYPE_DEBUG);
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
      //cout << " reading for key " << key << " not found" << endl;
      os << "WEClient: attempt to read(bs) from a nonexistent queue\n";
      throw runtime_error(os.str());
    }
	
    mqe = map_tok->second;
 	lk.unlock();

    //this method can block: you can't hold any locks here...
    (void)mqe->queue.pop(&bs);
	
	if (!bs)
		bs.reset(new ByteStream());
}

void WEClients::write(const messageqcpp::ByteStream &msg, uint32_t connection)
{
	if (pmCount == 0)
	{
		ostringstream oss;
		oss << "WECLIENT: There is no connection to WES and this = " << this ;
		writeToLog(__FILE__, __LINE__, oss.str() , LOG_TYPE_DEBUG);
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
		ostringstream oss;
		oss << "WECLIENT:  There is no connection to WES and this = " << this ;
		writeToLog(__FILE__, __LINE__, oss.str() , LOG_TYPE_DEBUG);
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

void WEClients::StartClientListener(boost::shared_ptr<MessageQueueClient> cl, uint32_t connIndex)
{
    boost::thread *thrd = new boost::thread(WEClientRunner(this, cl, connIndex));
    fWESReader.push_back(thrd);
}


void WEClients::addDataToOutput(SBS sbs, uint32_t connIndex)
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
		atomicops::atomicInc(&mqe->unackedWork[connIndex % pmCount]);
	}

	(void)mqe->queue.push(sbs);

} 
  
}

// vim:ts=4 sw=4:
