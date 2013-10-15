/* Copyright (C) 2013 Calpont Corp.

   This library is free software; you can redistribute it and/or
   modify it under the terms of the GNU Lesser General Public
   License as published by the Free Software Foundation;
   version 2.1 of the License.

   This library is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program; if not, write to the Free Software
   Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
   MA 02110-1301, USA. */

 /*********************************************************************
 * $Id: clientrotator.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 *
 ***********************************************************************/

#include <iostream>
#include <iomanip>
#include <sstream>
#include <cstring>
#include <cassert>
#include <stdexcept>
using namespace std;

#include <boost/timer.hpp>
#include <boost/thread.hpp>
using namespace boost;

#include "configcpp.h"
using namespace config;

#include "messagequeue.h"
using namespace messageqcpp;

#include "messagelog.h"
#include "messageobj.h"
#include "loggingid.h"
using namespace logging;

#include "clientrotator.h"

#define LOG_TO_CERR

namespace execplan
{
  ostream& operator<<(ostream& output, const ClientRotator& rhs)
  {
    output << __FILE__ << rhs.fName << "\t" << rhs.fSessionId << endl;
    return output;
  }

  ClientRotator::ClientRotator(u_int32_t sid, const std::string& name) : 
    fName(name), fSessionId(sid), fClient(0), fClients(), fCf(Config::makeConfig()), fDebug(0)
  { 
      if (! fCf) 
	    throw runtime_error((string)__FILE__ + ": No configuration file"); 
      fDebug = static_cast<int>(Config::fromText(fCf->getConfig("CalpontConnector", "DebugLevel")));
  }


  void ClientRotator::loadClients()
  {
    //This object is statically allocated somewhere. We need to reload the config file here
    // to search the extproc env for changes made after libcalora.so is loaded.
    fCf = Config::makeConfig();

    stringstream ss(fName);
    size_t pos = fName.length();
    string str;
    int i = 1;
    do
    {
	ss.seekp(pos);
	ss << i++;
	str = fCf->getConfig(ss.str(), "Port");
	if (str.length() )  fClients.push_back(ss.str());	
    } while ( str.length() );
    if (fClients.empty())
      throw runtime_error((string)__FILE__ + ": No configuration tags for " + fName + "\n");

  }
  
	void ClientRotator::resetClient()
	{
		try //one more time...
		{
			delete fClient;
			fClient = 0;
			connectList(fSessionId % fClients.size());
			//fClient->write(msg);
		}
		catch (std::exception &e)
		{
			/* Can't fail silently */
			writeToLog(__LINE__, e.what(), true);
#ifdef LOG_TO_CERR
			cerr << "ClientRotator::write() failed: " << e.what() << endl;
#endif
			throw;
		}
	}

	void ClientRotator::write(const ByteStream& msg)
	{
		if (!fClient)
			connect();

		try
		{
			fClient->write(msg);
			return;
		}
		catch (std::exception& e)
		{
			resetClient();
			string errmsg = "ClientRotator caught exception: " + string(e.what());
			cout << errmsg << endl;
			throw runtime_error(errmsg);
		}
		catch (...)
		{
			resetClient();
			string errmsg = "ClientRotator caught unknown exception";
			cout << errmsg << endl;
			throw runtime_error(errmsg);
		}
	}

	ByteStream ClientRotator::read()
	{
		mutex::scoped_lock lk(fClientLock);
	
		ByteStream bs;
	
		if (!fClient)
			connect();
	
		try
		{
			bs = fClient->read();
			return bs;
		}
		catch (std::exception& e)
		{
			resetClient();
			string errmsg = "ClientRotator caught exception: " + string(e.what());
			cout << errmsg << endl;
			throw runtime_error(errmsg);
		}
		catch (...)
		{
			resetClient();
			string errmsg = "ClientRotator caught unknown exception";
			cout << errmsg << endl;
			throw runtime_error(errmsg);
		}

#if 0
    try //one more time...
    {
	delete fClient;
	fClient = 0;
	connectList(fSessionId % fClients.size());
	bs = fClient->read();
	return bs;
    }
    catch (std::exception &e)
    {
		/* Can't fail silently */
		writeToLog(__LINE__, e.what(), true);
#ifdef LOG_TO_CERR
		cerr << "ClientRotator::read() failed: " << e.what() << endl;
#endif
		throw;
    }
#endif
    return bs;
  }


  void ClientRotator::connect(double timeout)
  {

    if (fClients.empty())
      loadClients();

    if (fClient) 
    {
		return;
    }

    size_t idx = fSessionId % fClients.size();
    bool connected = false;
    
    try 
    {
		connected = exeConnect(fClients.at(idx));
    }
    catch (... ) 
    {
	}

    if (!connected)
		connectList(idx, timeout);

  }

  bool ClientRotator::exeConnect( const string& clientName )
  {
    fClient = new messageqcpp::MessageQueueClient( clientName, fCf);
     if (fDebug > 12)
    {
        stringstream ss;
	ss << fSessionId;
#ifdef LOG_TO_CERR
		cerr << "Connecting to " << clientName << " with sessionId " << ss.str() << endl;
#endif
    	writeToLog( __LINE__, "Connecting to  " + clientName + " with sessionId " + ss.str(), 0);
    }

	try {
	    if (!fClient->connect())
		{
			delete fClient;
			fClient = 0;
			return false;
		}
	}
	catch(...) {
		delete fClient;
		fClient = 0;
		return false;
	}
	return true;
  }


  void ClientRotator::connectList(size_t idx, double timeout)
  {
    if (fClient) return;

    if (fClients.empty())
      loadClients();

    if (++idx >= fClients.size() ) 
	idx = 0;
    timer runTime;
    while ( runTime.elapsed() < timeout)
    {
	try 
	{
	    if (exeConnect(fClients.at(idx++)))
	      return;
	    if (fClients.size() == idx)
	      idx = 0;
	}
	catch (... ) 
	{	    }	    
    }
#ifdef LOG_TO_CERR
	cerr << "Could not get a " << fName << " connection.\n";
#endif
    writeToLog(__LINE__, "Could not get a " + fName + " connection.", 1);
    throw runtime_error((string)__FILE__ + ": Could not get a connection to a " + fName);
  }

  void  ClientRotator::writeToLog(int line, const string& msg, bool critical) const
  {
        LoggingID lid(05);
        MessageLog ml(lid);
        Message::Args args;
        Message m(0);
	args.add(__FILE__); 
	args.add("@");
	args.add(line);
        args.add(msg);
	m.format(args); 
	if (critical)
	  ml.logCriticalMessage(m);
	else if (fDebug)
	  ml.logDebugMessage(m);
  }


}
// vim:ts=4 sw=4:

