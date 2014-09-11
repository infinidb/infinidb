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

/******************************************************************************************
******************************************************************************************/
#include <string>
#include <unistd.h>
#include <signal.h>
#include <clocale>
using namespace std;

#include "ddlproc.h"
#include "ddlprocessor.h"
#include "messageobj.h"
#include "messagelog.h"
#include "configcpp.h"
using namespace logging;

using namespace config;

#include "liboamcpp.h"
using namespace oam;

#include "distributedenginecomm.h"
using namespace joblist;

#include "boost/filesystem/operations.hpp"
#include "boost/filesystem/path.hpp"
#include "boost/progress.hpp"
#include <boost/scoped_ptr.hpp>
#include <boost/scoped_array.hpp>
#include <boost/thread.hpp>

using namespace boost;

#include "calpontsystemcatalog.h"
using namespace execplan;

#include "writeengine.h"
#include "cacheutils.h"
namespace fs = boost::filesystem;
namespace
{
	DistributedEngineComm *Dec;

    void setupCwd()
    {
        string workdir = config::Config::makeConfig()->getConfig("SystemConfig", "WorkingDir");
        if (workdir.length() == 0)
            workdir = ".";
        (void)chdir(workdir.c_str());
        if (access(".", W_OK) != 0)
            (void)chdir("/tmp");
    }

	void added_a_pm(int)
	{
		Dec->Setup();
	}
}

int main(int argc, char* argv[])
{
    // get and set locale language
	string systemLang = "C";

    Oam oam;
    try{
        oam.getSystemConfig("SystemLang", systemLang);
    }
    catch(...)
    {
		systemLang = "C";
	}

    setlocale(LC_ALL, systemLang.c_str());
    printf ("Locale is : %s\n", systemLang.c_str() );

	//BUG 2991
	setlocale(LC_NUMERIC, "C");

    setupCwd();

    WriteEngine::WriteEngineWrapper::init( WriteEngine::SUBSYSTEM_ID_DDLPROC );

	ResourceManager rm;
	Dec = DistributedEngineComm::instance(rm);

#ifndef _MSC_VER
	/* set up some signal handlers */
    struct sigaction ign;
    memset(&ign, 0, sizeof(ign));
    ign.sa_handler = added_a_pm;
	sigaction(SIGHUP, &ign, 0);
	ign.sa_handler = SIG_IGN;
	sigaction(SIGPIPE, &ign, 0);
#endif

    ddlprocessor::DDLProcessor ddlprocessor(5, 10);

    {
        Oam oam;
        try
        {
            oam.processInitComplete("DDLProc");
        }
        catch (...)
        {
        }
    }

    try
    {
        ddlprocessor.process();
    }
    catch (std::exception& ex)
    {
        cerr << ex.what() << endl;
        Message::Args args;
        Message message(8);
        args.add("DDLProc failed on: ");
        args.add(ex.what());
        message.format( args );

    }
    catch (...)
    {
        cerr << "Caught unknown exception!" << endl;
        Message::Args args;
        Message message(8);
        args.add("DDLProc failed on: ");
        args.add("receiving DDLPackage");
        message.format( args );
    }
	
    return 0;
}
// vim:ts=4 sw=4:

