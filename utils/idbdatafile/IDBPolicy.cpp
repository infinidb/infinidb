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

#include <unistd.h>
#include <iostream>
#include <sstream>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/convenience.hpp>
#include <boost/algorithm/string.hpp>    // to_upper
#include <boost/thread/thread.hpp>

#include "configcpp.h"                   // for Config
#include "IDBPolicy.h"
#include "PosixFileSystem.h"
//#include "HdfsFileSystem.h"
//#include "HdfsFsCache.h"
#include "IDBLogger.h"
#include "IDBFactory.h"
#ifdef _MSC_VER
#include "utils_utf8.h"
#endif

using namespace std;

namespace idbdatafile
{

bool IDBPolicy::s_usehdfs = false;
bool IDBPolicy::s_bUseRdwrMemBuffer = false;
int64_t IDBPolicy::s_hdfsRdwrBufferMaxSize = 0; 
std::string IDBPolicy::s_hdfsRdwrScratch;
bool IDBPolicy::s_configed = false;
boost::mutex IDBPolicy::s_mutex;

void IDBPolicy::init( bool bEnableLogging, bool bUseRdwrMemBuffer, const string& hdfsRdwrScratch, int64_t hdfsRdwrBufferMaxSize )
{
	IDBFactory::installDefaultPlugins();

	IDBLogger::enable( bEnableLogging );

	s_bUseRdwrMemBuffer = bUseRdwrMemBuffer;
	s_hdfsRdwrBufferMaxSize = hdfsRdwrBufferMaxSize;
	s_hdfsRdwrScratch = hdfsRdwrScratch;

	// Create our scratch directory
	if( hdfsRdwrScratch.length() > 0 )
	{
		// TODO-check to make sure this directory has sufficient space, whatever that means.
		boost::filesystem::path tmpfilepath( hdfsRdwrScratch );
		if (boost::filesystem::exists(tmpfilepath))
		{
			if (!boost::filesystem::is_directory(tmpfilepath) && useHdfs())
			{
				// We found a file named what we want our scratch directory to be named.
				// Major issue, since we can't assume anything about where to put our tmp files
				ostringstream oss;
				oss << "IDBPolicy::init: scratch diretory setting " << hdfsRdwrScratch.c_str()
					<< " exists as a file. Can't create hdfs buffer files.";
				throw runtime_error(oss.str());
			}
		}
		else
		{
			if (!boost::filesystem::create_directory(tmpfilepath))
			{
				// We failed to create the scratch directory
				ostringstream oss;
				oss << "IDBPolicy::init: failed to create hdfs scratch directory "
					<< hdfsRdwrScratch.c_str() << ". Can't create hdfs buffer files.";
				throw runtime_error(oss.str());
			}
		}
	}
}

bool IDBPolicy::installPlugin(const std::string& plugin)
{
	bool ret = IDBFactory::installPlugin(plugin);
	// this is a cheesy way to do this, but it seems as good as anything for
	// now.  At some point, this policy class needs to be data driven - some
	// type of specification to drive the logic here.
	try
	{
		// see if there is an HDFS plugin
		IDBFactory::name(IDBDataFile::HDFS);
		s_usehdfs = true;
	}
	catch (std::exception& )
	{
		// nothing to do - this just means the plugin was not HDFS
		;
	}

	return ret;
}

bool IDBPolicy::isLocalFile( const std::string& path )
{
    boost::filesystem::path filepath( path );
#ifdef _MSC_VER
    size_t strmblen = funcexp::utf8::idb_wcstombs(0, filepath.extension().c_str(), 0) + 1;
	char* outbuf = (char*)alloca(strmblen * sizeof(char));
	strmblen = funcexp::utf8::idb_wcstombs(outbuf, filepath.extension().c_str(), strmblen);
    string fileExt(outbuf, strmblen);
#else
    string fileExt  = filepath.extension().c_str();
#endif
	bool isXml = (fileExt == ".xml");

	bool isVb = path.find("versionbuffer") != string::npos;
	bool isInDbroot = path.find("/Calpont/data") != string::npos;
	bool isScratch = path.find(s_hdfsRdwrScratch) == 0;

	return !isInDbroot || isXml || isVb || isScratch;
}

IDBDataFile::Types IDBPolicy::getType( const std::string& path, Contexts ctxt )
{
	bool isLocal = isLocalFile( path );

	if( ctxt == PRIMPROC )
	{
		if( isLocal || !useHdfs() )
			return IDBDataFile::UNBUFFERED;
		else
			return IDBDataFile::HDFS;
	}
	else
	{
		if( isLocal || !useHdfs() )
			return IDBDataFile::BUFFERED;
		else
			return IDBDataFile::HDFS;
	}
}

IDBFileSystem& IDBPolicy::getFs( const std::string& path )
{
	// for now context doesn't actually matter so just pass PRIMPROC
	// later the whole logic around file -> filesystem type mapping
	// needs to be data driven
	return IDBFactory::getFs( getType( path, PRIMPROC) );
}

// ported from we_config.cpp
void IDBPolicy::configIDBPolicy()
{
	// make sure this is done once.
	boost::mutex::scoped_lock lk(s_mutex);

	if (s_configed)
		return;

	config::Config* cf = config::Config::makeConfig();

	//--------------------------------------------------------------------------
	// IDBDataFile logging
	//--------------------------------------------------------------------------
	bool idblog = false;
	string idblogstr = cf->getConfig("SystemConfig", "DataFileLog");
	if ( idblogstr.length() != 0 )
	{
		boost::to_upper(idblogstr);
		idblog = ( idblogstr == "ON" );
	}

	//--------------------------------------------------------------------------
	// Optional File System Plugin - if a HDFS type plugin is loaded
	// then the system will use HDFS for all IDB data files
	//--------------------------------------------------------------------------
	string fsplugin = cf->getConfig("SystemConfig", "DataFilePlugin");
	if ( fsplugin.length() != 0 )
	{
		IDBPolicy::installPlugin(fsplugin);
	}

	//--------------------------------------------------------------------------
	// HDFS file buffering
	//--------------------------------------------------------------------------
	// Maximum amount of memory to use for hdfs buffering.
	bool bUseRdwrMemBuffer = true;  // If true, use in-memory buffering, else use file buffering
	int64_t hdfsRdwrBufferMaxSize = 0;
	string strBufferMaxSize = cf->getConfig("SystemConfig", "hdfsRdwrBufferMaxSize");
	// Default is use membuf with no maximum size unless changed by config file.
	if (strBufferMaxSize.length() > 0)
	{
		hdfsRdwrBufferMaxSize = static_cast<int64_t>(cf->uFromText(strBufferMaxSize));
		if ( hdfsRdwrBufferMaxSize == 0 )
		{
			// If we're given a size of 0, turn off membuffering.
			bUseRdwrMemBuffer = false;
		}
	}

	// Directory in which to place file buffer temporary files.
	string hdfsRdwrScratch = cf->getConfig("SystemConfig", "hdfsRdwrScratch");
	if ( hdfsRdwrScratch.length() == 0 )
	{
        string tmpPath = cf->getConfig("SystemConfig", "TempDiskPath");
        if ( tmpPath.length() == 0 )
        {
    		hdfsRdwrScratch = "/tmp/hdfsscratch";
        }
        else
        {
           	hdfsRdwrScratch = tmpPath;
            hdfsRdwrScratch += "/hdfsscratch";
        }

	}

	IDBPolicy::init( idblog, bUseRdwrMemBuffer, hdfsRdwrScratch, hdfsRdwrBufferMaxSize );

	s_configed = true;
}

}
