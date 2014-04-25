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
* $Id: configcpp.cpp 3899 2013-06-17 20:54:10Z rdempsey $
*
******************************************************************************************/
#include "config.h"

#include <string>
#include <stdexcept>
#include <iostream>
#include <sstream>
#include <fstream>
#include <vector>
#include <string>
using namespace std;

#include <boost/thread.hpp>
#include <boost/filesystem.hpp>
using namespace boost;
namespace fs=boost::filesystem;

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <libxml/xmlmemory.h>
#include <libxml/parser.h>
#include <fcntl.h>
#include <errno.h>
#include <stdlib.h>
#ifdef HAVE_ALLOCA_H
#include <alloca.h>
#endif
#include <cstring>
//#define NDEBUG
#include <cassert>

#include "configcpp.h"

#include "exceptclasses.h"
#include "installdir.h"
#ifdef _MSC_VER
#include "idbregistry.h"
#endif

#include "bytestream.h"

namespace
{
const fs::path defaultCalpontConfigFile("Calpont.xml");
}

namespace config
{
Config::configMap_t Config::fInstanceMap;
boost::mutex Config::fInstanceMapMutex;

Config* Config::makeConfig(const string& cf)
{
	return makeConfig(cf.c_str());
}

Config* Config::makeConfig(const char* cf)
{
	mutex::scoped_lock lk(fInstanceMapMutex);

	string configFile;
	string installDir = startup::StartUp::installDir();

	if (cf == 0)
	{
#ifdef _MSC_VER
		string cfStr = IDBreadRegistry("ConfigFile");
		if (!cfStr.empty())
			cf = cfStr.c_str();
#else
		cf = getenv("CALPONT_CONFIG_FILE");
#endif
		if (cf == 0 || *cf == 0)
		{
			fs::path configFilePath = fs::path(installDir) / fs::path("etc") / defaultCalpontConfigFile;
			configFile = configFilePath.string();
		}
		else
		{
			configFile = cf;
		}
	}
	else
	{
		configFile = cf;
	}

	if (fInstanceMap.find(configFile) == fInstanceMap.end())
	{
		Config* instance = new Config(configFile, installDir);
		fInstanceMap[configFile] = instance;
	}

	return fInstanceMap[configFile];
}

Config::Config(const string& configFile, const string& installDir) :
	fDoc(0), fConfigFile(configFile), fMtime(0), fInstallDir(installDir), fParser(fInstallDir)
{
	for ( int i = 0 ; i < 20 ; i++ )
	{
		if (access(fConfigFile.c_str(), R_OK) != 0) {
			if ( i >= 15 )
				throw runtime_error("Config::Config: error accessing config file " + fConfigFile);
			sleep (1);
		}
		else
			break;
	}

	struct stat statbuf;
	if (stat(configFile.c_str(), &statbuf) == 0)
		fMtime = statbuf.st_mtime;

	parseDoc();
}

Config::~Config()
{
 	if (fDoc != 0)
 		closeConfig();
}

void Config::parseDoc(void)
{
	struct flock fl;
	int fd;

	memset(&fl, 0, sizeof(fl));
	fl.l_type   = F_RDLCK;  // read lock
	fl.l_whence = SEEK_SET;
	fl.l_start  = 0;
	fl.l_len    = 0;	//lock whole file

	// lock file if exist
	if ((fd = open(fConfigFile.c_str(), O_RDONLY)) >= 0)
	{
		if (fcntl(fd, F_SETLKW, &fl) != 0)
		{
			ostringstream oss;
			oss << "Config::parseDoc: error locking file " <<
				fConfigFile <<
				": " <<
				strerror(errno) <<
				", proceding anyway.";
			cerr << oss.str() << endl;
		}

		fDoc = xmlParseFile(fConfigFile.c_str());

		fl.l_type   = F_UNLCK;	//unlock
		fcntl(fd, F_SETLK, &fl);

		close(fd);
	}
	else
	{
		ostringstream oss;
		oss << "Config::parseDoc: error opening file " <<
			fConfigFile <<
			": " <<
			strerror(errno);
		throw runtime_error(oss.str());
	}


	if (fDoc == 0 ) {
		throw runtime_error("Config::parseDoc: error parsing config file " + fConfigFile);
	}

	xmlNodePtr cur = xmlDocGetRootElement(fDoc);

	if (cur == NULL)
	{
		xmlFreeDoc(fDoc);
		fDoc = 0;
		throw runtime_error("Config::parseDoc: error parsing config file " + fConfigFile);
	}

	if (xmlStrcmp(cur->name, (const xmlChar *)"Calpont"))
	{
		xmlFreeDoc(fDoc);
		fDoc = 0;
		throw runtime_error("Config::parseDoc: error parsing config file " + fConfigFile);
	}

	return;
}

void Config::closeConfig(void)
{
	xmlFreeDoc(fDoc);
	fDoc = 0;
}

const string Config::getConfig(const string& section, const string& name)
{
	mutex::scoped_lock lk(fLock);

	if (section.length() == 0 || name.length() == 0)
		throw invalid_argument("Config::getConfig: both section and name must have a length");

	if (fDoc == 0){
		throw runtime_error("Config::getConfig: no XML document!");
	}

	struct stat statbuf;
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
	{
		if (statbuf.st_mtime != fMtime)
		{
			closeConfig();
			fMtime = statbuf.st_mtime;
			parseDoc();
		}
	}

	return fParser.getConfig(fDoc, section, name);
}

void Config::getConfig(const string& section, const string& name, vector<string>& values)
{
	mutex::scoped_lock lk(fLock);

	if (section.length() == 0)
		throw invalid_argument("Config::getConfig: section must have a length");

	if (fDoc == 0)
		throw runtime_error("Config::getConfig: no XML document!");

	struct stat statbuf;
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
	{
		if (statbuf.st_mtime != fMtime)
		{
			closeConfig();
			fMtime = statbuf.st_mtime;
			parseDoc();
		}
	}

	fParser.getConfig(fDoc, section, name, values);
}

void Config::setConfig(const string& section, const string& name, const string& value)
{
	mutex::scoped_lock lk(fLock);

	if (section.length() == 0 || name.length() == 0 )
		throw invalid_argument("Config::setConfig: all of section and name must have a length");

	if (fDoc == 0) {
		throw runtime_error("Config::setConfig: no XML document!");
	}

	struct stat statbuf;
	memset(&statbuf, 0, sizeof(statbuf));
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
	{
		if (statbuf.st_mtime != fMtime)
		{
			closeConfig();
			fMtime = statbuf.st_mtime;
			parseDoc();
		}
	}

	fParser.setConfig(fDoc, section, name, value);
	return;
}

void Config::delConfig(const string& section, const string& name)
{
	mutex::scoped_lock lk(fLock);

	if (section.length() == 0 || name.length() == 0)
		throw invalid_argument("Config::delConfig: both section and name must have a length");

	if (fDoc == 0){
		throw runtime_error("Config::delConfig: no XML document!");
	}

	struct stat statbuf;
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
	{
		if (statbuf.st_mtime != fMtime)
		{
			closeConfig();
			fMtime = statbuf.st_mtime;
			parseDoc();
		}
	}

	fParser.delConfig(fDoc, section, name);
	return;
}

void Config::writeConfig(const string& configFile) const
{
	FILE *fi;
	if (fDoc == 0)
		throw runtime_error("Config::writeConfig: no XML document!");

#ifdef _MSC_VER
	fs::path configFilePth(configFile);
	fs::path outFilePth(configFilePth);
	outFilePth.replace_extension("temp");
	if ((fi = fopen(outFilePth.string().c_str(), "wt")) == NULL)
		throw runtime_error("Config::writeConfig: error opening config file for write " + outFilePth.string());
	int rc = -1;
	rc = xmlDocDump(fi, fDoc);
	if (rc < 0) {
		throw runtime_error("Config::writeConfig: error writing config file " + outFilePth.string());
	}
	fclose(fi);
	if (fs::exists(configFilePth))
		fs::remove(configFilePth);
	fs::rename(outFilePth, configFilePth);
#else

	const fs::path defaultCalpontConfigFileTemp("Calpont.xml.temp");
	const fs::path saveCalpontConfigFileTemp("Calpont.xml.calpontSave");
	const fs::path tmpCalpontConfigFileTemp("Calpont.xml.temp1");

	fs::path etcdir = fs::path(fInstallDir) / fs::path("etc");

	fs::path dcf = etcdir / fs::path(defaultCalpontConfigFile);
	fs::path dcft = etcdir / fs::path(defaultCalpontConfigFileTemp);
	fs::path scft = etcdir / fs::path(saveCalpontConfigFileTemp);
	fs::path tcft = etcdir / fs::path(tmpCalpontConfigFileTemp);
	//perform a temp write first if Calpont.xml file to prevent possible corruption
	if ( configFile == dcf ) {

		if (exists(dcft)) fs::remove(dcft);
		if ((fi = fopen(dcft.string().c_str(), "w+")) == NULL)
			throw runtime_error("Config::writeConfig: error writing config file " + configFile);

		int rc;
		rc = xmlDocDump(fi, fDoc);
		if ( rc < 0) {
			throw runtime_error("Config::writeConfig: error writing config file " + configFile);
			//cout << "xmlDocDump " << rc << " " << errno << endl;
		}

		fclose(fi);

		//check temp file
		try {
			Config* c1 = makeConfig(dcft.string().c_str());

			string value;
			value = c1->getConfig("SystemConfig", "SystemName");

			//good read, save copy, copy temp file tp tmp then to Calpont.xml
			//move to /tmp to get around a 'same file error' in mv command
			try {
				if (exists(scft)) fs::remove(scft);
			} catch (fs::filesystem_error&) { }
			fs::copy_file(dcf, scft, fs::copy_option::overwrite_if_exists);
			try {
				fs::permissions(scft, fs::add_perms | fs::owner_read | fs::owner_write |
													  fs::group_read | fs::group_write |
													  fs::others_read | fs::others_write);
			} catch (fs::filesystem_error&) { }

			if (exists(tcft)) fs::remove(tcft);
			fs::rename(dcft, tcft);

			if (exists(dcf)) fs::remove(dcf);
			fs::rename(tcft, dcf);
		}
		catch (...)
		{
			throw runtime_error("Config::writeConfig: error writing config file " + configFile);
		}
	}
	else
	{ // non Calpont.xml, perform update
		if ((fi = fopen(configFile.c_str(), "w")) == NULL)
			throw runtime_error("Config::writeConfig: error writing config file " + configFile);

		xmlDocDump(fi, fDoc);

		fclose(fi);
	}
#endif
	return;
}

void Config::write(void) const
{
#ifdef _MSC_VER
	writeConfig(fConfigFile);
#else
	write(fConfigFile);
#endif
}

void Config::write(const string& configFile) const
{
	struct flock fl;
	int fd;

	fl.l_type   = F_WRLCK;  // write lock
	fl.l_whence = SEEK_SET;
	fl.l_start  = 0;
	fl.l_len    = 0;
	fl.l_pid    = getpid();

	// lock file if it exists
	if ((fd = open(configFile.c_str(), O_WRONLY)) >= 0)
	{
		if (fcntl(fd, F_SETLKW, &fl) == -1)
			throw runtime_error("Config::write: file lock error " + configFile);

		writeConfig(configFile);

		fl.l_type   = F_UNLCK;	//unlock
		if (fcntl(fd, F_SETLK, &fl) == -1)
			throw runtime_error("Config::write: file unlock error " + configFile);

		close(fd);
	}
	else
	{
		writeConfig(configFile);
	}
}

void Config::writeConfigFile(messageqcpp::ByteStream msg) const
{
	struct flock fl;
	int fd;

	//get config file name being udated
	string fileName;
	msg >> fileName;

	fl.l_type   = F_WRLCK;  // write lock
	fl.l_whence = SEEK_SET;
	fl.l_start  = 0;
	fl.l_len    = 0;
	fl.l_pid    = getpid();

	// lock file if it exists
	if ((fd = open(fileName.c_str(), O_WRONLY)) >= 0)
	{
		if (fcntl(fd, F_SETLKW, &fl) == -1)
			throw runtime_error("Config::write: file lock error " + fileName);

		ofstream out(fileName.c_str());
		out << msg;

		fl.l_type   = F_UNLCK;	//unlock
		if (fcntl(fd, F_SETLK, &fl) == -1)
			throw runtime_error("Config::write: file unlock error " + fileName);

		close(fd);
	}
	else
	{
		ofstream out(fileName.c_str());
		out << msg;
	}
}


/* static */
void Config::deleteInstanceMap()
{
	mutex::scoped_lock lk(fInstanceMapMutex);
	for (Config::configMap_t::iterator iter = fInstanceMap.begin();
		iter != fInstanceMap.end(); ++iter)
	{
		Config* instance = iter->second;
		delete instance;
	}
	fInstanceMap.clear();
}

/* static */
int64_t Config::fromText(const std::string& text)
{
	if (text.length() == 0) return 0;

	int64_t val = 0;
	char* ctext = static_cast<char*>(alloca(text.length() + 1));
	strcpy(ctext, text.c_str());
	char* cptr;

	val = strtoll(ctext, &cptr, 0);

	switch (*cptr)
	{
	case 'T':
	case 't':
		val *= 1024;
		/* fallthru */
	case 'G':
	case 'g':
		val *= 1024;
		/* fallthru */
	case 'M':
	case 'm':
		val *= 1024;
		/* fallthru */
	case 'K':
	case 'k':
		val *= 1024;
		/* fallthru */
	case '\0':
		break;
	default:
		ostringstream oss;
		oss << "Invalid character '" << *cptr << "' found in numeric parameter '" << text <<
			"'. Since this will not do what you want it is fatal." << endl;
		throw runtime_error(oss.str());
		break;
	}

	return val;
}

time_t Config::getCurrentMTime()
{
	mutex::scoped_lock lk(fLock);

	struct stat statbuf;
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
		return statbuf.st_mtime;
	else
		return 0;
}

const vector<string> Config::enumConfig()
{
	mutex::scoped_lock lk(fLock);

	if (fDoc == 0){
		throw runtime_error("Config::getConfig: no XML document!");
	}

	struct stat statbuf;
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
	{
		if (statbuf.st_mtime != fMtime)
		{
			closeConfig();
			fMtime = statbuf.st_mtime;
			parseDoc();
		}
	}

	return fParser.enumConfig(fDoc);
}

const vector<string> Config::enumSection(const string& section)
{
	mutex::scoped_lock lk(fLock);

	if (fDoc == 0){
		throw runtime_error("Config::getConfig: no XML document!");
	}

	struct stat statbuf;
	if (stat(fConfigFile.c_str(), &statbuf) == 0)
	{
		if (statbuf.st_mtime != fMtime)
		{
			closeConfig();
			fMtime = statbuf.st_mtime;
			parseDoc();
		}
	}

	return fParser.enumSection(fDoc, section);
}

} //namespace config
// vim:ts=4 sw=4:

