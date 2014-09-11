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
* $Id: configcpp.cpp 3145 2012-06-12 19:30:58Z rdempsey $
*
******************************************************************************************/
#include "config.h"

#include <string>
#include <stdexcept>
#include <iostream>
#include <sstream>
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

#ifdef _MSC_VER
#include "idbregistry.h"
#endif

#define LIBCONFIG_DLLEXPORT
#include "configcpp.h"
#undef LIBCONFIG_DLLEXPORT

namespace
{
const string defaultCalpontConfigFile("Calpont.xml");
const string defaultCalpontConfigFileTemp("Calpont.xml.temp");
const string tmpCalpontConfigFileTemp("Calpont.xml.temp1");
const string saveCalpontConfigFileTemp("Calpont.xml.calpontSave");
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
	string installDir("/usr/local/Calpont");

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
			configFile = installDir + "/etc/" + defaultCalpontConfigFile;
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
	fDoc(0), fConfigFile(configFile), fMtime(0), fInstallDir(installDir)
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

void Config::parseDoc(void) const
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

void Config::closeConfig(void) const
{
	xmlFreeDoc(fDoc);
	fDoc = 0;
}

const string Config::getConfig(const string& section, const string& name, const bool close) const
{
	mutex::scoped_lock lk(fLock);

	string res;

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

	xmlNodePtr cur1 = xmlDocGetRootElement(fDoc);
	if (cur1 == NULL)
		throw runtime_error("Config::getConfig: error parsing config file " + fConfigFile);

	cur1 = cur1->xmlChildrenNode;
	while (cur1 != NULL)
	{
		if ((!xmlStrcmp(cur1->name, (const xmlChar *)section.c_str())))
		{
			xmlNodePtr cur2 = cur1->xmlChildrenNode;
			while (cur2 != NULL)
			{
				if ((!xmlStrcmp(cur2->name, (const xmlChar*)name.c_str())))
				{
						xmlNodePtr cur3 = cur2->xmlChildrenNode;
						if ( cur3 == 0 )
							res = "";
						else
							res = (const char*)cur3->content;
						return expand(res);
				}
				cur2 = cur2->next;
			}
		}
		cur1 = cur1->next;
	}
	// maybe nullstr if not found
	return expand(res);
}

void Config::getConfig(const string& section, const string& name, vector<string>& values) const
{
	mutex::scoped_lock lk(fLock);

	string res;

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

	xmlNodePtr cur1 = xmlDocGetRootElement(fDoc);
	if (cur1 == NULL)
		throw runtime_error("Config::getConfig: error parsing config file " + fConfigFile);

	cur1 = cur1->xmlChildrenNode;
	while (cur1 != NULL)
	{
		if ((!xmlStrcmp(cur1->name, (const xmlChar *)section.c_str())))
		{
			xmlNodePtr cur2 = cur1->xmlChildrenNode;
			while (cur2 != NULL)
			{
				if ((!xmlStrcmp(cur2->name, (const xmlChar*)name.c_str())))
				{
						xmlNodePtr cur3 = cur2->xmlChildrenNode;
						if ( cur3 == 0 )
							res = "";
						else
							res = (const char*)cur3->content;
						values.push_back(expand(res));
				}
				cur2 = cur2->next;
			}
		}
		cur1 = cur1->next;
	}
}

void Config::setConfig(const string& section, const string& name, const string& value)
{
	mutex::scoped_lock lk(fLock);

	if (section.length() == 0 || name.length() == 0 || value.length() == 0)
		throw invalid_argument("Config::setConfig: all of section, name and value must have a length");

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

	xmlNodePtr cur1 = xmlDocGetRootElement(fDoc);
	if (cur1 == NULL)
		throw runtime_error("Config::setConfig: error parsing config file " + fConfigFile);

	xmlNodePtr cur2;

	cur1 = cur1->xmlChildrenNode;
	while (cur1 != NULL)
	{
		if (xmlStrcmp(cur1->name, (const xmlChar *)section.c_str()) == 0)
		{
			cur2 = cur1->xmlChildrenNode;
			while (cur2 != NULL)
			{
				if (xmlStrcmp(cur2->name, (const xmlChar*)name.c_str()) == 0)
				{
						xmlNodePtr cur3 = cur2->xmlChildrenNode;
						xmlFree(cur3->content);
						cur3->content = xmlStrdup((const xmlChar*)value.c_str());
						return;
				}
				cur2 = cur2->next;
			}
			// We found the section, but not the name, so we need to add a new node here
			xmlAddChild(cur1, xmlNewText((const xmlChar*)"\t"));
			xmlNewTextChild(cur1, NULL, (const xmlChar*)name.c_str(), (const xmlChar*)value.c_str());
			xmlAddChild(cur1, xmlNewText((const xmlChar*)"\n\t"));
			return;
		}
		cur1 = cur1->next;
	}

	// We did not find the section, so we need to add it and the name here
	cur1 = xmlDocGetRootElement(fDoc);
	xmlAddChild(cur1, xmlNewText((const xmlChar*)"\t"));
	cur2 = xmlNewChild(cur1, NULL, (const xmlChar*)section.c_str(), NULL);
	xmlAddChild(cur2, xmlNewText((const xmlChar*)"\n\t\t"));
	xmlNewTextChild(cur2, NULL, (const xmlChar*)name.c_str(), (const xmlChar*)value.c_str());
	xmlAddChild(cur2, xmlNewText((const xmlChar*)"\n\t"));
	xmlAddChild(cur1, xmlNewText((const xmlChar*)"\n"));

	return;
}

void Config::delConfig(const string& section, const string& name)
{
	mutex::scoped_lock lk(fLock);

	string res;

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

	xmlNodePtr cur1 = xmlDocGetRootElement(fDoc);
	if (cur1 == NULL)
		throw runtime_error("Config::getConfig: error parsing config file " + fConfigFile);

	cur1 = cur1->xmlChildrenNode;
	while (cur1 != NULL)
	{
		if ((!xmlStrcmp(cur1->name, (const xmlChar *)section.c_str())))
		{
			xmlNodePtr cur2 = cur1->xmlChildrenNode;
			while (cur2 != NULL)
			{
				xmlNodePtr tmp = cur2;
				cur2 = cur2->next;
				if ((!xmlStrcmp(tmp->name, (const xmlChar*)name.c_str())))
				{
					xmlUnlinkNode(tmp);
					xmlFreeNode(tmp);
				}
			}
		}
		cur1 = cur1->next;
	}

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

	string dcf = fInstallDir + "/etc/" + defaultCalpontConfigFile;
	string dcft = fInstallDir + "/etc/" + defaultCalpontConfigFileTemp;
	string scft = fInstallDir + "/etc/" + saveCalpontConfigFileTemp;
	string tcft = fInstallDir + "/etc/" + tmpCalpontConfigFileTemp;
	//perform a temp write first if Calpont.xml file to prevent possible corruption
	if ( configFile == dcf ) {

		string cmd = "rm -f " + dcft;
		::system(cmd.c_str());
		if ((fi = fopen(dcft.c_str(), "w+")) == NULL)
			throw runtime_error("Config::writeConfig: error writing config file " + configFile);
	
		int rc, err=0;
		rc = xmlDocDump(fi, fDoc);
		if ( rc < 0) {
			err = errno;
			throw runtime_error("Config::writeConfig: error writing config file " + configFile);
			//cout << "xmlDocDump " << rc << " " << errno << endl;
		}

		fclose(fi);
	
		//check temp file
		try {
			Config* c1 = makeConfig(dcft.c_str());
	
			string value;
			value = c1->getConfig("SystemConfig", "SystemName");

			//good read, save copy, copy temp file tp tmp then to Calpont.xml
			//move to /tmp to get around a 'same file error' in mv command
			string cmd = "rm -f " + scft;
			::system(cmd.c_str());
			cmd = "cp " + dcf + " " + scft;
			::system(cmd.c_str());

			cmd = "rm -f " + tcft;
			::system(cmd.c_str());
			cmd = "mv -f " + dcft + " " + tcft;
			::system(cmd.c_str());

			cmd = "mv -f " + tcft + " " + dcf;
			::system(cmd.c_str());
		}
		catch(...)
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
		break;
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

const string Config::expand(const std::string& in) const
{
	string out(in);
	string::size_type pos;
	const string::size_type len=11;

	pos = out.find("$INSTALLDIR");
	while (pos != string::npos)
	{
		out.replace(pos, len, fInstallDir);
		pos = out.find("$INSTALLDIR");
	}

	return out;
}

} //namespace config

