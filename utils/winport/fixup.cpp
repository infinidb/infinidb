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
#include <string>
#include <iostream>
#include <fstream>
using namespace std;

#include <boost/filesystem.hpp>
namespace fs=boost::filesystem;

#include "configcpp.h"
using namespace config;

#include "sedit.h"
#include "idbregistry.h"
using namespace winport;

#include "fixup.h"

namespace
{

int fixupCalpontXML()
{
	int rc = -1; //assume the worst

    DWORDLONG totalMemSize = 1024ULL * 1024ULL * 1024ULL * 2ULL;
#ifdef _WIN64
	totalMemSize *= 4ULL;
#endif
	MEMORYSTATUSEX memStat;
	memStat.dwLength = sizeof(memStat);
	if (GlobalMemoryStatusEx(&memStat) == 0)
		//FIXME: Assume 2GB?
		(void)0;
	else
	{
#ifndef _WIN64
		memStat.ullTotalPhys = std::min(memStat.ullTotalVirtual, memStat.ullTotalPhys);
#endif
		//We now have the total phys mem in bytes
		//FIXME: should we use avail phys mem instead?
		totalMemSize = memStat.ullTotalPhys;
	}
	try
	{
		Config* cf = Config::makeConfig();
		string section;
		string parm;
		string val;

		//Fixup ConnectionsPerPrimProc
		section = "PrimitiveServers";
		parm = "ConnectionsPerPrimProc";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = "2";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup PrefetchThreshold
		parm = "PrefetchThreshold";
		val = cf->getConfig(section, parm);
		if (val == "30")
		{
			val = "5";
			cf->setConfig(section, parm, val);
			cout << "Fixing " << section << "." << parm << " = " << val << endl;
		}

		//Fixup Count
		parm = "Count";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = "1";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup PMS2
		section = "PMS2";
		parm = "IPAddr";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = "127.0.0.1";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "Port";
			val = "8620";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup UmMaxMemorySmallSide
		section = "HashJoin";
		parm = "UmMaxMemorySmallSide";
		val = cf->getConfig(section, parm);
#ifdef _WIN64
		if (val == "4G")
#else
		if (val == "256M")
#endif
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		//Fixup TotalUmMaxMemorySmallSide
		parm = "TotalUmMaxMemorySmallSide";
		val = cf->getConfig(section, parm);
#ifdef _WIN64
		if (val == "8G")
#else
		if (val == "512M")
#endif
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		//Fixup TotalUmMemory
		parm = "TotalUmMemory";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
#ifdef _WIN64
			totalMemSize /= (1024ULL * 1024ULL * 1024ULL);
			if (totalMemSize >= 31)
				val = "16G";
			else if (totalMemSize >= 15)
				val = "8G";
			else if (totalMemSize >= 7)
				val = "4G";
			else
				val = "2G";
#else
			val = "2G";
#endif
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup MaxMemoryPerUnion
		section = "Union";
		parm = "MaxMemoryPerUnion";
		val = cf->getConfig(section, parm);
#ifdef _WIN64
		if (val == "4G")
#else
		if (val == "256M")
#endif
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		//Fixup TotalUnionMemory
		parm = "TotalUnionMemory";
		val = cf->getConfig(section, parm);
#ifdef _WIN64
		if (val == "8G")
#else
		if (val == "512M")
#endif
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		//Fixup RowAggregation.MaxMemory
		section = "RowAggregation";
		parm = "MaxMemory";
		val = cf->getConfig(section, parm);
#ifdef _WIN64
		if (val == "1G")
#else
		if (val == "128M")
#endif
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		//Fixup OrderByLimit.MaxMemory
		section = "OrderByLimit";
		parm = "MaxMemory";
		val = cf->getConfig(section, parm);
#ifdef _WIN64
		if (val == "1G")
#else
		if (val == "128M")
#endif
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		//Fixup Installation.UMStorageType
		section = "Installation";
		parm = "UMStorageType";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = "internal";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup DBRootStorageType
		parm = "DBRootStorageType";
		val = cf->getConfig(section, parm);
		if (val == "local")
		{
			val = "internal";
			cf->setConfig(section, parm, val);
			cout << "Fixing " << section << "." << parm << " = " << val << endl;
		}

		//Fixup WES
		section = "pm1_WriteEngineServer";
		parm = "IPAddr";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = "127.0.0.1";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "Port";
			val = "8630";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup TableLockSaveFile
		section = "SystemConfig";
		parm = "TableLockSaveFile";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = IDBreadRegistry("") + "/dbrm/tablelocks";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup SystemModuleConfig
		section = "SystemModuleConfig";
		parm = "ModuleDBRootCount1-1";
		val = cf->getConfig(section, parm);
		if (val.empty())
		{
			val = "0";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "ModuleDBRootCount1-2";
			val = "0";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "ModuleDBRootCount1-3";
			val = "1";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "ModuleDBRootID1-1-1";
			val = "0";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "ModuleDBRootID1-1-2";
			val = "0";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
			parm = "ModuleDBRootID1-1-3";
			val = "1";
			cf->setConfig(section, parm, val);
			cout << "Adding " << section << "." << parm << " = " << val << endl;
		}

		//Fixup NVBF
		section = "VersionBuffer";
		parm = "NumVersionBufferFiles";
		val = cf->getConfig(section, parm);
		if (!val.empty())
		{
			cf->delConfig(section, parm);
			cout << "Deleting " << section << "." << parm << endl;
		}

		cf->write();

		rc = 0;
	}
	catch (...)
	{
	}

	return rc;
}

int fixupMyIni()
{
	return 0;
}

}

namespace bootstrap
{

int fixupConfig(const string& installDir, const string& mysqlPort)
{
	int rc = -1;

	string id = installDir;
	string::size_type p;
	p = id.find('\\');
	while (p != string::npos)
	{
		id[p] = '/';
		p = id.find('\\');
	}

	fs::path cFilePath;
	fs::path tmpPath;
	ifstream ifs;
	ofstream ofs;
	string strLine;
	bool okayToRename = false;

	cFilePath = installDir;
	cFilePath /= "etc";
	cFilePath /= "Calpont.xml";
	tmpPath = cFilePath;
	tmpPath.replace_extension(".tmp");
	fs::remove(tmpPath);
	ifs.open(cFilePath.string().c_str());
	ofs.open(tmpPath.string().c_str());
	getline(ifs, strLine);
	while (ifs.good())
	{
		sedit(strLine, "##INSTDIR##", id);
		ofs << strLine << endl;
		getline(ifs, strLine);
	}
	if (!ifs.bad() && !ofs.bad())
		okayToRename = true;
	ofs.close();
	ifs.close();
	if (okayToRename)
	{
		fs::remove(cFilePath);
		fs::rename(tmpPath, cFilePath);
	}
	else
		return -1;
	ifs.clear();
	ofs.clear();

	okayToRename = false;
	cFilePath = installDir;
	cFilePath /= "my.ini";
	tmpPath = cFilePath;
	tmpPath.replace_extension(".tmp");
	fs::remove(tmpPath);
	ifs.open(cFilePath.string().c_str());
	ofs.open(tmpPath.string().c_str());
	getline(ifs, strLine);
	while (ifs.good())
	{
		sedit(strLine, "##INSTDIR##", id);
		sedit(strLine, "##PORT##", mysqlPort);
#ifndef SKIP_MYSQL_SETUP4
		sedit(strLine, "#infinidb_compression_type=0", "infinidb_compression_type=2");
		sedit(strLine, "infinidb_compression_type=1", "infinidb_compression_type=2");
#endif
		ofs << strLine << endl;
		getline(ifs, strLine);
	}
	if (!ifs.bad() && !ofs.bad())
		okayToRename = true;
	ofs.close();
	ifs.close();
	if (okayToRename)
	{
		fs::remove(cFilePath);
		fs::rename(tmpPath, cFilePath);
	}
	else
		return -1;
	ifs.clear();
	ofs.clear();

	//Now do any final fixups...
	rc = fixupMyIni();
	if (rc == 0)
		rc = fixupCalpontXML();
	return rc;
}

}
