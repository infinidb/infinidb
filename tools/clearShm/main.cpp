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

// $Id: main.cpp 2101 2013-01-21 14:12:52Z rdempsey $

#include "config.h"

#include <iostream>
#include <sys/types.h>
#include <cstring>
#include <cerrno>
#include <unistd.h>
#include <cstdlib>
#include <cstdio>
#include <sstream>
#include <iomanip>
#include <string>
using namespace std;

#include <boost/interprocess/shared_memory_object.hpp>
#include <boost/interprocess/sync/named_semaphore.hpp>
namespace bi=boost::interprocess;

#include <boost/thread/thread.hpp>
using namespace boost;

#include "shmkeys.h"
using namespace BRM;

namespace
{

bool vFlg;
bool nFlg;
mutex coutMutex;

void shmDoit(key_t shm_key, const string& label)
{
	string key_name = ShmKeys::keyToName(shm_key);
	if (vFlg)
	{
		try
		{
			bi::shared_memory_object memObj(bi::open_only,
				key_name.c_str(),
				bi::read_only);
			bi::offset_t memSize = 0;
			memObj.get_size(memSize);
			mutex::scoped_lock lk(coutMutex);
			cout << label << ": shm_key: " << shm_key <<
				"; key_name: " << key_name <<
				"; size: " << memSize << endl;
		}
		catch (...)
		{
		}
	}
	if (!nFlg)
	{
		bi::shared_memory_object::remove(key_name.c_str());
	}
}

void shmDoitRange(key_t shm_key, const string& label)
{
	if (shm_key == 0) return;

	unsigned shm_key_cnt;

	for (shm_key_cnt = 0; shm_key_cnt < ShmKeys::KEYRANGE_SIZE; shm_key_cnt++, shm_key++)
	{
		shmDoit(shm_key, label);
	}
}

void semDoit(key_t sem_key, const string& label)
{
	string key_name = ShmKeys::keyToName(sem_key);
	if (vFlg)
	{
		try
		{
			bi::shared_memory_object memObj(bi::open_only,
                                            key_name.c_str(),
                                            bi::read_only);
			bi::offset_t memSize = 0;
			memObj.get_size(memSize);
			mutex::scoped_lock lk(coutMutex);
			cout << label << ": sem_key: " << sem_key <<
				"; key_name: " << key_name <<
				"; size: " << memSize << endl;
		}
		catch (...)
		{
		}
	}
	if (!nFlg)
	{
		bi::shared_memory_object::remove(key_name.c_str());
	}
}

void usage()
{
	cout << "usage: clearShm [-cvnh]" << endl;
	cout << "   delete all InfiniDB shared memory data" << endl;
	cout << "   -h display this help" << endl;
	cout << "   -c only clear InfiniDB Engine data, leave OAM intact" << endl;
	cout << "   -v verbose output" << endl;
	cout << "   -n don't actually delete anything (implies -v)" << endl;
}

class ThdFunc
{
public:
	ThdFunc() : fShm_key(0) { }
	ThdFunc(key_t shm_key, const string& label) : fShm_key(shm_key), fLabel(label) { }

	~ThdFunc() { }

	void operator()() const
	{
		shmDoitRange(fShm_key, fLabel);
	}

private:
	//ThdFunc(const ThdFunc& rhs);
	//ThdFunc& operator=(const ThdFunc& rhs);

	key_t fShm_key;
	string fLabel;
};

}

int main(int argc, char** argv)
{
#ifdef _MSC_VER
	const char* envp = getenv("SystemRoot");
	string SystemRoot;
	if (envp && *envp)
		SystemRoot = envp;
	else
		SystemRoot = "C:\\WINDOWS";
	string tmpEnv = "TMP=" + SystemRoot + "\\Temp";
	_putenv(tmpEnv.c_str());
#endif

	int c;
	opterr = 0;
	bool cFlg = false;
	vFlg = false;
	nFlg = false;

	while ((c = getopt(argc, argv, "cvnh")) != EOF)
		switch (c)
		{
		case 'c':
			cFlg = true;
			break;
		case 'v':
			vFlg = true;
			break;
		case 'n':
			nFlg = true;
			break;
		case 'h':
		default:
			usage();
			return (c == 'h' ? 0 : 1);
			break;
		}

	if (nFlg)
		vFlg = true;

	ShmKeys BrmKeys;

	boost::thread_group tg;
	boost::thread* tp=0;
	tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_CL_BASE,         "COPYLOCK   "));
	tg.add_thread(tp);
	tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_EXTENTMAP_BASE,  "EXTMAP     "));
	tg.add_thread(tp);
	tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_EMFREELIST_BASE, "EXTMAP_FREE"));
	tg.add_thread(tp);
	tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_VBBM_BASE,       "VBBM       "));
	tg.add_thread(tp);
	tp = new boost::thread(ThdFunc(BrmKeys.KEYRANGE_VSS_BASE,        "VSS        "));
	tg.add_thread(tp);
	tg.join_all();

	shmDoit(BrmKeys.MST_SYSVKEY,           "MST        ");
	if (!cFlg)
	{
		shmDoit(BrmKeys.PROCESSSTATUS_SYSVKEY, "PROC_STAT  ");
		shmDoit(BrmKeys.SYSTEMSTATUS_SYSVKEY,  "SYS_STAT   ");
		shmDoit(BrmKeys.SWITCHSTATUS_SYSVKEY,  "SW_STAT    ");
		shmDoit(BrmKeys.STORAGESTATUS_SYSVKEY, "STORE_STAT ");
		shmDoit(BrmKeys.NICSTATUS_SYSVKEY,     "NIC_STAT   ");
		shmDoit(BrmKeys.DBROOTSTATUS_SYSVKEY,  "DBROOT_STAT");
	}
	shmDoit(BrmKeys.DECOMSVRMUTEX_SYSVKEY, "DCMSVRMUTEX");

	semDoit(BrmKeys.KEYRANGE_CL_BASE,         "COPYLOCK   ");
	semDoit(BrmKeys.KEYRANGE_EXTENTMAP_BASE,  "EXTMAP     ");
	semDoit(BrmKeys.KEYRANGE_EMFREELIST_BASE, "EXTMAP_FREE");
	semDoit(BrmKeys.KEYRANGE_VBBM_BASE,       "VBBM       ");
	semDoit(BrmKeys.KEYRANGE_VSS_BASE,        "VSS        ");
	semDoit(BrmKeys.MST_SYSVKEY,              "MST        ");
	if (!cFlg)
	{
		semDoit(BrmKeys.PROCESSSTATUS_SYSVKEY, "PROC_STAT  ");
		semDoit(BrmKeys.SYSTEMSTATUS_SYSVKEY,  "SYS_STAT   ");
		semDoit(BrmKeys.SWITCHSTATUS_SYSVKEY,  "SW_STAT    ");
		semDoit(BrmKeys.STORAGESTATUS_SYSVKEY, "STORE_STAT ");
		semDoit(BrmKeys.NICSTATUS_SYSVKEY,     "NIC_STAT   ");
		shmDoit(BrmKeys.DBROOTSTATUS_SYSVKEY,  "DBROOT_STAT");
	}

	return 0;
}
// vim:ts=4 sw=4:

