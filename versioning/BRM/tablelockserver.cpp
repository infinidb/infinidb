/* Copyright (C) 2013 Calpont Corp.

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

/*****************************************************************************
 * $Id$
 *
 ****************************************************************************/

#include <exception>
#include <boost/scoped_ptr.hpp>

#include "configcpp.h"
#include "IDBDataFile.h"
#include "IDBPolicy.h"

#define BRMTBLLOCKSVR_DLLEXPORT
#include "tablelockserver.h"
#undef BRMTBLLOCKSVR_DLLEXPORT

using namespace std;
using namespace boost;
using namespace idbdatafile;

namespace BRM {

TableLockServer::TableLockServer(SessionManagerServer *sm) : sms(sm)
{
	mutex::scoped_lock lk(mutex);
	config::Config *config = config::Config::makeConfig();

	filename = config->getConfig("SystemConfig", "TableLockSaveFile");
	if (filename == "")
		throw invalid_argument("TableLockServer: Need to define SystemConfig/TableLockSaveFile in config file");  // todo, replace this

	load();
}

TableLockServer::~TableLockServer()
{
}

// call with lock held
void TableLockServer::save()
{
	lit_t it;
	uint count = locks.size();

	const char* filename_p = filename.c_str();
	if (IDBPolicy::useHdfs()) {
		scoped_ptr<IDBDataFile> out(IDBDataFile::open(
					                IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
									filename_p, "wb", 0));
		if (!out)
			throw runtime_error("TableLockServer::save():  could not open save file");
		out->write((char *) &count, 4);
		for (it = locks.begin(); it != locks.end(); ++it) {
			if (!out)
				throw runtime_error("TableLockServer::save():  could not write save file");
			it->second.serialize(out.get());
		}
	}
	else {
		ofstream out(filename.c_str(), ios::trunc | ios::binary | ios::out );

		if (!out)
			throw runtime_error("TableLockServer::save():  could not open save file");
		out.write((char *) &count, 4);
		for (it = locks.begin(); it != locks.end(); ++it) {
			if (!out)
				throw runtime_error("TableLockServer::save():  could not write save file");
			it->second.serialize(out);
		}
	}
}

// call with lock held
void TableLockServer::load()
{
	uint32_t size;
	uint i = 0;
	TableLockInfo tli;

	/* Need to standardize the file error handling */
	if (IDBPolicy::useHdfs()) {
		const char* filename_p = filename.c_str();
		scoped_ptr<IDBDataFile>  in(IDBDataFile::open(
					                IDBPolicy::getType(filename_p, IDBPolicy::WRITEENG),
									filename_p, "rb", 0));
		if (!in) {
			ostringstream os;
			os << "TableLockServer::load(): could not open the save file"
					<< filename;
			log(os.str(), logging::LOG_TYPE_WARNING);
			return;
		}

		try {
			in->read((char *) &size, 4);
			for (i = 0; i < size; i++) {
				tli.deserialize(in.get());
				tli.id = sms->getUnique64();   // Need new #s...
				if (tli.id == 0)	// 0 is an error code
					tli.id = sms->getUnique64();
				locks[tli.id] = tli;
			}
		}
		catch (std::exception &e) {
			ostringstream os;
			os << "TableLockServer::load(): could not load save file " << filename <<
				" loaded " << i << "/" << size << " entries\n";
			throw;
		}
	}
	else {
		ifstream in(filename.c_str(), ios::binary | ios::in);
		if (!in) {
			ostringstream os;
			os << "TableLockServer::load(): could not open the save file"
					<< filename;
			log(os.str(), logging::LOG_TYPE_WARNING);
			return;
		}
		in.exceptions(ios::failbit | ios::badbit);
		try {
			in.read((char *) &size, 4);
			for (i = 0; i < size; i++) {
				tli.deserialize(in);
				tli.id = sms->getUnique64();   // Need new #s...
				if (tli.id == 0)	// 0 is an error code
					tli.id = sms->getUnique64();
				locks[tli.id] = tli;
			}
		}
		catch (std::exception &e) {
			ostringstream os;

			os << "TableLockServer::load(): could not load save file " << filename <<
				" loaded " << i << "/" << size << " entries\n";
			throw;
		}
	}
}


// throws on a failed save()
uint64_t TableLockServer::lock(TableLockInfo *tli)
{
	set<uint32_t> dbroots;
	lit_t it;
	uint i;
	mutex::scoped_lock lk(mutex);

	for (i = 0; i < tli->dbrootList.size(); i++)
		dbroots.insert(tli->dbrootList[i]);

	for (it = locks.begin(); it != locks.end(); ++it) {
		if (it->second.overlaps(*tli, dbroots)) {
			tli->ownerName = it->second.ownerName;
			tli->ownerPID = it->second.ownerPID;
			tli->ownerSessionID = it->second.ownerSessionID;
			tli->ownerTxnID = it->second.ownerTxnID;
			return false;
		}
	}
	tli->id = sms->getUnique64();
	if (tli->id == 0)   // 0 is an error code
		tli->id = sms->getUnique64();
	locks[tli->id] = *tli;
	try {
		save();
	}
	catch (...) {
		locks.erase(tli->id);
		throw;
	}
	return tli->id;
}

bool TableLockServer::unlock(uint64_t id)
{
	std::map<uint64_t, TableLockInfo>::iterator it;
	TableLockInfo tli;

	mutex::scoped_lock lk(mutex);
	it = locks.find(id);
	if (it != locks.end()) {
		tli = it->second;
		locks.erase(it);
		try {
			save();
		}
		catch (...) {
			locks[tli.id] = tli;
			throw;
		}
		return true;
	}
	return false;
}

bool TableLockServer::changeState(uint64_t id, LockState state)
{
	lit_t it;
	mutex::scoped_lock lk(mutex);
	LockState old;

	it = locks.find(id);
	if (it == locks.end())
		return false;
	old = it->second.state;
	it->second.state = state;
	try {
		save();
	}
	catch (...) {
		it->second.state = old;
		throw;
	}
	return true;
}

bool TableLockServer::changeOwner(uint64_t id, const string &ownerName, uint pid, int32_t session,
		int32_t txnID)
{
	lit_t it;
	mutex::scoped_lock lk(mutex);
	string oldName;
	uint32_t oldPID;
	int32_t oldSession;
	int32_t oldTxnID;

	it = locks.find(id);
	if (it == locks.end())
		return false;
	oldName = it->second.ownerName;
	oldPID = it->second.ownerPID;
	oldSession = it->second.ownerSessionID;
	oldTxnID = it->second.ownerTxnID;
	it->second.ownerName = ownerName;
	it->second.ownerPID = pid;
	it->second.ownerSessionID = session;
	it->second.ownerTxnID = txnID;
	try {
		save();
	}
	catch (...) {
		it->second.ownerName = oldName;
		it->second.ownerPID = oldPID;
		it->second.ownerSessionID = oldSession;
		it->second.ownerTxnID = oldTxnID;
		throw;
	}
	return true;
}

vector<TableLockInfo> TableLockServer::getAllLocks() const
{
	vector<TableLockInfo> ret;
	mutex::scoped_lock lk(mutex);
	constlit_t it;

	for (it = locks.begin(); it != locks.end(); ++it)
		ret.push_back(it->second);
	return ret;
}

void TableLockServer::releaseAllLocks()
{
	std::map<uint64_t, TableLockInfo> tmp;

	mutex::scoped_lock lk(mutex);
	tmp.swap(locks);
	try {
		save();
	}
	catch (...) {
		tmp.swap(locks);
		throw;
	}
}

bool TableLockServer::getLockInfo(uint64_t id, TableLockInfo *out) const
{
	constlit_t it;
	mutex::scoped_lock lk(mutex);

	it = locks.find(id);
	if (out == NULL)
		return (it != locks.end());
	if (it != locks.end()) {
		*out = it->second;
		return true;
	}
	return false;
}

}

