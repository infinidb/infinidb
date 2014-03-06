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

/*****************************************************************************
 * $Id: sessionmonitor.cpp 9210 2013-01-21 14:10:42Z rdempsey $
 *
 ****************************************************************************/

using namespace std;
 
#include <sys/types.h>
#include <sys/ipc.h>
#include <sys/sem.h>
#include <sys/shm.h>
#include <sys/types.h>
#include <time.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>

#include <iostream>
#include <fstream>
#include <string>
#include <stdexcept>
#include <ios>
#include <vector>

#include "calpontsystemcatalog.h"
#include "sessionmonitor.h"
#include "configcpp.h"

#include "vendordmlstatement.h"
#include "calpontdmlpackage.h"
#include "calpontdmlfactory.h"
using namespace dmlpackage;

#include "bytestream.h"
#include "messagequeue.h"
using namespace messageqcpp;

using namespace BRM;

namespace execplan {

SessionMonitor::SessionMonitor()
{		
	config::Config* conf;
	int madeSems;
	string stmp;

	fuid=getuid();
	conf = config::Config::makeConfig();
	try {
		stmp = conf->getConfig("SessionManager", "MaxConcurrentTransactions");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		stmp.empty();
	}
		
	int tmp = static_cast<int>(config::Config::fromText(stmp));
	if (tmp <=0)
		fMaxTxns = 1000;
	else
		fMaxTxns = tmp;

	stmp.clear();
	try {
		stmp = conf->getConfig("SessionMonitor", "SharedMemoryTmpFile");
	}
	catch(exception& e) {
		cout << e.what() << endl;
		stmp.empty();
	}

	// Instantiate/delete a SessionManager to make sure it's shared memory segments are present.
	SessionManager* mgr = new SessionManager();
	delete mgr;
		
	if (stmp != "")
		fSegmentFilename = strdup(stmp.c_str());
	else
		fSegmentFilename = strdup("/tmp/CalpontSessionMonitorShm");

	try {
	 	madeSems = getSems();
		fHaveSemaphores=true;
	}
	catch (...) {
		fHaveSemaphores=false;
	}

	stmp.clear();
	try {
		stmp = conf->getConfig("SessionMonitor", "TransactionAgeLimit");
	}
	catch(exception& e) {
		cerr << e.what() << endl;
		stmp.empty();
	}
		
	tmp = static_cast<int>(config::Config::fromText(stmp));
	if (tmp<=0)
		fAgeLimit = fDefaultAgeLimit;
	else
		fAgeLimit = tmp;

	fIsAttached=false;
	getSharedData();
	unlock();
	fCurrentSegment=NULL;
	fPrevSegment.activeTxns=NULL;

	if (haveSharedMemory())
		copyCurrentSegment();

	if (haveSemaphores())
		copyPreviousSegment();
}

SessionMonitor::SessionMonitor(const SessionMonitor& sm)
{
}

SessionMonitor::~SessionMonitor()
{
	saveAsMonitorData(segmentFilename());
	if (fSessionManagerData!= NULL) {
		lock();
		detachSegment();
		unlock();
	}

	//delete [] fCurrentSegment;
	//delete [] fSessionMonitorData.activeTxns;
	//delete [] fPrevSegment.activeTxns;
}

void SessionMonitor::detachSegment()
{
	//delete [] fSessionManagerData;

	fIsAttached=false;
}

//returns 1 if it attached to pre-existing semaphores, else it throws exception.
int SessionMonitor::getSems()
{
	return 1;
}
		
void SessionMonitor::lock()
{
}

void SessionMonitor::unlock()
{
}

void SessionMonitor::printTxns(const MonSIDTIDEntry& txn) const
{
	cout << "sessionid " << txn.sessionid
		<< " txnid " << txn.txnid.id
		<< " valid " << (txn.txnid.valid==true?"TRUE":"FALSE")
		<< " time_t " << txn.txnid.firstrecord
		<< " tdiff " << time(NULL)-txn.txnid.firstrecord
		<< " ctime " << ctime(&txn.txnid.firstrecord);
}

#ifdef SM_DEBUG
void SessionMonitor::printTxns(const SessionManager::SIDTIDEntry& txn) const
{
	cout << "sessionid " << txn.sessionid
		<< " txnid " << txn.txnid.id
		<< " valid " << (txn.txnid.valid==true?"TRUE":"FALSE")
		<< endl;
}

void SessionMonitor::printMonitorData(const int len) const
{
	MonSIDTIDEntry* txns = fPrevSegment.activeTxns;
		
	cout << "Monitor txnCount " << fPrevSegment.txnCount << " verID " << fPrevSegment.verID << endl;
	for(int idx=0;txns && idx<len; idx++)
	{
		printTxns(txns[idx]);
	}
	cout << "==" << endl;
}

void SessionMonitor::printSegment(const SessionManagerData_t* seg, const int len) const
{
	if (seg==NULL) {
		cerr << "No SessionManagerData" << endl;
		return;
	}
		
	cout << "Manager txnCount " << seg->txnCount << " verID " << seg->verID << endl;
	for(int idx=0; idx<len;idx++)
	{
		printTxns(seg->activeTxns[idx]);
	}
}
#endif

void SessionMonitor::getSharedData()
{
	int len;

	fSessionManagerData = reinterpret_cast<SessionManagerData_t*>(sm.getShmContents(len));
	if (fSessionManagerData == NULL) {
		cerr << "SessionMonitor::getSharedData(): getShmContents() failed" << endl;
		throw runtime_error("SessionMonitor::getSharedData(): getShmContents() failed.  Check the error log.");
	}

	fIsAttached=true;
}

void SessionMonitor::initSegment(SessionMonitorData_t *seg)
{
	if (!seg)
		return;
		
	seg->txnCount=0;
	seg->verID=0;
	int size = maxTxns()*sizeof(MonSIDTIDEntry_t);

	if (seg->activeTxns)
		memset(seg->activeTxns, 0, size);
}

void SessionMonitor::initSegment(SessionManagerData_t *seg)
{
	if (!seg)
		return;
		
	int size = maxTxns()*sizeof(SIDTIDEntry_t);
	seg->txnCount=0;
	seg->verID=0;
	memset(seg->activeTxns, 0, size);
}

void SessionMonitor::copyPreviousSegment()
{
	try {
		bool loadSuccessful = readMonitorDataFromFile(segmentFilename());
		if (!loadSuccessful)
		{
			saveAsMonitorData(segmentFilename());
			readMonitorDataFromFile(segmentFilename());
		}
	}
	catch(...) {
		saveAsMonitorData(segmentFilename());
		readMonitorDataFromFile(segmentFilename());
	}
}

bool SessionMonitor::readMonitorDataFromFile(const std::string filename)
{
	int err = 0;
	uint32_t headerSize = 2*sizeof(int);
	char* data = reinterpret_cast<char *>(&fPrevSegment);
	int fd = open(filename.c_str(), O_RDONLY);
	
	if (fd < 0) {
		perror("SessionMonitor::readMonitorDataFromFile(): open");
		return false;
	}
		
	err = read(fd, data, headerSize);
	if (err < 0)
	{
		if (errno != EINTR) {
			perror("SessionMonitor::readMonitorDataFromFile(): read");
		}
	}
	else if (err == 0)
	{
		close(fd);
		return false;
	}

	int dataSize=maxTxns()*sizeof(MonSIDTIDEntry_t);
	delete [] fPrevSegment.activeTxns;

	fPrevSegment.activeTxns = new MonSIDTIDEntry[maxTxns()];
	data = reinterpret_cast<char *>(fPrevSegment.activeTxns);
	memset(data, 0, sizeof(MonSIDTIDEntry)*maxTxns());
	err = read(fd, data, dataSize);
	if (err < 0)
	{
		if (errno != EINTR) {
			perror("SessionMonitor::readMonitorDataFromFile(): read");
		}
	}
	else if (err == 0)
	{
		close(fd);
		perror("SessionMonitor::readMonitorDataFromFile(): read 0");
		return false;
	}

	close(fd);

	return true;
}

void SessionMonitor::saveAsMonitorData(const std::string)
{
	int fd;
	int err = 0;
	char *data = reinterpret_cast<char *>(&fSessionMonitorData);

	if (!fSessionMonitorData.activeTxns) {
		fSessionMonitorData.activeTxns = new MonSIDTIDEntry[maxTxns()];
	}

	initSegment(&fSessionMonitorData);

	// get the most recent SessionManagerData
	copyCurrentSegment();
	fSessionMonitorData.txnCount=fCurrentSegment->txnCount;
	fSessionMonitorData.verID=fCurrentSegment->verID;

	for(int idx=0; idx<maxTxns(); idx++)
	{
		// is this a new txns or previously existing
		MonSIDTIDEntry* monitor = &fSessionMonitorData.activeTxns[idx];
		MonSIDTIDEntry* prevMonitor = (fPrevSegment.activeTxns?&fPrevSegment.activeTxns[idx]:NULL);
		SIDTIDEntry* manager = &fCurrentSegment->activeTxns[idx];

		if (prevMonitor) {
			if (!isEqualSIDTID(*manager, *prevMonitor)) {
			 	monitor->txnid.firstrecord = time(NULL);
			}
			else
			if (isEqualSIDTID(*manager, *prevMonitor) && isUsed(*prevMonitor)) {
				if (prevMonitor && prevMonitor->txnid.firstrecord==0)
					monitor->txnid.firstrecord = time(NULL);
				else
					monitor->txnid.firstrecord = prevMonitor->txnid.firstrecord;
			} else
			if (manager->txnid.valid==false && monitor->txnid.id == manager->txnid.id)
				monitor->txnid.firstrecord=0;
		} else {
			if (manager->txnid.valid && manager->txnid.id) {
			 	monitor->txnid.firstrecord = time(NULL);
			} else
				monitor->txnid.firstrecord=0;
		}

		monitor->sessionid = manager->sessionid;
		monitor->txnid.id = manager->txnid.id;
		monitor->txnid.valid = manager->txnid.valid;
	}

	// Always write to a new empty file
	fd = open(segmentFilename(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
	if (fd < 0) {
		perror("SessionMonitor::saveAsMonitorData(): open");
		throw ios_base::failure("SessionMonitor::saveAsMonitorData(): open failed.	Check the error log.");
	}
		
	int headerSize = 2*(sizeof(int));
	err = write(fd, data, headerSize);
	if (err < 0) {
		if (errno != EINTR)
			perror("SessionMonitor::saveAsMonitorData(): write");
	}
		
	int dataSize=maxTxns()*sizeof(MonSIDTIDEntry);
	data=reinterpret_cast<char *>(fSessionMonitorData.activeTxns);
	err = write(fd, data, dataSize);
	if (err < 0) {
		if (errno != EINTR)
			perror("SessionMonitor::saveAsMonitorData(): write");
	}

	close(fd);
}

void SessionMonitor::copyCurrentSegment()
{
	const int cpsize = 4*sizeof(int) + 2*sizeof(boost::interprocess::interprocess_semaphore) + maxTxns()*sizeof(SIDTIDEntry);

	delete [] reinterpret_cast<char*>(fCurrentSegment);

	fCurrentSegment = reinterpret_cast<SessionManagerData_t *>(new char[cpsize]);
	lock();
	memcpy(fCurrentSegment, fSessionManagerData, cpsize);
	unlock();
}

bool SessionMonitor::isUsed( const MonSIDTIDEntry& e) const
{
	if (e.sessionid==0 && e.txnid.id==0 && e.txnid.valid==false && e.txnid.firstrecord==0)
		return false;

	return true;
}		

bool SessionMonitor::isUsedSIDTID( const SIDTIDEntry& e) const
{
	if (e.sessionid == 0 && e.txnid.id == 0 && e.txnid.valid == false)
		return false;

	return true;
}		

bool SessionMonitor::isStaleSIDTID( const SIDTIDEntry& a,  const MonSIDTIDEntry& b) const
{
	if (b.txnid.valid && isEqualSIDTID(a,b) && b.txnid.firstrecord && (time(NULL)-b.txnid.firstrecord) > fAgeLimit)
		return true;

	return false;
}

bool SessionMonitor::isEqualSIDTID( const SIDTIDEntry& a,  const MonSIDTIDEntry& b) const
{
	if (a.sessionid == b.sessionid &&
		a.txnid.id == b.txnid.id &&
		a.txnid.valid == b.txnid.valid)
		return true;

	return false;
}

vector<SessionMonitor::MonSIDTIDEntry_t*> SessionMonitor::timedOutTxns()
{
	vector<MonSIDTIDEntry_t*> txnsVec;

	copyCurrentSegment();

	if (fCurrentSegment && fPrevSegment.activeTxns != NULL) {
		for(int idx=0; fCurrentSegment->activeTxns  && fPrevSegment.activeTxns && idx<maxTxns();idx++)
		{
			if (isUsedSIDTID(fCurrentSegment->activeTxns[idx]) &&
				isStaleSIDTID(fCurrentSegment->activeTxns[idx], fPrevSegment.activeTxns[idx]))
			{
				txnsVec.push_back(&fPrevSegment.activeTxns[idx]);
			}
		}

    	sort(txnsVec.begin(), txnsVec.end(), lessMonSIDTIDEntry());
    }

	return txnsVec;
}

const int SessionMonitor::txnCount() const {
	return fSessionMonitorData.txnCount;
}

}  //namespace
