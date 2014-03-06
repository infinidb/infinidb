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
 * $Id: objectidmanager.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 ****************************************************************************/

/** @file
 * This file implements OIDServer
 *
 * The OIDServer is responsible for allocating and deallocating
 * Object IDs from a 24-bit space consistently across all processes & threads.
 * The expected allocation size is 1 or 2 OIDs, and the usage pattern is
 * unknown.  The OID space is described by a 16Mbit bitmap file on disk
 * and a brief free list.  Accesses are synchronized by using flock().
 *
 * This class must implement a high degree of correctness.  However, it
 * also requires file IO.  Most functions throw an exception if a hard IO error
 * occurs more than MaxRetries times in a row.  Right now the code makes
 * no attempt to back out changes that occured before the error although it
 * may be possible to do so for certain errors.  Probably the best course of 
 * action would be to halt the system if an exception is thrown here
 * to prevent database corruption resulting allocation of OIDs from a 
 * possibly corrupt OID bitmap.  The OID bitmap can be rebuilt at system
 * startup if necessary (need to write a tool to do that still).
 *
 * There are a few checks to verify the safety of allocations and 
 * deallocations & the correctness of this implementation in general.  
 * Those errors will throw logic_error.  IO errors will throw 
 * ios_base::failure.
 *
 * There are probably oodles of optimizations possible given this implmementation.
 * For example:
 * 		- make fullScan() construct a freelist
 *		- sorting & coalescing free list entries will raise the hit rate 
 *		  (at what cost?)
 *		- implement a bias for high numbered OIDs in the free list to reduce
 *	 	  the number of fullScans searching far in the bitmap
 *		- avoid the extra read in flipOIDBlock when called by fullscan
 * 		- singleton object operating entirely in memory (no file IO)
 *			- singleton object using only a free list (no bitmap)
 *		- a different implementation altogether.
 *
 * Time permitting we might look into those things.
 */

#define BRMOIDSVR_DLLEXPORT
#include "oidserver.h"
#undef BRMOIDSVR_DLLEXPORT

#include "configcpp.h"
#include <cstdio>
#include <cstdlib>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <iostream>
#include <errno.h>
#include <cstring>
#include <stdexcept>
#if __linux__
#include <values.h>
#include <sys/file.h>
#endif
#include <sstream>
//#define NDEBUG
#include <cassert>
#include <boost/thread/thread.hpp>

#include "IDBDataFile.h"
#include "IDBPolicy.h"

using namespace std;
using namespace idbdatafile;

#include "dbrm.h"

#ifndef O_BINARY
#  define O_BINARY 0
#endif
#ifndef O_DIRECT
#  define O_DIRECT 0
#endif
#ifndef O_LARGEFILE
#  define O_LARGEFILE 0
#endif
#ifndef O_NOATIME
#  define O_NOATIME 0
#endif

namespace
{
boost::mutex CtorMutex;

class EOFException : public exception
{};
}

namespace BRM {

boost::mutex OIDServer::fMutex;

#if 0
void OIDServer::lockFile() const
{
	int err, errCount;

	int errnoSave = 0;
	for (errCount = 0, err = -1; err != 0 && errCount < MaxRetries;) {
		err = flock(fFd, LOCK_EX);
		if (err < 0 && errno != EINTR) {  // EINTR isn't really an error
			errCount++;
			errnoSave = errno; // save errno because perror may overwrite
			perror("OIDServer::lockFile(): flock (retrying)");
		}
	}
	if (errCount == MaxRetries) {
		ostringstream oss;
		oss << "OIDServer::lockFile(): flock error:  " << strerror(errnoSave);
		throw ios_base::failure(oss.str());
	}
}
#endif

void OIDServer::writeData(uint8_t *buf, off_t offset, int size) const
{
	int errCount, err, progress;
	off_t seekerr = -1;

	if (size == 0)
		return;

	if (IDBPolicy::useHdfs()) {
		for (errCount = 0; errCount < MaxRetries && seekerr != offset; errCount++) {
			seekerr = fFp->seek(offset, SEEK_SET);
			if (seekerr >= 0)
				seekerr = fFp->tell(); // IDBDataFile may use fseek for seek.
			if (seekerr < 0)
				perror("OIDServer::writeDataHdfs(): lseek");
		}
		if (errCount == MaxRetries)
			throw ios_base::failure("OIDServer::writeDataHdfs(): lseek failed "
					"too many times");

		for (progress = 0, errCount = 0; progress < size && errCount < MaxRetries;) {
			err = fFp->write(&buf[progress], size - progress);
			if (err < 0) {
				if (errno != EINTR) {  // EINTR isn't really an error
					errCount++;
					perror("OIDServer::writeDataHdfs(): write (retrying)");
				}
			}
			else
				progress += err;
		}

		fFp->tell();
	}
	else {
		for (errCount = 0; errCount < MaxRetries && seekerr != offset; errCount++) {
			seekerr = lseek(fFd, offset, SEEK_SET);
			if (seekerr < 0)
				perror("OIDServer::writeData(): lseek");
		}
		if (errCount == MaxRetries)
			throw ios_base::failure("OIDServer::writeData(): lseek failed "
					"too many times");

		for (progress = 0, errCount = 0; progress < size && errCount < MaxRetries;) {
			err = write(fFd, &buf[progress], size - progress);
			if (err < 0) {
				if (errno != EINTR) {  // EINTR isn't really an error
					errCount++;
					perror("OIDServer::writeData(): write (retrying)");
				}
			}
			else
				progress += err;
		}
	}

	if (errCount == MaxRetries)
		throw ios_base::failure("OIDServer::writeData(): write error");
}

void OIDServer::readData(uint8_t *buf, off_t offset, int size) const
{
	int errCount, err, progress;
	off_t seekerr = -1;

	if (size == 0)
		return;

	if (IDBPolicy::useHdfs()) {
		for (errCount = 0; errCount < MaxRetries && seekerr != offset; errCount++) {
			seekerr = fFp->seek(offset, SEEK_SET);
			if (seekerr >= 0)
				seekerr = fFp->tell(); // IDBDataFile may use fseek for seek.
			if (seekerr < 0)
				perror("OIDServer::readDataHdfs(): lseek");
		}
		if (errCount == MaxRetries)
			throw ios_base::failure("OIDServer::readDataHdfs(): lseek failed "
					"too many times");

		for (progress = 0, errCount = 0; progress < size && errCount < MaxRetries;) {
			err = fFp->read(&buf[progress], size - progress);
			if (err < 0) {
				if (errno != EINTR) {  // EINTR isn't really an error
					errCount++;
					perror("OIDServer::readDataHdfs(): read (retrying)");
				}
			}
			else if (err == 0)
				throw EOFException();
			else
				progress += err;
		}
	}
	else {
		for (errCount = 0; errCount < MaxRetries && seekerr != offset; errCount++) {
			seekerr = lseek(fFd, offset, SEEK_SET);
			if (seekerr < 0)
				perror("OIDServer::readData(): lseek");
		}
		if (errCount == MaxRetries)
			throw ios_base::failure("OIDServer::readData(): lseek failed "
					"too many times");

		for (progress = 0, errCount = 0; progress < size && errCount < MaxRetries;) {
			err = read(fFd, &buf[progress], size - progress);
			if (err < 0) {
				if (errno != EINTR) {  // EINTR isn't really an error
					errCount++;
					perror("OIDServer::readData(): read (retrying)");
				}
			}
			else if (err == 0)
				throw EOFException();
			else
				progress += err;
		}
	}

	if (errCount == MaxRetries)
		throw ios_base::failure("OIDServer::readData(): read error");
}

void OIDServer::initializeBitmap() const
{
	uint8_t buf[HeaderSize];
	int i, bitmapSize = StartOfVBOidSection - HeaderSize;
	struct FEntry *h1;
	string stmp;
	int64_t ltmp;
	int firstOID;
	config::Config *conf;

	conf = config::Config::makeConfig();
	try {
		stmp = conf->getConfig("OIDManager", "FirstOID");
	}
	catch(exception&) {
	}
	if (stmp.empty()) stmp = "3000";
	ltmp = config::Config::fromText(stmp);
	if (ltmp > numeric_limits<int32_t>::max() || ltmp < 0)
	{
		ltmp = config::Config::fromText("3000");
	}
	firstOID = static_cast<int>(ltmp);

	boost::mutex::scoped_lock lk(fMutex);
	h1 = reinterpret_cast<struct FEntry*>(buf);
	//write the initial header
	h1[0].begin = firstOID;
	h1[0].end = 0x00ffffff;
	for (i = 1; i < FreeListEntries; i++) {
		h1[i].begin = -1;
		h1[i].end = -1;
	}
	writeData(buf, 0, HeaderSize);

	// reset buf to all 0's and write the bitmap
	for (i = 0; i < HeaderSize; i++)
		buf[i] = 0;

	for (i = 0; i < bitmapSize; i += HeaderSize)
		writeData(buf, HeaderSize+i, (bitmapSize-i > HeaderSize ? HeaderSize : bitmapSize-i));

	flipOIDBlock(0, firstOID, 0);

	/* append a 16-bit 0 to indicate 0 entries in the vboid->dbroot mapping */
	writeData(buf, StartOfVBOidSection, 2);
}

OIDServer::OIDServer() : fFp(NULL), fFd(-1)
{
	boost::mutex::scoped_lock lk(CtorMutex);

	config::Config* conf;
	string tmp;
	ostringstream os;

	conf = config::Config::makeConfig();
	fFilename = conf->getConfig("OIDManager", "OIDBitmapFile");

	if (fFilename.empty()) {
		os << "OIDServer: <OIDManager><OIDBitmapFile> must exist in the config file";
		log(os.str());
		throw runtime_error(os.str());
	}

	if (IDBPolicy::useHdfs()) {
		if (!IDBPolicy::exists(fFilename.c_str())) { //no bitmap file
			BRM::DBRM em;
			if (!em.isEMEmpty())
			{
				os << "Extent Map not empty and " << fFilename << " not found. Setting system to read-only";
				cerr << os.str() << endl;
				em.setReadOnly(true);
				throw runtime_error(os.str());
			}

			fFp = IDBDataFile::open(IDBPolicy::getType(fFilename.c_str(), IDBPolicy::WRITEENG),
									fFilename.c_str(), "w+b", 0, 1);

			if (!fFp) {
				os << "Couldn't create oid bitmap file " << fFilename << ": " <<
						strerror(errno);
				log(os.str());
				throw ios_base::failure(os.str());
			}
#ifndef _MSC_VER
			//FIXME:
			//fchmod(fFd, 0666);   // XXXPAT: override umask at least for testing
			if (fFp)
				chmod(fFilename.c_str(), 0666);   // XXXPAT: override umask at least for testing
#endif
			try {
				initializeBitmap();
			}
			catch(...) {
				delete fFp;
				fFp = NULL;
				throw;
			}
		}
		else {
			fFp = IDBDataFile::open(IDBPolicy::getType(fFilename.c_str(), IDBPolicy::WRITEENG),
									fFilename.c_str(), "r+b", 0, 1);
			if (!fFp) {
				ostringstream os;
				os << "Couldn't open oid bitmap file" << fFilename << ": " <<
						strerror(errno);
				log(os.str());
				throw ios_base::failure(os.str());
			}
		}
	}
	else {
		if (access(fFilename.c_str(), F_OK) != 0) //no bitmap file
		{
			BRM::DBRM em;
			if (!em.isEMEmpty())
			{
				os << "Extent Map not empty and " << fFilename << " not found. Setting system to read-only";
				cerr << os.str() << endl;
				em.setReadOnly(true);
				throw runtime_error(os.str());
			}
		}

		fFd = open(fFilename.c_str(), O_CREAT | O_EXCL | O_RDWR | O_BINARY, 0666);
		if (fFd >= 0) {
#ifndef _MSC_VER
			//FIXME:
			fchmod(fFd, 0666);   // XXXPAT: override umask at least for testing
#endif
			try {
				initializeBitmap();
			}
			catch(...) {
				close(fFd);
				throw;
			}
		}
		else if (errno == EEXIST) {
			fFd = open(fFilename.c_str(), O_RDWR | O_BINARY);
			if (fFd < 0) {
				os << "Couldn't open oid bitmap file " << fFilename << ": " <<
						strerror(errno);
				log(os.str());
				throw ios_base::failure(os.str());
			}
		}
		else {
			os << "Couldn't create oid bitmap file " << fFilename << ": " <<
					strerror(errno);
			log(os.str());
			throw ios_base::failure(os.str());
		}
	}

	loadVBOIDs();
}

OIDServer::~OIDServer()
{
	if (fFd >= 0)
		close(fFd);

	delete fFp;
	fFp = NULL;
}

void OIDServer::loadVBOIDs()
{
	uint16_t size;
	try {
		readData((uint8_t *) &size, StartOfVBOidSection, 2);
	}
	catch (EOFException &e) {
		/* convert old OID bitmap */
		size = 0;
		writeData((uint8_t *) &size, StartOfVBOidSection, 2);
	}

	if (size == 0)
		return;

	vbOidDBRootMap.resize(size);
	readData((uint8_t *) &vbOidDBRootMap[0], StartOfVBOidSection + 2, size * 2);

	//cout << "loaded " << size << " vboids: ";
	//for (uint32_t i = 0; i < size; i++)
	//	cout << (int) vbOidDBRootMap[i] << " ";
	//cout << endl;
}

void OIDServer::useFreeListEntry(struct FEntry &fe, int num)
{
	int blockSize;

	blockSize = fe.end - fe.begin + 1;
	if (blockSize == num) {
		fe.begin = -1;
		fe.end = -1;
	}
	else
		fe.begin += num;
}

// mode = 0 -> allocate, mode = 1 -> deallocate
// this currently verifies the request as it makes the requested changes
// it might make more sense to verify before making the changes instead.
// either way, it implies a larger programming error, so maybe it doesn't
// matter.
void OIDServer::flipOIDBlock(int blockStart, int num, int mode) const
{
	int offset, i, oidCount, byteSize, blockEnd;
	uint8_t *buf;
	uint8_t mask;

	// safety check
	if (blockStart + num - 1 > 0x00ffffff)
		throw logic_error("flipOIDBlock: request overruns oid space");

	blockEnd = blockStart + num - 1;
	offset = blockStart/8 + HeaderSize;
	byteSize = blockEnd/8 - blockStart/8 + 1;
	i = 0;
retry:
	try {
		buf = new uint8_t[byteSize];
	}
	catch(bad_alloc&) {
		if (++i == MaxRetries)
			throw;
		cerr << "flipOIDBlock: mem alloc failed (retrying)" << endl;
		goto retry;
	}
	oidCount = 0;
	readData(buf, offset, byteSize);

	// verify 1st byte
	mask = 0x80 >> (blockStart % 8);
	while (mask != 0 && oidCount < num) {
		if ((buf[0] & mask) != (mode ? mask : 0)) {
			delete [] buf;
			throw logic_error("flipOIDBlock: bad allocation or deallocation attempted (1)");
		}
		if (mode)
			buf[0] &= ~mask;
		else
			buf[0] |= mask;
		mask >>= 1;
		oidCount++;
	}
	if (oidCount == num) {
		writeData(buf, offset, byteSize);
		if (IDBPolicy::useHdfs())
			fFp->flush();

		delete [] buf;
		return;
	}

	// verify the middle bytes
	for (i = 1; i < byteSize - 1; i++, oidCount+=8)
		if (buf[i] != (mode ? 0xff : 0)) {
			delete [] buf;
			throw logic_error("flipOIDBlock: bad allocation or deallocation attempted (2)");
		}
		else
			if (mode)
				buf[i] = 0;
			else
				buf[i] = 0xff;

	// verify the last byte
	mask = 0x80;
	while (mask != 0 && oidCount < num) {
		if ((buf[byteSize-1] & mask) != (mode ? mask : 0)) {
			delete [] buf;
			if ( mode )
				throw logic_error("flipOIDBlock: bad deallocation attempted");
			else
				throw logic_error("flipOIDBlock: bad allocation attempted");
		}
		if (mode)
			buf[byteSize-1] &= ~mask;
		else
			buf[byteSize-1] |= mask;
		mask >>= 1;
		oidCount++;
	}
	if (oidCount == num) {
		writeData(buf, offset, byteSize);
		if (IDBPolicy::useHdfs())
			fFp->flush();

		delete [] buf;
		return;
	}

	delete [] buf;
	throw logic_error("logic error in flipOIDBlock detected");
}

int OIDServer::fullScan(int num, struct FEntry* freelist) const
{
	uint8_t buf[4096];
	int fileOffset, i, blockCount=0, blockStart=0, bitOffset;
	uint8_t mask;
	bool countingZeros = false;

	fileOffset = HeaderSize;
	// this assumes the bitmap is a multiple of 4096
	while (fileOffset < StartOfVBOidSection) {
		readData(reinterpret_cast<uint8_t*>(buf), fileOffset, 4096);

		for (i = 0; i < 4096; i++) {
			if (countingZeros) {
				mask = 0x80;
				bitOffset = 0;
				goto countZeros;
			}
			else
			if (buf[i] != 0xff) {
				mask = 0x80;
				bitOffset = 0;
skipOnes:		while ((buf[i] & mask) == mask && bitOffset < 8) {
					mask >>= 1;
					bitOffset++;
				}
				if (bitOffset == 8)
					continue;
				countingZeros = true;
				blockStart = ((fileOffset - HeaderSize + i) * 8) + bitOffset;
				blockCount = 1;
				bitOffset++;
				mask >>= 1;
countZeros:		while ((buf[i] & mask) == 0 && bitOffset < 8 && blockCount < num) {
					mask >>= 1;
					blockCount++;
					bitOffset++;
				}
				if (blockCount == num) {   //found a match
					patchFreelist(freelist, blockStart, blockCount);
					flipOIDBlock(blockStart, blockCount, 0);
					return blockStart;
				}
				if (bitOffset == 8)
					continue;
				countingZeros = false;
				goto skipOnes;
			}
		}
		fileOffset += 4096;
	}
	return -1;
}

void OIDServer::patchFreelist(struct FEntry* freelist, int start, int num) const
{
	int i, changed = 0, end = start + num - 1;

	for (i = 0; i < FreeListEntries; i++) {

		if (freelist[i].begin == -1)
			continue;

		// the allocated block overlaps the beginning of this entry.
		// this is the only clause that should execute unless there's an
		// error in the implementation somewhere (probably fullscan)
		if (start <= freelist[i].begin && end >= freelist[i].begin) {
			changed = 1;

			// if possible, truncate this entry otherwise invalidate it
			if (end < freelist[i].end)
				freelist[i].begin = end + 1;
			else {
				freelist[i].begin = -1;
				freelist[i].end = -1;
			}
		}

		// the allocated block is contained in the middle of this block.
		// (this shouldn't be possible; allocOIDs should have
		// picked this entry to allocate from)
		else if (start >= freelist[i].begin && end <= freelist[i].end)
			throw logic_error("patchFreelist: a block was allocated in the "
					"middle of a known-free block");
	}

	if (changed) {
		writeData(reinterpret_cast<uint8_t*>(freelist), 0, HeaderSize);
		if (IDBPolicy::useHdfs())
			fFp->flush();
	}
}

int OIDServer::allocVBOID(uint16_t dbroot)
{
	int ret;
	uint32_t offset;

	ret = vbOidDBRootMap.size();

	offset = StartOfVBOidSection + 2 + (vbOidDBRootMap.size() * 2);
	vbOidDBRootMap.push_back(dbroot);

	try {
		uint16_t size = vbOidDBRootMap.size();
		boost::mutex::scoped_lock lk(fMutex);
		writeData((uint8_t *) &size, StartOfVBOidSection, 2);
		writeData((uint8_t *) &dbroot, offset, 2);
	}
	catch(...) {
		vbOidDBRootMap.pop_back();
		throw;
	}

	if (IDBPolicy::useHdfs())
		fFp->flush();

	return ret;
}

int OIDServer::getDBRootOfVBOID(uint32_t vbOID)
{
	//cout << "getDBRootOfVBOID, vbOID = " << vbOID << " size = " << vbOidDBRootMap.size() << endl;
	if (vbOID >= vbOidDBRootMap.size())
		return -1;
	return vbOidDBRootMap[vbOID];
}

const vector<uint16_t> & OIDServer::getVBOIDToDBRootMap()
{
	return vbOidDBRootMap;
}

int OIDServer::getVBOIDOfDBRoot(uint32_t dbRoot)
{
	uint32_t i;

	for (i = 0; i < vbOidDBRootMap.size(); i++)
		if (vbOidDBRootMap[i] == dbRoot)
			return i;
	return -1;
}

int OIDServer::allocOIDs(int num)
{
	struct FEntry freelist[FreeListEntries];
	int i, size, bestMatchIndex, bestMatchSize, bestMatchBegin=0;

	boost::mutex::scoped_lock lk(fMutex);
	readData(reinterpret_cast<uint8_t*>(freelist), 0, HeaderSize);

	// scan freelist using best fit strategy (an attempt to maximize hits on
	// the freelist)
	bestMatchSize = numeric_limits<int>::max();
	for (i = 0, bestMatchIndex = -1; i < FreeListEntries; i++) {
		if (freelist[i].begin == -1)
			continue;
		size = freelist[i].end - freelist[i].begin + 1;
		if (size == num) {
			bestMatchIndex = i;
			bestMatchBegin = freelist[i].begin;
			break;
		}
		if (size > num && size < bestMatchSize) {
			bestMatchIndex = i;
			bestMatchSize = size;
			bestMatchBegin = freelist[i].begin;
		}
	}

	if (bestMatchIndex == -1) {
		return fullScan(num, freelist);
	}

	useFreeListEntry(freelist[bestMatchIndex], num);
	writeData(reinterpret_cast<uint8_t*>(freelist), 0, HeaderSize);
	flipOIDBlock(bestMatchBegin, num, 0);

	if (IDBPolicy::useHdfs())
		fFp->flush();

	return bestMatchBegin;
}

void OIDServer::returnOIDs(int start, int end) const
{
	//@Bug 1412. Do not reuse oids for now.
/*	struct FEntry freelist[FreeListEntries];
	int i, emptyEntry = -1, entryBefore = -1, entryAfter = -1;
	bool changed = true;

	lockFile();

	try {
		readData(reinterpret_cast<uint8_t*>(freelist), 0, HeaderSize);
	}
	catch (...) {
		flock(fFd, LOCK_UN);
		throw;
	}

	for (i = 0; i < FreeListEntries; i++) {

		//this entry is empty
	    if (freelist[i].begin == -1) {
			if (emptyEntry == -1)
				emptyEntry = i;
		}
		else
		//this entry is before the block to return
		if (freelist[i].end == start - 1)
			entryBefore = i;
		//the entry is after
		else if (freelist[i].begin == end + 1)
			entryAfter = i;
	}

		if (entryBefore != -1 && entryAfter != -1) {
			freelist[entryBefore].end = freelist[entryAfter].end;
			freelist[entryAfter].begin = -1;
		}
		else if (entryBefore != -1)
			freelist[entryBefore].end = end;
		else if (entryAfter != -1)
			freelist[entryAfter].begin = start;
		else if (emptyEntry != -1) {
			freelist[emptyEntry].begin = start;
			freelist[emptyEntry].end = end;
		}
		else
			changed = false;


//	    "Old Reliable"
	for (i = 0; i < FreeListEntries; i++)
		if (freelist[i].begin == -1) {
			freelist[i].begin = start;
			freelist[i].end = end;
			break;
		}


	try {
 		if (i != FreeListEntries)
		if (changed)
			writeData(reinterpret_cast<uint8_t*>(freelist), 0, HeaderSize);
		flipOIDBlock(start, end - start + 1, 1);
	}
	catch (...) {
		flock(fFd, LOCK_UN);
		throw;
	}

	flock(fFd, LOCK_UN);
*/
}

int OIDServer::size() const
{
	int ret = 0, offset = 0, bytenum = 0;
	uint8_t buf[4096], mask = 0;

	boost::mutex::scoped_lock lk(fMutex);
	for (offset = HeaderSize; offset < StartOfVBOidSection; offset+=4096) {
		readData(buf, offset, 4096);
		for (bytenum = 0; bytenum < 4096; bytenum++)
			for (mask = 0x80; mask != 0; mask >>= 1)
				if ((buf[bytenum] & mask) == mask)
					ret++;
	}

	return ret;
}

const string OIDServer::getFilename() const
{
	return fFilename;
}

}  // namespace
