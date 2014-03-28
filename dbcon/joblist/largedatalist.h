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

/******************************************************************************
 * $Id: largedatalist.h 9655 2013-06-25 23:08:13Z xlou $
 *
 *****************************************************************************/

/** @file
 * class XXX interface
 */

#ifndef _LARGEDATALIST_HPP
#define _LARGEDATALIST_HPP

#include <iostream>
#include <fstream>
#include <vector>
#include <stdexcept>
#include <sstream>

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>
#include <boost/date_time/posix_time/posix_time.hpp>

#include "datalistimpl.h"
#include "configcpp.h"
#include "elementcompression.h"
#include "resourcemanager.h"

#include "exceptclasses.h"

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

namespace joblist {
extern const std::string defaultTempDiskPath;   // defined in resourcemanager.cpp
const uint32_t defaultMaxElements = 0x2000000;  // 32M

// Enumeration used to specify element type compression mode for elements
// saved to temporary disk.
enum CompressionModeType
{
	COMPRESS_NO_COMPRESS = 1, // no compression of RID or data value
	COMPRESS_TO_64_32    = 2, // compress to 64 bit RID, 32 bit value
	COMPRESS_TO_32_64    = 3, // compress to 32 bit RID, 64 bit value
	COMPRESS_TO_32_32    = 4, // compress to 32 bit RID, 32 bit value
	COMPRESS_TO_32       = 5, // compress RID only to 32 bit RID only
	COMPRESS_TO_32_STR   = 6  // compress to 32 bit RID for a StringElementType
};

#ifndef _DISKIOINFO__
#define _DISKIOINFO__
struct DiskIoInfo
{
	boost::posix_time::ptime fStart;
	boost::posix_time::ptime fEnd;
	uint64_t fBytes;
	bool     fWrite;

	// c: byte count; b: is write operation?
	DiskIoInfo(bool b = false) : fBytes(0), fWrite(b) {}
};
#endif  //_DISKIOINFO__

/* element_t has to implement ostream & operator<<(). */
/* This is an abstract class, but doesn't look like it. */

/** @brief class LargeDataList
 *
 */
template<typename container_t, typename element_t>
class LargeDataList : public DataListImpl<container_t, element_t>
{
	typedef DataListImpl<container_t, element_t> base;

	public:
		LargeDataList(uint numConsumers,
			uint32_t elementSaveSize1,
			uint32_t elementSaveSize2,
			const ResourceManager& rm);
		virtual ~LargeDataList();

		virtual void endOfInput();
		virtual void insert(const element_t& e);
		virtual uint64_t totalSize();
		virtual void setMultipleProducers(bool);
		virtual uint64_t numberOfTempFiles() const;
		virtual void setDiskElemSize(uint32_t size1st,uint32_t size2nd);
		virtual void setReuseInfo(SetRestoreInfo* info, const std::string filename, bool readonly);
		virtual void restoreSetForReuse(const struct SetRestoreInfo& info);
		virtual void traceOn(bool b) { fTraceOn = b; }
        std::list<DiskIoInfo>& diskIoList() { return fDiskIoList; }

	protected:
		// @bug 721. add append option
		virtual void load(uint64_t setNumber, bool append = false);

		/* Warning!!  The container must have enough memory reserved, it must be
		contiguous, and element_t must be inline data!! */
		// @bug 721. add append option
		virtual void load_contiguous(uint64_t setNumber, bool append = false);

		// these save methods return the number of bytes written by the save
		// operation
		virtual uint64_t save();
		virtual uint64_t save(container_t *c);
		virtual uint64_t save_contiguous();
		virtual uint64_t save_contiguous(container_t *c);

		virtual void registerNewSet();
		virtual std::string getFilename();

		virtual bool next(uint64_t it, element_t *e);
		virtual bool next_nowait(uint64_t it, element_t *e);
		virtual void waitForConsumePhase();
		virtual void resetIterators();
		virtual void resetIterators_nowait();
		void removeFile();
		bool saveForReuse() { return fSaveForReuse; }

		std::string path;
		uint64_t loadedSet, setCount;
		int64_t phase;  // 0 = produce phase, 1 = consume phase
		uint64_t totSize;
		bool multipleProducers;
		bool fTraceOn;
		std::list<DiskIoInfo> fDiskIoList;

	private:
		// Declare but don't define default and copy constuctor, and assignment
		// operator to disable their use.
		explicit LargeDataList();
		LargeDataList(const LargeDataList<container_t, element_t> &);
		LargeDataList& operator=(const LargeDataList<container_t, element_t> &);

		void createTempFile();
		void save_contiguousCompressed(container_t *c);
		void save_noncontiguousCompressed(container_t *c);
		template<typename saveElement_t>
			void writeContiguousCompressed(container_t *c);
		void load_contiguousCompressed(bool append, uint64_t count);
		template<typename saveElement_t>
			void readContiguousCompressed(
				uint64_t   count,
				element_t* pElementData);
		void setCompressionMode(); // set current compression mode
		void saveRestoreInfo();

		boost::condition consumePhase;		// consumers block here until endOfInput()
		uint64_t filenameCounter;
		std::string  fFilename;				// LDL file name
		std::vector<std::fstream::pos_type> fSetStartPositions;	//file offsets
		std::fstream fFile;					// stream used to store the LDL file
		uint64_t	 fLoadedSetCount;		// number of sets that've been loaded
		CompressionModeType fCompressMode;	// compression used when saved to disk

		bool fReUse;						// flag for reuse
		bool fSaveForReuse;					// flag to save the restore infomation
		SetRestoreInfo* fRestoreInfo;		// point to the restore data in control
};

template<typename container_t, typename element_t>
LargeDataList<container_t, element_t>::LargeDataList(uint nc, uint32_t elementSaveSize1st, uint32_t elementSaveSize2nd, const ResourceManager& rm):
		base(nc), path(rm.getScTempDiskPath()), fTraceOn(false), fReUse(false), fSaveForReuse(false), fRestoreInfo(NULL)
{
// 	config::Config *config = config::Config::makeConfig();

	loadedSet = 0;
	setCount = 1;
	filenameCounter = 1;
	phase = 0;
	totSize = 0;
	multipleProducers = false;
	fLoadedSetCount   = 0;
	setDiskElemSize ( elementSaveSize1st, elementSaveSize2nd );
// 	try {
// 		path = config->getConfig("SystemConfig", "TempDiskPath");
// 	}
// 	catch (...) {
// 	}
// 	if (path.length() == 0)
// 		path = defaultTempDiskPath;
	//pthread_cond_init(&consumePhase, NULL);
}

template<typename container_t, typename element_t>
LargeDataList<container_t, element_t>::~LargeDataList()
{
	std::vector<std::string>::iterator it;

	//pthread_cond_destroy(&consumePhase);
	removeFile();
}


template<typename container_t, typename element_t>
inline void LargeDataList<container_t, element_t>::removeFile()
{
	if (!fFilename.empty() && !fReUse)
	{
		unlink(fFilename.c_str());
		fFilename = "";
	}
}


template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::setMultipleProducers(bool b)
{
	multipleProducers = b;
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::endOfInput()
{
	if (fSaveForReuse == true)
		saveRestoreInfo();

	base::endOfInput();
	phase = 1;
	consumePhase.notify_all(); //pthread_cond_broadcast(&consumePhase);
}

template<typename container_t, typename element_t>
std::string LargeDataList<container_t, element_t>::getFilename()
{
	std::stringstream o;

	o << path << "/LDL-0x" << std::hex << (ptrdiff_t)this << std::dec << "-" << filenameCounter++;
	return o.str();
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::setReuseInfo(
									SetRestoreInfo* info, const std::string filename, bool readonly)
{
	fReUse = true;
	fSaveForReuse = !readonly;
	fRestoreInfo = info;
	fFilename = filename;
	std::ios_base::openmode mode = std::ios_base::in | std::ios_base::binary;

	// if need to create the file
	if (fSaveForReuse) mode |= std::ios_base::out | std::ios_base::trunc;

	fFile.open(filename.c_str(), mode);
	if (!(fFile.is_open()))
	{
		std::string errMsg("Error opening BucketReuse file ");
		errMsg += filename;
		perror(errMsg.c_str());
		throw logging::LargeDataListExcept(errMsg);
	}
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::saveRestoreInfo()
{
	fRestoreInfo->fSetCount = setCount;
	fRestoreInfo->fTotalSize = totSize;
	fRestoreInfo->fSetStartPositions = fSetStartPositions;
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::restoreSetForReuse(const struct SetRestoreInfo& info)
{
	setCount = info.fSetCount;
	totSize = info.fTotalSize;
	fSetStartPositions = info.fSetStartPositions;

	load(0);
}

//------------------------------------------------------------------------------
// Return the number of temporary files created by this datalist.
// With the current implementation, there will only be 1 file, but this method
// will help protect the application code from knowing this, in case we
// want to change the implementation to sometimes output more than 1 file.
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
uint64_t LargeDataList<container_t, element_t>::numberOfTempFiles() const
{
// 	uint64_t nFiles = 0;
// 	if ( !fFilename.empty() )
// 		nFiles = 1;
//
// 	return nFiles;
	return  ((!fSetStartPositions.empty() && !fReUse) ? 1 : 0);
}

//------------------------------------------------------------------------------
// Create and open the temporary LDL file we will be saving data to.  With
// the current implementation, we are creating a single file to hold all the
// sets for a LargeDataList.
// exceptions: runtime_error thrown if file creation or file open fails
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::createTempFile()
{
	int64_t fd;
	/* Is there a good way to do this through an fstream? */
	do {
		fFilename = this->getFilename();
		fd = open(fFilename.c_str(), O_CREAT | O_RDWR | O_EXCL | O_BINARY, 0666);
	} while (fd < 0 && errno == EEXIST);

	if (fd < 0)
	{
		std::ostringstream errmsg;
		errmsg << "LargeDataList::createTempFile(): could not save to disk ("
			<< errno << ") - " << std::strerror(errno);
		std::cerr << errmsg.str() << std::endl;
		throw std::runtime_error(errmsg.str());
	}

	close(fd);

	//std::cout << "Creating/opening file: " << fFilename << std::endl;
	fFile.open(fFilename.c_str(),
		std::ios_base::in | std::ios_base::out | std::ios_base::binary );
	if (!(fFile.is_open())) {
		std::string errMsg("Error opening temp file ");
		errMsg += fFilename;
		perror(errMsg.c_str());
		throw logging::LargeDataListExcept(errMsg);
	}
}

// Need to grab the mutex at a higher level

/*
	File format:
	int: # of elements
	element_t[# of elements] stored according to ostream& element_t::operator<<
*/
template<typename container_t, typename element_t>
uint64_t LargeDataList<container_t, element_t>::save()
{
	container_t *c = (base::c);
	return save(c);
}

// Need to grab the mutex at a higher level

/*
	File format:
	int: # of elements
	element_t[# of elements] stored according to ostream& element_t::operator<<
*/
template<typename container_t, typename element_t>
uint64_t LargeDataList<container_t, element_t>::save(container_t *c )
{
	uint64_t count, nBytesWritten=0;
	typename container_t::iterator it;

#ifdef PROFILE
	struct timespec ts1, ts2;
	clock_gettime(CLOCK_REALTIME, &ts1);
#endif

	/* XXXPAT: catch exceptions in save/load or at the higher level? */
	try {
		//...Create the temporary LDL file (if necessary)
		if ( fFilename.empty() )
			createTempFile();

		// Save our file offset for this set, to be used when we load the set.
		std::fstream::pos_type firstByte = fFile.tellp();
		fSetStartPositions.push_back( firstByte );
		//std::cout << "Saving " << fFilename << "; count-" << c->size()  <<
		//	"; tellp-" << fFile.tellp() << std::endl;

		DiskIoInfo info(true);
		if (fTraceOn) info.fStart = boost::posix_time::microsec_clock::local_time();

		count = c->size();
		fFile.write((char *) &count, sizeof(count));

		if (fCompressMode == COMPRESS_NO_COMPRESS) {
			std::copy(c->begin(),
					  c->end(),
					  std::ostream_iterator<element_t>(fFile));
		}
		else {
			save_noncontiguousCompressed ( c );
		}
		nBytesWritten = fFile.tellp() - firstByte;

		if (fTraceOn) {
			info.fEnd= boost::posix_time::microsec_clock::local_time();
			info.fBytes = nBytesWritten;
			fDiskIoList.push_back(info);
		}
	}
	catch(const std::runtime_error& e) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred saving non-contiguous container into file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg + e.what());
	}
	catch(...) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred saving non-contiguous container into file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg);
	}

#ifdef PROFILE
	clock_gettime(CLOCK_REALTIME, &ts2);
	/* What should we do with this profile info? */
#endif

	return nBytesWritten;
}

template<typename container_t, typename element_t>
uint64_t LargeDataList<container_t, element_t>::save_contiguous()
{
	std::vector<element_t> *c = reinterpret_cast<std::vector<element_t> *>(base::c);
	return save_contiguous(c);
}

template<typename container_t, typename element_t>
uint64_t LargeDataList<container_t, element_t>::save_contiguous(container_t *c)
{
	uint64_t count, nBytesWritten=0;
	typename container_t::iterator it;

#ifdef PROFILE
	struct timespec ts1, ts2;
	clock_gettime(CLOCK_REALTIME, &ts1);
#endif

	/* XXXPAT: catch exceptions in save/load or at the higher level? */
	try {
		//...Create the temporary LDL file (if necessary)
		if ( fFilename.empty() )
			createTempFile();

		// Save our file offset for this set, to be used when we load the set.
		std::fstream::pos_type firstByte = fFile.tellp();
		fSetStartPositions.push_back( firstByte );
		//std::cout << "SavingC " << fFilename << "; count-" << c->size()  <<
		//	"; tellp-" << fFile.tellp() << std::endl;

		DiskIoInfo info(true);
		if (fTraceOn) info.fStart = boost::posix_time::microsec_clock::local_time();

		count = c->size();
		fFile.write((char *) &count, sizeof(count));

		// Perform compression of data, as it is saved, "if" applicable
		if (fCompressMode == COMPRESS_NO_COMPRESS) {
			fFile.write((char *) (c->begin().operator->()),
								sizeof(element_t) * count);
		}
		else {
			save_contiguousCompressed ( c );
		}
		nBytesWritten = fFile.tellp() - firstByte;

		if (fTraceOn) {
			info.fEnd= boost::posix_time::microsec_clock::local_time();
			info.fBytes = nBytesWritten;
			fDiskIoList.push_back(info);
		}
	}
	catch(const std::runtime_error& e) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred saving contiguous container into file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg + e.what());
	}
	catch(...) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred saving contiguous container into file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg);
	}

#ifdef PROFILE
	clock_gettime(CLOCK_REALTIME, &ts2);
	/* What should we do with this profile info? */
#endif

	return nBytesWritten;
}

//------------------------------------------------------------------------------
// Compress and save contiguous data to temp file.
// c - container to be saved
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::save_contiguousCompressed(
	container_t *c)
{
	//...Compress and write out the elements based on the compression mode
	switch (fCompressMode)
	{
		case COMPRESS_TO_64_32:
		{
			writeContiguousCompressed<CompElement64Rid32Val>(c);
			break;
		}
		case COMPRESS_TO_32_64:
		{
			writeContiguousCompressed<CompElement32Rid64Val>(c);
			break;
		}
		case COMPRESS_TO_32_32:
		{
			writeContiguousCompressed<CompElement32Rid32Val>(c);
			break;
		}
		case COMPRESS_TO_32:
		{
			writeContiguousCompressed<CompElement32RidOnly>(c);
			break;
		}
		default:
		{
			std::ostringstream errmsg;
			errmsg << "save_contiguousCompressed() called "
				" without compression mode being set";
			std::cerr << errmsg << std::endl;
			throw std::logic_error( errmsg.str() );
			break;
		}
	}
}

//------------------------------------------------------------------------------
// Template method that compresses a contiguous collection of element_t,
// to a vector of saveElement_t; and then saves the data to temp file.
// c - container to be written to temporary disk file.
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
template<typename saveElement_t>
void LargeDataList<container_t, element_t>::writeContiguousCompressed(
	container_t *c)
{
	uint64_t count = c->size();
	std::vector<element_t>* v = reinterpret_cast<std::vector<element_t> *>(c);

	//...copy/compress data into vector of saveElement_t
	std::vector<saveElement_t> cSave;
	cSave.resize( count );
	ElementCompression::compress ( *v, cSave );

	//...write saveElement_t vector to temp file
	fFile.write((char *) (cSave.begin().operator->()),
		(sizeof(saveElement_t)*count));
}

//------------------------------------------------------------------------------
// Compress and save noncontiguous data to temp file.
// c - container to be saved
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::save_noncontiguousCompressed(
	container_t *c)
{
	//...Compress and write out the elements based on the compression mode.
	//...The only compression currently supported here is compression of the
	//...RID from 64 to 32 bits.
	switch (fCompressMode)
	{
		case COMPRESS_TO_32_64:
		case COMPRESS_TO_32:
		case COMPRESS_TO_32_STR:
		{
			typename container_t::const_iterator iter = c->begin();
			while ( iter != c->end() ) {
				ElementCompression::writeWith32Rid ( *iter, fFile );
				++iter;
			}
			break;
		}
		default:
		{
			std::ostringstream errmsg;
			errmsg << "save_noncontigousCompressed incorrectly called for "
				"compression mode " << fCompressMode;
			std::cerr << errmsg.str() << std::endl;
			throw std::logic_error( errmsg.str() );
			break;
		}
	}
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::load(uint64_t setNumber, bool append)
{
	uint64_t i, count;
	std::vector<element_t> *v;
	std::set<element_t> *s;

#ifdef PROFILE
	struct timespec ts1, ts2;
	clock_gettime(CLOCK_REALTIME, &ts1);
#endif

	if (loadedSet == setNumber && phase != 0) {
		resetIterators();
		return;
	}

	/* XXXPAT: How to handle errors here?  Specifically, unless the entire load
	is successful, things will be left in a relatively undefined state.  Do we
	have to care about things like that here?  Initial guess: no. */
	try {
		DiskIoInfo info;
		if (fTraceOn) info.fStart = boost::posix_time::microsec_clock::local_time();

		// Position the file to the correct file offset for this set.
		fFile.seekg( fSetStartPositions.at(setNumber) );
		//std::cout << "Loading filename-" << fFilename <<
		//	"; set-" << setNumber << "; fPos-" <<
		//	fSetStartPositions.at(setNumber) << std::endl;

		std::streampos startPos = fFile.tellg();
		fFile.read((char *) &count, sizeof(count));
	   	//std::cout << "really slow load, count=" << count << std::endl;

		// Specific logic to handle saving of a std::vector
		if (typeid(*base::c) == typeid(std::vector<element_t>)) {
			v = reinterpret_cast<std::vector<element_t> *>(base::c);
			// @bug 721. merge all saving sets to current loaded set 0 and sort
			if (!append)
				v->resize(0);
			if (count > v->size())
				v->reserve( count );

			switch (fCompressMode)
			{
				case COMPRESS_TO_32_64:
				case COMPRESS_TO_32:
				case COMPRESS_TO_32_STR:
				{
					element_t e;
					for (i = 0; i < count; ++i) {
						ElementCompression::readWith32Rid ( e, fFile );
						v->push_back(e);
					}
					break;
				}

				default:
				{
					std::istream_iterator<element_t> it(fFile);
					for (i = 0; i < count; ++i) {
						v->push_back(*it);

						// Increment stream iterator except for last element.
						// We don't want to go past the end of this set.
						if ((i+1) < count)
							++it; // advance to next element in file
					}
					break;
				}
			}
		}
		// Specific logic to handle saving of a std::set
		else if (typeid(*base::c) == typeid(std::set<element_t>)) {
			s = reinterpret_cast<std::set<element_t> *>(base::c);
			if ( !append)
				s->clear();

			switch (fCompressMode)
			{
				case COMPRESS_TO_32_64:
				case COMPRESS_TO_32:
				case COMPRESS_TO_32_STR:
				{
					element_t e;
					for (i = 0; i < count; ++i) {
						ElementCompression::readWith32Rid ( e, fFile );
						s->insert(e);
					}
					break;
				}

				default:
				{
					std::istream_iterator<element_t> it(fFile);
					for (i = 0; i < count; ++i) {
						s->insert(*it);

						// Increment stream iterator except for last element.
						// We don't want to go past the end of this set.
						if ((i+1) < count)
							++it; // advance to next element in file
					}
					break;
				}
			}
		}
		else {
			/* this is a slow fallback.  If we need it, we should write
			a specialization for whatever container c is */
			if (!append) {
				delete base::c;
				base::c = new container_t();
			}

			switch (fCompressMode)
			{
				case COMPRESS_TO_32_64:
				case COMPRESS_TO_32:
				case COMPRESS_TO_32_STR:
				{
					element_t e;
					for (i = 0; i < count; ++i) {
						ElementCompression::readWith32Rid ( e, fFile );
						base::insert(e);
					}
					break;
				}

				default:
				{
					std::istream_iterator<element_t> it(fFile);
					for (i = 0; i < count; ++i) {
					//std::cout << "inserting " << loaded << std::endl;
						// we might want to use the derived class for inserting
						// instead but we will want to add to the base class
						// interface to support reentrancy. Possibly an
						// "insert_nolock()" would be sufficient.
						base::insert(*it);

						// Increment stream iterator except for last element.
						// We don't want to go past the end of this set.
						if ((i+1) < count)
							++it; // advance to next element in file
					}
					break;
				}
			}
		}
//  		std::cout << "... done" << std::endl;

		if (fTraceOn) {
			info.fEnd= boost::posix_time::microsec_clock::local_time();
			info.fBytes = fFile.tellg() - startPos;
			fDiskIoList.push_back(info);
		}
	}
	catch(const std::runtime_error& e) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred loading non-contiguous container from file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg + e.what());
	}
	catch(...) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred loading non-contiguous container from file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg);
	}
	resetIterators_nowait();
	loadedSet = setNumber;
	fLoadedSetCount++;

	//...Close the file once all the sets have been loaded.  We could compare
	//...setNumber to see if it is the last set, but we make no assumptions
	//...about the order in which the sets are loaded; so we instead track
	//...the number of sets loaded, and use that to know when we are done.
	if ( fLoadedSetCount == fSetStartPositions.size() )
	{
		fFile.close();
	}

#ifdef PROFILE
	clock_gettime(CLOCK_REALTIME, &ts2);
	/* What should we do with this profile info? */
#endif
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::load_contiguous(uint64_t setNumber, bool append)
{
	uint64_t count;
	//	char *buf = NULL;
	std::vector<element_t> *v;

#ifdef PROFILE
	struct timespec ts1, ts2;
	clock_gettime(CLOCK_REALTIME, &ts1);
#endif

	if (loadedSet == setNumber && phase != 0) {
		resetIterators();
		return;
	}

	/* XXXPAT: How to handle errors here?  Specifically, unless the entire load
	is successful, things will be left in a relatively undefined state.  Do we
	have to care about things like that here?  Initial guess: no. */

	try {
		DiskIoInfo info;
		if (fTraceOn) info.fStart = boost::posix_time::microsec_clock::local_time();

		v = reinterpret_cast<std::vector<element_t> *>(base::c);
		// Position the file to the correct file offset for this set.
		fFile.seekg( fSetStartPositions.at(setNumber) );
		//std::cout << "LoadingC filename-" << fFilename <<
		//	"; set-" << setNumber << "; fPos-" <<
		//	fSetStartPositions.at(setNumber) << std::endl;

		std::streampos startPos = fFile.tellg();
		fFile.read((char *) &count, sizeof(count));

		// Perform expansion of data, as it is loaded, "if" applicable
		if (fCompressMode == COMPRESS_NO_COMPRESS) {
			if (append){
				// @bug 721. append to current set for sorting purpose
				uint64_t ctn = base::c->size();
				base::c->resize(ctn+count);
				fFile.read((char *) ((v->begin()+ctn).operator->()),
					count * sizeof(element_t));
			}
			else {
				if (count != base::c->size())
					base::c->resize( count );
				fFile.read((char *) (v->begin().operator->()),
					count * sizeof(element_t));
			}
		}
		else {
			load_contiguousCompressed( append, count );
		}

		if (fTraceOn) {
			info.fEnd= boost::posix_time::microsec_clock::local_time();
			info.fBytes = fFile.tellg() - startPos;
			fDiskIoList.push_back(info);
		}
	}
	catch(const std::runtime_error& e) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred loading contiguous container from file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg + e.what());
	}
	catch(...) {
		if (fFile.is_open())
			fFile.close();
		std::string msg("Error occurred loading contiguous container from file " + fFilename + " ");
		std::cerr << msg << std::endl;
		throw logging::LargeDataListExcept(msg);
	}
	resetIterators_nowait();
	loadedSet = setNumber;
	fLoadedSetCount++;

	//...Close the file once all the sets have been loaded.  We could compare
	//...setNumber to see if it is the last set, but we make no assumptions
	//...about the order in which the sets are loaded; so we instead track
	//...the number of sets loaded, and use that to know when we are done.
	if ( fLoadedSetCount == fSetStartPositions.size() )
	{
		fFile.close();
	}

#ifdef PROFILE
	clock_gettime(CLOCK_REALTIME, &ts2);
	/* What should we do with this profile info? */
#endif

}

//------------------------------------------------------------------------------
// Load and expand contiguous data from temp file.
// append - flag that indicates whether data is to be appended to container
// count  - the number of elements to be read and expanded
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::load_contiguousCompressed(bool append, uint64_t count)
{
	std::vector<element_t>*	v =
		reinterpret_cast<std::vector<element_t> *>(base::c);
	element_t*	  pElementData = 0;

	if (append){
		uint64_t currentCount = base::c->size();
		base::c->resize(currentCount+count);
		pElementData = ((v->begin()+currentCount).operator->());
	}
	else {
		if (count != base::c->size())
			base::c->resize( count );
		pElementData = (v->begin().operator->());
	}

	//...Read in and expand the elements based on the compression mode
	switch (fCompressMode)
	{
		case COMPRESS_TO_64_32:
		{
			readContiguousCompressed<CompElement64Rid32Val>
				( count, pElementData);
			break;
		}
		case COMPRESS_TO_32_64:
		{
			readContiguousCompressed<CompElement32Rid64Val>
				( count, pElementData);
			break;
		}
		case COMPRESS_TO_32_32:
		{
			readContiguousCompressed<CompElement32Rid32Val>
				( count, pElementData);
			break;
		}
		case COMPRESS_TO_32:
		{
			readContiguousCompressed<CompElement32RidOnly>
				( count, pElementData);
			break;
		}
		default:
		{
			std::ostringstream errmsg;
			errmsg << "load_contiguousCompressed() called "
				" without compression mode being set";
			std::cerr << errmsg.str() << std::endl;
			throw std::logic_error( errmsg.str() );
			break;
		}
	}
}

//------------------------------------------------------------------------------
// Template method that reads a vector of elements of type saveElement_t
// from a temp file, and then expands to a contiguous collection of element_t.
// count - (input) number of elements to be read from temp file
// pElementData - (output) element_t data that has been read and expanded
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
template<typename saveElement_t>
void LargeDataList<container_t, element_t>::readContiguousCompressed(
	uint64_t   count,
	element_t* pElementData)
{
	//...read data from temp file into saveElement_t vector
	std::vector<saveElement_t> cLoad;
	cLoad.resize( count );
	fFile.read((char *) (cLoad.begin().operator->()),
		(sizeof(saveElement_t)*count));

	//...copy/expand data into element_t container
	ElementCompression::expand ( cLoad, pElementData );
}

template<typename container_t, typename element_t>
inline bool LargeDataList<container_t, element_t>::next(uint64_t it, element_t *e)
{
	bool ret;

	waitForConsumePhase();
	ret = base::next(it, e);
	return ret;
}

template<typename container_t, typename element_t>
inline bool LargeDataList<container_t, element_t>::next_nowait(uint64_t it, element_t *e)
{
	bool ret;
	ret = base::next(it, e);
	return ret;
}

template<typename container_t, typename element_t>
inline void LargeDataList<container_t, element_t>::waitForConsumePhase()
{
	while (phase == 0)
		consumePhase.wait(this->mutex); //pthread_cond_wait(&consumePhase, &(this->mutex));
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::registerNewSet()
{
	delete base::c;
	base::c = new container_t();
	loadedSet++;
	setCount++;
}

template<typename container_t, typename element_t>
uint64_t LargeDataList<container_t, element_t>::totalSize()
{
	waitForConsumePhase();
	return totSize;
}

template<typename container_t, typename element_t>
inline void LargeDataList<container_t, element_t>::insert(const element_t& e)
{
	totSize++;
	base::insert(e);
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::resetIterators()
{
	waitForConsumePhase();
	for (int i = 0; i < (int) base::numConsumers; ++i)
		base::cIterators[i] = base::c->begin();
}

template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::resetIterators_nowait()
{
	for (uint64_t i = 0; i < base::numConsumers; ++i)
	{
		base::cIterators[i] = base::c->begin();
	}
}

//------------------------------------------------------------------------------
// Set save element size values stored in our base class, and update the
// compression mode enumeration.
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::setDiskElemSize(
	uint32_t elementSaveSize1st, uint32_t elementSaveSize2nd )
{
	base::setDiskElemSize ( elementSaveSize1st, elementSaveSize2nd );

	// update our compression mode enumeration to reflect the save element size
	setCompressionMode();
}

//------------------------------------------------------------------------------
// Sets compression mode enumeration based on the current sizes for
// save element size1st (RID) and save element size2nd (value).
//------------------------------------------------------------------------------
template<typename container_t, typename element_t>
void LargeDataList<container_t, element_t>::setCompressionMode()
{
	const uint32_t COMPRESS_4BYTE_LENGTH = 4;
	fCompressMode = COMPRESS_NO_COMPRESS;

	uint32_t size1st = base::getDiskElemSize1st();

	if ( typeid(element_t) == typeid(RIDElementType) )
	{
		if ( size1st == COMPRESS_4BYTE_LENGTH )
		{
			fCompressMode = COMPRESS_TO_32;
		}
	}
	else if ( typeid(element_t) == typeid(StringElementType) )
	{
		if ( size1st == COMPRESS_4BYTE_LENGTH )
		{
			fCompressMode = COMPRESS_TO_32_STR;
		}
	}
	else
	{
		uint32_t size2nd = base::getDiskElemSize2nd();

		if (( size1st == COMPRESS_4BYTE_LENGTH) &&
			( size2nd != COMPRESS_4BYTE_LENGTH))
		{
			fCompressMode = COMPRESS_TO_32_64;
		}
		else
		if (( size1st != COMPRESS_4BYTE_LENGTH) &&
			( size2nd == COMPRESS_4BYTE_LENGTH))
		{
			fCompressMode = COMPRESS_TO_64_32;
		}
		else
		if (( size1st == COMPRESS_4BYTE_LENGTH) &&
			( size2nd == COMPRESS_4BYTE_LENGTH))
		{
			fCompressMode = COMPRESS_TO_32_32;
		}
	}
}

}   // namespace

#endif
