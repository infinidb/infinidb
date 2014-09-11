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

/******************************************************************************
 * $Id: tuplewsdl.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *****************************************************************************/

#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "tuplewsdl.h"
#include "bucketreuse.h"
#include "configcpp.h"

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

using namespace std;
using namespace config;

namespace joblist {
extern const string defaultTempDiskPath;          // defined in bucketreuse.cpp
  //const uint64_t defaultInitialCapacity = 1 * 1024*1024;     // 1M

TupleWSDL::TupleWSDL(uint64_t nm, uint64_t maxSize, ResourceManager& rm) : 
						DataList<TupleType>(), 
						fRm(rm),
						fData(0), 
						fNumConsumers(nm),
						fMaxSize(maxSize),
						fItIndex(0),
						fCapacity(rm.getTwInitialCapacity()),
						fCurPos(0),
						fRidSize(0),
						fDataSize(0),
						fTupleSize(0),
						fTotSize(0),
						fMultipleProducers(false),
						fLoadedSet(0),
						fSetCount(1),
						fInitialSetCnt(0),
						fLoadedSetCount(0),
						fFilenameCounter(1),
						fSaveSize(0),
						fPath(rm.getScTempDiskPath()),
						fPhase(PRODUCTION),
						fWaitingConsumers(0),
						fTraceOn(false)
{
//     config::Config *config = config::Config::makeConfig();
//     fPath = config->getConfig("SystemConfig", "TempDiskPath");
//     if (fPath.length() == 0)
//         fPath = defaultTempDiskPath;
//       
//     string capacity;
//     capacity = config->getConfig("TupleWSDL", "InitialCapacity");
//     if (capacity.length() == 0)
//         fCapacity = defaultInitialCapacity;
//     else
//         fCapacity = Config::uFromText(capacity);
    
    //pthread_cond_init(&fConsumePhase, NULL);
    //pthread_cond_init(&fNextSetLoaded, NULL);
    
    fIters = new uint64_t [fNumConsumers];
    TDEBUG << "TupleWSDL: " << "capacity=" << fCapacity << " maxsize=" << fMaxSize << endl;
}

TupleWSDL::~TupleWSDL()
{ 
    delete [] fData;
    delete [] fIters;
    //pthread_cond_destroy(&fConsumePhase);
    //pthread_cond_destroy(&fNextSetLoaded);
    removeFile();
}

void TupleWSDL::setNumConsumers(uint nc)
{
    resetNumConsumers(nc);
}

void TupleWSDL::resetNumConsumers(uint nc)
{
    if (fItIndex != 0)
        throw logic_error("TupleWSDL::resetNumConsumers(): attempt to change numConsumers "
            "after iterators have been issued");
    fNumConsumers = nc;
    delete [] fIters;
    fIters = new uint64_t [fNumConsumers];
}

uint64_t TupleWSDL::getIterator()
{
    if (fItIndex >= fNumConsumers)
    {
        ostringstream oss;
        oss << "getIterator::getIterator(): caller attempted to grab too many iterators: " <<
            "have " << fNumConsumers << " asked for " << (fItIndex + 1);
        throw logic_error(oss.str().c_str());
    }
    fIters[fItIndex] = 0;
    return fItIndex++;
}

void TupleWSDL::insert(const vector<TupleType> &v)
{
    throw logic_error("TupleWSDL insert vector is not implemented");
}
    
//-----------------------------------------------------------------------
// This is different from wsdl logic. This will not save the in-memory
// set at end of input, no matter this is the only set or not. 
//-----------------------------------------------------------------------
void TupleWSDL::endOfInput()
{
    base::lock();
    fInitialSetCnt = fSetCount;
    if (fLoadedSet > 0)
    {
        fLoadedSet = 0;
    }
    
    resetIterators();	
    
    base::endOfInput();
    fPhase = CONSUMPTION;
    fConsumePhase.notify_all(); //pthread_cond_broadcast(&fConsumePhase);
    base::unlock();
}

void TupleWSDL::tupleSize (const uint64_t ridSize, const uint64_t dataSize)
{
    fRidSize = ridSize;
    fDataSize = dataSize;
    fTupleSize = ridSize + dataSize;
    TDEBUG << "<<<RidSize=" << fRidSize << " DataSize=" << fDataSize << endl;
}

void TupleWSDL::resetIterators()
{
    for (uint64_t i = 0; i < fNumConsumers; i++)
        fIters[i] = 0;
}

void TupleWSDL::shrink()
{
    delete [] fData;
    fData = 0;
}

inline void TupleWSDL::removeFile()
{
    if (!fFilename.empty())
    {
        unlink(fFilename.c_str());
        fFilename = "";
    }
}

string TupleWSDL::getFilename()
{
    stringstream o;
    o << fPath << "/Tuple-0x" << hex << (ptrdiff_t)this << dec << "-" << fFilenameCounter++;
    return o.str();
}

//------------------------------------------------------------------------------
// Return the number of temporary files created by this datalist.
// With the current implementation, there will only be 1 file, but this method
// will help protect the application code from knowing this, in case we
// want to change the implementation to sometimes output more than 1 file.
//------------------------------------------------------------------------------
uint64_t TupleWSDL::numberOfTempFiles() const
{
    return  ((!fSetStartPositions.empty()) ? 1 : 0);
}

//-------------------------------------------------------------------------------
// Create and open the temporary Tuple file we will be saving data to.  With
// the current implementation, we are creating a single file to hold all the
// sets for a TupleWSDL.
// exceptions: runtime_error thrown if file creation or file open fails
//--------------------------------------------------------------------------------
void TupleWSDL::createTempFile()
{
    int64_t fd;
    /* Is there a good way to do this through an fstream? */
    do {
        fFilename = this->getFilename();
        fd = open(fFilename.c_str(), O_CREAT | O_RDWR | O_EXCL | O_BINARY, 0666);
    } while (fd < 0 && errno == EEXIST);
    
    if (fd < 0)
    {
        ostringstream errmsg;
        errmsg << "TupleWSDL::createTempFile(): could not save to disk ("
            << errno << ") - " << strerror(errno);
        cerr << errmsg.str() << endl;
        throw runtime_error(errmsg.str());
    }
    
    close(fd);
    
    //cout << "Creating/opening file: " << fFilename << endl;
    fFile.open(fFilename.c_str(), ios_base::in | ios_base::out | ios_base::binary );
    if (!(fFile.is_open())) 
    {
        perror("open");
        throw runtime_error("LDL: open() failed");
    }
}


uint64_t TupleWSDL::save() 
{
    uint64_t nBytesWritten=0;

    try {
        //...Create the temporary LDL file (if necessary)
        if ( fFilename.empty() )
            createTempFile();
        
        // Save our file offset for this set, to be used when we load the set.
        fstream::pos_type firstByte = fFile.tellp();
        fSetStartPositions.push_back( firstByte );
        TDEBUG << "Saving " << fFilename << "; count-" << fCurPos  <<
            "; tellp-" << fFile.tellp() << endl;

		DiskIoInfo info(true);
		if (fTraceOn) info.fStart = boost::posix_time::microsec_clock::local_time();

        // write size
        fFile.write((char*) &fCurPos, sizeof(fCurPos));
        
        // write data
        fFile.write(fData, fCurPos);
        nBytesWritten = fFile.tellp() - firstByte;
        fCurPos = 0;

		if (fTraceOn)
		{
			info.fEnd= boost::posix_time::microsec_clock::local_time();
			info.fBytes = nBytesWritten;
			fDiskIoList.push_back(info);
		}
    }
    catch(...) 
    {
        delete [] fData;
        if (fFile.is_open())
            fFile.close();
        cerr << "Error occurred saving contiguous Tuple array "
            "into file " << fFilename << endl;
        throw;
    }

    return nBytesWritten;
}

void TupleWSDL::load(uint64_t setNumber, bool append)
{
    uint64_t prevPos = fCurPos;

    if (fLoadedSetCount != 0 && fLoadedSet == setNumber && fPhase != PRODUCTION) 
    {
        resetIterators();
        return;
    }

    try 
    {
        // Position the file to the correct file offset for this set.
//        fFile.seekg( fSetStartPositions.at(setNumber) );
        if (setNumber == 0)
            fFile.seekg(0);
            
        TDEBUG << "Loading filename-" << fFilename <<
          "; set-" << setNumber << "; fPos-" << 
          fSetStartPositions.at(setNumber) << endl;

		DiskIoInfo info;
		if (fTraceOn) info.fStart = boost::posix_time::microsec_clock::local_time();
		streampos startPos = fFile.tellg();

        fFile.read((char *) &fCurPos, sizeof(fCurPos));		
        
        if (append)
        {           
            if (fCapacity - prevPos < fCurPos)
            {
                char* tmp = new char[fCapacity*2];
                memcpy(tmp, fData, fCapacity);
                fData = tmp;
                fCapacity *= 2;
            } 
            fFile.read(fData, fCurPos);
            fCurPos += prevPos;
        }
        else 
        {
            // array should always hold saved sets becuase they're in there before
            fFile.read(fData, fCurPos);
        }

		if (fTraceOn)
		{
			info.fEnd= boost::posix_time::microsec_clock::local_time();
			info.fBytes = fFile.tellg() - startPos;
			fDiskIoList.push_back(info);
		}
    }
    catch (...) 
    {
        delete [] fData;
        fFile.close();
        cerr << "Error occurred loading contiguous container "
            "from file " << fFilename << " vector size=" << fSetStartPositions.size() << endl;
        throw;
    }
    resetIterators();
    fLoadedSet = setNumber;
    fLoadedSetCount++;
    
    //...Close the file once all the sets have been loaded.  
    if ( fLoadedSetCount == fSetStartPositions.size() )
    {
        fFile.close();
    }
}

inline void TupleWSDL::waitForConsumePhase()
{
    base::lock();

    while (fPhase == PRODUCTION)
    {
        fConsumePhase.wait(this->mutex); //pthread_cond_wait(&fConsumePhase, &(this->mutex));
    }
        base::unlock();
}

void TupleWSDL::registerNewSet()
{
    fLoadedSet++;
    fSetCount++;
}

uint64_t TupleWSDL::totalSize()
{ 
    waitForConsumePhase();
    return fTotSize; 
}

uint64_t TupleWSDL::saveSize()
{ 
    waitForConsumePhase();
    return fSaveSize; 
}

bool TupleWSDL::totalDiskIoTime(uint64_t& w, uint64_t& r)
{
	boost::posix_time::time_duration wTime(0,0,0,0);
	boost::posix_time::time_duration rTime(0,0,0,0);
	bool diskIo = false;

	list<DiskIoInfo>::iterator k = fDiskIoList.begin();
	while (k != fDiskIoList.end())
	{
		if (k->fWrite == true)
			wTime += k->fEnd - k->fStart;
		else
			rTime += k->fEnd - k->fStart;
		k++;
	}

	w = wTime.total_seconds();
	r = rTime.total_seconds();

	return diskIo;
}

list<DiskIoInfo>& TupleWSDL::diskIoInfoList()
{
	return fDiskIoList;
}

}   //namespace

