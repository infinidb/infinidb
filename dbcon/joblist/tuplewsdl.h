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
 * $Id: tuplewsdl.h 8436 2012-04-04 18:18:21Z rdempsey $
 *
 *****************************************************************************/

/** @file */

#include <vector>
#include <boost/date_time/posix_time/posix_time.hpp>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "datalist.h"
#include "elementtype.h"
#include "resourcemanager.h"

#ifndef _TUPLEWSDL_HPP_
#define _TUPLEWSDL_HPP_

/** Debug macro */
#define TW_DEBUG 0
#if TW_DEBUG
#define TDEBUG std::cout
#else
#define TDEBUG if (false) std::cout
#endif

namespace joblist {

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
 
/** @brief class TupleWSDL
 *
 * Working set datalist for TupleType
 */
class TupleWSDL : public DataList<TupleType>
{
    typedef DataList<TupleType> base;    

    public:
        enum Phase
        {
            PRODUCTION = 0,
            CONSUMPTION
        };
    
        /** @brief Constructor */
        TupleWSDL(uint64_t numConsumers, uint64_t maxSize, ResourceManager& rm);
        
        /** @brief Destructor */
        virtual ~TupleWSDL();
        
        /** @brief insert to datalist
         *
         * @param e reference of insert tuple element
         */        
        inline virtual void insert( const TupleType &e);
        
        /** @brief insert to datalist without locking
         *
         * @param e reference of insert tuple element
         */
        //void insert_nolock(const TupleType &e)
        inline void insert_nolock(TupleType &e);
        
        /** @brief insert a vector of e to datalist
         *
         * @param v reference of insert tuple vector
         */        
        inline virtual void insert(const std::vector<TupleType> &v);
            
        /** @brief get iterator for consumer
         *
         * @return consumer id
         */            
        virtual uint64_t getIterator();
        
        /** @brief get next element from datalist
         *
         * @param it consumer id
         * @param e pointer of read back tuple 
         */        
        inline virtual bool next(uint64_t id, TupleType *e);
        
        /** @brief end of input to datalist */        
        virtual void endOfInput();
        
        /** @brief set number of consumers */  
        void setNumConsumers(uint);
        
        /** @brief get number of consumers */
        uint getNumConsumers() const { return fNumConsumers; }
        
        /** @brief reset number of consumers */
        void resetNumConsumers(uint numConsumers);
        
        /** @brief return datalist total size */
        virtual uint64_t totalSize(); 
            
        /** @brief set multiple producers flag */
        virtual void setMultipleProducers(bool m) {fMultipleProducers = m;}
        
        /** @brief return the number of temp files generated */
        uint64_t numberOfTempFiles() const; 
               
        /** @brief return tuple size (rid+data) */
        const uint64_t tupleSize() const {return fTupleSize;}
        
        /** @set tuple size */
        void tupleSize(const uint64_t ridSize, const uint64_t dataSize);
        
        /** @brief return initial set count before merges */
        uint64_t initialSetCount() const { return fInitialSetCnt; }
        
        /** @brief return the save size of datalist. */
        uint64_t saveSize();

		/** @brief returns the disk I/O time in seconds */
		bool totalDiskIoTime(uint64_t& w, uint64_t& r);

		/** @brief returns the reference of the disk I/O info list */
		std::list<DiskIoInfo>& diskIoInfoList();
		    
		/** @brief set trace flag */
		void traceOn(bool b) { fTraceOn = b; }
        
        /** @brief get diskiolist */
        std::list<DiskIoInfo>& diskIoList() { return fDiskIoList; }
    protected:
        /** @brief reset read offset for all consumers */
        void resetIterators();
        
        /** @brief delete datalist data structure */
        void shrink();
        
        /** @brief load set from disk to memory 
         * @param setNumber the set number being loaded
         * @param append append to in memory set or not (ZDL set append true)
         */
        void load(uint64_t setNumber, bool append = false);
        
        /** @brief save the in memory set to disk */
        uint64_t save();
        
        /** @brief register a new saved set by bookkeeping */      
        void registerNewSet();
        
        /** @brief get save file name */
        std::string getFilename();
            
        /** @brief wait until consumption phase starts */
        void waitForConsumePhase();
        
        /** @brief read next tuple from the byte array 
         * @param it iterator id
         * @param e pointer of read back tuple
         */
        inline bool read(uint64_t id, TupleType *e);
        
        /** @brief create a temp file to save */
        void createTempFile();
        
        /** @brief remove all temp filess of this datalist */
        void removeFile();
               
    private:
        /** @brief constructor/assignment op for completeness */
        explicit TupleWSDL();
        TupleWSDL(const TupleWSDL &dl);
        TupleWSDL& operator=(const TupleWSDL &dl);        

	ResourceManager& fRm;        
        char*       fData;
        uint64_t*   fIters;
        uint64_t    fNumConsumers;
        uint64_t    fMaxSize;           // max size in array before saving
        uint64_t    fItIndex;
        uint64_t    fCapacity;          // array pre-allocated size
        uint64_t    fCurPos;            // in memory array current write offset
        uint64_t    fRidSize;
        uint64_t    fDataSize;
        uint64_t    fTupleSize;
        uint64_t    fTotSize;
        bool        fMultipleProducers;
        
        uint64_t    fLoadedSet;
        uint64_t    fSetCount;
        uint64_t    fInitialSetCnt; 
        uint64_t    fLoadedSetCount;  
        uint64_t    fFilenameCounter;         
        uint64_t    fSaveSize;      
        std::string fPath;
        std::string fFilename;                                
        std::fstream fFile;               
        std::vector<std::fstream::pos_type> fSetStartPositions;
        
        int64_t     fPhase; 
        uint64_t    fWaitingConsumers;
		boost::condition fNextSetLoaded;		
		boost::condition fConsumePhase;	

		std::list<DiskIoInfo> fDiskIoList;
		bool fTraceOn;

};

void TupleWSDL::insert( const TupleType &e)
{
    if (fMultipleProducers) 
        base::lock();     	
    insert_nolock(const_cast<TupleType&>(e));
    if (fMultipleProducers)
        base::unlock();
}

void TupleWSDL::insert_nolock(TupleType &e)
{ 
    // lazy allocation
    if (!fData)
        fData = new char [fCapacity];
    
    // check if need to save
    if (fMaxSize - fCurPos < fTupleSize)
    {
        TDEBUG << "TupleWSDL-" << this << " full and save: " << fCurPos << std::endl;
        fSaveSize += save();
        registerNewSet();
        TDEBUG << "setcount=" << fSetCount << std::endl;
        fCurPos = 0;		
    }
    
    // check if need expand memory
    else if (fCapacity - fCurPos < fTupleSize)
    {
        fCapacity *= 2;
        TDEBUG << "TupleWSDL-" << this << " expand array to " << fCapacity << std::endl;
        char *tmp = new char[fCapacity];
        memcpy(tmp, fData, fCurPos);
        fData = tmp;
    }
    
    // insert
    fTotSize++;
    switch(fRidSize)
    {
        case 4:
            memcpy(fData + fCurPos, &e.first, 4);
            break;
        case 8:
            memcpy(fData + fCurPos, &e.first, 8);
            break;
        default:
            memcpy(fData + fCurPos, &e.first, fRidSize);
    }
    memcpy(fData + fCurPos + fRidSize, e.second, fDataSize);
    fCurPos += fTupleSize;	 
    e.deleter();
}

bool TupleWSDL::next(uint64_t id, TupleType *e)
{
    bool ret, locked = false;
    uint64_t nextSet;

    if (fPhase == PRODUCTION) {
        locked = true;
        base::lock();
        waitForConsumePhase();
    }

    ret = read(id, e);

    if (ret == false && (fLoadedSet < fSetCount - 1)) 
    {
        if (!locked && (fNumConsumers > 1 || fPhase == PRODUCTION)) 
        {
            locked = true;
            base::lock();
        }
        if (fLoadedSetCount == 0)
        {
            nextSet = 0;
            fSetCount--;
        }
        else
            nextSet = fLoadedSet + 1;

        fWaitingConsumers++;
        if (fWaitingConsumers < fNumConsumers)
        {
            while (nextSet != fLoadedSet || fLoadedSetCount == 0) 
            {
                fNextSetLoaded.wait(this->mutex); //pthread_cond_wait(&fNextSetLoaded, &(this->mutex));
            }
        }
        else 
        {
            load(nextSet);
            fNextSetLoaded.notify_all(); //pthread_cond_broadcast(&fNextSetLoaded);
        }
        fWaitingConsumers--;
        ret = read(id, e);
    }

    if (ret == false && ++base::consumersFinished == fNumConsumers) 
    {	
        shrink();
        removeFile();
    }
    if (locked)
        base::unlock();
        
    return ret;
}

bool TupleWSDL::read(uint64_t id, TupleType *e)
{     
    if ( !fData || fIters[id] == fCurPos )
        return false;
        
    e->first = 0;
    switch (fRidSize)
    {
        case 4:
            memcpy(&e->first, fData+fIters[id], 4);
            break;
        case 8:
            memcpy(&e->first, fData+fIters[id], 8);
            break;
        default:
            memcpy(&e->first, fData+fIters[id], fRidSize);
    }

    e->second = fData + fIters[id] + fRidSize;
    fIters[id] += fTupleSize;
    return true;
}

}   //namespace

#endif
