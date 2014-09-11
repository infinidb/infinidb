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

/***********************************************************************
 *   $Id: pidxwalk.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *
 ***********************************************************************/
using namespace std;
#include "pidxwalk.h"
#include "distributedenginecomm.h"
#include "elementtype.h"
#include "unique32generator.h"

#include "messagequeue.h"
using namespace messageqcpp;
#include "configcpp.h"
using namespace config;

using namespace logging;

using namespace execplan;

#include "brm.h"
using namespace BRM;

#ifdef PROFILE
void itimespec_sub(const struct timespec &tv1, const struct timespec &tv2,
		  struct timespec &diff) 
{
  if (tv2.tv_nsec < tv1.tv_nsec) 
  {
    diff.tv_sec = tv2.tv_sec - tv1.tv_sec - 1;
    diff.tv_nsec = tv1.tv_nsec - tv2.tv_nsec;
  }
  else 
  {
    diff.tv_sec = tv2.tv_sec - tv1.tv_sec;
    diff.tv_nsec = tv2.tv_nsec - tv1.tv_nsec;
  }
}
#endif

namespace joblist
{
  const size_t SEARCH_STRING_SIZE = 2;
  const size_t ELEMENTS_PER_MESSAGE = (MAX_BUFFER_SIZE - sizeof(IndexWalkHeader))/sizeof(uint64_t);

  struct pIdxWalkPrimitive
  {
    pIdxWalkPrimitive(pIdxWalk* ptr) : fpIdxWalk(ptr)   {}
    pIdxWalk*  fpIdxWalk;
    void operator()()
    {
      try
      {
	  fpIdxWalk->sendPrimitiveMessages();
      }
      catch(runtime_error& re)
        { 
		catchHandler(re.what());
	}
      catch(...)
        { 
		catchHandler("pIdxWalk send caught an unknown exception");
	}    
     }
  };

  struct pIdxWalkAggregator
  {
    pIdxWalkAggregator(pIdxWalk* ptr) : fpIdxWalk(ptr)
    {}
    pIdxWalk* fpIdxWalk;
    void operator()()
    {
      try
      {
	  fpIdxWalk->receivePrimitiveMessages();
      }
      catch(runtime_error& re)
        { 
		catchHandler(re.what());
	}
      catch(...)
        { 
		catchHandler("pIdxWalk receive caught an unknown exception");
	}    
    }
  };


  pIdxWalk::pIdxWalk(const JobStepAssociation& inputJobStepAssociation,
		     const JobStepAssociation& outputJobStepAssociation,
		     DistributedEngineComm* dec,
		     CalpontSystemCatalog* cat,
		     CalpontSystemCatalog::OID idxTreeOid,
			 execplan::CalpontSystemCatalog::OID columnOid,
			 execplan::CalpontSystemCatalog::OID tableOid,
		     uint32_t session,
		     uint32_t txn,
		     uint32_t verID,
		     uint16_t step,
		     uint32_t statement) :
    fInputJobStepAssociation(inputJobStepAssociation),
    fOutputJobStepAssociation(outputJobStepAssociation),
    fDec(dec),
    fSysCat(cat),
    fOid(idxTreeOid),
	fColumnOid(columnOid),
	fTableOid(tableOid),
    fSessionId(session),
    fTxnId(txn),
    fVerId(verID),
    fStepId(step),
    fStatementId(statement),
    fBop(BOP_NONE),
    fColType(),
    fLBID(0),
    fMsgsSent(0),
    fMsgsRecvd(0),
    fFinishedSending(false),
    fRecvWaiting(false),
    fSearchStrings(),
    fCop1(COMPARE_EQ),
    fCop2(COMPARE_NIL),
    fConsumerThread(),
    fProducerThread(),
    fMutex(),
    fCondvar(),
    fFilterCount(0)
  {
    DBRM dbrm;
    fColType = fSysCat->colType(fColumnOid);

    bool err = dbrm.lookup(fOid, 0, fLBID);
    if (err)
      throw runtime_error("pIdxWalk: BRM error!");

	uniqueID = Unique32Generator::instance()->getUnique32();
	if (fDec)
		fDec->addQueue(uniqueID);
  }

pIdxWalk::~pIdxWalk()
{
	if (fDec)
		fDec->removeQueue(uniqueID);
}

  void pIdxWalk::startPrimitiveThread()
  {
    fProducerThread.reset(new boost::thread(pIdxWalkPrimitive(this)));
  }

  void pIdxWalk::startAggregationThread()
  {
    fConsumerThread.reset(new boost::thread(pIdxWalkAggregator(this)));
  }

  void pIdxWalk::run()
  {
#ifdef PROFILE
    clock_gettime(CLOCK_REALTIME, &fTs1);
#endif
	if (traceOn())
	{
		syslogStartStep(16,           // exemgr subsystem
			std::string("pIdxWalk")); // step name
	}

    pthread_mutex_init(&fMutex, NULL);
    pthread_cond_init(&fCondvar, NULL);
    startPrimitiveThread();
    startAggregationThread();
  }

  void pIdxWalk::join()
  {
    fProducerThread->join();
    fConsumerThread->join();
    pthread_mutex_destroy(&fMutex);
    pthread_cond_destroy(&fCondvar);
  }


  void pIdxWalk::addSearchStr(int8_t cop, int64_t value)
  {
    if (SEARCH_STRING_SIZE > fSearchStrings.size())
    {
    	if (fSearchStrings.empty())
      		fCop1 = cop;
	else 	fCop2 = cop;

    	fSearchStrings.push_back(value);
    }
    else
    {
#ifdef DEBUG
	    cout << "IdxWalk only allows " << SEARCH_STRING_SIZE << " search strings\n";
#endif
	;
    }
  }


  void pIdxWalk::makeIndexHeader(IndexWalkHeader& indexHdr)
  {
    indexHdr.ism.Reserve=0;
    indexHdr.ism.Flags=planFlagsToPrimFlags(fTraceFlags);
    indexHdr.ism.Command=INDEX_WALK;
    indexHdr.ism.Size=sizeof(IndexWalkHeader);
    indexHdr.ism.Type=CMD;

    // Init the Index List Structure
    indexHdr.Hdr.SessionID = fSessionId;
    indexHdr.Hdr.StatementID = 0;
    indexHdr.Hdr.TransactionID = fTxnId;
    indexHdr.Hdr.VerID = fVerId;
    indexHdr.Hdr.StepID = fStepId;

    indexHdr.LBID = fLBID;
    assert(indexHdr.LBID >= 0);
   
    indexHdr.SSlen = fColType.colWidth * 8;
    indexHdr.Shift = 0;
    indexHdr.BOP = fBop;
    indexHdr.COP1 = fCop1;
    indexHdr.COP2 = fCop2;
    indexHdr.State = 0;
    indexHdr.NVALS = (uint16_t)(fSearchStrings.size() > SEARCH_STRING_SIZE ? SEARCH_STRING_SIZE : fSearchStrings.size()); 

    indexHdr.SubBlock = 1;
    indexHdr.SBEntry = 0;

    for (size_t i = 0; i < indexHdr.NVALS; ++i)
      	indexHdr.SearchString[i] = fSearchStrings[i];
    fSearchStrings.clear();
    fCop1 = COMPARE_EQ;
    fCop2 = COMPARE_NIL;

  }

  void pIdxWalk::sendError(uint16_t status)
  {
	fOutputJobStepAssociation.status(status);

	uint16_t size =  sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader);
	ISMPacketHeader ism; 
	ism.Command  = INDEX_WALK;
        ism.Size  = size;
	ism.Type  = 2;
	ism.Status = status;
	PrimitiveHeader ph = {fSessionId, 0, fTxnId, fVerId, fStepId, uniqueID };
	ByteStream primMsg(size);
	primMsg.load((const uint8_t *) &ism, sizeof(ism));
	primMsg.append((const uint8_t *) &ph, sizeof(ph));
	fDec->write(primMsg);
	updateMsgSent();
  }

	
  void pIdxWalk::updateMsgSent()
  {
	    pthread_mutex_lock(&fMutex);
	    fMsgsSent++;
	    if (fRecvWaiting)
	      pthread_cond_signal(&fCondvar);
#ifdef DEBUG
	    cout << "IndexWalk msgsSent++ = " << fMsgsSent << endl;
#endif
	    pthread_mutex_unlock(&fMutex);
  }

  void pIdxWalk::sendPrimitiveMessages()
  {

try
{
    if (0 < fInputJobStepAssociation.status() )
    {
	sendError(fInputJobStepAssociation.status());
    }
    else
    {

    IndexWalkHeader indexHdr;
    makeIndexHeader(indexHdr);

    ByteStream primMsg(MAX_BUFFER_SIZE);
    ByteStream msgElements;

    if (fInputJobStepAssociation.outSize() )
    {
   	ZonedDL* tokenList = fInputJobStepAssociation.outAt(0)->zonedDL(); 
   	int it = tokenList->getIterator();
    	ElementType e;
     	bool more = tokenList->next(it, &e);  
	//if there are 2 or less tokens use the SearchString, otherwise add after header.
      if (SEARCH_STRING_SIZE >= tokenList->totalSize() )
      {
	while (more )
	{
	      indexHdr.SearchString[indexHdr.NVALS++] = e.second;  
	      more = tokenList->next(it, &e);
	}
	indexHdr.COP2 = COMPARE_EQ;
      }
      else
     	while ( more )   
	  {
	    if (ELEMENTS_PER_MESSAGE > indexHdr.NVALS)
	      {
		msgElements << e.second;
		indexHdr.NVALS++;
		more = tokenList->next(it, &e);
	      }
	    else
	      {	    
		indexHdr.ism.Size =+ indexHdr.NVALS * 8;  

		primMsg.load((const uint8_t *) &indexHdr, sizeof(IndexWalkHeader));
		primMsg += msgElements;
		fDec->write(primMsg);
		updateMsgSent();

		makeIndexHeader(indexHdr);  
		primMsg.reset();
		msgElements.reset();
	      }
	  }
      }
 
    primMsg.load((const uint8_t *) &indexHdr, sizeof(IndexWalkHeader));
    if (SEARCH_STRING_SIZE < indexHdr.NVALS)
      {
	indexHdr.ism.Size =+ indexHdr.NVALS * 8;
	primMsg += msgElements;
      }
    fDec->write(primMsg);
    updateMsgSent();
   } //else fInputJobStepAssociation.status() == 0
 }  //try
catch(const exception& e)
{
	catchHandler(e.what());
	sendError(pIdxWalkErr);
}
catch(...)
{
	catchHandler("pIdxWalk send caught an unknown exception.");
	sendError(pIdxWalkErr);
}

    pthread_mutex_lock(&fMutex);
    fFinishedSending = true;
    if (fRecvWaiting)
      pthread_cond_signal(&fCondvar);
    pthread_mutex_unlock(&fMutex);
  }


   bool pIdxWalk::syncWithSend()
   {
      pthread_mutex_lock(&fMutex);
      while (!fFinishedSending && fMsgsSent == fMsgsRecvd) 
	{
	  fRecvWaiting = true;

	  pthread_cond_wait(&fCondvar, &fMutex);

	  fRecvWaiting = false;
	}
      if (fMsgsSent == fMsgsRecvd) 
	{
	  pthread_mutex_unlock(&fMutex);
	  return true;
	}
      fMsgsRecvd++;
#ifdef DEBUG
      cout << "IndexWalk msgsRecvd++ = " << fMsgsRecvd << endl;
#endif
      pthread_mutex_unlock(&fMutex);
      return false;
 }

  void pIdxWalk::receivePrimitiveMessages()
  {
    AnyDataListSPtr dl = fOutputJobStepAssociation.outAt(0);
    ZonedDL* dlp = dl->zonedDL();
    int64_t results = 0;
  try
    {
      uint16_t error = fOutputJobStepAssociation.status();
    while (! error) 
    {
      if (syncWithSend())
	break;
			
      // read the results
      ByteStream bs = fDec->read(uniqueID);
      if (bs.length() == 0) break;

#ifdef DEBUG
      cout << "IdxWalk: Got an IndexResultHeader!: " << bs.length() << " bytes" << endl;
#endif

      const ByteStream::byte* bsp = bs.buf();
      const ISMPacketHeader*  ism = reinterpret_cast<const ISMPacketHeader*>(bsp);
     if (0 == (error = ism->Status))
     {
      bsp += sizeof(ISMPacketHeader);
      const IndexResultHeader*  resultHdr = reinterpret_cast<const IndexResultHeader*>(bsp);
      uint16_t nvals = resultHdr->NVALS;
      bsp += sizeof(IndexResultHeader);
      // The rest of the bytestream is returned data from the primitive server.
      // If it has been shifted all the way it is complete and the results are
      // put into the output as an IndexListParam, otherwise it is 
      // sent back to the primitives with an INDEX_WALK command.
      ByteStream primMsg;
      ByteStream primMsgParam;
#ifdef DEBUG
      
	cout << "  IdxWalk: returned values = " << nvals << endl;
#endif
       IndexListParam listParam;
       listParam.type = 0;
       listParam.spare = 0;

      for(uint16_t j = 0; j < nvals; j++)
      {
	  const IndexWalkHeader* walk = reinterpret_cast<const IndexWalkHeader*>(bsp);
		
	  //This one is finished 
	  if (walk->Shift >= walk->SSlen)
	  {
	      listParam.fbo = walk->LBID;
	      listParam.sbid = walk->SubBlock;
	      listParam.entry = walk->SBEntry;
		//what about an rid?
	      dlp->insert(ElementType(0, *reinterpret_cast<uint64_t*>(&listParam)));
	      bsp += sizeof(IndexWalkHeader);
	      if (SEARCH_STRING_SIZE < walk->NVALS)
		bsp += walk->NVALS * 8;		
	      pthread_mutex_lock(&fMutex); 
	      ++results;
	      pthread_mutex_unlock(&fMutex);
	  }
	  else
	    //This one is sent back to the primitives with its data and an INDEX_WALK command
	  {
	      int len = sizeof(IndexWalkHeader);
	      if (2 < walk->NVALS)
		len += (walk->NVALS * 8);  
	      //We need the header and the data behind it. 
	      char*  buffer = new char[len];
	      memcpy(buffer, (char*)walk, len);
	      reinterpret_cast<IndexWalkHeader*>(buffer)->ism.Command = INDEX_WALK;
	      primMsg.load((const uint8_t*)buffer, len );
	      fDec->write(primMsg);
	      bsp += len;
	      delete [] buffer;
	      updateMsgSent();
	  }
	} //for
      } // if error == 0
    }  //while
} //try

  catch(const exception& e)
  {
      catchHandler(e.what());
      fOutputJobStepAssociation.status(pIdxWalkErr);
  }
  catch(...)
  {
      catchHandler("pIdxWalk receive caught an unknown exception.");
      fOutputJobStepAssociation.status(pIdxWalkErr);
  }

      dlp->endOfInput();

		fDec->removeQueue(uniqueID);

      if (traceOn())
      {
        syslogEndStep(16, // exemgr subsystem
            0,            // no blocked datalist input  to report
            0);           // no blocked datalist output to report
	  }
		
#ifdef PROFILE
      timespec diff;
      clock_gettime(CLOCK_REALTIME, &fTs2);
      itimespec_sub(fTs1, fTs2, diff);
      cout << "pIdxWalk execution stats:" << endl;
      cout << "  Primitive msgs sent & recvd: " << fMsgsSent << endl;
      cout << "  # of data results returned: " << results << endl;
      cout << "  total runtime: " << diff.tv_sec << "s " << diff.tv_nsec <<
	"ns" << endl;
#endif
    }

    const string pIdxWalk::toString() const
    {
      ostringstream oss;
	oss << "pIdxWalk    ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/col/idxTree:" << fTableOid << "/" << fColumnOid << "/" << fOid;
	oss << " " << omitOidInDL
		<< fOutputJobStepAssociation.outAt(0) << showOidInDL;
	oss << " nf:" << fFilterCount;
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
	return oss.str();
    }

	void pIdxWalk::addFilter(int8_t COP, float value)
	{
		fFilterString << (uint8_t) COP;
		fFilterString << *((uint32_t *) &value);
		fFilterCount++;
	}

	void pIdxWalk::addFilter(int8_t COP, int64_t value)
	{
		int8_t tmp8;
		int16_t tmp16;
		int32_t tmp32;

		fFilterString << (uint8_t) COP;

		// converts to a type of the appropriate width, then bitwise
		// copies into the filter ByteStream
	switch(fColType.colWidth) {
		case 1:	
			tmp8 = value;	
			fFilterString << *((uint8_t *) &tmp8); 
			break;
		case 2: 
			tmp16 = value;
			fFilterString << *((uint16_t *) &tmp16); 
			break;
		case 4: 
			tmp32 = value;
			fFilterString << *((uint32_t *) &tmp32); 
			break;
		case 8: 
			fFilterString << *((uint64_t *) &value); 
			break;
		default:
			ostringstream o;

			o << "pColScanStep: CalpontSystemCatalog says OID " << fOid << 
				" has a width of " << fColType.colWidth;
			throw runtime_error(o.str());
	}
	fFilterCount++;
}
  }   //namespace


