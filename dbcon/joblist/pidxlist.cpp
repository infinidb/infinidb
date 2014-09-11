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
 *   $Id: pidxlist.cpp 7409 2011-02-08 14:38:50Z rdempsey $
 *
 *
 ***********************************************************************/
using namespace std;
#include "pidxlist.h"
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
void ltimespec_sub(const struct timespec &tv1, const struct timespec &tv2,
		  struct timespec &diff) 
{
  if (tv2.tv_nsec < tv1.tv_nsec) {
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

  const size_t ELEMENTS_PER_MESSAGE = (MAX_BUFFER_SIZE - sizeof(IndexListHeader))/sizeof(uint64_t);

  struct IndexElementComparer
  {
    bool operator() (const IndexListParam& lhs, const IndexListParam& rhs) const
    {
      return (lhs.fbo < rhs.fbo);
    }
  };

  struct pIdxListPrimitive
  {
    pIdxListPrimitive(pIdxList* ptr) : fpIdxList(ptr)   {}
    pIdxList*  fpIdxList;
    void operator()()
    {
      try
        {
	  fpIdxList->sendPrimitiveMessages();
        }
      catch(runtime_error& re)
        { 
		catchHandler(re.what());
	}
      catch(...)
        { 
		catchHandler("pIdxList send caught an unknown exception");
	}

    }
  };

  struct pIdxListAggregator
  {
    pIdxListAggregator(pIdxList* ptr) : fpIdxList(ptr)
    {}
    pIdxList* fpIdxList;
    void operator()()
    {
      try
        {
	  fpIdxList->receivePrimitiveMessages();
        }
       catch(runtime_error& re)
        { 
		catchHandler(re.what());
	}
      catch(...)
        { 
		catchHandler("pIdxList receive caught an unknown exception");
	}
    }
  };


  pIdxList::pIdxList(const JobStepAssociation& inputJobStepAssociation,
		     const JobStepAssociation& outputJobStepAssociation,
		     DistributedEngineComm* dec,
		     CalpontSystemCatalog* cat,
		     execplan::CalpontSystemCatalog::OID oid,
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
    fOid(oid),
	fTableOid(tableOid),
    fSessionId(session),
    fTxnId(txn),
    fVerId(verID),
    fStepId(step),
    fStatementId(statement),
    fMsgsSent(0),
    fMsgsRecvd(0),
    fFinishedSending(false),
    fRecvWaiting(false),
    fConsumerThread(),
    fProducerThread(),
    fMutex(),
    fCondvar()
{	  
	uniqueID = Unique32Generator::instance()->getUnique32();
	if (fDec)
		fDec->addQueue(uniqueID);
}

pIdxList::~pIdxList()
{
	if (fDec)
		fDec->removeQueue(uniqueID);
}

  void pIdxList::startPrimitiveThread()
  {
    fProducerThread.reset(new boost::thread(pIdxListPrimitive(this)));
  }

  void pIdxList::startAggregationThread()
  {
    fConsumerThread.reset(new boost::thread(pIdxListAggregator(this)));
  }

  void pIdxList::run()
  {
#ifdef PROFILE
    clock_gettime(CLOCK_REALTIME, &fTs1);
#endif
	if (traceOn())
	{
		syslogStartStep(16,           // exemgr subsystem
			std::string("pIdxList")); // step name
	}

    pthread_mutex_init(&fMutex, NULL);
    pthread_cond_init(&fCondvar, NULL);
    startPrimitiveThread();
    startAggregationThread();
  }

  void pIdxList::join()
  {
    fProducerThread->join();
    fConsumerThread->join();
    pthread_mutex_destroy(&fMutex);
    pthread_cond_destroy(&fCondvar);
  }


  void pIdxList::makeIndexHeader(IndexListHeader& indexHdr)
  {
    indexHdr.ism.Reserve=0;
    indexHdr.ism.Flags=planFlagsToPrimFlags(fTraceFlags);
    indexHdr.ism.Command=INDEX_LIST;
    indexHdr.ism.Type=2;
    indexHdr.ism.Status=0;

    // Init the Index List Structure
    indexHdr.Hdr.SessionID = fSessionId;
    indexHdr.Hdr.TransactionID = fTxnId;
    indexHdr.Hdr.VerID = fVerId;
    indexHdr.Hdr.StepID = fStepId;
    indexHdr.Hdr.UniqueID = uniqueID;

    indexHdr.NVALS = 0;
    indexHdr.Pad1 = 0;
    indexHdr.Pad1 = 0;
 }

  void pIdxList::sendError(uint16_t status)
  {
	fOutputJobStepAssociation.status(status);

	uint16_t size =  sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader);
	ISMPacketHeader ism;
	ism.Command  = INDEX_LIST;
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

  void pIdxList::updateMsgSent(bool notify)
  {
	  pthread_mutex_lock(&fMutex);
	    fMsgsSent++;
	  if (notify && fRecvWaiting )
	      pthread_cond_signal(&fCondvar);
#ifdef DEBUG
	    cout << "IndexList msgsSent++ = " << fMsgsSent << endl;
#endif
	    pthread_mutex_unlock(&fMutex);
  }


  void pIdxList::sendPrimitiveMessages()
  {
    try
    {
    if (0 < fInputJobStepAssociation.status() )
    {
	sendError(fInputJobStepAssociation.status());
    }
    else
    {
    IndexListHeader indexHdr;
    makeIndexHeader(indexHdr);
    
    if ( ! fInputJobStepAssociation.outSize() )
	throw logic_error("No input datalist to fIdxList");

    ZonedDL*  indexParams = fInputJobStepAssociation.outAt(0)->zonedDL(); 

    if ( ! indexParams )
      throw logic_error("IdxList requires IndexParam list");

    ByteStream msgParams;
    ByteStream primMsg(MAX_BUFFER_SIZE);

    //Repackage the IndexListParams into send messages according to block (fbo)
    ElementType e;
    int it = indexParams->getIterator();
    bool more = indexParams->next(it, &e);
    while (more)
    {
	IndexListParam* param = reinterpret_cast<IndexListParam*>(&e.second);
	uint64_t block = param->fbo;
	while (ELEMENTS_PER_MESSAGE > indexHdr.NVALS && more && block == param->fbo )
	{
	    msgParams << *((uint64_t* ) &(*param));
	    indexHdr.NVALS++;
	    more = indexParams->next(it, &e);
	}

#ifdef DEBUG
	cout << "IndexList sending a prim msg" << endl;
#endif
	if (indexHdr.NVALS)
	{
	indexHdr.ism.Size =+ indexHdr.NVALS * 8;  
	indexHdr.LBID = block;
	primMsg.load((const uint8_t *) &indexHdr, sizeof(IndexListHeader));
	primMsg += msgParams;
	fDec->write(primMsg);
	updateMsgSent();

	makeIndexHeader(indexHdr); 
	primMsg.reset();
	msgParams.reset();
	}
    }
   } // else no error
 } // try
catch(const exception& e)
{
	catchHandler(e.what());
	sendError(pIdxListErr);
}
catch(...)
{
	catchHandler("pIdxList send caught an unknown exception.");
	sendError(pIdxListErr);
}
		
    pthread_mutex_lock(&fMutex);
    fFinishedSending = true;
    if (fRecvWaiting)
      pthread_cond_signal(&fCondvar);
    pthread_mutex_unlock(&fMutex);
  }

   bool pIdxList::syncWithSend()
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
      cout << "IndexList msgsRecvd++ = " << fMsgsRecvd << endl;
#endif
      pthread_mutex_unlock(&fMutex);
      return false;
   }



void pIdxList::receivePrimitiveMessages()
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
	  cout << "Got an IndexListHeader!: " << bs.length() << " bytes" << endl;
#endif

	  const ByteStream::byte* bsp = bs.buf();

	  // get the IdxListHeader out of the bytestream and ready one for resend
	  const IndexListHeader*  idxHdr = reinterpret_cast<const IndexListHeader*>(bsp);
	  if (0 == (error = idxHdr->ism.Status))
	    {
	      uint16_t nvals = idxHdr->NVALS;

	      IndexListHeader msgHdr(*idxHdr);
	      msgHdr.NVALS = 1;
      
	      msgHdr.ism.Command = INDEX_LIST;
	      bsp += sizeof(IndexListHeader);

	      // The remainder of the bytestream is the returned data from the primitive server.
	      // If the data is RID type, then it is a result and put in the output datalist.
	      // Otherwise it gets sent back to the primitive processor with the fbo as the lbid.

#ifdef DEBUG
      
	      cout << "  IndexList returned values = " << nvals << endl;
#endif
	      for(uint16_t j = 0; j < nvals; j++)
		{
		  const IndexListEntry *walk = reinterpret_cast<const IndexListEntry*>(bsp);
		  switch (walk->type)
		    {
		    case RID:
		      {   
			dlp->insert(ElementType(walk->value, 0));
			pthread_mutex_lock(&fMutex);
			++results;
			pthread_mutex_unlock(&fMutex);
#ifdef DEBUG
			cout << "  IndexList -- inserting <"  << walk->value << ">\n";
#endif
			break;
		      }
		    case LLP_SUBBLK:
		    case LLP_BLK:
		      {
			const IndexListParam* param = reinterpret_cast<const IndexListParam*>(bsp);   //same size as IndexListEntry	
			msgHdr.LBID = param->fbo;  //update header with LBID from param.
			ByteStream primMsg;
			primMsg.load((const uint8_t*) &msgHdr, sizeof(IndexListHeader) );
			primMsg.append((const uint8_t*) param, sizeof(IndexListEntry) );
			fDec->write(primMsg);

			updateMsgSent(false);
			
#ifdef DEBUG
			cout << "  IndexList -- resending < " << idxHdr->LBID << " >" << endl;
#endif
			primMsg.reset();
			break;
		      }
		    default:
		      break;

		    }//switch
		  bsp += sizeof(IndexListEntry);
		} //for
	    } // if error == 0

	} //while == 0
    }
  catch(const exception& e)
    {
      catchHandler(e.what());
      fOutputJobStepAssociation.status(pIdxListErr);
    }
  catch(...)
    {
      catchHandler("pIdxList receive caught an unknown exception.");
      fOutputJobStepAssociation.status(pIdxListErr);
    }


  fDec->removeQueue(uniqueID);

  dlp->endOfInput();
      
  //@bug 699: Reset StepMsgQueue

  if (traceOn())
    {
      syslogEndStep(16, // exemgr subsystem
		    0,            // no blocked datalist input  to report
		    0);           // no blocked datalist output to report
    }
		
#ifdef PROFILE
  timespec diff;
  clock_gettime(CLOCK_REALTIME, &fTs2);
  ltimespec_sub(fTs1, fTs2, diff);
  cout << "pIdxList execution stats:" << endl;
  cout << "  Primitive msgs sent & recvd: " << fMsgsSent << endl;
  cout << "  # of data results returned: " << results << endl;
  cout << "  total runtime: " << diff.tv_sec << "s " << diff.tv_nsec <<
    "ns" << endl;
#endif
}

    const string pIdxList::toString() const
    {
      ostringstream oss;
	oss << "pIdxList    ses:" << fSessionId << " txn:" << fTxnId << " ver:" << fVerId << " st:" << fStepId <<
		" tb/idxList:" << fTableOid << "/" << fOid;
	oss << " " << omitOidInDL
		<< fOutputJobStepAssociation.outAt(0) << showOidInDL;
	oss << " nf:0";
	oss << " in:";
	for (unsigned i = 0; i < fInputJobStepAssociation.outSize(); i++)
	{
		oss << fInputJobStepAssociation.outAt(i) << ", ";
	}
	return oss.str();
    }

  }   //namespace


