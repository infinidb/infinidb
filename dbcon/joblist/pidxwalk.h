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
 *   $Id: pidxwalk.h 8436 2012-04-04 18:18:21Z rdempsey $
 *
 *
 ***********************************************************************/
/** @file */

#ifndef JOBLIST_PIDXWALK_H_
#define JOBLIST_PIDXWALK_H_

#include <set>
#include <boost/thread.hpp>
#include <boost/thread/condition.hpp>

#include "jobstep.h"

namespace joblist
{
  /** @brief pIdxWalk definition
   * Does a full index scan using using up to two search filters and with
   * filters from tokens from input data list.  
   */
  class pIdxWalk : public JobStep, public PrimitiveMsg
  {
  public:
    /** @brief pIdxWalk constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     * @param ec the DistributedEngineComm pointer
     */
    pIdxWalk(const JobStepAssociation& inputJobStepAssociation,
	     const JobStepAssociation& outputJobStepAssociation,
	     DistributedEngineComm* dec,
	     execplan::CalpontSystemCatalog* syscat,
	     execplan::CalpontSystemCatalog::OID oid,
	     execplan::CalpontSystemCatalog::OID columnOid,
	     execplan::CalpontSystemCatalog::OID tableOid,
	     uint32_t sessionId,
	     uint32_t txnId,
	     uint32_t verId,
	     uint16_t stepId,
		 uint32_t statementId);

    virtual ~pIdxWalk();

    /** @brief Starts processing.
     * 
     * Starts processing.
     */
    virtual void run();

    /** @brief Sync's the caller with the end of execution.
     *
     * Does nothing.  Returns when this instance is finished.
     */
    virtual void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    virtual const JobStepAssociation& inputAssociation() const
    {
      return fInputJobStepAssociation;
    }
    virtual void inputAssociation(const JobStepAssociation& inputAssociation)
    {
      fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    virtual const JobStepAssociation& outputAssociation() const
    {
      return fOutputJobStepAssociation;
    }
    virtual void outputAssociation(const JobStepAssociation& outputAssociation)
    {
      fOutputJobStepAssociation = outputAssociation;
    }

    /** @brief The main loop for the send-side thread 
     * 
     * The main loop for the primitive-issuing thread.  Don't call it directly.
     */
    void sendPrimitiveMessages();

    /** @brief The main loop for the recv-side thread
     *
     * The main loop for the receive-side thread.  Don't call it directly.
     */
    void receivePrimitiveMessages();

    /** @brief Set the DistributedEngineComm object this instance should use
     *
     * Set the DistributedEngineComm object this instance should use
     */
    void dec(DistributedEngineComm* dec) { fDec = dec; if (fDec) fDec->addQueue(uniqueID); }

    /** @brief Set the step ID of this JobStep
     *
     * Set the step ID of this JobStep.  
     */
    virtual void stepId(uint16_t stepId) { fStepId = stepId; }
    virtual uint16_t stepId() const  { return fStepId; }

    virtual uint32_t sessionId() const    { return fSessionId; }
    virtual uint32_t txnId() const        { return fTxnId; }
    virtual uint32_t statementId() const  { return fStatementId; }
	
   /** @brief Add a search string
    * 
    * Adds a search string with an operator. (Only 2 are implemented.)
    */
    void addSearchStr(int8_t cop, int64_t value);

   /** @brief set Bop
    * 
    * Sets the binary operator, valid only if there a 2 search strings.
    */
    void setBOP(int8_t bop) { fBop = bop; }

    virtual execplan::CalpontSystemCatalog::OID oid() const { return fOid; }
    execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }

    virtual const std::string toString() const;
    
    void logger(const SPJL& logger) { fLogger = logger; }
	/** @brief Add a filter when the column is a 4-byte float type 
 	* 
 	* Add a filter when the column is a 4-byte float type
 	*/
	void addFilter(int8_t COP, float value);

	/** @brief Add a filter when the column is anything but a 4-byte float type.
 	*
 	* Add a filter when the column is anything but a 4-byte float type, including
 	* 8-byte doubles.
 	*/
	void addFilter(int8_t COP, int64_t value);

  private:
    pIdxWalk(const pIdxWalk& rhs);
    pIdxWalk& operator=(const pIdxWalk& rhs);
    void makeIndexHeader(IndexWalkHeader& hdr);
    void updateMsgSent();
    bool syncWithSend();
    void sendError(uint16_t status);

    typedef boost::shared_ptr<boost::thread> SPTHD;

    void startPrimitiveThread();
    void startAggregationThread();

    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
    DistributedEngineComm* fDec;
    execplan::CalpontSystemCatalog* fSysCat;
    execplan::CalpontSystemCatalog::OID fOid;
    execplan::CalpontSystemCatalog::OID fColumnOid;
    execplan::CalpontSystemCatalog::OID fTableOid;
    uint32_t fSessionId;
    uint32_t fTxnId;
    uint32_t fVerId;
    uint16_t fStepId;
    uint32_t fStatementId;
    uint8_t  fBop;
    execplan::CalpontSystemCatalog::ColType fColType;
    int64_t fLBID;
    int fNOPS;
    uint64_t* fVals;
    int fMsgsSent;
    int fMsgsRecvd; 
    bool fFinishedSending;
    bool fRecvWaiting;
    std::vector <uint64_t> fSearchStrings;
    uint8_t fCop1:5;
    uint8_t fCop2:5;

    SPTHD fConsumerThread;
    SPTHD fProducerThread;
	boost::mutex fMutex;
	boost::condition fCondvar;
    SPJL fLogger;
	messageqcpp::ByteStream fFilterString;
	uint fFilterCount;
#ifdef PROFILE
    timespec fTs1, fTs2;
#endif
	uint32_t uniqueID;
  };

}

#endif

