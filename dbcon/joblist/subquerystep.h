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

//  $Id: subquerystep.h 6370 2010-03-18 02:58:09Z xlou $


/** @file */

#ifndef SUBQUERY_STEP_H
#define SUBQUERY_STEP_H

#include <boost/scoped_ptr.hpp>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/thread.hpp>

#include "jobstep.h"
#include "joblist.h"
#include "funcexpwrapper.h"

namespace joblist
{
struct JobInfo;

class SubQueryStep : public JobStep
{

public:
    /** @brief SubQueryStep constructor
     *  @param sessionId the session Id
     *  @param txnId the transaction Id
     *  @param statementId the statement Id
     */
    SubQueryStep(
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t statementId);

    /** @brief SubQueryStep destructor
     */
   ~SubQueryStep();

    /** @brief virtual void run method
     */
    void run();

    /** @brief virtual void join method
     */
    void join();
	
	void abort();

    /** @brief virtual const JobStepAssociation& inputAssociation
     *  @returns const JobStepAssociation&
     */
    const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }

    /** @brief virtual JobStepAssociation& inputAssociation
     */
    void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual const JobStepAssociation& outputAssociation
     *  @returns const JobStepAssocation&
     */
    const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

    /** @brief virtual JobStepAssociation& outputAssociation
     */
    void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	/** @brief virtual set step Id
	 */
    void stepId(uint16_t stepId) { fStepId = stepId; }

	/** @brief virtual get step Id
     *  @returns uint16
	 */
    uint16_t stepId() const { return fStepId; }

	/** @brief virtual get session Id
     *  @returns uint32
	 */
    uint32_t sessionId()   const { return fSessionId; }

	/** @brief virtual get transaction Id
     *  @returns uint32
	 */
    uint32_t txnId()       const { return fTxnId; }

	/** @brief virtual get statement Id
     *  @returns uint32
	 */
    uint32_t statementId() const { return fStatementId; }

	/** @brief virtual get table OID
     *  @returns OID
	 */
	execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }

	/** @brief virtual set table OID
	 */
	void tableOid(const execplan::CalpontSystemCatalog::OID& id) { fTableOid = id; }

	/** @brief virtual output info to a string
     *  @returns string
	 */
    const std::string toString() const;

	/** @brief virtual set default logger
	 */
	void logger(const SPJL& logger) { fLogger = logger; }

	/** @brief virtual set the output rowgroup
	 */
	virtual void setOutputRowGroup(const rowgroup::RowGroup& rg) { fOutputRowGroup = rg; }

	/** @brief virtual get the output rowgroup
     *  @returns RowGroup
	 */
	virtual const rowgroup::RowGroup& getOutputRowGroup() const { return fOutputRowGroup; }

	/** @brief virtual set the sub-query's joblist
	 */
	virtual void subJoblist(const STJLP& sjl) { fSubJobList = sjl; }

	/** @brief virtual get the sub-query's joblist
     *  @returns boost::shared_ptr<TupleJobList>
	 */
	virtual const STJLP& subJoblist() const   { return fSubJobList; }


protected:
    JobStepAssociation                               fInputJobStepAssociation;
    JobStepAssociation                               fOutputJobStepAssociation;
	uint32_t                                         fSessionId;
	uint32_t                                         fTxnId;
    uint16_t                                         fStepId;
	uint32_t                                         fStatementId;
	uint64_t                                         fRowsReturned;
    SPJL                                             fLogger;

	execplan::CalpontSystemCatalog::OID              fTableOid;
	std::vector<execplan::CalpontSystemCatalog::OID> fColumnOids;
	rowgroup::RowGroup                               fOutputRowGroup;
	STJLP                                            fSubJobList;

	boost::scoped_ptr<boost::thread>                 fRunner;
};


class SubAdapterStep : public JobStep, public TupleDeliveryStep
{

public:
    /** @brief SubAdapterStep constructor
     *  @param sessionId the session Id
     *  @param txnId the transaction Id
     *  @param statementId the statement Id
     */
    SubAdapterStep(
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t statementId,
			SJSTEP& s);

    /** @brief SubAdapterStep destructor
     */
   ~SubAdapterStep();

    /** @brief virtual void run method
     */
    void run();

    /** @brief virtual void join method
     */
    void join();
	
	void abort();

    /** @brief virtual const JobStepAssociation& inputAssociation
     *  @returns const JobStepAssociation&
     */
    const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }

    /** @brief virtual JobStepAssociation& inputAssociation
     */
    void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }

    /** @brief virtual const JobStepAssociation& outputAssociation
     *  @returns const JobStepAssocation&
     */
    const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }

    /** @brief virtual JobStepAssociation& outputAssociation
     */
    void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

	/** @brief virtual set step Id
	 */
    void stepId(uint16_t stepId) { fStepId = stepId; }

	/** @brief virtual get step Id
     *  @returns uint16
	 */
    uint16_t stepId() const { return fStepId; }

	/** @brief virtual get session Id
     *  @returns uint32
	 */
    uint32_t sessionId()   const { return fSessionId; }

	/** @brief virtual get transaction Id
     *  @returns uint32
	 */
    uint32_t txnId()       const { return fTxnId; }

	/** @brief virtual get statement Id
     *  @returns uint32
	 */
    uint32_t statementId() const { return fStatementId; }

	/** @brief virtual get table OID
     *  @returns OID
	 */
	execplan::CalpontSystemCatalog::OID tableOid() const { return fTableOid; }

	/** @brief virtual set table OID
	 */
	void tableOid(const execplan::CalpontSystemCatalog::OID& id) { fTableOid = id; }

	/** @brief virtual output info to a string
     *  @returns string
	 */
    const std::string toString() const;

	/** @brief virtual set default logger
	 */
	void logger(const SPJL& logger) { fLogger = logger; }

	/** @brief virtual set the rowgroup for FE to work on
	 */
	void setFeRowGroup(const rowgroup::RowGroup& rg);

	/** @brief virtual set the output rowgroup
	 */
	void setOutputRowGroup(const rowgroup::RowGroup& rg);

	/** @brief virtual get the output rowgroup
     *  @returns RowGroup
	 */
	const rowgroup::RowGroup& getOutputRowGroup() const { return fRowGroupOut; }

	/** @brief TupleDeliveryStep's pure virtual methods nextBand
     *  @returns row count
	 */
	uint nextBand(messageqcpp::ByteStream &bs);

	/** @brief Delivered Row Group
     *  @returns RowGroup
	 */
	const rowgroup::RowGroup& getDeliveredRowGroup() const { return fRowGroupDeliver; }

	/** @brief set delivery falg
	 */
	void setIsDelivery(bool b) { fDelivery = b; }

	/** @brief get subquery step
	 */
	const SJSTEP& subStep() const { return fSubStep; }

	/** @brief add filters (expression steps)
	 */
	void addExpression(const JobStepVector&, JobInfo&);

	/** @brief add function columns (returned columns)
	 */
	void addExpression(const vector<execplan::SRCP>&);


protected:
	void execute();
	void outputRow(rowgroup::Row&, rowgroup::Row&);
	void checkDupOutputColumns();
	void dupOutputColumns(rowgroup::Row&);
	void printCalTrace();
	void formatMiniStats();

    JobStepAssociation                               fInputJobStepAssociation;
    JobStepAssociation                               fOutputJobStepAssociation;
	uint32_t                                         fSessionId;
	uint32_t                                         fTxnId;
    uint16_t                                         fStepId;
	uint32_t                                         fStatementId;
    SPJL                                             fLogger;

	execplan::CalpontSystemCatalog::OID              fTableOid;
	rowgroup::RowGroup                               fRowGroupIn;
	rowgroup::RowGroup                               fRowGroupOut;
	rowgroup::RowGroup                               fRowGroupFe;
	rowgroup::RowGroup                               fRowGroupDeliver;
	SJSTEP                                           fSubStep;
	uint64_t                                         fRowsReturned;
	bool                                             fEndOfResult;
	bool                                             fDelivery;
	boost::shared_array<int>                         fIndexMap;
	std::vector<std::pair<uint, uint> >              fDupColumns;

	RowGroupDL*                                      fInputDL;
	RowGroupDL*                                      fOutputDL;
	uint64_t                                         fInputIterator;
	uint64_t                                         fOutputIterator;

	class Runner
	{
	public:
		Runner(SubAdapterStep* step) : fStep(step) { }
		void operator()() { fStep->execute(); }

		SubAdapterStep* fStep;
	};
	boost::scoped_ptr<boost::thread>				 fRunner;

	boost::scoped_ptr<funcexp::FuncExpWrapper>       fExpression;
};


}

#endif  // SUBQUERY_STEP_H
// vim:ts=4 sw=4:

