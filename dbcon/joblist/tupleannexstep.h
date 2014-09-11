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

//  $Id: tupleannexstep.h 7829 2011-06-30 20:09:00Z xlou $


#ifndef JOBLIST_TUPLEANNEXSTEP_H
#define JOBLIST_TUPLEANNEXSTEP_H

#include "jobstep.h"


// forward reference
namespace fucexp
{
class FuncExp;
}


namespace joblist
{
class TupleConstantStep;
class LimitedOrderBy;
}


namespace joblist
{
/** @brief class TupleAnnexStep
 *
 */
class TupleAnnexStep : public JobStep, public TupleDeliveryStep
{
public:
    /** @brief TupleAnnexStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     */
    TupleAnnexStep( uint32_t sessionId, uint32_t txnId, uint32_t verId, uint32_t statementId);

    /** @brief TupleAnnexStep destructor
     */
   ~TupleAnnexStep();

	// inherited methods
	void run();
	void join();
	const JobStepAssociation& inputAssociation() const { return fInputJobStepAssociation; }
	void inputAssociation(const JobStepAssociation& in) { fInputJobStepAssociation = in; }
	const JobStepAssociation& outputAssociation() const { return fOutputJobStepAssociation; }
	void outputAssociation(const JobStepAssociation& out) { fOutputJobStepAssociation = out; }
	JobStepAssociation& outputAssociation() { return fOutputJobStepAssociation; }

	const std::string toString() const;
	void stepId(uint16_t sId) { fStepId = sId; }
	uint16_t stepId() const { return fStepId; }
	uint32_t sessionId()   const { return fSessionId; }
	uint32_t txnId()	   const { return fTxnId; }
	uint32_t verId()	   const { return fVerId; }
	uint32_t statementId() const { return fStatementId; }
	void logger(const SPJL& logger) { fLogger = logger; }

	/** @brief TupleJobStep's pure virtual methods
	 */
	const rowgroup::RowGroup& getOutputRowGroup() const;
	void  setOutputRowGroup(const rowgroup::RowGroup&);

	/** @brief TupleDeliveryStep's pure virtual methods
	 */
	uint nextBand(messageqcpp::ByteStream &bs);
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	void setIsDelivery(bool b) { fDelivery = b; }

	void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

	void addOrderBy(LimitedOrderBy* lob)     { fOrderBy = lob; }
	void addConstant(TupleConstantStep* tcs) { fConstant = tcs; }
	void setDistinct()                       { fDistinct = true; }

protected:
	void execute();
	void executeNoOrderByLimit();
	void executeWithOrderByLimit();
	void executeNoOrderByLimitWithDistinct();
	void formatMiniStats();
	void printCalTrace();

	// for virtual function
	JobStepAssociation      fInputJobStepAssociation;
	JobStepAssociation      fOutputJobStepAssociation;
	uint32_t                fSessionId;
	uint32_t                fTxnId;
	uint32_t                fVerId;
	uint16_t                fStepId;
	uint32_t                fStatementId;
	SPJL                    fLogger;

	// input/output rowgroup and row
	rowgroup::RowGroup      fRowGroupIn;
	rowgroup::RowGroup      fRowGroupOut;
	rowgroup::RowGroup      fRowGroupDeliver;
	rowgroup::Row           fRowIn;
	rowgroup::Row           fRowOut;

	// for datalist
	RowGroupDL*             fInputDL;
	RowGroupDL*             fOutputDL;
	uint64_t                fInputIterator;
	uint64_t                fOutputIterator;

	class Runner
	{
	public:
		Runner(TupleAnnexStep* step) : fStep(step) { }
		void operator()() { fStep->execute(); }

		TupleAnnexStep*     fStep;
	};
	boost::scoped_ptr<boost::thread> fRunner;

	uint64_t                fRowsReturned;
	bool                    fEndOfResult;
	bool                    fDelivery;
	bool                    fDistinct;

	LimitedOrderBy*         fOrderBy;
	TupleConstantStep*      fConstant;

	funcexp::FuncExp*       fFeInstance;
};


} // namespace

#endif  // JOBLIST_TUPLEANNEXSTEP_H

// vim:ts=4 sw=4:
