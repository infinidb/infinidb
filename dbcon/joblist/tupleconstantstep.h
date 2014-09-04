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

//  $Id: tupleconstantstep.h 8436 2012-04-04 18:18:21Z rdempsey $


#ifndef JOBLIST_TUPLECONSTANTSTEP_H
#define JOBLIST_TUPLECONSTANTSTEP_H

#include "jobstep.h"

namespace joblist
{

/** @brief class TupleConstantStep
 *
 */
class TupleConstantStep : public JobStep, public TupleDeliveryStep
{
public:
    /** @brief TupleConstantStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     */
    TupleConstantStep(
			const JobStepAssociation& inputJobStepAssociation,
			const JobStepAssociation& outputJobStepAssociation,
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t verId,
			uint32_t statementId);

    /** @brief TupleConstantStep destructor
     */
   ~TupleConstantStep();

    /** @brief virtual void Run method
     */
    void run();
    void join();

    /** @brief virtual JobStepAssociation * inputAssociation
     * 
     * @returns JobStepAssociation *
     */
    const JobStepAssociation& inputAssociation() const
    {
        return fInputJobStepAssociation;
    }
    void inputAssociation(const JobStepAssociation& inputAssociation)
    {
        fInputJobStepAssociation = inputAssociation;
    }
    /** @brief virtual JobStepAssociation * outputAssociation
     * 
     * @returns JobStepAssocation *
     */
    const JobStepAssociation& outputAssociation() const
    {
        return fOutputJobStepAssociation;
    }
    void outputAssociation(const JobStepAssociation& outputAssociation)
    {
        fOutputJobStepAssociation = outputAssociation;
    }

    void stepId(uint16_t stepId) { fStepId = stepId; }
    uint16_t stepId() const { return fStepId; }
    uint32_t sessionId()   const { return fSessionId; }
    uint32_t txnId()   const { return fTxnId; }
    uint32_t verId()   const { return fVerId; }
    uint32_t statementId() const { return fStatementId; }

    const std::string toString() const;

	void  setOutputRowGroup(const rowgroup::RowGroup&);
	const rowgroup::RowGroup& getOutputRowGroup() const;
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	uint nextBand(messageqcpp::ByteStream &bs);
	void logger(const SPJL& logger) { fLogger = logger; }
	void setIsDelivery(bool b) { fDelivery = b; }

	virtual void initialize(const JobInfo& jobInfo, const rowgroup::RowGroup* rgIn);
	virtual void fillInConstants(const rowgroup::Row& rowIn, rowgroup::Row& rowOut);
    static SJSTEP addConstantStep(const JobInfo& jobInfo, const rowgroup::RowGroup* rg = NULL);

protected:
	virtual void execute();
	virtual void fillInConstants();
	virtual void formatMiniStats();
	virtual void printCalTrace();
	virtual void constructContanstRow(const JobInfo& jobInfo);

	// for base
    JobStepAssociation fInputJobStepAssociation;
    JobStepAssociation fOutputJobStepAssociation;
	uint32_t fSessionId;
	uint32_t fTxnId;
	uint32_t fVerId;
    uint16_t fStepId;
	uint32_t fStatementId;
	uint64_t fRowsReturned;
    SPJL     fLogger;

	// input/output rowgroup and row
	rowgroup::RowGroup fRowGroupIn;
	rowgroup::RowGroup fRowGroupOut;
	rowgroup::Row fRowIn;
	rowgroup::Row fRowOut;

	// mapping
	std::vector<uint64_t> fIndexConst;    // consts in output row
	std::vector<uint64_t> fIndexMapping;  // from input row to output row

	// store constants
	rowgroup::Row fRowConst;
	boost::scoped_array<uint8_t> fConstRowData;

	// for datalist
	RowGroupDL* fInputDL;
	RowGroupDL* fOutputDL;
	uint64_t fInputIterator;

	class Runner
	{
	public:
		Runner(TupleConstantStep* step) : fStep(step) { }
		void operator()() { fStep->execute(); }

		TupleConstantStep* fStep;
	};

	boost::scoped_ptr<boost::thread> fRunner;
	bool fEndOfResult;
	bool fDelivery;
};


class TupleConstantOnlyStep : public TupleConstantStep
{
public:
    /** @brief TupleConstantOnlyStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     */
    TupleConstantOnlyStep(
			const JobStepAssociation& inputJobStepAssociation,
			const JobStepAssociation& outputJobStepAssociation,
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t verId,
			uint32_t statementId);

    /** @brief TupleConstantOnlyStep destructor
     */
   ~TupleConstantOnlyStep();

    /** @brief virtual void Run method
     */
    void run();

	/** @brief virtual void initialize method
	 */
	void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

    const std::string toString() const;
	uint nextBand(messageqcpp::ByteStream &bs);

protected:
	void fillInConstants();

};


class TupleConstantBooleanStep : public TupleConstantStep
{
public:
    /** @brief TupleConstantBooleanStep constructor
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     */
    TupleConstantBooleanStep(
			const JobStepAssociation& inputJobStepAssociation,
			const JobStepAssociation& outputJobStepAssociation,
			uint32_t sessionId,
			uint32_t txnId,
			uint32_t verId,
			uint32_t statementId,
			bool value);

    /** @brief TupleConstantBooleanStep destructor
     */
   ~TupleConstantBooleanStep();

    /** @brief virtual void Run method
     */
    void run();

	/** @brief virtual void initialize method
	 */
	void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

    const std::string toString() const;
	uint nextBand(messageqcpp::ByteStream &bs);

	virtual void boolValue(bool b) { fValue = b; }
	virtual bool boolValue() const { return fValue; }

protected:
	void execute() {}
	void fillInConstants() {}
	void constructContanstRow(const JobInfo& jobInfo) {}

	// boolean value
	bool fValue;
};


} // namespace

#endif  // JOBLIST_TUPLECONSTANTSTEP_H

// vim:ts=4 sw=4:
