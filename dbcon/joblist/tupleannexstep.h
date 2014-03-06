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

//  $Id: tupleannexstep.h 9596 2013-06-04 19:59:04Z xlou $


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
     */
    TupleAnnexStep(const JobInfo& jobInfo);

    /** @brief TupleAnnexStep destructor
     */
   ~TupleAnnexStep();

	// inherited methods
	void run();
	void join();
	const std::string toString() const;

	/** @brief TupleJobStep's pure virtual methods
	 */
	const rowgroup::RowGroup& getOutputRowGroup() const;
	void  setOutputRowGroup(const rowgroup::RowGroup&);

	/** @brief TupleDeliveryStep's pure virtual methods
	 */
	uint32_t nextBand(messageqcpp::ByteStream &bs);
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	void  deliverStringTableRowGroup(bool b);
	bool  deliverStringTableRowGroup() const;

	void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);

	void addOrderBy(LimitedOrderBy* lob)     { fOrderBy = lob; }
	void addConstant(TupleConstantStep* tcs) { fConstant = tcs; }
	void setDistinct()                       { fDistinct = true; }
	void setLimit(uint64_t s, uint64_t c)    { fLimitStart = s; fLimitCount = c; }
	
	virtual bool stringTableFriendly() { return true; }
	
	rowgroup::Row row1, row2;  // scratch space for distinct comparisons todo: make them private

protected:
	void execute();
	void executeNoOrderBy();
	void executeWithOrderBy();
	void executeNoOrderByWithDistinct();
	void formatMiniStats();
	void printCalTrace();

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

	uint64_t                fRowsProcessed;
	uint64_t                fRowsReturned;
	uint64_t                fLimitStart;
	uint64_t                fLimitCount;
	bool                    fLimitHit;
	bool                    fEndOfResult;
	bool                    fDistinct;

	LimitedOrderBy*         fOrderBy;
	TupleConstantStep*      fConstant;

	funcexp::FuncExp*       fFeInstance;
	JobList*                fJobList;
	
};


} // namespace

#endif  // JOBLIST_TUPLEANNEXSTEP_H

// vim:ts=4 sw=4:
