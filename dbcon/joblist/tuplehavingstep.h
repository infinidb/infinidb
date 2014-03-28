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

//  $Id: tuplehavingstep.h 9596 2013-06-04 19:59:04Z xlou $


#ifndef JOBLIST_TUPLEHAVINGSTEP_H
#define JOBLIST_TUPLEHAVINGSTEP_H

#include "jobstep.h"
#include "expressionstep.h"

// forward reference
namespace fucexp
{
class FuncExp;
}


namespace joblist
{
/** @brief class TupleHavingStep
 *
 */
class TupleHavingStep : public ExpressionStep, public TupleDeliveryStep
{
public:
    /** @brief TupleHavingStep constructor
     */
    TupleHavingStep(const JobInfo& jobInfo);

    /** @brief TupleHavingStep destructor
     */
   ~TupleHavingStep();

    /** @brief virtual void Run method
     */
    void run();
    void join();

    const std::string toString() const;

	/** @brief TupleJobStep's pure virtual methods
	 */
	const rowgroup::RowGroup& getOutputRowGroup() const;
	void  setOutputRowGroup(const rowgroup::RowGroup&);

	/** @brief TupleDeliveryStep's pure virtual methods
	 */
	uint nextBand(messageqcpp::ByteStream &bs);
	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	void  deliverStringTableRowGroup(bool b);
	bool  deliverStringTableRowGroup() const;

	void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);
	void expressionFilter(const execplan::ParseTree* filter, JobInfo& jobInfo);
	
	virtual bool stringTableFriendly() { return true; }

protected:
	void execute();
	void doHavingFilters();
	void formatMiniStats();
	void printCalTrace();

	// input/output rowgroup and row
	rowgroup::RowGroup fRowGroupIn;
	rowgroup::RowGroup fRowGroupOut;
	rowgroup::Row fRowIn;
	rowgroup::Row fRowOut;

	// for datalist
	RowGroupDL* fInputDL;
	RowGroupDL* fOutputDL;
	uint64_t fInputIterator;

	class Runner
	{
	public:
		Runner(TupleHavingStep* step) : fStep(step) { }
		void operator()() { fStep->execute(); }

		TupleHavingStep* fStep;
	};

	boost::scoped_ptr<boost::thread> fRunner;

	uint64_t fRowsReturned;
	bool     fEndOfResult;

	funcexp::FuncExp* fFeInstance;
};


} // namespace

#endif  // JOBLIST_TUPLEHAVINGSTEP_H

// vim:ts=4 sw=4:
