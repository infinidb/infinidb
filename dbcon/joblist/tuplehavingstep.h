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

//  $Id: tuplehavingstep.h 8526 2012-05-17 02:28:10Z xlou $


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
     * @param in the inputAssociation pointer
     * @param out the outputAssociation pointer
     */
    TupleHavingStep( uint32_t sessionId, uint32_t txnId, uint32_t verId, uint32_t statementId);

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
	void setIsDelivery(bool b) { fDelivery = b; }

	void initialize(const rowgroup::RowGroup& rgIn, const JobInfo& jobInfo);
	void expressionFilter(const execplan::ParseTree* filter, JobInfo& jobInfo);

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
	bool     fDelivery;

	funcexp::FuncExp* fFeInstance;
};


} // namespace

#endif  // JOBLIST_TUPLEHAVINGSTEP_H

// vim:ts=4 sw=4:
