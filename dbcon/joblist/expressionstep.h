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

//  $Id: expressionstep.h 8326 2012-02-15 18:58:10Z xlou $


/** @file
 * class ExpStep interface
 */

#ifndef JOBLIST_EXPRESSION_STEP_H
#define JOBLIST_EXPRESSION_STEP_H

//#define NDEBUG
#include "jobstep.h"
#include "filter.h"

namespace joblist
{

struct JobInfo;

class ExpressionStep : public JobStep
{
  public:
	// constructors
	ExpressionStep(
		uint32_t sessionId,
		uint32_t txnId,
		uint32_t verId,
		uint32_t statementId);

	// destructor constructors
	virtual ~ExpressionStep();

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

	execplan::CalpontSystemCatalog::OID tableOid() const
	{ return fTableOids.empty() ? 0 : fTableOids.front(); }
	std::string alias() const { return fAliases.empty() ? "" : fAliases.front(); }
	std::string view() const { return fViews.empty() ? "" : fViews.front(); }

	void expression(const execplan::SRCP exp, JobInfo& jobInfo);
	execplan::SRCP expression() const { return fExpression; }

	virtual void expressionFilter(const execplan::Filter* filter, JobInfo& jobInfo);
	virtual void expressionFilter(const execplan::ParseTree* filter, JobInfo& jobInfo);
	execplan::ParseTree* expressionFilter() const { return fExpressionFilter; }

	void expressionId(uint64_t eid) { fExpressionId = eid; }
	uint64_t expressionId() const { return fExpressionId; }

	const std::vector<execplan::CalpontSystemCatalog::OID>& tableOids() const { return fTableOids; }
	const std::vector<execplan::CalpontSystemCatalog::OID>& oids() const { return fOids; }
	const std::vector<std::string>& aliases() const { return fAliases; }
	const std::vector<std::string>& views() const { return fViews; }

	virtual void updateInputIndex(std::map<uint, uint>& indexMap, const JobInfo& jobInfo);
	virtual void updateOutputIndex(std::map<uint, uint>& indexMap, const JobInfo& jobInfo);
	virtual void updateColumnOidAlias(JobInfo& jobInfo);

	std::vector<execplan::ReturnedColumn*>& columns() { return fColumns; }

	void selectFilter(bool b) { fSelectFilter = b; }
	bool selectFilter() const { return fSelectFilter; }

	void associatedJoinId(uint64_t i) { fAssociatedJoinId = i; }
	uint64_t associatedJoinId() const { return fAssociatedJoinId; }

  protected:
	virtual void addColumn(execplan::ReturnedColumn* rc, JobInfo& jobInfo);
	virtual void populateColumnInfo(execplan::SimpleColumn* sc, JobInfo& jobInfo);

	JobStepAssociation                               fInputJobStepAssociation;
	JobStepAssociation                               fOutputJobStepAssociation;
	uint32_t                                         fSessionId;
	uint32_t                                         fTxnId;
	uint32_t                                         fVerId;
	uint16_t                                         fStepId;
	uint32_t                                         fStatementId;
	SPJL	                                         fLogger;

	// expression
	execplan::SRCP                                   fExpression;
	execplan::ParseTree*                             fExpressionFilter;
	uint64_t                                         fExpressionId;

	// columns accessed
	std::vector<execplan::CalpontSystemCatalog::OID> fTableOids;
	std::vector<execplan::CalpontSystemCatalog::OID> fOids;
	std::vector<std::string>                         fAliases;
	std::vector<std::string>                         fViews;
	std::vector<execplan::ReturnedColumn*>           fColumns;

  private:
	// disable copy constructor
	// Cannot copy fColumns, which depends on fExpressionFilter.
	ExpressionStep(const ExpressionStep&);

	// for VARBINARY, only support limited number of functions: like hex(), length(), etc.
	bool                                             fVarBinOK;

	// @bug3780, for select filter
	bool                                             fSelectFilter;

	// @bug 3037, outer join with additional comparison
	uint64_t                                         fAssociatedJoinId;
};

}

#endif //  JOBLIST_EXPRESSION_STEP_H

