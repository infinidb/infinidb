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

//  $Id: subquerytransformer.h 6370 2010-03-18 02:58:09Z xlou $


/** @file */

#ifndef SUBQUERY_TRANSFORMER_H
#define SUBQUERY_TRANSFORMER_H


#include "elementtype.h"
#include "jobstep.h"
#include "joblist.h"
#include "rowgroup.h"
#include "virtualtable.h"

namespace execplan
{

class CalpontSelectExecutionPlan;

}


namespace joblist
{

struct JobInfo;
class  TupleHashJoinStep;

class SubQueryTransformer
{

public:
    /** @brief SubQueryTransformer constructor
     *  @param jobInfo
     *  @param errorInfo
     */
    SubQueryTransformer(JobInfo*, SErrorInfo&);

    /** @brief SubQueryTransformer constructor
     *  @param jobInfo
     *  @param errorInfo
     *  @param view
     */
    SubQueryTransformer(JobInfo*, SErrorInfo&, const std::string&);

    /** @brief SubQueryTransformer constructor
     *  @param jobInfo
     *  @param errorInfo
     *  @param alias
     *  @param view
     */
    SubQueryTransformer(
		JobInfo*, SErrorInfo&, const std::string&, const std::string&);

    /** @brief SubQueryTransformer destructor
     */
   virtual ~SubQueryTransformer();

    /** @brief virtual make a subquery step
     *  @param csep the execution plan
     *  @param b if the subquery is in FROM clause
     *  @returns boost::shared_ptr<JobStep>
     */
    virtual SJSTEP& makeSubQueryStep(execplan::CalpontSelectExecutionPlan*, bool b = false);

    /** @brief virtual make a virtual table
     *  @param csep the execution plan
     *  @returns const VirtualTable&
     */
    virtual void updateCorrelateInfo();

    /** @brief virtual void run method
     */
    virtual void run();

    /** @brief virtual outer query jobinfo
     *  @returns JobInfo*
     */
    virtual JobInfo* outJobInfo() const { return fOutJobInfo; }

    /** @brief virtual subquery jobinfo
     *  @returns JobInfo*
     */
    virtual JobInfo* subJobInfo() const { return fSubJobInfo; }

    /** @brief virtual error info pointer
     *  @returns SErrorInfo&
     */
    virtual SErrorInfo& errorInfo() const { return fErrorInfo; }

    /** @brief virtual joblist
     *  @returns STJLP&
     */
    virtual const STJLP& subJobList() const { return fSubJobList; }

    /** @brief virtual subquery step
     *  @returns SJSTEP&
     */
    virtual const SJSTEP& subQueryStep() const { return fSubQueryStep; }

    /** @brief virtual get correlated steps
     *  @returns const JobStepVector&
     */
    virtual JobStepVector& correlatedSteps() { return fCorrelatedSteps; }

    /** @brief get virtual table
     *  @returns const VirtualTable&
     */
    const VirtualTable& virtualTable() const { return fVtable; }

    /** @brief set varbinary support flag
     */
    void setVarbinaryOK() { fVtable.varbinaryOK(true); }


protected:

	void checkCorrelateInfo(TupleHashJoinStep*, const JobInfo&);

    JobInfo*      fOutJobInfo;
    JobInfo*      fSubJobInfo;
    SErrorInfo&   fErrorInfo;
    JobStepVector fCorrelatedSteps;
	RetColsVector fSubReturnedCols;
	STJLP         fSubJobList;
	SJSTEP        fSubQueryStep;
	VirtualTable  fVtable;

	// disable copy constructor and assignment operator
	SubQueryTransformer(const SubQueryTransformer&);
	SubQueryTransformer& operator=(const SubQueryTransformer&);

};


class SimpleScalarTransformer : public SubQueryTransformer
{

public:
    /** @brief SimpleScalarTransformer constructor
     *  @param jobInfo
     *  @param errorInfo for error info
     *  @param existFilter indicate if exist filter
     */
    SimpleScalarTransformer(JobInfo* jobInfo, SErrorInfo& errInfo, bool existFilter);

    /** @brief SimpleScalarTransformer constructor
     *  @param SubQueryTransformer
     */
    SimpleScalarTransformer(const SubQueryTransformer& rhs);

    /** @brief SimpleScalarTransformer destructor
     */
    virtual ~SimpleScalarTransformer();

    /** @brief virtual void run method
     */
    void run();

    /** @brief virtual get scalar result
     *  @param jobInfo
     */
    virtual void getScalarResult();

    /** @brief check if result set is empty
     *  @returns bool
     */
    bool emptyResultSet() const { return fEmptyResultSet; }

    /** @brief retrieve result row
     *  @returns Row
     */
    const rowgroup::Row& resultRow() const { return fRow; }


protected:
    RowGroupDL*                  fInputDl;
    int                          fDlIterator;
	rowgroup::RowGroup           fRowGroup;
	rowgroup::Row                fRow;
	boost::scoped_array<uint8_t> fRowData;
    bool                         fEmptyResultSet;
    bool                         fExistFilter;

	// disable copy constructor and assignment operator
	SimpleScalarTransformer(const SimpleScalarTransformer&);
	SimpleScalarTransformer& operator=(const SimpleScalarTransformer&);

};


}

#endif  // SUBQUERY_STEP_H
// vim:ts=4 sw=4:

