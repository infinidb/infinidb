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

//  $Id: crossenginestep.h 9620 2013-06-13 15:51:52Z pleblanc $


#ifndef JOBLIST_CROSSENGINESTEP_H
#define JOBLIST_CROSSENGINESTEP_H

#include <boost/scoped_array.hpp>

#include "jobstep.h"
#include "primitivestep.h"

// forward reference
namespace  execplan
{
class ParseTree;
class ReturnedColumn;
}

namespace funcexp
{
class FuncExp;
}

namespace joblist
{

/** @brief class CrossEngineStep
 *
 */
class CrossEngineStep : public BatchPrimitive, public TupleDeliveryStep
{
public:
    /** @brief CrossEngineStep constructor
     */
    CrossEngineStep(
			const string& schema,
			const string& table,
			const string& alias,
			const JobInfo& jobInfo);

    /** @brief CrossEngineStep destructor
     */
   ~CrossEngineStep();

    /** @brief virtual void Run method
     */
    void run();

    /** @brief virtual void join method
     */
    void join();

    /** @brief virtual string toString method
     */
    const std::string toString() const;

	// from BatchPrimitive
	bool getFeederFlag() const { return false; }
	uint64_t getLastTupleId() const { return 0; }
	uint32_t getStepCount () const { return 1; }
	void setBPP(JobStep* jobStep);
	void setFirstStepType(PrimitiveStepType firstStepType) {}
	void setIsProjectionOnly() {}
	void setLastTupleId(uint64_t id) {}
	void setOutputType(BPSOutputType outputType) {}
	void setProjectBPP(JobStep* jobStep1, JobStep* jobStep2);
	void setStepCount() {}
	void setSwallowRows(const bool swallowRows) {}
	void setBppStep() {}
	void dec(DistributedEngineComm* dec) {}
	const OIDVector& getProjectOids() const { return fOIDVector; }
	uint64_t blksSkipped() const { return 0; }
	bool wasStepRun() const { return fRunExecuted; }
	BPSOutputType getOutputType() const { return ROW_GROUP; }
	uint64_t getRows() const { return fRowsReturned; }
	const string& schemaName() const { return fSchema; }
	const string& tableName() const { return fTable; }
	const string& tableAlias() const { return fAlias; }
	void useJoiner(boost::shared_ptr<joiner::Joiner>) {}
	void setJobInfo(const JobInfo* jobInfo) {}
	void  setOutputRowGroup(const rowgroup::RowGroup&);
	const rowgroup::RowGroup& getOutputRowGroup() const;

	// from DECEventListener
	void newPMOnline(uint32_t) {}

	const rowgroup::RowGroup& getDeliveredRowGroup() const;
	void  deliverStringTableRowGroup(bool b);
	bool  deliverStringTableRowGroup() const;
	uint32_t nextBand(messageqcpp::ByteStream &bs);

	void addFcnExpGroup1(const boost::shared_ptr<execplan::ParseTree>&);
	void setFE1Input(const rowgroup::RowGroup&);
	void setFcnExpGroup3(const vector<boost::shared_ptr<execplan::ReturnedColumn> >&);
	void setFE23Output(const rowgroup::RowGroup&);

	void addFilter(JobStep* jobStep);
	void addProject(JobStep* jobStep);

	static void init_mysqlcl_idb();

protected:
	virtual void execute();
	virtual void getMysqldInfo(const JobInfo&);
	virtual void makeMappings();
	virtual void addFilterStr(const std::vector<const execplan::Filter*>&, const std::string&);
	virtual std::string makeQuery();
	virtual void setField(int, const char*, rowgroup::Row&);
	inline void addRow(rowgroup::RGData &);
	//inline  void addRow(boost::shared_array<uint8_t>&);
	virtual int64_t convertValueNum(
						const char*, const execplan::CalpontSystemCatalog::ColType&, int64_t);
	virtual void formatMiniStats();
	virtual void printCalTrace();
	virtual void handleMySqlError(const char*, unsigned int);

	uint64_t fRowsRetrieved;
	uint64_t fRowsReturned;
	uint64_t fRowsPerGroup;

	// output rowgroup and row
	rowgroup::RowGroup fRowGroupOut;
	rowgroup::RowGroup fRowGroupDelivered;
	rowgroup::Row fRowDelivered;

	// for datalist
	RowGroupDL* fOutputDL;
	uint64_t    fOutputIterator;

	class Runner
	{
	public:
		Runner(CrossEngineStep* step) : fStep(step) { }
		void operator()() { fStep->execute(); }

		CrossEngineStep* fStep;
	};

	boost::scoped_ptr<boost::thread> fRunner;
	OIDVector fOIDVector;
	bool fEndOfResult;
	bool fRunExecuted;

	// MySQL server info
	std::string  fHost;
	std::string  fUser;
	std::string  fPasswd;
	std::string  fSchema;
	std::string  fTable;
	std::string  fAlias;
	unsigned int fPort;

	// returned columns and primitive filters
	std::string fWhereClause;
	std::string fSelectClause;

	// Function & Expression columns
	boost::shared_ptr<execplan::ParseTree> fFeFilters;
	std::vector<boost::shared_ptr<execplan::ReturnedColumn> > fFeSelects;
	std::map<uint32_t, uint32_t> fColumnMap;   // projected key position (k->p)
	uint64_t fColumnCount;
	boost::scoped_array<int> fFe1Column;
	boost::shared_array<int> fFeMapping1;
	boost::shared_array<int> fFeMapping3;
	rowgroup::RowGroup fRowGroupFe1;
	rowgroup::RowGroup fRowGroupFe3;

	funcexp::FuncExp* fFeInstance;

};


} // namespace



#endif  // JOBLIST_CROSSENGINESTEP_H

// vim:ts=4 sw=4:


