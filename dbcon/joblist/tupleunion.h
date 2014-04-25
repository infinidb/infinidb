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

//
// C++ Interface: TupleUnion
//
// Description:
//
//
// Author: Patrick <pleblanc@localhost.localdomain>, (C) 2009
//
// Copyright: See COPYING file that comes with this distribution
//
//

#include "jobstep.h"
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

#include "stlpoolallocator.h"

#ifndef TUPLEUNION2_H_
#define TUPLEUNION2_H_

namespace joblist
{



class TupleUnion : public JobStep, public TupleDeliveryStep
{
public:
	TupleUnion(execplan::CalpontSystemCatalog::OID tableOID, const JobInfo& jobInfo);
	~TupleUnion();

	void run();
	void join();

	const std::string toString() const;
	execplan::CalpontSystemCatalog::OID tableOid() const;

	void setInputRowGroups(const std::vector<rowgroup::RowGroup> &);
	void setOutputRowGroup(const rowgroup::RowGroup &);
	void setDistinctFlags(const std::vector<bool> &);

	const rowgroup::RowGroup& getOutputRowGroup() const { return outputRG; }
	const rowgroup::RowGroup& getDeliveredRowGroup() const { return outputRG; }
	void  deliverStringTableRowGroup(bool b)  { outputRG.setUseStringTable(b); }
	bool  deliverStringTableRowGroup() const  { return outputRG.usesStringTable(); }

	// @bug 598 for self-join
	std::string alias1() const { return fAlias1; }
	void alias1(const std::string& alias) { fAlias = fAlias1 = alias; }
	std::string alias2() const { return fAlias2; }
	void alias2(const std::string& alias) { fAlias2 = alias; }

	std::string view1() const { return fView1; }
	void view1(const std::string& vw) { fView = fView1 = vw; }
	std::string view2() const { return fView2; }
	void view2(const std::string& vw) { fView2 = vw; }

	uint32_t nextBand(messageqcpp::ByteStream &bs);


private:

	struct RowPosition
	{
		uint64_t group : 48;
		uint64_t row   : 16;

		inline RowPosition(uint64_t i = 0, uint64_t j = 0) : group(i), row(j) {};
		static const uint64_t normalizedFlag = 0x800000000000ULL;   // 48th bit is set
	};

	void getOutput(rowgroup::RowGroup *rg, rowgroup::Row *row, rowgroup::RGData *data);
	void addToOutput(rowgroup::Row *r, rowgroup::RowGroup *rg, bool keepit,
		rowgroup::RGData &data);
	void normalize(const rowgroup::Row &in, rowgroup::Row *out);
	void writeNull(rowgroup::Row *out, uint32_t col);
	void readInput(uint32_t);
	void formatMiniStats();

	execplan::CalpontSystemCatalog::OID fTableOID;
	// @bug 598 for self-join
	std::string fAlias1;
	std::string fAlias2;

	std::string fView1;
	std::string fView2;

	rowgroup::RowGroup outputRG;
	std::vector<rowgroup::RowGroup> inputRGs;
	std::vector<RowGroupDL *> inputs;
	RowGroupDL *output;
	uint32_t outputIt;

	struct Runner {
		TupleUnion *tu;
		uint32_t index;
		Runner(TupleUnion *t, uint32_t in) : tu(t), index(in) { }
		void operator()() { tu->readInput(index); }
	};
	std::vector<boost::shared_ptr<boost::thread> > runners;

	struct Hasher {
		TupleUnion *ts;
		utils::Hasher_r h;
		Hasher(TupleUnion *t) : ts(t) { }
		uint64_t operator()(const RowPosition &) const;
	};
	struct Eq {
		TupleUnion *ts;
		Eq(TupleUnion *t) : ts(t) { }
		bool operator()(const RowPosition &, const RowPosition &) const;
	};

	typedef std::tr1::unordered_set<RowPosition, Hasher, Eq,
		utils::STLPoolAllocator<RowPosition> > Uniquer_t;

	boost::scoped_ptr<Uniquer_t> uniquer;
	std::vector<rowgroup::RGData> rowMemory;
	boost::mutex sMutex, uniquerMutex;
	uint64_t memUsage;
	uint32_t rowLength;
	rowgroup::Row row, row2;
	std::vector<bool> distinctFlags;
	ResourceManager& rm;
	utils::STLPoolAllocator<RowPosition> allocator;
	boost::scoped_array<rowgroup::RGData> normalizedData;

	uint32_t runnersDone;
	uint32_t distinctCount;
	uint32_t distinctDone;

	uint64_t fRowsReturned;

	// temporary hack to make sure JobList only calls run, join once
	boost::mutex jlLock;
	bool runRan, joinRan;
};

}

#endif
