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

//  $Id: rowaggregation.h 3449 2012-12-10 21:08:06Z xlou $


#ifndef ROWAGGREGATION_H
#define ROWAGGREGATION_H

/** @file rowaggregation.h
 * Classes in this file are used to aggregate Rows in RowGroups.
 * RowAggregation is the class that performs the aggregation.
 * RowAggGroupByCol and RowAggFunctionCol are support classes used to describe
 * the columns involved in the aggregation.
 * @endcode
 */

#include <cstring>
#include <stdint.h>
#include <vector>
#ifdef _MSC_VER
#include <unordered_map>
#else
#include <tr1/unordered_map>
#endif
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>
#include <boost/scoped_array.hpp>

#include "serializeable.h"
#include "bytestream.h"
#include "rowgroup.h"
#include "hasher.h"
#include "stlpoolallocator.h"
#include "returnedcolumn.h"

// To do: move code that depends on joblist to a proper subsystem.
namespace joblist
{
class ResourceManager;
}

namespace rowgroup
{

typedef std::tr1::unordered_map<uint8_t*, uint8_t*, utils::TupleHasher, utils::TupleComparator,
            utils::STLPoolAllocator<std::pair<uint8_t* const, uint8_t*> > > RowAggMap_t;

/** @brief Enumerates aggregate functions supported by RowAggregation
 */
enum RowAggFunctionType
{
	ROWAGG_FUNCT_UNDEFINE, // default
	ROWAGG_COUNT_ASTERISK, // COUNT(*) counts all rows including nulls
	ROWAGG_COUNT_COL_NAME, // COUNT(column_name) only counts non-null rows
	ROWAGG_SUM,
	ROWAGG_AVG,
	ROWAGG_MIN,
	ROWAGG_MAX,

	// Statistics Function, ROWAGG_STATS is the generic name.
	ROWAGG_STATS,
	ROWAGG_STDDEV_POP,
	ROWAGG_STDDEV_SAMP,
	ROWAGG_VAR_POP,
	ROWAGG_VAR_SAMP,

	// BIT Function, ROWAGG_BIT_OP is the generic name.
	ROWAGG_BIT_OP,
	ROWAGG_BIT_AND,
	ROWAGG_BIT_OR,
	ROWAGG_BIT_XOR,

	// GROUP_CONCAT
	ROWAGG_GROUP_CONCAT,

	// DISTINCT: performed on UM only
	ROWAGG_COUNT_DISTINCT_COL_NAME, // COUNT(distinct column_name) only counts non-null rows
	ROWAGG_DISTINCT_SUM,
	ROWAGG_DISTINCT_AVG,

	// Constant
	ROWAGG_CONSTANT,

	// internal function type to avoid duplicate the work
	// handling ROWAGG_COUNT_NO_OP, ROWAGG_DUP_FUNCT and ROWAGG_DUP_AVG is a little different
	// ROWAGG_COUNT_NO_OP  :  count done by AVG, no need to copy
	// ROWAGG_DUP_FUNCT    :  copy data before AVG calculation, because SUM may share by AVG
	// ROWAGG_DUP_AVG      :  copy data after AVG calculation
	ROWAGG_COUNT_NO_OP,    // COUNT(column_name), but leave count() to AVG
	ROWAGG_DUP_FUNCT,      // duplicate aggregate Function(), except AVG, in select
	ROWAGG_DUP_AVG,        // duplicate AVG(column_name) in select
	ROWAGG_DUP_STATS       // duplicate statistics functions in select

};


//------------------------------------------------------------------------------
/** @brief Specifies a column in a RowGroup that is part of the aggregation
 *   "GROUP BY" clause.
 */
//------------------------------------------------------------------------------
struct RowAggGroupByCol
{
	/** @brief RowAggGroupByCol constructor
	 *
	 * @param inputColIndex(in) column index into input row
	 * @param outputColIndex(in) column index into output row
	 *    outputColIndex argument should be omitted if this GroupBy
	 *    column is not to be included in the output.
	 */
	RowAggGroupByCol(int32_t inputColIndex, int32_t outputColIndex=-1) :
		fInputColumnIndex(inputColIndex), fOutputColumnIndex(outputColIndex) {}
	~RowAggGroupByCol() {}

	uint32_t	fInputColumnIndex;
	uint32_t	fOutputColumnIndex;
};

inline messageqcpp::ByteStream& operator<<(messageqcpp::ByteStream& b, RowAggGroupByCol& o)
{ return (b << o.fInputColumnIndex << o.fOutputColumnIndex); }
inline messageqcpp::ByteStream& operator>>(messageqcpp::ByteStream& b, RowAggGroupByCol& o)
{ return (b >> o.fInputColumnIndex >> o.fOutputColumnIndex); }

//------------------------------------------------------------------------------
/** @brief Specifies a column in a RowGroup that is to be aggregated, and what
 *   aggregation function is to be performed.
 *
 *   If a column is aggregated more than once(ex: SELECT MIN(l_shipdate),
 *   MAX(l_shipdate)...), then 2 RowAggFunctionCol objects should be created
 *   with the same inputColIndex, one for the MIN function, and one for the
 *   MAX function.
 */
//------------------------------------------------------------------------------
struct RowAggFunctionCol
{
	/** @brief RowAggFunctionCol constructor
	 *
	 * @param aggFunction(in)    aggregation function to be performed
	 * @param inputColIndex(in)  column index into input row
	 * @param outputColIndex(in) column index into output row
	 * @param auxColIndex(in)    auxiliary index into output row for avg/count
	 * @param stats(in)          real statistics function where generic name in aggFunction
	 */
	RowAggFunctionCol(RowAggFunctionType aggFunction, RowAggFunctionType stats,
		int32_t inputColIndex, int32_t outputColIndex, int32_t auxColIndex = -1) :
			fAggFunction(aggFunction), fStatsFunction(stats), fInputColumnIndex(inputColIndex),
			fOutputColumnIndex(outputColIndex), fAuxColumnIndex(auxColIndex) {}
	~RowAggFunctionCol() {}

	RowAggFunctionType  fAggFunction;      // aggregate function
	// statistics function stores ROWAGG_STATS in fAggFunction and real function in fStatsFunction
	RowAggFunctionType  fStatsFunction;

	uint32_t            fInputColumnIndex;
	uint32_t            fOutputColumnIndex;

	// fAuxColumnIndex is used in 3 cases:
	// 1. for AVG - point to the count column, the fInputColumnIndex is for sum
	// 2. for statistics function - point to sum(x), +1 is sum(x**2)
	// 3. for duplicate - point to the real aggretate column to be copied from
	// Set only on UM, the fAuxColumnIndex is defaulted to fOutputColumnIndex+1 on PM.
	uint32_t            fAuxColumnIndex;
};

inline messageqcpp::ByteStream& operator<<(messageqcpp::ByteStream& b, RowAggFunctionCol& o)
{ return (b << (uint8_t)o.fAggFunction << o.fInputColumnIndex << o.fOutputColumnIndex); }
inline messageqcpp::ByteStream& operator>>(messageqcpp::ByteStream& b, RowAggFunctionCol& o)
{ return (b >> (uint8_t&)o.fAggFunction >> o.fInputColumnIndex >> o.fOutputColumnIndex); }

typedef boost::shared_ptr<RowAggGroupByCol>  SP_ROWAGG_GRPBY_t;
typedef boost::shared_ptr<RowAggFunctionCol> SP_ROWAGG_FUNC_t;

struct ConstantAggData
{
	std::string        fConstValue;
	RowAggFunctionType fOp;
	bool               fIsNull;

	ConstantAggData() : fOp(ROWAGG_FUNCT_UNDEFINE), fIsNull(false)
	{}

	ConstantAggData(const std::string& v, RowAggFunctionType f, bool n) :
		fConstValue(v), fOp(f), fIsNull(n)
	{}
};


struct GroupConcat
{
	// GROUP_CONCAT(DISTINCT col1, 'const', col2 ORDER BY col3 desc SEPARATOR 'sep')
	std::vector<std::pair<uint, uint> > fGroupCols;    // columns to concatenate, and position
	std::vector<std::pair<uint, bool> > fOrderCols;    // columns to order by [asc/desc]
	std::string                         fSeparator;
	std::vector<std::pair<std::string, uint> >  fConstCols; // constant columns in group
	bool                                fDistinct;
	uint64_t                            fSize;

	RowGroup                            fRowGroup;
	boost::shared_array<int>            fMapping;
	std::vector<std::pair<int, bool> >  fOrderCond;    // position to order by [asc/desc]
	joblist::ResourceManager*           fRm;           // resource manager

	GroupConcat() : fRm(NULL) {}
};

typedef boost::shared_ptr<GroupConcat>  SP_GroupConcat;


class GroupConcatAg
{
public:
	GroupConcatAg(SP_GroupConcat&);
	virtual ~GroupConcatAg();

	virtual void initialize() {};
	virtual void processRow(const rowgroup::Row&) {};
	virtual void merge(const rowgroup::Row&, uint64_t) {};

	void getResult(uint8_t*) {};

protected:
	rowgroup::SP_GroupConcat              fGroupConcat;
};

typedef boost::shared_ptr<GroupConcatAg>  SP_GroupConcatAg;


//------------------------------------------------------------------------------
/** @brief Class that aggregates RowGroups.
 */
//------------------------------------------------------------------------------
class RowAggregation : public messageqcpp::Serializeable
{
	public:
		/** @brief RowAggregation default constructor
		 *
		 * @param rowAggGroupByCols(in) specify GroupBy columns and their
		 *    mapping from input to output.  If vector is empty, then all the
		 *    rows will be aggregated into a single implied group.  Order is
		 *    important here. The primary GroupBy column should be first, the
		 *    secondary GroupBy column should be second, etc.
		 * @param rowAggFunctionCols(in) specify function columns and their
		 *    mapping from input to output.
		 */
		RowAggregation();
		RowAggregation(const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
					   const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols);
		RowAggregation(const RowAggregation& rhs);

		/** @brief RowAggregation default destructor
		 */
		virtual ~RowAggregation();

		/** @brief clone this object for multi-thread use
		 */
		inline virtual RowAggregation* clone() const { return new RowAggregation (*this); }

		/** @brief Denotes end of data insertion following multiple calls to addRowGroup().
		 */
		virtual void endOfInput();

		/** @brief reset RowAggregation outputRowGroup and hashMap
		 */
		void reset();

		/** @brief Define content of data to be aggregated and its aggregated output.
		 *
		 * @param pRowGroupIn(in)   contains definition of the input data.
		 * @param pRowGroupOut(out) contains definition of the output data.
		 */
		virtual void setInputOutput(const RowGroup &pRowGroupIn, RowGroup* pRowGroupOut)
		{ fRowGroupIn = pRowGroupIn; fRowGroupOut = pRowGroupOut; initialize();}


		/** @brief Define content of data to be joined
		 *
		 *    This method must be call after setInputOutput() for PM hashjoin case.
		 *
		 * @param pSmallSideRG(in) contains definition of the small side data.
		 * @param pLargeSideRG(in) contains definition of the large side data.
		 */
		void setJoinRowGroups(std::vector<RowGroup> *pSmallSideRG, RowGroup *pLargeSideRG);

		/** @brief Returns group by column vector
		 *
		 * This function is used to duplicate the RowAggregation object
		 *
		 * @returns a reference of the group by vector
		 */
		std::vector<SP_ROWAGG_GRPBY_t>&	getGroupByCols() { return fGroupByCols; }

		/** @brief Returns aggregate function vector
		 *
		 * This function is used to duplicate the RowAggregation object
		 *
		 * @returns a reference of the aggregation function vector
		 */
		std::vector<SP_ROWAGG_FUNC_t>&	getAggFunctions() { return fFunctionCols; }

		/** @brief Add a group of rows to be aggregated.
		 *
		 * This function can be called to iteratively add RowGroups for aggregation.
		 *
		 * @parm pRowGroupIn(in) RowGroup to be added to aggregation.
		 */
		virtual void addRowGroup(const RowGroup* pRowGroupIn);
		virtual void addRowGroup(const RowGroup* pRowGroupIn, std::vector<uint8_t*>& inRows);

		/** @brief Apply the hashjoin result and perform aggregation.
		 *
		 * This function avoids the creation of the hashjoin results set.
		 * Called on PM after the joiner has built the match table.
		 *
		 * @parm pRowGroupProj(in)  Partially filled project RowGroup.
		 * @parm matches(in)        Hashjoin matched entries
		 */
// 		void addHashJoinRowGroup(const RowGroup &pRowGroupLarge,
// 			const boost::shared_array<std::vector<std::vector<uint32_t> > >& matches,
// 			const boost::shared_array<boost::shared_array<uint8_t> > &rowData);

		/** @brief Serialize RowAggregation object into a ByteStream.
		 *
		 * @parm bs(out) BytesStream that is to be written to.
		 */
		void serialize(messageqcpp::ByteStream& bs) const;

		/** @brief Unserialize RowAggregation object from a ByteStream.
		 *
		 * @parm bs(in) BytesStream that is to be read from.
		 */
		void deserialize(messageqcpp::ByteStream& bs);

		/** @brief set the memory limit for RowAggregation
		 *
		 * @parm limit(in) memory limit for both Map and secondary RowGroups
		 */
		void setMaxMemory(uint64_t limit) { fMaxMemory = limit; }

		/** @brief load result set into byte stream
		 *
		 * @parm bs(out) BytesStream that is to be written to.
		 */
		void loadResult(messageqcpp::ByteStream& bs);
		void loadEmptySet(messageqcpp::ByteStream& bs);

		/** @brief get output rowgroup
		 *
		 * @returns a const pointer of the output rowgroup
		 */
		const RowGroup* getOutputRowGroup() const { return fRowGroupOut; }
		RowGroup* getOutputRowGroup() { return fRowGroupOut; }

		uint32_t aggMapKeyLength() { return fAggMapKeyLength; }
		RowAggMap_t* mapPtr() {return fAggMapPtr;}
		std::vector<uint8_t*>& resultDataVec() { return fResultDataVec; }

		void aggregateRow(Row& row);

	protected:
		virtual void initialize();
		virtual void calculateMapKeyLength();
		virtual uint8_t* makeMapKey(const uint8_t* key) { return (fRow.getData() + 2); }
		virtual void initMapData(const Row& row);
		virtual void attachGroupConcatAg();

		virtual void updateEntry(const Row& row);
		virtual void doMinMaxSum(const Row&, int64_t, int64_t, int);
		virtual void doAvg(const Row&, int64_t, int64_t, int64_t);
		virtual void doStatistics(const Row&, int64_t, int64_t, int64_t);
		virtual void doBitOp(const Row&, int64_t, int64_t, int);
		virtual bool countSpecial(const RowGroup* pRG)
		{ fRow.setIntField<8>(fRow.getIntField<8>(0) + pRG->getRowCount(), 0); return true; }

		virtual bool newRowGroup();
		virtual void clearAggMap() { if (fAggMapPtr) fAggMapPtr->clear(); }

		inline bool isNull(const RowGroup* pRowGroup, const Row& row, int64_t col);
		inline void makeAggFieldsNull(Row& row);
		inline void copyNullRow(Row& row)
			{ memcpy(row.getData(), fNullRowData.get(), row.getSize()); }

		inline void updateIntMinMax(int64_t val1, int64_t val2, int64_t col, int func);
		inline void updateUintMinMax(uint64_t val1, uint64_t val2, int64_t col, int func);
		inline void updateCharMinMax(uint64_t val1, uint64_t val2, int64_t col, int func);
		inline void updateDoubleMinMax(double val1, double val2, int64_t col, int func);
		inline void updateFloatMinMax(float val1, float val2, int64_t col, int func);
		inline void updateStringMinMax(std::string val1, std::string val2, int64_t col, int func);
		inline void updateIntSum(int64_t val1, int64_t val2, int64_t col);
		inline void updateDoubleSum(double val1, double val2, int64_t col);
		inline void updateFloatSum(float val1, float val2, int64_t col);

		std::vector<SP_ROWAGG_GRPBY_t>                  fGroupByCols;
		std::vector<SP_ROWAGG_FUNC_t>                   fFunctionCols;
		RowAggMap_t*                                    fAggMapPtr;
		uint32_t                                        fAggMapKeyLength;
		RowGroup                                        fRowGroupIn;
		RowGroup*                                       fRowGroupOut;

		Row                                             fRow;
		boost::scoped_array<uint8_t>                    fNullRowData;
		std::vector<uint8_t*>                           fResultDataVec;

		uint64_t                                        fTotalRowCount;
		uint64_t                                        fMaxTotalRowCount;
		uint64_t                                        fMaxMemory;

		uint8_t*                                        fPrimaryRowData;

		boost::shared_array<uint8_t>                    fSecondaryRowData;
		std::vector<boost::shared_array<uint8_t> >      fSecondaryRowDataVec;

		// for support PM aggregation after PM hashjoin
		std::vector<RowGroup>*                          fSmallSideRGs;
		RowGroup*                                       fLargeSideRG;
		boost::shared_array<boost::shared_array<int> >  fSmallMappings;
		boost::shared_array<int>                        fLargeMapping;
		uint                                            fSmallSideCount;
		boost::scoped_array<Row> rowSmalls;

		// for hashmap
		boost::shared_ptr<utils::STLPoolAllocator<std::pair<uint8_t* const, uint8_t*> > > fAlloc;
		// for keys in the map if they are allocated seperately
		boost::shared_ptr<utils::STLPoolAllocator<uint8_t> > fKeyAlloc;

		// for 8k poc
		RowGroup                                        fEmptyRowGroup;
		boost::scoped_array<uint8_t>                    fEmptyRowData;
		Row                                             fEmptyRow;
};


//------------------------------------------------------------------------------
/** @brief derived Class that aggregates multi-rowgroups on UM
 *    One-phase case: aggregate from projected RG to final aggregated RG.
 */
//------------------------------------------------------------------------------
class RowAggregationUM : public RowAggregation
{
	public:
		/** @brief RowAggregationUM constructor
		 */
		RowAggregationUM() {}
		RowAggregationUM(
			const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
			const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
			joblist::ResourceManager *);
		RowAggregationUM(const RowAggregationUM& rhs);

		/** @brief RowAggregationUM default destructor
		 */
		~RowAggregationUM();

		/** @brief Denotes end of data insertion following multiple calls to addRowGroup().
		 */
		void endOfInput();

		/** @brief Finializes the result set before sending back to the front end.
		 */
		void finalize();

		/** @brief Returns aggregated rows in a RowGroup.
		 *
		 * This function should be called repeatedly until false is returned (meaning end of data).
		 *
		 * @returns true if more data, else false if no more data.
		 */
		bool nextRowGroup();

		/** @brief Add an aggregator for DISTINCT aggregation
		 */
		void distinctAggregator(const boost::shared_ptr<RowAggregation>& da)
			{ fDistinctAggregator = da; }

		/** @brief expressions to be evaluated after aggregation
		 */
		void expression(const std::vector<execplan::SRCP>& exp) { fExpression = exp; }
		const std::vector<execplan::SRCP>& expression() { return fExpression; }

		// for multi threaded
		joblist::ResourceManager* getRm() {return fRm;}
		inline virtual RowAggregationUM* clone() const { return new RowAggregationUM (*this); }

		/** @brief access the aggregate(constant) columns
		 */
		void constantAggregate(const std::vector<ConstantAggData>& v) { fConstantAggregate = v; }
		const std::vector<ConstantAggData>& constantAggregate() const { return fConstantAggregate; }

		/** @brief access the group_concat
		 */
		void groupConcat(const std::vector<SP_GroupConcat>& v) { fGroupConcat = v; }
		const std::vector<SP_GroupConcat>& groupConcat() const { return fGroupConcat; }

	protected:
		// virtual methods from base
		void initialize();
		uint8_t* makeMapKey(const uint8_t* key);
		void attachGroupConcatAg();
		void updateEntry(const Row& row);
		bool countSpecial(const RowGroup* pRG)
		{ fRow.setIntField<8>(
		    fRow.getIntField<8>(
		        fFunctionCols[0]->fOutputColumnIndex) + pRG->getRowCount(),
		        fFunctionCols[0]->fOutputColumnIndex);
		  return true; }

		bool newRowGroup();

		// calculate the average after all rows received. UM only function.
		void calculateAvgColumns();

		// calculate the statistics function all rows received. UM only function.
		void calculateStatisticsFunctions();

		// fix duplicates. UM only function.
		void fixDuplicates(RowAggFunctionType funct);

		// evaluate expressions
		virtual void evaluateExpression();

		// fix the aggregate(constant)
		virtual void fixConstantAggregate();
		virtual void doNullConstantAggregate(const ConstantAggData&, uint64_t);
		virtual void doNotNullConstantAggregate(const ConstantAggData&, uint64_t);

		// @bug3362, group_concat
		virtual void doGroupConcat(const Row&, int64_t, int64_t);
		virtual void setGroupConcatString();

		bool fHasAvg;
		bool fKeyOnHeap;
		bool fHasStatsFunc;

		boost::shared_ptr<RowAggregation> fDistinctAggregator;

		// for function on aggregation
		std::vector<execplan::SRCP>       fExpression;

		/* Derived classes that use a lot of memory need to update totalMemUsage and request
		 * the memory from rm in that order. */
		uint64_t                          fTotalMemUsage;

		joblist::ResourceManager*         fRm;

		// @bug3475, aggregate(constant), sum(0), count(null), etc
		std::vector<ConstantAggData>      fConstantAggregate;

		// @bug3362, group_concat
		std::vector<SP_GroupConcat>       fGroupConcat;
		std::vector<SP_GroupConcatAg>     fGroupConcatAg;
		std::vector<SP_ROWAGG_FUNC_t>     fFunctionColGc;


	private:
		uint64_t fLastMemUsage;
};


//------------------------------------------------------------------------------
/** @brief derived Class that aggregates PM partially aggregated RowGroups on UM
 *    Two-phase case:
 *      phase 1 - aggregate from projected RG to partial aggregated RG on PM
 *                The base RowAggregation handles the 1st phase.
 *      phase 2 - aggregate from partially aggregated RG to final RG on UM
 *                This class handles the 2nd phase.
 */
//------------------------------------------------------------------------------
class RowAggregationUMP2 : public RowAggregationUM
{
	public:
		/** @brief RowAggregationUM constructor
		 */
		RowAggregationUMP2() {}
		RowAggregationUMP2(
			const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
			const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
			joblist::ResourceManager *);
		RowAggregationUMP2(const RowAggregationUMP2& rhs);

		/** @brief RowAggregationUMP2 default destructor
		 */
		~RowAggregationUMP2();
		inline virtual RowAggregationUMP2* clone() const { return new RowAggregationUMP2 (*this); }

	protected:
		// virtual methods from base
		void updateEntry(const Row& row);
		void doAvg(const Row&, int64_t, int64_t, int64_t);
		void doStatistics(const Row&, int64_t, int64_t, int64_t);
		void doGroupConcat(const Row&, int64_t, int64_t);
		void doBitOp(const Row&, int64_t, int64_t, int);
		bool countSpecial(const RowGroup* pRG) { return false; }
};



//------------------------------------------------------------------------------
/** @brief derived Class that aggregates on distinct columns on UM
 *    The internal aggregator will handle one or two phases aggregation
 */
//------------------------------------------------------------------------------
class RowAggregationDistinct : public RowAggregationUMP2
{
	public:
		/** @brief RowAggregationDistinct constructor
		 */
		RowAggregationDistinct() {}
		RowAggregationDistinct(
			const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
			const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
			joblist::ResourceManager *);

		/** @brief Copy Constructor for multi-threaded aggregation
		 */
		RowAggregationDistinct(const RowAggregationDistinct& rhs);

		/** @brief RowAggregationDistinct default destructor
		 */
		~RowAggregationDistinct();

		/** @brief Add an aggregator for pre-DISTINCT aggregation
		 */
		void addAggregator(const boost::shared_ptr<RowAggregation>& agg, const RowGroup& rg);

		void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);

		virtual void doDistinctAggregation();
		virtual void doDistinctAggregation_rowVec(std::vector<uint8_t*>& inRows);
		void addRowGroup(const RowGroup* pRowGroupIn);
		void addRowGroup(const RowGroup* pRowGroupIn, std::vector<uint8_t*>& inRows);

		// multi-threade debug
		boost::shared_ptr<RowAggregation>& aggregator() { return fAggregator; }
		void aggregator(boost::shared_ptr<RowAggregation> aggregator) {fAggregator = aggregator;}
		RowGroup& rowGroupDist() { return fRowGroupDist; }
		void rowGroupDist(RowGroup& rowGroupDist) {fRowGroupDist = rowGroupDist;}
		inline virtual RowAggregationDistinct* clone() const { return new RowAggregationDistinct (*this); }

	protected:
		// virtual methods from base
		void updateEntry(const Row& row);

		boost::shared_ptr<RowAggregation>   fAggregator;
		RowGroup                            fRowGroupDist;
		boost::scoped_array<uint8_t>        fDataForDist;
};


//------------------------------------------------------------------------------
/** @brief derived Class for aggregates multiple columns with distinct key word
 *    Get distinct values of the column per group by entry
 */
//------------------------------------------------------------------------------
class RowAggregationSubDistinct : public RowAggregationUM
{
	public:
		/** @brief RowAggregationSubDistinct constructor
		 */
		RowAggregationSubDistinct() {}
		RowAggregationSubDistinct(
			const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
			const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
			joblist::ResourceManager *);
		RowAggregationSubDistinct(const RowAggregationSubDistinct& rhs);

		/** @brief RowAggregationSubDistinct default destructor
		 */
		~RowAggregationSubDistinct();

		void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);
		void addRowGroup(const RowGroup* pRowGroupIn);
		inline virtual RowAggregationSubDistinct* clone() const
		{
			return new RowAggregationSubDistinct (*this);
		}

		void addRowGroup(const RowGroup* pRowGroupIn, std::vector<uint8_t*>& inRow);

	protected:
		// virtual methods from RowAggregationUM
		void doGroupConcat(const Row&, int64_t, int64_t);

		// for groupby columns and the aggregated distinct column
		Row                                             fDistRow;
		boost::scoped_array<uint8_t>                    fDistRowData;
};


//------------------------------------------------------------------------------
/** @brief derived Class that aggregates multiple columns with distinct key word
 *    Each distinct column will have its own aggregator
 */
//------------------------------------------------------------------------------
class RowAggregationMultiDistinct : public RowAggregationDistinct
{
	public:
		/** @brief RowAggregationMultiDistinct constructor
		 */
		RowAggregationMultiDistinct() {}
		RowAggregationMultiDistinct(
			const std::vector<SP_ROWAGG_GRPBY_t>& rowAggGroupByCols,
			const std::vector<SP_ROWAGG_FUNC_t>&  rowAggFunctionCols,
			joblist::ResourceManager *);
		RowAggregationMultiDistinct(const RowAggregationMultiDistinct& rhs);

		/** @brief RowAggregationMultiDistinct default destructor
		 */
		~RowAggregationMultiDistinct();

		/** @brief Add sub aggregators
		 */
		void addSubAggregator(const boost::shared_ptr<RowAggregationUM>& agg,
							  const RowGroup& rg,
							  const std::vector<SP_ROWAGG_FUNC_t>& funct);

		void setInputOutput(const RowGroup& pRowGroupIn, RowGroup* pRowGroupOut);
		void addRowGroup(const RowGroup* pRowGroupIn);

		virtual void doDistinctAggregation();
		virtual void doDistinctAggregation_rowVec(std::vector<std::vector<uint8_t*> >& inRows);

		inline virtual RowAggregationMultiDistinct* clone() const
		{
			return new RowAggregationMultiDistinct (*this);
		}

		void addRowGroup(const RowGroup* pRowGroupIn, std::vector<std::vector<uint8_t*> >& inRows);

		std::vector<boost::shared_ptr<RowAggregationUM> >& subAggregators()
		{
			return fSubAggregators;
		}

		void subAggregators(std::vector<boost::shared_ptr<RowAggregationUM> >& subAggregators)
		{
			fSubAggregators = subAggregators;
		}

	protected:
		// virtual methods from base
		std::vector<boost::shared_ptr<RowAggregationUM> > fSubAggregators;
		std::vector<RowGroup>                             fSubRowGroups;
		std::vector<boost::shared_array<uint8_t> >        fSubRowData;
		std::vector<std::vector<SP_ROWAGG_FUNC_t> >       fSubFunctions;
};



typedef boost::shared_ptr<RowAggregation>           SP_ROWAGG_t;
typedef boost::shared_ptr<RowAggregation>           SP_ROWAGG_PM_t;
typedef boost::shared_ptr<RowAggregationUM>         SP_ROWAGG_UM_t;
typedef boost::shared_ptr<RowAggregationDistinct>   SP_ROWAGG_DIST;

} // end of rowgroup namespace

#endif  // ROWAGGREGATION_H

