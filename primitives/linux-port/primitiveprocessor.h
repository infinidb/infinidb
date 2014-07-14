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

/******************************************************************************
 * $Id: primitiveprocessor.h 2035 2013-01-21 14:12:19Z rdempsey $
 *
 *****************************************************************************/

/** @file */

#ifndef PRIMITIVEPROCESSOR_H_
#define PRIMITIVEPROCESSOR_H_

#include <stdexcept>
#include <vector>
#ifndef _MSC_VER
#include <tr1/unordered_set>
#else
#include <unordered_set>
#endif

#ifdef __linux__
#define POSIX_REGEX
#endif

#ifdef POSIX_REGEX
#include <regex.h>
#else
#include <boost/regex.hpp>
#endif
#include <cstddef>
#include <boost/shared_ptr.hpp>
#include <boost/shared_array.hpp>

#include "primitivemsg.h"
#include "calpontsystemcatalog.h"
#include "stats.h"
#include "primproc.h"
#include "hasher.h"

class PrimTest;

namespace primitives
{

enum ColumnFilterMode {
	STANDARD,
	TWO_ARRAYS,
	UNORDERED_SET
};

class pcfHasher
{
	public:
		inline size_t operator()(const int64_t i) const
		{
			return i;
		}
};

class pcfEqual
{
	public:
		inline size_t operator()(const int64_t f1, const int64_t f2) const
		{
			return f1 == f2;
		}
};

typedef std::tr1::unordered_set<int64_t, pcfHasher, pcfEqual> prestored_set_t;
typedef std::tr1::unordered_set<std::string, utils::Hasher> DictEqualityFilter;

struct idb_regex_t
{
#ifdef POSIX_REGEX
	regex_t regex;
#else
	boost::regex regex;
#endif
	bool used;
	idb_regex_t() : used(false) { }
	~idb_regex_t() {
#ifdef POSIX_REGEX
		if (used)
			regfree(&regex);
#endif
	}
};

struct ParsedColumnFilter {
	ColumnFilterMode columnFilterMode;
	boost::shared_array<int64_t> prestored_argVals;
	boost::shared_array<uint8_t> prestored_cops;
	boost::shared_array<uint8_t> prestored_rfs;
	boost::shared_ptr<prestored_set_t> prestored_set;
	boost::shared_array<idb_regex_t> prestored_regex;
	uint8_t  likeOps;

	ParsedColumnFilter();
	~ParsedColumnFilter();
};

//@bug 1828 These need to be public so that column operations can use it for 'like'
struct p_DataValue {
	int len;
	const uint8_t *data;
};

boost::shared_ptr<ParsedColumnFilter> parseColumnFilter(const uint8_t *filterString,
	uint32_t colWidth, uint32_t colType, uint32_t filterCount, uint32_t BOP);

/** @brief This class encapsulates the primitive processing functionality of the system.
 *
 *  This class encapsulates the primitive processing functionality of the system.
 */
class PrimitiveProcessor
{
public:
	PrimitiveProcessor(int debugLevel=0);
	virtual ~PrimitiveProcessor();

	/** @brief Sets the block to operate on
	 *
	 * The primitive processing functions operate on one block at a time.  The caller
	 * sets which block to operate on next with this function.
	 */
	void setBlockPtr(int *data)
    {
    	block = data;
    }
	void setPMStatsPtr(dbbc::Stats* p)
	{
		fStatsPtr=p;
	}


	/** @brief The interface to Mark's NIOS primitive processing code.
	 *
	 * The interface to Mark's NIOS primitive processing code.  Instead of reading
	 * and writing to a bus, it will read/write to buffers specified by inBuf
	 * and outBuf.  The primitives implemented this way are:
	 * - p_Col and p_ColAggregate
	 * - p_GetSignature
	 *
	 * @param inBuf (in) The buffer containing a command to execute
	 * @param inLength (in) The size of inBuf in 4-byte words
	 * @param outBuf (in) The buffer to store the output in
	 * @param outLength (in) The size of outBuf in 4-byte words
	 * @param written (out) The number of bytes written to outBuf.
	 * @note Throws logic_error if the output buffer is too small for the result.
	 */
	void processBuffer(int *inBuf, unsigned inLength, int *outBuf, unsigned outLength,
	 unsigned *written);

	/* Patrick */

	/** @brief The p_TokenByScan primitive processor
	 *
	 * The p_TokenByScan primitive processor.  It relies on the caller setting
	 * the block to operate on with setBlockPtr().  It assumes the continuation
	 * pointer is not used.
	 * @param t (in) The arguments to the primitive
	 * @param out (out) This must point to memory of some currently unknown max size
	 * @param outSize (in) The size of the output buffer in bytes.
	 * @note Throws logic_error if the output buffer is too small for the result.
	 */
	void p_TokenByScan(const TokenByScanRequestHeader *t,
		TokenByScanResultHeader *out, unsigned outSize,bool utf8,
		boost::shared_ptr<DictEqualityFilter> eqFilter);

	/** @brief The p_IdxWalk primitive processor
	 *
	 * The p_IdxWalk primitive processor.  The caller must set the block to operate
	 * on with setBlockPtr().  This primitive can return intermediate results.
	 * All results returned will have an different LBID than the input.  They can
	 * also be in varying states of completion.  A result is final when
	 * Shift >= SSlen, otherwise it is intermediate and needs to be reissued with
	 * the specified LBID loaded.
	 * @note If in->NVALS > 2, new vectors may be returned in the result set, which
	 * will have to be deleted by the caller.  The test to use right now is
	 * ({element}->NVALS > 2 && {element}->State == 0).  If that condition is true,
	 * delete the vector, otherwise don't.  This kludginess is for efficiency's sake
	 * and may go away for the sake of sanity later.
	 * @note It is safe to delete any vector passed in after the call.
	 * @param out The caller should pass in an empty vector.  The results
	 * will be returned as elements of this vector.
	 */
	void p_IdxWalk(const IndexWalkHeader *in, std::vector<IndexWalkHeader *> *out) throw();

	/** @brief The p_IdxList primitive processor.
	 *
	 * The p_IdxList primitive processor.  The caller must set the block to operate
	 * on with setBlockPtr().  This primitive can return one intermediate result
	 * for every call made.  If there is an intermediate result returned, it will
	 * be the first element, distinguished by its type field.  If the
	 * first element has a type == RID (3) , there is no intermediate result.  If
	 * the first element had a type == LLP_SUBBLK (4) or type == LLP_BLK (5),
	 * that element is the intermediate result.  Its value field will be a pointer
	 * to the next section of the list.
	 *
	 * @param rqst (in) The request header followed by NVALS IndexWalkParams
	 * @param rslt (out) The caller passes in a buffer which will be filled
	 * by the primitive on return.  It will consist of an IndexListHeader,
	 * followed by NVALS IndexListEntrys.
	 * @param mode (optional, in) 0 specifies old behavior (the last entry of a block might
	 * be a pointer).  1 specifies new behavior (the last entry should be ignored).
	 */
	void p_IdxList(const IndexListHeader *rqst, IndexListHeader *rslt, int mode = 1);

	/** @brief The p_AggregateSignature primitive processor.
	 *
	 * The p_AggregateSignature primitive processor.  It operates on a dictionary
	 * block and assumes the continuation pointer is not used.
	 * @param in The input parameters
	 * @param out A pointer to a buffer where the result will be written.
	 * @param outSize The size of the output buffer in bytes.
	 * @param written (out parameter) A pointer to 1 int, which will contain the
	 * number of bytes written to out.
	 */
	void p_AggregateSignature(const AggregateSignatureRequestHeader *in,
		AggregateSignatureResultHeader *out, unsigned outSize, unsigned *written, bool utf8);

	/** @brief The p_Col primitive processor.
	 *
	 * The p_Col primitive processor.  It operates on a column block specified using setBlockPtr().
	 * @param in The buffer containing the command parameters.
	 * 		The buffer should begin with a NewColRequestHeader structure, followed by
	 * 		an array of 'NOPS' defining the filter to apply (optional),
	 * 		followed by an array of RIDs to apply the filter to (optional).
	 * @param out The buffer that will contain the results.  On return, it will start with
	 * a NewColResultHeader, followed by the output type specified by in->OutputType.
	 * \li If OT_RID, it will be an array of RIDs
	 * \li If OT_DATAVALUE, it will be an array of matching data values stored in the column
	 * \li If OT_BOTH, it will be an array of <DataValue, RID> pairs
	 * @param outSize The size of the output buffer in bytes.
	 * @param written (out parameter) A pointer to 1 int, which will contain the
	 * number of bytes written to out.
	 * @note See PrimitiveMsg.h for the type definitions.
	 */
	void p_Col(NewColRequestHeader *in, NewColResultHeader *out, unsigned outSize,
		unsigned *written);

	boost::shared_ptr<ParsedColumnFilter> parseColumnFilter(const uint8_t *filterString,
		uint32_t colWidth, uint32_t colType, uint32_t filterCount, uint32_t BOP);
	void setParsedColumnFilter(boost::shared_ptr<ParsedColumnFilter>);

	/** @brief The p_ColAggregate primitive processor.
	 *
	 * The p_ColAggregate primitive processor.  It operates on a column block
	 * specified using setBlockPtr().
	 * @param in The buffer containing the command parameters.  The buffer should begin
	 *		with a NewColAggRequestHeader, followed by an array of RIDs to generate
	 * 		the data for (optional).
	 * @param out The buffer to put the result in.  On return, it will contain a
	 * NewCollAggResultHeader.
	 * @note See PrimitiveMsg.h for the type definitions.
	 */
//	void p_ColAggregate(const NewColAggRequestHeader *in, NewColAggResultHeader *out);

	void p_Dictionary(const DictInput *in, std::vector<uint8_t> *out, bool utf8,
			bool skipNulls, boost::shared_ptr<DictEqualityFilter> eqFilter,
			uint8_t eqOp);

	inline void setLogicalBlockMode(bool b) { logicalBlockMode = b; }



	static int convertToRegexp(idb_regex_t *regex, const p_DataValue *str);
	inline static bool isEscapedChar(char c);
	boost::shared_array<idb_regex_t> makeLikeFilter(const DictFilterElement *inputMsg, uint32_t count);
	void setLikeFilter(boost::shared_array<idb_regex_t> filter) { parsedLikeFilter = filter; }

private:
	PrimitiveProcessor(const PrimitiveProcessor& rhs);
	PrimitiveProcessor& operator=(const PrimitiveProcessor& rhs);

	int *block;

	bool compare(int cmpResult, uint8_t COP, int len1, int len2) throw();
	int compare(int val1, int val2, uint8_t COP, bool lastStage) throw();
	void indexWalk_1(const IndexWalkHeader *in, std::vector<IndexWalkHeader *> *out) throw();
	void indexWalk_2(const IndexWalkHeader *in, std::vector<IndexWalkHeader *> *out) throw();
	void indexWalk_many(const IndexWalkHeader *in, std::vector<IndexWalkHeader *> *out) throw();
	void grabSubTree(const IndexWalkHeader *in, std::vector<IndexWalkHeader *> *out) throw();

	void nextSig(int NVALS, const PrimToken *tokens, p_DataValue *ret,
		uint8_t outputFlags = 0, bool oldGetSigBehavior = false, bool skipNulls = false) throw();
	bool isLike(const p_DataValue *dict, const idb_regex_t *arg) throw();

//	void do_sum8(NewColAggResultHeader *out, int64_t val);
//    void do_unsignedsum8(NewColAggResultHeader *out, int64_t val);

	uint64_t masks[11];
	int dict_OffsetIndex, currentOffsetIndex;		// used by p_dictionary
	int fDebugLevel;
	dbbc::Stats* fStatsPtr; // pointer for pmstats
	bool logicalBlockMode;

	boost::shared_ptr<ParsedColumnFilter> parsedColumnFilter;
	boost::shared_array<idb_regex_t> parsedLikeFilter;

	friend class ::PrimTest;
};

} //namespace primitives

#endif
// vim:ts=4 sw=4:

