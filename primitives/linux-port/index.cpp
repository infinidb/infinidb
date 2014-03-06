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

/*****************************************************************************
 * $Id: index.cpp 2035 2013-01-21 14:12:19Z rdempsey $
 *
 ****************************************************************************/

#include <iostream>
using namespace std;

#include "primitiveprocessor.h"
#include "we_index.h"
#include "messagelog.h"
#include "messageobj.h"

/** @file
 * Brief description of the file contents
 *
 * More detailed description
 */

using namespace logging;

#ifdef VERBOSE
#define GET_BITTEST(test, string, x) \
	if (in->Shift + x >= in->SSlen) { \
		test = (string & masks[in->SSlen - in->Shift]); \
		lastStage = true; \
 		cerr << "  bittest is 0x" << hex << (int) test << dec << " this is the last iteration" << endl; \
	} \
	else { \
		test = (string >> (in->SSlen - in->Shift - x)) & masks[x]; \
		lastStage = false; \
 		cerr << "  bittest is 0x" << hex << (int) test << dec << endl; \
	}
#else
#define GET_BITTEST(test, string, x) \
	if (in->Shift + x >= in->SSlen) { \
		test = (string & masks[in->SSlen - in->Shift]); \
		lastStage = true; \
	} \
	else { \
		test = (string >> (in->SSlen - in->Shift - x)) & masks[x]; \
		lastStage = false; \
	}
#endif

#define IDXWALK_INIT() \
	niceBlock = reinterpret_cast<uint8_t *>(block); \
	blockOffset = (in->SubBlock * WriteEngine::SUBBLOCK_TOTAL_BYTES) + \
		(in->SBEntry * 8); \
	treePtr = reinterpret_cast<WriteEngine::IdxBitTestEntry *>(&niceBlock[blockOffset]);

#define GET_GROUP_SIZE() \
		switch (treePtr->group) { \
			case 0: bitTestGroupSize = 1; break; \
			case 1: bitTestGroupSize = 2; break; \
			case 2: bitTestGroupSize = 4; break; \
			case 3: bitTestGroupSize = 8; break; \
			case 4: bitTestGroupSize = 16; break; \
			case 5: bitTestGroupSize = 32; break; \
			default: \
				cerr << "PrimitiveProcessor::IndexWalk*(): bad group field " << \
					treePtr->group << endl; \
				return; \
		}

#ifdef VERBOSE
#define ADD_ELEMENT(index, shift, state) \
	element = new IndexWalkHeader(); \
	memcpy(element, in, sizeof(IndexWalkHeader)); \
	element->ism.Command = INDEX_WALK_RESULTS; \
	element->Shift += shift; \
	element->LBID = treePtr[index].fbo; \
	element->SubBlock = treePtr[index].sbid; \
	element->SBEntry = treePtr[index].entry; \
	element->State = state; \
	cerr << "  (no convert) creating a result from subblock entry " << (int) index << " with Shift=" << (int) element->Shift << \
		" LBID=" << element->LBID << " Subblock=" << (int) element->SubBlock << " Subblock entry=" << \
		(int) element->SBEntry << " State=" << (int) state; \
	if (element->LBID == in->LBID && element->Shift < element->SSlen) { \
		cerr << "  recursing..." << endl; \
		p_IdxWalk(element, out); \
		delete element; \
	} \
	else { \
		cerr << "  adding this to the result set" << endl; \
		out->push_back(element); \
	}
#else
#define ADD_ELEMENT(index, shift, state) \
	element = new IndexWalkHeader(); \
	memcpy(element, in, sizeof(IndexWalkHeader)); \
	element->ism.Command = INDEX_WALK_RESULTS; \
	element->Shift += shift; \
	element->LBID = treePtr[index].fbo; \
	element->SubBlock = treePtr[index].sbid; \
	element->SBEntry = treePtr[index].entry; \
	element->State = state; \
	if (element->LBID == in->LBID && element->Shift < element->SSlen) { \
		p_IdxWalk(element, out); \
		delete element; \
	} \
	else \
		out->push_back(element);
#endif

#ifdef VERBOSE
#define ADD_ELEMENT_WITH_CONVERT(index, shift, state) \
	element = new IndexWalkHeader(); \
	memcpy(element, in, sizeof(IndexWalkHeader)); \
	element->ism.Command = INDEX_WALK_RESULTS; \
	element->Shift += shift; \
	element->LBID = treePtr[index].fbo; \
	element->SubBlock = treePtr[index].sbid; \
	element->SBEntry = treePtr[index].entry; \
	element->State = state; \
	cerr << "  (convert) creating a result from subblock entry " << (int) index << " with Shift=" << (int) element->Shift << \
		" LBID=" << element->LBID << " Subblock=" << (int) element->SubBlock << " Subblock entry=" << \
		(int) element->SBEntry << " State=" << (int) state << endl; \
	if (convertToSingleOp != -1) { \
		cerr << "  converting it to a single COP filter" << endl; \
		element->NVALS = 1; \
		if (convertToSingleOp == 1) { \
			element->COP1 = element->COP2; \
			element->SearchString[0] = element->SearchString[1]; \
		} \
	} \
	if (element->LBID == in->LBID && element->Shift < element->SSlen) { \
		cerr << "  recursing..." << endl; \
		p_IdxWalk(element, out); \
		delete element; \
	} \
	else { \
		cerr << "  adding it to the result set" << endl;\
		out->push_back(element); \
	}
#else
#define ADD_ELEMENT_WITH_CONVERT(index, shift, state) \
	element = new IndexWalkHeader(); \
	memcpy(element, in, sizeof(IndexWalkHeader)); \
	element->ism.Command = INDEX_WALK_RESULTS; \
	element->Shift += shift; \
	element->LBID = treePtr[index].fbo; \
	element->SubBlock = treePtr[index].sbid; \
	element->SBEntry = treePtr[index].entry; \
	element->State = state; \
	if (convertToSingleOp != -1) { \
		element->NVALS = 1; \
		if (convertToSingleOp == 1) { \
			element->COP1 = element->COP2; \
			element->SearchString[0] = element->SearchString[1]; \
		} \
	} \
	if (element->LBID == in->LBID && element->Shift < element->SSlen) { \
		p_IdxWalk(element, out); \
		delete element; \
	} \
	else \
		out->push_back(element);
#endif

namespace primitives
{

void PrimitiveProcessor::indexWalk_1(const IndexWalkHeader *in, 
	vector<IndexWalkHeader *> *out) throw()
{
	uint16_t bitTest;
	uint8_t *niceBlock;
	int blockOffset, bitTestGroupSize, i;
	WriteEngine::IdxBitTestEntry *treePtr;
	IndexWalkHeader *element;
	int cmp;

	IDXWALK_INIT();

	if (in->SubBlock == 1 && in->Shift == 0) {		//assume this is the first lookup step 
													//which happens in the direct pointer block
		bitTest = in->SearchString[0] >> (in->SSlen - 5);
#ifdef VERBOSE
 		cerr << "  first iteration of search for 0x" << hex << in->SearchString[0] << dec << endl;
 		cerr << "  bitTest is 0x" << hex << bitTest << dec << endl;
#endif
		switch (in->COP1) {
			case COMPARE_LT:
			case COMPARE_LE:
				for (i = 0; i <= bitTest; i++) {
					if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
						continue;
					ADD_ELEMENT(i, 5, (i < bitTest ? 1 : 0));
				}
				break;
			case COMPARE_GT:
			case COMPARE_GE:
				for (i = bitTest; i < 32; i++) {
					if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
						continue;
					ADD_ELEMENT(i, 5, (i > bitTest ? 1 : 0));
				}
				break;
			case COMPARE_EQ:
				if (treePtr[bitTest].fbo == 0 && treePtr[bitTest].sbid == 0 && treePtr[bitTest].entry == 0)
						break;
				ADD_ELEMENT(bitTest, 5, 0);
				break;
			case COMPARE_NE:
				for (i = 0; i < 32; i++) {
					if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
						continue;
					ADD_ELEMENT(i, 5, (i != bitTest ? 1 : 0));
				}
				break;
			default:
				MessageLog logger(LoggingID(28));
				logging::Message::Args colWidth;
				Message msg(34);

				colWidth.add(in->COP1);
				colWidth.add("indexWalk_1");
				msg.format(colWidth);
				logger.logDebugMessage(msg);
				return;
		}
	}
	// This is the general case where we're working within a bit test group
	else {
		GET_GROUP_SIZE();
#ifdef VERBOSE
 		cerr << "  search string is 0x" << hex << in->SearchString[0] << dec << endl;
#endif 
		

		for (i = 0; i < bitTestGroupSize; i++) {
			bool lastStage;

			// skip holes
			if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
				continue;

			if (treePtr[i].bitCompare == 0) {
				GET_BITTEST(bitTest, in->SearchString[0], 5);
			}
			else {
				GET_BITTEST(bitTest, in->SearchString[0], 10);
			}
			
			cmp = compare(treePtr[i].bitTest, bitTest, in->COP1, lastStage);
			if (cmp > 0) {
				ADD_ELEMENT(i, (treePtr[i].bitCompare == 0 ? 5 : 10), (cmp == 2 ? 1 : 0));
			}
		}
	} 
}

inline int PrimitiveProcessor::compare(int val1, int val2, uint8_t COP, 
	bool lastStage) throw()
{
	switch (COP) {
		case COMPARE_LT:
			if (val1 < val2)
				return 2;
			if (val1 == val2 && !lastStage)
				return 1;
			return 0;
		case COMPARE_LE:
			if (val1 < val2)
				return 2;
			if (val1 == val2)
				return 1;
			return 0;
		case COMPARE_GT:
			if (val1 > val2)
				return 2;
			if (val1 == val2 && !lastStage)
				return 1;
			return 0;
		case COMPARE_GE:
			if (val1 > val2)
				return 2;
			if (val1 == val2)
				return 1;
			return 0;
		case COMPARE_EQ:
			if (val1 == val2)
				return 1;
			return 0;
		case COMPARE_NE:
			if (val1 != val2)
				return 2;
			if (!lastStage)
				return 1;
			return 0;
		default:
			MessageLog logger(LoggingID(28));
			logging::Message::Args colWidth;
			Message msg(34);

			colWidth.add(COP);
			colWidth.add("compare");
			msg.format(colWidth);
			logger.logDebugMessage(msg);
			return false;
	}
}

void PrimitiveProcessor::indexWalk_2(const IndexWalkHeader *in, 
	vector<IndexWalkHeader *> *out) throw()
{
	uint16_t bitTest1, bitTest2;
	uint8_t *niceBlock;
	int blockOffset, bitTestGroupSize, cmp[2], i;
	WriteEngine::IdxBitTestEntry *treePtr;
	IndexWalkHeader *element;
	int convertToSingleOp;
	bool lastStage, setState;

	IDXWALK_INIT();

	if (in->SubBlock == 1 && in->Shift == 0) {		//assume this is the first lookup step 
													//which happens in the direct pointer block
		bitTest1 = in->SearchString[0] >> (in->SSlen - 5);
		bitTest2 = in->SearchString[1] >> (in->SSlen - 5);
#ifdef VERBOSE
 		cerr << "  first iteration.  SearchString[0]=0x" << hex << in->SearchString[0] << 
 			" bittest=0x" << bitTest1 << " SearchString[1]=0x" << in->SearchString[1] <<
 			" bittest=0x" << bitTest2 << dec << endl;
#endif
		for (i = 0; i < 32; i++) {
			if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
				continue;
			setState = false;
			convertToSingleOp = -1;
			cmp[0] = compare(i, bitTest1, in->COP1, false);
			cmp[1] = compare(i, bitTest2, in->COP2, false);
	
			switch (in->BOP) {
				case BOP_OR:
					if (cmp[0] == 2 || cmp[1] == 2) {
						setState = true;
						goto add2;
					}
// 					if (cmp[0] == 1 || cmp[1] == 1)
// 						goto add2;

					/* XXXPAT: clean up this logic */

 					if (cmp[0] == 1 && cmp[1] == 1) 
 						goto add2;
					if (cmp[0] == 1) {
						convertToSingleOp = 0;
						ADD_ELEMENT_WITH_CONVERT(i, 5, 0);
						goto skip2;
					}
					else if (cmp[1] == 1) {
						convertToSingleOp = 1;
						ADD_ELEMENT_WITH_CONVERT(i, 5, 0);
						goto skip2;
					}
					break;

				/* XXXPAT:  Need to verify the logic for AND.
					observations: if control reaches this point, then in the previous iteration the
					decision must have been 1, which implies equality for every previous comparison.

					If one of the comparisons returns 0, then there are no entries in this subtree
						in the result set and it can stop here.
					else, if both of the comparisons return 2, then the whole subtree is in the result set.
					else, if only one of the comparisons returns 2, then that comparison will be 2 for 
						every comparison made down this subtree, and it is equivalent to being
						(cmp1 && true).  In this case, we repackage the query as a single argument version
						(using the comparison operator that was 1) to be processed by indexwalk_1().
					else, the comparisons are both 1, meaning we have equality so far and can't decide on
						this subtree yet.
				*/
				case BOP_AND:
					if (cmp[0] == 0 || cmp[1] == 0)
						break;
					else if (cmp[0] == 2 && cmp[1] == 2)
						setState = true;
					else if (cmp[0] == 2) 
						convertToSingleOp = 1;
					else if (cmp[1] == 2)
						convertToSingleOp = 0;
					goto add2;

				default:
					MessageLog logger(LoggingID(28));
					logging::Message::Args colWidth;
					Message msg(39);

					colWidth.add(in->BOP);
					colWidth.add("indexwalk_2");
					msg.format(colWidth);
					logger.logDebugMessage(msg);
					return;
			}
			continue;
add2:
			ADD_ELEMENT_WITH_CONVERT(i, 5, (setState ? 1 : 0));
skip2:		;
		}
	}
	else {			// the general case
		GET_GROUP_SIZE();

#ifdef VERBOSE
 		cerr << "  SearchString[0]=0x" << hex << in->SearchString[0] << 
 			" SearchString[1]=0x" << in->SearchString[1] << dec << endl;
#endif
		
		for (i = 0; i < bitTestGroupSize; i++) {
			
			if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
				continue;
		
			setState = false;
			lastStage = false;
			convertToSingleOp = -1;

			if (treePtr[i].bitCompare == 0) {
				GET_BITTEST(bitTest1, in->SearchString[0], 5);
				GET_BITTEST(bitTest2, in->SearchString[1], 5);
			}
			else {
				GET_BITTEST(bitTest1, in->SearchString[0], 10);
				GET_BITTEST(bitTest2, in->SearchString[1], 10);
			}

			cmp[0] = compare(treePtr[i].bitTest, bitTest1, in->COP1, lastStage);
			cmp[1] = compare(treePtr[i].bitTest, bitTest2, in->COP2, lastStage);
			
			switch (in->BOP) {
				case BOP_OR:
					if (cmp[0] == 2 || cmp[1] == 2) {
						setState = true;
						goto add3;
					}
// 					if (cmp[0] == 1 || cmp[1] == 1) 
// 						goto add3;

					/* XXXPAT: clean up this logic */

 					if (cmp[0] == 1 && cmp[1] == 1) 
 						goto add3;
					if (cmp[0] == 1) {
						convertToSingleOp = 0;
						ADD_ELEMENT_WITH_CONVERT(i, (treePtr[i].bitCompare == 0 ? 5 : 10), 0);
						goto skip3;
					}
					else if (cmp[1] == 1) {
						convertToSingleOp = 1;
						ADD_ELEMENT_WITH_CONVERT(i, (treePtr[i].bitCompare == 0 ? 5 : 10), 0);
						goto skip3;
					}

					break;			

			/* XXXPAT:  Need to verify the logic for AND.
					observations: if control reaches this point, then in the previous iteration the
					decision must have been 1, which implies equality for every previous comparison.

					If one of the comparisons returns 0, then there are no entries in this subtree
						in the result set and it can stop here.
					else, if both of the comparisons return 2, then the whole subtree is in the result set.
					else, if only one of the comparisons returns 2, then that comparison will be 2 for 
						every comparison made down this subtree, and it is equivalent to being
						(cmp1 && true).  In this case, we repackage the query as a single argument version
						(using the comparison operator that was 1) to be processed by indexwalk_1().
					else, the comparisons are both 1, meaning we have equality so far and can't decide on
						this subtree yet.
				*/
				case BOP_AND:
					if (cmp[0] == 0 || cmp[1] == 0)
						break;
					else if (cmp[0] == 2 && cmp[1] == 2)
						setState = true;
					else if (cmp[0] == 2) 
						convertToSingleOp = 1;
					else if (cmp[1] == 2)
						convertToSingleOp = 0;
					goto add3;
											
				default:
					MessageLog logger(LoggingID(28));
					logging::Message::Args colWidth;
					Message msg(39);

					colWidth.add(in->BOP);
					colWidth.add("indexWalk_2");
					msg.format(colWidth);
					logger.logDebugMessage(msg);
					return;
			}
			continue;
add3:
			ADD_ELEMENT_WITH_CONVERT(i, (treePtr[i].bitCompare == 0 ? 5 : 10), 
				(setState ? 1 : 0));
skip3: 		;
		}
	}
}

void PrimitiveProcessor::indexWalk_many(const IndexWalkHeader *in, 
	vector<IndexWalkHeader *> *out) throw()
{
	uint16_t bitTest;
	uint8_t *niceBlock;
	int blockOffset, bitTestGroupSize, i;
	WriteEngine::IdxBitTestEntry *treePtr;
	IndexWalkHeader *element;
	bool lastStage;
	vector<uint64_t>::const_iterator it;
	struct {
		int action;
		vector<uint64_t> *searchStrings;
	} nextIteration[32];

	IDXWALK_INIT();

	/* 
		Here's the high-level algorithm for this function:
		1) Iterate over the bit test tree entries (index is i)
			1a) if it is not a match, set nextIteration[i].action = 0 (searchStrings = NULL)
			1b) if it is a match which determines the whole subtree goes in the result set action = 2 (searchStrings = NULL)
			1c) if it's a match that determines the result is somewhere further down the tree, set action = 1,
					stuff the matching search string into nextIteration[i].searchStrings.
		2) Iterate over the nextIteration structures
			2a) if action == 0 do nothing
			2b) if action == 1,
					make an intermediate IndexWalkHeader configured s.t. it makes sense given the # of search strings
					that match on that subtree
			2c) if action == 2,
					make an intermediate IndexWalkHeader with state = 1 so the whole subtree will be included.
			2d) if the current LBID is also the next LBID, recurse at p_IdxWalk()
				otherwise, add it to the result set to return.

		An implementation that is definitely simpler and which may be faster
		in some circumstances:
		1) if BOP is OR, COP is =, so split the query into one single-op
			query for each search string.
		2) if BOP is AND, COP is !=, so grab every indexed value and eliminate
			the entries in the argument list.  (Need to store the index values somewhere).
		*** 3) Generalize the index_many case; get rid of the NVALS=[1,2] cases.
				- need to find a way to avoid dynamic mem allocation.
	*/


	if (in->SubBlock == 1 && in->Shift == 0) {			// direct pointer block
		bitTestGroupSize = 32;
#ifdef VERBOSE
 		cerr << "  first iteration using many search strings" << endl;
#endif
		switch (in->BOP) {
			case BOP_OR:
				// according to the design doc, it's safe to assume COP1 is '='
				for (i = 0; i < bitTestGroupSize; i++) {
					nextIteration[i].action = 0;
					nextIteration[i].searchStrings = NULL;
				}
				for (it = in->SearchStrings->begin(); it != in->SearchStrings->end(); it++) {
					bitTest = *it >> (in->SSlen - 5);
#ifdef VERBOSE
 					cerr << "  search string=0x" << hex << *it << " bittest=0x" << bitTest << dec << endl;
#endif
					if (treePtr[bitTest].fbo == 0 && treePtr[bitTest].sbid == 0 && treePtr[bitTest].entry == 0)
						continue;
					
					if (nextIteration[bitTest].action == 0) {
						nextIteration[bitTest].searchStrings = new vector<uint64_t>();
						nextIteration[bitTest].action = 1;
					}
					nextIteration[bitTest].searchStrings->push_back(*it);
				}
				break;
			case BOP_AND:
				// safe to assume COP1 is '!='
				/*  Here's the logic here....
					With a set of 0 search strings, the result set is the entire index.
					With a set of 1 search strings, recurse the subtree containing that string, return every other subtree.
					So, for every search string, we flag the respective subtree as one that has to be recursed.
				*/

				for (i = 0; i < bitTestGroupSize; i++) {
					if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
						nextIteration[i].action = 0;
					else
						nextIteration[i].action = 2;
					nextIteration[i].searchStrings = NULL;
				}
			
				for (it = in->SearchStrings->begin();
					it != in->SearchStrings->end();
					it++) {
					
					bitTest = *it >> (in->SSlen - 5);
#ifdef VERBOSE
 					cerr << "  search string=0x" << hex << *it << " bittest=0x" << bitTest << dec << endl;
#endif
					if (nextIteration[bitTest].action == 2) {
						nextIteration[bitTest].searchStrings = new vector<uint64_t>();
						nextIteration[bitTest].action = 1;
					}
					nextIteration[bitTest].searchStrings->push_back(*it);
				}
				break;
			default:
					MessageLog logger(LoggingID(28));
					logging::Message::Args colWidth;
					Message msg(39);

					colWidth.add(in->BOP);
					msg.format(colWidth);
					logger.logDebugMessage(msg);
					return;
		}
	}
	else {		// The general case of being at a node in the middle of the tree
		GET_GROUP_SIZE();

		switch (in->BOP) {
			case BOP_OR:
				// COP1 == '='
				for (i = 0; i < bitTestGroupSize; i++) {
					nextIteration[i].action = 0;
					nextIteration[i].searchStrings = NULL;
				}
				for (i = 0; i < bitTestGroupSize; i++) {
		
					if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
						continue;

					for (it = in->SearchStrings->begin(); it != in->SearchStrings->end(); it++) {
#ifdef VERBOSE
 					cerr << "  search string=0x" << hex << *it << dec << endl;
#endif
						if (treePtr[i].bitCompare == 0) {
							GET_BITTEST(bitTest, *it, 5);
						}
						else {
							GET_BITTEST(bitTest, *it, 10);
						}

						if (bitTest == treePtr[i].bitTest) {
							if (nextIteration[i].action == 0) {
								nextIteration[i].searchStrings = new vector<uint64_t>();
								nextIteration[i].action = 1;
							}
							nextIteration[i].searchStrings->push_back(*it);
						}
					}
				}
				break;
			case BOP_AND:
				// COP1 == '!='
				for (i = 0; i < bitTestGroupSize; i++) {
					nextIteration[i].action = 2;
					nextIteration[i].searchStrings = NULL;
				}

				for (i = 0; i < bitTestGroupSize; i++) {

					if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0) {
						nextIteration[i].action = 0;
						continue;
					}

					for (it = in->SearchStrings->begin(); it != in->SearchStrings->end(); it++) {
#ifdef VERBOSE
 						cerr << "  search string=0x" << hex << *it << dec << endl;
#endif
						if (treePtr[i].bitCompare == 0) {
							GET_BITTEST(bitTest, *it, 5);
						}
						else {
							GET_BITTEST(bitTest, *it, 10);
						}

						// note: at the last stage, matches are left with action = 2 and NULL searchStrings
						if (!lastStage && bitTest == treePtr[i].bitTest) {
							if (nextIteration[i].action == 2) {
								nextIteration[i].searchStrings = new vector<uint64_t>();
								nextIteration[i].action = 1;
							}
							nextIteration[i].searchStrings->push_back(*it);
						}
						else if (lastStage && bitTest == treePtr[i].bitTest)
							nextIteration[i].action = 0;
					}
				}
				break;
			default:
				MessageLog logger(LoggingID(28));
				logging::Message::Args colWidth;
				Message msg(39);

				colWidth.add(in->BOP);
				colWidth.add("indexWalk_many");
				msg.format(colWidth);
				logger.logDebugMessage(msg);
				return;	
		}
	}

	for (i = 0; i < bitTestGroupSize; i++) {
		if (nextIteration[i].action > 0) {
			element = new IndexWalkHeader();
			memcpy(element, in, sizeof(IndexWalkHeader));
			element->ism.Command = INDEX_WALK_RESULTS;
			element->Shift += (treePtr[i].bitCompare ? 10 : 5);
			element->LBID = treePtr[i].fbo;
			element->SubBlock = treePtr[i].sbid;
			element->SBEntry = treePtr[i].entry;
#ifdef VERBOSE
			cerr << "  (inline _many) creating a result from subblock entry " << i << " with Shift=" << (int) element->Shift << 
				" LBID=" << element->LBID << " Subblock=" << (int)element->SubBlock << " Subblock entry=" << 
				(int) element->SBEntry << " State=0" << endl;
#endif
			if (nextIteration[i].action == 1) {
				element->NVALS = nextIteration[i].searchStrings->size();
				if (nextIteration[i].searchStrings->size() > 2) 
					element->SearchStrings = nextIteration[i].searchStrings;
				else if (nextIteration[i].searchStrings->size() == 2) {
					element->SearchString[0] = nextIteration[i].searchStrings->at(0);
					element->SearchString[1] = nextIteration[i].searchStrings->at(1);
					element->COP2 = element->COP1;
					delete nextIteration[i].searchStrings;
				}
				else { // size == 1
					element->SearchString[0] = nextIteration[i].searchStrings->at(0);
					delete nextIteration[i].searchStrings;
				}
			}
			else		// action == 2
				element->State = 1;
			if (element->LBID == in->LBID && element->Shift < element->SSlen) {
#ifdef VERBOSE	
				cerr << "  recursing..." << endl;
#endif
				p_IdxWalk(element, out);
				if (element->State == 0 && element->NVALS > 2)
					delete element->SearchStrings;
				delete element;
			}
			else 
				out->push_back(element);
		}
	}
}

void PrimitiveProcessor::grabSubTree(const IndexWalkHeader *in, 
	vector<IndexWalkHeader *> *out) throw()
{
	uint8_t *niceBlock;
	int blockOffset, bitTestGroupSize, i;
	WriteEngine::IdxBitTestEntry *treePtr;
	IndexWalkHeader *element;

	IDXWALK_INIT();
	GET_GROUP_SIZE();

	for (i = 0; i < bitTestGroupSize; i++) {

		if (treePtr[i].fbo == 0 && treePtr[i].sbid == 0 && treePtr[i].entry == 0)
			continue;

		ADD_ELEMENT(i, (treePtr[i].bitCompare ? 10 : 5), 1);
	}
}

void PrimitiveProcessor::p_IdxWalk(const IndexWalkHeader *in, 
	vector<IndexWalkHeader *> *out) throw()
{
	
#ifdef VERBOSE
	cerr << "p_IdxWalk()" << endl;
	cerr << "  COP1=" << (int) in->COP1 << endl;
	cerr << "  COP2=" << (int) in->COP2 << endl;
	cerr << "  BOP=" << (int) in->BOP << endl;
	cerr << "  Shift=" << (int) in->Shift << endl;
	cerr << "  SSlen=" << (int) in->SSlen << endl;
	cerr << "  LBID=" << (int) in->LBID << endl;
	cerr << "  Subblock=" << (int) in->SubBlock << endl;
	cerr << "  SBEntry=" << (int) in->SBEntry << endl;
	cerr << "  NVALS=" << (int) in->NVALS << endl;
#endif

#ifdef PRIM_DEBUG
	if (in->Shift >= in->SSlen)
		throw logic_error("p_IdxWalk: called on a completed search");
#endif

	if (in->State == 1)
		grabSubTree(in, out);
	else 
		switch(in->NVALS) {
			case 1:
				indexWalk_1(in, out);
				break;
			case 2:
				indexWalk_2(in, out);
				break;
			default:
				indexWalk_many(in, out);
				break;
		}
#ifdef VERBOSE
	cerr << "/p_IdxWalk()" << endl;
#endif
}

/*
 * Following the headers for both the request and result will be at variable
 * length list of the IndexListParam structure, where the type field is used to
 * indicate whether the Index List section is a header (0), sub-block (4), or 
 * a block (5).
 */
void PrimitiveProcessor::p_IdxList(const IndexListHeader *rqst,
				   IndexListHeader *rslt, int mode)
{
    uint8_t *listPtr;
    IndexListParam *linkList;
    IndexListEntry *listEntry, *rsltList, *sizeEntry=0;
    int listOfst, listType, ridCt, i, j, originalRidCt;
	unsigned entryNumber, lastEntry=0;
    int subblk_sz  = WriteEngine::SUBBLOCK_TOTAL_BYTES;
    int entry_sz   = WriteEngine::NEXT_PTR_BYTES;

#ifdef VERBOSE
	cerr << "p_IdxList()" << endl;
#endif

	memcpy(rslt, rqst, sizeof(IndexListHeader));
	rslt->ism.Command = INDEX_LIST_RESULTS;
	rslt->NVALS = 0;

    listPtr  = (uint8_t *)block;
    linkList = (IndexListParam *)(rqst + 1);
    rsltList = (IndexListEntry *)(rslt + 1);

    for (i = 0; i < rqst->NVALS; i++) {
		j = 0;
		listOfst  = (linkList->sbid * subblk_sz) + (linkList->entry * entry_sz);
		entryNumber = linkList->entry;
		listEntry = (IndexListEntry *)(listPtr + listOfst);

#ifdef VERBOSE
		cerr << "  processing argument number " << i + 1 << endl;
		cerr << "    type=" << linkList->type << " LBID=" << linkList->fbo << " subblock=" <<
			linkList->sbid << " entry=" << linkList->entry << endl;
#endif

		listType = linkList->type;
		if (listType == LIST_SIZE) {
			if (listEntry->type != LIST_SIZE) {
				MessageLog logger(LoggingID(28));
				Message msg(40);
				logger.logDebugMessage(msg);
#ifdef VERBOSE
				cerr << "PrimitiveProcessor::p_IdxList: was told to parse a header, but the given pointer does not point to one.  sbid=" << linkList->sbid << " entry=" << linkList->entry << endl;
#endif
				throw runtime_error("p_IdxList: not a header");
			}
		    ridCt      = listEntry->value;
			originalRidCt = ridCt;
#ifdef VERBOSE
			uint64_t *tmp = (uint64_t *) &listEntry[1];
			cerr << "    ridCount=" << ridCt << " key value=0x" << hex << *tmp << 
				dec << endl;
#endif
		    listEntry += 2;	// Skip size and key values
			entryNumber += 2;
			lastEntry = linkList->entry + 4;
		}

		else {
		    if (listType == LLP_SUBBLK) {
				sizeEntry = listEntry + WriteEngine::LIST_SUB_LLP_POS; 
				lastEntry = linkList->entry + 32;
			}
			else if (listType == LLP_BLK) {
				sizeEntry = listEntry + WriteEngine::LIST_BLOCK_LLP_POS;
				lastEntry = linkList->entry + (mode == 0 ? 1024 : 1023);  
					// ignore the size entry for blocks if mode == 1
			}
			else
				cerr << "p_IdxList: bad pointer type " << listType << endl;
		    ridCt = sizeEntry->ridCt;		//XXXPAT: make sure these are the right bits
			originalRidCt = ridCt;
#ifdef VERBOSE
			cerr << "    ridCount for this " << (listType == LLP_SUBBLK ? "subblock: " : "block: ") << 
				ridCt << endl;
#endif
		}

// 		while (ridCt > 0 && entryNumber < lastEntry) {
		while (entryNumber < lastEntry) {
		    switch (listEntry->type) {
			    case LLP_SUBBLK:
		    	case LLP_BLK:

					/* the second to last entry of a subblock 
					can now be a HWM disguised as a continuation ptr. */
					if (mode &&
						entryNumber == static_cast<unsigned>(linkList->entry + 30) &&
						listType == LLP_SUBBLK)
						break; 
#ifdef VERBOSE
					ilptmp = reinterpret_cast<IndexListParam *>(listEntry);

					cerr << "   found a continuation pointer: LBID=" << ilptmp->fbo <<
						" subblock=" << ilptmp->sbid << " subblock entry=" <<
 						 ilptmp->entry << endl;
#endif
					*rsltList = *listEntry;
					rslt->NVALS++;
					rsltList++;
					break;
/*
#ifdef VERBOSE
					cerr << "   reached the end of the list" << endl;
					if (listEntry != sizeEntry && listType != LIST_SIZE) 
						cerr << "   ERROR: there's a list pointer in the middle of the list" << endl;
					else if (listType != LIST_SIZE)
						cerr << "   Possible error: the pointer clause executed for a subblock or block" << endl;
#endif
					endOfList = true;
					break;
*/
			    case RID:
#ifdef VERBOSE
// 					cerr << "    returning rid " << listEntry->value << endl;
#endif
					*rsltList = *listEntry;
					rslt->NVALS++;
					rsltList++;
					ridCt--;
					j++;
					break;
			    case LIST_SIZE:
	    		case NOT_IN_USE:
	    		case EMPTY_LIST_PTR:
	    		case EMPTY_PTR:
				case PARENT:
					break;
		
				default:
#ifdef VERBOSE
					IndexListParam *tmp = reinterpret_cast<IndexListParam *>(listEntry);
					cerr << "PrimitiveProcessor::p_IdxList: invalid list entry type" << endl;
					cerr << "   Entry contents: type=" << listEntry->type << " value/RID=" << listEntry->value << endl;
					cerr << "   (if a pointer, fbo=" << tmp->fbo << " subblock=" << tmp->sbid << 
						" entry=" << tmp->entry << ")" << endl;
					cerr << "   Location of the entry: lbid=" << linkList->fbo << " subblock=" << 
						linkList->sbid + entryNumber/32 << " entry=" << entryNumber % 32 << endl;
					cerr << "   Input parameter indicates the start of the list is sbid=" <<
						linkList->sbid << " entry=" << linkList->entry << endl;
					cerr << "   Type of search: " << 
						(linkList->type == LIST_SIZE ? "header" : 
						(linkList->type == LLP_SUBBLK ? "subblock" : 
						(linkList->type == LLP_BLK ? "block" : "unknown..?"))) << endl;
					cerr << "   Processed " << entryNumber - linkList->entry << 
						" entries for this request so far." << endl;
					cerr << "   Rid count read from the list is " << originalRidCt << endl;
#endif
					MessageLog logger(LoggingID(28));
					Message msg(40);
					logger.logDebugMessage(msg);
					throw runtime_error("Bad index list entry, see stderr");
				return;
			}

			listEntry++;
			entryNumber++;
		}
#ifdef VERBOSE
	cerr << "  RIDs in result generated from input arg #" << i + 1 << ": " << j << endl;
#endif

	linkList++;
    }

    return;
}

}
// vim:ts=4 sw=4:

