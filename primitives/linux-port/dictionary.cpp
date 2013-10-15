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

/*
 * $Id: dictionary.cpp 2122 2013-07-08 16:33:50Z bpaul $
 */

#include <iostream>
#include <boost/scoped_array.hpp>
#include <sys/types.h>
using namespace std;

#include "primitiveprocessor.h"
#include "we_type.h"
#include "messagelog.h"
#include "messageobj.h"
#include "exceptclasses.h"
#include "utils_utf8.h"
#include <sstream>

using namespace funcexp;
using namespace logging;

const char *nullString = " ";  // this is not NULL to preempt segfaults.
const int nullStringLen = 0;

namespace
{
const char* signatureNotFound = joblist::CPSTRNOTFOUND.c_str();
}

namespace primitives
{

inline bool PrimitiveProcessor::compare(int cmp, uint8_t COP, int len1, int len2) throw()
{

	switch(COP) {
		case COMPARE_NIL:
			return false;
		case COMPARE_LT:
			return (cmp < 0 || (cmp == 0 && len1 < len2));
		case COMPARE_EQ:
			return (cmp == 0 && len1 == len2 ? true : false);
		case COMPARE_LE:
			return (cmp < 0  || (cmp == 0 && len1 <= len2));
		case COMPARE_GT:
			return (cmp > 0 || (cmp == 0 && len1 > len2));
		case COMPARE_NE:
			return (cmp != 0 || len1 != len2 ? true : false);
		case COMPARE_GE:
			return (cmp > 0 || (cmp == 0 && len1 >= len2));
		case COMPARE_LIKE:
			return cmp;							// is done elsewhere; shouldn't get here.  Exception?
		case COMPARE_NOT:
			return false;  						// throw an exception here?
		default:
			MessageLog logger(LoggingID(28));
			logging::Message::Args colWidth;
			Message msg(34);

			colWidth.add(COP);
			colWidth.add("compare");
			msg.format(colWidth);
			logger.logErrorMessage(msg);
			return false;						// throw an exception here?
		}
}

/* 
Notes: 
	- assumes no continuation pointer
*/

void PrimitiveProcessor::p_TokenByScan(const TokenByScanRequestHeader *h,
	TokenByScanResultHeader *ret, unsigned outSize, bool utf8,
	boost::shared_ptr<DictEqualityFilter> eqFilter)
{
	const DataValue *args;
	const u_int8_t *niceBlock;		// block cast to a byte-indexed type
	const u_int8_t *niceInput;		// h cast to a byte-indexed type
	const u_int16_t *offsets;
	int offsetIndex, argIndex, argsOffset;	
	bool cmpResult=false;
	int tmp, i, err;

	const char *sig;
	u_int16_t siglen;

	PrimToken *retTokens;
	DataValue *retDataValues;
	int rdvOffset;
	u_int8_t *niceRet;			// ret cast to a byte-indexed type

	boost::scoped_array<idb_regex_t> regex;

	// set up pointers to fields within each structure

	// either retTokens or retDataValues will be used but not both.
	niceRet = reinterpret_cast<u_int8_t *>(ret);
	rdvOffset = sizeof(TokenByScanResultHeader);

	retTokens = reinterpret_cast<PrimToken *>(&niceRet[rdvOffset]);
	retDataValues = reinterpret_cast<DataValue *>(&niceRet[rdvOffset]);
	memcpy(ret, h, sizeof(PrimitiveHeader) + sizeof(ISMPacketHeader));
	ret->NVALS = 0;
	ret->NBYTES = sizeof(TokenByScanResultHeader);
	ret->ism.Command = DICT_SCAN_COMPARE_RESULTS;

	//...Initialize I/O counts
	ret->CacheIO    = 0;
	ret->PhysicalIO = 0;

	niceBlock = reinterpret_cast<const u_int8_t *>(block);
	offsets = reinterpret_cast<const u_int16_t *>(&niceBlock[10]);
	niceInput = reinterpret_cast<const u_int8_t *>(h);

	// if LIKE is an operator, compile regexp's in advance.
	if ((h->NVALS > 0 && h->COP1 & COMPARE_LIKE) || 
	  (h->NVALS == 2 && h->COP2 & COMPARE_LIKE)) {
		regex.reset(new idb_regex_t[h->NVALS]);
		for (i = 0, argsOffset = sizeof(TokenByScanRequestHeader); i < h->NVALS; i++) {
			p_DataValue pdvTmp;
			
			args = reinterpret_cast<const DataValue *>(&niceInput[argsOffset]);
			pdvTmp.len = args->len;
			pdvTmp.data = (const uint8_t *) args->data;
			err = convertToRegexp(&regex[i], &pdvTmp);
			if (err != 0) {
				MessageLog logger(LoggingID(28));
				Message msg(37);	
				logger.logErrorMessage(msg);

				return;
			}
			
			argsOffset += sizeof(uint16_t) + args->len;
		}
	}

	for (offsetIndex = 1; offsets[offsetIndex] != 0xffff; offsetIndex++) {

		siglen = offsets[offsetIndex-1] - offsets[offsetIndex];
		sig = reinterpret_cast<const char *>(&niceBlock[offsets[offsetIndex]]);
		argsOffset = sizeof(TokenByScanRequestHeader);
		argIndex = 0;
		args = reinterpret_cast<const DataValue *>(&niceInput[argsOffset]);

		string sig_utf8;
		string arg_utf8;

		if (eqFilter) {
			bool gotIt = eqFilter->find(string(sig, siglen)) != eqFilter->end();
			if ((h->COP1 == COMPARE_EQ && gotIt) || (h->COP1 == COMPARE_NE &&
					!gotIt))
				goto store;
			goto no_store;
		}

		// BUG 5110: If it is utf, we need to create utf strings to compare
		if(utf8)
		{
		  	sig_utf8 = string(sig, siglen);
		    arg_utf8 = string(args->data, args->len);
		}
		switch (h->NVALS) {
			case 1: {
				if (h->COP1 & COMPARE_LIKE) {
					p_DataValue dv;
			
					dv.len = siglen;
					dv.data = (uint8_t *) sig;
					cmpResult = isLike(&dv, &regex[argIndex]);
					if (h->COP1 & COMPARE_NOT)
						cmpResult = !cmpResult;
				}
				else {
					if (utf8) {
						tmp = utf8::idb_strcoll(sig_utf8.c_str(), arg_utf8.c_str());
						cmpResult = compare(tmp, h->COP1, siglen, args->len);
					} else {
						tmp = strncmp(sig, args->data, std::min(siglen, args->len));
						cmpResult = compare(tmp, h->COP1, siglen, args->len);
					}
				}
				if (cmpResult)
					goto store;
				goto no_store;
			}
			case 2: {
				if (h->COP1 & COMPARE_LIKE) {
					p_DataValue dv;
			
					dv.len = siglen;
					dv.data = (uint8_t *) sig;
					cmpResult = isLike(&dv, &regex[argIndex]);
					if (h->COP1 & COMPARE_NOT)
						cmpResult = !cmpResult;
				}

				else {
					if (utf8) {
						tmp = utf8::idb_strcoll(sig_utf8.c_str(), arg_utf8.c_str());
						cmpResult = compare(tmp, h->COP1, siglen, args->len);
					} else {
						tmp = strncmp(sig, args->data, std::min(siglen, args->len));
						cmpResult = compare(tmp, h->COP1, siglen, args->len);
					}
				}

				if (!cmpResult && h->BOP == BOP_AND)
					goto no_store;
				if (cmpResult && h->BOP == BOP_OR)
					goto store;

				argsOffset += sizeof(uint16_t) + args->len;
				argIndex++;
				args = (DataValue *) &niceInput[argsOffset];
				if (h->COP2 & COMPARE_LIKE) {
					p_DataValue dv;
			
					dv.len = siglen;
					dv.data = (uint8_t *) sig;
					cmpResult = isLike(&dv, &regex[argIndex]);
					if (h->COP2 & COMPARE_NOT)
						cmpResult = !cmpResult;
				}

				else {
					if (utf8) {
						arg_utf8 = string(args->data, args->len);
						tmp = utf8::idb_strcoll(sig_utf8.c_str(), arg_utf8.c_str());
						cmpResult = compare(tmp, h->COP2, siglen, args->len);
					} else {
						tmp = strncmp(sig, args->data, std::min(siglen, args->len));
						cmpResult = compare(tmp, h->COP2, siglen, args->len);
					}
				}

				if (cmpResult)
					goto store;
				goto no_store;
			}
			default: {
				for (i = 0, cmpResult = true; i < h->NVALS; i++) {
					if (h->COP1 & COMPARE_LIKE) {
						p_DataValue dv;
			
						dv.len = siglen;
						dv.data = (uint8_t *) sig;
						cmpResult = isLike(&dv, &regex[argIndex]);
						if (h->COP1 & COMPARE_NOT)
							cmpResult = !cmpResult;
					}	

					else {
						if (utf8) {
							tmp = utf8::idb_strcoll(sig_utf8.c_str(), arg_utf8.c_str());
							cmpResult = compare(tmp, h->COP2, siglen, args->len);
						} else {
							tmp = strncmp(sig, args->data, std::min(siglen, args->len));
							cmpResult = compare(tmp, h->COP1, siglen, args->len);
						}
					}

					if (!cmpResult && h->BOP == BOP_AND)
						goto no_store;
					if (cmpResult && h->BOP == BOP_OR)
						goto store;
			
					argsOffset += sizeof(uint16_t) + args->len;
					argIndex++;
					args = (DataValue *) &niceInput[argsOffset];
					if ( utf8) {
						arg_utf8 = string(args->data, args->len);
					}
				}
				if (i == h->NVALS && cmpResult)
					goto store;
				else
					goto no_store;
			}
		}

store:
		if (h->OutputType == OT_DATAVALUE) {
			if ((ret->NBYTES + sizeof(DataValue) + siglen) > outSize) {
				MessageLog logger(LoggingID(28));
				logging::Message::Args marker;
				Message msg(35);

				marker.add(8);
				msg.format(marker);
				logger.logErrorMessage(msg);

				throw logging::DictionaryBufferOverflow();
			}

			retDataValues->len = siglen;
			memcpy(retDataValues->data, sig, siglen);
			rdvOffset += sizeof(DataValue) + siglen;
			retDataValues = (DataValue *) &niceRet[rdvOffset];
			ret->NVALS++;
			ret->NBYTES += sizeof(DataValue) + siglen;
		}
		else if (h->OutputType == OT_TOKEN) {
			if ((ret->NBYTES + sizeof(PrimToken)) > outSize) {
				MessageLog logger(LoggingID(28));
				logging::Message::Args marker;
				Message msg(35);

				marker.add(9);
				msg.format(marker);
				logger.logErrorMessage(msg);

				throw logging::DictionaryBufferOverflow();
			}

			retTokens[ret->NVALS].LBID = h->LBID;
			retTokens[ret->NVALS].offset = offsetIndex;  // need index rather than the block offset... rp 12/19/06
			retTokens[ret->NVALS].len = args->len;
			ret->NVALS++;
			ret->NBYTES += sizeof(PrimToken);
		}
		/*
		 * XXXPAT: HACK!  Ron requested a special case where the input string
		 * that matched and the token of the matched string were returned.
		 * It will not be used in cases where there are multiple input strings.
		 * We need to rethink the requirements for this primitive after Dec 15.
		 */
		else if (h->OutputType == OT_BOTH) {
			if (ret->NBYTES + sizeof(PrimToken) + sizeof(DataValue) + args->len > outSize) {
				MessageLog logger(LoggingID(28));
				logging::Message::Args marker;
				Message msg(35);

				marker.add(10);
				msg.format(marker);
				logger.logErrorMessage(msg);

				throw logging::DictionaryBufferOverflow();
			}

			retDataValues->len = args->len;
			memcpy(retDataValues->data, args->data, args->len);
			rdvOffset += sizeof(DataValue) + args->len;
			retTokens = reinterpret_cast<PrimToken *>(&niceRet[rdvOffset]);
			retTokens->LBID = h->LBID;
			retTokens->offset = offsetIndex;  // need index rather than the block offset... rp 12/19/06
			retTokens->len = args->len;
			rdvOffset += sizeof(PrimToken);
			retDataValues = reinterpret_cast<DataValue *>(&niceRet[rdvOffset]);
			ret->NBYTES += sizeof(PrimToken) + sizeof(DataValue) + args->len;
			ret->NVALS++;
		}
no_store:
		;			//this is intentional
	}

	return;
}

void PrimitiveProcessor::nextSig(int NVALS,
				const PrimToken *tokens,
				p_DataValue *ret,
				uint8_t outputFlags,
				bool oldGetSigBehavior, bool skipNulls) throw()
{
	const u_int8_t* niceBlock = reinterpret_cast<const u_int8_t *>(block);
	const u_int16_t *offsets
		= reinterpret_cast<const u_int16_t *>(&niceBlock[10]);

	if (NVALS == 0) {		
		if (offsets[dict_OffsetIndex + 1] == 0xffff) {
			ret->len = -1;
			return;
		}
		ret->len = offsets[dict_OffsetIndex] - offsets[dict_OffsetIndex + 1];
		ret->data = &niceBlock[offsets[dict_OffsetIndex + 1]];
		if (outputFlags & OT_TOKEN)
			currentOffsetIndex = dict_OffsetIndex + 1;
	} else {

again:
		if (dict_OffsetIndex >= NVALS) {
			ret->len = -1;
			return;
		}

		if (oldGetSigBehavior) {

			const OldGetSigParams *oldParams=
						reinterpret_cast<const OldGetSigParams *>(tokens);
			if (oldParams[dict_OffsetIndex].rid & 0x8000000000000000LL) {
				if (skipNulls) {
					/* Bug 3321.  For some cases the NULL token should be skipped.  The
					 * isnull filter is handled by token columncommand or by the F & E
					 * framework.  This primitive should only process nulls
					 * when it's for projection.
					 */
					dict_OffsetIndex++;
					goto again;
				}
				ret->len = nullStringLen;
				ret->data = (const uint8_t *) nullString;
			}
			else {
				ret->len = offsets[oldParams[dict_OffsetIndex].offsetIndex - 1] - 
					offsets[oldParams[dict_OffsetIndex].offsetIndex];
				//Whoa! apparently we have come across a missing signature! That is, the requested ordinal
				//  is larger than the number of signatures in this block. Return a "special" string so that
				//  the query keeps going, but that can be recognized as an internal error upon inspection.
				//@Bug 2534. Change the length check to 8000
				if (ret->len < 0 || ret->len > 8001)
				{
					ret->data = reinterpret_cast<const u_int8_t*>(signatureNotFound);
					ret->len = strlen(reinterpret_cast<const char*>(ret->data));
				}
				else
					ret->data = &niceBlock[offsets[oldParams[dict_OffsetIndex].offsetIndex]];
			}
// 			idbassert(ret->len >= 0);
			currentOffsetIndex = oldParams[dict_OffsetIndex].offsetIndex;
			dict_OffsetIndex++;
			return;
		}

		/* XXXPAT: Need to check for the NULL token here */
		ret->len = tokens[dict_OffsetIndex].len;
		ret->data = &niceBlock[tokens[dict_OffsetIndex].offset];

		if (outputFlags & OT_TOKEN) {
			//offsets = reinterpret_cast<const u_int16_t *>(&niceBlock[10]);
			for (currentOffsetIndex = 1; offsets[currentOffsetIndex] != 0xffff; currentOffsetIndex++)
				if (tokens[dict_OffsetIndex].offset == offsets[currentOffsetIndex])
					break;

			if (offsets[currentOffsetIndex] == 0xffff) {
				MessageLog logger(LoggingID(28));
				logging::Message::Args offset;
				Message msg(38);

				offset.add(tokens[dict_OffsetIndex].offset);
				msg.format(offset);
				logger.logErrorMessage(msg);

				currentOffsetIndex = -1;
				dict_OffsetIndex++;
				return;
			}
		}
	}
	dict_OffsetIndex++;
}

void PrimitiveProcessor::p_AggregateSignature(const AggregateSignatureRequestHeader *in,
	AggregateSignatureResultHeader *out, unsigned outSize, unsigned *written, bool utf8)
{	

	u_int8_t *niceOutput;		// h cast to a byte-indexed type
	int cmp;	
	char cMin[BLOCK_SIZE], cMax[BLOCK_SIZE];
	int cMinLen, cMaxLen;
	p_DataValue sigptr;
	
	DataValue *min;
	DataValue *max;

	memcpy(out, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
	out->ism.Command = DICT_AGGREGATE_RESULTS;
	niceOutput = reinterpret_cast<u_int8_t *>(out);

	// The first sig is the min and the max.
	out->Count = 0;
	dict_OffsetIndex = 0;
	nextSig(in->NVALS, in->tokens, &sigptr);
	if (sigptr.len == -1)
		return;
	out->Count++;
	memcpy(cMin, sigptr.data, sigptr.len);
	memcpy(cMax, sigptr.data, sigptr.len);
	cMinLen = cMaxLen = sigptr.len;
	
	for (nextSig(in->NVALS, in->tokens, &sigptr); sigptr.len != -1;
	  nextSig(in->NVALS, in->tokens, &sigptr), out->Count++) {
		string sig_utf8;
		if (utf8) {
			string cMin_utf8(cMin, cMinLen);
			string tmpString((char*)sigptr.data, sigptr.len);
			sig_utf8 = tmpString;
			cmp = utf8::idb_strcoll(cMin_utf8.c_str(), sig_utf8.c_str());
		} else {
			cmp = strncmp(cMin, (char*)sigptr.data, std::min(cMinLen, sigptr.len));
		}
		if (cmp > 0) {
			memcpy(cMin, sigptr.data, sigptr.len);
			cMinLen = sigptr.len;
		}
		if (utf8) {
			string cMax_utf8(cMax, cMaxLen);
			cmp = utf8::idb_strcoll(cMax_utf8.c_str(), sig_utf8.c_str());
		} else {
			cmp = strncmp(cMax, (char*)sigptr.data, std::min(cMaxLen, sigptr.len));
		}
		if (cmp < 0) {
			memcpy(cMax, sigptr.data, sigptr.len);
			cMaxLen = sigptr.len;
		}
	}

	//we now have the results, stuff them into the output buffer
#ifdef PRIM_DEBUG
	unsigned size = sizeof(AggregateSignatureResultHeader) + cMaxLen + cMinLen
		+ sizeof(uint16_t) * 2;
	if (outSize < size) {
		MessageLog logger(LoggingID(28));
		logging::Message::Args marker;
		Message msg(35);

		marker.add(11);
		msg.format(marker);
		logger.logErrorMessage(msg);

		throw length_error("PrimitiveProcessor::p_AggregateSignature(): output buffer is too small");
	}
#endif

	min = reinterpret_cast<DataValue *>
		(&niceOutput[sizeof(AggregateSignatureResultHeader)]);
	max = reinterpret_cast<DataValue *>
		(&niceOutput[sizeof(AggregateSignatureResultHeader) + cMinLen + sizeof(uint16_t)]);
	min->len = cMinLen;
	max->len = cMaxLen;
	memcpy(min->data, cMin, cMinLen);
	memcpy(max->data, cMax, cMaxLen);
	*written = sizeof(AggregateSignatureResultHeader) + cMaxLen + cMinLen
		+ sizeof(uint16_t) * 2;
}

const char backslash = '\\';

inline bool PrimitiveProcessor::isEscapedChar(char c)
{
	return ('%' == c || '_' == c);
}

//FIXME: copy/pasted to dataconvert.h: refactor
int PrimitiveProcessor::convertToRegexp(idb_regex_t *regex, const p_DataValue *str)
{
	//In the worst case, every char is quadrupled, plus some leading/trailing cruft...
	char* cBuf = (char*)alloca(((4 * str->len) + 3) * sizeof(char));
	char c;
	int i, cBufIdx = 0;
	// translate to regexp symbols
	cBuf[cBufIdx++] = '^';  // implicit leading anchor
	for (i = 0; i < str->len; i++) {
		c = (char) str->data[i];
		switch (c) {

			// chars to substitute
			case '%':
				cBuf[cBufIdx++] = '.';
				cBuf[cBufIdx++] = '*';
				break;
			case '_':
				cBuf[cBufIdx++] = '.';
				break;

			// escape the chars that are special in regexp's but not in SQL
			// default special characters in perl: .[{}()\*+?|^$
			case '.':
			case '*':
			case '^':
			case '$':
 			case '?':
 			case '+':
 			case '|':
 			case '[':
 			case ']':
 			case '{':
 			case '}':
 			case '(':
 			case ')':
				cBuf[cBufIdx++] = backslash;
				cBuf[cBufIdx++] = c;
				break;
			case backslash:  //this is the sql escape char
				if ( i + 1 < str->len)
				{
					if (isEscapedChar(str->data[i+1]))
					{
						cBuf[cBufIdx++] = str->data[++i];
						break;
					}
					else if (backslash == str->data[i+1])
					{
						cBuf[cBufIdx++] = c;
						cBuf[cBufIdx++] = str->data[++i];
						break;
					}
					
				}  //single slash
				cBuf[cBufIdx++] = backslash;
				cBuf[cBufIdx++] = c;
				break;
			default:
				cBuf[cBufIdx++] = c;
		}
	}
	cBuf[cBufIdx++] = '$';  // implicit trailing anchor
	cBuf[cBufIdx++] = '\0';

#ifdef VERBOSE
  	cerr << "regexified string is " << cBuf << endl;
#endif

#ifdef POSIX_REGEX
  	regcomp(&regex->regex, cBuf, REG_NOSUB | REG_EXTENDED);
#else
	regex->regex = cBuf;
#endif
	regex->used = true;
	return 0;
}

bool PrimitiveProcessor::isLike(const p_DataValue *dict, const idb_regex_t *regex) throw()
{
#ifdef POSIX_REGEX
	char cBuf[dict->len + 1];
	memcpy(cBuf, dict->data, dict->len);
	cBuf[dict->len] = '\0';

	return (regexec(&regex->regex, cBuf, 0, NULL, 0) == 0);
#else
	/* Note, the passed-in pointers are effectively begin() and end() iterators */
	return regex_match(dict->data, dict->data + dict->len, regex->regex);
#endif
}

boost::shared_array<idb_regex_t>
PrimitiveProcessor::makeLikeFilter (const DictFilterElement *filterString, uint count)
{
	boost::shared_array<idb_regex_t> ret;
	uint filterIndex, filterOffset;
	uint8_t *in8 = (uint8_t *) filterString;
	const DictFilterElement *filter;
	p_DataValue filterptr = {0, NULL};

	for (filterIndex = 0, filterOffset = 0; filterIndex < count; filterIndex++) {
		filter = reinterpret_cast<const DictFilterElement *>(&in8[filterOffset]);

		if (filter->COP & COMPARE_LIKE) {
			if (!ret)
				ret.reset(new idb_regex_t[count]);

			filterptr.len = filter->len;
			filterptr.data = filter->data;
			convertToRegexp(&ret[filterIndex], &filterptr);
		}
		filterOffset += sizeof(DictFilterElement) + filter->len;
	}
	return ret;
}

void PrimitiveProcessor::p_Dictionary(const DictInput *in, vector<uint8_t> *out, bool utf8,
		bool skipNulls, boost::shared_ptr<DictEqualityFilter> eqFilter, uint8_t eqOp)
{
	PrimToken *outToken;
	const DictFilterElement *filter=0;
	const uint8_t* in8;
	DataValue *outValue;
	p_DataValue min={0, NULL}, max={0, NULL}, sigptr={0, NULL};
	int tmp, filterIndex, filterOffset;
	uint16_t aggCount;
	bool cmpResult;
	DictOutput header;

	// default size of the ouput to something sufficiently large to prevent
	// excessive reallocation and copy when resizing
	const unsigned DEF_OUTSIZE = 16*1024;
	// use this factor to scale out size of future resize calls
	const int SCALE_FACTOR = 2;
	out->resize(DEF_OUTSIZE);

	in8 = reinterpret_cast<const u_int8_t *>(in);

	memcpy(&header, in, sizeof(ISMPacketHeader) + sizeof(PrimitiveHeader));
	header.ism.Command = DICT_RESULTS;
	header.NVALS = 0;
	header.LBID = in->LBID;
	dict_OffsetIndex = 0;
	filterIndex = 0;
	aggCount = 0;
	min.len = 0;
	max.len = 0;

	//...Initialize I/O counts
	header.CacheIO    = 0;
	header.PhysicalIO = 0;

	header.NBYTES = sizeof(DictOutput);

	for (nextSig(in->NVALS, in->tokens, &sigptr, in->OutputType,
			(in->InputFlags ? true : false), skipNulls);
	  sigptr.len != -1;
	  nextSig(in->NVALS, in->tokens, &sigptr, in->OutputType,
			  (in->InputFlags ? true : false), skipNulls)) {

		string sig_utf8;
		if (utf8) {
			string tmpString((char*)sigptr.data, sigptr.len);
			sig_utf8 = tmpString;
		}

		// do aggregate processing
		if (in->OutputType & OT_AGGREGATE) {
			// len == 0 indicates this is the first pass
			if (max.len != 0) {
				if (utf8 ) {
					string max_utf8((char*)max.data, max.len);
					tmp = utf8::idb_strcoll(sig_utf8.c_str(), max_utf8.c_str());
				} else {
					tmp = strncmp((char *)sigptr.data, (char *)max.data, std::min(sigptr.len, max.len));
				}
				if (tmp > 0)
					max = sigptr;
			}
			else
				max = sigptr;

			if (min.len != 0) {
				if (utf8) {
					string min_utf8((char*)min.data, min.len);
					tmp = utf8::idb_strcoll(sig_utf8.c_str(), min_utf8.c_str());
				} else {
					tmp = strncmp((char *)sigptr.data, (char *)min.data, std::min(sigptr.len, min.len));
				}
				if (tmp < 0)
					min = sigptr;
			}
			else
				min = sigptr;
			aggCount++;
		}
	
		// filter processing
		if (in->InputFlags == 1)
			filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(OldGetSigParams));
		else
			filterOffset = sizeof(DictInput) + (in->NVALS * sizeof(PrimToken));

		if (eqFilter) {
			bool gotIt = (eqFilter->find(string((char *) sigptr.data, sigptr.len))
					!= eqFilter->end());
			if ((gotIt && eqOp == COMPARE_EQ) || (!gotIt && eqOp == COMPARE_NE))
				goto store;
			goto no_store;
		}

		for (filterIndex = 0; filterIndex < in->NOPS; filterIndex++) {
			filter = reinterpret_cast<const DictFilterElement *>(&in8[filterOffset]);
			string filt_utf8;
			size_t filt_utf8_len=0;
			if (utf8) {
				string tmpString((const char *)filter->data, filter->len);
				filt_utf8 = tmpString;
				filt_utf8_len = filt_utf8.length();
			}

			if (filter->COP & COMPARE_LIKE) {
  				cmpResult = isLike(&sigptr, &parsedLikeFilter[filterIndex]);
 				if (filter->COP & COMPARE_NOT)
 					cmpResult = !cmpResult;
			}
			else {
				if (utf8) {
					size_t sig_utf8_len = sig_utf8.length();
					tmp = utf8::idb_strcoll(sig_utf8.c_str(), filt_utf8.c_str());
					cmpResult = compare(tmp, filter->COP, sig_utf8_len, filt_utf8_len);
				} else {
					tmp = strncmp((const char *) sigptr.data, (const char *)filter->data, 
						std::min(sigptr.len, static_cast<int>(filter->len)));
				}
				cmpResult = compare(tmp, filter->COP, sigptr.len, filter->len);
			}

			if (!cmpResult && in->BOP != BOP_OR)
				goto no_store;
			if (cmpResult && in->BOP != BOP_AND)
				goto store;
			filterOffset += sizeof(DictFilterElement) + filter->len;	
		}
		if (filterIndex == in->NOPS && in->BOP != BOP_OR) {
store:
			//cout << "storing it, str = " << string((char *)sigptr.data, sigptr.len) << endl;
			header.NVALS++;
			if (in->OutputType & OT_RID && in->InputFlags == 1) {			// hack that indicates old GetSignature behavior
				const OldGetSigParams *oldParams;
				u_int64_t *outRid;
				oldParams = reinterpret_cast<const OldGetSigParams *>(in->tokens);
				uint32_t newlen = header.NBYTES + 8;
				if( newlen > out->size() )
				{
					out->resize( out->size() * SCALE_FACTOR );
				}
				outRid = (uint64_t *) &(*out)[header.NBYTES];
				// mask off the upper bit of the rid; signifies the NULL token was passed in
				*outRid = (oldParams[dict_OffsetIndex - 1].rid & 0x7fffffffffffffffLL);
				header.NBYTES += 8;
			}

			if (in->OutputType & OT_INPUTARG && in->InputFlags == 0) {
				uint32_t newlen = header.NBYTES + sizeof(DataValue) + filter->len;
				if( newlen > out->size() )
				{
					out->resize( out->size() * SCALE_FACTOR );
				}
				outValue = reinterpret_cast<DataValue *>(&(*out)[header.NBYTES]);
				outValue->len = filter->len;
				memcpy(outValue->data, filter->data, filter->len);
				header.NBYTES += sizeof(DataValue) + filter->len;
			}
			if (in->OutputType & OT_TOKEN) {
				uint32_t newlen = header.NBYTES + sizeof(PrimToken);
				if( newlen > out->size() )
				{
					out->resize( out->size() * SCALE_FACTOR );
				}
				outToken = reinterpret_cast<PrimToken *>(&(*out)[header.NBYTES]);
				outToken->LBID = in->LBID;
				outToken->offset = currentOffsetIndex;
				outToken->len = filter->len;
				header.NBYTES += sizeof(PrimToken);
			}
			if (in->OutputType & OT_DATAVALUE) {
				uint32_t newlen = header.NBYTES + sizeof(DataValue) + sigptr.len;
				if( newlen > out->size() )
				{
					out->resize( out->size() * SCALE_FACTOR );
				}
				outValue = reinterpret_cast<DataValue *>(&(*out)[header.NBYTES]);
				outValue->len = sigptr.len;
				memcpy(outValue->data, sigptr.data, sigptr.len);
				header.NBYTES += sizeof(DataValue) + sigptr.len;
			}
		}
no_store: ;  // intentional
	}

	if (in->OutputType & OT_AGGREGATE) {
		uint32_t newlen = header.NBYTES + 3*sizeof(uint16_t) + min.len + max.len;
		if( newlen > out->size() )
		{
			out->resize( out->size() * SCALE_FACTOR );
		}
		uint16_t *tmp16 = reinterpret_cast<uint16_t *>(&(*out)[header.NBYTES]);
		DataValue *tmpDV = reinterpret_cast<DataValue *>(&(*out)[header.NBYTES + sizeof(uint16_t)]);

		*tmp16 = aggCount;
		tmpDV->len = min.len;
		memcpy(tmpDV->data, min.data, min.len);
		header.NBYTES += 2*sizeof(uint16_t) + min.len;

		tmpDV = reinterpret_cast<DataValue *>(&(*out)[header.NBYTES]);
		tmpDV->len = max.len;
		memcpy(tmpDV->data, max.data, max.len);
		header.NBYTES += sizeof(uint16_t) + max.len;
	}

	out->resize( header.NBYTES );

	memcpy(&(*out)[0], &header, sizeof(DictOutput));
}

}
// vim:ts=4 sw=4:

